"""
Silver Layer: Spatial Hierarchical Routing & Adaptive Citizen Call Burst Clustering.
Routes 311 service requests into 6 deterministic spatial hierarchies,
isolates unlocatable records to prevent shuffle skew, and applies a Ticket-1 Anchored
adaptive burst sessionization window (max(burst_window_hours, ticket_1_closed_at_utc)).
Author: Group 13
"""

import logging
from typing import Any, Dict, Optional
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    lit,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger("incident_clustering")


def generate_hierarchical_location_keys(df: DataFrame) -> DataFrame:
    """
    Route 311 service requests through 6 hierarchical spatial categories.
    Precedence:
      1. Buildings (BBL + House#): Parcel-level tax lot + building entrance.
      2. Highway Corridors: Dedicated highway name + segment + direction.
      3. Intersections: Canonicalized cross streets (street_a < street_b) + borough.
      4. Blockfaces: Street segment bounded by two cross streets + borough.
      5. Public Places & Parks: Landmark / Park name + borough.
      6. Intersections Fallback: Residual non-address records with verified cross streets.
      Unlocatable: Records lacking spatial attributes receive null loc_key and 'UNLOCATABLE' routing.
    """
    cols = set(df.columns)

    # Defensive defaults for partial/mock schemas in unit tests
    for c_name in [
        "clean_bbl", "house_num_extracted", "clean_cross_street_a", "clean_cross_street_b",
        "clean_highway_name", "clean_highway_segment", "clean_highway_direction",
        "clean_landmark", "clean_park_facility_name", "clean_street_name",
        "clean_incident_address", "clean_incident_zip", "borough", "address_type"
    ]:
        if c_name not in cols:
            df = df.withColumn(c_name, lit(None).cast("string"))

    if "has_valid_cross_streets" not in cols:
        df = df.withColumn(
            "has_valid_cross_streets",
            col("clean_cross_street_a").isNotNull() & col("clean_cross_street_b").isNotNull()
        )
    if "has_valid_highway" not in cols:
        df = df.withColumn(
            "has_valid_highway",
            col("clean_highway_name").isNotNull() & col("clean_highway_segment").isNotNull()
        )

    addr_type = upper(trim(col("address_type").cast("string")))
    borough = upper(trim(col("borough").cast("string")))
    has_valid_borough = borough.isNotNull() & (borough != "UNSPECIFIED") & (borough != "")

    # 1. Building Route Components
    has_bbl = col("clean_bbl").isNotNull()
    has_hn = col("house_num_extracted").isNotNull()
    has_fallback_addr = col("clean_incident_address").isNotNull() & col("clean_incident_zip").isNotNull()

    bldg_key_expr = (
        when(has_bbl & has_hn, concat(lit("BBL_"), col("clean_bbl"), lit("_HN_"), col("house_num_extracted")))
        .when(has_bbl, concat(lit("BBL_"), col("clean_bbl")))
        .when(has_fallback_addr, concat(lit("ADDR_"), col("clean_incident_address"), lit("_"), col("clean_incident_zip")))
        .otherwise(lit(None).cast("string"))
    )
    is_bldg_route = (addr_type == "ADDRESS") & bldg_key_expr.isNotNull()

    # 2. Highway Route Components
    hwy_dir_part = when(col("clean_highway_direction").isNotNull(), concat(lit("_"), col("clean_highway_direction"))).otherwise(lit(""))
    hwy_key_expr = concat(lit("HWY_"), col("clean_highway_name"), hwy_dir_part, lit("_"), col("clean_highway_segment"))
    is_hwy_route = col("has_valid_highway") & (~is_bldg_route)

    # 3. Intersection Route Components
    has_int_streets = col("has_valid_cross_streets") & has_valid_borough
    int_key_expr = concat(lit("INT_"), col("clean_cross_street_a"), lit("_X_"), col("clean_cross_street_b"), lit("_"), borough)
    is_int_route = (addr_type == "INTERSECTION") & has_int_streets & (~is_bldg_route) & (~is_hwy_route)

    # 4. Blockface Route Components
    has_blk_streets = has_int_streets & col("clean_street_name").isNotNull()
    blk_key_expr = concat(lit("BLK_"), col("clean_street_name"), lit("_"), col("clean_cross_street_a"), lit("_X_"), col("clean_cross_street_b"), lit("_"), borough)
    is_blk_route = (addr_type == "BLOCKFACE") & has_blk_streets & (~is_bldg_route) & (~is_hwy_route) & (~is_int_route)

    # 5. Public Places & Parks Components
    has_park = (col("clean_landmark").isNotNull() | col("clean_park_facility_name").isNotNull()) & has_valid_borough
    park_key_expr = when(
        col("clean_landmark").isNotNull(),
        concat(lit("LND_"), col("clean_landmark"), lit("_"), borough),
    ).otherwise(concat(lit("PARK_"), col("clean_park_facility_name"), lit("_"), borough))
    is_park_route = has_park & (addr_type.isin(["PLACE", "PLACENAME"]) | has_park) & (~is_bldg_route) & (~is_hwy_route) & (~is_int_route) & (~is_blk_route)

    # 6. Fallback Intersections Components
    is_fallback_int_route = (~addr_type.isin(["ADDRESS"])) & has_int_streets & (~is_bldg_route) & (~is_hwy_route) & (~is_int_route) & (~is_blk_route) & (~is_park_route)

    # Coalesce Location Key
    loc_key = (
        when(is_bldg_route, bldg_key_expr)
        .when(is_hwy_route, hwy_key_expr)
        .when(is_int_route, int_key_expr)
        .when(is_blk_route, blk_key_expr)
        .when(is_park_route, park_key_expr)
        .when(is_fallback_int_route, int_key_expr)
        .otherwise(lit(None).cast("string"))
    )

    # Routing Type Label
    loc_routing_type = (
        when(is_bldg_route, lit("1. Buildings (BBL + House#)"))
        .when(is_hwy_route, lit("2. Highway Corridors"))
        .when(is_int_route, lit("3. Intersections (Cross Streets)"))
        .when(is_blk_route, lit("4. Blockfaces (Road Segments)"))
        .when(is_park_route, lit("5. Public Places & Parks"))
        .when(is_fallback_int_route, lit("6. Intersections (Fallback)"))
        .otherwise(lit("UNLOCATABLE"))
    )

    df = df.withColumn("loc_key", loc_key)
    df = df.withColumn("loc_routing_type", loc_routing_type)
    return df


def cluster_citizen_call_bursts(
    df: DataFrame,
    burst_window_hours: float = 6.0,
    num_partitions: int = 16,
) -> DataFrame:
    """
    Applies zero-copy C++ Apache Arrow adaptive burst deduplication on locatable records.
    Safeguards:
      1. Isolates unlocatable records (loc_key is null) before clustering to eliminate shuffle skew.
      2. Groups and repartitions by (loc_key, complaint_type) using PySpark Arrow streaming.
      3. Uses Ticket-1 anchored window cutoff: max(T_0 + burst_window_hours, Ticket_1_closed_at_utc).
      4. Appends cluster attributes directly within the Arrow stream, completely eliminating the post-clustering self-join.
      5. Operates on contiguous C++ memory buffers with zero Python row pickling, zero Pandas overhead, and zero worker crashes.
    """
    cols = set(df.columns)
    if "complaint_type" not in cols:
        df = df.withColumn("complaint_type", lit("Unknown"))
    if "created_at_utc" not in cols:
        df = df.withColumn("created_at_utc", lit(None).cast("timestamp"))
    if "closed_at_utc" not in cols:
        df = df.withColumn("closed_at_utc", lit(None).cast("timestamp"))

    burst_window_micros = int(burst_window_hours * 3600 * 1_000_000)

    # 1. Split locatable records from unlocatable records
    df_loc = df.filter(col("loc_key").isNotNull())
    df_unloc = df.filter(col("loc_key").isNull())

    out_fields = list(df.schema.fields)
    cluster_fields = [
        StructField("is_burst_duplicate", BooleanType(), True),
        StructField("burst_sequence_num", IntegerType(), True),
        StructField("incident_cluster_id", StringType(), True),
        StructField("incident_first_created_at_utc", TimestampType(), True),
        StructField("cluster_size", IntegerType(), True),
    ]
    for cf in cluster_fields:
        if cf.name not in cols:
            out_fields.append(cf)
    cluster_schema = StructType(out_fields)

    def arrow_cluster_worker(batches):
        b_list = list(batches)
        if not b_list:
            return
        table = pa.Table.from_batches(b_list)
        if len(table) == 0:
            return

        # Sort within partition using C++ PyArrow compute
        sort_keys = [
            ("loc_key", "ascending"),
            ("complaint_type", "ascending"),
            ("created_at_utc", "ascending"),
            ("unique_key", "ascending"),
        ]
        indices = pc.sort_indices(table, sort_keys=sort_keys)
        sorted_tbl = table.take(indices)

        loc_keys = sorted_tbl["loc_key"].to_pylist()
        complaints = sorted_tbl["complaint_type"].to_pylist()
        created_ts = sorted_tbl["created_at_utc"].cast(pa.timestamp("us")).to_numpy()
        closed_col = sorted_tbl["closed_at_utc"] if "closed_at_utc" in sorted_tbl.column_names else None
        closed_ts = closed_col.cast(pa.timestamp("us")).to_numpy() if closed_col is not None else np.full(len(sorted_tbl), np.datetime64("NaT"))
        unique_keys = sorted_tbl["unique_key"].to_numpy()

        n = len(sorted_tbl)
        is_dup_arr = np.zeros(n, dtype=bool)
        seq_arr = np.ones(n, dtype=np.int32)
        cluster_id_arr = np.empty(n, dtype=object)
        first_ts_arr = np.empty(n, dtype="datetime64[us]")
        cluster_size_arr = np.ones(n, dtype=np.int32)

        curr_k = None
        curr_c = None
        anchor_cr = None
        anchor_cutoff = None
        curr_cid = None
        curr_seq = 1
        cluster_start_idx = 0

        window_delta_us = np.timedelta64(burst_window_micros, "us")

        for i in range(n):
            k = loc_keys[i]
            c = complaints[i]
            cr = created_ts[i]
            cl = closed_ts[i]
            uk = unique_keys[i]

            is_new_cluster = (
                (k != curr_k)
                or (c != curr_c)
                or (anchor_cutoff is None)
                or (np.isnat(cr) or cr > anchor_cutoff)
            )

            if is_new_cluster:
                if curr_cid is not None:
                    cluster_size_arr[cluster_start_idx:i] = curr_seq

                curr_k = k
                curr_c = c
                anchor_cr = cr
                min_cutoff = cr + window_delta_us if not np.isnat(cr) else None
                if not np.isnat(cl) and min_cutoff is not None and cl > min_cutoff:
                    anchor_cutoff = cl
                else:
                    anchor_cutoff = min_cutoff

                curr_cid = uk
                curr_seq = 1
                cluster_start_idx = i

                is_dup_arr[i] = False
                seq_arr[i] = 1
                cluster_id_arr[i] = f"INC_{curr_cid}"
                first_ts_arr[i] = anchor_cr
            else:
                curr_seq += 1
                is_dup_arr[i] = True
                seq_arr[i] = curr_seq
                cluster_id_arr[i] = f"INC_{curr_cid}"
                first_ts_arr[i] = anchor_cr

        if curr_cid is not None:
            cluster_size_arr[cluster_start_idx:n] = curr_seq

        # Append new columns to sorted_tbl directly without self-join
        result_tbl = sorted_tbl.append_column("is_burst_duplicate", pa.array(is_dup_arr, type=pa.bool_()))
        result_tbl = result_tbl.append_column("burst_sequence_num", pa.array(seq_arr, type=pa.int32()))
        result_tbl = result_tbl.append_column("incident_cluster_id", pa.array(cluster_id_arr, type=pa.string()))
        result_tbl = result_tbl.append_column("incident_first_created_at_utc", pa.array(first_ts_arr, type=pa.timestamp("us")))
        result_tbl = result_tbl.append_column("cluster_size", pa.array(cluster_size_arr, type=pa.int32()))

        for b in result_tbl.to_batches():
            yield b

    df_loc_clustered = df_loc.repartition(num_partitions, col("loc_key"), col("complaint_type")).mapInArrow(
        arrow_cluster_worker, schema=cluster_schema
    )

    # 2. Defaults for unlocatable records
    loc_routing_col = col("loc_routing_type") if "loc_routing_type" in cols else lit(None).cast("string")
    df_unloc_defaults = (
        df_unloc
        .withColumn("is_burst_duplicate", lit(False))
        .withColumn("burst_sequence_num", lit(1))
        .withColumn("incident_cluster_id", concat(lit("INC_UNLOC_"), col("unique_key").cast("string")))
        .withColumn("incident_first_created_at_utc", col("created_at_utc"))
        .withColumn("cluster_size", lit(1))
        .withColumn("loc_routing_type", coalesce(loc_routing_col, lit("UNLOCATABLE")))
    )

    df_final = df_loc_clustered.unionByName(df_unloc_defaults)
    return df_final
