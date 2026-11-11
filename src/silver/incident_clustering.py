"""
Silver Layer: Spatial Hierarchical Routing & Adaptive Citizen Call Burst Clustering.
Routes 311 service requests into 6 deterministic spatial hierarchies,
isolates unlocatable records to prevent shuffle skew, and applies a Ticket-1 Anchored
adaptive burst sessionization window (max(burst_window_hours, ticket_1_closed_at_utc)).
Author: Group 13
"""

import logging
from typing import Dict, Any, Optional
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    hash as spark_hash,
    lit,
    pmod,
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


def _sessionize_partition_udf(pdf: pd.DataFrame, burst_window_hours: float = 6.0) -> pd.DataFrame:
    """
    Stateful vectorized partition worker:
    Groups citizen calls by (loc_key, complaint_type) and applies Ticket-1 anchored clustering.
    Window Cutoff = max(Ticket_1_created + burst_window_hours, Ticket_1_closed_at_utc).
    """
    pdf = pdf.sort_values(["loc_key", "complaint_type", "created_at_utc", "unique_key"]).reset_index(drop=True)
    n = len(pdf)
    if n == 0:
        return pd.DataFrame(
            columns=[
                "unique_key",
                "is_burst_duplicate",
                "burst_sequence_num",
                "incident_cluster_id",
                "cluster_size",
                "incident_first_created_at_utc",
                "_cluster_bucket",
            ]
        )

    is_dup = np.zeros(n, dtype=bool)
    seq_num = np.ones(n, dtype=np.int32)
    cluster_first_ts = pdf["created_at_utc"].copy()
    cluster_id_arr = np.zeros(n, dtype=np.int64)

    k_arr = pdf["loc_key"].to_numpy()
    c_arr = pdf["complaint_type"].to_numpy()
    cr_arr = pdf["created_at_utc"].to_numpy()
    cl_arr = (
        pdf["closed_at_utc"].to_numpy()
        if "closed_at_utc" in pdf.columns
        else np.full(n, np.datetime64("NaT"))
    )
    uk_arr = pdf["unique_key"].to_numpy()

    curr_k = None
    curr_c = None
    anchor_cr = None
    anchor_cutoff = None
    curr_cluster_id = None
    curr_seq = 1

    window_delta = np.timedelta64(int(burst_window_hours * 3600), "s")

    for i in range(n):
        k = k_arr[i]
        c = c_arr[i]
        cr = cr_arr[i]
        cl = cl_arr[i]
        uk = uk_arr[i]

        # New partition group: (loc_key, complaint_type)
        if (k != curr_k) or (c != curr_c):
            curr_k = k
            curr_c = c
            anchor_cr = cr
            min_cutoff = cr + window_delta
            anchor_cutoff = max(min_cutoff, cl) if (pd.notna(cl) and cl > min_cutoff) else min_cutoff
            curr_cluster_id = uk
            curr_seq = 1
            is_dup[i] = False
            seq_num[i] = 1
            cluster_first_ts.iloc[i] = anchor_cr
            cluster_id_arr[i] = curr_cluster_id
        # Arrived within Ticket 1's active window cutoff
        elif cr <= anchor_cutoff:
            curr_seq += 1
            is_dup[i] = True
            seq_num[i] = curr_seq
            cluster_first_ts.iloc[i] = anchor_cr
            cluster_id_arr[i] = curr_cluster_id
        # Arrived past Ticket 1's cutoff -> Spawns new wave / new incident
        else:
            anchor_cr = cr
            min_cutoff = cr + window_delta
            anchor_cutoff = max(min_cutoff, cl) if (pd.notna(cl) and cl > min_cutoff) else min_cutoff
            curr_cluster_id = uk
            curr_seq = 1
            is_dup[i] = False
            seq_num[i] = 1
            cluster_first_ts.iloc[i] = anchor_cr
            cluster_id_arr[i] = curr_cluster_id

    # Format output DataFrame
    out_df = pd.DataFrame({
        "unique_key": pdf["unique_key"],
        "is_burst_duplicate": is_dup,
        "burst_sequence_num": seq_num,
        "incident_cluster_id": ["INC_" + str(cid) for cid in cluster_id_arr],
        "incident_first_created_at_utc": cluster_first_ts,
        "_cluster_bucket": pdf["_cluster_bucket"],
    })

    # Compute cluster size
    cluster_sizes = out_df.groupby("incident_cluster_id")["unique_key"].transform("count").astype(np.int32)
    out_df["cluster_size"] = cluster_sizes

    return out_df


def cluster_citizen_call_bursts(
    df: DataFrame,
    burst_window_hours: float = 6.0,
    num_partitions: int = 100,
) -> DataFrame:
    """
    Applies adaptive burst deduplication using PySpark applyInPandas on locatable records.
    Safeguards:
      1. Isolates unlocatable records (loc_key is null) before clustering to eliminate shuffle skew.
      2. Groups only by loc_key and complaint_type (never loc_env) to prevent false splits.
      3. Uses Ticket-1 anchored window cutoff: max(T_0 + burst_window_hours, Ticket_1_closed_at_utc).
      4. Left-joins cluster attributes back on unique_key and coalesces unlocated records.
    """
    cols = set(df.columns)
    if "complaint_type" not in cols:
        df = df.withColumn("complaint_type", lit("Unknown"))
    if "created_at_utc" not in cols:
        df = df.withColumn("created_at_utc", lit(None).cast("timestamp"))
    if "closed_at_utc" not in cols:
        df = df.withColumn("closed_at_utc", lit(None).cast("timestamp"))

    # 1. Split locatable records (99.8%) from unlocatable records (0.2%)
    df_locatable = (
        df.filter(col("loc_key").isNotNull())
        .select("unique_key", "loc_key", "complaint_type", "created_at_utc", "closed_at_utc")
    )

    # 2. Add deterministic partition bucket for distributed Spark executors
    df_locatable = df_locatable.withColumn("_cluster_bucket", pmod(spark_hash(col("loc_key")), lit(num_partitions)))

    cluster_schema = StructType([
        StructField("unique_key", LongType(), True),
        StructField("is_burst_duplicate", BooleanType(), True),
        StructField("burst_sequence_num", IntegerType(), True),
        StructField("incident_cluster_id", StringType(), True),
        StructField("incident_first_created_at_utc", TimestampType(), True),
        StructField("_cluster_bucket", IntegerType(), True),
        StructField("cluster_size", IntegerType(), True),
    ])

    def sessionize_wrapper(pdf: pd.DataFrame) -> pd.DataFrame:
        return _sessionize_partition_udf(pdf, burst_window_hours=burst_window_hours)

    # 3. Distributed sessionization
    df_clustered_attrs = (
        df_locatable.groupBy("_cluster_bucket")
        .applyInPandas(sessionize_wrapper, schema=cluster_schema)
        .drop("_cluster_bucket")
    )

    # 4. Left Join back to the full dataset on unique_key
    df_final = df.join(df_clustered_attrs, on="unique_key", how="left")

    # 5. Coalesce defaults for unlocated records
    loc_routing_col = col("loc_routing_type") if "loc_routing_type" in cols else lit(None).cast("string")
    df_final = (
        df_final
        .withColumn("is_burst_duplicate", coalesce(col("is_burst_duplicate"), lit(False)))
        .withColumn("burst_sequence_num", coalesce(col("burst_sequence_num"), lit(1)))
        .withColumn("cluster_size", coalesce(col("cluster_size"), lit(1)))
        .withColumn(
            "incident_cluster_id",
            coalesce(col("incident_cluster_id"), concat(lit("INC_UNLOC_"), col("unique_key").cast("string"))),
        )
        .withColumn("incident_first_created_at_utc", coalesce(col("incident_first_created_at_utc"), col("created_at_utc")))
        .withColumn("loc_routing_type", coalesce(loc_routing_col, lit("UNLOCATABLE")))
    )

    return df_final
