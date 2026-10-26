"""
Unit tests for the Bronze/Silver Partition Compaction Utility.
Validates row-count invariance, atomic file replacement, and dry-run safety.
Author: Group 13
"""

import tempfile
import unittest
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.compaction import compact_partition, compact_partitions, find_leaf_partitions


class TestPartitionCompaction(unittest.TestCase):

    def test_compaction_integrity_and_invariance(self):
        """Verify that multi-file partitions coalesce into 1 file with zero row loss."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            part_dir = base_dir / "year=2026" / "month=10"
            part_dir.mkdir(parents=True)

            # Create 3 small mock parquet files
            df1 = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
            df2 = pd.DataFrame({"id": [3, 4, 5], "val": ["c", "d", "e"]})
            df3 = pd.DataFrame({"id": [6], "val": ["f"]})

            df1.to_parquet(part_dir / "part_001.parquet", index=False)
            df2.to_parquet(part_dir / "part_002.parquet", index=False)
            df3.to_parquet(part_dir / "part_003.parquet", index=False)

            self.assertEqual(len(list(part_dir.glob("*.parquet"))), 3)

            res = compact_partition(part_dir)
            self.assertEqual(res["status"], "compacted")
            self.assertEqual(res["files_before"], 3)
            self.assertEqual(res["files_after"], 1)
            self.assertEqual(res["rows_before"], 6)
            self.assertEqual(res["rows_after"], 6)

            # Check that only 1 file remains on disk and contains all 6 rows
            remaining_files = list(part_dir.glob("*.parquet"))
            self.assertEqual(len(remaining_files), 1)
            df_compacted = pd.read_parquet(remaining_files[0])
            self.assertEqual(len(df_compacted), 6)
            self.assertEqual(list(df_compacted["id"]), [1, 2, 3, 4, 5, 6])

    def test_single_file_partition_skipped(self):
        """Verify that single-file partitions are skipped without modification."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            part_dir = base_dir / "grid_id=GRID_40.7_-74.0" / "year=2026"
            part_dir.mkdir(parents=True)

            df = pd.DataFrame({"id": [1, 2], "temp": [20.0, 21.0]})
            df.to_parquet(part_dir / "part_001.parquet", index=False)

            res = compact_partition(part_dir)
            self.assertEqual(res["status"], "skipped")
            self.assertEqual(res["files_before"], 1)

    def test_dry_run_mode(self):
        """Verify that dry_run mode does not alter files on disk."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            part_dir = base_dir / "year=2026" / "month=07"
            part_dir.mkdir(parents=True)

            df1 = pd.DataFrame({"id": [1]})
            df2 = pd.DataFrame({"id": [2]})
            df1.to_parquet(part_dir / "part_001.parquet", index=False)
            df2.to_parquet(part_dir / "part_002.parquet", index=False)

            res = compact_partition(part_dir, dry_run=True)
            self.assertEqual(res["status"], "dry_run")
            self.assertEqual(len(list(part_dir.glob("*.parquet"))), 2)


if __name__ == "__main__":
    unittest.main()
