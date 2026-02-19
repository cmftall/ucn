"""Unit tests for commons_dsn.dsn_io."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from commons_dsn.dsn_io import check_parquet_is_empty


def _write_parquet(path: Path, rows: int) -> None:
    values = pa.array(list(range(rows)), type=pa.int64())
    table = pa.table({"id": values})
    pq.write_table(table, str(path))


class CheckParquetIsEmptyTestCase(unittest.TestCase):
    def test_returns_true_when_path_does_not_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing_path = Path(tmp_dir) / "missing"
            self.assertTrue(check_parquet_is_empty(str(missing_path)))

    def test_returns_true_for_empty_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dataset_dir = Path(tmp_dir) / "dataset"
            dataset_dir.mkdir(parents=True, exist_ok=True)
            self.assertTrue(check_parquet_is_empty(str(dataset_dir)))

    def test_returns_true_when_directory_has_no_parquet_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dataset_dir = Path(tmp_dir) / "dataset"
            dataset_dir.mkdir(parents=True, exist_ok=True)
            (dataset_dir / "_SUCCESS").write_text("", encoding="utf-8")
            (dataset_dir / "notes.txt").write_text("no parquet", encoding="utf-8")
            self.assertTrue(check_parquet_is_empty(str(dataset_dir)))

    def test_returns_true_when_all_parquet_files_are_zero_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dataset_dir = Path(tmp_dir) / "dataset"
            dataset_dir.mkdir(parents=True, exist_ok=True)
            _write_parquet(dataset_dir / "part-0.parquet", rows=0)
            _write_parquet(dataset_dir / "part-1.parquet", rows=0)
            self.assertTrue(check_parquet_is_empty(str(dataset_dir)))

    def test_returns_false_when_parquet_has_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dataset_dir = Path(tmp_dir) / "dataset"
            dataset_dir.mkdir(parents=True, exist_ok=True)
            _write_parquet(dataset_dir / "part-0.parquet", rows=0)
            _write_parquet(dataset_dir / "part-1.parquet", rows=3)
            self.assertFalse(check_parquet_is_empty(str(dataset_dir)))

    def test_supports_file_uri_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dataset_dir = Path(tmp_dir) / "dataset"
            dataset_dir.mkdir(parents=True, exist_ok=True)
            _write_parquet(dataset_dir / "part-0.parquet", rows=2)
            self.assertFalse(check_parquet_is_empty(dataset_dir.as_uri()))


if __name__ == "__main__":
    unittest.main()
