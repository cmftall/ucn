"""Unit tests for commons_dsn.dsn_io."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from commons_dsn.dsn_io import check_parquet_is_empty

try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError:  # pragma: no cover - environment specific
    SparkSession = None


@unittest.skipIf(SparkSession is None, "pyspark is not installed")
class CheckParquetIsEmptyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            cls.spark = (
                SparkSession.builder.master("local[1]")
                .appName("check-parquet-is-empty-tests")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
        except Exception as error:  # pragma: no cover - depends on runtime
            raise unittest.SkipTest(f"Unable to start SparkSession: {error}") from error

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_returns_true_when_path_does_not_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing_path = Path(tmp_dir) / "missing"
            self.assertTrue(check_parquet_is_empty(str(missing_path), self.spark))

    def test_returns_true_for_empty_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dataset_dir = Path(tmp_dir) / "dataset"
            dataset_dir.mkdir(parents=True, exist_ok=True)
            self.assertTrue(check_parquet_is_empty(str(dataset_dir), self.spark))

    def test_returns_true_when_directory_has_no_parquet_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dataset_dir = Path(tmp_dir) / "dataset"
            dataset_dir.mkdir(parents=True, exist_ok=True)
            (dataset_dir / "_SUCCESS").write_text("", encoding="utf-8")
            self.assertTrue(check_parquet_is_empty(str(dataset_dir), self.spark))

    def test_returns_true_when_all_parquet_files_are_zero_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dataset_dir = Path(tmp_dir) / "dataset"
            empty_df = self.spark.createDataFrame([], "id INT")
            empty_df.write.mode("overwrite").parquet(str(dataset_dir))
            self.assertTrue(check_parquet_is_empty(str(dataset_dir), self.spark))

    def test_returns_false_when_parquet_has_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dataset_dir = Path(tmp_dir) / "dataset"
            dataframe = self.spark.createDataFrame([(1,), (2,), (3,)], "id INT")
            dataframe.write.mode("overwrite").parquet(str(dataset_dir))
            self.assertFalse(check_parquet_is_empty(str(dataset_dir), self.spark))

    def test_supports_file_uri_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dataset_dir = Path(tmp_dir) / "dataset"
            dataframe = self.spark.createDataFrame([(1,), (2,)], "id INT")
            dataframe.write.mode("overwrite").parquet(str(dataset_dir))
            self.assertFalse(check_parquet_is_empty(dataset_dir.as_uri(), self.spark))


if __name__ == "__main__":
    unittest.main()
