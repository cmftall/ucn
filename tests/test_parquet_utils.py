from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from dsfi.parquet_utils import (
    _is_visible_parquet_file,
    _path_contains_parquet_data_files,
    check_parquet_is_empty,
)


class FakePath:
    def __init__(self, raw_path: str):
        self._raw_path = raw_path

    def getFileSystem(self, hadoop_conf):
        return hadoop_conf.filesystem

    def getName(self):
        return self._raw_path.rstrip("/").split("/")[-1]

    def toString(self):
        return self._raw_path


class FakeFileStatus:
    def __init__(self, raw_path: str, is_file: bool, length: int):
        self._path = FakePath(raw_path)
        self._is_file = is_file
        self._length = length

    def isFile(self):
        return self._is_file

    def getLen(self):
        return self._length

    def getPath(self):
        return self._path


class FakeRemoteIterator:
    def __init__(self, items):
        self._items = list(items)
        self._index = 0

    def hasNext(self):
        return self._index < len(self._items)

    def next(self):
        item = self._items[self._index]
        self._index += 1
        return item


class FakeFileSystem:
    def __init__(
        self,
        *,
        exists: bool,
        root_is_file: bool = False,
        root_length: int = 0,
        listed_files=None,
    ):
        self._exists = exists
        self._root_is_file = root_is_file
        self._root_length = root_length
        self._listed_files = listed_files or []

    def exists(self, _path):
        return self._exists

    def getFileStatus(self, path):
        return FakeFileStatus(path.toString(), self._root_is_file, self._root_length)

    def listFiles(self, _path, _recursive):
        statuses = [FakeFileStatus(raw_path, True, length) for raw_path, length in self._listed_files]
        return FakeRemoteIterator(statuses)


def build_spark_for_fs(fs):
    jsc = SimpleNamespace(hadoopConfiguration=lambda: SimpleNamespace(filesystem=fs))
    jvm = SimpleNamespace(
        org=SimpleNamespace(
            apache=SimpleNamespace(hadoop=SimpleNamespace(fs=SimpleNamespace(Path=FakePath)))
        )
    )
    return SimpleNamespace(_jsc=jsc, _jvm=jvm)


class PathContainsParquetDataFilesTests(unittest.TestCase):
    def test_returns_false_when_path_does_not_exist(self):
        spark = build_spark_for_fs(FakeFileSystem(exists=False))
        self.assertFalse(_path_contains_parquet_data_files(spark, "/tmp/missing"))

    def test_returns_false_when_directory_is_empty(self):
        spark = build_spark_for_fs(FakeFileSystem(exists=True))
        self.assertFalse(_path_contains_parquet_data_files(spark, "/tmp/empty"))

    def test_returns_false_when_directory_has_only_hidden_files(self):
        spark = build_spark_for_fs(
            FakeFileSystem(
                exists=True,
                listed_files=[
                    ("/tmp/data/_SUCCESS", 0),
                    ("/tmp/data/.temporary", 120),
                    ("/tmp/data/_metadata", 64),
                ],
            )
        )
        self.assertFalse(_path_contains_parquet_data_files(spark, "/tmp/data"))

    def test_returns_true_when_directory_has_visible_parquet_file(self):
        spark = build_spark_for_fs(
            FakeFileSystem(
                exists=True,
                listed_files=[
                    ("/tmp/data/_SUCCESS", 0),
                    ("/tmp/data/part-00000-aaaa.snappy.parquet", 12),
                ],
            )
        )
        self.assertTrue(_path_contains_parquet_data_files(spark, "/tmp/data"))

    def test_returns_false_when_single_parquet_file_is_zero_bytes(self):
        spark = build_spark_for_fs(
            FakeFileSystem(exists=True, root_is_file=True, root_length=0)
        )
        self.assertFalse(_path_contains_parquet_data_files(spark, "/tmp/data/file.parquet"))

    def test_returns_true_when_single_parquet_file_has_content(self):
        spark = build_spark_for_fs(
            FakeFileSystem(exists=True, root_is_file=True, root_length=200)
        )
        self.assertTrue(_path_contains_parquet_data_files(spark, "/tmp/data/file.parquet"))


class CheckParquetIsEmptyTests(unittest.TestCase):
    def _build_spark_read_mock(self, row_count: int):
        spark = MagicMock()
        dataframe = MagicMock()
        limited_dataframe = MagicMock()
        limited_dataframe.count.return_value = row_count
        dataframe.limit.return_value = limited_dataframe
        spark.read.parquet.return_value = dataframe
        return spark

    def test_returns_true_for_empty_path_input(self):
        spark = MagicMock()
        self.assertTrue(check_parquet_is_empty(spark, "  "))

    def test_returns_true_when_no_visible_parquet_data_files(self):
        spark = MagicMock()
        with patch("dsfi.parquet_utils._path_contains_parquet_data_files", return_value=False):
            self.assertTrue(check_parquet_is_empty(spark, "/tmp/path"))
            spark.read.parquet.assert_not_called()

    def test_returns_true_when_dataset_has_zero_row(self):
        spark = self._build_spark_read_mock(row_count=0)
        with patch("dsfi.parquet_utils._path_contains_parquet_data_files", return_value=True):
            self.assertTrue(check_parquet_is_empty(spark, "/tmp/path"))

    def test_returns_false_when_dataset_has_at_least_one_row(self):
        spark = self._build_spark_read_mock(row_count=1)
        with patch("dsfi.parquet_utils._path_contains_parquet_data_files", return_value=True):
            self.assertFalse(check_parquet_is_empty(spark, "/tmp/path"))

    def test_returns_true_for_known_missing_path_exception_message(self):
        spark = MagicMock()
        spark.read.parquet.side_effect = RuntimeError("Path does not exist: /tmp/path")
        with patch("dsfi.parquet_utils._path_contains_parquet_data_files", return_value=True):
            self.assertTrue(check_parquet_is_empty(spark, "/tmp/path"))

    def test_raises_for_unexpected_exception(self):
        spark = MagicMock()
        spark.read.parquet.side_effect = RuntimeError("Some unrelated Spark error")
        with patch("dsfi.parquet_utils._path_contains_parquet_data_files", return_value=True):
            with self.assertRaises(RuntimeError):
                check_parquet_is_empty(spark, "/tmp/path")


class VisibleParquetFileTests(unittest.TestCase):
    def test_visible_parquet_file_detection(self):
        self.assertTrue(_is_visible_parquet_file("part-0000.snappy.parquet"))
        self.assertFalse(_is_visible_parquet_file("_SUCCESS"))
        self.assertFalse(_is_visible_parquet_file(".hidden.parquet"))
        self.assertFalse(_is_visible_parquet_file("README.txt"))


if __name__ == "__main__":
    unittest.main()
