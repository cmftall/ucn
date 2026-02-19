"""Utilities for parquet datasets in DSFI pipelines."""

from __future__ import annotations

from typing import Any

try:  # PySpark >= 3.4
    from pyspark.errors import AnalysisException
except Exception:  # pragma: no cover - fallback for older/newer variants
    try:
        from pyspark.sql.utils import AnalysisException
    except Exception:  # pragma: no cover - local tests without pyspark
        class AnalysisException(Exception):
            """Fallback AnalysisException when PySpark is unavailable."""


try:
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - local tests without pyspark
    SparkSession = Any  # type: ignore[misc,assignment]


def _is_visible_parquet_file(file_name: str) -> bool:
    """Return True for non-hidden parquet files."""

    normalized_name = file_name.strip()
    if not normalized_name:
        return False
    if normalized_name.startswith("_") or normalized_name.startswith("."):
        return False
    return normalized_name.lower().endswith(".parquet")


def _path_contains_parquet_data_files(spark: SparkSession, parquet_path: str) -> bool:
    """
    Check if path contains at least one non-hidden parquet file.

    If Hadoop filesystem inspection is unavailable, return True and let Spark read
    handle the final decision.
    """

    try:
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(parquet_path)
        hadoop_conf = spark._jsc.hadoopConfiguration()
        filesystem = hadoop_path.getFileSystem(hadoop_conf)

        if not filesystem.exists(hadoop_path):
            return False

        path_status = filesystem.getFileStatus(hadoop_path)
        if path_status.isFile():
            file_name = path_status.getPath().getName()
            return _is_visible_parquet_file(file_name) and path_status.getLen() > 0

        files_iterator = filesystem.listFiles(hadoop_path, True)
        while files_iterator.hasNext():
            listed_file = files_iterator.next()
            file_name = listed_file.getPath().getName()
            if _is_visible_parquet_file(file_name) and listed_file.getLen() > 0:
                return True

        return False
    except Exception:
        return True


def _is_known_empty_parquet_error(error: Exception) -> bool:
    """Match Spark errors that indicate an empty or missing parquet dataset."""

    message = str(error).lower()
    empty_indicators = (
        "path does not exist",
        "unable to infer schema for parquet",
        "unable to infer schema",
        "no such file or directory",
        "file not found",
        "not found",
    )
    return any(indicator in message for indicator in empty_indicators)


def check_parquet_is_empty(spark: SparkSession, parquet_path: str) -> bool:
    """
    Return True when parquet dataset is empty or missing, else False.

    Covered scenarios include:
      - path does not exist
      - existing directory with no files
      - existing directory with only metadata/hidden files
      - parquet dataset with schema but no rows
    """

    if spark is None:
        raise ValueError("spark must not be None")

    if parquet_path is None or not parquet_path.strip():
        return True

    normalized_path = parquet_path.strip()

    if not _path_contains_parquet_data_files(spark, normalized_path):
        return True

    try:
        dataframe = spark.read.parquet(normalized_path)
        return dataframe.limit(1).count() == 0
    except AnalysisException as error:
        if _is_known_empty_parquet_error(error):
            return True
        raise
    except Exception as error:
        if _is_known_empty_parquet_error(error):
            return True
        raise
