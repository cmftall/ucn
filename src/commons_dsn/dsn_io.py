"""Helpers for reading DSN data sources."""

from __future__ import annotations

from pyspark.sql import SparkSession

try:  # pyspark >= 3.4
    from pyspark.errors import AnalysisException as SparkAnalysisException
except ImportError:  # pyspark < 3.4
    from pyspark.sql.utils import AnalysisException as SparkAnalysisException

_EMPTY_PARQUET_ERROR_SNIPPETS = (
    "path does not exist",
    "unable to infer schema for parquet",
    "cannot infer schema for parquet",
    "cannot find path",
    "does not exist",
)


def _is_empty_perimeter_error(exception: SparkAnalysisException) -> bool:
    """Return True when Spark error means empty/missing parquet perimeter."""
    lowered_message = str(exception).lower()
    return any(snippet in lowered_message for snippet in _EMPTY_PARQUET_ERROR_SNIPPETS)


def check_parquet_is_empty(path: str, spark: SparkSession) -> bool:
    """
    Return True when a parquet perimeter is empty.

    The perimeter is considered empty when:
    - the path does not exist,
    - the directory exists but contains no parquet schema/data,
    - parquet files exist but all contain zero row.

    This helper relies only on PySpark and works for local paths and HDFS paths.
    """
    try:
        dataframe = spark.read.parquet(path)
    except SparkAnalysisException as exception:
        if _is_empty_perimeter_error(exception):
            return True
        raise

    if hasattr(dataframe, "isEmpty"):
        return dataframe.isEmpty()
    return len(dataframe.take(1)) == 0
