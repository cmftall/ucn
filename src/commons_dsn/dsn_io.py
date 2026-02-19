"""I/O helpers shared across DSN modules."""

from __future__ import annotations

from pathlib import Path
from typing import Optional


def _is_marker_file(file_path: Path) -> bool:
    """Return True for technical files that do not carry parquet rows."""
    name = file_path.name
    return name.startswith("_") or name.startswith(".")


def _has_local_data_files(parquet_path: Path) -> bool:
    """
    Check whether a local parquet directory contains candidate data files.

    A parquet folder may contain only technical artifacts (_SUCCESS, metadata).
    We treat such folders as empty when no candidate data file exists.
    """
    if parquet_path.is_file():
        return parquet_path.stat().st_size > 0

    candidate_files = []
    for item in parquet_path.rglob("*"):
        if item.is_file() and not _is_marker_file(item):
            candidate_files.append(item)

    if not candidate_files:
        return False

    # A zero-sized file cannot contain parquet rows.
    return any(file_path.stat().st_size > 0 for file_path in candidate_files)


def _check_with_spark(parquet_uri: str, spark) -> bool:
    """
    Check parquet emptiness using Hadoop FS + Spark read.

    Returns True when:
    - folder does not exist
    - folder exists but is empty
    - folder contains no readable parquet rows
    """
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(parquet_uri)
    fs = hadoop_path.getFileSystem(hadoop_conf)

    if not fs.exists(hadoop_path):
        return True

    statuses = fs.listStatus(hadoop_path)
    if not statuses:
        return True

    try:
        # Take one row only to avoid full scan.
        first_rows = spark.read.parquet(parquet_uri).take(1)
        return len(first_rows) == 0
    except Exception:
        # Covers malformed parquet and "no data" edge cases where Spark cannot read rows.
        return True


def check_parquet_is_empty(parquet_uri: str, spark: Optional[object] = None) -> bool:
    """
    Return True when a parquet perimeter should be considered empty.

    Supported scenarios:
    - non-existing folder
    - existing but empty folder
    - existing non-empty folder that contains no parquet data rows

    When a Spark session is provided, this function works for HDFS/local paths
    and uses Spark to detect "no rows" reliably.
    Without Spark, a local-filesystem fallback is used.
    """
    if not parquet_uri or not parquet_uri.strip():
        raise ValueError("parquet_uri must be a non-empty string")

    if spark is not None:
        return _check_with_spark(parquet_uri, spark)

    local_path = Path(parquet_uri)
    if not local_path.exists():
        return True
    if local_path.is_dir() and not any(local_path.iterdir()):
        return True

    return not _has_local_data_files(local_path)
