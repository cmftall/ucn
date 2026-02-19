"""Detect empty DACD perimeter and write an HDFS/local flag file."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional


ROOT_SRC = Path(__file__).resolve().parents[1]
if str(ROOT_SRC) not in sys.path:
    sys.path.insert(0, str(ROOT_SRC))

from commons_dsn.dsn_io import check_parquet_is_empty


FLAG_NAME = "PERIMETRAGE_DACD_VIDE"


def _build_spark_session() -> Optional[object]:
    """Create a Spark session when pyspark is available."""
    try:
        from pyspark.sql import SparkSession
    except Exception:
        return None

    return (
        SparkSession.builder.appName("dsn_verificationdeclarative_perimetrage_vide")
        .getOrCreate()
    )


def _create_flag_file(flag_uri: str, spark: Optional[object]) -> None:
    """Create an empty flag file on Hadoop FS when Spark exists, else local FS."""
    if spark is not None:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        jvm = spark._jvm
        flag_path = jvm.org.apache.hadoop.fs.Path(flag_uri)
        fs = flag_path.getFileSystem(hadoop_conf)

        parent = flag_path.getParent()
        if parent is not None and not fs.exists(parent):
            fs.mkdirs(parent)

        output = fs.create(flag_path, True)
        output.close()
        return

    local_path = Path(flag_uri)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.touch(exist_ok=True)


def _join_output_flag(output_uri: str, flag_name: str) -> str:
    cleaned_output = output_uri.rstrip("/")
    return f"{cleaned_output}/{flag_name}"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect empty DACD perimeter and create a stop flag."
    )
    parser.add_argument(
        "--perimetrage-dacd-path",
        required=True,
        help="Parquet path for DACD perimeter.",
    )
    parser.add_argument(
        "--tir-output-path",
        required=True,
        help="Output path of TIR where the flag must be created.",
    )
    parser.add_argument(
        "--flag-name",
        default=FLAG_NAME,
        help=f"Flag filename to create when perimeter is empty (default: {FLAG_NAME}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    spark = _build_spark_session()

    try:
        is_empty = check_parquet_is_empty(args.perimetrage_dacd_path, spark=spark)
        if is_empty:
            flag_uri = _join_output_flag(args.tir_output_path, args.flag_name)
            _create_flag_file(flag_uri, spark)
            print(f"Perimetrage DACD vide detecte: flag cree -> {flag_uri}")
        else:
            print("Perimetrage DACD non vide: aucun flag cree.")
        return 0
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

