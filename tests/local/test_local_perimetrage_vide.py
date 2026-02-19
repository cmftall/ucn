#!/usr/bin/env python3
"""Local smoke test for dsn_verificationdeclarative_perimetrage_vide.main."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MAIN_SCRIPT = (
    REPO_ROOT / "src" / "dsn_verificationdeclarative_perimetrage_vide" / "main.py"
)
FLAG_NAME = "PERIMETRAGE_DACD_VIDE"


def _run_main(perimetrage_path: Path, tir_output_path: Path) -> subprocess.CompletedProcess:
    cmd = [
        sys.executable,
        str(MAIN_SCRIPT),
        "--perimetrage-dacd-path",
        str(perimetrage_path),
        "--tir-output-path",
        str(tir_output_path),
    ]
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def _assert_flag(tir_output_path: Path, expected: bool, scenario: str) -> None:
    flag_exists = (tir_output_path / FLAG_NAME).exists()
    if flag_exists != expected:
        raise AssertionError(
            f"[{scenario}] expected flag existence={expected}, got {flag_exists}"
        )


def _prepare_non_empty_parquet_like_dir(perimetrage_path: Path) -> None:
    perimetrage_path.mkdir(parents=True, exist_ok=True)
    (perimetrage_path / "part-00000-0000.snappy.parquet").write_bytes(b"PAR1dummy")


def _prepare_marker_only_dir(perimetrage_path: Path) -> None:
    perimetrage_path.mkdir(parents=True, exist_ok=True)
    (perimetrage_path / "_SUCCESS").touch()


def run() -> None:
    temp_dir = Path(tempfile.mkdtemp(prefix="dsn-perimetrage-"))
    try:
        # 1) Missing folder => empty => flag should exist.
        perim_missing = temp_dir / "missing_perimetrage"
        out_missing = temp_dir / "output_missing"
        result = _run_main(perim_missing, out_missing)
        if result.returncode != 0:
            raise AssertionError(result.stderr or result.stdout)
        _assert_flag(out_missing, expected=True, scenario="missing folder")

        # 2) Existing and empty folder => empty => flag should exist.
        perim_empty_dir = temp_dir / "empty_perimetrage"
        perim_empty_dir.mkdir(parents=True, exist_ok=True)
        out_empty_dir = temp_dir / "output_empty_dir"
        result = _run_main(perim_empty_dir, out_empty_dir)
        if result.returncode != 0:
            raise AssertionError(result.stderr or result.stdout)
        _assert_flag(out_empty_dir, expected=True, scenario="empty folder")

        # 3) Existing non-empty marker-only folder => empty => flag should exist.
        perim_marker_only = temp_dir / "marker_only"
        _prepare_marker_only_dir(perim_marker_only)
        out_marker_only = temp_dir / "output_marker_only"
        result = _run_main(perim_marker_only, out_marker_only)
        if result.returncode != 0:
            raise AssertionError(result.stderr or result.stdout)
        _assert_flag(out_marker_only, expected=True, scenario="marker-only folder")

        # 4) Existing folder with parquet-like file => not empty => no flag.
        perim_non_empty = temp_dir / "non_empty_perimetrage"
        _prepare_non_empty_parquet_like_dir(perim_non_empty)
        out_non_empty = temp_dir / "output_non_empty"
        result = _run_main(perim_non_empty, out_non_empty)
        if result.returncode != 0:
            raise AssertionError(result.stderr or result.stdout)
        _assert_flag(out_non_empty, expected=False, scenario="non-empty folder")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    run()
    print("test_local_perimetrage_vide.py: OK")

