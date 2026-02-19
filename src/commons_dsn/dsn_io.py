"""Helpers for reading DSN data sources."""

from __future__ import annotations

from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

_IGNORED_FILE_NAMES = {"_SUCCESS", "_metadata", "_common_metadata"}


def _strip_uri_if_needed(path: str) -> str:
    """Return a filesystem-compatible path when a URI is provided."""
    parsed = urlparse(path)
    if not parsed.scheme:
        return path
    return parsed.path or "/"


def _resolve_filesystem(
    path: str, filesystem: pa_fs.FileSystem | None
) -> tuple[pa_fs.FileSystem, str]:
    """Resolve the effective filesystem and normalized path."""
    if filesystem is not None:
        return filesystem, _strip_uri_if_needed(path)
    if "://" in path:
        return pa_fs.FileSystem.from_uri(path)
    return pa_fs.LocalFileSystem(), path


def _iter_candidate_files(filesystem: pa_fs.FileSystem, path: str) -> list[str]:
    """Return candidate files to inspect for parquet rows."""
    file_info = filesystem.get_file_info(path)

    if file_info.type == pa_fs.FileType.NotFound:
        return []
    if file_info.type == pa_fs.FileType.File:
        return [path]
    if file_info.type != pa_fs.FileType.Directory:
        return []

    selector = pa_fs.FileSelector(path, recursive=True, allow_not_found=True)
    nested_file_infos = filesystem.get_file_info(selector)

    files: list[str] = []
    for nested_file in nested_file_infos:
        if nested_file.type != pa_fs.FileType.File:
            continue

        base_name = nested_file.path.rsplit("/", maxsplit=1)[-1]
        if base_name.startswith(".") or base_name in _IGNORED_FILE_NAMES:
            continue
        files.append(nested_file.path)
    return files


def check_parquet_is_empty(
    path: str, filesystem: pa_fs.FileSystem | None = None
) -> bool:
    """
    Return True when a parquet perimeter is empty.

    The perimeter is considered empty when:
    - the path does not exist,
    - the directory exists but contains no readable parquet file,
    - parquet files exist but all contain zero row.

    This helper works with local FS (default) and remote FS such as HDFS.
    """
    resolved_fs, resolved_path = _resolve_filesystem(path=path, filesystem=filesystem)
    candidate_files = _iter_candidate_files(filesystem=resolved_fs, path=resolved_path)

    if not candidate_files:
        return True

    total_rows = 0
    parquet_found = False

    for file_path in candidate_files:
        try:
            metadata = pq.read_metadata(file_path, filesystem=resolved_fs)
        except pa.ArrowInvalid:
            # Not a parquet file: keep scanning other files in the perimeter.
            continue

        parquet_found = True
        total_rows += metadata.num_rows
        if total_rows > 0:
            return False

    return not parquet_found or total_rows == 0
