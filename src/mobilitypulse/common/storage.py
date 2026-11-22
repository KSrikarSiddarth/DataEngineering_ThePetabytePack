"""Storage layer: one code path for a local folder and for an S3-compatible bucket (MinIO) addressed as s3a://.

Spark itself reads and writes ``s3a://`` through hadoop-aws on the executors. This module covers everything the
*driver process* does outside Spark (existence checks, small text files, pandas/pyarrow reads and writes, deleting
a tree, uploading the downloaded raw files), using pyarrow's S3 filesystem, so no Spark JVM is needed for them.

``RemotePath`` mimics the few ``pathlib.Path`` methods the project uses, so code like
``cfg.path("gold") / "forecasts"`` and ``p.exists()`` works unchanged for both kinds of location.
"""
from __future__ import annotations

import fnmatch
import os
import shutil
from pathlib import Path
from typing import Union
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

REMOTE_SCHEMES = ("s3a://", "s3://", "s3n://")

_S3_SETTINGS: dict[str, str | None] = {"endpoint": None, "access_key": None, "secret_key": None, "region": None}
_FS_CACHE: dict[tuple, pafs.FileSystem] = {}


def is_uri(value: object) -> bool:
    return isinstance(value, str) and value.lower().startswith(REMOTE_SCHEMES)


def configure_s3(endpoint: str | None = None, access_key: str | None = None, secret_key: str | None = None,
                 region: str | None = None) -> None:
    """Set the S3 endpoint and keys (None keeps the current value; unset values fall back to S3_* env vars)."""
    for key, value in (("endpoint", endpoint), ("access_key", access_key), ("secret_key", secret_key),
                       ("region", region)):
        if value:
            _S3_SETTINGS[key] = value
    _FS_CACHE.clear()


def s3_settings() -> dict[str, str | None]:
    return {
        "endpoint": _S3_SETTINGS["endpoint"] or os.environ.get("S3_ENDPOINT"),
        "access_key": _S3_SETTINGS["access_key"] or os.environ.get("S3_ACCESS_KEY"),
        "secret_key": _S3_SETTINGS["secret_key"] or os.environ.get("S3_SECRET_KEY"),
        "region": _S3_SETTINGS["region"] or os.environ.get("S3_REGION") or "us-east-1",
    }


def s3_filesystem() -> pafs.FileSystem:
    cfg = s3_settings()
    if not cfg["endpoint"]:
        raise RuntimeError("s3a:// path used but no S3 endpoint is configured: set S3_ENDPOINT, S3_ACCESS_KEY and "
                           "S3_SECRET_KEY (see setupF.env.example) or the storage section of config.yaml")
    key = tuple(cfg.values())
    if key not in _FS_CACHE:
        url = urlparse(cfg["endpoint"] if "://" in cfg["endpoint"] else "http://" + cfg["endpoint"])
        _FS_CACHE[key] = pafs.S3FileSystem(
            access_key=cfg["access_key"], secret_key=cfg["secret_key"], region=cfg["region"],
            endpoint_override=url.netloc, scheme=url.scheme or "http", allow_bucket_creation=True)
    return _FS_CACHE[key]


class RemotePath:
    """An s3a://bucket/key location. Immutable; ``/`` appends a segment (like pathlib)."""

    __slots__ = ("_uri",)

    def __init__(self, uri: Union[str, "RemotePath"]):
        text = str(uri)
        if not is_uri(text):
            raise ValueError(f"not an object-store URI: {text!r}")
        rest = "/".join(seg for seg in text.split("://", 1)[1].split("/") if seg)
        self._uri = "s3a://" + rest

    # ---- pathlib-like surface
    def __str__(self) -> str:
        return self._uri

    def __repr__(self) -> str:
        return f"RemotePath({self._uri!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RemotePath) and other._uri == self._uri

    def __hash__(self) -> int:
        return hash(self._uri)

    def __truediv__(self, other: object) -> "RemotePath":
        return RemotePath(f"{self._uri}/{str(other).replace(os.sep, '/').strip('/')}")

    @property
    def name(self) -> str:
        return self._uri.rsplit("/", 1)[-1]

    @property
    def parent(self) -> "RemotePath":
        head = self._uri.rsplit("/", 1)[0]
        return self if head == "s3a:/" else RemotePath(head)  # the bucket is the root

    @property
    def key(self) -> str:
        """Path as pyarrow wants it: ``bucket/key``."""
        return self._uri[len("s3a://"):]

    def exists(self) -> bool:
        return s3_filesystem().get_file_info(self.key).type != pafs.FileType.NotFound

    def mkdir(self, parents: bool = False, exist_ok: bool = False) -> None:
        """Object stores have no folders: prefixes appear when something is written under them."""
        return None

    def read_text(self, encoding: str = "utf-8") -> str:
        with s3_filesystem().open_input_stream(self.key) as f:
            return f.read().decode(encoding)

    def write_text(self, data: str, encoding: str = "utf-8") -> int:
        with s3_filesystem().open_output_stream(self.key) as f:
            f.write(data.encode(encoding))
        return len(data)

    def glob(self, pattern: str) -> list["RemotePath"]:
        """Direct children whose name matches ``pattern`` (no recursion; enough for ``month=*``)."""
        fs = s3_filesystem()
        infos = fs.get_file_info(pafs.FileSelector(self.key, allow_not_found=True, recursive=False))
        return sorted((self / i.base_name for i in infos if fnmatch.fnmatch(i.base_name, pattern)),
                      key=str)


AnyPath = Union[Path, RemotePath]


def make_path(value: str) -> AnyPath:
    """URI -> RemotePath, anything else -> local Path (``~`` expanded)."""
    return RemotePath(value) if is_uri(value) else Path(os.path.expanduser(value))


def is_remote(path: object) -> bool:
    return isinstance(path, RemotePath)


# ---------------------------------------------------------------- helpers working on both kinds of path
def exists(path: AnyPath) -> bool:
    return path.exists()


def rmtree(path: AnyPath) -> None:
    """Delete a file or a whole tree; nothing happens when it is missing."""
    if isinstance(path, RemotePath):
        fs = s3_filesystem()
        info = fs.get_file_info(path.key)
        if info.type == pafs.FileType.Directory:
            fs.delete_dir(path.key)
        elif info.type == pafs.FileType.File:
            fs.delete_file(path.key)
    elif Path(path).is_dir():
        shutil.rmtree(path)
    elif Path(path).exists():
        Path(path).unlink()


def arrow_location(path: AnyPath) -> tuple[pafs.FileSystem | None, str]:
    """(filesystem, path) for pyarrow.dataset / pyarrow.parquet; filesystem is None for local paths."""
    if isinstance(path, RemotePath):
        return s3_filesystem(), path.key
    return None, str(path)


def read_parquet_pandas(path: AnyPath):
    fs, p = arrow_location(path)
    return pq.read_table(p, filesystem=fs).to_pandas()


def write_parquet_pandas(df, path: AnyPath) -> None:
    fs, p = arrow_location(path)
    if fs is None:
        Path(p).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), p, filesystem=fs)


def upload_tree(src: Path, dst: AnyPath) -> int:
    """Copy every file under local ``src`` to ``dst`` (same relative paths); files whose size already matches are
    skipped, so repeated calls only move what changed. Returns the number of files uploaded."""
    src = Path(src)
    if not src.exists():
        return 0
    if not isinstance(dst, RemotePath):
        if src.resolve() == Path(dst).resolve():
            return 0
        raise ValueError("upload_tree needs a remote destination")
    fs = s3_filesystem()
    uploaded = 0
    for f in sorted(p for p in src.rglob("*") if p.is_file() and not p.name.endswith(".part")):
        target = (dst / f.relative_to(src).as_posix()).key
        info = fs.get_file_info(target)
        if info.type == pafs.FileType.File and info.size == f.stat().st_size:
            continue
        pafs.copy_files(str(f), target, destination_filesystem=fs)
        uploaded += 1
    return uploaded
