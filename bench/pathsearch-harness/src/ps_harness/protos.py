"""Lazy protobuf/gRPC stub access for the pathsearch harness.

The three ``.proto`` files (``common``, ``search``, ``path_search``) are vendored
under ``proto/bitmagnet/`` so the harness is self-contained. Python stubs are
generated on first use into ``_generated/`` via ``grpc_tools.protoc`` and cached
there; delete that dir (or run ``ps-harness gen --force``) to regenerate.

Generated modules import each other as ``from bitmagnet import search_pb2`` (the
proto package path is ``bitmagnet/...``), so ``_generated/`` is placed on
``sys.path`` and a ``bitmagnet/`` package dir holds the outputs.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path
from types import ModuleType

# proto/ and _generated/ live relative to the project root (two levels up from
# this file: src/ps_harness/protos.py -> project root).
_PKG_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _PKG_DIR.parent.parent
_PROTO_DIR = _PROJECT_ROOT / "proto"
_GEN_DIR = _PKG_DIR / "_generated"

_PROTO_FILES = (
    "bitmagnet/common.proto",
    "bitmagnet/search.proto",
    "bitmagnet/path_search.proto",
)

# Sentinel that proves codegen finished (the grpc service stub for path_search).
_SENTINEL = _GEN_DIR / "bitmagnet" / "path_search_pb2_grpc.py"


def generate(force: bool = False) -> None:
    """Generate Python stubs from the vendored protos into ``_generated/``.

    Idempotent: skips if the sentinel already exists unless ``force`` is set.
    """
    if _SENTINEL.exists() and not force:
        return
    if not _PROTO_DIR.is_dir():
        raise FileNotFoundError(f"vendored proto dir missing: {_PROTO_DIR}")
    _GEN_DIR.mkdir(parents=True, exist_ok=True)

    # Imported lazily so a plain `--help` does not require grpcio-tools.
    from grpc_tools import protoc  # noqa: PLC0415

    args = [
        "protoc",
        f"-I{_PROTO_DIR}",
        f"--python_out={_GEN_DIR}",
        f"--grpc_python_out={_GEN_DIR}",
        *(_PROTO_FILES),
    ]
    rc = protoc.main(args)
    if rc != 0:
        raise RuntimeError(f"protoc failed (exit {rc}) for {args}")

    # protoc does not emit package __init__.py files; create one so the
    # `bitmagnet` package imports cleanly on every Python.
    init = _GEN_DIR / "bitmagnet" / "__init__.py"
    if not init.exists():
        init.write_text("")
    if not _SENTINEL.exists():
        raise RuntimeError("protoc reported success but stubs are missing")


def _ensure_on_path() -> None:
    gen = str(_GEN_DIR)
    if gen not in sys.path:
        sys.path.insert(0, gen)


def load() -> tuple[ModuleType, ModuleType, ModuleType]:
    """Return ``(path_search_pb2, path_search_pb2_grpc, search_pb2)``.

    Generates the stubs on first call. ``search_pb2`` is returned because the
    request's ``SortBy`` and the ``HealthCheckRequest`` live there.
    """
    generate(force=False)
    _ensure_on_path()
    search_pb2 = importlib.import_module("bitmagnet.search_pb2")
    ps_pb2 = importlib.import_module("bitmagnet.path_search_pb2")
    ps_grpc = importlib.import_module("bitmagnet.path_search_pb2_grpc")
    return ps_pb2, ps_grpc, search_pb2
