"""Key encoding, blob base64, TypeQL escape helpers.

LangGraph's `JsonPlusSerializer.dumps_typed` returns `(type, bytes)`. TypeDB 3.x
attributes have no native bytes type, so blobs are stored as base64-encoded
strings. Composite keys (Postgres-style multi-column PKs) are encoded into a
single string attribute and enforced with `@key`.
"""

from __future__ import annotations

import base64
import re

NS_SEP = "\x1f"
"""Separator between components of composite keys. Forbidden in valid LangGraph
thread/namespace/checkpoint/task identifiers (control character), so collisions
are impossible."""


def encode_cp_path(thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> str:
    return f"{thread_id}{NS_SEP}{checkpoint_ns}{NS_SEP}{checkpoint_id}"


def encode_blob_path(
    thread_id: str, checkpoint_ns: str, channel: str, version: str
) -> str:
    return f"{thread_id}{NS_SEP}{checkpoint_ns}{NS_SEP}{channel}{NS_SEP}{version}"


def encode_write_path(
    thread_id: str,
    checkpoint_ns: str,
    checkpoint_id: str,
    task_id: str,
    idx: int,
) -> str:
    return (
        f"{thread_id}{NS_SEP}{checkpoint_ns}{NS_SEP}"
        f"{checkpoint_id}{NS_SEP}{task_id}{NS_SEP}{idx}"
    )


def b64encode(blob: bytes) -> str:
    return base64.b64encode(blob).decode("ascii")


def b64decode(payload: str) -> bytes:
    return base64.b64decode(payload.encode("ascii"))


_TQL_STRING_ESCAPE_RE = re.compile(r'([\\"])')


def escape_tql_string(value: str) -> str:
    """Escape a Python string for inclusion as a TypeQL double-quoted literal.

    Backslashes and double quotes are escaped. The Unit Separator used for
    composite-key encoding is passed through unchanged — it is not special to
    TypeQL.
    """
    return _TQL_STRING_ESCAPE_RE.sub(r"\\\1", value)
