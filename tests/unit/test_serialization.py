"""Unit tests for serialization helpers."""

from __future__ import annotations

import pytest

from langgraph_checkpoint_typedb.serialization import (
    NS_SEP,
    b64decode,
    b64encode,
    encode_blob_path,
    encode_cp_path,
    encode_write_path,
    escape_tql_string,
)


def test_encode_cp_path_components():
    p = encode_cp_path("tid", "ns", "cid")
    assert p == f"tid{NS_SEP}ns{NS_SEP}cid"


def test_encode_blob_path_components():
    p = encode_blob_path("tid", "ns", "ch", "v")
    assert p == f"tid{NS_SEP}ns{NS_SEP}ch{NS_SEP}v"


def test_encode_write_path_components():
    p = encode_write_path("tid", "ns", "cid", "task-1", 3)
    assert p == f"tid{NS_SEP}ns{NS_SEP}cid{NS_SEP}task-1{NS_SEP}3"


@pytest.mark.parametrize("payload", [b"", b"hello", bytes(range(256))])
def test_base64_roundtrip(payload):
    assert b64decode(b64encode(payload)) == payload


def test_escape_tql_string_escapes_quotes_and_backslashes():
    assert escape_tql_string('he said "hi"') == 'he said \\"hi\\"'
    assert escape_tql_string("c:\\path") == "c:\\\\path"
