"""Unit tests for TypeQL query builders.

Asserts presence of key tokens rather than exact string match, so harmless
formatting changes don't break the suite.
"""

from __future__ import annotations

from langgraph_checkpoint_typedb import queries
from langgraph_checkpoint_typedb.serialization import encode_cp_path


def test_fetch_checkpoint_by_path_uses_cp_path_and_fetches_all_fields():
    q = queries.fetch_checkpoint_by_path(encode_cp_path("tid", "ns", "cid"))
    assert "isa checkpoint" in q
    assert "cp_path" in q
    for var in ("$tid", "$ns", "$cid", "$pid", "$ct", "$cb", "$mt", "$mb", "$rid"):
        assert var in q


def test_list_checkpoints_with_filters():
    q = queries.list_checkpoints(thread_id="t1", checkpoint_ns="ns1")
    assert 'has thread_id "t1"' in q
    assert 'has checkpoint_ns "ns1"' in q


def test_list_checkpoints_without_filters():
    q = queries.list_checkpoints(thread_id=None, checkpoint_ns=None)
    assert "isa checkpoint" in q
    assert 'has thread_id "' not in q


def test_insert_checkpoint_emits_all_attributes():
    q = queries.insert_checkpoint(
        cp_path="p",
        thread_id="tid",
        checkpoint_ns="ns",
        checkpoint_id="cid",
        parent_checkpoint_id="",
        cp_type="json",
        cp_blob="AAAA",
        md_type="json",
        md_blob="BBBB",
        run_id="r1",
    )
    assert q.startswith("insert")
    for s in ("cp_path", "thread_id", "checkpoint_ns", "checkpoint_id",
              "parent_checkpoint_id", "cp_type", "cp_blob", "md_type",
              "md_blob", "run_id"):
        assert s in q


def test_insert_write_quotes_idx_as_integer_literal():
    q = queries.insert_write(
        write_path="p",
        thread_id="tid",
        checkpoint_ns="ns",
        checkpoint_id="cid",
        task_id="t",
        task_path="",
        write_idx=7,
        channel="ch",
        write_type="json",
        write_blob="X",
    )
    assert "has write_idx 7" in q
