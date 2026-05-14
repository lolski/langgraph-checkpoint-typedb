"""Integration: put → get_tuple round-trip."""

from __future__ import annotations

from langgraph.checkpoint.base import empty_checkpoint


def _config(thread_id: str = "t1", checkpoint_ns: str = "") -> dict:
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}}


def test_put_then_get_tuple_returns_same_checkpoint(saver):
    cp = empty_checkpoint()
    cp["channel_values"] = {"x": 42}
    cp["channel_versions"] = {"x": "1"}
    metadata = {"source": "loop", "step": 0, "parents": {}, "run_id": "r-1"}

    new_cfg = saver.put(_config(), cp, metadata, {"x": "1"})

    tup = saver.get_tuple(new_cfg)
    assert tup is not None
    assert tup.checkpoint["id"] == cp["id"]
    assert tup.checkpoint["channel_values"] == {"x": 42}
    assert tup.metadata["source"] == "loop"
    assert tup.metadata["run_id"] == "r-1"


def test_get_tuple_latest_when_checkpoint_id_absent(saver):
    metadata = {"source": "loop", "step": 0, "parents": {}, "run_id": "r"}
    cp1 = empty_checkpoint()
    cp1["channel_values"] = {"x": 1}
    cp1["channel_versions"] = {"x": "1"}
    saver.put(_config(), cp1, metadata, {"x": "1"})

    cp2 = empty_checkpoint()
    cp2["channel_values"] = {"x": 2}
    cp2["channel_versions"] = {"x": "2"}
    saver.put(_config(), cp2, metadata, {"x": "2"})

    tup = saver.get_tuple(_config())
    assert tup is not None
    assert tup.checkpoint["channel_values"] == {"x": 2}


def test_get_tuple_missing_returns_none(saver):
    assert saver.get_tuple(_config("nope")) is None


def test_parent_config_set_when_checkpoint_has_parent(saver):
    metadata = {"source": "loop", "step": 0, "parents": {}, "run_id": "r"}
    cp1 = empty_checkpoint()
    cp1["channel_versions"] = {}
    cfg1 = saver.put(_config(), cp1, metadata, {})

    cp2 = empty_checkpoint()
    cp2["channel_versions"] = {}
    cfg2 = saver.put(cfg1, cp2, metadata, {})

    tup = saver.get_tuple(cfg2)
    assert tup is not None
    assert tup.parent_config is not None
    assert tup.parent_config["configurable"]["checkpoint_id"] == cp1["id"]
