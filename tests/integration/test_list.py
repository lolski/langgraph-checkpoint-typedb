"""Integration: list ordering, before, limit, filter."""

from __future__ import annotations

from langgraph.checkpoint.base import empty_checkpoint


def _config(thread_id: str = "t1") -> dict:
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}


def _put_n(saver, n: int, thread_id: str = "t1") -> list[dict]:
    cfgs: list[dict] = []
    cfg = _config(thread_id)
    for i in range(n):
        cp = empty_checkpoint()
        cp["channel_versions"] = {}
        metadata = {"source": "loop", "step": i, "parents": {}, "run_id": "r"}
        cfg = saver.put(cfg, cp, metadata, {})
        cfgs.append(cfg)
    return cfgs


def test_list_returns_descending_by_id(saver):
    cfgs = _put_n(saver, 3)
    ids = [c["configurable"]["checkpoint_id"] for c in cfgs]
    listed = list(saver.list(_config()))
    listed_ids = [t.checkpoint["id"] for t in listed]
    assert listed_ids == sorted(ids, reverse=True)


def test_list_respects_limit(saver):
    _put_n(saver, 5)
    assert len(list(saver.list(_config(), limit=2))) == 2


def test_list_before_filters_strictly_less_than(saver):
    cfgs = _put_n(saver, 4)
    middle = cfgs[2]
    listed = list(saver.list(_config(), before=middle))
    for t in listed:
        assert t.checkpoint["id"] < middle["configurable"]["checkpoint_id"]


def test_list_filter_by_metadata_field(saver):
    cfg = _config()
    cp_a = empty_checkpoint()
    cp_a["channel_versions"] = {}
    saver.put(cfg, cp_a, {"source": "input", "step": -1, "parents": {}, "run_id": "r"}, {})
    cp_b = empty_checkpoint()
    cp_b["channel_versions"] = {}
    saver.put(cfg, cp_b, {"source": "loop", "step": 0, "parents": {}, "run_id": "r"}, {})

    only_loop = list(saver.list(_config(), filter={"source": "loop"}))
    assert all(t.metadata["source"] == "loop" for t in only_loop)
    assert len(only_loop) == 1
