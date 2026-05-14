"""Integration: put_writes round-trip through get_tuple.pending_writes."""

from __future__ import annotations

from langgraph.checkpoint.base import empty_checkpoint


def _config(thread_id: str = "t1") -> dict:
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}


def test_writes_round_trip(saver):
    metadata = {"source": "loop", "step": 0, "parents": {}, "run_id": "r"}
    cp = empty_checkpoint()
    cp["channel_versions"] = {}
    cfg = saver.put(_config(), cp, metadata, {})

    saver.put_writes(cfg, [("ch_a", "value-a"), ("ch_b", 99)], task_id="task-1")

    tup = saver.get_tuple(cfg)
    assert tup is not None
    assert tup.pending_writes is not None
    by_channel = {(task_id, ch): val for task_id, ch, val in tup.pending_writes}
    assert by_channel[("task-1", "ch_a")] == "value-a"
    assert by_channel[("task-1", "ch_b")] == 99
