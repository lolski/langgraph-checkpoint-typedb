"""Integration: copy_thread duplicates source rows under target thread_id."""

from __future__ import annotations

from langgraph.checkpoint.base import empty_checkpoint


def _config(thread_id: str) -> dict:
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}


def test_copy_thread_mirrors_checkpoints_writes_and_blobs(saver):
    src_cfg = _config("src")
    cp = empty_checkpoint()
    cp["channel_values"] = {"x": 1}
    cp["channel_versions"] = {"x": "1"}
    metadata = {"source": "loop", "step": 0, "parents": {}, "run_id": "r"}
    placed = saver.put(src_cfg, cp, metadata, {"x": "1"})
    saver.put_writes(placed, [("ch_a", "v")], task_id="task-1")

    saver.copy_thread("src", "dst")

    dst_tup = saver.get_tuple(_config("dst"))
    assert dst_tup is not None
    assert dst_tup.checkpoint["channel_values"] == {"x": 1}
    assert dst_tup.pending_writes is not None
    assert any(ch == "ch_a" for _, ch, _ in dst_tup.pending_writes)

    # Source untouched.
    src_tup = saver.get_tuple(_config("src"))
    assert src_tup is not None
    assert src_tup.checkpoint["channel_values"] == {"x": 1}
