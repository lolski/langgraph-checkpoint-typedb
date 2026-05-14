"""Integration: delete_thread / delete_for_runs / prune."""

from __future__ import annotations

from langgraph.checkpoint.base import empty_checkpoint


def _config(thread_id: str = "t1") -> dict:
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}


def _put(saver, thread_id: str, step: int, run_id: str = "r") -> dict:
    cp = empty_checkpoint()
    cp["channel_versions"] = {"x": str(step)}
    cp["channel_values"] = {"x": step}
    metadata = {"source": "loop", "step": step, "parents": {}, "run_id": run_id}
    return saver.put(_config(thread_id), cp, metadata, {"x": str(step)})


def test_delete_thread_removes_only_target_thread(saver):
    _put(saver, "keep", 0)
    _put(saver, "drop", 0)
    saver.delete_thread("drop")
    assert saver.get_tuple(_config("drop")) is None
    assert saver.get_tuple(_config("keep")) is not None


def test_delete_for_runs_removes_matching_checkpoints(saver):
    _put(saver, "t1", 0, run_id="run-A")
    _put(saver, "t1", 1, run_id="run-B")
    saver.delete_for_runs(["run-A"])
    remaining = list(saver.list(_config("t1")))
    assert all(t.metadata["run_id"] == "run-B" for t in remaining)


def test_prune_keep_latest_retains_only_newest_per_namespace(saver):
    _put(saver, "t1", 0)
    cfg = _put(saver, "t1", 1)
    _put(saver, "t1", 2)
    saver.prune(["t1"])
    remaining = list(saver.list(_config("t1")))
    assert len(remaining) == 1
    # latest is whichever was put last
    assert remaining[0].checkpoint["id"] >= cfg["configurable"]["checkpoint_id"]
