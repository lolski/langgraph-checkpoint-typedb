"""Integration: parallel put on distinct threads, all succeed."""

from __future__ import annotations

import threading

from langgraph.checkpoint.base import empty_checkpoint


def _put(saver, thread_id: str) -> None:
    cp = empty_checkpoint()
    cp["channel_versions"] = {}
    saver.put(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        cp,
        {"source": "loop", "step": 0, "parents": {}, "run_id": "r"},
        {},
    )


def test_parallel_distinct_thread_writes_all_commit(saver):
    threads = [
        threading.Thread(target=_put, args=(saver, f"t-{i}")) for i in range(10)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for i in range(10):
        tup = saver.get_tuple(
            {"configurable": {"thread_id": f"t-{i}", "checkpoint_ns": ""}}
        )
        assert tup is not None
