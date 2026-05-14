"""Smoke test for `AsyncTypeDBSaver` thread-offload wrapper."""

from __future__ import annotations

import pytest
from langgraph.checkpoint.base import empty_checkpoint

from langgraph_checkpoint_typedb import AsyncTypeDBSaver


@pytest.mark.asyncio
async def test_async_put_get_alist(saver):
    asaver = AsyncTypeDBSaver(saver)
    cfg = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}

    cp = empty_checkpoint()
    cp["channel_values"] = {"x": "hi"}
    cp["channel_versions"] = {"x": "1"}
    metadata = {"source": "loop", "step": 0, "parents": {}, "run_id": "r"}
    new_cfg = await asaver.aput(cfg, cp, metadata, {"x": "1"})

    tup = await asaver.aget_tuple(new_cfg)
    assert tup is not None
    assert tup.checkpoint["channel_values"] == {"x": "hi"}

    collected = [t async for t in asaver.alist(cfg)]
    assert len(collected) == 1
