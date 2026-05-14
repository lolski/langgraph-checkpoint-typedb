"""Async wrapper around `TypeDBSaver`.

The TypeDB Python driver is synchronous-only. `AsyncTypeDBSaver` offloads each
operation to a thread via `asyncio.to_thread`. The driver does I/O over gRPC
(releases the GIL) so this gives real concurrency.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)

from langgraph_checkpoint_typedb.saver import TypeDBSaver


class AsyncTypeDBSaver(BaseCheckpointSaver[str]):
    """Thread-offload async wrapper around a sync `TypeDBSaver`."""

    def __init__(self, sync_saver: TypeDBSaver) -> None:
        super().__init__()
        self._s = sync_saver

    # ---- sync methods delegate directly (LangGraph may call either flavor)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return self._s.put(config, checkpoint, metadata, new_versions)

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        return self._s.put_writes(config, writes, task_id, task_path)

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return self._s.get_tuple(config)

    def list(self, config, *, filter=None, before=None, limit=None):  # type: ignore[override]
        return self._s.list(config, filter=filter, before=before, limit=limit)

    def delete_thread(self, thread_id: str) -> None:
        return self._s.delete_thread(thread_id)

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        return self._s.delete_for_runs(run_ids)

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        return self._s.copy_thread(source_thread_id, target_thread_id)

    def prune(self, thread_ids: Sequence[str], *, strategy: str = "keep_latest") -> None:
        return self._s.prune(thread_ids, strategy=strategy)

    # ---- async equivalents offload to a worker thread

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return await asyncio.to_thread(
            self._s.put, config, checkpoint, metadata, new_versions
        )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await asyncio.to_thread(
            self._s.put_writes, config, writes, task_id, task_path
        )

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await asyncio.to_thread(self._s.get_tuple, config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        # Materialize the sync iterator inside the worker thread, then yield.
        tuples = await asyncio.to_thread(
            lambda: list(self._s.list(config, filter=filter, before=before, limit=limit))
        )
        for t in tuples:
            yield t

    async def adelete_thread(self, thread_id: str) -> None:
        await asyncio.to_thread(self._s.delete_thread, thread_id)

    async def adelete_for_runs(self, run_ids: Sequence[str]) -> None:
        await asyncio.to_thread(self._s.delete_for_runs, run_ids)

    async def acopy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        await asyncio.to_thread(
            self._s.copy_thread, source_thread_id, target_thread_id
        )

    async def aprune(
        self, thread_ids: Sequence[str], *, strategy: str = "keep_latest"
    ) -> None:
        await asyncio.to_thread(self._s.prune, thread_ids, strategy=strategy)


__all__ = ["AsyncTypeDBSaver"]
