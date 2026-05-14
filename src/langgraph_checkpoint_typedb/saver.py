"""Synchronous `TypeDBSaver` — the primary checkpoint implementation."""

from __future__ import annotations

import threading
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
    get_checkpoint_metadata,
)

from langgraph_checkpoint_typedb import queries
from langgraph_checkpoint_typedb.errors import ConflictError
from langgraph_checkpoint_typedb.schema import apply_schema
from langgraph_checkpoint_typedb.serialization import (
    b64decode,
    b64encode,
    encode_blob_path,
    encode_cp_path,
    encode_write_path,
)

if TYPE_CHECKING:
    from typedb.driver import Driver


def _is_conflict_exception(exc: BaseException) -> bool:
    """Best-effort detection of a TypeDB commit-conflict (mirrors sibling store)."""
    name = type(exc).__name__
    if "TypeDB" not in name:
        return False
    msg = str(exc).lower()
    return any(s in msg for s in ("conflict", "concurrent", "retry", "isolation"))


def _row_value(row: Any, key: str) -> Any:
    """Unwrap a fetched concept-document value (TypeDB returns `{"value": x}` envelopes
    around scalars in some cases)."""
    val = row[key]
    if isinstance(val, dict) and "value" in val and len(val) == 1:
        return val["value"]
    return val


class TypeDBSaver(BaseCheckpointSaver[str]):
    """LangGraph `BaseCheckpointSaver` backed by a TypeDB 3.x database.

    The saver does not own the driver lifecycle — the caller closes the driver.
    Transactions are short-lived (one per top-level operation).
    """

    def __init__(
        self,
        driver: Driver,
        database: str,
        *,
        retry_on_conflict: int = 1,
    ) -> None:
        super().__init__()
        self._driver = driver
        self._db = database
        self._retry_on_conflict = retry_on_conflict
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ setup

    def ensure_database(self) -> None:
        """Create the database if it does not exist. Idempotent."""
        dbs = self._driver.databases
        if not dbs.contains(self._db):
            dbs.create(self._db)

    def ensure_schema(self) -> None:
        """Apply the bundled schema. Safe to call once after `ensure_database`."""
        apply_schema(self._driver, self._db)

    # ------------------------------------------------------------------ writes

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        configurable = config["configurable"]
        thread_id = str(configurable["thread_id"])
        checkpoint_ns = str(configurable.get("checkpoint_ns", ""))
        checkpoint_id = checkpoint["id"]
        parent_checkpoint_id = str(configurable.get("checkpoint_id", "") or "")

        merged_md = get_checkpoint_metadata(config, metadata)
        run_id = str(merged_md.get("run_id", "") or "")

        cp_copy: dict[str, Any] = {**checkpoint, "channel_values": {}}
        cp_type, cp_bytes = self.serde.dumps_typed(cp_copy)
        md_type, md_bytes = self.serde.dumps_typed(dict(merged_md))
        cp_blob = b64encode(cp_bytes)
        md_blob = b64encode(md_bytes)

        blob_inserts: list[str] = []
        for channel, version in new_versions.items():
            if channel not in checkpoint["channel_values"]:
                continue
            value = checkpoint["channel_values"][channel]
            bt, bbytes = self.serde.dumps_typed(value)
            blob_inserts.append(
                queries.delete_blob_by_path(
                    encode_blob_path(thread_id, checkpoint_ns, channel, str(version))
                )
            )
            blob_inserts.append(
                queries.insert_blob(
                    blob_path=encode_blob_path(
                        thread_id, checkpoint_ns, channel, str(version)
                    ),
                    thread_id=thread_id,
                    checkpoint_ns=checkpoint_ns,
                    channel=channel,
                    version=str(version),
                    blob_type=bt,
                    blob_value=b64encode(bbytes),
                )
            )

        cp_path = encode_cp_path(thread_id, checkpoint_ns, checkpoint_id)
        cp_delete = queries.delete_checkpoint_by_path(cp_path)
        cp_insert = queries.insert_checkpoint(
            cp_path=cp_path,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
            parent_checkpoint_id=parent_checkpoint_id,
            cp_type=cp_type,
            cp_blob=cp_blob,
            md_type=md_type,
            md_blob=md_blob,
            run_id=run_id,
        )

        self._run_write([*blob_inserts, cp_delete, cp_insert])

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        configurable = config["configurable"]
        thread_id = str(configurable["thread_id"])
        checkpoint_ns = str(configurable.get("checkpoint_ns", ""))
        checkpoint_id = str(configurable["checkpoint_id"])

        from typedb.driver import TransactionType

        with self._lock:
            with self._driver.transaction(self._db, TransactionType.WRITE) as tx:
                for offset, (channel, value) in enumerate(writes):
                    # Special channels (WRITES_IDX_MAP) have stable indices and are
                    # upserted (match Postgres `ON CONFLICT DO UPDATE`).
                    # Other writes are insert-only and addressed by `(task_id, offset)`.
                    idx = WRITES_IDX_MAP.get(channel, offset)
                    is_special = channel in WRITES_IDX_MAP
                    wt, wbytes = self.serde.dumps_typed(value)
                    wpath = encode_write_path(
                        thread_id, checkpoint_ns, checkpoint_id, task_id, idx
                    )
                    if is_special:
                        tx.query(queries.delete_write_by_path(wpath)).resolve()
                        tx.query(
                            queries.insert_write(
                                write_path=wpath,
                                thread_id=thread_id,
                                checkpoint_ns=checkpoint_ns,
                                checkpoint_id=checkpoint_id,
                                task_id=task_id,
                                task_path=task_path,
                                write_idx=idx,
                                channel=channel,
                                write_type=wt,
                                write_blob=b64encode(wbytes),
                            )
                        ).resolve()
                    else:
                        # Insert-if-absent: check existence first.
                        existing = tx.query(queries.fetch_writes_for_checkpoint(
                            thread_id, checkpoint_ns, checkpoint_id
                        )).resolve()
                        rows = list(existing.as_concept_documents())
                        if any(
                            _row_value(r, "tk") == task_id
                            and int(_row_value(r, "i")) == idx
                            for r in rows
                        ):
                            continue
                        tx.query(
                            queries.insert_write(
                                write_path=wpath,
                                thread_id=thread_id,
                                checkpoint_ns=checkpoint_ns,
                                checkpoint_id=checkpoint_id,
                                task_id=task_id,
                                task_path=task_path,
                                write_idx=idx,
                                channel=channel,
                                write_type=wt,
                                write_blob=b64encode(wbytes),
                            )
                        ).resolve()
                tx.commit()

    # ------------------------------------------------------------------ reads

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        configurable = config["configurable"]
        thread_id = str(configurable["thread_id"])
        checkpoint_ns = str(configurable.get("checkpoint_ns", ""))
        checkpoint_id = get_checkpoint_id(config)

        from typedb.driver import TransactionType

        with self._driver.transaction(self._db, TransactionType.READ) as tx:
            if checkpoint_id is not None:
                cp_path = encode_cp_path(thread_id, checkpoint_ns, checkpoint_id)
                result = tx.query(queries.fetch_checkpoint_by_path(cp_path)).resolve()
                rows = list(result.as_concept_documents())
                if not rows:
                    return None
                row = rows[0]
                cid = str(_row_value(row, "cid"))
            else:
                result = tx.query(
                    queries.fetch_latest_checkpoint(thread_id, checkpoint_ns)
                ).resolve()
                rows = list(result.as_concept_documents())
                if not rows:
                    return None
                # Lexicographic max — LangGraph checkpoint IDs are time-prefixed.
                row = max(rows, key=lambda r: str(_row_value(r, "cid")))
                cid = str(_row_value(row, "cid"))

            cp_type = str(_row_value(row, "ct"))
            cp_blob = str(_row_value(row, "cb"))
            md_type = str(_row_value(row, "mt"))
            md_blob = str(_row_value(row, "mb"))
            pid = str(_row_value(row, "pid"))

            blob_result = tx.query(
                queries.fetch_blobs_for_thread_ns(thread_id, checkpoint_ns)
            ).resolve()
            blob_rows = list(blob_result.as_concept_documents())

            write_result = tx.query(
                queries.fetch_writes_for_checkpoint(thread_id, checkpoint_ns, cid)
            ).resolve()
            write_rows = list(write_result.as_concept_documents())

        checkpoint = self.serde.loads_typed((cp_type, b64decode(cp_blob)))
        metadata = self.serde.loads_typed((md_type, b64decode(md_blob)))

        blobs_by_cv: dict[tuple[str, str], tuple[str, str]] = {}
        for br in blob_rows:
            ch = str(_row_value(br, "ch"))
            v = str(_row_value(br, "v"))
            blobs_by_cv[(ch, v)] = (str(_row_value(br, "bt")), str(_row_value(br, "bv")))

        channel_values: dict[str, Any] = {}
        for channel, version in checkpoint.get("channel_versions", {}).items():
            bv = blobs_by_cv.get((channel, str(version)))
            if bv is not None:
                bt, payload = bv
                channel_values[channel] = self.serde.loads_typed((bt, b64decode(payload)))
        checkpoint["channel_values"] = channel_values

        pending_writes: list[tuple[str, str, Any]] = []
        for wr in write_rows:
            wt = str(_row_value(wr, "wt"))
            wb = str(_row_value(wr, "wb"))
            pending_writes.append(
                (
                    str(_row_value(wr, "tk")),
                    str(_row_value(wr, "ch")),
                    self.serde.loads_typed((wt, b64decode(wb))),
                )
            )

        cur_config: RunnableConfig = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": cid,
            }
        }
        parent_config: RunnableConfig | None = None
        if pid:
            parent_config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": pid,
                }
            }

        return CheckpointTuple(
            config=cur_config,
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending_writes,
        )

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        thread_id: str | None = None
        checkpoint_ns: str | None = None
        if config is not None:
            configurable = config.get("configurable", {})
            if "thread_id" in configurable:
                thread_id = str(configurable["thread_id"])
            if "checkpoint_ns" in configurable:
                checkpoint_ns = str(configurable["checkpoint_ns"])

        before_id: str | None = None
        if before is not None:
            before_id = get_checkpoint_id(before)

        from typedb.driver import TransactionType

        with self._driver.transaction(self._db, TransactionType.READ) as tx:
            result = tx.query(
                queries.list_checkpoints(
                    thread_id=thread_id, checkpoint_ns=checkpoint_ns
                )
            ).resolve()
            rows = list(result.as_concept_documents())

        rows.sort(key=lambda r: str(_row_value(r, "cid")), reverse=True)

        emitted = 0
        for row in rows:
            if limit is not None and emitted >= limit:
                break
            cid = str(_row_value(row, "cid"))
            if before_id is not None and not (cid < before_id):
                continue
            tid = str(_row_value(row, "tid"))
            ns = str(_row_value(row, "ns"))
            tup = self.get_tuple(
                {
                    "configurable": {
                        "thread_id": tid,
                        "checkpoint_ns": ns,
                        "checkpoint_id": cid,
                    }
                }
            )
            if tup is None:
                continue
            if filter and not _matches_metadata_filter(tup.metadata, filter):
                continue
            emitted += 1
            yield tup

    # ------------------------------------------------------------------ delete / copy / prune

    def delete_thread(self, thread_id: str) -> None:
        self._run_write(
            [
                queries.delete_writes_for_thread(thread_id),
                queries.delete_blobs_for_thread(thread_id),
                queries.delete_checkpoints_for_thread(thread_id),
            ]
        )

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        if not run_ids:
            return
        # Fetch checkpoints whose run_id is in run_ids, then delete each by cp_path.
        # Orphan blobs (versions no longer referenced) are not cleaned up in v0.1.
        from typedb.driver import TransactionType

        run_id_set = {str(r) for r in run_ids}
        with self._driver.transaction(self._db, TransactionType.READ) as tx:
            result = tx.query(queries.list_checkpoints(
                thread_id=None, checkpoint_ns=None
            )).resolve()
            rows = list(result.as_concept_documents())

        to_delete: list[tuple[str, str, str]] = []
        for r in rows:
            rid = str(_row_value(r, "rid"))
            if rid in run_id_set:
                to_delete.append(
                    (
                        str(_row_value(r, "tid")),
                        str(_row_value(r, "ns")),
                        str(_row_value(r, "cid")),
                    )
                )
        if not to_delete:
            return
        statements: list[str] = []
        for tid, ns, cid in to_delete:
            statements.append(
                queries.delete_writes_for_checkpoint(tid, ns, cid)
            )
            statements.append(
                queries.delete_checkpoint_by_path(encode_cp_path(tid, ns, cid))
            )
        self._run_write(statements)

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        # Mirror Postgres semantics: collision is an error (handled implicitly by
        # @key uniqueness — re-insert fails at commit).
        from typedb.driver import TransactionType

        with self._driver.transaction(self._db, TransactionType.READ) as tx:
            cp_rows = list(
                tx.query(queries.fetch_checkpoint_ids_for_thread(source_thread_id))
                .resolve()
                .as_concept_documents()
            )

        statements: list[str] = []
        for r in cp_rows:
            ns = str(_row_value(r, "ns"))
            cid = str(_row_value(r, "cid"))
            with self._driver.transaction(self._db, TransactionType.READ) as tx:
                src_row = list(
                    tx.query(
                        queries.fetch_checkpoint_by_path(
                            encode_cp_path(source_thread_id, ns, cid)
                        )
                    )
                    .resolve()
                    .as_concept_documents()
                )
                writes = list(
                    tx.query(
                        queries.fetch_writes_for_checkpoint(source_thread_id, ns, cid)
                    )
                    .resolve()
                    .as_concept_documents()
                )
                blobs = list(
                    tx.query(queries.fetch_blobs_for_thread_ns(source_thread_id, ns))
                    .resolve()
                    .as_concept_documents()
                )
            if not src_row:
                continue
            srow = src_row[0]
            statements.append(
                queries.insert_checkpoint(
                    cp_path=encode_cp_path(target_thread_id, ns, cid),
                    thread_id=target_thread_id,
                    checkpoint_ns=ns,
                    checkpoint_id=cid,
                    parent_checkpoint_id=str(_row_value(srow, "pid")),
                    cp_type=str(_row_value(srow, "ct")),
                    cp_blob=str(_row_value(srow, "cb")),
                    md_type=str(_row_value(srow, "mt")),
                    md_blob=str(_row_value(srow, "mb")),
                    run_id=str(_row_value(srow, "rid")),
                )
            )
            for w in writes:
                idx = int(_row_value(w, "i"))
                tk = str(_row_value(w, "tk"))
                statements.append(
                    queries.insert_write(
                        write_path=encode_write_path(
                            target_thread_id, ns, cid, tk, idx
                        ),
                        thread_id=target_thread_id,
                        checkpoint_ns=ns,
                        checkpoint_id=cid,
                        task_id=tk,
                        task_path=str(_row_value(w, "tp")),
                        write_idx=idx,
                        channel=str(_row_value(w, "ch")),
                        write_type=str(_row_value(w, "wt")),
                        write_blob=str(_row_value(w, "wb")),
                    )
                )
            # Blobs are keyed by (thread_id, ns, channel, version) and are
            # mirrored once per (ns, channel, version) pair. Deduplication is
            # handled by the @key uniqueness — duplicates from the same source
            # blob row across multiple checkpoints all map to the same target
            # blob_path, so we filter here.
            seen_blob_paths: set[str] = set()
            for b in blobs:
                ch = str(_row_value(b, "ch"))
                v = str(_row_value(b, "v"))
                tgt = encode_blob_path(target_thread_id, ns, ch, v)
                if tgt in seen_blob_paths:
                    continue
                seen_blob_paths.add(tgt)
                statements.append(
                    queries.insert_blob(
                        blob_path=tgt,
                        thread_id=target_thread_id,
                        checkpoint_ns=ns,
                        channel=ch,
                        version=v,
                        blob_type=str(_row_value(b, "bt")),
                        blob_value=str(_row_value(b, "bv")),
                    )
                )
        if statements:
            self._run_write(statements)

    def prune(
        self,
        thread_ids: Sequence[str],
        *,
        strategy: str = "keep_latest",
    ) -> None:
        if strategy != "keep_latest":
            raise NotImplementedError(
                f"strategy={strategy!r} not supported in v0.1; only 'keep_latest'"
            )
        for thread_id in thread_ids:
            self._prune_keep_latest(str(thread_id))

    def _prune_keep_latest(self, thread_id: str) -> None:
        from typedb.driver import TransactionType

        with self._driver.transaction(self._db, TransactionType.READ) as tx:
            rows = list(
                tx.query(queries.fetch_checkpoint_ids_for_thread(thread_id))
                .resolve()
                .as_concept_documents()
            )
        # Group by (ns); keep latest cid per ns.
        by_ns: dict[str, list[str]] = {}
        for r in rows:
            ns = str(_row_value(r, "ns"))
            cid = str(_row_value(r, "cid"))
            by_ns.setdefault(ns, []).append(cid)
        statements: list[str] = []
        for ns, cids in by_ns.items():
            cids.sort()
            keep = cids[-1]
            for cid in cids[:-1]:
                statements.append(
                    queries.delete_writes_for_checkpoint(thread_id, ns, cid)
                )
                statements.append(
                    queries.delete_checkpoint_by_path(
                        encode_cp_path(thread_id, ns, cid)
                    )
                )
            del keep
        if statements:
            self._run_write(statements)

    def get_delta_channel_history(self, *, config: RunnableConfig, channels: Sequence[str]) -> Any:
        raise NotImplementedError(
            "get_delta_channel_history is not supported in v0.1"
        )

    # ------------------------------------------------------------------ helpers

    def _run_write(self, statements: list[str]) -> None:
        from typedb.driver import TransactionType

        last_exc: BaseException | None = None
        for attempt in range(self._retry_on_conflict + 1):
            try:
                with self._lock:
                    with self._driver.transaction(self._db, TransactionType.WRITE) as tx:
                        for stmt in statements:
                            tx.query(stmt).resolve()
                        tx.commit()
                return
            except Exception as exc:  # noqa: BLE001
                if _is_conflict_exception(exc) and attempt < self._retry_on_conflict:
                    last_exc = exc
                    continue
                if _is_conflict_exception(exc):
                    raise ConflictError(
                        f"write failed after {attempt + 1} attempts: {exc}"
                    ) from exc
                raise
        if last_exc is not None:  # pragma: no cover - defensive
            raise ConflictError(str(last_exc)) from last_exc


# ---------------------------------------------------------------- module helpers


def _matches_metadata_filter(
    metadata: CheckpointMetadata, filter_: dict[str, Any]
) -> bool:
    """Exact-match top-level filter against metadata. Operator dicts ($gt, …) deferred."""
    for k, v in filter_.items():
        if isinstance(v, dict):
            raise NotImplementedError(
                "operator filters ($gt, $lt, …) are not supported in v0.1"
            )
        if metadata.get(k) != v:  # type: ignore[attr-defined]
            return False
    return True


__all__ = ["TypeDBSaver"]
