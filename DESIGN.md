# langgraph-checkpoint-typedb — Design

A LangGraph `BaseCheckpointSaver` implementation backed by TypeDB 3.x.

Companion to `langgraph-store-typedb` (sibling repo). Conventions, build setup, and connection model mirror that project unless explicitly noted.

## Constraints (proposed)

- **Python only** (no TS).
- **TypeDB 3.x only** (no 2.x). PyPI package: `typedb-driver` (≥ 3.8).
- **Opaque-blob storage**: checkpoint state and intermediate writes are serialized via LangGraph's `JsonPlusSerializer` and stored as base64-encoded strings on simple entity types. No typed schema for checkpoint contents in v0.1.
- **Composite keys encoded into one string attribute** (same trick as `langgraph-store-typedb`), since TypeDB `@key` is per-attribute.
- **Build system: Poetry** (`poetry-core` backend) — same as the sibling project.
- **No vector / semantic search** in v0.1. Checkpoints are not searchable; only listable by thread + namespace.

## Research findings (LangGraph checkpoint API)

Confirmed against `libs/checkpoint/langgraph/checkpoint/base/__init__.py` and the Postgres/SQLite reference savers in `libs/checkpoint-postgres` / `libs/checkpoint-sqlite`.

### Required `BaseCheckpointSaver` methods (sync + `a*` async twins)

```python
def put(config, checkpoint, metadata, new_versions) -> RunnableConfig
def put_writes(config, writes, task_id, task_path="") -> None
def get_tuple(config) -> CheckpointTuple | None
def list(config, *, filter=None, before=None, limit=None) -> Iterator[CheckpointTuple]
def delete_thread(thread_id) -> None
def delete_for_runs(run_ids) -> None
def copy_thread(source_thread_id, target_thread_id) -> None
def prune(thread_ids, *, strategy="keep_latest") -> None
def get_delta_channel_history(*, config, channels) -> Mapping[str, DeltaChannelHistory]
def get_next_version(current, channel) -> V
```

Also: `serde: SerializerProtocol` attribute (default `JsonPlusSerializer`), `config_specs` property, and `with_allowlist(extra_allowlist)`.

### Key data shapes

- **`Checkpoint`** (TypedDict): `v`, `id`, `ts`, `channel_values`, `channel_versions`, `versions_seen`, `updated_channels`.
- **`CheckpointMetadata`** (TypedDict): `source` (`"input"|"loop"|"update"|"fork"`), `step`, `parents`, `run_id`, `counters_since_delta_snapshot`.
- **`CheckpointTuple`** (NamedTuple): `config`, `checkpoint`, `metadata`, `parent_config`, `pending_writes`.
- **`PendingWrite`**: `tuple[str, str, Any]` — `(channel, task_id, value)`.

### `RunnableConfig` carries

```
config["configurable"]["thread_id"]      # required for all ops
config["configurable"]["checkpoint_ns"]  # default ""
config["configurable"]["checkpoint_id"]  # optional — None means "latest"
```

### Postgres reference schema (used as template)

Three tables — verbatim DDL from `checkpoint-postgres`:

```sql
CREATE TABLE checkpoints (
    thread_id TEXT NOT NULL,
    checkpoint_ns TEXT NOT NULL DEFAULT '',
    checkpoint_id TEXT NOT NULL,
    parent_checkpoint_id TEXT,
    type TEXT,
    checkpoint JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
);

CREATE TABLE checkpoint_blobs (
    thread_id TEXT NOT NULL,
    checkpoint_ns TEXT NOT NULL DEFAULT '',
    channel TEXT NOT NULL,
    version TEXT NOT NULL,
    type TEXT NOT NULL,
    blob BYTEA,
    PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
);

CREATE TABLE checkpoint_writes (
    thread_id TEXT NOT NULL,
    checkpoint_ns TEXT NOT NULL DEFAULT '',
    checkpoint_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    idx INTEGER NOT NULL,
    channel TEXT NOT NULL,
    type TEXT,
    blob BYTEA NOT NULL,
    task_path TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
);
```

Channel **metadata** (versions, seen) lives inside `checkpoint` JSONB. The **actual binary values** for each channel are split out into `checkpoint_blobs`, keyed by `(thread_id, checkpoint_ns, channel, version)`. This lets multiple checkpoints in the same thread share unchanged channel values via reference, instead of re-serializing them.

### TypeDB 3.x driver recap (from sibling design)

- Sync-only Python driver. `TypeDB.driver(addr, Credentials(...), DriverOptions(...))`.
- `driver.transaction(db, TransactionType.{READ,WRITE,SCHEMA})`, `tx.query("...").resolve()`, `tx.commit()`.
- Thread-safe driver, snapshot isolation, optimistic-concurrency commit conflicts must retry.
- Attribute value types: `boolean`, `string`, `integer`, `decimal`, `double`, `date`, `datetime`, `datetime-tz`, `duration`. **No native bytes** — binary blobs base64-encoded as strings.

---

## A. Repo layout

`src/` layout, single top-level package. Mirrors `langgraph-store-typedb`.

```
langgraph-checkpoint-typedb/
├── pyproject.toml
├── README.md
├── LICENSE                          # MIT
├── DESIGN.md                        # this file
├── .gitignore
├── .python-version                  # 3.11
├── src/
│   └── langgraph_checkpoint_typedb/
│       ├── __init__.py              # re-exports TypeDBSaver, AsyncTypeDBSaver
│       ├── saver.py                 # TypeDBSaver (sync)
│       ├── async_saver.py           # AsyncTypeDBSaver (asyncio.to_thread wrapper)
│       ├── schema.py                # apply_schema(driver, db_name)
│       ├── queries.py               # TypeQL builders (pure)
│       ├── serialization.py         # key encoding, blob base64, serde shim
│       ├── errors.py                # TypeDBSaverError, ConflictError
│       ├── _types.py                # internal aliases
│       └── resources/
│           └── schema.tql
├── tests/
│   ├── conftest.py                  # testcontainers + driver fixture (lifted from sibling)
│   ├── unit/
│   │   ├── test_serialization.py
│   │   └── test_query_builders.py
│   └── integration/
│       ├── test_put_get_tuple.py
│       ├── test_list.py
│       ├── test_put_writes.py
│       ├── test_delete_thread.py
│       ├── test_copy_thread.py
│       ├── test_async.py
│       └── test_concurrency.py
└── examples/
    └── hello_checkpoint.py          # LangGraph agent with TypeDB-backed checkpointing
```

---

## B. Packaging (Poetry)

Mirrors `langgraph-store-typedb/pyproject.toml`. Differences:

- Name: `langgraph-checkpoint-typedb`.
- Dependency on `langgraph-checkpoint` (the standalone checkpoint package, not `langgraph` core) so we install the minimal surface — Postgres/SQLite reference savers follow this convention. Confirm published name during implementation (likely `langgraph-checkpoint`).

```toml
[project]
name = "langgraph-checkpoint-typedb"
version = "0.1.0"
description = "LangGraph BaseCheckpointSaver implementation backed by TypeDB 3.x"
authors = [{name = "Ganeshwara Hananda", email = "ganesh@typedb.com"}]
readme = "README.md"
license = "MIT"
requires-python = ">=3.11,<3.13"
dependencies = [
    "langgraph-checkpoint (>=2.0,<3.0)",   # verify exact published name+range
    "typedb-driver (>=3.8,<4.0.0)"
]
```

`[tool.poetry.group.dev.dependencies]`, `[tool.pytest.ini_options]`, `[tool.ruff]`, `[tool.mypy]` are identical to the sibling repo.

**User-facing API:**

```python
from langgraph_checkpoint_typedb import TypeDBSaver
from typedb.driver import TypeDB, Credentials, DriverOptions, DriverTlsConfig

driver = TypeDB.driver(
    "localhost:1729",
    Credentials("admin", "password"),
    DriverOptions(DriverTlsConfig.disabled()),
)
saver = TypeDBSaver(driver, database="langgraph_checkpoints")
saver.ensure_database()
saver.ensure_schema()

graph = builder.compile(checkpointer=saver)
graph.invoke({"input": "hi"}, config={"configurable": {"thread_id": "u-1"}})
```

---

## C. TypeDB schema (opaque-blob)

`src/langgraph_checkpoint_typedb/resources/schema.tql`:

```
define
  entity checkpoint,
    owns cp_path @key,             # encoded (thread_id, ns, checkpoint_id)
    owns thread_id,
    owns checkpoint_ns,
    owns checkpoint_id,
    owns parent_checkpoint_id,
    owns cp_type,
    owns cp_blob,                  # base64(serde.dumps(checkpoint))
    owns md_blob,                  # base64(serde.dumps(metadata))
    owns run_id;                   # denormalized from metadata.run_id for delete_for_runs

  entity checkpoint_blob,
    owns blob_path @key,           # encoded (thread_id, ns, channel, version)
    owns thread_id,
    owns checkpoint_ns,
    owns channel,
    owns version,
    owns blob_type,
    owns blob_value;               # base64(bytes), nullable

  entity checkpoint_write,
    owns write_path @key,          # encoded (thread_id, ns, checkpoint_id, task_id, idx)
    owns thread_id,
    owns checkpoint_ns,
    owns checkpoint_id,
    owns task_id,
    owns task_path,
    owns write_idx,
    owns channel,
    owns write_type,
    owns write_blob;               # base64(bytes)

  attribute cp_path, value string;
  attribute blob_path, value string;
  attribute write_path, value string;
  attribute thread_id, value string;
  attribute checkpoint_ns, value string;
  attribute checkpoint_id, value string;
  attribute parent_checkpoint_id, value string;
  attribute cp_type, value string;
  attribute cp_blob, value string;
  attribute md_blob, value string;
  attribute run_id, value string;
  attribute channel, value string;
  attribute version, value string;
  attribute blob_type, value string;
  attribute blob_value, value string;
  attribute task_id, value string;
  attribute task_path, value string;
  attribute write_idx, value integer;
  attribute write_type, value string;
  attribute write_blob, value string;
```

### Key encoding

Reuses `NS_SEP = "\x1f"` from the sibling project. None of these characters appear in valid thread / namespace / checkpoint / task IDs (UUIDs, monotonic IDs, or user-controlled strings without control chars).

```python
def encode_cp_path(thread_id, ns, checkpoint_id) -> str:
    return f"{thread_id}{NS_SEP}{ns}{NS_SEP}{checkpoint_id}"

def encode_blob_path(thread_id, ns, channel, version) -> str:
    return f"{thread_id}{NS_SEP}{ns}{NS_SEP}{channel}{NS_SEP}{version}"

def encode_write_path(thread_id, ns, checkpoint_id, task_id, idx) -> str:
    return f"{thread_id}{NS_SEP}{ns}{NS_SEP}{checkpoint_id}{NS_SEP}{task_id}{NS_SEP}{idx}"
```

`@key` on the encoded path enforces uniqueness, matching the Postgres composite PKs.

### Why denormalize `thread_id` / `checkpoint_ns` / etc.?

To support `list(config)` filters and `delete_thread(thread_id)` without parsing `cp_path`. The `@key` on `cp_path` is purely for uniqueness; the redundant attribute columns are for filtering. `run_id` is denormalized from `metadata` for `delete_for_runs`.

### Binary handling

`serde.dumps_typed(...)` returns `(type, bytes)`. We base64-encode the bytes (`base64.b64encode(...).decode("ascii")`) before storing in `cp_blob` / `blob_value` / `write_blob`. Inverse on read. This keeps the schema all-string and avoids whatever escaping headaches TypeQL string literals impose on arbitrary bytes.

---

## D. `BaseCheckpointSaver` method-by-method mapping

All queries run inside `driver.transaction(self._db, TransactionType.{READ,WRITE})`. Writes commit.

### `put(config, checkpoint, metadata, new_versions) -> RunnableConfig`

1. Pull `thread_id`, `checkpoint_ns`, `parent_checkpoint_id` from `config["configurable"]`. New `checkpoint_id` is `checkpoint["id"]`.
2. Serialize each `(channel, value)` in `checkpoint["channel_values"]` for **channels with versions in `new_versions`** (only the ones that changed) via `serde.dumps_typed`. Strip `channel_values` from the in-memory checkpoint dict before serializing (matches Postgres — channel values live in the blobs table, not in the checkpoint JSON).
3. WRITE txn:
   - For each new blob: `match $b isa checkpoint_blob, has blob_path "<p>"; delete $b;` then `insert ... isa checkpoint_blob, ...`. Postgres uses `ON CONFLICT DO NOTHING` — TypeDB equivalent is `match` first, skip if exists. We **delete-then-insert** unconditionally; it's idempotent and saves the existence check round-trip.
   - For the checkpoint row: same delete-then-insert pattern. Equivalent to Postgres's `ON CONFLICT DO UPDATE SET checkpoint=..., metadata=...`.
4. Commit, with **single retry on commit conflict** (same policy as the sibling store). Return updated config with the new `checkpoint_id` in `configurable`.

### `put_writes(config, writes, task_id, task_path="") -> None`

1. Extract `thread_id`, `checkpoint_ns`, `checkpoint_id` from `config["configurable"]`.
2. WRITE txn — for each `(channel, value)` at index `idx`:
   - Serialize `value` via `serde.dumps_typed`.
   - **Special-channel rule (matches Postgres):** if `channel` is in `WRITES_IDX_MAP` (the LangGraph-reserved channels like `__error__`, `__interrupt__`), use a plain upsert (`ON CONFLICT DO UPDATE`). Otherwise use insert-if-absent (`ON CONFLICT DO NOTHING`). In TypeDB: branch on whether to delete-then-insert vs match-first-skip-if-exists.
3. Commit, retry-once on conflict.

### `get_tuple(config) -> CheckpointTuple | None`

Two READ txns (or one with multiple match clauses):

1. Resolve `checkpoint_id`. If `config["configurable"]["checkpoint_id"]` is present, use it. Otherwise fetch the latest:
   ```
   match
     $c isa checkpoint, has thread_id "<tid>", has checkpoint_ns "<ns>",
       has checkpoint_id $cid;
   fetch { "cid": $cid };
   # Python: max() by checkpoint_id (lexicographic order matches creation order — UUID7 / timestamp-prefix)
   ```
2. Fetch the row + all related writes + all blobs whose `(channel, version)` appears in `checkpoint["channel_versions"]`:
   - Three separate match queries in one READ txn (TypeDB doesn't joining cleanly across entities without relations; cheaper to do three filtered fetches than to model a relation).
3. Reconstruct: `serde.loads_typed` for blobs into `channel_values`, splice into the checkpoint dict; assemble `pending_writes` as `(task_id, channel, value)` (note: API spec says `(channel, task_id, value)` — confirm exact tuple order from `PendingWrite`; Postgres returns `(task_id, channel, value)` per `_load_writes`). Verify during impl.

### `list(config, *, filter=None, before=None, limit=None) -> Iterator[CheckpointTuple]`

1. WHERE-clause equivalent: optional `thread_id`, `checkpoint_ns` from config (both optional — `config=None` lists everything). Optional `before["configurable"]["checkpoint_id"]` upper bound. `filter` keys mapped against denormalized fields and `metadata` (for non-denorm keys, fetch + client-filter — see deferred items).
2. Sort by `checkpoint_id` DESC (lexicographic; LangGraph uses time-prefixed IDs).
3. For each checkpoint row, fetch its writes + blobs and yield a `CheckpointTuple`. Hydration identical to `get_tuple`.
4. `limit` applied via Python slicing (TypeQL `limit`/`offset` available in 3.x — use server-side if confirmed; otherwise client-side, document the perf cost).

### `delete_thread(thread_id) -> None`

WRITE txn:
```
match $c isa checkpoint, has thread_id "<tid>"; delete $c;
match $b isa checkpoint_blob, has thread_id "<tid>"; delete $b;
match $w isa checkpoint_write, has thread_id "<tid>"; delete $w;
```
Commit.

### `copy_thread(source_thread_id, target_thread_id) -> None`

v0.1: READ source rows in one txn, build a WRITE txn that inserts mirrored rows with `target_thread_id` substituted in `thread_id` and `*_path`. Atomic at commit boundary, not across the read/write split — acceptable for a copy op (no concurrent writer expected on a thread mid-copy).

### `delete_for_runs(run_ids) -> None`

WRITE txn — `match $c isa checkpoint, has run_id $r; { $r == "<r1>"; } or { $r == "<r2>"; } …; delete $c;`. Then cascade-delete orphan blobs/writes by `(thread_id, ns, checkpoint_id)` reference. v0.1 keeps the simple form; orphan blob cleanup is documented as a v0.2 task (some blob versions are shared across checkpoints).

### `prune(thread_ids, *, strategy="keep_latest") -> None`

v0.1: implement `keep_latest` only. For each thread: find max `checkpoint_id`, delete all others + their writes; blobs left alone (shared). Unknown strategy raises `NotImplementedError`.

### `get_delta_channel_history(*, config, channels) -> Mapping[str, DeltaChannelHistory]`

v0.1: **raise `NotImplementedError`**. Requires parent-chain traversal and delta accounting; not blocking for basic checkpointing. Deferred to v0.2.

### `get_next_version(current, channel) -> str`

Inherit the default implementation from `BaseCheckpointSaver`. No override.

---

## E. Sync vs async

Same pattern as `langgraph-store-typedb`:

- **`TypeDBSaver` (sync) is primary.**
- **`AsyncTypeDBSaver` wraps the sync saver** via `asyncio.to_thread`. Each `a*` method delegates:
  ```python
  async def aput(self, config, checkpoint, metadata, new_versions):
      return await asyncio.to_thread(self._s.put, config, checkpoint, metadata, new_versions)
  async def alist(self, config, **kw):
      for tup in await asyncio.to_thread(lambda: list(self._s.list(config, **kw))):
          yield tup
  ```
  `alist` materializes the iterator inside the thread, then yields — simple and correct, at the cost of eager fetch. Documented limitation.

Both ship in v0.1.

---

## F. Connection management

Identical to `TypeDBStore`:

```python
class TypeDBSaver(BaseCheckpointSaver):
    def __init__(self, driver, database: str, *, retry_on_conflict: int = 1):
        super().__init__()
        self._driver = driver
        self._db = database
        self._retry_on_conflict = retry_on_conflict

    def ensure_database(self) -> None: ...   # idempotent
    def ensure_schema(self) -> None: ...     # idempotent
```

User owns the driver. Transaction-per-operation. Thread-safe (saver holds only immutable refs).

---

## G. Testing strategy

Mirror sibling repo's `tests/` layout. Lift `conftest.py` (testcontainers + driver fixture) wholesale and adapt the per-test fixture to build a `TypeDBSaver`.

### Unit tests
- `test_serialization.py` — key encoders round-trip; base64 round-trip for synthetic blobs.
- `test_query_builders.py` — generated TypeQL string assertions vs golden fixtures.

### Integration tests
- `test_put_get_tuple.py` — `put` then `get_tuple` returns equivalent `Checkpoint`/metadata; latest-by-default when `checkpoint_id` absent.
- `test_put_writes.py` — writes round-trip through `get_tuple.pending_writes`; ON-CONFLICT-DO-NOTHING for normal channels, DO-UPDATE for special channels.
- `test_list.py` — DESC ordering, `before=` bound, `limit=`, filter dict.
- `test_delete_thread.py` — purges checkpoints + writes + blobs for one thread, leaves siblings.
- `test_copy_thread.py` — source unchanged, target identical, no key collisions on re-copy (or expected error if we choose to forbid overwrite — decide in impl).
- `test_async.py` — smoke `AsyncTypeDBSaver.aput`/`aget_tuple`/`alist` under `asyncio.run`.
- `test_concurrency.py` — parallel `put` on distinct threads all succeed; same-thread race resolves with one-retry, no exception escapes.

Image pinned to `typedb/typedb:3.8.1` (same as sibling).

---

## H. Open questions (resolve during implementation)

1. **`langgraph-checkpoint` published package name and version range.** Confirm before pinning in `pyproject.toml`. The reference savers depend on `langgraph-checkpoint`, but the user-facing import path is `from langgraph.checkpoint.base import BaseCheckpointSaver`. Should not need `langgraph` core itself.
2. **`PendingWrite` tuple order.** API doc says `(channel, task_id, value)` but Postgres `_load_writes` returns `(task_id, channel, value)`. Verify from `BaseCheckpointSaver` source before wiring `get_tuple`.
3. **TypeDB 3.x `limit` / `offset` support in TypeQL.** Confirm; if available, push `list(..., limit=)` server-side.
4. **TypeQL string-literal escaping for base64 payloads.** Base64 is `[A-Za-z0-9+/=]` so no `"` or `\`, but confirm against driver.
5. **Commit-conflict exception class** in `typedb-driver` 3.x (same open question as sibling).
6. **Whether to model `checkpoint_blob` and `checkpoint_write` as relations** between `checkpoint` and channel/task entities. Cleaner, queryable joins — but more schema, more writes. v0.1 sticks with flat entities; revisit in v0.2 if `list`/`get_tuple` perf needs it.
7. **`copy_thread` overwrite semantics.** API doc doesn't specify. Postgres just inserts; collision → error. We will mirror that. Confirm.
8. **`get_next_version` default.** The base class default returns a UUID-based version string. Confirm it's sufficient (Postgres overrides; SQLite doesn't). Probably leave inherited.

---

## I. Phased delivery

### v0.1.0 — MVP

In scope:
- Sync `TypeDBSaver` with `put`, `put_writes`, `get_tuple`, `list`, `delete_thread`, `copy_thread`.
- `AsyncTypeDBSaver` thread-offload wrapper.
- `ensure_database()` and `ensure_schema()` helpers.
- One-retry on commit conflict.
- Schema as `.tql` resource.
- Unit + integration tests via testcontainers against `typedb/typedb:3.8.1`.
- README with quickstart + limitations table.
- Example: `examples/hello_checkpoint.py` — graph with `TypeDBSaver` running across two invocations on the same thread.

Cut from v0.1:
- `delete_for_runs` — present but no orphan blob cleanup; documented caveat.
- `prune` — `keep_latest` only; other strategies raise `NotImplementedError`.
- `get_delta_channel_history` — raises `NotImplementedError`.
- Server-side `filter` pushdown — client-side only.
- Server-side `limit` (depends on Open Question #3).
- Atomic batching across put + put_writes (each op is its own txn).
- Multi-tenancy / per-tenant database routing.
- TLS/auth helpers — user constructs `Credentials` / `DriverOptions`.

### v0.2.0 — completeness

- `get_delta_channel_history` via parent-chain walk.
- `delete_for_runs` with orphan blob cleanup.
- Full `prune` strategies.
- Server-side filter pushdown for indexed metadata fields (`source`, `step`, `run_id`).
- Investigate modeling blobs/writes as TypeDB relations (Open Question #6) if list-perf demands.

### v0.3.0 — TBD

- Multi-database / per-tenant routing helpers.
- Native bytes storage if TypeDB adds a bytes value type.
- Tooling: `migrate_from_postgres` script.
