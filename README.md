# langgraph-checkpoint-typedb

A [LangGraph](https://langchain-ai.github.io/langgraph/) `BaseCheckpointSaver` implementation
backed by [TypeDB 3.x](https://typedb.com/).

Persists graph state across invocations using TypeDB as the backing store. Mirrors the
schema shape of `langgraph-checkpoint-postgres` (checkpoints, blobs, writes) translated to
TypeDB entities.

## Install

```bash
pip install langgraph-checkpoint-typedb
```

Requires Python 3.11+ and a TypeDB 3.8+ server.

## Quickstart

```python
from typedb.driver import TypeDB, Credentials, DriverOptions, DriverTlsConfig
from langgraph_checkpoint_typedb import TypeDBSaver

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

The saver does not own the driver lifecycle — call `driver.close()` yourself when done.

## Schema

Three entity types:

- `checkpoint` — one per `(thread_id, checkpoint_ns, checkpoint_id)`.
- `checkpoint_blob` — channel values keyed by `(thread_id, checkpoint_ns, channel, version)`.
- `checkpoint_write` — intermediate writes keyed by `(thread_id, checkpoint_ns, checkpoint_id, task_id, idx)`.

Composite keys are encoded into a single `@key` string attribute using ASCII Unit Separator
(`\x1f`) as a delimiter. Binary blobs (from LangGraph's `JsonPlusSerializer`) are
base64-encoded — TypeDB 3.x has no native bytes attribute type.

See `src/langgraph_checkpoint_typedb/resources/schema.tql`.

## Async

```python
from langgraph_checkpoint_typedb import AsyncTypeDBSaver, TypeDBSaver

sync = TypeDBSaver(driver, database="langgraph_checkpoints")
sync.ensure_database()
sync.ensure_schema()
asaver = AsyncTypeDBSaver(sync)

graph = builder.compile(checkpointer=asaver)
await graph.ainvoke({"input": "hi"}, config={"configurable": {"thread_id": "u-1"}})
```

`AsyncTypeDBSaver` offloads each operation to a thread via `asyncio.to_thread`. The
underlying TypeDB Python driver is synchronous-only.

## Limitations (v0.1)

| Feature | Status |
| --- | --- |
| `get_delta_channel_history` | Raises `NotImplementedError` |
| `prune` strategies | `keep_latest` only; others raise `NotImplementedError` |
| `delete_for_runs` | Implemented; orphan blob cleanup deferred |
| `list(filter=...)` operators | Top-level exact match only |
| Server-side `limit` pushdown | Applied client-side |
| Atomic batching across put + put_writes | Each op runs in its own transaction |

See `DESIGN.md` for the full design and roadmap.

## License

MIT
