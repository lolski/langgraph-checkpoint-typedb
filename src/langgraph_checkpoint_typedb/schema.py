"""Schema loading and application helpers."""

from __future__ import annotations

from importlib import resources
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typedb.driver import Driver


SCHEMA_RESOURCE = "schema.tql"


def load_schema_tql() -> str:
    """Read the bundled schema TypeQL definition."""
    return (
        resources.files("langgraph_checkpoint_typedb.resources")
        .joinpath(SCHEMA_RESOURCE)
        .read_text(encoding="utf-8")
    )


def apply_schema(driver: Driver, database: str) -> None:
    """Apply the bundled schema to `database`. Idempotent on a fresh DB.

    Re-applying against a database that already has the schema may fail —
    callers should treat this as a one-time setup step.
    """
    from typedb.driver import TransactionType

    schema_tql = load_schema_tql()
    with driver.transaction(database, TransactionType.SCHEMA) as tx:
        tx.query(schema_tql).resolve()
        tx.commit()
