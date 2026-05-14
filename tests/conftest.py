"""Shared fixtures for tests.

Integration fixtures live here but only run when TypeDB driver + Docker are
available. Unit tests don't depend on them.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from typedb.driver import Driver


@pytest.fixture(scope="session")
def typedb_container():
    pytest.importorskip("testcontainers")
    pytest.importorskip("typedb.driver")
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    c = DockerContainer("typedb/typedb:3.8.1").with_exposed_ports(1729)
    c.start()
    try:
        wait_for_logs(c, "TypeDB server is now running", timeout=60)
        yield c
    finally:
        c.stop()


@pytest.fixture(scope="session")
def driver(typedb_container) -> Driver:
    from typedb.driver import Credentials, DriverOptions, DriverTlsConfig, TypeDB

    host = typedb_container.get_container_host_ip()
    port = typedb_container.get_exposed_port(1729)
    d = TypeDB.driver(
        f"{host}:{port}",
        Credentials("admin", "password"),
        DriverOptions(DriverTlsConfig.disabled()),
    )
    try:
        yield d
    finally:
        d.close()


@pytest.fixture
def saver(driver):
    from langgraph_checkpoint_typedb import TypeDBSaver

    db = f"test_{uuid.uuid4().hex[:8]}"
    s = TypeDBSaver(driver, database=db)
    s.ensure_database()
    s.ensure_schema()
    try:
        yield s
    finally:
        driver.databases.get(db).delete()
