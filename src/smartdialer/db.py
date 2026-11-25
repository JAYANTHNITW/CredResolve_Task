"""SQLite connections and explicit transactions. Each worker owns its connection."""

import random
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.resources import files
from os import PathLike


class WorkerConnection(sqlite3.Connection):
    busy_retries: int = 0


def _execute_with_retry(conn: sqlite3.Connection, statement: str) -> None:
    for attempt in range(3):
        try:
            conn.execute(statement)
            return
        except sqlite3.OperationalError as exc:
            code = getattr(exc, "sqlite_errorcode", 0)
            if (code & 0xFF) != sqlite3.SQLITE_BUSY or attempt == 2:
                raise
            if isinstance(conn, WorkerConnection):
                conn.busy_retries += 1
            time.sleep(random.uniform(0.001, 0.010))


def connect(path: str | PathLike[str]) -> WorkerConnection:
    conn = sqlite3.connect(path, isolation_level=None, factory=WorkerConnection)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        _execute_with_retry(conn, "PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn
    except BaseException:
        conn.close()
        raise


@contextmanager
def tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Retry busy transaction boundaries. Execute the caller's body once."""
    _execute_with_retry(conn, "BEGIN IMMEDIATE")
    try:
        yield conn
        _execute_with_retry(conn, "COMMIT")
    except BaseException:
        if conn.in_transaction:
            conn.rollback()
        raise


def init_schema(conn: sqlite3.Connection) -> None:
    """Create the schema atomically. Safe to repeat on an initialized database."""
    if conn.in_transaction:
        raise ValueError("initialize the schema outside an active transaction")
    schema = files("smartdialer").joinpath("schema.sql").read_text(encoding="utf-8")
    # executescript implicitly commits an open transaction, so keep DDL inside tx.
    with tx(conn):
        statement = ""
        for line in schema.splitlines(keepends=True):
            statement += line
            if sqlite3.complete_statement(statement):
                conn.execute(statement)
                statement = ""
        if statement.strip():
            raise ValueError("incomplete schema statement")
