"""Data-access layer.

Both databases use the same schema:

* ``ledger_entries``   - the business write protected by the prepared
  transaction (a debit row in the debit database, a credit row in the credit
  database). A unique constraint on settlement_no makes duplicate posting
  impossible even if recovery is replayed.
* ``xact_log``         - the coordinator log. Rows are written in short
  autocommit transactions, so they survive even when the prepared business
  transaction is later rolled back.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row

from .config import GID_RE

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ledger_entries (
    id            BIGSERIAL PRIMARY KEY,
    settlement_no TEXT NOT NULL,
    direction     TEXT NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount        NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ledger_settlement
    ON ledger_entries(settlement_no);

CREATE TABLE IF NOT EXISTS xact_log (
    id            BIGSERIAL PRIMARY KEY,
    settlement_no TEXT NOT NULL,
    event         TEXT NOT NULL CHECK (event IN (
                      'prepared', 'decision_commit', 'decision_abort',
                      'committed', 'aborted')),
    note          TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_xact_log_settlement ON xact_log(settlement_no);
"""

EVENT_PREPARED = "prepared"
EVENT_DECISION_COMMIT = "decision_commit"
EVENT_DECISION_ABORT = "decision_abort"
EVENT_COMMITTED = "committed"
EVENT_ABORTED = "aborted"


def _sql_literal(value: str) -> str:
    """Quote a value as a PostgreSQL string literal.

    Only used for GIDs in PREPARE/COMMIT/ROLLBACK TRANSACTION, which cannot
    take bind parameters. Callers additionally validate against GID_RE, so
    the generic doubling here is defense in depth.
    """
    return "'" + value.replace("'", "''") + "'"


def _check_gid(gid: str) -> str:
    if not GID_RE.fullmatch(gid):
        raise ValueError(f"unsafe prepared-transaction GID: {gid!r}")
    return gid


@contextmanager
def connect(dsn: str) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(dsn, connect_timeout=10)
    try:
        yield conn
    finally:
        conn.close()


def _table_exists(conn: psycopg.Connection, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = %s",
            (table,),
        )
        return cur.fetchone() is not None


def init_schema(dsn: str) -> None:
    """Create the schema when absent.

    Tables are checked through pg_tables first: if the schema already exists
    we run no DDL at all, so startup never blocks on a strong relation lock
    held by a residual prepared transaction from a crashed coordinator.
    """
    with connect(dsn) as conn, conn.cursor() as cur:
        if _table_exists(conn, "ledger_entries") and _table_exists(conn, "xact_log"):
            conn.commit()
            return
        cur.execute(SCHEMA_SQL)
        conn.commit()


def list_prepared(dsn: str) -> list[dict[str, Any]]:
    """Residual prepared transactions visible in pg_prepared_xacts."""
    with connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT gid, owner, database AS dbname, prepared
            FROM pg_prepared_xacts
            ORDER BY prepared
            """
        )
        rows = cur.fetchall()
    for row in rows:
        if isinstance(row.get("prepared"), datetime):
            row["prepared"] = row["prepared"].isoformat(timespec="seconds")
    return rows


def prepared_exists(dsn: str, gid: str) -> bool:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_prepared_xacts WHERE gid = %s", (gid,))
        return cur.fetchone() is not None


def log_event(dsn: str, settlement_no: str, event: str, note: str | None = None) -> None:
    """Append a coordinator log row in its own committed transaction."""
    with connect(dsn) as conn, conn.cursor() as cur:
        conn.autocommit = True
        cur.execute(
            "INSERT INTO xact_log (settlement_no, event, note) VALUES (%s, %s, %s)",
            (settlement_no, event, note),
        )


def get_logs(dsn: str, settlement_no: str) -> list[dict[str, Any]]:
    with connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT event, note, created_at
            FROM xact_log
            WHERE settlement_no = %s
            ORDER BY id
            """,
            (settlement_no,),
        )
        rows = cur.fetchall()
    for row in rows:
        if isinstance(row.get("created_at"), datetime):
            row["created_at"] = row["created_at"].isoformat(timespec="seconds")
    return rows


def has_ledger_entry(dsn: str, settlement_no: str) -> bool:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM ledger_entries WHERE settlement_no = %s",
            (settlement_no,),
        )
        return cur.fetchone() is not None


def prepare_business_tx(dsn: str, settlement_no: str, direction: str,
                        amount: float, gid: str) -> None:
    """Prepare the business transaction.

    The unique index on settlement_no turns any replay attempt into a hard
    database error instead of a double posting. GID is validated, then
    embedded as a literal because PREPARE TRANSACTION rejects bind params.
    """
    _check_gid(gid)
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute(
            "INSERT INTO ledger_entries (settlement_no, direction, amount) "
            "VALUES (%s, %s, %s)",
            (settlement_no, direction, amount),
        )
        cur.execute(f"PREPARE TRANSACTION {_sql_literal(gid)}")
        # PREPARE TRANSACTION already committed the work into a 2PC state;
        # psycopg's commit() is now a no-op for this connection.


def commit_prepared(dsn: str, gid: str) -> str:
    """COMMIT PREPARED. Returns 'committed' or 'already_gone' (idempotent)."""
    _check_gid(gid)
    with connect(dsn) as conn, conn.cursor() as cur:
        conn.autocommit = True
        try:
            cur.execute(f"COMMIT PREPARED {_sql_literal(gid)}")
            return "committed"
        except psycopg.errors.UndefinedObject:
            return "already_gone"


def rollback_prepared(dsn: str, gid: str) -> str:
    """ROLLBACK PREPARED. Returns 'rolled_back' or 'already_gone'."""
    _check_gid(gid)
    with connect(dsn) as conn, conn.cursor() as cur:
        conn.autocommit = True
        try:
            cur.execute(f"ROLLBACK PREPARED {_sql_literal(gid)}")
            return "rolled_back"
        except psycopg.errors.UndefinedObject:
            return "already_gone"


@contextmanager
def recovery_lock(dsn: str, settlement_no: str) -> Iterator[bool]:
    """Non-blocking, process-wide interlock for one settlement number.

    A session-level advisory lock held on an open connection. Concurrent or
    duplicate recovery of the same settlement gets ``False`` and must stop;
    a coordinator crash releases the lock automatically when Postgres drops
    the dead connection, so restart can always proceed.
    """
    conn = psycopg.connect(dsn, connect_timeout=10)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(hashtext('settle_recovery'), hashtext(%s))",
                (settlement_no,),
            )
            acquired = cur.fetchone()[0]
        try:
            yield bool(acquired)
        finally:
            if acquired:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_unlock(hashtext('settle_recovery'), hashtext(%s))",
                        (settlement_no,),
                    )
    finally:
        conn.close()
