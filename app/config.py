"""Runtime configuration for the two-phase settlement ops console.

Everything is environment-driven so the same image runs under Docker Compose
(talking to the debit-db / credit-db hostnames) and under the local
self-test harness (talking to two pgserver-managed instances on 127.0.0.1).
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass

# Failure injection points used by the demo coordinator.
#   none                 - run the whole 2PC to completion
#   after_both_prepared  - die after both databases have PREPARE TRANSACTION,
#                          before any decision log row is written
#   after_one_commit     - die after the debit database committed its
#                          prepared transaction but before credit does
FAIL_NONE = "none"
FAIL_AFTER_BOTH_PREPARED = "after_both_prepared"
FAIL_AFTER_ONE_COMMIT = "after_one_commit"
VALID_FAILPOINTS = {FAIL_NONE, FAIL_AFTER_BOTH_PREPARED, FAIL_AFTER_ONE_COMMIT}


@dataclass(frozen=True)
class Settings:
    dsn_debit: str
    dsn_credit: str
    failpoint: str
    app_host: str
    app_port: int

    @staticmethod
    def from_env() -> "Settings":
        dsn_debit = os.environ.get(
            "DSN_DEBIT",
            "host=debit-db port=5432 dbname=billing user=ops password=ops",
        )
        dsn_credit = os.environ.get(
            "DSN_CREDIT",
            "host=credit-db port=5432 dbname=billing user=ops password=ops",
        )
        failpoint = os.environ.get("FAILPOINT", FAIL_NONE)
        if failpoint not in VALID_FAILPOINTS:
            raise ValueError(f"unknown FAILPOINT={failpoint!r}, want one of {sorted(VALID_FAILPOINTS)}")
        return Settings(
            dsn_debit=dsn_debit,
            dsn_credit=dsn_credit,
            failpoint=failpoint,
            app_host=os.environ.get("APP_HOST", "0.0.0.0"),
            app_port=int(os.environ.get("APP_PORT", "8080")),
        )


# GID used inside PostgreSQL's pg_prepared_xacts for a given settlement number.
# PREPARE/COMMIT/ROLLBACK PREPARED only accept a string literal (not a bind
# parameter), so the settlement number is restricted to a safe character set
# and the resulting GID can be embedded without any quoting risk.
SETTLEMENT_NO_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
GID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class InvalidSettlementNo(ValueError):
    pass


def validate_settlement_no(settlement_no: str) -> str:
    settlement_no = (settlement_no or "").strip()
    if not SETTLEMENT_NO_RE.fullmatch(settlement_no):
        raise InvalidSettlementNo(
            "结算号只能包含字母、数字、下划线、连字符，长度 1-64")
    return settlement_no


def gid_for(settlement_no: str) -> str:
    gid = f"settle_{settlement_no}"
    if not GID_RE.fullmatch(gid):
        raise InvalidSettlementNo("生成的事务 GID 非法")
    return gid
