"""Two-phase settlement coordinator with crash injection and recovery.

Lifecycle of one settlement (GID = ``settle_<settlement_no>`` in both DBs):

  1. each DB prepares its business write with PREPARE TRANSACTION
  2. each DB records event 'prepared' in xact_log (separate committed tx)
  3. the decision is written to xact_log of BOTH databases
     ('decision_commit' or 'decision_abort')
  4. the prepared transactions are finalized (COMMIT/ROLLBACK PREPARED)
  5. each DB records the terminal event ('committed' / 'aborted')

Injected failure points (FAILPOINT env var):

  after_both_prepared - hard process exit once both DBs have prepared but
                        before any decision row exists ("no decision yet")
  after_one_commit    - hard process exit after the debit DB has committed
                        its prepared transaction but before the credit DB

Recovery rules:

  * decision_commit visible in either log  -> finish COMMIT on both DBs
  * decision_abort  visible in either log  -> finish ROLLBACK on both DBs
  * no decision row anywhere and both prepared -> ABORT
  * no decision row and one side already finalized (commit/rollback cannot
    be told apart from the log) and the other still prepared -> KEEP both
    prepared transactions in place and report "cannot verify"
  * terminal status is only reported once neither DB still holds the
    prepared transaction; otherwise the transaction is preserved and the
    operator is told to intervene.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from . import db
from .config import (
    FAIL_AFTER_BOTH_PREPARED,
    FAIL_AFTER_ONE_COMMIT,
    gid_for,
    validate_settlement_no,
)


@dataclass
class RecoveryResult:
    settlement_no: str
    status: str                       # committed | aborted | held | noop | duplicate
    decision_basis: str               # human-readable evidence description
    detail: dict[str, Any] = field(default_factory=dict)


class InconsistentSettlement(RuntimeError):
    """The two databases disagree in a way that needs a human on-call."""


def _hard_exit(reason: str) -> None:
    os.write(2, f"[coordinator] INJECTED CRASH at {reason}, exiting\n".encode())
    os._exit(99)


def start_settlement(dsn_debit: str, dsn_credit: str,
                     settlement_no: str, amount: float,
                     failpoint: str) -> None:
    """Run (or crash inside) the two-phase settlement."""
    settlement_no = validate_settlement_no(settlement_no)
    gid = gid_for(settlement_no)

    # Idempotency guard: a fully or partially processed settlement must not
    # be driven through prepare a second time (that would double-post).
    for name, dsn in (("debit", dsn_debit), ("credit", dsn_credit)):
        if db.prepared_exists(dsn, gid) or db.has_ledger_entry(dsn, settlement_no):
            raise InconsistentSettlement(
                f"settlement {settlement_no} already exists in {name} database; "
                "use recovery instead of starting it again")

    # ---- Phase 1: prepare both databases --------------------------------
    db.prepare_business_tx(dsn_debit, settlement_no, "DEBIT", amount, gid)
    db.prepare_business_tx(dsn_credit, settlement_no, "CREDIT", amount, gid)
    # The 'prepared' log rows are committed independently, so they remain
    # visible even if the prepared business tx is later rolled back.
    db.log_event(dsn_debit, settlement_no, db.EVENT_PREPARED, "business tx prepared")
    db.log_event(dsn_credit, settlement_no, db.EVENT_PREPARED, "business tx prepared")

    if failpoint == FAIL_AFTER_BOTH_PREPARED:
        _hard_exit("after_both_prepared")

    # ---- Phase 2: durable decision, written to BOTH coordinator logs ----
    db.log_event(dsn_debit, settlement_no, db.EVENT_DECISION_COMMIT, "decision: commit")
    db.log_event(dsn_credit, settlement_no, db.EVENT_DECISION_COMMIT, "decision: commit")

    # ---- Phase 3: finalize ----------------------------------------------
    debit_outcome = db.commit_prepared(dsn_debit, gid)
    db.log_event(dsn_debit, settlement_no, db.EVENT_COMMITTED,
                 f"COMMIT PREPARED result={debit_outcome}")

    if failpoint == FAIL_AFTER_ONE_COMMIT:
        _hard_exit("after_one_commit")

    credit_outcome = db.commit_prepared(dsn_credit, gid)
    db.log_event(dsn_credit, settlement_no, db.EVENT_COMMITTED,
                 f"COMMIT PREPARED result={credit_outcome}")


def _decide(dsn_debit: str, dsn_credit: str,
            settlement_no: str, debit_prepared: bool,
            credit_prepared: bool) -> tuple[str, str]:
    """Return (decision, basis) from the coordinator logs."""
    debit_log = {row["event"] for row in db.get_logs(dsn_debit, settlement_no)}
    credit_log = {row["event"] for row in db.get_logs(dsn_credit, settlement_no)}
    all_events = debit_log | credit_log

    if db.EVENT_DECISION_COMMIT in all_events:
        where = []
        if db.EVENT_DECISION_COMMIT in debit_log:
            where.append("debit")
        if db.EVENT_DECISION_COMMIT in credit_log:
            where.append("credit")
        return "commit", f"decision_commit found in {','.join(where)} log"

    if db.EVENT_DECISION_ABORT in all_events:
        where = []
        if db.EVENT_DECISION_ABORT in debit_log:
            where.append("debit")
        if db.EVENT_DECISION_ABORT in credit_log:
            where.append("credit")
        return "abort", f"decision_abort found in {','.join(where)} log"

    # No decision recorded anywhere.
    if debit_prepared and credit_prepared:
        return "abort", "no decision recorded; both databases still prepared -> abort"

    # One side is already finalized without any durable decision and the
    # other still holds a prepared transaction: we cannot tell whether the
    # finalized side committed or rolled back. Touching the remaining
    # prepared tx could create an inconsistent split, so preserve it.
    if debit_prepared != credit_prepared:
        return "hold", (
            "no decision recorded and only one database still holds the "
            "prepared transaction; cannot verify the finalized side"
        )

    return "hold", "no decision recorded and no prepared transaction remains; cannot verify"


def recover_settlement(dsn_debit: str, dsn_credit: str,
                       settlement_no: str) -> RecoveryResult:
    """Resume one interrupted settlement. Safe to call concurrently/repeatedly."""
    settlement_no = validate_settlement_no(settlement_no)
    gid = gid_for(settlement_no)

    # The debit database hosts the advisory lock that serializes recovery.
    with db.recovery_lock(dsn_debit, settlement_no) as acquired:
        if not acquired:
            return RecoveryResult(
                settlement_no, "duplicate",
                "another recovery for this settlement is already running; request rejected",
            )

        debit_prepared = db.prepared_exists(dsn_debit, gid)
        credit_prepared = db.prepared_exists(dsn_credit, gid)

        # Nothing left to recover. Distinguish completed business from an
        # unknown settlement number for the operator UI.
        if not debit_prepared and not credit_prepared:
            debit_done = db.has_ledger_entry(dsn_debit, settlement_no)
            credit_done = db.has_ledger_entry(dsn_credit, settlement_no)
            if debit_done and credit_done:
                return RecoveryResult(
                    settlement_no, "committed",
                    "no residual prepared transaction; ledger entries exist in both databases",
                    {"debit_prepared": False, "credit_prepared": False})
            if not debit_done and not credit_done:
                return RecoveryResult(
                    settlement_no, "noop",
                    "no prepared transaction and no ledger entry for this settlement number",
                    {"debit_prepared": False, "credit_prepared": False})
            return RecoveryResult(
                settlement_no, "held",
                "no prepared transaction remains but ledger entries exist in only one "
                "database; human investigation required",
                {"debit_ledger": debit_done, "credit_ledger": credit_done})

        decision, basis = _decide(dsn_debit, dsn_credit, settlement_no,
                                  debit_prepared, credit_prepared)

        if decision == "hold":
            return RecoveryResult(
                settlement_no, "held", basis,
                {"debit_prepared": debit_prepared, "credit_prepared": credit_prepared})

        if decision == "commit":
            # Durable decision first, so a crash during recovery still leaves
            # an unambiguous trail even if one log was previously missing it.
            for dsn in (dsn_debit, dsn_credit):
                if db.EVENT_DECISION_COMMIT not in {
                        r["event"] for r in db.get_logs(dsn, settlement_no)}:
                    db.log_event(dsn, settlement_no, db.EVENT_DECISION_COMMIT,
                                 "decision replicated during recovery")
            outcomes = {}
            if debit_prepared:
                outcomes["debit"] = db.commit_prepared(dsn_debit, gid)
                db.log_event(dsn_debit, settlement_no, db.EVENT_COMMITTED,
                             f"recovery COMMIT PREPARED result={outcomes['debit']}")
            if credit_prepared:
                outcomes["credit"] = db.commit_prepared(dsn_credit, gid)
                db.log_event(dsn_credit, settlement_no, db.EVENT_COMMITTED,
                             f"recovery COMMIT PREPARED result={outcomes['credit']}")

        else:  # decision == "abort"
            for dsn in (dsn_debit, dsn_credit):
                if db.EVENT_DECISION_ABORT not in {
                        r["event"] for r in db.get_logs(dsn, settlement_no)}:
                    db.log_event(dsn, settlement_no, db.EVENT_DECISION_ABORT,
                                 "decision recorded during recovery")
            outcomes = {}
            if debit_prepared:
                outcomes["debit"] = db.rollback_prepared(dsn_debit, gid)
                db.log_event(dsn_debit, settlement_no, db.EVENT_ABORTED,
                             f"recovery ROLLBACK PREPARED result={outcomes['debit']}")
            if credit_prepared:
                outcomes["credit"] = db.rollback_prepared(dsn_credit, gid)
                db.log_event(dsn_credit, settlement_no, db.EVENT_ABORTED,
                             f"recovery ROLLBACK PREPARED result={outcomes['credit']}")

        # Terminal state only after BOTH databases have been finalized.
        still_debit = db.prepared_exists(dsn_debit, gid)
        still_credit = db.prepared_exists(dsn_credit, gid)
        if still_debit or still_credit:
            return RecoveryResult(
                settlement_no, "held",
                "finalization incomplete; prepared transaction preserved for retry",
                {"debit_prepared": still_debit, "credit_prepared": still_credit})

        return RecoveryResult(
            settlement_no, "committed" if decision == "commit" else "aborted",
            basis, {"finalize": outcomes,
                    "debit_prepared": False, "credit_prepared": False})
