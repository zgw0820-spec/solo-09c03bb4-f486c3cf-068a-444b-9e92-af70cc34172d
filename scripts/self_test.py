#!/usr/bin/env python3
"""End-to-end self-test for the two interrupt points and recovery guarantees.

Runs the real FastAPI app in a uvicorn subprocess against the two local
PostgreSQL clusters managed by scripts/local_pg.py. The coordinator is
"killed" via its own os._exit(99) failpoints, then a fresh process is started
to exercise restart-and-continue - exactly the operator drill.

Checks:
  T1 failpoint after_both_prepared -> both DBs prepared, no decision ->
     restart -> recovery aborts; zero ledger rows; no residual prepared tx
  T2 failpoint after_one_commit -> debit committed, credit prepared,
     decision_commit logged -> restart -> recovery commits credit;
     exactly one ledger row per DB; no residual prepared tx
  T3 duplicate / concurrent recovery for the same settlement is rejected or
     converges without double posting (unique constraint + advisory lock)
  T4 cannot-verify case: prepared tx with no log evidence at all is held,
     not force-committed or force-rolled-back
  T5 normal happy path with no failpoint completes in one shot

Run:  .venv/bin/python -I scripts/self_test.py
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psycopg

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app import db as app_db  # noqa: E402

STATE = ROOT / ".local-pg" / "state.env"


def load_dsns() -> tuple[str, str]:
    env = {}
    for line in STATE.read_text().splitlines():
        k, v = line.split("=", 1)
        env[k] = v.strip().strip("'").strip('"')
    return env["DSN_DEBIT"], env["DSN_CREDIT"]


class WebApp:
    def __init__(self, dsn_debit: str, dsn_credit: str, failpoint: str):
        self.env = dict(os.environ)
        self.env.update({"DSN_DEBIT": dsn_debit, "DSN_CREDIT": dsn_credit,
                         "FAILPOINT": failpoint, "APP_PORT": "8080",
                         "PYTHONUNBUFFERED": "1"})
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        # Fail fast on a stale server holding the port, instead of waiting out
        # the whole health-check timeout.
        try:
            stale = httpx.get("http://127.0.0.1:8080/healthz", timeout=1)
            raise RuntimeError(
                f"port 8080 already answers (HTTP {stale.status_code}); "
                "stop the stale uvicorn before running the self-test")
        except httpx.HTTPError:
            pass
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", "8080", "--workers", "1"],
            cwd=str(ROOT), env=self.env,
            stdout=open(ROOT / ".local-pg" / "web.out.log", "ab"),
            stderr=open(ROOT / ".local-pg" / "web.err.log", "ab"))
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"web app exited during startup (rc={self.proc.returncode}); "
                    "port 8080 may already be in use")
            try:
                r = httpx.get("http://127.0.0.1:8080/healthz", timeout=2)
                if r.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.3)
        self.stop()
        raise RuntimeError("web app did not become healthy")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.proc = None

    def post_form(self, path: str, data: dict, timeout: float = 15):
        return httpx.post(f"http://127.0.0.1:8080{path}", data=data, timeout=timeout)

    def get_json(self, path: str):
        return httpx.get(f"http://127.0.0.1:8080{path}", timeout=10).json()


def prepared_gids(dsn: str) -> set[str]:
    with psycopg.connect(dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("SELECT gid FROM pg_prepared_xacts")
        return {r[0] for r in cur.fetchall()}


def ledger_count(dsn: str, settlement_no: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("SELECT count(*) FROM ledger_entries WHERE settlement_no=%s",
                    (settlement_no,))
        return cur.fetchone()[0]


def make_prepared_without_logs(dsn: str, gid: str, settlement_no: str) -> None:
    """Simulate an orphan prepared tx whose coordinator evidence is missing."""
    safe_gid = gid.replace("'", "''")
    with psycopg.connect(dsn) as c, c.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute(
            "INSERT INTO ledger_entries (settlement_no, direction, amount) "
            "VALUES (%s, 'DEBIT', 9.99)", (settlement_no,))
        cur.execute(f"PREPARE TRANSACTION '{safe_gid}'")


PASS, FAIL = 0, 0


def check(name: str, cond: bool, extra: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name} {extra}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} {extra}")


def main() -> int:
    dsn_debit, dsn_credit = load_dsns()
    # Make sure the application schema exists, then start from a clean slate.
    app_db.init_schema(dsn_debit)
    app_db.init_schema(dsn_credit)
    for dsn in (dsn_debit, dsn_credit):
        with psycopg.connect(dsn, autocommit=True) as c, c.cursor() as cur:
            # Residual prepared transactions hold relation locks, so they must
            # be finalized (GID is a literal, not a bind parameter) before any
            # row-level cleanup that needs the same lock.
            cur.execute("SELECT gid FROM pg_prepared_xacts")
            for (gid,) in cur.fetchall():
                cur.execute("ROLLBACK PREPARED '" + gid.replace("'", "''") + "'")
            cur.execute("DELETE FROM xact_log")
            cur.execute("DELETE FROM ledger_entries")

    # ---------- T1: crash after both prepared, no decision -> abort ------
    print("T1 两库已预备、尚无决定 (after_both_prepared)")
    no1 = "T1-ABORT-001"
    gid1 = f"settle_{no1}"
    app = WebApp(dsn_debit, dsn_credit, "after_both_prepared")
    app.start()
    with app.proc:
        try:
            app.post_form("/api/settlement/start",
                          {"settlement_no": no1, "amount": "100.00"})
        except httpx.HTTPError:
            pass
        time.sleep(1.0)
        check("coordinator process exited at failpoint",
              app.proc.poll() == 99, f"rc={app.proc.returncode}")
    check("debit holds prepared tx", gid1 in prepared_gids(dsn_debit))
    check("credit holds prepared tx", gid1 in prepared_gids(dsn_credit))
    logs1 = [set(r["event"] for r in app_db.get_logs(d, no1))
             for d in (dsn_debit, dsn_credit)]
    check("no decision_commit/abort logged anywhere",
          all("decision_commit" not in s and "decision_abort" not in s for s in logs1),
          str(logs1))

    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        r = app2.post_form("/api/settlement/recover", {"settlement_no": no1}).json()
        print("       recovery response:", json.dumps(r["result"], ensure_ascii=False))
        check("recovery status=aborted", r["result"]["status"] == "aborted")
        check("no residual prepared tx on debit", gid1 not in prepared_gids(dsn_debit))
        check("no residual prepared tx on credit", gid1 not in prepared_gids(dsn_credit))
        check("zero debit ledger rows", ledger_count(dsn_debit, no1) == 0)
        check("zero credit ledger rows", ledger_count(dsn_credit, no1) == 0)
        # Repeated recovery must be harmless.
        r2 = app2.post_form("/api/settlement/recover", {"settlement_no": no1}).json()
        check("repeat recovery -> noop (no double effect)",
              r2["result"]["status"] in ("noop", "aborted"))
        check("ledger counts still zero",
              ledger_count(dsn_debit, no1) == 0 and ledger_count(dsn_credit, no1) == 0)
    finally:
        app2.stop()

    # ---------- T2: crash after one commit -> finish commit --------------
    print("T2 仅一库已提交 (after_one_commit)")
    no2 = "T2-COMMIT-002"
    gid2 = f"settle_{no2}"
    app = WebApp(dsn_debit, dsn_credit, "after_one_commit")
    app.start()
    with app.proc:
        try:
            app.post_form("/api/settlement/start",
                          {"settlement_no": no2, "amount": "250.50"})
        except httpx.HTTPError:
            pass
        time.sleep(1.0)
        check("coordinator process exited at failpoint",
              app.proc.poll() == 99, f"rc={app.proc.returncode}")
    check("debit prepared tx already gone (committed)",
          gid2 not in prepared_gids(dsn_debit))
    check("credit still holds prepared tx", gid2 in prepared_gids(dsn_credit))
    check("debit has exactly one ledger row", ledger_count(dsn_debit, no2) == 1)
    check("credit has no finalized ledger row yet", ledger_count(dsn_credit, no2) == 0)

    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        view = app2.get_json(f"/api/settlement/{no2}")
        events = {r["event"] for r in view["debit_log"]} | {r["event"] for r in view["credit_log"]}
        check("decision_commit present in coordinator logs",
              "decision_commit" in events, str(sorted(events)))
        r = app2.post_form("/api/settlement/recover", {"settlement_no": no2}).json()
        print("       recovery response:", json.dumps(r["result"], ensure_ascii=False))
        check("recovery status=committed", r["result"]["status"] == "committed")
        check("no residual prepared tx on debit", gid2 not in prepared_gids(dsn_debit))
        check("no residual prepared tx on credit", gid2 not in prepared_gids(dsn_credit))
        check("exactly one debit ledger row", ledger_count(dsn_debit, no2) == 1)
        check("exactly one credit ledger row", ledger_count(dsn_credit, no2) == 1)
        r2 = app2.post_form("/api/settlement/recover", {"settlement_no": no2}).json()
        check("repeat recovery converges, no double posting",
              r2["result"]["status"] == "committed"
              and ledger_count(dsn_debit, no2) == 1
              and ledger_count(dsn_credit, no2) == 1)
    finally:
        app2.stop()

    # ---------- T3: concurrent recovery of the same settlement ----------
    print("T3 同号并发恢复防护")
    no3 = "T3-CONC-003"
    gid3 = f"settle_{no3}"
    app = WebApp(dsn_debit, dsn_credit, "after_both_prepared")
    app.start()
    with app.proc:
        try:
            app.post_form("/api/settlement/start",
                          {"settlement_no": no3, "amount": "7.00"})
        except httpx.HTTPError:
            pass
        time.sleep(1.0)
    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        results = []
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futs = [pool.submit(app2.post_form, "/api/settlement/recover",
                                {"settlement_no": no3}) for _ in range(5)]
            for f in concurrent.futures.as_completed(futs):
                results.append(f.result().json()["result"]["status"])
        print("       concurrent statuses:", results)
        check("exactly one recovery won, others rejected as duplicate",
              results.count("aborted") == 1 and results.count("duplicate") == 4,
              str(results))
        check("ledger never double-posted",
              ledger_count(dsn_debit, no3) == 0 and ledger_count(dsn_credit, no3) == 0)
        check("no prepared tx remains",
              gid3 not in prepared_gids(dsn_debit) and gid3 not in prepared_gids(dsn_credit))
    finally:
        app2.stop()

    # ---------- T4: cannot verify -> preserve prepared tx ---------------
    print("T4 无法核实日志 -> 保留事务并提示")
    no4 = "T4-HOLD-004"
    gid4 = f"settle_{no4}"
    # Orphan prepared tx on debit only; no xact_log rows anywhere.
    make_prepared_without_logs(dsn_debit, gid4, no4)
    app2 = WebApp(dsn_debit, dsn_credit, "none")
    app2.start()
    try:
        r = app2.post_form("/api/settlement/recover", {"settlement_no": no4}).json()
        print("       recovery response:", json.dumps(r["result"], ensure_ascii=False))
        check("recovery status=held", r["result"]["status"] == "held")
        check("prepared tx preserved on debit", gid4 in prepared_gids(dsn_debit))
        check("credit untouched", gid4 not in prepared_gids(dsn_credit))
        check("debit ledger row still inside preserved tx (not committed)",
              ledger_count(dsn_debit, no4) == 0)
    finally:
        app2.stop()
    # Cleanup the orphan so later reruns start clean.
    with psycopg.connect(dsn_debit, autocommit=True) as c, c.cursor() as cur:
        cur.execute("ROLLBACK PREPARED '" + gid4.replace("'", "''") + "'")

    # ---------- T5: happy path, no interrupt -----------------------------
    print("T5 无故障点的完整两阶段提交")
    no5 = "T5-HAPPY-005"
    gid5 = f"settle_{no5}"
    app = WebApp(dsn_debit, dsn_credit, "none")
    app.start()
    try:
        r = app.post_form("/api/settlement/start",
                          {"settlement_no": no5, "amount": "42.00"}).json()
        check("start ok", r.get("ok") is True, str(r))
        check("no prepared tx anywhere",
              gid5 not in prepared_gids(dsn_debit) and gid5 not in prepared_gids(dsn_credit))
        check("one ledger row per database",
              ledger_count(dsn_debit, no5) == 1 and ledger_count(dsn_credit, no5) == 1)
        # Restarting the same settlement number must be refused.
        try:
            dup = app.post_form("/api/settlement/start",
                                {"settlement_no": no5, "amount": "42.00"})
            check("duplicate start refused (409)", dup.status_code == 409)
        except Exception as exc:  # pragma: no cover
            check("duplicate start refused (409)", False, str(exc))
        check("ledger still exactly one per database",
              ledger_count(dsn_debit, no5) == 1 and ledger_count(dsn_credit, no5) == 1)
    finally:
        app.stop()

    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
