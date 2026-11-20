#!/usr/bin/env python3
"""Manage two local PostgreSQL instances for the 2PC drill (Docker-free path).

This exists for environments where Docker Compose is unavailable. It uses the
PostgreSQL binaries bundled in the `pgserver` wheel to initialize and run two
independent clusters under .local-pg/, equivalent to the debit-db and
credit-db Compose services. State (ports, DSNs) is written to
.local-pg/state.env so the web app and test harness can source it.

Usage:
  python -I scripts/local_pg.py init|start|stop|status|clean
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path

from pgserver._commands import POSTGRES_BIN_PATH

BIN = Path(POSTGRES_BIN_PATH)
BASE = Path(__file__).resolve().parent.parent / ".local-pg"
STATE = BASE / "state.env"
META = BASE / "meta.json"
CLUSTERS = {
    "debit": 55432,   # mirrors the host port in docker-compose.yml
    "credit": 55433,
}


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=True, capture_output=True, text=True, **kw)


def _run_to(cmd: list[str], out) -> None:
    subprocess.run(cmd, check=True, stdout=out, stderr=out, text=True)


def _write_meta() -> None:
    BASE.mkdir(parents=True, exist_ok=True)
    META.write_text(json.dumps(CLUSTERS, indent=2))
    lines = [
        f"DSN_DEBIT='host=127.0.0.1 port={CLUSTERS['debit']} dbname=postgres user=postgres'",
        f"DSN_CREDIT='host=127.0.0.1 port={CLUSTERS['credit']} dbname=postgres user=postgres'",
    ]
    STATE.write_text("\n".join(lines) + "\n")


def init_clusters() -> None:
    if (BASE / "debit").exists():
        print("clusters already initialized")
        # Refresh state.env/meta.json in case an older run wrote a stale format.
        for name in CLUSTERS:
            conf = (BASE / name / "postgresql.conf").read_text()
            for line in conf.splitlines():
                if line.startswith("port = "):
                    CLUSTERS[name] = int(line.split("=", 1)[1].strip())
        _write_meta()
        return
    BASE.mkdir(parents=True, exist_ok=True)
    for name, preferred_port in CLUSTERS.items():
        data = BASE / name
        data.mkdir(parents=True)
        _run([str(BIN / "initdb"), "-D", str(data), "-U", "postgres", "--auth=trust"])
        port = preferred_port
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                port = _free_port()
        (data / "postgresql.conf").write_text(
            f"port = {port}\n"
            "listen_addresses = '127.0.0.1'\n"
            f"unix_socket_directories = '{BASE}'\n"
            "fsync = off\nsynchronous_commit = off\nfull_page_writes = off\n"
            "max_prepared_transactions = 200\n")
        CLUSTERS[name] = port
    _write_meta()
    print("initialized clusters under", BASE)


def start() -> None:
    init_clusters()
    for name in CLUSTERS:
        log = open(BASE / f"{name}.log", "ab")
        _run_to([str(BIN / "pg_ctl"), "-D", str(BASE / name),
                 "-l", str(BASE / f"{name}.log"), "-w", "start"], log)
    print("started:", json.dumps(CLUSTERS))


def stop() -> None:
    for name in CLUSTERS:
        data = BASE / name
        if data.exists():
            subprocess.run([str(BIN / "pg_ctl"), "-D", str(data),
                            "-m", "fast", "stop"], capture_output=True, text=True)
    print("stopped")


def status() -> None:
    if not META.exists():
        print("not initialized")
        sys.exit(1)
    for name, port in json.loads(META.read_text()).items():
        r = subprocess.run([str(BIN / "pg_isready"), "-h", "127.0.0.1",
                            "-p", str(port), "-U", "postgres"],
                           capture_output=True, text=True)
        print(f"{name:7s} port={port} -> {r.stdout.strip() or r.stderr.strip()}")


def clean() -> None:
    stop()
    shutil.rmtree(BASE, ignore_errors=True)
    print("cleaned", BASE)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    {"init": init_clusters, "start": start, "stop": stop,
     "status": status, "clean": clean}[cmd]()
