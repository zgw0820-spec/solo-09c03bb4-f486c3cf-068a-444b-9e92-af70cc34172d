"""Smoke test: start two independent PG instances via pgserver binaries, exercise PREPARE/COMMIT."""
import os, sys, socket, subprocess, time, shutil
from pathlib import Path

import pgserver._commands as cmds
BIN = Path(cmds.POSTGRES_BIN_PATH)
BASE = Path("/tmp/pg_smoke")
shutil.rmtree(BASE, ignore_errors=True)

def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p

def start(name, port):
    data = BASE / name
    data.mkdir(parents=True)
    env = dict(os.environ)
    subprocess.run([str(BIN/"initdb"), "-D", str(data), "-U", "postgres", "--auth=trust"],
                   check=True, capture_output=True, env=env)
    (data/"postgresql.conf").write_text(
        f"port = {port}\nlisten_addresses = '127.0.0.1'\nunix_socket_directories = '{BASE}'\n"
        "fsync = off\nsynchronous_commit = off\nfull_page_writes = off\nmax_prepared_transactions = 200\n")
    log = open(BASE/f"{name}.log", "wb")
    subprocess.run([str(BIN/"pg_ctl"), "-D", str(data), "-l", str(BASE/f"{name}.log"), "-w", "start"],
                   check=True, capture_output=True, env=env)
    return f"host=127.0.0.1 port={port} user=postgres dbname=postgres"

import psycopg
ports = [free_port(), free_port()]
dsn_a = start("a", ports[0]); dsn_b = start("b", ports[1])
try:
    with psycopg.connect(dsn_a, autocommit=True) as c:
        c.execute("CREATE TABLE t(x int)")
        c.execute("INSERT INTO t VALUES (1)")
    with psycopg.connect(dsn_a) as c:
        c.execute("BEGIN"); c.execute("INSERT INTO t VALUES (2)")
        c.execute("PREPARE TRANSACTION 'gid-1'")
        c.commit()
    with psycopg.connect(dsn_a, autocommit=True) as c:
        rows = c.execute("SELECT gid, owner, database FROM pg_prepared_xacts").fetchall()
        print("prepared on A:", rows)
        c.execute("COMMIT PREPARED 'gid-1'")
        print("rows after commit:", c.execute("SELECT * FROM t ORDER BY x").fetchall())
    with psycopg.connect(dsn_b, autocommit=True) as c:
        print("B version:", c.execute("SHOW server_version").fetchone())
        print("prepared on B:", c.execute("SELECT count(*) FROM pg_prepared_xacts").fetchone())
    print("SMOKE OK")
finally:
    for name in ("a", "b"):
        subprocess.run([str(BIN/"pg_ctl"), "-D", str(BASE/name), "-m", "immediate", "stop"],
                       capture_output=True)
