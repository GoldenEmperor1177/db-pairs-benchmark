#!/usr/bin/env python3
"""Database benchmark: five engines on their own, four source -> target pairs and a file-analytics test, under equal resources.

    python bench.py        run everything, append the report to results.txt and the raw numbers to results.csv
    python bench.py down   remove the containers and their network

Engines   PostgreSQL, MariaDB, ClickHouse (containers) and DuckDB, SQLite (embedded: they run inside the Python process).
Pairs     PostgreSQL->DuckDB, MariaDB->ClickHouse, SQLite->DuckDB, PostgreSQL->ClickHouse.
Files     DuckDB on CSV and Parquet files directly, against loading them into PostgreSQL and against plain Python.
Fairness  every engine gets 2 physical cores (4 threads) and 2 GiB RAM; only the engines a test needs are running;
          the whole measurement is repeated LAB_ROUNDS times in a rotated order and reported as median and range.
Env       LAB_ORDERS (rows in orders, default 1000000), LAB_RUNS (warm query runs, default 5),
          LAB_ROUNDS (repeats of everything, default 3), KEEP=1 (leave containers up),
          LAB_DOCKER (container command, default docker, else podman; use "sudo podman" if rootless podman cannot pin CPUs).
"""
import copy
import csv
import gc
import hashlib
import os
import platform
import random
import re
import resource
import shutil
import statistics as st
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

os.environ["MARIADB_PYTHON_CONNECTOR"] = "binary"   # MariaDB's C connector, always: an inherited value must not select pure Python

import apsw                      # SQLite through apsw, which bundles the current SQLite release
import clickhouse_connect
import duckdb
import mariadb
import psycopg

ROOT = Path(__file__).resolve().parent
ORDERS = int(os.environ.get("LAB_ORDERS", "1000000"))
RUNS = int(os.environ.get("LAB_RUNS", "5"))
ROUNDS = int(os.environ.get("LAB_ROUNDS", "3"))
HOST, PGP, MYP, CHP = "127.0.0.1", 5434, 3307, 8124


def core_sets():
    """Three sets of 2 physical cores each, read from the machine's layout so a core's threads always stay together."""
    cores = {}
    for line in subprocess.run(["lscpu", "-p=CPU,CORE"], capture_output=True, text=True).stdout.splitlines():
        if line and not line.startswith("#"):
            cpu, core = map(int, line.split(","))
            cores.setdefault(core, set()).add(cpu)
    groups = [c for _, c in sorted(cores.items()) if c <= os.sched_getaffinity(0)]
    if len(groups) < 6:
        sys.exit(f"need at least 6 physical cores (2 for each side of a pair and 2 for the client), found {len(groups)}")
    return groups[0] | groups[1], groups[2] | groups[3], groups[4] | groups[5]

# Resource budget, the same for every engine: 2 physical cores (all their threads) and 2 GiB RAM.
CPU_A, CPU_B, CPU_CLIENT = core_sets()   # A: PostgreSQL, MariaDB, SQLite; B: ClickHouse, DuckDB; client: Python
THREADS, MEM_GB = len(CPU_A), 2
CPUS = {"postgres": CPU_A, "mariadb": CPU_A, "sqlite": CPU_A, "clickhouse": CPU_B, "duckdb": CPU_B}
SVC = {"postgres": "pg", "mariadb": "mariadb", "clickhouse": "clickhouse"}
ENGINES = ["postgres", "mariadb", "clickhouse", "duckdb", "sqlite"]
PAIRS = [("postgres", "duckdb"), ("mariadb", "clickhouse"), ("sqlite", "duckdb"), ("postgres", "clickhouse")]

TABLES = ["customers", "products", "orders"]
COLS = {"customers": ["id", "country", "segment"], "products": ["id", "category", "price"],
        "orders": ["id", "customer_id", "product_id", "qty", "amount", "status", "created_at"]}
CASTS = {"customers": (int, str, str), "products": (int, str, float),
         "orders": (int, int, int, int, float, str, str)}
ORDER_COLS = "id, customer_id, product_id, qty, amount, status, created_at"

# The analytic queries. {m} is the engine's own "start of month" expression, everything else is shared.
QUERIES = {
    "Q1 revenue by month, country": """
        SELECT {m} AS month, c.country, count(*) AS orders, sum(o.amount) AS revenue
        FROM orders o JOIN customers c ON c.id = o.customer_id
        WHERE o.status IN ('paid', 'shipped') GROUP BY 1, 2 ORDER BY 1, 2""",
    "Q2 top 3 products per category": """
        SELECT category, product_id, revenue FROM (
          SELECT category, product_id, revenue,
                 row_number() OVER (PARTITION BY category ORDER BY revenue DESC, product_id) AS rn
          FROM (SELECT p.category AS category, o.product_id AS product_id, sum(o.amount) AS revenue
                FROM orders o JOIN products p ON p.id = o.product_id
                WHERE o.status <> 'cancelled' GROUP BY p.category, o.product_id) a) t
        WHERE rn <= 3 ORDER BY category, rn""",
    "Q3 distinct customers, avg": """
        SELECT c.segment, o.status, count(DISTINCT o.customer_id) AS customers,
               round(avg(o.amount), 2) AS avg_amount, max(o.created_at) AS last_order
        FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY 1, 2 ORDER BY 1, 2""",
    "Q4 recent slice by status": """
        SELECT status, count(*) AS orders, sum(amount) AS revenue FROM orders
        WHERE created_at >= '2025-12-01 00:00:00' GROUP BY status ORDER BY status""",
}

TITLE = {"postgres": "PostgreSQL", "mariadb": "MariaDB", "clickhouse": "ClickHouse", "duckdb": "DuckDB", "sqlite": "SQLite"}
SHORT = {"postgres": "PG", "mariadb": "Maria", "sqlite": "SQLite", "duckdb": "DuckDB", "clickhouse": "CH"}
T0 = time.perf_counter()
ROUND = 0       # current round, stamped on every raw measurement
RAW = []        # every individual query run: (round, label, engine, query, run no., ms); run 0 is the first run


def say(*a):
    print(f"[{time.perf_counter() - T0:6.0f}s]", *a, flush=True)


def clock(f, *a):
    t = time.perf_counter()
    r = f(*a)
    return time.perf_counter() - t, r


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p))]


def pin(cpus):
    """Move every thread of this process (Python, DuckDB workers, client threads) onto these CPUs."""
    for tid in os.listdir("/proc/self/task"):
        try:
            os.sched_setaffinity(int(tid), cpus)
        except OSError:
            pass


def majflt():
    """Major page faults of this process: pages DuckDB/SQLite (or Python) had to read back from disk or swap."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_majflt


def reset_peak_rss():
    """Restart this process's peak-memory counter (VmHWM), so the next reading covers one test only."""
    try:
        with open("/proc/self/clear_refs", "w") as f:
            f.write("5")
    except OSError:
        pass


def peak_mb(e):
    """Most memory an engine used: the container's cgroup peak (it is started fresh for each test, and the
    figure includes its page cache), or the peak resident memory of this process for an embedded engine."""
    if e in SVC:
        r = ctr("exec", CONTAINERS[e][0], "cat", "/sys/fs/cgroup/memory.peak", check=False).stdout.strip()
        return int(r) / 1e6 if r.isdigit() else None
    kb = next((int(l.split()[1]) for l in open("/proc/self/status") if l.startswith("VmHWM")), None)
    return kb * 1024 / 1e6 if kb else None


def swap_pages():
    v = dict(l.split() for l in open("/proc/vmstat"))
    return int(v["pswpin"]) + int(v["pswpout"])


# ---------- test data: one set of CSV files, loaded into every engine ----------

def make_csvs(d):
    r = random.Random(42)
    nc, npr = max(1000, ORDERS // 10), 10000
    countries = ["US", "GB", "DE", "FR", "IN", "BR", "JP", "CA", "AU", "NG"]
    segments = ["consumer", "smb", "enterprise"]
    cats = ["books", "games", "tools", "toys", "food", "garden", "sports", "music"]
    status = ["paid", "paid", "paid", "shipped", "refunded", "cancelled"]
    prices = [r.randrange(100, 50000) for _ in range(npr)]
    base = datetime(2023, 1, 1)
    with open(d / "customers.csv", "w", newline="") as f:
        csv.writer(f, lineterminator="\n").writerows((i, r.choice(countries), r.choice(segments)) for i in range(1, nc + 1))
    with open(d / "products.csv", "w", newline="") as f:
        csv.writer(f, lineterminator="\n").writerows((i, r.choice(cats), f"{prices[i - 1] / 100:.2f}") for i in range(1, npr + 1))
    with open(d / "orders.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        for i in range(1, ORDERS + 1):
            p, q = r.randint(1, npr), r.randint(1, 5)
            ts = (base + timedelta(seconds=r.randrange(94608000))).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow((i, r.randint(1, nc), p, q, f"{prices[p - 1] * q / 100:.2f}", r.choice(status), ts))
    return {"customers": nc, "products": npr, "orders": ORDERS}


# ---------- engine adapters: same methods, different databases ----------

def make_ddl(I, D, S, TS, pk="", tail=""):
    return {"customers": f"CREATE TABLE customers (id {I}{pk}, country {S}, segment {S}){tail}",
            "products": f"CREATE TABLE products (id {I}{pk}, category {S}, price {D}){tail}",
            "orders": f"CREATE TABLE orders (id {I}{pk}, customer_id {I}, product_id {I}, qty {I}, "
                      f"amount {D}, status {S}, created_at {TS}){tail}"}


class DB:
    idx, analyze = [], []
    scratch_sql = "CREATE TABLE scratch (id INTEGER PRIMARY KEY, v VARCHAR(10))"
    update_n = 200

    def lookup(s, i): s.q(f"SELECT * FROM orders WHERE id = {i}")
    def ping(s, _): s.q("SELECT 1")
    def ins1(s, i): s.x(f"INSERT INTO scratch VALUES ({i}, 'x')")
    def insn(s, a, b): s.x("INSERT INTO scratch VALUES " + ",".join(f"({i}, 'x')" for i in range(a, b)))
    def upd1(s, i): s.x(f"UPDATE scratch SET v = 'y' WHERE id = {i}")
    def reset(s): pass
    def new(s): return type(s)()

    def close(s):
        try:
            s.con.close()
        except Exception:
            pass

    def make_sync(s):      # 200k-row table the sync tests may change without touching 'orders'
        s.x("DROP TABLE IF EXISTS orders_sync")
        s.x("CREATE TABLE orders_sync AS SELECT * FROM orders WHERE id <= 200000")
        s.x("CREATE UNIQUE INDEX orders_sync_pk ON orders_sync (id)")

    def insert_new(s, k, base):
        s.x(f"INSERT INTO orders_sync SELECT id + {base}, customer_id, product_id, qty, amount, status, created_at "
            f"FROM orders_sync WHERE id <= {k}")

    def change_some(s):
        s.x("UPDATE orders_sync SET status = 'updated' WHERE id <= 1000")
        s.x("DELETE FROM orders_sync WHERE id BETWEEN 1001 AND 2000")

    def drop_sync(s): s.x("DROP TABLE IF EXISTS orders_sync")


class PG(DB):
    name = "postgres"
    month = "CAST(date_trunc('month', o.created_at) AS date)"
    ddl = make_ddl("integer", "numeric(12,2)", "text", "timestamp")
    idx = ["ALTER TABLE customers ADD PRIMARY KEY (id)", "ALTER TABLE products ADD PRIMARY KEY (id)",
           "ALTER TABLE orders ADD PRIMARY KEY (id)", "CREATE INDEX orders_customer_idx ON orders (customer_id)",
           "CREATE INDEX orders_created_idx ON orders (created_at)"]
    analyze = ["ANALYZE"]

    def __init__(s): s.con = psycopg.connect(f"host={HOST} port={PGP} user=lab password=lab dbname=lab", autocommit=True)
    def q(s, sql): return s.con.execute(sql).fetchall()
    def x(s, sql): s.con.execute(sql)
    def version(s): return "PostgreSQL " + s.q("SHOW server_version")[0][0]
    def reset(s): s.x("DROP TABLE IF EXISTS customers, products, orders, scratch, orders_sync CASCADE")

    def load(s, t, path):
        with s.con.cursor().copy(f"COPY {t} FROM STDIN (FORMAT csv)") as cp, open(path, "rb") as f:
            while b := f.read(1 << 20):
                cp.write(b)

    def size(s):
        return s.q("SELECT " + " + ".join(f"pg_total_relation_size('{t}')" for t in TABLES))[0][0]


class Maria(DB):
    name = "mariadb"
    # start of month without string functions (a reviewer's suggestion)
    month = "DATE_ADD(LAST_DAY(o.created_at - INTERVAL 1 MONTH), INTERVAL 1 DAY)"
    ddl = make_ddl("INT", "DECIMAL(12,2)", "VARCHAR(32)", "DATETIME")
    idx = PG.idx
    analyze = ["ANALYZE TABLE customers, products, orders"]

    def __init__(s):
        s.con = mariadb.connect(host=HOST, port=MYP, user="lab", password="lab", database="lab",
                                autocommit=True, local_infile=True)
        s.cur = s.con.cursor()

    def q(s, sql):
        s.cur.execute(sql)
        return s.cur.fetchall()

    def x(s, sql): s.cur.execute(sql)
    def version(s): return "MariaDB " + s.q("SELECT version()")[0][0].split("-")[0]
    def reset(s): s.x("DROP TABLE IF EXISTS customers, products, orders, scratch, orders_sync")

    def load(s, t, path):
        # InnoDB's bulk-load path: no per-row unique or foreign-key checks while loading into an empty table
        s.x("SET unique_checks = 0")
        s.x("SET foreign_key_checks = 0")
        s.x(f"LOAD DATA LOCAL INFILE '{path}' INTO TABLE {t} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'")
        s.x("SET unique_checks = 1")
        s.x("SET foreign_key_checks = 1")

    def size(s):
        return int(s.q("SELECT sum(data_length + index_length) FROM information_schema.tables "
                       "WHERE table_schema = 'lab' AND table_name IN ('customers','products','orders')")[0][0])


class CH(DB):
    name = "clickhouse"
    month = "toStartOfMonth(o.created_at)"
    ddl = make_ddl("Int32", "Decimal(12,2)", "String", "DateTime", tail=" ENGINE = MergeTree ORDER BY id")
    scratch_sql = "CREATE TABLE scratch (id Int32, v String) ENGINE = MergeTree ORDER BY id"
    update_n = 30

    def __init__(s, database="lab"):
        s.db = database
        s.con = clickhouse_connect.get_client(
            host=HOST, port=CHP, username="lab", password="lab", database=database,
            settings={"max_threads": THREADS, "mysql_datatypes_support_level": "decimal,datetime64",
                      # ClickHouse 26 buffers client INSERTs by default and makes each one wait for a flush (~50 ms);
                      # off, so an INSERT is complete when it returns, as in every other engine
                      "async_insert": 0})

    def q(s, sql): return s.con.query(sql).result_rows
    def x(s, sql): s.con.command(sql)
    def new(s): return CH(s.db)
    def version(s): return "ClickHouse " + s.q("SELECT version()")[0][0]
    def upd1(s, i): s.x(f"ALTER TABLE scratch UPDATE v = 'y' WHERE id = {i} SETTINGS mutations_sync = 1")
    def reset(s): [s.x(f"DROP TABLE IF EXISTS {t}") for t in TABLES + ["scratch", "orders_sync"]]

    def load(s, t, path):
        s.con.raw_insert(t, column_names=COLS[t], insert_block=Path(path).read_bytes(), fmt="CSV")

    def size(s, db="lab"):
        return int(s.q(f"SELECT sum(bytes_on_disk) FROM system.parts WHERE database = '{db}' AND active "
                       "AND table IN ('customers','products','orders')")[0][0])


class Duck(DB):
    name = "duckdb"
    month = "CAST(date_trunc('month', CAST(o.created_at AS TIMESTAMP)) AS DATE)"
    ddl = make_ddl("INTEGER", "DECIMAL(12,2)", "VARCHAR", "TIMESTAMP", pk=" PRIMARY KEY")

    def __init__(s, path, attach=None, use=False):
        s.path = str(path)
        cfg = {"threads": THREADS, "memory_limit": f"{MEM_GB}GiB"}     # the same 2 GiB as the containers' --memory 2g
        if EXT_DIR:                        # extensions go to the run's temporary folder, not ~/.duckdb
            cfg["extension_directory"] = str(EXT_DIR)
        s.con = duckdb.connect(s.path, config=cfg)
        if attach:
            pg = attach.name == "postgres"
            s.con.execute(f"INSTALL {'postgres' if pg else 'sqlite'}")
            s.con.execute(f"LOAD {'postgres' if pg else 'sqlite'}")
            src = f"host={HOST} port={PGP} user=lab password=lab dbname=lab" if pg else attach.path
            s.con.execute(f"ATTACH '{src}' AS s (TYPE {'postgres' if pg else 'sqlite'}, READ_ONLY)")
            if use:
                s.con.execute("USE s.public" if pg else "USE s.main")

    def q(s, sql): return s.con.execute(sql).fetchall()
    def x(s, sql): s.con.execute(sql)
    def version(s): return "DuckDB " + duckdb.__version__
    def load(s, t, path): s.x(f"COPY {t} FROM '{path}' (FORMAT csv)")
    def new(s): d = copy.copy(s); d.con = s.con.cursor(); return d

    def size(s):
        s.x("CHECKPOINT")
        return os.path.getsize(s.path)


class SQLite(DB):
    name = "sqlite"
    month = "strftime('%Y-%m-01', o.created_at)"
    ddl = make_ddl("INTEGER", "REAL", "TEXT", "TEXT", pk=" PRIMARY KEY")
    idx = ["CREATE INDEX orders_customer_idx ON orders (customer_id)", "CREATE INDEX orders_created_idx ON orders (created_at)"]
    analyze = ["ANALYZE"]

    def __init__(s, path):
        s.path = str(path)
        s.con = apsw.Connection(s.path)                 # autocommit unless BEGIN; SQLite's default synchronous=FULL
        # 1 GiB page cache, like PostgreSQL's and MariaDB's main cache. It is per connection (the 8-thread test opens
        # 8), but a cache never holds more than the file, and the file is ~95 MB, so the total stays far below 1 GiB.
        s.con.execute("PRAGMA cache_size = -1048576")

    def q(s, sql): return s.con.execute(sql).fetchall()
    def x(s, sql): s.con.execute(sql)
    def version(s): return "SQLite " + apsw.sqlite_lib_version()
    def new(s): return SQLite(s.path)
    def size(s): return os.path.getsize(s.path)

    def load(s, t, path):
        s.x("BEGIN")
        with open(path, newline="") as f:
            rows = csv.reader(f)
            while True:
                chunk = [tuple(c(v) for c, v in zip(CASTS[t], r)) for _, r in zip(range(50000), rows)]
                if not chunk:
                    break
                s.con.executemany(f"INSERT INTO {t} VALUES ({','.join('?' * len(CASTS[t]))})", chunk)
        s.x("COMMIT")


# ---------- running only what a test needs ----------

RUNTIME = os.environ.get("LAB_DOCKER") or ("docker" if shutil.which("docker") else "podman")
NET = "hybridlab"
MARIA_ENV = {"MARIADB_ROOT_PASSWORD": "lab", "MARIADB_DATABASE": "lab", "MARIADB_USER": "lab", "MARIADB_PASSWORD": "lab"}
# text compares byte by byte in every engine (PostgreSQL C.UTF-8; DuckDB, ClickHouse and SQLite do it by default)
MARIA_ARGS = ["--local-infile=1", "--character-set-server=utf8mb4", "--collation-server=utf8mb4_bin"]
# engine: (container, network alias, image, host port, container port, environment, extra options, command)
CONTAINERS = {
    "postgres": ("hybridlab-pg", "pg", "docker.io/library/postgres:18.6", PGP, 5432,
                 {"POSTGRES_USER": "lab", "POSTGRES_PASSWORD": "lab", "POSTGRES_DB": "lab",
                  "POSTGRES_INITDB_ARGS": "--encoding=UTF8 --locale=C.UTF-8"}, ["--shm-size=512m"],
                 ["postgres", "-c", "shared_buffers=1GB", "-c", "effective_cache_size=1536MB", "-c", "work_mem=64MB",
                  "-c", "max_worker_processes=8", "-c", f"max_parallel_workers={THREADS}",
                  "-c", f"max_parallel_workers_per_gather={THREADS}", "-c", "random_page_cost=1.1"]),
    "mariadb": ("hybridlab-mariadb", "mariadb", "docker.io/library/mariadb:13.0.2", MYP, 3306, MARIA_ENV, [],
                MARIA_ARGS + ["--innodb-buffer-pool-size=1G", "--innodb-log-file-size=2G"]),
    "clickhouse": ("hybridlab-clickhouse", "clickhouse", "docker.io/clickhouse/clickhouse-server:26.9.24.8", CHP, 8123,
                   {"CLICKHOUSE_DB": "lab", "CLICKHOUSE_USER": "lab", "CLICKHOUSE_PASSWORD": "lab",
                    "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}, ["--ulimit", "nofile=262144:262144"], []),
}


def ctr(*args, check=True):
    """Run a docker/podman command; both understand the same commands used here."""
    r = subprocess.run(RUNTIME.split() + list(args), capture_output=True, text=True)
    if check and r.returncode:
        sys.exit(f"'{RUNTIME} {' '.join(args[:2])}' failed:\n{r.stderr.strip()}")
    return r


def start(e):
    name, alias, image, host_port, port, env, extra, cmd = CONTAINERS[e]
    if ctr("container", "inspect", name, check=False).returncode == 0:
        ctr("start", name)
        return
    if ctr("network", "inspect", NET, check=False).returncode:
        ctr("network", "create", NET)
    ctr("run", "-d", "--name", name, "--network", NET, "--network-alias", alias, "-p", f"127.0.0.1:{host_port}:{port}",
        "--cpuset-cpus", ",".join(map(str, sorted(CPUS[e]))), "--memory", f"{MEM_GB}g", "--memory-swap", f"{MEM_GB}g",
        "--security-opt", "seccomp=unconfined",       # the default seccomp profile blocks io_uring; allowed for all equally
        *[x for k, v in env.items() for x in ("-e", f"{k}={v}")], *extra, image, *cmd)


def only(*engines):
    """Leave running exactly the containers of these engines, stop the others."""
    for e, (name, *_) in CONTAINERS.items():
        if e not in engines and ctr("container", "inspect", name, check=False).returncode == 0:
            ctr("stop", "-t", "30", name, check=False)
    for e in engines:
        if e in CONTAINERS:
            start(e)


def remove_all():
    for name, *_ in CONTAINERS.values():
        ctr("rm", "-f", "-v", name, check=False)
    ctr("network", "rm", NET, check=False)


def ready(make, timeout=120):
    """Connect once a real query works: a container can pass its health check while it is still initialising."""
    end = time.time() + timeout
    while True:
        try:
            db = make()
            db.q("SELECT 1")
            return db
        except Exception:
            if time.time() > end:
                raise
            time.sleep(1)


def open_engine(e, work):
    if e in SVC:
        return ready({"postgres": PG, "mariadb": Maria, "clickhouse": CH}[e])
    return Duck(work / f"lab_{ROUND}.duckdb") if e == "duckdb" else SQLite(work / f"lab_{ROUND}.sqlite")


def client_cpus(e):
    """Embedded engines run inside this process, so the process sits on the engine's cores."""
    return CPU_CLIENT if e in SVC else CPUS[e]


# ---------- measuring ----------

def norm(rows):
    return [tuple(float(v) if isinstance(v, (int, float, Decimal)) and not isinstance(v, bool) else str(v)[:19]
                  for v in r) for r in rows]


def agree(a, b):
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        for x, y in zip(ra, rb):
            if isinstance(x, float):
                if not isinstance(y, float) or abs(x - y) > max(0.011, 1e-6 * abs(x)):
                    return False
            elif x != y:
                return False
    return True


def run_queries(db, runs=RUNS, label=""):
    """-> {query: (first_ms, median_ms)}, {query: normalised answer}"""
    res, ans = {}, {}
    for name, sql in QUERIES.items():
        sql = sql.format(m=db.month)
        times = []
        gc.collect()
        for _ in range(runs + 1):
            dt, rows = clock(db.q, sql)
            times.append(dt * 1000)
            RAW.append((ROUND, label, db.name, name, len(times) - 1, dt * 1000))
        res[name] = (times[0], st.median(times[1:]))
        ans[name] = norm(rows)
    return res, ans


def lat(f, items):
    gc.collect()
    v = []
    for i in items:
        t = time.perf_counter()
        f(i)
        v.append((time.perf_counter() - t) * 1000)
    return {"med": st.median(v), "p95": pct(v, 0.95), "ops": 1000 / st.mean(v)}


def concurrent_qps(db, ids, threads=8, per=100):
    conns = [db.new() for _ in range(threads)]

    def work(k):
        for i in ids[k * per:(k + 1) * per]:
            conns[k].lookup(i)
    ts = [threading.Thread(target=work, args=(k,)) for k in range(threads)]
    gc.collect()
    t0 = time.perf_counter()
    [t.start() for t in ts]
    [t.join() for t in ts]
    dt = time.perf_counter() - t0
    [c.close() for c in conns]
    return threads * per / dt


def ops(db, m):
    """Count, round trip, point lookups, concurrency, inserts and updates. Run on every engine and every synced copy."""
    r = random.Random(1)
    ids = r.sample(range(1, ORDERS + 1), 800)
    m["count_ms"] = st.median(clock(db.q, "SELECT count(*) FROM orders")[0] * 1000 for _ in range(RUNS))
    m["ping"] = lat(db.ping, range(300))
    m["lookup"] = lat(db.lookup, ids[:300])
    m["qps8"] = concurrent_qps(db, ids)
    db.x(db.scratch_sql)
    gc.collect()
    m["batch_rows_s"] = st.median(10000 / clock(db.insn, a, a + 10000)[0] for a in (1, 10001, 20001))
    m["insert"] = lat(db.ins1, range(30001, 30301))
    m["update"] = lat(db.upd1, r.sample(range(1, 30001), db.update_n))
    db.x("DROP TABLE scratch")


def baseline(e, work, counts):
    only()          # stop everything first, so the engine starts in a new cgroup and memory.peak covers this test only
    only(e)
    pin(client_cpus(e))
    db = open_engine(e, work)
    say(f"round {ROUND + 1}/{ROUNDS}  baseline {TITLE[e]}")
    sw, mf = swap_pages(), majflt()
    reset_peak_rss()
    m = {"version": db.version()}
    db.reset()
    for sql in db.ddl.values():
        db.x(sql)
    gc.collect()
    m["load_s"] = sum(clock(db.load, t, work / f"{t}.csv")[0] for t in TABLES)
    m["load_rows_s"] = sum(counts.values()) / m["load_s"]
    m["index_s"] = clock(lambda: [db.x(s) for s in db.idx])[0] if db.idx else None
    [db.x(s) for s in db.analyze]
    m["size_mb"] = db.size() / 1e6
    m["q"], answers = run_queries(db, label="baseline")
    ops(db, m)
    m["swap"], m["majflt"] = swap_pages() - sw, majflt() - mf
    m["peak_mb"] = peak_mb(e)
    db.close()
    return m, answers


def pull_and_copy(src, kind, copy_db, fed_name):
    """Return full(t) and incremental(t) closures for one pair."""
    def pull(t, where=""):
        if kind == "duckdb":
            ref = "s." + ("public." if src.name == "postgres" else "") + t
            cols = ORDER_COLS.replace("created_at", "CAST(created_at AS TIMESTAMP) AS created_at") \
                if src.name == "sqlite" and t.startswith("orders") else "*"
        else:
            ref, cols = f"{fed_name}.{t}", "*"
        return f"SELECT {cols} FROM {ref}" + (f" WHERE {where}" if where else "")

    def full(t):
        if kind == "duckdb":
            copy_db.x(f"CREATE OR REPLACE TABLE {t} AS {pull(t)}")
        else:
            copy_db.x(f"TRUNCATE TABLE {t}")
            copy_db.x(f"INSERT INTO {t} {pull(t)}")

    def incr(t):
        wm = copy_db.q(f"SELECT max(id) FROM {t}")[0][0]
        copy_db.x(f"INSERT INTO {t} {pull(t, f'id > {wm}')}")
    return full, incr


def run_pair(s, kind, work, src_m, ref):
    only(s, kind)
    pin(client_cpus(kind))     # a DuckDB target runs in this process, on DuckDB's cores
    src = ready(PG) if s == "postgres" else ready(Maria) if s == "mariadb" else SQLite(work / f"lab_{ROUND}.sqlite")
    if kind == "clickhouse":
        ready(CH).close()
    say(f"round {ROUND + 1}/{ROUNDS}  pair {TITLE[s]} -> {TITLE[kind]}")
    for sql in QUERIES.values():            # warm the source's cache after its restart, untimed
        src.q(sql.format(m=src.month))
    sw, mf = swap_pages(), majflt()
    P = {}
    tag = f"{s}_{ROUND}"
    if kind == "duckdb":
        copy_path = work / f"sync_{tag}.duckdb"
        copy_db = Duck(copy_path, attach=src)
        fed = Duck(":memory:", attach=src, use=True)
        fed_name = None
    else:
        admin = CH("lab")
        copy_name, fed_name = f"sync_{tag}", f"fed_{tag}"
        engine = ("PostgreSQL('pg:5432', 'lab', 'lab', 'lab')" if s == "postgres"
                  else "MySQL('mariadb:3306', 'lab', 'lab', 'lab')")
        for d in (copy_name, fed_name):
            admin.x(f"DROP DATABASE IF EXISTS {d}")
        admin.x(f"CREATE DATABASE {copy_name}")
        admin.x(f"CREATE DATABASE {fed_name} ENGINE = {engine}")
        copy_db, fed = CH(copy_name), CH(fed_name)
        for sql in CH.ddl.values():
            copy_db.x(sql)
        copy_db.x("CREATE TABLE orders_sync AS orders")
    full, incr = pull_and_copy(src, kind, copy_db, fed_name)

    # A. full copy of the three tables
    P["copy"] = {}
    gc.collect()
    for t in TABLES:
        dt, _ = clock(full, t)
        P["copy"][t] = (copy_db.q(f"SELECT count(*) FROM {t}")[0][0], dt)
    P["copy_s"] = sum(v[1] for v in P["copy"].values())
    P["copy_mb"] = (copy_db.size() if kind == "duckdb" else copy_db.size(copy_name)) / 1e6

    # B. the four queries on the copy and live through the source, then the baseline tests on the copy
    P["q_src"] = src_m["q"]
    P["q_fed"], ans_f = run_queries(fed, runs=2, label=f"{s}->{kind} live via source")
    P["q_copy"], ans_c = run_queries(copy_db, label=f"{s}->{kind} synced copy")
    P["ops"] = {}
    ops(copy_db, P["ops"])
    P["agree"] = {n: agree(ref[n], ans_c[n]) and agree(ref[n], ans_f[n]) for n in QUERIES}
    gain = sum(v[1] for v in P["q_src"].values()) - sum(v[1] for v in P["q_copy"].values())
    P["breakeven"] = P["copy_s"] * 1000 / gain if gain > 0 else None   # rounds of the 4 queries

    # C. change propagation on a 200k-row table
    src.make_sync()
    full("orders_sync")
    P["lag"] = {}
    for k, b in [(1, 10_000_000), (1000, 20_000_000), (50000, 30_000_000)]:
        t_ins, _ = clock(src.insert_new, k, b)
        t_sync, _ = clock(incr, "orders_sync")
        same = src.q("SELECT count(*) FROM orders_sync")[0][0] == copy_db.q("SELECT count(*) FROM orders_sync")[0][0]
        P["lag"][k] = (t_ins * 1000, t_sync * 1000, same)
    src.change_some()
    incr("orders_sync")
    seen = lambda d: (d.q("SELECT count(*) FROM orders_sync WHERE status = 'updated'")[0][0],
                      d.q("SELECT count(*) FROM orders_sync WHERE id BETWEEN 1001 AND 2000")[0][0])
    P["after_incr"] = seen(copy_db)
    P["refresh_s"], _ = clock(full, "orders_sync")
    P["after_refresh"] = seen(copy_db)
    P["swap"], P["majflt"] = swap_pages() - sw, majflt() - mf

    src.drop_sync()
    if kind == "duckdb":
        for d in (copy_db, fed):
            d.close()
        copy_path.unlink(missing_ok=True)
    else:
        for d in (copy_name, fed_name):
            admin.x(f"DROP DATABASE IF EXISTS {d}")
        [d.close() for d in (copy_db, fed, admin)]
    src.close()
    return P


# ---------- file analytics: CSV and Parquet files queried directly, the way an application might use DuckDB ----------

FILE_RUNS = 2       # warm runs per task after the first; plain Python re-reads the whole file on every run
FILE_COLS = ("{'id': 'INTEGER', 'customer_id': 'INTEGER', 'product_id': 'INTEGER', 'qty': 'INTEGER', "
             "'amount': 'DECIMAL(12,2)', 'status': 'VARCHAR', 'created_at': 'TIMESTAMP'}")
FILE_TASKS = {
    "F1 filter + aggregate": "SELECT status, count(*), sum(amount) FROM {t} "
                             "WHERE created_at >= '2025-01-01 00:00:00' GROUP BY status ORDER BY status",
    "F2 top 10 customers": "SELECT customer_id, sum(amount) AS revenue FROM {t} "
                           "GROUP BY customer_id ORDER BY revenue DESC, customer_id LIMIT 10",
    "F3 filter + export CSV": "SELECT * FROM {t} WHERE status = 'refunded' AND amount > 1000 ORDER BY id",
}
FILE_ORDERS = int(os.environ.get("LAB_FILE_ORDERS", "5000000"))   # rows in the file for section 4, about 270 MB
FILE_WAYS = ["postgres", "duck_table", "duck_csv", "duck_parquet", "python"]
FILE_TITLE = {"postgres": "PostgreSQL (after load)", "duck_table": "DuckDB table (after import)",
              "duck_csv": "DuckDB on the CSV", "duck_parquet": "DuckDB on Parquet", "python": "Python csv module"}


def make_file(path, n):
    """A larger orders file for the file tests, same columns and value ranges as the orders table."""
    r = random.Random(7)
    status = ["paid", "paid", "paid", "shipped", "refunded", "cancelled"]
    base = datetime(2023, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        for i in range(1, n + 1):
            q = r.randint(1, 5)
            ts = (base + timedelta(seconds=r.randrange(94608000))).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow((i, r.randint(1, 100000), r.randint(1, 10000), q, f"{r.randrange(100, 50000) * q / 100:.2f}",
                        r.choice(status), ts))


def export_summary(path):
    """(rows, SHA-256 of every field of every row) of an exported CSV, read back untimed, so every way is checked
    the same way. Numbers are normalised first (amount in whole cents), so only real differences change the digest."""
    n, h = 0, hashlib.sha256()
    with open(path, newline="") as f:
        for r in csv.reader(f):
            n += 1
            h.update(f"{int(r[0])}|{int(r[1])}|{int(r[2])}|{int(r[3])}|{round(float(r[4]) * 100)}|{r[5]}|{r[6][:19]}\n".encode())
    return [(float(n), h.hexdigest())]


def python_task(task, path, out):
    """The same three tasks in plain Python, reading the CSV row by row like an application script would."""
    def rows():
        with open(path, newline="") as f:
            yield from csv.reader(f)
    if task.startswith("F1"):
        agg = {}
        for r in rows():
            if r[6] >= "2025-01-01 00:00:00":
                a = agg.setdefault(r[5], [0, 0.0])
                a[0] += 1
                a[1] += float(r[4])
        return sorted((k, v[0], v[1]) for k, v in agg.items())
    if task.startswith("F2"):
        rev = {}
        for r in rows():
            rev[int(r[1])] = rev.get(int(r[1]), 0.0) + float(r[4])
        return sorted(rev.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    with open(out, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        for r in rows():
            if r[5] == "refunded" and float(r[4]) > 1000:
                w.writerow(r)


def files_test(work):
    say(f"round {ROUND + 1}/{ROUNDS}  files: {FILE_ORDERS:,}-row CSV via PostgreSQL, DuckDB and plain Python")
    path, pq, dkfile = work / "file_orders.csv", work / "file_orders.parquet", work / "files.duckdb"
    sw, mf = swap_pages(), majflt()
    F = {"t": {w: {} for w in FILE_WAYS}, "ans": {w: {} for w in FILE_WAYS}, "prep": {}}

    def measure(way, task, run_once, out):
        times = []
        gc.collect()
        for _ in range(FILE_RUNS + 1):
            dt, res = clock(run_once)
            times.append(dt * 1000)
            RAW.append((ROUND, "files", way, task, len(times) - 1, dt * 1000))
        F["t"][way][task] = (times[0], st.median(times[1:]))
        F["ans"][way][task] = export_summary(out) if task.startswith("F3") else norm(res)

    # PostgreSQL: the file has to be loaded into a table first (no indexes, like a fresh import)
    only("postgres")
    pin(CPU_CLIENT)
    pg = ready(PG)
    pg.x("DROP TABLE IF EXISTS files_orders")
    pg.x(PG.ddl["orders"].replace("TABLE orders", "TABLE files_orders"))
    F["prep"]["pg_load_s"], _ = clock(pg.load, "files_orders", path)
    pg.x("ANALYZE files_orders")
    for task, sql in FILE_TASKS.items():
        q, out = sql.format(t="files_orders"), work / f"out_pg_{task[:2]}.csv"
        if task.startswith("F3"):
            def run(q=q, out=out):
                with open(out, "wb") as f, pg.con.cursor().copy(f"COPY ({q}) TO STDOUT (FORMAT csv)") as cp:
                    for chunk in cp:
                        f.write(chunk)
        else:
            run = lambda q=q: pg.q(q)
        measure("postgres", task, run, out)
    pg.x("DROP TABLE files_orders")
    pg.close()
    only()

    # DuckDB, embedded, on its own cores: straight on the CSV, then on a Parquet copy of it
    pin(CPU_B)
    dkfile.unlink(missing_ok=True)
    dk = Duck(dkfile)
    dk.x(f"CREATE VIEW csv_orders AS SELECT * FROM read_csv('{path}', header = false, columns = {FILE_COLS})")
    F["prep"]["duck_import_s"], _ = clock(dk.x, "CREATE TABLE table_orders AS SELECT * FROM csv_orders")
    dk.x("CHECKPOINT")
    F["prep"]["parquet_s"], _ = clock(dk.x, f"COPY (SELECT * FROM csv_orders) TO '{pq}' (FORMAT parquet)")
    dk.x(f"CREATE VIEW parquet_orders AS SELECT * FROM read_parquet('{pq}')")
    F["prep"]["csv_mb"], F["prep"]["parquet_mb"] = path.stat().st_size / 1e6, pq.stat().st_size / 1e6
    for way, view in [("duck_table", "table_orders"), ("duck_csv", "csv_orders"), ("duck_parquet", "parquet_orders")]:
        for task, sql in FILE_TASKS.items():
            q, out = sql.format(t=view), work / f"out_{way}_{task[:2]}.csv"
            run = (lambda q=q, out=out: dk.x(f"COPY ({q}) TO '{out}' (FORMAT csv, HEADER false)")) \
                if task.startswith("F3") else (lambda q=q: dk.q(q))
            measure(way, task, run, out)
    dk.close()

    # plain Python, single-threaded, on the row-store cores
    pin(CPU_A)
    for task in FILE_TASKS:
        out = work / f"out_py_{task[:2]}.csv"
        measure("python", task, lambda task=task, out=out: python_task(task, path, out), out)

    F["agree"] = {w: {t: agree(F["ans"]["postgres"][t], F["ans"][w][t]) for t in FILE_TASKS} for w in FILE_WAYS[1:]}
    F["swap"], F["majflt"] = swap_pages() - sw, majflt() - mf
    for f in list(work.glob("out_*.csv")) + [pq, dkfile]:
        f.unlink(missing_ok=True)
    return F


# ---------- the report: every number is the median over the rounds, with its range ----------

def agg(vals):
    vals = [v for v in vals if v is not None]
    return (st.median(vals), min(vals), max(vals)) if vals else None


def show(a, fmt):
    med, lo, hi = a
    spread = (hi - lo) / 2 / med * 100 if med and ROUNDS > 1 else 0
    return fmt.format(med) + (f" ±{spread:.0f}%" if ROUNDS > 1 and hi != lo else "")


def mat(title, cols, rows, first="metric"):
    """rows: (label, per-cell lists of per-round values, format, lower_is_better|None).
    '*' = clear winner (its range across rounds overlaps no other value's range); '~' = best values that overlap (a tie)."""
    out = ["", title, "-" * len(title)]
    cells = []
    for label, vals, fmt, low in rows:
        A = [agg(v) if isinstance(v, list) else v for v in vals]
        mark = [""] * len(A)
        nums = [(i, a) for i, a in enumerate(A) if isinstance(a, tuple)]
        if low is not None and len(nums) > 1 and len({a[0] for _, a in nums}) > 1:
            bi, b = sorted(nums, key=lambda x: x[1][0], reverse=not low)[0]
            ties = [i for i, a in nums if i != bi and a[1] <= b[2] and a[2] >= b[1]]
            for i in [bi] + ties:
                mark[i] = " ~" if ties else " *"
        cells.append([label] + ["n/a" if a is None else (a if isinstance(a, str) else show(a, fmt[i] if isinstance(fmt, list) else fmt)) + mark[i]
                                for i, a in enumerate(A)])
    head = [first] + cols
    w = [max(len(r[i]) for r in [head] + cells) for i in range(len(head))]
    line = lambda r: "  ".join(c.ljust(w[i]) if i == 0 else c.rjust(w[i]) for i, c in enumerate(r))
    return out + [line(head), "  ".join("-" * x for x in w)] + [line(r) for r in cells]


def wrap(lines, width=100):
    """Wrap long lines; a continuation line starts under the text column ('label   text...')."""
    out = []
    for l in lines:
        if len(l) <= width:
            out.append(l)
            continue
        m = re.match(r"\s*\S.*?\s{2,}(?=\S)", l)
        hang = len(m.group(0)) if m else len(l) - len(l.lstrip())
        out += textwrap.wrap(l, width, subsequent_indent=" " * hang, break_long_words=False)
    return out


def cpus(c):
    """{0, 1, 8, 9} -> '0-1,8-9'"""
    c, out = sorted(c), []
    for x in c:
        if out and x == out[-1][1] + 1:
            out[-1][1] = x
        else:
            out.append([x, x])
    return ",".join(f"{a}-{b}" if a != b else f"{a}" for a, b in out)


def spec(counts):
    o = ["", "TEST SPECIFICATION", "------------------",
         "commands   python bench.py   (starts and stops the containers itself; python bench.py down removes them)",
         f"data       synthetic shop, random seed 42, same CSV files loaded into every engine: customers {counts['customers']:,}, "
         f"products {counts['products']:,}, orders {counts['orders']:,}",
         "           orders(id, customer_id, product_id, qty 1-5, amount, status, created_at over 3 years); amount = price x qty",
         "           status mix: 50% paid, 17% shipped, 17% refunded, 17% cancelled",
         f"fairness   every engine: 2 physical cores = {THREADS} threads, {MEM_GB} GiB RAM budget (a hard cap for containers only), no swap, 1 GiB main cache",
         f"           CPUs {cpus(CPU_A)}: PostgreSQL, MariaDB, SQLite; CPUs {cpus(CPU_B)}: ClickHouse, DuckDB; "
         f"CPUs {cpus(CPU_CLIENT)}: the Python client for server engines",
         "           containers: cpuset + memory limit from the container runtime; DuckDB: threads=4, memory_limit=2GiB; "
         "SQLite: single-threaded, cache_size 1 GiB; embedded engines run in the client process, pinned to their own cores",
         "           PostgreSQL: shared_buffers 1 GiB, 4 parallel workers; MariaDB: buffer pool 1 GiB, redo log 2 GiB, loads with "
         "unique_checks=0 and foreign_key_checks=0; ClickHouse: max_threads 4, async_insert=0 (on by default since 26.x)",
         "           text compares byte by byte everywhere (MariaDB utf8mb4_bin, PostgreSQL C.UTF-8); containers run with "
         "seccomp unconfined so io_uring is allowed in all of them",
         "           only the engines a test needs are running; in a pair the source and target use different cores",
         f"           everything runs {ROUNDS} times (rounds); each round reloads the data and rotates the engine and pair order",
         "queries    run unchanged on every engine; {m} is the engine's own start-of-month function:"]
    for n, q in QUERIES.items():
        o += [f"  {n}:"] + ["    " + l for l in textwrap.dedent(q).strip().splitlines()]
    o += ["tests      per engine (section 1), in every round:",
          "  load           bulk load of the 3 CSV files with the engine's fastest native loader, then build indexes",
          f"  analytic       each query: 1 first run + {RUNS} warm runs; the round's value is the median of the warm runs",
          "  round trip     300 x 'SELECT 1': the cost of one call (network for servers, Python for embedded)",
          "  point lookup   300 random 'SELECT * FROM orders WHERE id = N', one connection; median and p95",
          "  concurrency    8 threads x 100 random point lookups, one connection per thread; total lookups per second",
          "  single INSERT  300 one-row INSERTs, one transaction each (default durability); per second and p95",
          "  batch INSERT   3 statements each inserting 10,000 rows; median rows per second",
          "  single UPDATE  200 one-row UPDATEs by primary key (ClickHouse: 30, as mutations)",
          "           per pair (section 2), in every round:",
          "  warm-up        the 4 queries once on the source (untimed), because its container was just started",
          "  full copy      every table copied from source to target with INSERT/CREATE TABLE ... SELECT; time per table",
          f"  4 queries      live through the source (2 warm runs) and on the copy ({RUNS} warm runs), plus the baseline tests",
          "  break-even     copy time divided by (sum of the 4 query medians on source - on copy); rounds of the 4 queries",
          "  incremental    insert 1 / 1,000 / 50,000 rows in the source; sync = INSERT ... SELECT WHERE id > target's max id;"
          " lag = time of that sync call; 'target matches' = same row count as the source",
          "  changes        in a 200k-row copy: 1,000 rows UPDATEd and 1,000 rows DELETEd at the source, then an incremental sync,"
          " then a full refresh of that table; counts how many changes the copy shows",
          f"           files (section 4), in every round: a {FILE_ORDERS:,}-row orders CSV, queried by PostgreSQL after a COPY load,"
          " by DuckDB after importing it into a table, by DuckDB directly on the CSV and on a Parquet copy, and by Python's csv"
          f" module; 1 first run + {FILE_RUNS} warm runs per task",
          "pass rules  the answers of all 4 queries must equal PostgreSQL's on every engine, live and on copies, in every round;"
          " the file tasks must match PostgreSQL's too (exported files: same row count and a SHA-256 over every field)",
          "            (text and counts exact, numbers within 0.011 or 1e-6 relative); the run fails otherwise",
          f"reading     each value is the median of the {ROUNDS} rounds; ±x% is half the min-max range as a share of it;",
          "            * = clear winner: its range overlaps no other value's range; ~ = the best values overlap, a tie within",
          "            noise; no mark = the row has no winner to pick", ""]
    return o


def env_lines(dbs_versions):
    cpu = next((l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), "?")
    mem = {l.split(":")[0]: int(l.split()[1]) for l in open("/proc/meminfo")}
    gov = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    gov = gov.read_text().strip() if gov.exists() else "unknown"
    o = [f"host       {cpu}, {os.cpu_count()} threads, {mem['MemTotal'] / 1048576:.0f} GiB RAM, {platform.platform()}",
         f"           CPU governor '{gov}'" + (" (clock speed varies; 'performance' gives steadier numbers)" if gov != "performance" else "") +
         f"; RAM free at start {START_FREE:.1f} GiB",
         "engines    " + "; ".join(dbs_versions),
         "runtime    " + ctr("--version", check=False).stdout.strip() + f" (command: {RUNTIME})"]
    return o


def caps_table(limits):
    """Compute caps per engine. Container values are read back from the container runtime, not copied from the config."""
    got = {e: (cs, mem, swp) for e, cs, mem, swp in limits}
    cores = lambda c: f"{len(c) // 2} cores / {len(c)} threads"
    col = {e: [got[e][0] + " (container)", cores(CPUS[e]), got[e][1] + " (container)", got[e][2]] for e in SVC}
    col["duckdb"] = [cpus(CPU_B) + " (pinned)", cores(CPU_B), f"{MEM_GB} GiB (buffers only)", "n/a, in-process"]
    col["sqlite"] = [cpus(CPU_A) + " (pinned)", cores(CPU_A), "no hard cap", "n/a, in-process"]
    col["client"] = [cpus(CPU_CLIENT) + " (pinned)", cores(CPU_CLIENT), "-", "-"]
    per_query = {"postgres": f"{THREADS} (parallel workers)", "mariadb": "1 (by design)", "clickhouse": f"{THREADS} (max_threads)",
                 "duckdb": f"{THREADS} (threads)", "sqlite": "1 (by design)", "client": "-"}
    cache = {"postgres": "1 GiB shared_buffers", "mariadb": "1 GiB buffer pool", "clickhouse": "default, inside 2 GiB",
             "duckdb": "inside memory_limit", "sqlite": "1 GiB cache_size", "client": "-"}
    io = {e: "allowed" if e in SVC else "n/a, in-process" for e in ENGINES}
    io["client"] = "-"
    order = ENGINES + ["client"]
    names = [TITLE[e] for e in ENGINES] + ["Python client"]
    rows = [(label, [col[e][i] for e in order], "{}", None)
            for i, label in enumerate(["CPUs", "cores", "RAM cap", "swap"])]
    rows += [("threads per query", [per_query[e] for e in order], "{}", None),
             ("main cache", [cache[e] for e in order], "{}", None),
             ("io_uring", [io[e] for e in order], "{}", None)]
    return mat("COMPUTE CAPS (the same nominal budget for every engine)", names, rows, first="cap") + [
        "Each CPU set is two whole physical cores (all their threads), read from the machine's layout.",
        "The client column applies while a server is measured. Embedded engines run inside the client process,",
        "which is then pinned to the engine's own CPUs.",
        "RAM is a hard cap only for the containers. DuckDB's memory_limit covers its buffer manager,",
        "not the whole process, and SQLite has no cap, so peak memory is measured for every engine."]


def report(base, pairs, files, counts, problems, limits):
    o = ["=" * 78, f"RUN {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC",
         "orders     " + f"{counts['orders']:,} orders, {counts['customers']:,} customers, {counts['products']:,} products; "
         f"{ROUNDS} rounds, {RUNS} warm runs per query per round"]
    o += env_lines([base[e][0]["version"] for e in ENGINES])
    o += ["caps       every engine: 2 physical cores (4 threads) and a 2 GiB RAM budget, no swap; a hard RAM cap for the containers only - full table below",
          "reading    median of the rounds ±half range; * clear winner, ~ tie within noise (details below)"]
    host_swap = sum(m["swap"] for e in ENGINES for m in base[e]) + sum(P["swap"] for Ps in pairs.values() for P in Ps) + \
        sum(F["swap"] for F in files)
    hit = [f"{TITLE[e]} round {i + 1} ({m['majflt']:,})" for e in ENGINES for i, m in enumerate(base[e]) if m["majflt"]] + \
          [f"{SHORT[s]}->{SHORT[k]} round {i + 1} ({P['majflt']:,})" for (s, k), Ps in pairs.items()
           for i, P in enumerate(Ps) if P["majflt"]] + \
          [f"files round {i + 1} ({F['majflt']:,})" for i, F in enumerate(files) if F["majflt"]]
    o += ["memory     containers cannot swap (memory limit = swap limit)" +
          ("; a container ran out of memory: " + ", ".join(OOM) if OOM else "; no container ran out of memory"),
          "           Python process (the client for every test, and the engine itself for DuckDB and SQLite): " +
          ("no page had to be read back from disk or swap during any test" if not hit else
                                            "WARNING, pages were read back from disk or swap during: " + ", ".join(hit) +
                                            " - those values are less reliable"),
          f"           other programs on the host swapped {host_swap:,} pages in and out during the measurements"]
    o += ["=" * 78]
    o = wrap(o)
    o += caps_table(limits)
    o += wrap(spec(counts))

    cols = [TITLE[e] for e in ENGINES]
    g = lambda f: [[f(m) for m in base[e]] for e in ENGINES]
    upd = lambda m: m["update"]["ops"]
    o += mat("1. BASELINE - each engine on its own", cols, [
        ("bulk load, all tables (s)", g(lambda m: m["load_s"]), "{:.2f}", True),
        ("bulk load (rows/s)", g(lambda m: m["load_rows_s"]), "{:,.0f}", False),
        ("index / PK build (s)", g(lambda m: m["index_s"]), "{:.2f}", True),
        ("size of the 3 tables (MB)", g(lambda m: m["size_mb"]), "{:.0f}", True),
        ("peak memory, baseline tests (MB)", g(lambda m: m.get("peak_mb")), "{:,.0f}", None)])
    o += ["peak memory: containers = cgroup memory.peak during the test (includes their page cache); DuckDB and SQLite ="
          " peak resident memory of the Python process during the test (includes the Python client itself)"]
    o += mat("1a. analytic queries, median ms", cols,
             [(q, g(lambda m, q=q: m["q"][q][1]), "{:.1f}", True) for q in QUERIES] +
             [("count(*) on orders", g(lambda m: m["count_ms"]), "{:.2f}", True)])
    o += mat("1b. analytic queries, first run ms", cols,
             [(q, g(lambda m, q=q: m["q"][q][0]), "{:.1f}", True) for q in QUERIES])
    o += mat("1c. row-by-row work", cols, [
        ("round trip 'SELECT 1', median ms", g(lambda m: m["ping"]["med"]), "{:.3f}", True),
        ("point lookup by PK, median ms", g(lambda m: m["lookup"]["med"]), "{:.3f}", True),
        ("  lookup minus round trip, ms", g(lambda m: m["lookup"]["med"] - m["ping"]["med"]), "{:.3f}", True),
        ("point lookup by PK, p95 ms", g(lambda m: m["lookup"]["p95"]), "{:.3f}", True),
        ("point lookups, 8 threads (per s)", g(lambda m: m["qps8"]), "{:,.0f}", False),
        ("single-row INSERT (per s)", g(lambda m: m["insert"]["ops"]), "{:,.0f}", False),
        ("single-row INSERT, p95 ms", g(lambda m: m["insert"]["p95"]), "{:.2f}", True),
        ("10,000-row INSERT (rows/s)", g(lambda m: m["batch_rows_s"]), "{:,.0f}", False),
        ("single-row UPDATE by PK (per s)", g(upd), "{:,.0f}", False)])
    o += ["'lookup minus round trip' is the database's own work: servers pay a network call, embedded engines a Python call."]

    def rel(f, higher_is_better):
        """Per round: engine vs PostgreSQL in the same round; > 1.00 = faster than PostgreSQL."""
        out = []
        for e in ENGINES:
            r = []
            for pm, m in zip(base["postgres"], base[e]):
                a, b = f(pm), f(m)
                r.append((b / a if higher_is_better else a / b) if a and b else None)
            out.append(r)
        return out
    o += mat("1d. relative speed vs PostgreSQL (median of the per-round ratios; above 1.00 = faster than PostgreSQL)",
             [f"col-{i + 1}: {TITLE[e]}" for i, e in enumerate(ENGINES)],
             [(q, rel(lambda m, q=q: m["q"][q][1], False), "{:.2f}", None) for q in QUERIES] +
             [("count(*) on orders", rel(lambda m: m["count_ms"], False), "{:.2f}", None),
              ("point lookup by PK", rel(lambda m: m["lookup"]["med"], False), "{:.2f}", None),
              ("point lookups, 8 threads", rel(lambda m: m["qps8"], True), "{:.2f}", None),
              ("single-row INSERT", rel(lambda m: m["insert"]["ops"], True), "{:.2f}", None),
              ("10,000-row INSERT", rel(lambda m: m["batch_rows_s"], True), "{:.2f}", None),
              ("single-row UPDATE by PK", rel(upd, True), "{:.2f}", None),
              ("bulk load", rel(lambda m: m["load_s"], False), "{:.2f}", None)], first="test")

    o += ["", "2. PAIRS - data moves from a source database to a target database", ""]
    for (s, k), Ps in pairs.items():
        sm, tm = base[s], base[k]
        o += ["-" * 78, f"{TITLE[s]} -> {TITLE[k]}   (source on CPUs {cpus(CPUS[s])}, target on CPUs {cpus(CPUS[k])})",
              "-" * 78]
        o += mat("full copy", ["rows", "time (s)", "rows/s"],
                 [(t, [f"{Ps[0]['copy'][t][0]:,}", [P["copy"][t][1] for P in Ps],
                       [P["copy"][t][0] / P["copy"][t][1] for P in Ps]], ["", "{:.2f}", "{:,.0f}"], None) for t in TABLES] +
                 [("all tables", ["", [P["copy_s"] for P in Ps], ""], "{:.2f}", None)], first="table")
        side = lambda f_src, f_tgt, f_copy: [[f_src(m) for m in sm], [f_tgt(m) for m in tm], [f_copy(P) for P in Ps]]
        rows = [("load / copy of all tables (s)", side(lambda m: m["load_s"], lambda m: m["load_s"], lambda P: P["copy_s"]) + [None], "{:.2f}", None),
                ("size of the 3 tables (MB)", side(lambda m: m["size_mb"], lambda m: m["size_mb"], lambda P: P["copy_mb"]) + [None], "{:.0f}", None)]
        for q in QUERIES:
            rows.append((q + ", median ms", side(lambda m, q=q: m["q"][q][1], lambda m, q=q: m["q"][q][1],
                                                 lambda P, q=q: P["q_copy"][q][1]) + [[P["q_fed"][q][1] for P in Ps]],
                         "{:,.1f}", None))
        for q in QUERIES:
            rows.append((q[:2] + " first run ms", side(lambda m, q=q: m["q"][q][0], lambda m, q=q: m["q"][q][0],
                                                       lambda P, q=q: P["q_copy"][q][0]) + [[P["q_fed"][q][0] for P in Ps]],
                         "{:,.1f}", None))
        for label, f, fmt in [("count(*) on orders, ms", lambda m: m["count_ms"], "{:.2f}"),
                              ("round trip 'SELECT 1', ms", lambda m: m["ping"]["med"], "{:.3f}"),
                              ("point lookup by id, median ms", lambda m: m["lookup"]["med"], "{:.3f}"),
                              ("point lookup by id, p95 ms", lambda m: m["lookup"]["p95"], "{:.3f}"),
                              ("point lookups, 8 threads (per s)", lambda m: m["qps8"], "{:,.0f}"),
                              ("single-row INSERT (per s)", lambda m: m["insert"]["ops"], "{:,.0f}"),
                              ("10,000-row INSERT (rows/s)", lambda m: m["batch_rows_s"], "{:,.0f}"),
                              ("single-row UPDATE by id (per s)", upd, "{:,.0f}")]:
            rows.append((label, side(f, f, lambda P, f=f: f(P["ops"])) + [None], fmt, None))
        o += mat("same tests as the baseline: source, target loaded directly, target as synced copy",
                 [f"{TITLE[s]} (source)", f"{TITLE[k]} loaded directly", f"{TITLE[k]} synced copy", "live via source"],
                 rows, first="test")
        sp = lambda P: sum(P["q_src"][q][1] for q in QUERIES) / sum(P["q_copy"][q][1] for q in QUERIES)
        o += ["'synced copy' has only what the sync created: no primary key or secondary index (ClickHouse copy is sorted by id).",
              "'live via source' ran 2 warm runs instead of " + str(RUNS) + "."]
        o += mat("copy versus source", ["value"], [
            ("speed-up, all 4 queries (x)", [[sp(P) for P in Ps]], "{:.1f}", None)] +
            [(f"speed-up, {q[:2]} (x)", [[P["q_src"][q][1] / P["q_copy"][q][1] for P in Ps]], "{:.1f}", None) for q in QUERIES] +
            [("break-even (rounds of the 4 queries)", [[P["breakeven"] for P in Ps]], "{:.2f}", None)])
        o += mat("incremental sync (new rows only, by id watermark)", ["insert on source ms", "sync = lag ms", "target matches"],
                 [(f"{n:,} new rows", [[P["lag"][n][0] for P in Ps], [P["lag"][n][1] for P in Ps],
                                       f"{sum(P['lag'][n][2] for P in Ps)}/{len(Ps)} rounds"], "{:,.1f}", None)
                  for n in Ps[0]["lag"]], first="batch")
        u = sorted({P["after_incr"][0] for P in Ps})
        d = sorted({P["after_incr"][1] for P in Ps})
        u2 = sorted({P["after_refresh"][0] for P in Ps})
        d2 = sorted({P["after_refresh"][1] for P in Ps})
        fmt_set = lambda v: "/".join(map(str, v))
        o += ["", "changes to existing rows (1,000 rows updated, 1,000 rows deleted at the source), every round:",
              f"  after an incremental sync the copy shows {fmt_set(u)}/1000 updates and still holds {fmt_set(d)}/1000 deleted rows",
              f"  a full refresh of the 200k-row table fixes it in {show(agg([P['refresh_s'] for P in Ps]), '{:.2f}')} s"
              f" -> {fmt_set(u2)}/1000 updates, {fmt_set(d2)}/1000 deleted rows left",
              "  reading the source live (federation) always shows the current rows",
              "answers equal to PostgreSQL's on the copy and live, all rounds: " +
              ", ".join(f"{q[:2]} {'OK' if all(P['agree'][q] for P in Ps) else 'DIFFERENT'}" for q in QUERIES), ""]

    pv = list(pairs.values())
    sp = lambda P: sum(P["q_src"][q][1] for q in QUERIES) / sum(P["q_copy"][q][1] for q in QUERIES)
    o += mat("3. PAIR SUMMARY", [f"{SHORT[s]}->{SHORT[k]}" for s, k in pairs],
             [("full copy, all tables (s)", [[P["copy_s"] for P in Ps] for Ps in pv], "{:.2f}", True),
              ("4 queries, copy vs source (x)", [[sp(P) for P in Ps] for Ps in pv], "{:.1f}", False),
              ("break-even (query rounds)", [[P["breakeven"] for P in Ps] for Ps in pv], "{:.2f}", True),
              ("lag for 1,000 new rows (ms)", [[P["lag"][1000][1] for P in Ps] for Ps in pv], "{:,.1f}", True),
              ("updates seen by sync (of 1000)", [[P["after_incr"][0] for P in Ps] for Ps in pv], "{:.0f}", False),
              ("deleted rows left (of 1000)", [[P["after_incr"][1] for P in Ps] for Ps in pv], "{:.0f}", True)],
             first="pair")
    o += ["", "", "4. FILES - analysing a CSV file directly, the way an application might use DuckDB next to its main database"]
    fp = lambda k: [F["prep"][k] for F in files]
    o += mat("preparation", ["value"], [
        ("CSV file (MB)", [fp("csv_mb")], "{:.0f}", None),
        ("load the CSV into PostgreSQL (s)", [fp("pg_load_s")], "{:.2f}", None),
        ("import the CSV into a DuckDB table (s)", [fp("duck_import_s")], "{:.2f}", None),
        ("convert the CSV to Parquet with DuckDB (s)", [fp("parquet_s")], "{:.2f}", None),
        ("Parquet file (MB)", [fp("parquet_mb")], "{:.0f}", None)], first="step")
    o += mat("the three tasks, median ms", [FILE_TITLE[w] for w in FILE_WAYS],
             [(t, [[F["t"][w][t][1] for F in files] for w in FILE_WAYS], "{:,.1f}", True) for t in FILE_TASKS] +
             [("all three", [[sum(F["t"][w][t][1] for t in FILE_TASKS) for F in files] for w in FILE_WAYS], "{:,.1f}", True)],
             first="task")
    o += mat("the three tasks, first run ms", [FILE_TITLE[w] for w in FILE_WAYS],
             [(t, [[F["t"][w][t][0] for F in files] for w in FILE_WAYS], "{:,.1f}", True) for t in FILE_TASKS], first="task")
    o += mat("relative speed vs PostgreSQL (after load), all three tasks; above 1.00 = faster", ["value"],
             [(FILE_TITLE[w], [[sum(F["t"]["postgres"][t][1] for t in FILE_TASKS) / sum(F["t"][w][t][1] for t in FILE_TASKS)
                                for F in files]], "{:.2f}", None) for w in FILE_WAYS[1:]], first="way")
    o += ["PostgreSQL and the DuckDB table need the load or import above first; DuckDB on the CSV and on Parquet does not.",
          "F3 writes the matching rows to a new CSV file. The Python way reads the CSV row by row with float arithmetic, standing in"
          " for parsing the file in application code.",
          "answers equal to PostgreSQL's, all rounds: " + ", ".join(
              f"{FILE_TITLE[w]} {'OK' if all(F['agree'][w][t] for F in files for t in FILE_TASKS) else 'DIFFERENT'}"
              for w in FILE_WAYS[1:])]
    o += ["", "answers identical across all engines, all queries, all rounds: " +
          ("YES" if not problems else "NO: " + "; ".join(problems))]
    o += ["", "NOTES",
          "- one machine, one synthetic data set; the client is Python, which adds the same kind of overhead to every engine",
          "- embedded engines share their 4 CPUs with the Python code calling them; that is the real cost of running in-process",
          "- the 8-thread test is limited by Python's GIL for embedded engines more than for servers",
          "- loads: PostgreSQL COPY, MariaDB LOAD DATA (bulk path), ClickHouse CSV insert, DuckDB COPY, SQLite executemany in Python",
          "- clients: psycopg (libpq), MariaDB Connector/Python with its C extension, clickhouse-connect (HTTP); DuckDB and"
          " SQLite through their Python modules",
          "- indexes: PK plus indexes on orders(customer_id) and orders(created_at) in PostgreSQL, MariaDB, SQLite; DuckDB PK only; "
          "ClickHouse sorted by id",
          "- single-row writes use each engine's default durability, one transaction per row; ClickHouse updates are mutations",
          "- ClickHouse 26 enables async_insert by default, which makes each client INSERT wait for a buffer flush (about 50 ms"
          " measured here); the benchmark turns it off so every engine's INSERT is finished when it returns",
          "- syncs are plain INSERT ... SELECT by id watermark through DuckDB ATTACH or ClickHouse database engines "
          "(PostgreSQL/MySQL), no change-data-capture", ""]
    return "\n".join(o)


# ---------- raw numbers, one per row ----------

def metrics(m):
    """Flatten one engine's measurements into (test, metric, value, unit) rows."""
    out = [(test, metric, m[k], u) for k, test, metric, u in [
        ("load_s", "bulk load", "time", "s"), ("load_rows_s", "bulk load", "speed", "rows/s"),
        ("index_s", "index build", "time", "s"), ("size_mb", "size of 3 tables", "size", "MB"),
        ("count_ms", "count(*) on orders", "median", "ms"), ("qps8", "point lookups, 8 threads", "throughput", "lookups/s"),
        ("batch_rows_s", "10,000-row insert", "speed", "rows/s"), ("swap", "host swap during test", "pages", "pages"),
        ("peak_mb", "peak memory used", "peak", "MB"),
        ("majflt", "major page faults of the Python process", "count", "faults")]
        if m.get(k) is not None]
    for q, (first, med) in m.get("q", {}).items():
        out += [(q, "first run", first, "ms"), (q, "median of warm runs", med, "ms")]
    for key, name in [("ping", "round trip SELECT 1"), ("lookup", "point lookup by id"),
                      ("insert", "single-row insert"), ("update", "single-row update")]:
        if m.get(key):
            out += [(name, "median", m[key]["med"], "ms"), (name, "p95", m[key]["p95"], "ms"),
                    (name, "throughput", m[key]["ops"], "per s")]
    return out


def write_csv(stamp, base, pairs, files, counts):
    out = []

    def add(rnd, sec, scope, eng, items):
        out.extend((stamp, rnd, sec, scope, eng, t, mt, f"{v:.6g}" if isinstance(v, float) else v, u)
                   for t, mt, v, u in items)
    add("-", "setup", "-", "-", [(t, "rows", n, "rows") for t, n in counts.items()] +
        [("warm runs", "count", RUNS, "runs"), ("rounds", "count", ROUNDS, "rounds"),
         ("cpus per engine", "count", THREADS, "threads"), ("ram per engine", "limit", MEM_GB, "GiB")])
    for e, cs, mem, swp in LIMITS:
        add("-", "setup", "-", e, [("cpu cap", "cpuset (container)", cs, "cpus"), ("ram cap", "limit (container)", mem, ""),
                                   ("swap", "allowed", swp, "")])
    add("-", "setup", "-", "duckdb", [("cpu cap", "pinned", cpus(CPU_B), "cpus"), ("ram cap", "memory_limit", f"{MEM_GB} GiB", "")])
    add("-", "setup", "-", "sqlite", [("cpu cap", "pinned", cpus(CPU_A), "cpus"), ("ram cap", "none (cache_size)", "1 GiB", "")])
    add("-", "setup", "-", "client", [("cpu cap", "pinned", cpus(CPU_CLIENT), "cpus")])
    for e in ENGINES:
        for i, m in enumerate(base[e]):
            add(i + 1, "baseline", "-", e, metrics(m))
    for (s, k), Ps in pairs.items():
        scope = f"{s}->{k}"
        for i, P in enumerate(Ps):
            items = metrics(P["ops"]) + [(q, "first run", f, "ms") for q, (f, _) in P["q_copy"].items()] + \
                [(q, "median of warm runs", md, "ms") for q, (_, md) in P["q_copy"].items()] + \
                [("copy of all tables", "time", P["copy_s"], "s"), ("size of 3 tables", "size", P["copy_mb"], "MB")]
            for t in TABLES:
                items += [(f"copy of {t}", "rows", P["copy"][t][0], "rows"), (f"copy of {t}", "time", P["copy"][t][1], "s")]
            add(i + 1, "pair", scope, k + " (synced copy)", items)
            add(i + 1, "pair", scope, s + " (read live from target)",
                [(q, "first run", f, "ms") for q, (f, _) in P["q_fed"].items()] +
                [(q, "median of warm runs", md, "ms") for q, (_, md) in P["q_fed"].items()])
            sync = [("break-even", "rounds of the 4 queries", P["breakeven"], "rounds"),
                    ("full refresh of 200k-row table", "time", P["refresh_s"], "s"),
                    ("updates visible after incremental sync", "of 1000", P["after_incr"][0], "rows"),
                    ("deleted rows still visible after incremental sync", "of 1000", P["after_incr"][1], "rows"),
                    ("updates visible after full refresh", "of 1000", P["after_refresh"][0], "rows"),
                    ("deleted rows still visible after full refresh", "of 1000", P["after_refresh"][1], "rows"),
                    ("host swap during test", "pages", P["swap"], "pages"),
                    ("major page faults of the Python process", "count", P["majflt"], "faults")]
            for n, (t_ins, t_sync, same) in P["lag"].items():
                sync += [(f"{n} new rows", "insert at source", t_ins, "ms"), (f"{n} new rows", "sync = lag", t_sync, "ms"),
                         (f"{n} new rows", "target matches source", int(same), "1=yes")]
            sync += [(q, "answers equal to PostgreSQL", int(ok), "1=yes") for q, ok in P["agree"].items()]
            add(i + 1, "pair", scope, "sync", [x for x in sync if x[2] is not None])
    for i, F in enumerate(files):
        add(i + 1, "files", "-", "-", [("load CSV into PostgreSQL", "time", F["prep"]["pg_load_s"], "s"),
                                       ("import CSV into a DuckDB table", "time", F["prep"]["duck_import_s"], "s"),
                                       ("convert CSV to Parquet", "time", F["prep"]["parquet_s"], "s"),
                                       ("CSV file", "size", F["prep"]["csv_mb"], "MB"),
                                       ("Parquet file", "size", F["prep"]["parquet_mb"], "MB"),
                                       ("host swap during test", "pages", F["swap"], "pages"),
                                       ("major page faults of the Python process", "count", F["majflt"], "faults")])
        for w in FILE_WAYS:
            add(i + 1, "files", "-", w, [(t, "first run", F["t"][w][t][0], "ms") for t in FILE_TASKS] +
                [(t, "median of warm runs", F["t"][w][t][1], "ms") for t in FILE_TASKS] +
                ([(t, "answers equal to PostgreSQL", int(F["agree"][w][t]), "1=yes") for t in FILE_TASKS] if w != "postgres" else []))
    out += [(stamp, rnd + 1, "raw query runs", lab, eng, q, "first run" if n == 0 else f"warm run {n}", f"{ms:.4f}", "ms")
            for rnd, lab, eng, q, n, ms in RAW]
    path = ROOT / "results.csv"
    new = not path.exists()
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["run", "round", "section", "scope", "engine", "test", "metric", "value", "unit"])
        w.writerows(out)
    return len(out)


def container_limits():
    """What the container runtime actually applied: (engine, cpuset, memory limit, swap) per container."""
    out = []
    for e, (name, *_) in CONTAINERS.items():
        r = ctr("inspect", "-f", "{{.HostConfig.CpusetCpus}} {{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}", name, check=False)
        cs, mem, swp = (r.stdout.split() + ["?", "0", "0"])[:3]
        if ctr("inspect", "-f", "{{.State.OOMKilled}}", name, check=False).stdout.strip() == "true":
            OOM.append(TITLE[e])
        out.append((e, cpus(int(x) for p in cs.split(",") for x in range(int(p.split("-")[0]), int(p.split("-")[-1]) + 1)),
                    f"{int(mem) / 2**30:.0f} GiB", "none" if mem == swp else "ALLOWED"))
    return out


START_FREE = 0.0
EXT_DIR = None
OOM = []
LIMITS = []


def main():
    global ROUND, START_FREE, EXT_DIR
    if sys.argv[1:] == ["down"]:
        return remove_all()
    START_FREE = int(next(l.split()[1] for l in open("/proc/meminfo") if l.startswith("MemAvailable"))) / 1048576
    if START_FREE < 4:
        say(f"warning: only {START_FREE:.1f} GiB RAM free; close other programs for steadier numbers")
    # next to the project, on real disk: /tmp is often a RAM disk, which would favour the embedded engines' files
    work = Path(tempfile.mkdtemp(prefix=".work-", dir=ROOT))
    EXT_DIR = work / "duckdb_ext"
    remove_all()             # a container left over from an earlier run could have another image or other limits
    say(f"containers via '{RUNTIME}'; CPUs {cpus(CPU_A)} | {cpus(CPU_B)} | client {cpus(CPU_CLIENT)}")
    try:
        pin(CPU_CLIENT)
        counts = make_csvs(work)
        make_file(work / "file_orders.csv", FILE_ORDERS)
        say(f"csv ready, {ORDERS:,} orders (+ a {FILE_ORDERS:,}-row file for section 4); {ROUNDS} rounds")
        gc.disable()             # collected explicitly between tests, never in the middle of one
        base = {e: [] for e in ENGINES}
        pairs = {p: [] for p in PAIRS}
        files, problems = [], []
        for ROUND in range(ROUNDS):
            answers = {}
            for e in ENGINES[ROUND % len(ENGINES):] + ENGINES[:ROUND % len(ENGINES)]:
                m, answers[e] = baseline(e, work, counts)
                base[e].append(m)
            ref = answers["postgres"]
            problems += [f"round {ROUND + 1} {TITLE[e]} {q[:2]}" for e in ENGINES[1:] for q in QUERIES
                         if not agree(ref[q], answers[e][q])]
            for s, k in PAIRS[ROUND % 4:] + PAIRS[:ROUND % 4]:
                P = run_pair(s, k, work, base[s][-1], ref)
                pairs[(s, k)].append(P)
                problems += [f"round {ROUND + 1} {SHORT[s]}->{SHORT[k]} {q[:2]}" for q, ok in P["agree"].items() if not ok]
            F = files_test(work)
            files.append(F)
            problems += [f"round {ROUND + 1} files {FILE_TITLE[w]} {t[:2]}" for w, ts in F["agree"].items()
                         for t, ok in ts.items() if not ok]
            for f in work.glob(f"lab_{ROUND}.*"):
                f.unlink()
        limits = LIMITS[:] = container_limits()
        only()
        pin(os.sched_getaffinity(0) | set(range(os.cpu_count())))
        text = report(base, pairs, files, counts, problems, limits)
        with open(ROOT / "results.txt", "a") as f:
            f.write(text + "\n")
        n = write_csv(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"), base, pairs, files, counts)
        print(text)
        say(f"wrote results.txt and results.csv ({n} rows)")
        if problems:
            print("DIFFERENT ANSWERS: " + "; ".join(problems))
            sys.exit(1)
    finally:
        shutil.rmtree(work, ignore_errors=True)
        if not os.environ.get("KEEP"):
            remove_all()


if __name__ == "__main__":
    main()
