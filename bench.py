#!/usr/bin/env python3
"""Database benchmark: five engines on their own, then four source -> target pairs, under equal resources.

    python bench.py        run everything, append the report to results.txt and the raw numbers to results.csv
    python bench.py down   remove the containers and their network

Engines   PostgreSQL, MariaDB, ClickHouse (containers) and DuckDB, SQLite (embedded: they run inside the Python process).
Pairs     PostgreSQL->DuckDB, MariaDB->ClickHouse, SQLite->DuckDB, PostgreSQL->ClickHouse.
Fairness  every engine gets 2 physical cores (4 threads) and 2 GB RAM; only the engines a test needs are running;
          the whole measurement is repeated LAB_ROUNDS times in a rotated order and reported as median and range.
Env       LAB_ORDERS (rows in orders, default 1000000), LAB_RUNS (warm query runs, default 5),
          LAB_ROUNDS (repeats of everything, default 3), KEEP=1 (leave containers up),
          LAB_DOCKER (container command, default docker, else podman; use "sudo podman" if rootless podman cannot pin CPUs).
"""
import copy
import csv
import gc
import os
import platform
import random
import re
import resource
import shutil
import sqlite3
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

import clickhouse_connect
import duckdb
import psycopg
import pymysql

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


# Resource budget, the same for every engine: 2 physical cores (all their threads) and 2 GB RAM.
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
    def ping(s, i): s.q("SELECT 1")
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
    month = "DATE_FORMAT(o.created_at, '%Y-%m-01')"
    ddl = make_ddl("INT", "DECIMAL(12,2)", "VARCHAR(32)", "DATETIME")
    idx = PG.idx
    analyze = ["ANALYZE TABLE customers, products, orders"]

    def __init__(s):
        s.con = pymysql.connect(host=HOST, port=MYP, user="lab", password="lab", database="lab",
                                autocommit=True, local_infile=True)

    def q(s, sql):
        c = s.con.cursor()
        c.execute(sql)
        return c.fetchall()

    def x(s, sql): s.con.cursor().execute(sql)
    def version(s): return "MariaDB " + s.q("SELECT version()")[0][0]
    def reset(s): s.x("DROP TABLE IF EXISTS customers, products, orders, scratch, orders_sync")

    def load(s, t, path):
        s.x(f"LOAD DATA LOCAL INFILE '{path}' INTO TABLE {t} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'")

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
            settings={"max_threads": THREADS, "mysql_datatypes_support_level": "decimal,datetime64"})

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
        cfg = {"threads": THREADS, "memory_limit": f"{MEM_GB}GB"}
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
        s.con = sqlite3.connect(s.path, isolation_level=None, check_same_thread=False)  # autocommit, synchronous=FULL
        s.con.execute("PRAGMA cache_size = -1000000")   # about 1 GB page cache, like the others' main cache

    def q(s, sql): return s.con.execute(sql).fetchall()
    def x(s, sql): s.con.execute(sql)
    def version(s): return "SQLite " + sqlite3.sqlite_version
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
# engine: (container, network alias, image, host port, container port, environment, extra options, command)
CONTAINERS = {
    "postgres": ("hybridlab-pg", "pg", "docker.io/library/postgres:16", PGP, 5432,
                 {"POSTGRES_USER": "lab", "POSTGRES_PASSWORD": "lab", "POSTGRES_DB": "lab"}, ["--shm-size=512m"],
                 ["postgres", "-c", "shared_buffers=1GB", "-c", "effective_cache_size=1536MB", "-c", "work_mem=64MB",
                  "-c", "max_worker_processes=8", "-c", f"max_parallel_workers={THREADS}",
                  "-c", f"max_parallel_workers_per_gather={THREADS}", "-c", "random_page_cost=1.1"]),
    "mariadb": ("hybridlab-mariadb", "mariadb", "docker.io/library/mariadb:11", MYP, 3306,
                {"MARIADB_ROOT_PASSWORD": "lab", "MARIADB_DATABASE": "lab", "MARIADB_USER": "lab", "MARIADB_PASSWORD": "lab"},
                [], ["--local-infile=1", "--innodb-buffer-pool-size=1G"]),
    "clickhouse": ("hybridlab-clickhouse", "clickhouse", "docker.io/clickhouse/clickhouse-server:25.8", CHP, 8123,
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
    only(e)
    pin(client_cpus(e))
    db = open_engine(e, work)
    say(f"round {ROUND + 1}/{ROUNDS}  baseline {TITLE[e]}")
    sw, mf = swap_pages(), majflt()
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
         f"fairness   every engine: 2 physical cores = {THREADS} threads, {MEM_GB} GB RAM, no swap, about 1 GB main cache",
         f"           CPUs {cpus(CPU_A)}: PostgreSQL, MariaDB, SQLite; CPUs {cpus(CPU_B)}: ClickHouse, DuckDB; "
         f"CPUs {cpus(CPU_CLIENT)}: the Python client for server engines",
         "           containers: cpuset + memory limit from the container runtime; DuckDB: threads=4, memory_limit=2GB; "
         "SQLite: single-threaded, cache_size 1 GB; embedded engines run in the client process, pinned to their own cores",
         "           PostgreSQL: shared_buffers 1GB, 4 parallel workers; MariaDB: buffer pool 1GB; ClickHouse: max_threads 4",
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
          "pass rules  the answers of all 4 queries must equal PostgreSQL's on every engine, live and on copies, in every round",
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
    o = [f"host       {cpu}, {os.cpu_count()} threads, {mem['MemTotal'] / 1048576:.0f} GB RAM, {platform.platform()}",
         f"           CPU governor '{gov}'" + (" (clock speed varies; 'performance' gives steadier numbers)" if gov != "performance" else "") +
         f"; RAM free at start {START_FREE:.1f} GB",
         "engines    " + "; ".join(dbs_versions),
         "runtime    " + ctr("--version", check=False).stdout.strip() + f" (command: {RUNTIME})"]
    return o


def caps_table(limits):
    """Compute caps per engine. Container values are read back from the container runtime, not copied from the config."""
    got = {e: (cs, mem, swp) for e, cs, mem, swp in limits}
    cores = lambda c: f"{len(c) // 2} cores / {len(c)} threads"
    col = {e: [got[e][0] + " (container)", cores(CPUS[e]), got[e][1] + " (container)", got[e][2]] for e in SVC}
    col["duckdb"] = [cpus(CPU_B) + " (pinned)", cores(CPU_B), f"{MEM_GB} GB (memory_limit)", "n/a, in-process"]
    col["sqlite"] = [cpus(CPU_A) + " (pinned)", cores(CPU_A), "no hard cap", "n/a, in-process"]
    col["client"] = [cpus(CPU_CLIENT) + " (pinned)", cores(CPU_CLIENT), "-", "-"]
    per_query = {"postgres": f"{THREADS} (parallel workers)", "mariadb": "1 (by design)", "clickhouse": f"{THREADS} (max_threads)",
                 "duckdb": f"{THREADS} (threads)", "sqlite": "1 (by design)", "client": "-"}
    cache = {"postgres": "1 GB shared_buffers", "mariadb": "1 GB buffer pool", "clickhouse": "default, inside 2 GB",
             "duckdb": "inside memory_limit", "sqlite": "1 GB cache_size", "client": "-"}
    order = ENGINES + ["client"]
    names = [TITLE[e] for e in ENGINES] + ["Python client"]
    rows = [(label, [col[e][i] for e in order], "{}", None)
            for i, label in enumerate(["CPUs", "cores", "RAM cap", "swap"])]
    rows += [("threads per query", [per_query[e] for e in order], "{}", None),
             ("main cache", [cache[e] for e in order], "{}", None)]
    return mat("COMPUTE CAPS (the same budget for every engine)", names, rows, first="cap") + [
        "Each CPU set is two whole physical cores (all their threads), read from the machine's layout.",
        "The client column applies while a server is measured. Embedded engines run inside the client process,",
        "which is then pinned to the engine's own CPUs."]


def report(base, pairs, counts, problems, limits):
    o = ["=" * 78, f"RUN {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC",
         "orders     " + f"{counts['orders']:,} orders, {counts['customers']:,} customers, {counts['products']:,} products; "
         f"{ROUNDS} rounds, {RUNS} warm runs per query per round"]
    o += env_lines([base[e][0]["version"] for e in ENGINES])
    o += ["caps       every engine: 2 physical cores (4 threads) and 2 GB RAM, no swap - full table below",
          "reading    median of the rounds ±half range; * clear winner, ~ tie within noise (details below)"]
    host_swap = sum(m["swap"] for e in ENGINES for m in base[e]) + sum(P["swap"] for Ps in pairs.values() for P in Ps)
    hit = [f"{TITLE[e]} round {i + 1} ({m['majflt']:,})" for e in ENGINES for i, m in enumerate(base[e]) if m["majflt"]] + \
          [f"{SHORT[s]}->{SHORT[k]} round {i + 1} ({P['majflt']:,})" for (s, k), Ps in pairs.items()
           for i, P in enumerate(Ps) if P["majflt"]]
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
    o += mat("1. BASELINE - each engine on its own", cols, [
        ("bulk load, all tables (s)", g(lambda m: m["load_s"]), "{:.2f}", True),
        ("bulk load (rows/s)", g(lambda m: m["load_rows_s"]), "{:,.0f}", False),
        ("index / PK build (s)", g(lambda m: m["index_s"]), "{:.2f}", True),
        ("size of the 3 tables (MB)", g(lambda m: m["size_mb"]), "{:.0f}", True)])
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
        ("single-row UPDATE by PK (per s)", g(lambda m: m["update"]["ops"]), "{:,.0f}", False)])
    o += ["'lookup minus round trip' is the database's own work: servers pay a network call, embedded engines a Python call."]

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
                              ("single-row UPDATE by id (per s)", lambda m: m["update"]["ops"], "{:,.0f}")]:
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
    o += ["", "answers identical across all engines, all queries, all rounds: " +
          ("YES" if not problems else "NO: " + "; ".join(problems))]
    o += ["", "NOTES",
          "- one machine, one synthetic data set; the client is Python, which adds the same kind of overhead to every engine",
          "- embedded engines share their 4 CPUs with the Python code calling them; that is the real cost of running in-process",
          "- the 8-thread test is limited by Python's GIL for embedded engines more than for servers",
          "- loads: PostgreSQL COPY, MariaDB LOAD DATA, ClickHouse CSV insert, DuckDB COPY, SQLite executemany in Python",
          "- indexes: PK plus indexes on orders(customer_id) and orders(created_at) in PostgreSQL, MariaDB, SQLite; DuckDB PK only; "
          "ClickHouse sorted by id",
          "- single-row writes use each engine's default durability, one transaction per row; ClickHouse updates are mutations",
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
        ("majflt", "major page faults of the Python process", "count", "faults")]
        if m.get(k) is not None]
    for q, (first, med) in m.get("q", {}).items():
        out += [(q, "first run", first, "ms"), (q, "median of warm runs", med, "ms")]
    for key, name in [("ping", "round trip SELECT 1"), ("lookup", "point lookup by id"),
                      ("insert", "single-row insert"), ("update", "single-row update")]:
        if key in m:
            out += [(name, "median", m[key]["med"], "ms"), (name, "p95", m[key]["p95"], "ms"),
                    (name, "throughput", m[key]["ops"], "per s")]
    return out


def write_csv(stamp, base, pairs, counts):
    out = []

    def add(rnd, sec, scope, eng, items):
        out.extend((stamp, rnd, sec, scope, eng, t, mt, f"{v:.6g}" if isinstance(v, float) else v, u)
                   for t, mt, v, u in items)
    add("-", "setup", "-", "-", [(t, "rows", n, "rows") for t, n in counts.items()] +
        [("warm runs", "count", RUNS, "runs"), ("rounds", "count", ROUNDS, "rounds"),
         ("cpus per engine", "count", THREADS, "threads"), ("ram per engine", "limit", MEM_GB, "GB")])
    for e, cs, mem, swp in LIMITS:
        add("-", "setup", "-", e, [("cpu cap", "cpuset (container)", cs, "cpus"), ("ram cap", "limit (container)", mem, ""),
                                   ("swap", "allowed", swp, "")])
    add("-", "setup", "-", "duckdb", [("cpu cap", "pinned", cpus(CPU_B), "cpus"), ("ram cap", "memory_limit", f"{MEM_GB} GB", "")])
    add("-", "setup", "-", "sqlite", [("cpu cap", "pinned", cpus(CPU_A), "cpus"), ("ram cap", "none (cache_size)", "1 GB", "")])
    add("-", "setup", "-", "client", [("cpu cap", "pinned", cpus(CPU_CLIENT), "cpus")])
    for e in ENGINES:
        for i, m in enumerate(base[e]):
            add(i + 1, "baseline", "-", e, metrics(m))
    for (s, k), Ps in pairs.items():
        scope = f"{s}->{k}"
        for i, P in enumerate(Ps):
            items = metrics(P["ops"]) + [(q, "first run", f, "ms") for q, (f, md) in P["q_copy"].items()] + \
                [(q, "median of warm runs", md, "ms") for q, (f, md) in P["q_copy"].items()] + \
                [("copy of all tables", "time", P["copy_s"], "s"), ("size of 3 tables", "size", P["copy_mb"], "MB")]
            for t in TABLES:
                items += [(f"copy of {t}", "rows", P["copy"][t][0], "rows"), (f"copy of {t}", "time", P["copy"][t][1], "s")]
            add(i + 1, "pair", scope, k + " (synced copy)", items)
            add(i + 1, "pair", scope, s + " (read live from target)",
                [(q, "first run", f, "ms") for q, (f, md) in P["q_fed"].items()] +
                [(q, "median of warm runs", md, "ms") for q, (f, md) in P["q_fed"].items()])
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
                    f"{int(mem) / 2**30:.0f} GB", "none" if mem == swp else "ALLOWED"))
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
        say(f"warning: only {START_FREE:.1f} GB RAM free; close other programs for steadier numbers")
    # next to the project, on real disk: /tmp is often a RAM disk, which would favour the embedded engines' files
    work = Path(tempfile.mkdtemp(prefix=".work-", dir=ROOT))
    EXT_DIR = work / "duckdb_ext"
    say(f"containers via '{RUNTIME}'; CPUs {cpus(CPU_A)} | {cpus(CPU_B)} | client {cpus(CPU_CLIENT)}")
    try:
        pin(CPU_CLIENT)
        counts = make_csvs(work)
        say(f"csv ready, {ORDERS:,} orders; {ROUNDS} rounds")
        gc.disable()             # collected explicitly between tests, never in the middle of one
        base = {e: [] for e in ENGINES}
        pairs = {p: [] for p in PAIRS}
        problems = []
        for ROUND in range(ROUNDS):
            answers = {}
            for e in ENGINES[ROUND % 5:] + ENGINES[:ROUND % 5]:
                m, answers[e] = baseline(e, work, counts)
                base[e].append(m)
            ref = answers["postgres"]
            problems += [f"round {ROUND + 1} {TITLE[e]} {q[:2]}" for e in ENGINES[1:] for q in QUERIES
                         if not agree(ref[q], answers[e][q])]
            for s, k in PAIRS[ROUND % 4:] + PAIRS[:ROUND % 4]:
                P = run_pair(s, k, work, base[s][-1], ref)
                pairs[(s, k)].append(P)
                problems += [f"round {ROUND + 1} {SHORT[s]}->{SHORT[k]} {q[:2]}" for q, ok in P["agree"].items() if not ok]
            for f in work.glob(f"lab_{ROUND}.*"):
                f.unlink()
        limits = LIMITS[:] = container_limits()
        only()
        pin(os.sched_getaffinity(0) | set(range(os.cpu_count())))
        text = report(base, pairs, counts, problems, limits)
        with open(ROOT / "results.txt", "a") as f:
            f.write(text + "\n")
        n = write_csv(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"), base, pairs, counts)
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
