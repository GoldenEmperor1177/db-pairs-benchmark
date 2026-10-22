# db-pairs-benchmark

Can two databases beat one? This benchmark measures five database engines on their own, four pairs of them working together, and DuckDB analysing files directly, all under the same CPU and memory budget, with every answer checked for correctness.

It started from a discussion about using PostgreSQL for an application and DuckDB for analysis. This is version 2: it fixes MariaDB-specific problems reviewers found in version 1, uses the current release of every engine and library, and adds a file-analytics test.

## tl;dr

From one run on an AMD Ryzen 7 9700X with 1,000,000 orders, 3 rounds. Ratios are relative to PostgreSQL, where above 1.00 means faster.

* **Analytics:** DuckDB ran the three heavy queries 26.52x to 46.57x faster than PostgreSQL, and ClickHouse 3.17x to 11.14x faster. MariaDB and SQLite were slower than PostgreSQL on Q1 and Q2.
* **Small reads and writes:** MariaDB led concurrent point lookups (3.75x PostgreSQL). PostgreSQL and MariaDB tied on single-row inserts and updates. SQLite had the fastest single lookup, since it has no network round trip. ClickHouse managed about 1 single-row UPDATE per second (see [ClickHouse 26.9](#clickhouse-269)).
* **Pairs:** copying the data from a row store into DuckDB or ClickHouse took 0.19 to 0.49 s and paid for itself within the first round of queries. But a simple sync never saw updates or deletes.
* **Files:** on a 274 MB CSV, DuckDB querying the file directly was 1.38x faster than PostgreSQL after loading it, and needed no load (PostgreSQL's took 2.58 s). After a 0.57 s import into a DuckDB table it was 14.92x faster, and plain Python was 0.19x.
* **Correctness and limits:** every engine returned the same answers as PostgreSQL in every round. In the baseline tests, no engine used more than 758 MB of its 2 GiB budget (peak memory is measured there only, not in the pair and file tests).

## Builds, configuration and hardware

**Hardware**

* AMD Ryzen 7 9700X, 8 cores and 16 threads, SMT on, L3 cache 32 MiB shared by all cores
* 32 GB RAM
* WD Blue SN5100 1 TB NVMe SSD, btrfs with `compress=zstd:1`. The containers' data and the embedded engines' files are on this same filesystem.
* Fedora 44, Linux 7.2.4, CPU governor `performance` during the run (driver `amd-pstate-epp`), Podman 5.8.4, rootful (needed for CPU pinning)

**Software**

| engine | version | client |
|---|---|---|
| PostgreSQL | 18.6 (image `postgres:18.6`) | psycopg 3.3.6 (libpq) |
| MariaDB | 13.0.2 (image `mariadb:13.0.2`) | MariaDB Connector/Python 2.0.0rc2 with its C extension (`mariadb[binary]`) |
| ClickHouse | 26.9.24.8 (image `clickhouse/clickhouse-server:26.9.24.8`) | clickhouse-connect 1.11.0 (HTTP) |
| DuckDB | 2.0.0 | its Python module |
| SQLite | 3.54.1 | apsw 3.54.1.0, which bundles that SQLite release |

Python 3.14.7 runs the client. Every version was the latest release on 22 October 2026. The only exception is the MariaDB connector, where 2.0.0rc2 is the newest build and 1.1.14 the newest stable. The stable one needs MariaDB's C library installed system-wide, while the release candidate ships it inside the package.

**The same nominal budget for every engine**

* **CPU:** 2 physical cores (4 threads) each, pinned, so no other part of the test shares them. In a pair, the source and the target are on different cores. The Python client gets 2 more cores of its own while it measures a server. The script reads the CPU layout itself (`lscpu`) and needs at least 6 physical cores.
* **Memory:** a 2 GiB budget each, no swap. It is a hard cap only for the three containers (`--memory 2g --memory-swap 2g`). DuckDB's `memory_limit=2GiB` limits its buffer manager, not the whole Python process, and SQLite has no cap. The main cache is 1 GiB: PostgreSQL `shared_buffers`, the MariaDB buffer pool, SQLite `cache_size` (per connection; the 8-thread test opens 8, but each can only cache the ~95 MB file). The report measures every engine's peak memory in the baseline tests instead (408 to 758 MB here).
* **Threads per query:** at most 4 (PostgreSQL parallel workers, ClickHouse `max_threads`, DuckDB `threads`). MariaDB with InnoDB and SQLite run each query on one thread by design.
* **Text:** compared byte by byte everywhere (MariaDB `utf8mb4_bin`, PostgreSQL `C.UTF-8`, the others by default). `status` is plain text in every engine rather than an engine-specific enum.
* **Loading:** each engine's fastest native path. PostgreSQL `COPY`, MariaDB `LOAD DATA` with `unique_checks=0` and `foreign_key_checks=0` and a 2 GiB redo log, ClickHouse CSV insert, DuckDB `COPY`, SQLite `executemany` in one transaction.
* **Writes:** each engine's default durability, one transaction per row. ClickHouse 26 buffers client inserts by default (`async_insert=1`) and makes each one wait for a flush. The benchmark sets `async_insert=0`, so an INSERT is finished when it returns, as in every other engine.
* **Containers:** run without the default seccomp profile, so io_uring is available to all of them alike.
* **Isolation and repeats:** only the containers a test needs are running. Everything runs 3 times, each round reloads the data and rotates the order of the engines, and each value is the median of the rounds.

Every setting is in `bench.py`, in the `CONTAINERS` table and the engine classes, and the report repeats them with the values the container runtime actually applied.

## Benchmark

One run of `python bench.py` does these steps, 3 rounds each:

* **load**: bulk-load 100,000 customers, 10,000 products and 1,000,000 orders from the same CSV files into every engine, then build the primary keys and indexes
* **Q1-Q4**: four analytic queries, the same SQL on every engine (join and group, window function, `count(DISTINCT)`, filtered slice). 1 first run + 5 warm runs, median of the warm runs
* **ping**: 300 x `SELECT 1`, the cost of one call: the network for servers, a Python call for embedded engines
* **lookup**: 300 random primary-key lookups, then 8 threads x 100 lookups at once
* **insert**: 300 single-row INSERTs, then 3 x 10,000-row INSERTs
* **update**: 200 single-row UPDATEs by primary key (ClickHouse: 30, as mutations)
* **pair**: copy all tables from source to target, run the queries on the copy and live through the source, sync 1, 1,000 and 50,000 new rows, then update and delete 1,000 rows at the source and see what the copy shows
* **files**: three tasks on a 5,000,000-row (274 MB) CSV: F1 filter and aggregate, F2 top 10 customers, F3 filter and export to a new CSV. Five ways: PostgreSQL after `COPY`, DuckDB after importing into a table, DuckDB on the CSV directly, DuckDB on a Parquet copy, and Python's `csv` module, which stands in for parsing the file in application code

**What a pair is.** A pair is two databases used together, each for what it does best. One stores the live data, and a sync copies it into the other, where the reports run. For example, PostgreSQL -> DuckDB means the application writes to PostgreSQL, a sync copies the tables into DuckDB, and the analytic queries run on DuckDB.

**The answer key.** Every answer on every engine is compared with PostgreSQL's, row by row, and the run fails on any difference. PostgreSQL has no other special role. It runs under the same limits as everyone else, and the comparison happens after the timed runs, so it never adds to a measurement.

## Results

The full results are in `results.txt`, and every single measurement is in `results.csv`. Every number in this section comes from `results.txt`, except in the last two subsections, which say where theirs come from.

### Engines on their own

Relative speed vs PostgreSQL, the median of the per-round ratios. Above 1.00 = faster than PostgreSQL.

* col-1 : PostgreSQL 18.6
* col-2 : MariaDB 13.0.2
* col-3 : ClickHouse 26.9.24.8
* col-4 : DuckDB 2.0.0
* col-5 : SQLite 3.54.1

| col-1 | col-2 | col-3 | col-4 | col-5 | test |
|---|---|---|---|---|---|
| 1.00 | 0.52 | 6.07 | 26.52 | 0.31 | Q1 revenue by month and country |
| 1.00 | 0.26 | 3.17 | 29.47 | 0.33 | Q2 top 3 products per category |
| 1.00 | 0.80 | 11.14 | 46.57 | 1.26 | Q3 distinct customers, average |
| 1.00 | 0.28 | 0.44 | 4.24 | 0.45 | Q4 recent slice by status |
| 1.00 | 1.49 | 0.02 | 0.14 | 3.49 | point lookup by primary key |
| 1.00 | 3.75 | 0.04 | 0.27 | 2.17 | point lookups, 8 threads |
| 1.00 | 1.13 | 0.37 | 0.58 | 0.28 | single-row INSERT |
| 1.00 | 1.29 | 8.83 | 0.20 | 3.04 | 10,000-row INSERT |
| 1.00 | 1.12 | 0.00 | 0.63 | 0.26 | single-row UPDATE by primary key |
| 1.00 | 0.36 | 2.18 | 1.47 | 0.35 | bulk load |

The absolute values, median of 3 rounds:

| ms, lower is better | PostgreSQL | MariaDB | ClickHouse | DuckDB | SQLite |
|---|---|---|---|---|---|
| Q1 revenue by month and country | 331.6 | 644.6 | 54.8 | 12.6 | 1,017.4 |
| Q2 top 3 products per category | 162.1 | 623.0 | 50.6 | 5.5 | 484.1 |
| Q3 distinct customers, average | 1,103.0 | 1,375.2 | 99.0 | 23.8 | 906.9 |
| Q4 recent slice by status | 5.7 | 20.3 | 12.8 | 1.4 | 12.7 |
| point lookup by primary key | 0.033 | 0.017 | 1.706 | 0.242 | 0.007 |
| point lookup minus the `SELECT 1` round trip | 0.017 | 0.007 | 0.891 | 0.185 | 0.006 |

| per second, higher is better | PostgreSQL | MariaDB | ClickHouse | DuckDB | SQLite |
|---|---|---|---|---|---|
| point lookups, 8 threads | 39,140 | 142,785 | 1,623 | 10,552 | 85,089 |
| single-row INSERT | 2,693 | 3,164 | 1,005 | 1,564 | 761 |
| 10,000-row INSERT, rows | 477,795 | 614,846 | 4,065,793 | 103,442 | 1,453,954 |
| single-row UPDATE by primary key | 2,655 | 3,043 | 1 | 1,666 | 725 |

| loading and size | PostgreSQL | MariaDB | ClickHouse | DuckDB | SQLite |
|---|---|---|---|---|---|
| bulk load, all tables (s) | 0.55 | 1.58 | 0.25 | 0.37 | 1.51 |
| index and key build (s) | 0.46 | 2.07 | - | - | 0.73 |
| size of the 3 tables (MB) | 133 | 103 | 27 | 37 | 95 |
| peak memory, baseline tests (MB) | 425 | 758 | 425 | 434 | 408 |

Noise: most values vary by less than 15% between rounds. These vary more and should not be read closely: MariaDB Q1 (±29%), the lookup times with the round trip removed (up to ±50%) and MariaDB's 8-thread lookups (±17%). Where two engines' ranges overlap, `results.txt` marks a tie (`~`) instead of a winner: PostgreSQL and MariaDB on single-row INSERT and UPDATE, and MariaDB and SQLite on the lookup without the round trip.

### Pairs

| | PG -> DuckDB | MariaDB -> ClickHouse | SQLite -> DuckDB | PG -> ClickHouse |
|---|---|---|---|---|
| full copy of all tables (s) | 0.19 | 0.49 | 0.32 | 0.38 |
| the 4 queries on the copy vs on the source | 28.1x faster | 11.8x faster | 57.2x faster | 7.5x faster |
| break-even, rounds of the 4 queries | 0.12 | 0.20 | 0.13 | 0.27 |
| lag for 1,000 new rows (ms) | 10.9 | 8.4 | 32.1 | 4.7 |
| updates seen by an incremental sync | 0 of 1,000 | 0 of 1,000 | 0 of 1,000 | 0 of 1,000 |
| deleted rows still in the copy | 1,000 of 1,000 | 1,000 of 1,000 | 1,000 of 1,000 | 1,000 of 1,000 |

The incremental sync copies only rows with a new id, so it never sees changes to existing rows. A full refresh of the 200,000-row test table fixes that in 0.07 to 0.12 s. A real pipeline needs change-data-capture or periodic full refreshes.

### Files

A 5,000,000-row CSV (274 MB), median ms:

| | PostgreSQL (after load) | DuckDB table (after import) | DuckDB on the CSV | DuckDB on Parquet | Python csv module |
|---|---|---|---|---|---|
| F1 filter + aggregate | 137.3 | 7.1 | 272.1 | 19.4 | 2,066.8 |
| F2 top 10 customers | 784.4 | 30.0 | 241.1 | 49.1 | 2,477.1 |
| F3 filter + export to CSV | 309.7 | 43.2 | 363.4 | 65.7 | 1,920.8 |
| all three | 1,233.1 | 80.3 | 867.5 | 134.2 | 6,385.5 |
| relative speed vs PostgreSQL, all three | 1.00 | 14.92 | 1.38 | 9.13 | 0.19 |

Before the tasks: loading the CSV into PostgreSQL took 2.58 s, importing it into a DuckDB table 0.57 s, and converting it to Parquet with DuckDB 0.48 s (116 MB). DuckDB on the CSV needs none of that.

### ClickHouse 26.9

Two ClickHouse results looked wrong next to version 1, which used ClickHouse 25.8. So I ran only the ClickHouse baseline with 25.8.33.6 and 26.9.23.15, on the same harness, limits and data. These are separate diagnostic runs, not part of `results.txt`:

| | 25.8.33.6 | 26.9.23.15 |
|---|---|---|
| Q1 / Q2 / Q3 / Q4 (ms) | 17.8 / 16.9 / 36.4 / 5.0 | 56.1 / 48.2 / 96.0 / 12.6 |
| single-row UPDATE (per second) | 250 | 0.8 |

* **Joins:** Q1 without its join takes 17.7 ms on 26.9, the same as all of Q1 on 25.8, so the extra time is the join. 26.9 runs it with `SpillingHashJoin(ConcurrentHashJoin)`. `join_algorithm = 'hash'` brings Q1 to 43.5 ms. The other defaults that changed between the versions (`join_algorithm`, `use_concurrency_control`, `query_plan_join_swap_table`) make no difference.
* **Updates:** after the 300 single-row inserts that come before the update test, 26.9 still has unmerged parts, and each `ALTER TABLE ... UPDATE` waits for background merges: 866 ms median, up to 1.7 s. After `OPTIMIZE TABLE ... FINAL` it takes 3.7 ms. 25.8 had already merged its parts and took 3.4 ms.

26.9.24.8, used for the results above, behaves the same: Q1 54.8 ms, about 1 update per second.

### Version effects since version 1

These comparisons use an earlier full run of this same harness with DuckDB 1.5.6 and SQLite 3.51.2. It isn't in the repository, so treat it as context. DuckDB 2.0.0 is faster than 1.5.6 on the queries (Q1 12.6 ms, against 19.0 ms) but slower on 10,000-row `INSERT ... VALUES` (103,442 rows per second, against 290,222). SQLite 3.54.1 through apsw did 761 single-row inserts per second, against 1,000 for SQLite 3.51.2 through Python's `sqlite3`. That mixes two changes, the SQLite release and the Python wrapper, so it doesn't say which one is responsible.

## Changes since version 1

* MariaDB Connector/Python with its C extension instead of PyMySQL, which is pure Python and made MariaDB's row-by-row numbers look worse than the server is.
* InnoDB's bulk-load path and a larger redo log, so MariaDB loads through its fastest path like the others.
* A start-of-month expression for MariaDB without string functions, `DATE_ADD(LAST_DAY(d - INTERVAL 1 MONTH), INTERVAL 1 DAY)`, instead of `DATE_FORMAT`.
* Current releases of every engine and library (version 1 had PostgreSQL 16, MariaDB 11.8, ClickHouse 25.8, DuckDB 1.5).
* Byte-order text in every engine, io_uring available in every container, the same 2 GiB budget for every engine (a hard cap for the containers only), and each engine's baseline peak memory measured.
* New: the file-analytics test, with relative-speed tables in the style of [Small Datum](https://smalldatum.blogspot.com/).

## Run it

You need Linux with at least 6 physical CPU cores, Docker or Podman, Python 3.11 or newer (tested on 3.14), about 6 GB of free RAM, and about 3 GB of disk for the container images plus 1 GB for the working files.

```
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python bench.py
```

A run takes about 17 minutes. The first one also downloads three container images.

| setting | what it does |
|---|---|
| `python bench.py down` | remove the containers by hand, for example after Ctrl+C |
| `LAB_ORDERS=5000000` | rows in the `orders` table, default 1,000,000 |
| `LAB_FILE_ORDERS=10000000` | rows in the file for the file tests, default 5,000,000 |
| `LAB_RUNS=10` | warm runs per query in each round, default 5 |
| `LAB_ROUNDS=5` | how many times everything is repeated, default 3 |
| `KEEP=1` | leave the containers running afterwards |
| `LAB_DOCKER="sudo podman"` | the container command. The default is `docker`, or `podman` if Docker is missing. Rootless Podman usually cannot pin CPUs, so use `sudo podman`. |

The script touches only its own containers (`hybridlab-*`) and network (`hybridlab`), which listen on 127.0.0.1 ports 5434, 3307 and 8124, and deletes them at the end. Working files go to a `.work-*` folder next to the script, on real disk rather than a RAM-backed `/tmp`, and are deleted too. The container images stay, so the next run starts faster.

## Output

`python bench.py` prints one progress line per stage, then the full report, and appends to two files:

* **`results.txt`**: the readable report. It has the machine and versions, the compute caps the container runtime actually applied, the memory checks (out-of-memory kills, swapping, page faults), the full specification with every query's SQL, and then the results. Each value is the median of the rounds, `±x%` is half the range between rounds, `*` marks a clear winner and `~` a tie within the noise.
* **`results.csv`**: the raw numbers, one value per row, every round separately, and every single query execution. Columns: `run, round, section, scope, engine, test, metric, value, unit`.

If any engine's answer differs from PostgreSQL's in any round, the script prints `DIFFERENT ANSWERS` and exits with an error.

## Limits

* One machine, one synthetic data set, and one client at a time apart from the 8-thread test.
* The client is Python, which adds overhead to every engine. The embedded engines also share their cores with the Python code calling them.
* The L3 cache is shared by all cores, so the client and the two sides of a pair still share it.
* The syncs are plain `INSERT ... SELECT` by id, not change-data-capture.
* During this run other programs on the machine swapped 18,213 pages, and the Python process had to read back 21 pages in round 1. `results.txt` lists where.
* Trust the ratios more than the absolute times.

## License

MIT, see `LICENSE`.
