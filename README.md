# db-pairs-benchmark

Can two databases beat one? This benchmark measures five database engines on their own, four pairs of them working together, and DuckDB analysing files directly, all under the same CPU and memory budget, with every answer checked for correctness.

It started from a discussion about using PostgreSQL for an application and DuckDB for analysis. This is version 2: it fixes MariaDB-specific problems reviewers found in version 1, uses the current release of every engine and library, and adds a file-analytics test.

## tl;dr

From one run on an AMD Ryzen 7 9700X with 1,000,000 orders, 3 rounds. Ratios are relative to PostgreSQL, where above 1.00 means faster.

* **Analytics:** DuckDB ran the three heavy queries 22.35x to 42.95x faster than PostgreSQL, and ClickHouse 3.23x to 10.69x faster. MariaDB and SQLite were slower than PostgreSQL on Q1 and Q2.
* **Small reads and writes:** MariaDB led concurrent point lookups (3.87x PostgreSQL) and was ahead on single-row inserts (1.13x) and updates (1.14x). SQLite had the fastest single lookup, since it has no network round trip. ClickHouse managed about 1 single-row UPDATE per second (see [ClickHouse 26.9](#clickhouse-269)).
* **Pairs:** copying the data from a row store into DuckDB or ClickHouse took 0.15 to 0.46 s and paid for itself within the first round of queries. But a simple sync never saw updates or deletes.
* **Files:** on a 274 MB CSV, DuckDB querying the file directly was 1.35x faster than PostgreSQL after loading it, and needed no load (PostgreSQL's took 2.26 s). After a 0.54 s import into a DuckDB table it was 15.28x faster, and plain Python was 0.19x.
* **Correctness and limits:** every engine returned the same answers as PostgreSQL in every round. In the baseline tests, no engine used more than 755 MB of its 2 GiB budget (peak memory is measured there only, not in the pair and file tests).

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
* **Memory:** a 2 GiB budget each, no swap. It is a hard cap only for the three containers (`--memory 2g --memory-swap 2g`). DuckDB's `memory_limit=2GiB` limits its buffer manager, not the whole Python process, and SQLite has no cap. The main cache is 1 GiB: PostgreSQL `shared_buffers`, the MariaDB buffer pool, SQLite `cache_size` (per connection; the 8-thread test opens 8, but each can only cache the ~95 MB file). The report measures every engine's peak memory in the baseline tests instead (401 to 755 MB here).
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
| 1.00 | 0.47 | 5.22 | 22.35 | 0.34 | Q1 revenue by month and country |
| 1.00 | 0.26 | 3.23 | 29.14 | 0.41 | Q2 top 3 products per category |
| 1.00 | 0.78 | 10.69 | 42.95 | 1.32 | Q3 distinct customers, average |
| 1.00 | 0.32 | 0.42 | 3.93 | 0.56 | Q4 recent slice by status |
| 1.00 | 1.64 | 0.01 | 0.10 | 3.49 | point lookup by primary key |
| 1.00 | 3.87 | 0.04 | 0.25 | 1.94 | point lookups, 8 threads |
| 1.00 | 1.13 | 0.37 | 0.56 | 0.27 | single-row INSERT |
| 1.00 | 1.10 | 6.21 | 0.15 | 2.97 | 10,000-row INSERT |
| 1.00 | 1.14 | 0.00 | 0.62 | 0.28 | single-row UPDATE by primary key |
| 1.00 | 0.38 | 1.92 | 1.23 | 0.31 | bulk load |

The absolute values, median of 3 rounds:

| ms, lower is better | PostgreSQL | MariaDB | ClickHouse | DuckDB | SQLite |
|---|---|---|---|---|---|
| Q1 revenue by month and country | 269.9 | 581.5 | 51.8 | 11.9 | 783.8 |
| Q2 top 3 products per category | 154.1 | 591.9 | 47.6 | 5.3 | 378.5 |
| Q3 distinct customers, average | 994.5 | 1,270.7 | 93.1 | 23.2 | 757.1 |
| Q4 recent slice by status | 5.3 | 16.7 | 12.5 | 1.3 | 9.4 |
| point lookup by primary key | 0.023 | 0.014 | 1.565 | 0.236 | 0.007 |
| point lookup minus the `SELECT 1` round trip | 0.012 | 0.006 | 0.853 | 0.181 | 0.006 |

| per second, higher is better | PostgreSQL | MariaDB | ClickHouse | DuckDB | SQLite |
|---|---|---|---|---|---|
| point lookups, 8 threads | 44,629 | 175,607 | 1,759 | 11,018 | 87,812 |
| single-row INSERT | 2,952 | 3,337 | 1,117 | 1,652 | 814 |
| 10,000-row INSERT, rows | 710,402 | 774,340 | 4,293,970 | 107,085 | 2,046,873 |
| single-row UPDATE by primary key | 2,822 | 3,229 | 1 | 1,762 | 798 |

| loading and size | PostgreSQL | MariaDB | ClickHouse | DuckDB | SQLite |
|---|---|---|---|---|---|
| bulk load, all tables (s) | 0.43 | 1.14 | 0.23 | 0.35 | 1.41 |
| index and key build (s) | 0.40 | 1.66 | - | - | 0.58 |
| size of the 3 tables (MB) | 133 | 103 | 27 | 37 | 95 |
| peak memory, baseline tests (MB) | 401 | 755 | 478 | 475 | 441 |

Noise: most values vary by less than 15% between rounds. These vary more and should not be read closely: peak memory (up to ±72%, ClickHouse), MariaDB Q4 (±46%), and SQLite's point lookup (±10%). Where two engines' ranges overlap, `results.txt` marks a tie (`~`) instead of a winner: PostgreSQL and MariaDB on the p95 of single-row INSERT, and MariaDB and SQLite on the lookup without the round trip.

### Pairs

| | PG -> DuckDB | MariaDB -> ClickHouse | SQLite -> DuckDB | PG -> ClickHouse |
|---|---|---|---|---|
| full copy of all tables (s) | 0.15 | 0.46 | 0.31 | 0.35 |
| the 4 queries on the copy vs on the source | 33.3x faster | 12.1x faster | 47.5x faster | 6.9x faster |
| break-even, rounds of the 4 queries | 0.11 | 0.21 | 0.17 | 0.29 |
| lag for 1,000 new rows (ms) | 6.8 | 6.8 | 30.5 | 4.4 |
| updates seen by an incremental sync | 0 of 1,000 | 0 of 1,000 | 0 of 1,000 | 0 of 1,000 |
| deleted rows still in the copy | 1,000 of 1,000 | 1,000 of 1,000 | 1,000 of 1,000 | 1,000 of 1,000 |

The incremental sync copies only rows with a new id, so it never sees changes to existing rows. A full refresh of the 200,000-row test table fixes that in 0.07 to 0.12 s. A real pipeline needs change-data-capture or periodic full refreshes.

### Files

A 5,000,000-row CSV (274 MB), median ms:

| | PostgreSQL (after load) | DuckDB table (after import) | DuckDB on the CSV | DuckDB on Parquet | Python csv module |
|---|---|---|---|---|---|
| F1 filter + aggregate | 129.0 | 6.2 | 255.9 | 17.6 | 1,874.3 |
| F2 top 10 customers | 728.2 | 28.2 | 226.6 | 45.1 | 2,348.5 |
| F3 filter + export to CSV | 255.8 | 38.6 | 341.7 | 58.0 | 1,812.2 |
| all three | 1,111.0 | 73.5 | 823.1 | 121.7 | 6,023.1 |
| relative speed vs PostgreSQL, all three | 1.00 | 15.28 | 1.35 | 9.49 | 0.19 |

Before the tasks: loading the CSV into PostgreSQL took 2.26 s, importing it into a DuckDB table 0.54 s, and converting it to Parquet with DuckDB 0.44 s (116 MB). DuckDB on the CSV needs none of that.

### ClickHouse 26.9

Two ClickHouse results looked wrong next to version 1, which used ClickHouse 25.8. So I ran only the ClickHouse baseline with 25.8.33.6 and 26.9.23.15, on the same harness, limits and data. These are separate diagnostic runs, not part of `results.txt`:

| | 25.8.33.6 | 26.9.23.15 |
|---|---|---|
| Q1 / Q2 / Q3 / Q4 (ms) | 17.8 / 16.9 / 36.4 / 5.0 | 56.1 / 48.2 / 96.0 / 12.6 |
| single-row UPDATE (per second) | 250 | 0.8 |

* **Joins:** Q1 without its join takes 17.7 ms on 26.9, the same as all of Q1 on 25.8, so the extra time is the join. 26.9 runs it with `SpillingHashJoin(ConcurrentHashJoin)`. `join_algorithm = 'hash'` brings Q1 to 43.5 ms. The other defaults that changed between the versions (`join_algorithm`, `use_concurrency_control`, `query_plan_join_swap_table`) make no difference.
* **Updates:** after the 300 single-row inserts that come before the update test, 26.9 still has unmerged parts, and each `ALTER TABLE ... UPDATE` waits for background merges: 866 ms median, up to 1.7 s. After `OPTIMIZE TABLE ... FINAL` it takes 3.7 ms. 25.8 had already merged its parts and took 3.4 ms.

26.9.24.8, used for the results above, behaves the same: Q1 51.8 ms, about 1 update per second.

### Version effects since version 1

These comparisons use an earlier full run of this same harness with DuckDB 1.5.6 and SQLite 3.51.2. It isn't in the repository, so treat it as context. DuckDB 2.0.0 is faster than 1.5.6 on the queries (Q1 11.9 ms, against 19.0 ms) but slower on 10,000-row `INSERT ... VALUES` (107,085 rows per second, against 290,222). SQLite 3.54.1 through apsw did 814 single-row inserts per second, against 1,000 for SQLite 3.51.2 through Python's `sqlite3`. That mixes two changes, the SQLite release and the Python wrapper, so it doesn't say which one is responsible.

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
* During this run no other program on the machine swapped, and the Python process read back a few pages from disk in round 1 only (DuckDB 14, PostgreSQL -> DuckDB 1, files 3). `results.txt` lists where.
* Trust the ratios more than the absolute times.

## License

MIT, see `LICENSE`.
