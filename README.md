# db-pairs-benchmark

A benchmark that answers one question for decision makers: which database, or which pair of databases, fits which job, and what does each choice cost?

It started from a discussion about using two databases together instead of one, such as PostgreSQL for the application and DuckDB for analysis. This project measures what that buys you and what it costs, for five engines and four pairs, under equal resources.

* Engines on their own: PostgreSQL, MariaDB, ClickHouse (containers, Docker or Podman), DuckDB, SQLite (embedded libraries that run inside the Python process).
* Pairs, data moving from a source to a target: PostgreSQL -> DuckDB, MariaDB -> ClickHouse, SQLite -> DuckDB, PostgreSQL -> ClickHouse.

The project is seven files: `bench.py` (all the code, including the container setup), `requirements.txt`, `README.md`, `LICENSE` (MIT), `.gitignore`, and two logs written by every run: `results.txt` (readable) and `results.csv` (raw numbers).

## Results (1,000,000 orders, 3 rounds, equal resources)

The `results.txt` and `results.csv` in this repository are from one run on an AMD Ryzen 7 9700X (8 cores, 32 GB RAM, Fedora 44) with the CPU governor on `performance` and Podman 5.8. The run was quiet: other programs swapped 15 pages and the Python process had to read back 1 page during the whole run, so most values vary by 0 to 3% between rounds.

Median of the rounds, with half the range between rounds. Milliseconds, lower is better:

| | PostgreSQL | MariaDB | ClickHouse | DuckDB | SQLite |
|---|---|---|---|---|---|
| Q1 revenue by month, country | 342 ±1% | 1,012 ±15% | **18.1 ±4% ~** | **18.7 ±1% ~** | 865 ±1% |
| Q2 top 3 products per category | 162 ±3% | 640 ±1% | 16.8 ±1% | **9.7 ±0%** | 451 ±1% |
| Q3 distinct customers, average | 1,041 ±0% | 1,625 ±2% | 36.1 ±1% | **32.2 ±1%** | 855 ±0% |
| point lookup by key | 0.023 | 0.038 | 1.205 | 0.180 | **0.007** |
| point lookup minus the `SELECT 1` round trip | 0.011 | 0.022 | 0.616 | 0.143 | **0.007** |

Per second, higher is better:

| | PostgreSQL | MariaDB | ClickHouse | DuckDB | SQLite |
|---|---|---|---|---|---|
| single-row INSERT | **3,023 ~** | **3,171 ~** | 1,309 | 1,713 | 1,043 |
| 10,000-row INSERT, rows | 716,755 | 741,771 | **5,052,202** | 318,623 | 2,356,909 |
| point lookups, 8 threads | 44,173 | 26,764 | 2,539 | 12,837 | **68,267** |

`~` marks a tie within the noise: ClickHouse and DuckDB on Q1, PostgreSQL and MariaDB on single-row INSERT and UPDATE.

The pairs:

| | PG -> DuckDB | MariaDB -> ClickHouse | SQLite -> DuckDB | PG -> ClickHouse |
|---|---|---|---|---|
| full copy of all tables | 0.17 s | 0.29 s | 0.37 s | 0.30 s |
| the 4 queries on the copy vs on the source | 24.5x faster | 42.8x faster | 36.2x faster | 20.3x faster |
| lag for 1,000 new rows | 7.3 ms | 6.3 ms | 35.0 ms | 4.1 ms |
| updates seen by an incremental sync | 0 of 1,000 | 0 of 1,000 | 0 of 1,000 | 0 of 1,000 |
| deleted rows still in the copy | 1,000 of 1,000 | 1,000 of 1,000 | 1,000 of 1,000 | 1,000 of 1,000 |

A full refresh of the 200,000-row test table fixes the missed updates and deletes in 0.07 to 0.09 s. Every engine returned the same answers as PostgreSQL in every round.

What this says, for this data and these limits:

* **Analytics:** on the three heavy queries (Q1 to Q3), DuckDB and ClickHouse are 10x to 56x faster than PostgreSQL, MariaDB and SQLite. On Q4, a small slice that PostgreSQL serves from an index, the gap almost closes: PostgreSQL 6.0 ms, ClickHouse 5.2 ms, DuckDB 1.5 ms.
* **Small reads and writes:** PostgreSQL and MariaDB lead on single-row inserts and updates, and SQLite on point lookups. ClickHouse is the slowest on lookups and updates (its updates are mutations, 245 per second), and SQLite has the slowest single-row inserts.
* **Bulk writes:** ClickHouse loads and batch-inserts fastest.
* **Pairs:** copying to an analytic engine pays for itself in the first round of queries. The cost is freshness: a sync that only looks for new ids never sees updates or deletes, so a real pipeline needs change-data-capture or periodic full refreshes.

Your numbers will differ, so run it and read your own `results.txt`.

## Run it

You need Linux with at least 6 physical CPU cores, Docker or Podman, Python 3.11 or newer (tested on 3.12 and 3.14), about 6 GB of free RAM and about 2.5 GB of disk for the container images.

```
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python bench.py
```

A run takes 6 to 11 minutes at the default size (3 rounds), depending on the machine. The first run also downloads the three container images.

Other commands and settings:

| | |
|---|---|
| `python bench.py down` | remove the containers by hand, for example after Ctrl+C |
| `LAB_ORDERS=5000000 python bench.py` | rows in the big `orders` table, default 1,000,000 |
| `LAB_RUNS=10 python bench.py` | warm runs per query in each round, default 5 |
| `LAB_ROUNDS=5 python bench.py` | how many times the whole measurement is repeated, default 3 |
| `KEEP=1 python bench.py` | leave the containers running afterwards |
| `LAB_DOCKER="sudo podman" python bench.py` | the container command. The default is `docker`, or `podman` if Docker is missing. Rootless Podman often cannot pin CPUs (no `cpuset` cgroup controller), so use `sudo podman` there. |

The script only touches its own containers (`hybridlab-pg`, `hybridlab-mariadb`, `hybridlab-clickhouse`) and its own network (`hybridlab`). They listen only on 127.0.0.1, ports 5434, 3307 and 8124. Containers and network are deleted at the end. The working files (data, DuckDB and SQLite files, DuckDB extensions) go to a `.work-*` folder next to the script, on real disk, which is removed too. The container images stay, so the next run starts faster.

## How the comparison is kept fair

Every engine gets the same budget, and the report checks that it held.

* **CPU:** 2 physical cores (all their threads) each, pinned so nothing else in the test shares them. The script reads which CPUs belong to which core (`lscpu`) and builds three sets: one for PostgreSQL, MariaDB and SQLite, one for ClickHouse and DuckDB, and one for the Python client when it measures a server. In a pair, the source and the target are on different cores. No editing is needed for a different machine.
* **Memory:** 2 GB each and no swap. The containers get a memory limit from the container runtime, DuckDB gets `memory_limit=2GB`, and each engine's main cache is about 1 GB (PostgreSQL `shared_buffers`, MariaDB buffer pool, SQLite `cache_size`).
* **Parallelism:** at most 4 threads per query everywhere: PostgreSQL parallel workers, ClickHouse `max_threads`, DuckDB `threads`. MariaDB and SQLite run a query on one thread anyway.
* **Isolation:** only the containers a test needs are running, and the others are stopped.
* **Repeats:** the whole measurement runs 3 times (rounds). Each round reloads the data and changes the order of the engines and pairs, so no engine always goes first or last. Each reported value is the median of the rounds, with the spread.
* **Network:** servers are reached over the network and embedded engines are not, so a `SELECT 1` round trip is measured on each engine. The report also shows the lookup time with that round trip taken out.
* **Checks:** the report header shows the limits the container runtime actually applied, whether a container ran out of memory, and whether the Python process (the client, and the engine itself for DuckDB and SQLite) had to read pages back from disk or swap. Python's garbage collector runs between tests, never during one.

What it cannot control: the CPU clock speed (the governor is printed in the header) and other programs on the machine. Close what you can before a run.

## What the script prints

The raw output, in order:

1. One progress line per stage, with seconds since the start (the container runtime's own output is kept quiet):
   ```
   [     0s] containers via 'sudo podman'; CPUs 0-1,8-9 | 2-3,10-11 | client 4-5,12-13
   [     3s] csv ready, 1,000,000 orders; 3 rounds
   [    18s] round 1/3  baseline PostgreSQL
   [    45s] round 1/3  baseline MariaDB
   ...
   [   124s] round 1/3  pair PostgreSQL -> DuckDB
   ...
   ```
2. The full report, which is exactly the text appended to `results.txt` (see below).
3. A last line such as `wrote results.txt and results.csv (1962 rows)`.

If any engine returns a different answer from PostgreSQL in any round, the script prints `DIFFERENT ANSWERS: ...` and exits with an error.

## What `results.txt` contains

Every run appends one report, so the file is a history. Each report has these parts:

1. **Header**: date, row counts, machine, CPU governor, free RAM, engine versions and the memory checks, then a **compute caps** table: CPUs, cores, RAM cap, swap, threads per query and main cache for every engine and the client, with the container values read back from the container runtime.
2. **Test specification**: the commands, how the data is made, the fairness rules, the exact SQL of the four queries, and what each test does and how often.
3. **Section 1, baseline**: each engine on its own, one column per engine.
   * 1: bulk load speed, index build time, size on disk
   * 1a: the four analytic queries, median in ms, plus `count(*)`
   * 1b: the same queries, first run
   * 1c: `SELECT 1` round trip, point lookups (median, with the round trip taken out, p95, 8 threads at once), single-row INSERT, 10,000-row INSERT, single-row UPDATE
4. **Section 2, pairs**: one block per pair.
   * full copy time per table
   * the same tests as the baseline, side by side for the source, the target loaded directly, the target as a synced copy, and the source read live from the target
   * speed-up of the copy over the source, and the break-even point (how many rounds of the queries before copying has paid for itself)
   * incremental sync: lag after 1, 1,000 and 50,000 new rows
   * changes to existing rows: 1,000 updates and 1,000 deletes at the source, how many the copy shows, and what a full refresh costs
5. **Section 3, pair summary**: one table to compare the four pairs.
6. **Notes**: the main caveats.

Every value is the median of the rounds. `±x%` is half the range between the lowest and highest round, as a share of the median. No `±` means every round gave the same value.

* `*` marks a clear winner: its range across the rounds does not overlap any other engine's range.
* `~` marks a tie: the best values overlap, so the difference between them is within the noise.
* No mark means the row has nothing to pick.

A time column is milliseconds or seconds, as its label says.

## What `results.csv` contains

The same numbers as raw data, one number per row, appended on every run. Columns: `run, round, section, scope, engine, test, metric, value, unit`. Every round is its own row, so you can check the spread yourself.

* `setup`: row counts, warm runs, rounds, and each engine's CPU and RAM cap as applied.
* `baseline`: every number of section 1, one row per engine, test and metric.
* `pair`: for each pair (`scope`, such as `postgres->duckdb`), the source, the target loaded directly, the synced copy, the source read live from the target, and the sync results (copy time, lag, break-even, updates and deletes seen, answers equal).
* `raw query runs`: every single query execution (first run and each warm run) in ms, not only the medians.

It opens directly in a spreadsheet. A pivot on `engine` and `test` with the median over `round` reproduces the tables in `results.txt`. The rows also include host swap and the Python process's major page faults for every test.

## How to read the results

* Pick the engine by the job: analytic questions favour ClickHouse and DuckDB, many small reads and writes favour PostgreSQL, MariaDB and SQLite.
* For a pair, the copy is much faster to query but is only as fresh as the last sync, and a sync that looks only at new ids never sees updates or deletes. Reading the source live is always current but slower than the copy.
* A run fails if any engine's answer differs from PostgreSQL's, so a fast result is also a correct one.

## Limits

One machine, one synthetic data set, one client at a time apart from the 8-thread test. Python adds client overhead to every number, embedded engines share their cores with the Python code calling them, and other programs still run on the host, so trust ratios more than absolute values. Syncs are plain `INSERT ... SELECT` by id through DuckDB `ATTACH` and ClickHouse database engines, not change-data-capture, so they show what a simple pipeline costs and misses. A copy made by the sync has no primary key or secondary indexes.

## License

MIT, see `LICENSE`.
