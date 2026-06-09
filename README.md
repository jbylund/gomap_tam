# treedb_pgext

A PostgreSQL [Table Access Method](https://www.postgresql.org/docs/current/tableam.html) backed by [TreeDB](../gomap/TreeDB), a mmap'd B+tree storage engine written in Go.

Tables using this AM store rows in TreeDB instead of PostgreSQL's heap. Point lookups and random-access writes go through the B+tree directly, bypassing heap I/O.

## Architecture

```
PostgreSQL backend (C)
  └─ TAM callbacks (treedb_tam.c)
       └─ iceoryx2 zero-copy IPC
            └─ Background worker (treedb_bgworker.c)
                 └─ CGO → treedb_shim.so (Go)
                      └─ TreeDB B+tree (per-relation mmap'd file)
```

Each backend gets its own iceoryx2 client. The background worker owns the Go runtime and all TreeDB file handles. The worker wakes on a kqueue/epoll event service (notifier+WaitSet) rather than polling, then drains all pending requests in a tight loop.

## Prerequisites

| Dependency | Version | Notes |
|---|---|---|
| PostgreSQL | 18 | `pg_config` must be on `PATH` |
| Go | 1.22+ | `CGO_ENABLED=1` required |
| Rust + Cargo | stable | for building iceoryx2 |
| cmake | 3.16+ | for iceoryx2 C bindings |

## Build and install

**1. Build iceoryx2 C bindings** (one-time, ~2 min):

```bash
cd ../iceoryx2
export PATH="$HOME/.cargo/bin:$PATH"
cmake -S . -B target/ff/cc/build \
  -DCMAKE_BUILD_TYPE=Release \
  -DIOX2_BUILDTYPE_RELEASE=ON
cmake --build target/ff/cc/build --parallel 8
```

**2. Build and install the extension**:

```bash
cd treedb_pgext
make install
```

This builds `treedb_shim.so` (Go, placed in `$(pg_config --pkglibdir)`) and `treedb_pgext.dylib`/`.so` (C extension).

**3. Configure PostgreSQL** (`postgresql.conf`):

```
shared_preload_libraries = 'treedb_pgext'
```

Restart PostgreSQL after changing this.

**4. Create the extension** (once per database):

```sql
CREATE EXTENSION treedb_pgext;
```

## Usage

Use `treedb` as the table access method on any table:

```sql
-- Per table:
CREATE TABLE my_table (id int PRIMARY KEY, val text) USING treedb;

-- Or as the default for all new tables in a session:
SET default_table_access_method = 'treedb';
```

Standard SQL works as normal — `INSERT`, `UPDATE`, `DELETE`, `SELECT`, indexes, and `TRUNCATE` all work. `DROP TABLE` cleans up TreeDB data atomically on commit.

Row data is stored under `$PGDATA/treedb_data/<relfilenode>/`.

## Benchmarks

All numbers on Apple M-series, PostgreSQL 18, pgbench scale=1 (100K rows in `pgbench_accounts`), 1 client, 30 s.

### OLTP (TPC-B)

```
pgbench -d postgres -c 1 -T 30
```

| AM | TPS | Avg latency |
|---|---|---|
| heap (baseline) | 6,291 | 0.159 ms |
| treedb | ~6,350 | ~0.157 ms |

treedb matches heap throughput. Each TPC-B transaction issues 7 RPCs over iceoryx2 shared memory (~19 µs/RPC). The B+tree does one lookup where heap would do an index lookup + heap fetch.

### Sequential scan (OLAP)

```sql
SELECT SUM(abalance) FROM pgbench_accounts;
```

| AM | Latency (warm) | Notes |
|---|---|---|
| heap | ~2 ms | linear shared_buffers scan |
| treedb | ~20 ms | 64 KB batches, ~150 RPCs |

treedb is ~10× slower for full-table scans. The bottleneck is the Go B+tree iterator — 100K `Next()` calls through tree leaf nodes costs ~10 ms regardless of batch size. Heap reads raw 8 KB pages sequentially with no tree overhead.

### IPC evolution (OLTP TPS, for reference)

| Transport | TPS | Change |
|---|---|---|
| Unix socket (original) | ~2,000 | — |
| iceoryx2 + sched_yield | ~5,963 | 3× |
| + notifier/WaitSet | ~6,753 | +13% |
| + client spin-then-yield | ~7,630 | +13% |

The peak of ~7,630 TPS (121% of heap) was measured before `SCAN_NEXT_BATCH` was added; the 256 KB bgworker response buffer used for batch scans slightly affects OLTP allocation sizing, settling at ~6,350 TPS in the current build.

## Tuning

| Constant | File | Default | Effect |
|---|---|---|---|
| `TDB_SCAN_BATCH_BUF` | `treedb_pgext.h` | 64 KB | Bytes fetched per scan RPC. 32–512 KB all perform similarly; 64 KB minimises memory waste. |
| `TDB_SPIN_ITERS` | `treedb_pgext.h` | 2048 | ARM `yield` spins before `sched_yield` fallback. 2048 ≈ 10 µs; doubling to 4096 decreased throughput. |

Only `treedb_pgext` needs recompiling after changing these constants — the Go shim reads `max_bytes` from the request payload at runtime.

## Profiling

The Go shim starts a pprof HTTP server at `localhost:6060` when the background worker loads:

```bash
# during a pgbench run:
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
```

A 30 s profile during TPC-B showed: 73% C IPC machinery, 21% Go scheduler (CGO crossings), <1% actual TreeDB operations.

## Limitations

- Single background worker — all backends share one Go runtime and one TreeDB handle per relation; no concurrent writes from multiple backends.
- No MVCC — snapshot isolation is not implemented; all reads see the latest committed state.
- No WAL — crash recovery is not implemented.
- Sequential scans are ~10× slower than heap for in-memory datasets.
- `FETCH_AND_UPDATE` cannot be collapsed to one RPC at the TAM layer; PostgreSQL evaluates `SET col = col + delta` before calling `tuple_update`.

See [PLAN.md](../PLAN.md) for the full implementation roadmap.
