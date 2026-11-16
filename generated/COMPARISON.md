# SQL Store — Design / Implementation Comparison

**Locked design:** `EDD.md` v0.2, `PRD.md` v0.1  
**Evidence (source-changing) commit:** `176c029364af20f9eb5eedcd0170497f23d6e75d`  
**Expanded green CI run:** [43784743983](https://github.com/jonathanselby/sql-store/actions/runs/43784743983)

| Design area | Locked intent | Recovered implementation | Assessment |
|---|---|---|---|
| Component boundary | database mechanics only | native `*sql.DB`, no domain repository, ORM or SQL translator | MATCH |
| Engine choice | SQLite local, PostgreSQL shared | `modernc.org/sqlite` and `pgx/v5/stdlib` | MATCH |
| Normal query path | no additional wrappers/round trips | native `database/sql` returned directly | MATCH — structural |
| Configuration | explicit safe defaults and external PostgreSQL credentials | checked SQLite paths, PRAGMAs, DB driver and pool configuration | MATCH |
| Migrations | ordered, checked, concurrent-safe, fail closed | checksummed ledger, SQLite immediate lock, PostgreSQL advisory lock | MATCH |
| Non-transactional migrations | dirty state blocks readiness | persistent `started` and explicit refusal on restart | MATCH |
| Health and readiness | connectivity distinct from schema-ready | bounded ping and migration-ledger verification | MATCH |
| Transaction ownership | application-controlled, no hidden retry | caller uses standard `*sql.Tx` | MATCH |
| State + event atomicity | one transaction | `Enqueue(ctx, *sql.Tx, Event)` | MATCH |
| Outbox durability/recovery | bounded claim, publication fence and restart retry | leased claims, stable IDs, fenced durable acknowledgment | MATCH |
| Event Stream boundary | producer external and non-durable embedded receipt refused | interop test against released Event Stream; reject embedded receipt | MATCH |
| Security | no credential leaks, application authorisation external | redacted operational errors, private SQLite file, no implicit authorisation | MATCH |
| Backups | engine-correct externalised mechanisms | SQLite `VACUUM INTO`, external `pg_dump` plan and restore test | MATCH |
| Fixtures | test-owned safe reset | disposable SQLite files and isolated local PostgreSQL schemas | MATCH |
| Resource limits | bounded outbox work/pools/waits | configurable native pool, SQLite timeout, payload/batch/lease bounds | MATCH |
| PRD acceptance | all 12 requirements evidenced | named test coverage, real-engine CI, native and ARM64 build | MATCH |
| Example execution | run example against both engines | both example runs included in expanded green CI | MATCH |
| Quantified latency | direct baseline vs SQL Store, p50/p95/p99 and throughput on both engines | native-handle benchmark executes; no full controlled comparison | PARTIAL — explicit evidence gap |

## CI-driven corrections

1. Corrected SQLite backup destination existence checking: an unrelated nil error had incorrectly refused valid new backup paths.
2. Corrected `pg_restore` test invocation to specify its target database.
3. Added canonical `go.mod` and `go.sum` produced by the Go 1.26 toolchain.
4. Added CI execution of the example on SQLite and PostgreSQL; previous workflow only built the example.

The first two corrections addressed actual failing CI tests. The changes did not expand the product boundary.

## Acceptance matrix cross-check

- `TestSQLiteContract`: present in the source-pinned acceptance suite and executed within green CI
- `TestPostgresContract`: present in the source-pinned acceptance suite and executed within green CI
- `TestTransactionRollback`: present in the source-pinned acceptance suite and executed within green CI
- `TestMigrationFailureNotReady`: present in the source-pinned acceptance suite and executed within green CI
- `TestMigrationVersioning`: present in the source-pinned acceptance suite and executed within green CI
- `TestContextCancellation`: present in the source-pinned acceptance suite and executed within green CI
- `TestCapabilities`: present in the source-pinned acceptance suite and executed within green CI
- `TestTransactionalOutboxAtomicity`: present in the source-pinned acceptance suite and executed within green CI
- `TestOutboxRecovery`: present in the source-pinned acceptance suite and executed within green CI
- `TestDependencyBoundary`: present in the source-pinned acceptance suite and executed within green CI
- `TestEngineSpecificContracts`: present in the source-pinned acceptance suite and executed within green CI
- `TestSQLiteBackupRestore`: present in the source-pinned acceptance suite and executed within green CI
- `TestPostgresBackupRestore`: present in the source-pinned acceptance suite and executed within green CI
- `TestOutboxEventStreamRoundTrip`: present in the source-pinned acceptance suite and executed within green CI

Other constraint-specific tests verify contention deadlines, startup concurrency, schema-version drift, outbox expiry/stale claims, payload limits, credential redaction, backup/restore and fixture isolation.

## Remaining quantitative evidence

EDD v0.2 section 14.2 calls for a matched-driver, matched-workload comparison of **read, write and explicit transaction p50/p95/p99 and throughput for both SQLite and PostgreSQL**, including representative concurrency. The current `BenchmarkDirectSQLVsSQLStore` is a useful hot-path smoke benchmark, but not that complete comparison.

**Release interpretation:** the complete locked PRD v0.1 acceptance suite and current CI build gates pass. The empirical latency equivalence requirement is not yet proven and must not be described as measured zero-overhead performance.

## Design verdict

The source shows no functionality removed from PRD v0.1 and no unapproved additional service, broker, ORM or authorisation authority. The only disclosed evidence gap is quantitative latency comparison; the architecture otherwise matches locked EDD v0.2.
