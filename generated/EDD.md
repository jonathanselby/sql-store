# SQL Store — Generated Engineering Design Evidence

**Method:** independent recovery from published Go source and tests  
**Final Go source-changing commit:** `176c029364af20f9eb5eedcd0170497f23d6e75d`  
**CI workflow commit:** `aba69b74f0457551ba6e4ef03e4ff3eb498ef586`  
**GitHub Actions evidence:** [successful run 43784743983](https://github.com/jonathanselby/sql-store/actions/runs/43784743983)  
**Baseline for comparison:** locked `PRD.md` v0.1 and `EDD.md` v0.2.

## Recovered component boundary

```text
Garage application (owns schema, query SQL, authorisation)
       |
       v
sqlstore — returns native *sql.DB
       +-- engine-specific configuration and availability checks
       +-- ordered forward migrations + ledger / readiness
       +-- caller-controlled explicit SQL transactions
       +-- persistent transactional outbox and claim fencing
       +-- SQLite consistent backup / PostgreSQL backup metadata
       |
       +-- fixtures (separately imported test-only helper package)
       |
       +-- examples/basic (SQLite and PostgreSQL demonstration)
```

Neither the normal query path nor transaction path is wrapped by a repository or ORM. `Open` returns `*sql.DB`; the caller uses native `ExecContext`, `QueryContext`, `BeginTx`, `Commit` and `Rollback`.

## Recovered public interface (extracted from source)

- **`sqlstore.go`:** `CapabilityOf`, `Open`, `Health`, `Ready`
- **`migration.go`:** `ExecContext`, `QueryContext`, `Commit`, `Rollback`, `Migrate`
- **`outbox.go`:** `NewOutbox`, `Ensure`, `Enqueue`, `Claim`, `MarkPublished`, `ReleaseClaim`, `Pending`, `IsPublished`
- **`backup.go`:** `BackupSQLite`, `PostgresBackupPlan`
- **`fixtures/fixtures.go`:** `Close`, `SQLiteFixture`, `PostgreSQLFixture`, `RefuseReset`

## Recovered engine mechanics

**SQLite**: driver `modernc.org/sqlite`, a private database file, restrictions on existing file type/permissions, WAL journalling, `foreign_keys(ON)`, `synchronous(FULL)`, bounded busy timeout and configurable native pool limits. One writer is available; no shared multi-node semantics are claimed.

**PostgreSQL**: `pgx/v5/stdlib` via Go `database/sql`; deployment-supplied URL and native connection-pool controls. Secrets are not included in operational error strings.

**Migrations**: trusted ordered application statements, migration checksums and an authoritative version ledger. SQLite takes a `BEGIN IMMEDIATE` write reservation before inspecting the ledger. PostgreSQL uses a session advisory lock and rechecks the ledger. Transactional work commits with the version row; explicitly non-transactional work records `started` first and fails closed if incomplete.

**Readiness**: bounded `Health` probe plus inspection of applied, ordered migration states against the required version. Ordinary queries are not intercepted or health-checked.

## Recovered transactional outbox

An application writes its own business state and `Outbox.Enqueue` within a single caller-owned transaction. `Outbox.Ensure` creates one SQL infrastructure table and an index at startup, not during each write.

`Claim` uses PostgreSQL `FOR UPDATE SKIP LOCKED` or SQLite atomic `UPDATE ... RETURNING`. Claim tokens and expiring leases fence late publishers and permit recovery from crashes. `MarkPublished` requires a durable, non-embedded publication proof and a matching live claim; an in-memory Event Stream receipt is insufficient.

The module stores an Event Stream-compatible opaque envelope. It does not run a broker, poller, consumer, business retry, or exactly-once deduplication framework. At-least-once re-publication remains possible in the acknowledgement/crash window.

## Recovered backup and fixture boundaries

`BackupSQLite` uses `VACUUM INTO` for a consistent private destination, with overwrite refusal. `PostgresBackupPlan` describes external `pg_dump`, rather than embedding a PostgreSQL backup engine. `fixtures` creates isolated disposable SQLite files and localhost-only PostgreSQL test schemas; a generic destructive production reset API is refused.

## Direct runtime dependencies from the canonical module file

- `github.com/jackc/pgx/v5 v5.13.0`
- `github.com/jonathanselby/event-stream v0.1.0`
- `modernc.org/sqlite v1.61.1`

No ORM, generic repository/query builder, schema registry, broker, secret manager or infrastructure provisioner is a runtime dependency of SQL Store.

## Executable acceptance inventory (extracted from tests)

### `sqlstore_test.go`
- `TestSQLiteContract`
- `TestPostgresContract`
- `TestTransactionRollback`
- `TestMigrationFailureNotReady`
- `TestMigrationVersioning`
- `TestConcurrentMigration`
- `TestNewerSchemaNotReady`
- `TestNonTransactionalMigrationFailClosed`
- `TestContextCancellation`
- `TestCapabilities`
- `TestSQLiteBusyDeadline`
- `TestCredentialRedaction`
- `TestSQLiteBackupRestore`
- `TestEngineSpecificContracts`
- `TestVersion`

### `postgres_test.go`
- `TestPostgresFixtureIsolation`
- `TestSQLiteFixtureIsolation`
- `TestResetRefusesUnownedTarget`
- `TestPostgresConcurrentMigration`
- `TestPostgresPoolDeadline`
- `TestPostgresRollbackAndOutbox`
- `TestPostgresBackupRestore`
- `BenchmarkDirectSQLVsSQLStore`

### `outbox_test.go`
- `TestTransactionalOutboxAtomicity`
- `TestOutboxRecovery`
- `TestOutboxAckCrashWindow`
- `TestStaleClaimCannotMarkPublished`
- `TestOutboxClaimRecovery`
- `TestEmbeddedReceiptDoesNotMarkPublished`
- `TestOutboxEnvelope`
- `TestOutboxLimits`

### `eventstream_test.go`
- `TestOutboxEventStreamRoundTrip`

### `dependency_test.go`
- `TestDependencyBoundary`

## Verified build evidence

The expanded CI run passed Go 1.26 module tidy/verify without a diff, formatting, vet, real SQLite/PostgreSQL integration, race tests, native build, Linux ARM64 build, dependency/version audit, backup/restore, benchmark execution and execution of the example against both engines.

**Evidence limitation:** the existing benchmark exercises the native database handle but does not yet produce a separately controlled driver-equivalent baseline or p50/p95/p99 comparison for both engines and concurrency levels. Structural lack of extra query hops is evident in the public contract; empirical latency equivalence remains to be measured before making a quantitative performance claim.
