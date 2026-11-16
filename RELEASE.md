# SQL Store

[![test](https://github.com/jonathanselby/sql-store/actions/workflows/test.yml/badge.svg)](https://github.com/jonathanselby/sql-store/actions/workflows/test.yml)

**The application owns its data model. SQL Store owns database mechanics.**

A small, deterministic Go `database/sql` persistence component: SQLite for local single-node deployments and PostgreSQL for shared enterprise deployments. It does **not** abstract their SQL dialects, isolate transactions for you, create an ORM or run a publisher.

## What v0.1 provides

- Native `*sql.DB`, explicit `*sql.Tx` and caller-owned SQL;
- configured open/close, context propagation and explicit health/readiness;
- ordered, checksum-verified forward migrations with cross-instance coordination;
- persistent fail-closed state for interrupted non-transactional migration;
- one atomic outbox insert within a caller's existing business transaction;
- bounded outbox claims, fencing tokens, lease recovery and explicit durable-acknowledgement marking;
- private SQLite and isolated PostgreSQL test fixtures;
- SQLite consistent `VACUUM INTO` snapshot and external `pg_dump` backup plan;
- Go 1.26 and Linux ARM64 build targets.

## Lifecycle

```go
cfg := sqlstore.Config{Engine: sqlstore.SQLite, SQLitePath: "/var/lib/myapp/app.db"}
db, err := sqlstore.Open(ctx, cfg)
if err != nil { return err }
defer db.Close()

err = sqlstore.Migrate(ctx, db, cfg.Engine, applicationMigrations)
if err != nil { return err }

ob, err := sqlstore.NewOutbox(cfg.Engine, sqlstore.OutboxOptions{})
if err != nil { return err }
if err = ob.Ensure(ctx, db); err != nil { return err }
if err = sqlstore.Ready(ctx, db, applicationMigrations[len(applicationMigrations)-1].Version); err != nil { return err }
// Application uses db.QueryContext / db.BeginTx directly.
```

Migration `Statements` are **trusted developer-authored SQL**, one statement per slice entry. `Migrate` checks a checksum for already applied units and refuses changed, unknown or partially applied versions. `NonTransactional` is explicit; interrupted execution leaves a persistent `started` marker requiring operator reconciliation. No automatic repair or business retry occurs. Migrators should run before serving business traffic.

## Transactional outbox

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil { return err }
defer tx.Rollback()
// application writes state using tx.ExecContext(...)
err = ob.Enqueue(ctx, tx, sqlstore.Event{ /* Event Stream envelope */ })
if err != nil { return err }
return tx.Commit()
```

The application-owned publisher polls `ob.Claim(ctx,db,batchSize)`, publishes reconstructed events through Event Stream, then invokes `ob.MarkPublished(ctx,db,id,token,proof)` **only after durable transport acknowledgment**. On failure it may call `ReleaseClaim`; after a process crash the lease makes the row eligible for reclaim. The proof includes `Durable: true` and an application-supplied broker receipt reference. At-least-once publication can duplicate stable event IDs. Downstream business idempotency remains application-owned.

**A receipt from Event Stream embedded mode is not durable** and must never close out a durable outbox row. Kafka topic routing, authentication and payload semantics remain outside SQL Store.

## Configuration and resource envelope

| Resource | Default | Bounded behaviour |
|---|---|---|
| SQLite physical connections | 1 | Single local writer; reject >1 configuration |
| SQLite busy timeout | 250 ms | Configurable to 30 s; contention returns error |
| SQLite journal / sync / foreign keys | WAL / FULL / ON | Driver PRAGMAs; one physical connection |
| PostgreSQL pool | `database/sql` defaults | `MaxOpenConns`, `MaxIdleConns`, `ConnMaxLifetime` configurable |
| Health probe | 2 s | Caller deadline wins |
| Outbox payload | 1 MiB | Configurable up to 64 MiB, refuse larger |
| Outbox batch | 100 | Configurable up to 1,000 |
| Outbox lease | 30 s | Configurable 1 s to 1 h; token fencing |
| Pending outbox count | Query on demand | No silent event deletion, operational monitoring external |

Outbox claiming uses an atomic statement: PostgreSQL `FOR UPDATE SKIP LOCKED`, SQLite `UPDATE ... RETURNING`. Publishing occurs **outside** all database transactions. Ordinary business SQL passes directly through Go's standard `database/sql` handle without extra probes, locks or SQL round trips imposed by SQL Store.

## Engine boundaries

| Difference | SQLite | PostgreSQL |
|---|---|---|
| Values | `?` parameters | `$1`, `$2` parameters |
| Transactions | Local single writer | Server transaction/isolation rules |
| Migration coordination | `BEGIN IMMEDIATE` | Session advisory lock + transactions |
| Lock contention | Configured busy timeout | Server lock + caller deadlines |
| Outbox claim | Atomic UPDATE RETURNING | SKIP LOCKED + UPDATE RETURNING |
| Snapshot/export | `BackupSQLite`/VACUUM INTO | External `pg_dump`/`pg_restore` |

SQLite files must live on a local filesystem, in a directory controlled by the deployment identity, with mode 0600 for existing files. PostgreSQL URLs must come from a secret source. Migration principals should have only schema rights needed for the app; runtime principals should have narrower data rights where supported.

## Example

Run the small application: `go run ./examples/basic`. To run against a disposable PostgreSQL database, supply `SQLSTORE_EXAMPLE_POSTGRES_URL` through your secret/deployment mechanism. The example commits business state and an outbox event atomically; it **does not** counterfeit a durable broker receipt.

## Verification

`go test ./...`, `go test -race ./...`, `go vet ./...`, `gofmt`, native build, Linux ARM64 build and a real PostgreSQL CI service are the release gates. PostgreSQL tests require the **local test-only** URL `SQLSTORE_POSTGRES_TEST_URL` with a database name starting `sqlstore_test`; fixtures create disposable isolated schemas and refuse remote hosts. Tests for SQLite need no services.

The implementation and its evidence must be compared against locked [`PRD.md`](PRD.md) v0.1 and [`EDD.md`](EDD.md) v0.2 before tagging. Do not treat CI files as evidence that CI ran.

## Engineering artefacts

- [`PRD.md`](PRD.md), [`EDD.md`](EDD.md), [`architecture.txt`](architecture.txt);
- [`SKILL.md`](SKILL.md), [`VERSION`](VERSION), [`CHANGELOG.md`](CHANGELOG.md), [`RELEASE.md`](RELEASE.md);
- [`fixtures`](fixtures) and [`examples/basic`](examples/basic);
- `.github/workflows/test.yml` — live SQLite/PostgreSQL acceptance gates;
- `generated/EDD.md` and `generated/COMPARISON.md` — **to be generated only after the final source-changing commit**.

## Licence

MIT.