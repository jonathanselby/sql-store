# SQL Store — Engineering Design Document

**Version:** 0.2  
**Status:** LOCKED — approved design baseline for implementation  
**Product baseline:** SQL Store PRD v0.1 (unchanged)  
**Design authority:** Garage engineering-under-constraints  
**Date:** 16 November 2026

## 1. Purpose

SQL Store provides the common database mechanics required by Garage mini-monoliths while leaving business schema, queries and repository semantics inside each application.

> **The application owns its data model. SQL Store owns database mechanics.**

SQLite is the small/local deployment engine. PostgreSQL is the shared/enterprise engine. The two engines are not represented as semantically identical.

**V0.2 decision:** Retain the complete PRD v0.1 scope and simplify the interface while making migration coordination, readiness, outbox recovery, resource bounds and failure evidence explicit. No additional service or infrastructure is required.

### 1.1 Non-negotiable design invariants

1. **Performance:** ordinary application queries and transactions use `database/sql` directly. SQL Store adds no per-query SQL round trip, lock, readiness probe, implicit retry or background coordination to that path. Any deliberate contention limits are configurable and benchmarked.
2. **Security:** no relaxation of credentials, authorisation, transaction isolation, data durability or failure handling. Security-sensitive errors and logging must not disclose credentials.
3. **Functionality:** all PRD v0.1 requirements and all 12 acceptance criteria remain supported, including test setup/reset and backup/export boundaries.
4. **Simplicity:** use existing database and Go primitives; no ORM, general data-access framework or new distributed component.
5. **Evidence:** design approval is not implementation proof. Release requires named executable tests, real PostgreSQL integration evidence and performance measurements.

## 2. Non-goals

V0.2 does not provide an ORM, generic repository, query builder, business schema, object mapping, database provisioning, replication, distributed transactions, automatic cross-engine SQL translation, secret storage, connection-pool product, business retry engine, database proxy, failover orchestrator, backup scheduler or remote backup storage.

The outbox is a **storage primitive**, not a broker, polling daemon, business publisher, deduplication service or exactly-once processing framework.

## 3. Component model

```text
                   APPLICATION
                       |
              domain store / SQL
                       |
                database/sql
                       |
            +----------+----------+
            |      SQL STORE      |
            |  open/config/close  |
            |  migration/ready   |
            |  capabilities      |
            |  test/backup hooks |
            |  outbox persistence|
            +----------+----------+
                       |
            +----------+----------+
            |                     |
            v                     v
          SQLITE              POSTGRESQL
      local/single-node       shared/enterprise

  Application publisher -> reads SQL Store outbox
                        -> Event Stream transport
```

The diagram shows ownership, not an additional layer inserted into every business query. Once opened, callers use the returned `*sql.DB` and its native `*sql.Tx`. Domain packages do not import concrete database drivers. The outbox-to-transport hand-off belongs to an application-owned publisher.

## 4. Core public model and ownership

Reuse `database/sql` rather than wrapping its API.

```go
type Engine string

const (
    SQLite     Engine = "sqlite"
    PostgreSQL Engine = "postgresql"
)

type Capabilities struct {
    Engine Engine
    // Only material, engine-specific capabilities that an
    // application actually needs may be added.
}
```

The small public surface consists conceptually of:

- `Open(ctx, config) (*sql.DB, error)`: validated configuration, driver opening and explicit connection setup; caller owns `db.Close()`;
- `Migrate(ctx, db, engine, migrations) error`: ordered, coordinated migrations, as specified below;
- `Health(ctx, db) error`: bounded database reachability check;
- `Ready(ctx, db, expectedSchemaVersion) error`: reachability **and** required migration-version check;
- `Capabilities(engine)`: explicit material engine identification;
- outbox operations using caller-supplied `*sql.Tx` for atomic enqueue, and context-aware operations for pending/claimed/published records;
- isolated **test-only** setup/reset helpers and engine-appropriate backup/export integration hooks.

These names express responsibilities, not an instruction to add unnecessary facade types. Implementation may reduce signatures where it makes the same contract clearer. **Do not create a `Store` wrapper merely to forward `DB`, `BeginTx` or `Close`.** Do not remove any listed capability in the name of API reduction.

`*sql.DB` owns the pool and its lifecycle. `*sql.Tx` owns transaction commit/rollback. Engine selection remains explicit; no implicit engine guessing or SQL rewriting. Do not cache authoritative readiness in an inconsistent second state store.

## 5. SQLite design

```text
Go executable
     |
     v
SQLite file (local / single-node)
```

- Accept an explicit database path and validated configuration. Open/create operations use appropriate OS file and directory permissions; do not assume a file can be shared safely over a network filesystem.
- SQLite has **one concurrent writer**. Contention and its bounded busy behaviour must be visible, documented and tested. Caller deadlines take precedence; never wait indefinitely.
- Journal mode, synchronous/durability settings, foreign-key enforcement and busy behaviour are explicit, documented engine settings; connection-scoped PRAGMAs must apply to all relevant physical connections. Do not silently weaken durability for speed.
- WAL may be used where appropriate, with its concurrency, checkpoint and backup implications stated. Do not imply WAL permits multiple concurrent writers.
- SQLite in-memory tests must not accidentally create a distinct empty database for each pooled connection. Prefer isolated temporary files unless shared-memory configuration is deliberate.
- Do not claim distributed, multi-node or PostgreSQL-style isolation semantics.

The default configuration must be safe for local operation, easy to install and modest in resource use. Tune actual defaults using test evidence, not arbitrary performance assumptions.

## 6. PostgreSQL design

```text
Go application instance(s)
          |
          v
      PostgreSQL
```

- Use a mature Go PostgreSQL driver compatible with `database/sql`; do not implement the wire protocol.
- Connection credentials and TLS/authentication are supplied externally. Never provide embedded credentials, persist them, or print an unredacted DSN.
- Use `database/sql`'s existing connection-pool controls. Expose configurable upper bounds, idle limits and lifetimes without introducing a separate pool manager.
- Do **not** set an arbitrarily restrictive pool size that needlessly serialises queries; benchmark with representative concurrent workloads.
- PostgreSQL availability, provisioning, role management, backups, high availability and failover remain deployment responsibilities.
- Any PostgreSQL-only capability is explicit to the application, not silently emulated by SQLite.

## 7. Migration model and concurrency safety

Applications register ordered, immutable migration units with unique, monotonically increasing versions. Forward migrations are the standard V0.1 contract; application-owned downgrade operations remain outside the generic forward path.

```text
OPEN / CONNECT
     |
     v
ACQUIRE ENGINE-NATIVE MIGRATION COORDINATION
     |
     v
READ + VALIDATE VERSION UNDER COORDINATION
     |
     +-- current ----------> RELEASE -> READY CHECK
     |
     +-- newer/invalid ----> REFUSE / NOT READY
     |
     v
APPLY NEXT MIGRATION
     |
     +-- failure ----------> ROLLBACK IF POSSIBLE
     |                       REFUSE / NOT READY
     v
RECORD VERSION ONCE
     |
     v
RECHECK REQUIRED VERSION -> RELEASE -> READY CHECK
```

### 7.1 Concurrent starts

- **PostgreSQL:** serialise concurrent migrators with a database-native lock on a dedicated connection/transaction as appropriate; check the version *after* acquiring coordination. For transactional migrations, use transaction-scoped coordination and atomic schema/version updates where supported. If a migration cannot run in a transaction, retain cross-instance coordination for its full execution and verify final state before readiness.
- **SQLite:** secure the engine's write reservation **before** reading the schema version and applying a migration (for example, immediate transaction semantics); use bounded contention behaviour. A second process must not independently execute the same migration.
- Record each successful schema version once under a uniqueness constraint. A concurrent process that starts later must observe completion rather than rerun the migration.

### 7.2 Failure, non-transactional SQL and version drift

- Prefer atomic DDL and version updates whenever the selected engine and operation permit them.
- Do **not** prohibit a legitimate non-transactional migration solely to simplify the library. Instead, require explicit designation/documentation, safe cross-instance serialisation, and a fail-closed outcome if execution is interrupted or partially completed.
- Never pretend a partially applied, non-transactional change has been rolled back. Such a state requires operator/application-owned reconciliation before readiness; automatic blind replay is forbidden.
- Refuse startup if the database reports a newer or otherwise incompatible schema version. Version advancement cannot be inferred from successful SQL execution alone.
- No automatic migration retries, hidden repair logic, remote coordinator or general migration framework.

Migration coordination happens on startup or explicit migration, **never around normal reads and writes**.

## 8. Transaction behaviour

```text
BEGIN (caller *sql.Tx)
  |
  +-- application writes
  +-- optional outbox enqueue using the SAME *sql.Tx
  |
  +-- error --> ROLLBACK
  |
  v
COMMIT
```

- Caller chooses transaction boundaries and is responsible for commit/rollback.
- Outbox enqueue must accept the caller's `*sql.Tx` and must not secretly begin or commit another transaction.
- No implicit retries of a caller transaction or business effects.
- Respect caller context/deadline and propagate operational failures; explicitly document engine-specific cancellation behaviour.
- Preserve the isolation semantics of the selected engine; do not claim a unified isolation model.

## 9. Transactional outbox

### 9.1 Core storage contract

SQL Store supplies minimal durable outbox persistence with the application state/event atomicity guarantee:

```text
APPLICATION TRANSACTION
   +-- update business state
   +-- insert complete event envelope and payload
             |
           COMMIT
             |
      event is pending
             |
    application publisher
             |
       Event Stream
             |
       transport receipt
             |
   mark published in SQL Store
```

A record holds a stable event ID, event type/version, the complete opaque payload and envelope fields needed to reconstruct the Event Stream `Event` (including source, subject, event time, correlation/causation identifiers, content type, schema reference and key where present), creation time, and publication state/timestamp. Retain original event identity on every redelivery. The application owns payload/business semantics.

Normal write overhead is precisely the **PRD-required outbox insert in the caller's existing transaction**. No additional coordinator, network request or transaction is inserted into ordinary application reads or writes.

### 9.2 Publication and recovery semantics

- A publisher reads pending records in bounded batches, publishes through the selected transport and marks each record published only after the relevant acknowledgement.
- Failed publication leaves the entry recoverable. Successful transport acknowledgement followed by a crash before SQL acknowledgement can cause duplicate publication; therefore delivery is **at least once**, not exactly once.
- A Kafka broker acknowledgement is transport evidence, **not** evidence of consumer/business processing. It must meet the application's configured durability requirements before the outbox is marked published.
- **Embedded Event Stream acceptance is memory-only and non-durable.** An application must not mark a durable outbox row published merely because the embedded stream accepted it. It may retain the row pending, or use another explicitly durable completion mechanism supplied outside SQL Store.
- If multiple publishers are allowed against one store, claim pending rows safely using engine-native atomic coordination and a recoverable lease/fencing token or equivalent compare-and-set. An expired claim is re-eligible; a stale owner must not mark a reclaimed row as published. The publisher performs network I/O **outside** database locks/transactions.
- If only one publisher is deployed, unnecessary claim machinery may be omitted, but the supported deployment assumption must be explicit and enforced operationally. PostgreSQL shared deployments must not silently assume single-publisher ownership.
- Publication order is deterministic for selecting pending rows where practical, but no global cross-publisher/external-broker ordering or exactly-once consumer semantics is promised.
- No unbounded batch, indefinite lease, unlimited publisher concurrency, implicit business retries, polling service or consumer deduplication framework is introduced.

### 9.3 Event Stream interface boundary

SQL Store persists an event envelope compatible with the published `event-stream` contract; the application-owned publisher converts it to the transport-neutral Event Stream `Event` without altering identity or payload. No Kafka topic or credential is stored in the outbox. Topic routing remains an Event Stream concern.

The integration example and tests must exercise a real round trip to Event Stream, including restart recovery and duplicate-publication windows. SQL Store need not import Event Stream in its production runtime merely to persist opaque events.

## 10. Health and readiness

`Health(ctx)` executes a bounded database operation and reports reachability. `Ready(ctx)` requires **both** a successful health/reachability check and a database schema version matching the application's required version.

```text
CONFIGURED / OPEN  !=  REACHABLE  !=  READY
READY = reachable AND required migrations successfully applied
```

- Failed, incomplete or incompatible migrations always fail readiness.
- Startup must not advertise readiness or accept dependent business work before the migration gate succeeds.
- Do not add a health/readiness check to **each SQL query**. Explicit startup and external readiness checks are sufficient.
- Readiness is checked against authoritative database state; no separate mutable readiness service or hidden permanent cached flag is required.
- Context cancellation and loss of database reachability return explicit errors.

## 11. Capability boundary

SQLite and PostgreSQL are not equivalent products. Expose the engine and **only material capability flags that applications cannot safely infer**. Do not maintain a giant capability matrix.

Document at least the differences relevant to this component: parameter syntax, transaction/isolation/concurrency behaviour, migration SQL and rollback limitations, connection settings, outbox claiming, and backup/export operations.

The application's SQL must be compatible with its selected engine or explicitly engine-specific. No automatic SQL translation or feature emulation is allowed. Do not duplicate a capability flag if the engine identity already supplies the same information.

## 12. Backup/export and test fixture boundaries

### 12.1 Backup/export hooks

Preserve the PRD requirement by exposing documented, engine-appropriate integration points enabling **external** deployment tooling to request a consistent backup/export.

- SQLite: use an engine-safe online backup/snapshot path or a correctly coordinated offline backup; a raw copy of a live WAL database is not assumed safe.
- PostgreSQL: integration uses the database's established backup/export mechanisms and deployment-owned credentials/procedures.
- Ensure that backup/export requests do not silently dump sensitive data or credentials to logs.
- No backup scheduler, remote store, retention service or generic backup product is built into SQL Store.

### 12.2 Test setup/reset helpers

Provide deterministic helpers for isolated SQLite and PostgreSQL test stores, applying migrations and cleaning up resources. Reset/drop functionality is **test-only**, opt-in, and restricted to disposable test targets with explicit ownership/provenance checks. Do not expose an unrestricted destructive reset operation on a production `Store`/`*sql.DB`.

SQLite tests run without an external database service. PostgreSQL tests use an ephemeral service in CI. Engine-specific failures must not be masked by cross-engine emulation.

## 13. Failure handling

| Failure | Required behaviour |
|---|---|
| Invalid configuration | Refuse open; sanitised, actionable error |
| Database unavailable | Operational error; not ready; no hidden retry |
| Authentication/TLS failure | Explicit error; redact secrets/DSN |
| Concurrent migration | One migrator executes; others recheck after coordination |
| Migration failure | Roll back where possible; refuse readiness |
| Interrupted non-transactional migration | Fail closed; require explicit reconciliation, no blind replay |
| Database schema newer than supported | Refuse readiness/startup |
| Transaction error | Caller chooses rollback; no hidden transaction retry |
| Context cancellation/deadline | Propagate; release resources safely |
| SQLite busy or PostgreSQL pool saturation | Bounded wait/error; do not hide contention |
| Outbox insert/transaction rollback | No committed outbox event and no partial business state |
| Outbox publication failure | Entry remains recoverable, including across restart |
| Publish acknowledged, mark fails | Entry may republish; stable ID, at-least-once contract |
| Concurrent publisher crash/lease expiry | Eligible for safe reclaim; stale owner cannot falsely complete |
| Embedded in-memory Event Stream receipt | Never claim durable publication from memory-only acceptance |
| Unsupported engine capability | Explicit refusal; no silent emulation |

## 14. Test strategy and performance proof

Run common contract tests on **both engines**, plus engine-specific and integration tests. Test against a real ephemeral PostgreSQL service in CI; SQLite requires no external service.

### 14.1 Test priorities

1. SQLite/PostgreSQL open, migrate, explicit transact, query and close.
2. Failure of application transaction leaves neither partial business state nor outbox row.
3. Duplicate/concurrent startup runs each migration version exactly once.
4. Migration failure blocks readiness; interrupted non-transactional work is not represented as rolled back.
5. Newer/invalid schema versions refuse readiness.
6. Context cancellations and deadlines propagate through representative database operations.
7. SQLite write contention, PostgreSQL connection saturation and bounded resource behaviour.
8. Full event-envelope round trip to Event Stream; outbox atomicity, publication failure, crash/restart recovery and duplicate window.
9. Concurrent publisher claim/expiry handling where multi-publisher support is configured.
10. Credentials excluded from errors/logs; safe test-only reset boundaries.
11. Backup/export hooks exercised with engine-correct fixture/restore procedure.
12. Documentation and dependency audits prohibit ORM/query builder, extra coordinator or hidden retry engine.

### 14.2 Latency and resource verification

- Compare direct `database/sql` baseline with SQL Store under **the same drivers, schema, data, pool configuration and workload**, measuring read, write and explicit transaction p50/p95/p99 latency and throughput. Test both engines and include representative concurrency.
- Confirm ordinary SQL operations incur **zero additional database round trips** and no new serialising lock in SQL Store. This is a design invariant, not a probabilistic benchmark target.
- Test startup migration coordination and outbox worker throughput separately; neither belongs on the hot query path.
- Record configured normal/maximum resource envelopes and behaviour when exhausted: connections, wait/deadline, outbox batch size/payload and pending backlog.
- Investigate material regressions before release. Do not assert empirical zero latency impact until evidence exists; retain raw benchmark outputs and environment details.

CI runs format, vet, race tests, native build and `linux/arm64` build. Integration tests must run against real engine behaviour rather than mocks alone.

## 15. Build and dependency boundary

Target **Go 1.26**. Use standard `database/sql`, a mature SQLite driver and a mature PostgreSQL driver compatible with the required deployment targets. No ORM or SQL query-builder dependencies.

Driver choice must be recorded with verified versions, security posture and Linux ARM64 build support. Where a dependency requires native components, prove its target builds rather than assuming cross-compilation works.

CI gates:

```text
gofmt / formatting
go vet ./...
go test ./...
go test -race ./...
SQLite + ephemeral PostgreSQL integration tests
native build
linux/arm64 build
module/dependency review
benchmark evidence + example application
```

A small example application must demonstrate both engines and executable outbox recovery. The release evidence includes implementation-derived design documentation compared with this locked EDD.

## 16. Operational limits and chosen constraints

- SQLite is a local/single-node deployment engine, not a shared multi-node database.
- PostgreSQL provisioning/HA, IAM, backup operations and role management are external.
- No distributed transaction coordinator, automatic failover, broker, schema registry, general data-access layer or SQL translation.
- Forward migrations are the default. Non-transactional changes are explicitly marked and fail closed when interrupted; application-specific downgrades remain application-owned.
- No exactly-once delivery promise; outbox recovery preserves durable, at-least-once hand-off.
- Connection, busy wait, health/migration deadlines and outbox payload/batch bounds must be finite and configurable; defaults are selected using measured workloads, not artificial scarcity.
- When a database is unavailable or schema is inconsistent, dependent business persistence is unavailable; do not fake offline writes or conceal failure.
- No silent discard of pending outbox rows; retention/purging policy is application/operator-owned and must not remove unpublished events.

### 16.1 Explicit resource envelope register

The implementation/README must document for each engine:

| Resource | Normal setting | Maximum/configured bound | At limit |
|---|---|---|---|
| Active/idle DB connections | Workload-tested default | `database/sql` configuration | Wait within caller deadline, then error |
| SQLite writer wait | Short, bounded | Configurable busy/deadline | Explicit contention/deadline error |
| Health/migration execution | Bounded context | Caller/operation deadline | Not ready / error |
| Outbox payload | Small transport-neutral event | Configurable maximum | Refuse oversized enqueue |
| Outbox polling batch | Small bounded batch | Configurable maximum | Leave remainder pending |
| Concurrent publishers/claims | Per deployment mode | Explicit bounded workers/lease | Back-pressure or reclaim after expiry |
| Outbox pending backlog | Observable count/age | Deployment-owned capacity policy | Surface pressure; do not discard silently |

Do not invent arbitrary numeric values in the locked design before profiling realistic workloads. Document the chosen defaults and behaviour in the implementation and generated evidence before release.

## 17. Security reasoning

SQL Store centralises database mechanics but **does not** confer application authorisation. Application checks must occur before governed reads/writes, including outbox enqueue and publication permissions.

- Credentials, tokens, connection strings and TLS secrets remain external; never embed in the repository, event envelope or diagnostics.
- Use parameterised application SQL for values; application-owned SQL and migrations are trusted code, not unchecked user inputs. SQL identifiers cannot be safely parameterised as values.
- Restrict database roles to required privileges. Migration credentials may differ from runtime credentials; do not silently elevate runtime permissions.
- Keep SQLite files/directories private to the intended OS identity and use secure test fixture isolation.
- Preserve engine durability/isolation settings and expose trade-offs rather than weakening them to hit latency targets.
- Test reset is never a general production operation; backup/export is externally governed.
- Guard against stale outbox claim owners; preserve event identity and write/publication atomicity.
- Sanitise driver/configuration errors that could contain secret-bearing DSNs.
- Pin and review driver versions and security advisories before release, including SQLite WAL-related correctness advisories where applicable.

## 18. Acceptance-to-evidence map — complete PRD v0.1 coverage

| PRD v0.1 acceptance criterion | Required executable evidence |
|---|---|
| 1. SQLite lifecycle | `TestSQLiteContract` |
| 2. PostgreSQL lifecycle | `TestPostgresContract` against real test PostgreSQL |
| 3. Rollback leaves no partial state | `TestTransactionRollback` |
| 4. Failed migration blocks readiness | `TestMigrationFailureNotReady` |
| 5. Schema advances once | `TestMigrationVersioning`, `TestConcurrentMigration` |
| 6. Context cancellation | `TestContextCancellation` |
| 7. Engine/capability reporting | `TestCapabilities` |
| 8. Business state and outbox atomicity | `TestTransactionalOutboxAtomicity` |
| 9. Outbox survives publisher failure/restart | `TestOutboxRecovery`, `TestOutboxAckCrashWindow` |
| 10. No ORM/query-builder dependency | `TestDependencyBoundary` and import audit |
| 11. Engine differences explicit | `TestEngineSpecificContracts`, documentation review |
| 12. Vet, race, native and ARM64 | `.github/workflows/test.yml` |

**Additional evidence to preserve in-scope PRD functions and close red-team risks:**

| Requirement / risk | Required evidence |
|---|---|
| Test setup/reset helpers | `TestSQLiteFixtureIsolation`, `TestPostgresFixtureIsolation`, `TestResetRefusesUnownedTarget` |
| Backup/export boundary | `TestSQLiteBackupRestore`, PostgreSQL documented export/restore integration test |
| Migration concurrency and newer schema | `TestConcurrentMigration`, `TestNewerSchemaNotReady` |
| Interrupted non-transactional migration | `TestNonTransactionalMigrationFailClosed` |
| SQLite contention and pool bounds | `TestSQLiteBusyDeadline`, `TestPostgresPoolDeadline` |
| Concurrent outbox claim, if supported | `TestOutboxClaimRecovery`, `TestStaleClaimCannotMarkPublished` |
| Embedded-memory non-durability | `TestEmbeddedReceiptDoesNotMarkPublished` |
| Full Event Stream compatibility | `TestOutboxEventStreamRoundTrip` |
| Secret-redacted errors | `TestCredentialRedaction` |
| Hot-path latency invariant | `BenchmarkDirectSQLVsSQLStore`, round-trip/lock instrumentation |
| Resource bounds and shutdown | Bound/cancellation tests and recorded envelope register |

Test names are **planned evidence identifiers**, not claims that code or tests already exist. The corresponding tests must be implemented and verified before release.

## 19. Change record: EDD v0.1 → v0.2

| Decision | Change | Why |
|---|---|---|
| KEEP | SQLite + PostgreSQL, domain-owned schema/query model | Locked PRD functionality |
| SIMPLIFY | Return/use native `*sql.DB` instead of redundant forwarding interface | Smaller hot path and API |
| SIMPLIFY | Remove duplicate capabilities when engine identity suffices | Avoid unnecessary abstraction |
| CLARIFY | Cross-instance migration coordination and version recheck | Prevent double execution |
| CLARIFY | Transactional where supported; explicitly govern non-transactional migrations | Safety without removing legitimate operations |
| CLARIFY | Readiness = reachable + correct applied migrations | One authoritative readiness gate |
| CLARIFY | Outbox acknowledgement, at-least-once semantics, claim/restart recovery | Preserve delivery without pretending exactly-once |
| CLARIFY | Do not mark durable publication on embedded in-memory receipt | Avoid silent event loss |
| CLARIFY | Configurable pool, busy, deadline and outbox bounds | Prevent runaway resource use without arbitrary throttling |
| PRESERVE | Test setup/reset helpers, backup/export hooks | Explicit PRD scope |
| STRENGTHEN | Security boundaries, real-engine and performance tests | Executable evidence of correctness |
| REMOVE | No required PRD feature or engine capability removed | Functional scope remains locked |

## 20. Design lock and change control

**Locked implementation baseline:** This EDD v0.2 and PRD v0.1 together govern the SQL Store v0.1 implementation.

- The PRD v0.1 remains **unchanged and locked**.
- The EDD v0.2 supersedes EDD v0.1 for implementation guidance.
- Engineering may choose small implementation details only if all three invariants—no added hot-path overhead, no reduced security, no PRD feature loss—continue to hold and evidence supports the choice.
- Any proposed **material** change to behaviour, public contract, risk boundary or scope requires a documented design change and explicit approval before implementation.
- This document records design approval, **not** completed code, benchmark results, security testing or GitHub publication.

**Release gate:** all 12 PRD acceptance criteria, additional red-team evidence, example execution on both engines, dependency/build gates and source-derived EDD comparison must pass before tagging.