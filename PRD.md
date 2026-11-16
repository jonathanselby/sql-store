# SQL Store --- Product Requirements Document

**Version:** 0.1

## 1. Problem

Garage mini-monoliths need durable relational persistence without each
application rebuilding connection lifecycle, transactions, migrations,
health checks and test setup.

Small/local deployments should remain simple. Enterprise deployments
commonly require PostgreSQL. Hiding the differences behind a large ORM
or false database abstraction would add complexity and conceal
operational behaviour.

## 2. Product objective

Provide a small Go SQL persistence component that supports SQLite for
local/small deployment and PostgreSQL for shared/enterprise deployment
while leaving domain schemas and queries with the application.

> **The application owns its data model. SQL Store owns database
> mechanics.**

The V0.1 product question is: can one small persistence boundary support
the two required deployment envelopes without becoming an ORM or
pretending SQLite and PostgreSQL are identical?

## 3. Users

Garage mini-monoliths, reusable components and engineers needing a
consistent SQL lifecycle across local and enterprise deployments.

## 4. In scope

-   SQLite;
-   PostgreSQL;
-   open/close lifecycle;
-   context-aware execution;
-   transaction begin/commit/rollback;
-   migrations and schema version;
-   health/readiness;
-   explicit database capability identification;
-   test database setup/reset helpers;
-   transactional outbox storage primitive;
-   deterministic configuration and errors;
-   backup/export hooks appropriate to each engine;
-   Go 1.26 and `linux/arm64` build proof.

## 5. Out of scope

ORM, generic domain repository, query builder, business schema,
automatic object mapping, distributed SQL, replication management,
database proxying, connection-pool product, secrets management, database
provisioning, automatic cross-engine SQL translation and hidden
transaction retries.

SQLite and PostgreSQL non-equivalence must remain visible.

## 6. Functional requirements

### FR1 --- SQLite deployment

The component opens and operates a SQLite store suitable for
single-node/local Garage applications.

### FR2 --- PostgreSQL deployment

The component opens and operates PostgreSQL using the same application
persistence boundary where the application's SQL is compatible.

### FR3 --- Transactions

Callers can execute explicit transactions with commit and rollback. The
component must not silently retry a transaction that could repeat
business effects.

### FR4 --- Migrations

Applications can register ordered migrations. Startup identifies the
current schema version and applies required migrations
deterministically.

### FR5 --- Migration failure

A failed migration prevents readiness. The application must not
advertise a partially upgraded database as ready.

### FR6 --- Health and readiness

The component distinguishes configured/open from database
reachable/ready.

### FR7 --- Capabilities

The active engine and material capabilities/differences are queryable so
applications do not infer false equivalence.

### FR8 --- Transactional outbox

The component provides a minimal outbox persistence primitive that can
be written in the same transaction as application state and later
consumed by `event-stream`.

### FR9 --- Context propagation

Database operations respect caller cancellation and deadlines.

### FR10 --- Testability

Tests can create isolated SQLite and PostgreSQL stores and prove the
same persistence contract where supported.

## 7. Acceptance criteria

V0.1 is accepted when executable evidence demonstrates:

1.  SQLite open, migrate, transact, query and close;
2.  PostgreSQL open, migrate, transact, query and close;
3.  rollback leaves no partial business state;
4.  failed migration prevents readiness;
5.  schema version advances exactly once;
6.  context cancellation reaches database work;
7.  engine/capability reporting is explicit;
8.  application state and outbox event commit atomically;
9.  outbox rows survive publisher failure/restart;
10. no ORM/query-builder dependency is present;
11. SQLite/PostgreSQL differences are documented rather than silently
    emulated;
12. `go vet`, race tests, native build and `linux/arm64` build pass.

## 8. Definition of done

V0.1 is complete when a small example application runs against SQLite
and PostgreSQL, transactional outbox recovery is executable, migrations
fail closed, every acceptance criterion has named evidence, and the
component remains a database-mechanics layer rather than a data-access
framework.