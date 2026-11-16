# SQL Store v0.1.0 — Release candidate notes

**Do not create a GitHub Release or tag until CI is green and generated engineering evidence is pinned to the final source-changing commit.**

## Scope

A small Go persistence mechanics component supporting SQLite and PostgreSQL, explicit forward migrations, transactional outbox and test/backup integration. No ORM, business repository, query builder, SQL translation, publisher or database provisioning layer.

## Evidence gates

- Full PRD v0.1 acceptance matrix verified for both engines.
- Go 1.26 format, vet, unit/integration, race and Linux ARM64 build gates green.
- SQLite and PostgreSQL real engine test results; outbox crash/restart and concurrency tests.
- Dependency audit, measured performance and code-derived EDD/comparison against the final source commit.

## Operational limitations

SQLite is single-node; PostgreSQL provisioning/HA and role management remain external. Forward migrations are standard; interrupted non-transactional migrations require explicit reconciliation. Outbox is at least once, not exactly once. Embedded Event Stream acceptance is not durable. No cross-engine automatic SQL translation.
