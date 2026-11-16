# SQL Store — Skill

## Use this when

Changing SQLite/PostgreSQL adapters, migration coordination, schema readiness, transaction/outbox semantics, test fixtures or backup/export hooks.

## Core rule

> **The application owns its data model. SQL Store owns database mechanics.**

## Change discipline

1. Keep the PRD v0.1 scope locked and EDD v0.2 as the implementation baseline.
2. Do not wrap `*sql.DB` or `*sql.Tx` just to forward existing methods.
3. Keep SQL dialect differences explicit; no ORM, repository, query builder or cross-engine translation.
4. Protect concurrent migrations; migration checksums, dirty states and newer schemas fail closed.
5. Keep migration coordination and readiness checks off the normal business query path.
6. Enqueue outbox records in the caller's SAME transaction; no hidden commit or retry.
7. Persist the full Event Stream envelope, but never treat embedded memory acceptance as durable evidence.
8. Publish only outside DB locks and enforce claim-token fencing for concurrent workers.
9. Bound resource use and respect cancellation; never log secrets or allow destructive production test resets.
10. Keep backup orchestration and database provisioning outside the module.
11. Run both engines' contract tests, failure/restart/concurrency/security gates, native/race and ARM64 builds.
12. Generate code-derived evidence against the final source-changing commit; only release after green CI.
13. Build offline, publish with a few coherent commits and avoid Git transport rabbit holes.