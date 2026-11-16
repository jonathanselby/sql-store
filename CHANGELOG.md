# Changelog

## 0.1.0 — Candidate (not released)

- Native SQLite and PostgreSQL `database/sql` lifecycle.
- Coordinated, checksum-verified deterministic forward migrations and fail-closed readiness.
- Atomic transactional outbox with lease-based claim/recovery and explicit durable acknowledgements.
- Isolated database test fixtures; SQLite consistent snapshot and PostgreSQL backup integration plan.
- PRD v0.1 / EDD v0.2 documentation and Go 1.26 CI test contract.

Do not mark this release final until integration, race, ARM64 and source-derived evidence gates pass.