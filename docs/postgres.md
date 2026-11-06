# Local PostgreSQL

```sh
docker compose up -d --wait
export IRO_POSTGRES_URL=postgres://iro:iro-local-test@127.0.0.1:54329/iro_test
cargo build --workspace
cargo test --workspace --test replay postgres_ -- --ignored
./target/debug/iro verify --manifest examples/manifests/postgres-correct.toml --report-dir reports/postgres
./target/debug/iro verify --manifest examples/manifests/postgres-buggy.toml --report-dir reports/postgres-buggy
```

Compose binds PostgreSQL 17 only to loopback, using disposable development credentials. Do not reuse production credentials. `docker compose down` stops it; no persistent host volume is configured.

The harness accepts `postgres`/`postgresql` URLs with localhost, 127.0.0.1, or ::1. It rejects other destinations, query parameters, and fragments. The configured environment variable name is reportable; its value is not. Failures avoid printing raw connection strings.

Each execution creates a random workspace-derived `iro_...` identifier (quoted, CREATE without IF NOT EXISTS). Schema creation must succeed before launching a consumer. Consumer connections set `search_path` to that schema and check `current_schema()`. Protocol hello attests it. Snapshot SELECTs use qualified schema/table names. Cleanup drops only this successfully created schema with CASCADE; existing/public schemas are never reused or dropped. Normal failures/timeouts clean up; abrupt termination of the harness can leave schemas. Inspect and remove abandoned test schemas manually; there is no wildcard cleanup operation.

Projection type support: NULL, INT2/4/8, exact NUMERIC (normalized decimal strings), TEXT/VARCHAR/BPCHAR/NAME, BOOL, BYTEA, JSON/JSONB, DATE, TIMESTAMP, TIMESTAMPTZ, FLOAT4/8 (bit-preserving). Arrays, enums, ranges, UUID and extension types are rejected rather than coerced silently. Application-owned views can explicitly represent such fields in supported types. No arbitrary user SQL is accepted as snapshot configuration. Keys must identify rows uniquely. Exact integer-string example accounting uses NUM BigInt in application code, not SQL floating-point arithmetic.

PostgreSQL integration tests are explicitly ignored in the default offline suite. CI supplies a service and invokes them separately. If Docker is unavailable, run the SQLite suite and report PostgreSQL as unverified, never as passed.
