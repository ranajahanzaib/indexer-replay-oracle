# Indexer Replay Oracle (`iro`)

[![CI](https://github.com/ranajahanzaib/indexer-replay-oracle/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ranajahanzaib/indexer-replay-oracle/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**`iro` tests whether an application's persistent event projection survives real-world replay conditions.** It feeds offline canonical blockchain events to your consumer, injects duplicate deliveries, triggers real process restarts, exercises a negotiated crash-after-commit boundary, and compares normalized SQLite or PostgreSQL projection state.

Use it alongside Ponder, Envio, The Graph, or custom indexer pipelines. Wrap your application's handler with a lightweight JSONL subprocess protocol—keep full ownership of migrations, transactions, and projection logic without requiring an active node, RPC endpoint, ABI decoder, or external indexing SaaS.

![Indexer Replay Oracle Verification Showcase](docs/screenshots/overview.png)

---

## The Problem It Solves

Indexer and event-driven consumers often pass one-pass linear tests, only to fail in production when reorgs, RPC retries, process crashes, or network redeliveries occur:

- **Double-crediting bugs**: Duplicate event deliveries can silently re-apply state mutations if idempotency checkpoints are uncommitted or disjoint.
- **Crash-after-commit inconsistencies**: If a process commits database writes but crashes before acknowledging delivery, a restart must not duplicate business effects.
- **Process restart state loss**: In-memory caches or unpersisted offsets can diverge after container restarts.
- **Reordering hazards**: Processing non-conflicting events concurrently or in shuffled arrival orders can reveal hidden serialization dependencies.

`iro` turns these distributed systems failure modes into repeatable, deterministic, local tests.

---

## Key Features

- **Zero-Dependency SQLite Mode**: Test immediately out of the box using bundled SQLite in fresh, isolated in-memory or WAL workspaces.
- **PostgreSQL Multi-Schema Isolation**: Execute parallel test runs against PostgreSQL with dedicated per-run `iro_*` schemas and automatic cleanup.
- **Deterministic Schedule Generator**: PRNG-driven schedules inject duplicate events, process restarts, crash-after-commit faults, and dependency-respecting reorderings.
- **Negotiated Crash-After-Commit Hook**: Distinguish between intentional crash points (exit code 86 immediately following transaction commit) and arbitrary crashes.
- **Delta-Debugging Minimization**: Automatically isolate the minimal failing scenario from thousands of operations down to 1 event and 2 operations using local 1-minimality reduction.
- **Self-Contained Reproduction Bundles**: Export complete failure traces with input fixtures, schedule steps, expected state, and binary fingerprints for offline reproduction.
- **Language-Agnostic Subprocess Protocol**: Integrate indexers written in Rust, TypeScript, Go, Python, or any executable supporting stdin/stdout JSONL.
- **CI-Ready Reporting**: Emits concise terminal summaries, machine-readable `report.json`, human-readable `report.md`, and JUnit XML test suites.

---

## Visual Demonstration Gallery

### 1. Deterministic Replay Verification

`iro verify` evaluates canonical execution, baseline determinism, exact replay, duplicate delivery, restart recovery, crash-after-commit faults, and seeded mixed schedules:

![Verification Run Overview](docs/screenshots/overview.png)

### 2. Bug Detection & Automated Minimization

When a consumer contains a flaw (such as double crediting on duplicate delivery), `iro` pinpoints exact table and column differences, isolates the failure into an actionable bundle, and reduces it to a 1-event minimal reproducible example:

![Bug Detection and Minimization](docs/screenshots/bug-detection-minimization.png)

### 3. Fixture Recording & Canonical Validation

Capture real event sequences into normalized JSONL fixtures with cryptographically stable SHA-256 fingerprints:

![Fixture Validation](docs/screenshots/fixture-validation.png)

---

## Quickstart

### Prerequisites

Install the current stable Rust toolchain (1.80+). SQLite is bundled; no database daemon or external service is required.

```sh
# Build workspace binaries (iro CLI and example consumer)
cargo build --workspace

# Add target/debug to your PATH for convenience
export PATH="$PWD/target/debug:$PATH"
```

### 1. Validate the Fixture

Verify that event fixtures are strictly ordered, EVM-normalized, and cycle-free:

```sh
iro fixture validate --manifest examples/manifests/correct.toml
# Output: VALID: 4 events; SHA-256 0927cfdfa982d4dfb2b9897f77f5552597fc7b79ade806dfa1b7eaeffb3ceaed
```

### 2. Run Verification on the Correct Consumer

Execute the suite against the reference compliant consumer:

```sh
iro verify --manifest examples/manifests/correct.toml --runs 10 --seed 42 --report-dir reports/correct
```

Every stage—canonical baseline, duplicate delivery, restart recovery, crash-after-commit fault, admissible reordering, and all 10 randomized scenarios—passes.

### 3. Exercise and Minimize the Deliberate Bug

Run verification against a deliberately flawed consumer that omits idempotency deduplication:

```sh
iro verify --manifest examples/manifests/buggy.toml --runs 10 --seed 42 --report-dir reports/buggy
# Exits with code 1: STATE_DIVERGENCE
# balances key [Text("alice")], balance: Some(Text("125")) -> Some(Text("325")); delta Some("200")
```

Now reduce the 6-operation failure into a minimal reproduction trace:

```sh
iro minimize --bundle reports/buggy/failure-duplicate-delivery
# Output: Minimized: 1 source events, 2 operations; status: "stable_1_minimal"

iro reproduce --bundle reports/buggy/failure-duplicate-delivery/minimized
# Exits with code 1: STATE_DIVERGENCE
# balances [Text("alice")] balance: Some(Text("100")) -> Some(Text("200"))
```

The reduced trace pinpoints the bug to a single event and two duplicate deliveries.

---

## CLI Reference

### `iro fixture record`

Filter and normalize raw JSONL event streams into canonical fixtures:

```sh
iro fixture record \
  --input exported-events.jsonl \
  --output fixtures/ledger/events.jsonl \
  --chain-id 31337 \
  --from-block 100 \
  --to-block 150
```

### `iro fixture validate`

Check fixture structure, block ranges, hash formats, conflict keys, and DAG dependencies:

```sh
iro fixture validate --manifest <path/to/manifest.toml>
```

### `iro verify`

Execute the verification matrix against a target consumer:

```sh
iro verify \
  --manifest examples/manifests/correct.toml \
  --runs 20 \
  --seed 42 \
  --report-dir reports/run-01 \
  --junit reports/run-01/results.xml
```

### `iro minimize`

Run delta debugging to produce a minimal 1-minimal reproduction bundle:

```sh
iro minimize --bundle <path/to/failure-bundle>
```

### `iro reproduce`

Re-execute an exact recorded failure schedule without re-randomization:

```sh
iro reproduce --bundle <path/to/bundle-or-minimized>
```

---

## What Is Tested

1. **Two Clean Canonical Baselines**: Two clean runs execute in separate, fresh workspaces. If they differ, `NONDETERMINISTIC_BASELINE` terminates verification.
2. **Optional Golden Snapshot Check**: If an `expected_state` file is declared, the canonical baseline is verified against it; a divergence reports `EXPECTED_STATE_MISMATCH`.
3. **Exact Replay**: Replaying the canonical schedule sequentially from scratch must produce identical state.
4. **Duplicate Deliveries**: Re-delivering already committed events must leave business balances and projection rows unmodified.
5. **Process Restarts**: Killing and restarting the consumer process between operations must preserve durable state.
6. **Commit-Before-Ack Crash Recovery**: The consumer process is signaled to commit its transaction and exit immediately with status `86` before sending an acknowledgement. Upon process relaunch, the event is redelivered and must maintain state equivalence.
7. **Admissible Reordering**: When `ordering.mode = "conflict_keys"` is enabled, non-conflicting events are permitted to commute while shared-key ordering is preserved. See [ordering model](docs/ordering-model.md).
8. **Seeded Mixed Schedules**: Randomized combinations of duplicate delivery, restarts, and reordering test edge cases under controlled pseudo-random seeds.

---

## Integrating Your Own Consumer

Consumers communicate with `iro` over stdin/stdout using newline-delimited JSON.

```
       iro (Harness)                           Consumer Subprocess
             │                                          │
             │── { "type": "hello", ... } ─────────────>│ (initialize schema)
             │<─ { "type": "hello", "capabilities": ...}│
             │                                          │
             │── { "type": "apply", "event": ... } ────>│ (begin tx, apply, commit)
             │<─ { "type": "ack" } ─────────────────────│
             │                                          │
             │── { "type": "apply", "fault": "..." } ──>│ (commit & exit 86)
      [exit observed]                                   │
      [relaunch proc]                                   │
             │── { "type": "apply", "event": ... } ────>│ (redelivery)
             │<─ { "type": "ack" } ─────────────────────│
```

1. **Startup**: Read database credentials from `IRO_DATABASE_URL` (and `IRO_DATABASE_SCHEMA` for PostgreSQL).
2. **Handshake**: Respond to `{ "type": "hello", "request_id": 0, "version": 1 }` with your capabilities (`["durable_ack", "shutdown", "fault.after_commit_before_ack"]`).
3. **Transactional Apply**: When receiving an event, apply and commit changes in a single durable transaction, then reply with `{"type": "ack", "request_id": n}`.
4. **Crash Hook**: If `fault == "after_commit_before_ack"`, commit the transaction and exit immediately with code `86` without emitting an acknowledgement.

See the full specification in [Consumer Subprocess Protocol v1](docs/consumer-protocol.md) and reference implementation in [src/bin/consumer.rs](src/bin/consumer.rs).

---

## Manifest Configuration

A TOML manifest configures the target indexer, fixtures, projection targets, and testing limits:

```toml
version = 1
name = "transfer-ledger-correct"
fixture = "../fixtures/events.jsonl"
chain_id = 31337
from_block = 100
to_block = 100
expected_state = "../fixtures/expected-state.json"

[consumer]
command = ["iro-example-consumer", "serve", "--mode", "correct"]
timeout_seconds = 10

[database]
kind = "sqlite"

[ordering]
mode = "conflict_keys"

[[projection.tables]]
name = "balances"
columns = ["account_id", "balance"]
order_by = ["account_id"]

[testing]
runs = 10
seed = 42
max_duplicates_per_event = 2
max_restarts = 4
enable_crash_faults = true
enable_reordering = true
```

---

## PostgreSQL Setup

To test with PostgreSQL, start a local test database:

```sh
docker compose up -d --wait
export IRO_POSTGRES_URL=postgres://iro:iro-local-test@127.0.0.1:54329/iro_test

# Run PostgreSQL-specific integration tests
cargo test --workspace --test replay postgres_ -- --ignored

# Verify with PostgreSQL manifest
iro verify --manifest examples/manifests/postgres-correct.toml --report-dir reports/postgres
```

Each run generates an isolated `iro_<hash>` schema, requires consumer schema attestation, qualifies all reads, and drops the schema upon completion. For details, see [PostgreSQL safeguards](docs/postgres.md).

---

## Architecture & Verification Pipeline

```
Canonical Fixture ──> Validation ──> 2x Canonical Baselines ──> Schedule Generator
                                                                      │
                                                                      ▼
                                                              Consumer Process
                                                                      │
                                                              SQLite / PostgreSQL
                                                                      │
                                                              Normalized Snapshot
                                                                      │
                                                                Replay Oracle
                                                                      │
                                                      Pass / Failure Bundle (.json)
                                                                      │
                                                        Delta-Debugging Minimizer
                                                                      │
                                                          Reduced Bundle Trace
```

See [docs/architecture.md](docs/architecture.md) for detailed snapshot sorting, numeric normalization, and memory bounds.

---

## Development & Testing

```sh
# Check code formatting
cargo fmt --all -- --check

# Run linter
cargo clippy --workspace --all-targets -- -D warnings

# Execute unit, integration, and proptest property tests
cargo test --workspace
```

---

## Limits & Scope

- **v0.1 Scope**: Sequential and bounded. Does not simulate deep blockchain reorgs, multichain causal dependencies, or Byzantine faults.
- **Trusted Executable**: Consumers run as local child processes without sandbox containerization; never execute unreviewed manifests or untrusted binaries.
- **Crash Hook Contract**: Exit code 86 relies on the consumer correctly placing the exit immediately after commit; an exit code cannot independently verify SQL transaction engine internals.

---

## Contributing

Contributions are welcome! Please review [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before submitting pull requests.

## License & Custom Work

[![Hire me](https://img.shields.io/badge/Open%20to%20work-work%40withrana.com-brightgreen)](mailto:work@withrana.com) [![LinkedIn](https://img.shields.io/badge/LinkedIn-workwithrana-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/workwithrana)
[![Upwork](https://img.shields.io/badge/Upwork-Hire%20Me-14A800?style=flat&logo=upwork&logoColor=white)](https://www.upwork.com/freelancers/~01bbcf6edb91a8df5f)
[![Contra](https://img.shields.io/badge/Contra-Hire%20Me-000000?style=flat)](https://contra.com/ranajahanzaib/)

**License:** MIT, see [LICENSE](LICENSE). Free for personal and commercial use.

**Author:** Rana Jahanzaib

**Need this built for your stack?** Email me what you're working on: work@withrana.com
