# Indexer Replay Oracle

[![CI](https://github.com/ranajahanzaib/indexer-replay-oracle/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ranajahanzaib/indexer-replay-oracle/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**`iro` tests whether an application's persistent event projection survives replay.** It feeds offline canonical blockchain events to your consumer, injects duplicate deliveries, restarts real processes, exercises a negotiated crash point, and compares normalized SQLite or PostgreSQL projection state.

Use it alongside Ponder, Envio, The Graph, or a custom indexer. Wrap your application's handler with a small subprocess protocol; keep ownership of migrations, transactions, and projection logic. No node, RPC endpoint, ABI decoder, or indexing service is required.

## Quickstart

Install the current stable Rust toolchain. SQLite is bundled; no database service is required.

```sh
cargo build --workspace --locked
export PATH="$PWD/target/debug:$PATH"
iro fixture validate --manifest examples/manifests/correct.toml
iro verify --manifest examples/manifests/correct.toml --runs 10 --seed 42 --report-dir reports/correct
```

The correct consumer passes canonical execution, baseline determinism, exact replay, duplicates, restart recovery, commit-before-ack crash recovery, admissible reordering, and seeded mixed schedules.

Now exercise the deliberate bug:

```sh
iro verify --manifest examples/manifests/buggy.toml --runs 10 --seed 42 --report-dir reports/buggy
# exits 1: STATE_DIVERGENCE
# balances, account alice, balance: 125 -> 325, delta 200 (duplicate-delivery)

iro minimize --bundle reports/buggy/failure-duplicate-delivery
iro reproduce --bundle reports/buggy/failure-duplicate-delivery/minimized
# exits 1: STATE_DIVERGENCE
```

The reduced duplicate trace contains one source event and two deliveries. A one-pass canonical test succeeds in buggy mode; the replay exposes double crediting. Minimization is explicit so verification has predictable execution cost.

## What is tested

Two clean canonical runs use separate empty workspaces. If they differ, `NONDETERMINISTIC_BASELINE` stops verification. An optional independently supplied expected snapshot detects `EXPECTED_STATE_MISMATCH`. Without that golden snapshot, a consistent but incorrect handler can pass replay equivalence.

Each subsequent schedule starts in another fresh database. Named scenarios exercise specific behaviors; the requested `--runs` adds seeded mixed schedules. All operations and observed acknowledgements are recorded in JSON, including automatic redelivery after a supported crash. Reports count baselines and named scenarios as well as mixed schedules.

Duplicate deliveries keep the source identity. A process can commit effects and die before acknowledging them; a retry must leave the business projection unchanged. The reference consumer exits with code 86 after a transaction commits, allowing the harness to distinguish this declared hook from an arbitrary kill.

Reordering is **strict by default**. Opting into `conflict_keys` declares that events with disjoint keys may commute. Shared keys preserve source order; explicit dependencies always apply. Missing conflict metadata is rejected under this policy. See [ordering semantics](docs/ordering-model.md).

A pass means no divergence was observed under the recorded bounded schedules. This is not a proof for all executions, concurrency patterns, or blockchain behavior.

## Import an offline fixture

```sh
iro fixture record --input exported-events.jsonl --output fixtures/ledger/events.jsonl \
  --chain-id 31337 --from-block 100 --to-block 150
```

Input uses the same decoded JSONL event schema as [the checked-in fixture](examples/fixtures/events.jsonl). Recording selects the declared chain and range, sorts canonically, validates, normalizes hashes and metadata, and refuses to overwrite an existing output. It does not query a blockchain. Validation prints the canonical SHA-256 fingerprint; every report and bundle records it.

See [fixture format](docs/fixture-format.md) and the [example manifest](examples/manifests/correct.toml). Relative fixture, expected-state, and executable paths resolve from the manifest directory. Executable names without `/` use `PATH`; executable arguments are literal arguments, with no shell expansion. Build the example binaries before using the manifests.

## Integrate a consumer

Implement the [version 1 consumer protocol](docs/consumer-protocol.md) over JSONL stdin/stdout. Use `IRO_DATABASE_URL` and, for PostgreSQL, `IRO_DATABASE_SCHEMA`; initialize tables idempotently before replying to `hello`. Apply events transactionally and acknowledge only durable commits. Keep diagnostics on stderr. Your consumer may be written in any language.

Configure explicitly selected tables, columns, and unique row keys. Only these domain projections are compared. The examples deliberately exclude `processed_events`; matching balances does not prove checkpoint correctness. Include checkpoint tables in the projection if they are part of your invariant. No columns are automatically ignored, including timestamps.

The example stores arbitrary-size integer balances as text and registers event IDs with a primary key in the same transaction as the balance update. PostgreSQL additionally locks balance rows before updating them.

## Reports and CI

```sh
iro verify --manifest examples/manifests/correct.toml --seed 12345 --runs 100 \
  --report-dir reports --junit reports/results.xml
```

Outputs: terminal summary, `report.json` (version 1), `report.md`, optional JUnit XML, and one bundle directory per failing scenario. JSON includes full configuration, seed, exact operations, capabilities, observations, fingerprints, classifications, row/column differences, and artifact paths. A failure is not explained by a hash alone. Reports intentionally omit raw consumer stderr to avoid persisting credentials in arbitrary application diagnostics.

Exit codes: **0** all required checks passed; **1** application correctness/protocol/crash/timeout failure; **2** invalid configuration, environment/harness error, or a required unsupported fault. Enabling crash faults makes the hook required: a missing capability is reported as `UNSUPPORTED_FAULT` and cannot count as success. Disable it explicitly for consumers that cannot expose the hook. Such runs provide no commit-boundary coverage.

GitHub Actions can run the command above and upload `reports/` with `actions/upload-artifact`. The repository's [CI](.github/workflows/ci.yml) builds and tests on Linux and macOS and runs PostgreSQL tests on Linux.

## PostgreSQL

```sh
docker compose up -d --wait
export IRO_POSTGRES_URL=postgres://iro:iro-local-test@127.0.0.1:54329/iro_test
cargo test --workspace --test replay postgres_ -- --ignored
cargo build --workspace
iro verify --manifest examples/manifests/postgres-correct.toml --report-dir reports/postgres
```

Each execution creates its own schema without `IF NOT EXISTS`, requires the consumer to attest its active schema, qualifies snapshot reads, and drops only that execution's schema afterward. Loopback hosts only; URL query parameters are rejected. Credentials live in an environment variable, never in the manifest or bundle. See [PostgreSQL setup and safeguards](docs/postgres.md).

## Reproduction and reduction

A bundle carries source events, the exact schedule, normalized expected and actual states, observations, and an executable fingerprint. `reproduce` executes that schedule without regeneration. Bundles are independent of the original fixture file. They still require the configured executable and a compatible environment; see [relocation instructions and reduction guarantees](docs/failure-minimization.md).

The minimizer removes events with all their deliveries, removes optional operations, and disables unnecessary fault hooks. It rejects dangling dependencies and incomplete schedules, establishes two fresh reduced baselines, and confirms divergence repeatedly. Results describe stable local 1-minimality under these reductions or budget exhaustion, never global optimality. Golden-state failures and infrastructure failures are reproducible but not minimized automatically, because deleting source events would invalidate those oracles.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

[Architecture](docs/architecture.md) · [Contributing](CONTRIBUTING.md) · [Code of conduct](CODE_OF_CONDUCT.md) · [MIT license](LICENSE)

## Limits

v0.1 is sequential and bounded. It does not simulate reorgs, concurrent writers, live capture, multichain causality, network partitions, or arbitrary operating-system crashes. A negotiated hook relies on the consumer honoring the documented transaction boundary; an exit code cannot independently prove its implementation. Consumers are trusted executables, inherit the host environment, and are not network-isolated. Prefer dedicated test credentials and never run an unreviewed manifest.

SQLite values preserve storage types; PostgreSQL supports the documented scalar types and rejects unsupported types. Snapshots require unique configured row keys, load selected rows into memory within fixed limits, and compare ordered typed values. Cross-backend snapshot equality is not promised. Process crashes of the harness itself may leave PostgreSQL schemas for manual inspection; normal completion and scenario failures clean them up. No database workspace preservation option is currently exposed.
