# Fixture and manifest format v1

Each nonempty UTF-8 JSONL line contains one `Event`; unknown fields are errors. Maximum fixture size is 256 MiB, each record 1 MiB; the manifest limits event count (default 10,000, hard ceiling 1,000,000).

Required fields: `version` (1), `chain_id` (u64), `block_number`, `block_hash` (0x + 64 hex), `transaction_hash` (0x + 64 hex), `transaction_index`, `log_index`, `event_type`, and object `payload`. Optional `contract_address` is null or 0x + 40 hex. `conflict_keys` and `dependencies` default to empty arrays. Hashes and addresses normalize to lowercase; metadata arrays sort. Do not put passwords, private keys, host paths, nondeterministic UUIDs, or wall-clock values in fixture payloads. Amounts in the sample are nonnegative base-10 integer strings, never floats.

Event ID is SHA-256 of compact JSON encoding `[chain_id, lowercase_block_hash, lowercase_transaction_hash, log_index]`. This unambiguous tuple is independent of delivery order and payload. Dependencies contain those 64-character hex IDs. The fixture fingerprint hashes compact JSON serialization of the complete validated normalized event array (not raw file bytes).

A validated fixture has one chain, lies inside the manifest's inclusive block range, has unique identities, consistent block/transaction metadata, one transaction per transaction position, strictly increasing block/transaction/log coordinates and block-global log indices, existing dependencies, and a DAG whose canonical order is valid. Forward dependencies are rejected because the clean source order must itself respect the contract. Diagnostics identify fixture file and record line.

Recording accepts this same export schema, filters chain and range, sorts, validates, and writes normalized JSONL. It never fabricates events, performs ABI decoding, or contacts RPC. It leaves manifest/projection selection to the developer.

Manifest required sections are demonstrated in `examples/manifests/correct.toml`. All sections reject unknown fields. `version`, `name`, `fixture`, `chain_id`, `from_block`, and `to_block` are required; `expected_state` is optional. Relative paths resolve from the manifest directory, including an executable containing `/`. Consumer argument strings are unchanged and should not contain host-specific paths if portability is needed. The harness does not change consumer working directory.

Defaults:

| Setting                                            | Default          |
| -------------------------------------------------- | ---------------- |
| consumer.timeout_seconds / startup_timeout_seconds | 10 / 10          |
| consumer.max_message_bytes / max_stderr_bytes      | 1 MiB / 64 KiB   |
| ordering.mode                                      | strict           |
| testing.runs / seed                                | 20 / 42          |
| max_duplicates_per_event / max_restarts            | 2 / 4            |
| enable_crash_faults / enable_reordering            | true / false     |
| max_events / max_schedule_length                   | 10,000 / 100,000 |
| max_run_seconds                                    | 120              |
| max_minimization_attempts / minimization_seconds   | 100 / 120        |
| confirmations                                      | 2                |

`database.kind` is `sqlite` or `postgres`; only PostgreSQL accepts `url_env` (default `IRO_POSTGRES_URL`). Projection identifiers are restricted ASCII SQL identifiers up to 63 characters, always quoted. Columns and keys must be nonempty; keys must be selected columns and uniquely identify rows. Business metadata exclusions are explicit through column selection; the tool never guesses volatile fields.

`expected-state.json` uses the normalized snapshot representation shown in `examples/fixtures/expected-state.json`. Tables, columns, keys, row widths, version, unique keys and canonical row sorting are checked. Every column value is tagged (`null`, `integer`, `decimal`, `text`, `binary`, `json`, `boolean`, `timestamp`, `float`); binary uses lowercase hex, float uses IEEE bits in hex, exact numbers use strings. Golden states should come from an independent calculation, not a blind copy of the consumer output.

Additional fixed guards: manifests 1 MiB, at most 128 projection tables and 256 columns per table, snapshots at most 100,000 rows per table and 64 MiB selected SQL data across tables, expected snapshots 128 MiB, bundles 256 MiB. Protocol timeouts are at most 300 seconds; scenario/minimization durations at most 3600 seconds. Deadline cleanup may add the time needed to terminate a child and release its local database workspace.
