# Architecture

```text
Canonical fixture -> Validator -> Two clean baselines -> Schedule generator
                                                             |
                                                             v
                                                     Consumer subprocess
                                                             |
                                                     SQLite / PostgreSQL
                                                             |
                                                     Normalized snapshot
                                                             |
                                                       Replay oracle
                                                             |
                                              Pass / failure bundle
                                                             |
                                               Verified failure minimizer
                                                             |
                                                Reduced reproduction bundle
```

A single Rust library contains domain modules; `iro` handles CLI/output and `iro-example-consumer` implements the reference external protocol. `fixture` validates identities, source coordinates, bounded metadata, and dependencies. `schedule` constructs source constraints and uses deterministic topological sorting. `protocol` owns a real child and bounded I/O; `runner` executes operations in an isolated database workspace. `database` selects configured projection values into `snapshot` typed cells. `report` compares executions and persists evidence. `minimize` invokes the same runner and oracle rather than approximating application effects.

Each execution has an independent temporary directory. SQLite uses a WAL database there. PostgreSQL uses a generated `iro_` schema created by this workspace, with no fallback to public. A restart retains that database while replacing the process. A supported fault exits before acknowledging; the harness checks code 86, starts another process, and redelivers the same event before continuing.

Canonical state is JSON with version 1, sorted table names, manifest column order, typed cells, and rows ordered by the configured unique keys. Fingerprints are SHA-256 of its compact serialization. SQL NULL differs from text, numeric, and JSON null. Integers and decimals remain exact; float values preserve bits and never enter amount arithmetic. JSON object keys are sorted by the default serde_json map implementation. PostgreSQL timestamps use a stable UTC encoding for timestamptz. SQLite JSON stored as TEXT remains TEXT; PostgreSQL native JSON/JSONB receives canonical JSON encoding. Select explicit supported native types or expose an application-owned view with suitable representations.

The report distinguishes state divergence, unstable baseline, golden mismatch, handler failure, unexpected crash, malformed protocol, timeouts, missing capabilities, and harness errors. SQL/environment errors are not called projection bugs. The harness does not infer semantic correctness or dependency contracts.
