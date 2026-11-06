# Changelog

All notable changes to Indexer Replay Oracle are documented here.

This changelog follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/). Project releases follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial Rust CLI and library for deterministic replay testing of event projections.
- Versioned JSONL fixtures, TOML manifests, consumer subprocess protocol, and SQLite/PostgreSQL adapters.
- Duplicate delivery, process restart, deterministic crash-hook, and dependency-aware ordering scenarios.
- Normalized projection snapshots, row-level differences, JSON/Markdown/JUnit reports, reproduction bundles, and state-divergence minimization.
- Correct and deliberately buggy reference consumers with example fixtures and manifests.
- User and contributor documentation, GitHub Actions CI, and local PostgreSQL Compose setup.
