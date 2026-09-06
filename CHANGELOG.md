# Changelog

All notable changes to this public, versioned package are documented here.

## Unreleased

- Add transactional, versioned SQLite persistence for runs, leases, workers, attempts,
  and idempotent events.
- Add exclusive ticket claims, owner-checked lease renewal and release, safe expired
  lease recovery, resumable run identity, and process-identity-aware worker capacity.

## 0.1.0 - 2026-09-05

- Add strict, secret-safe YAML configuration validation.
- Add independent Codex/Claude session routing contracts.
- Add a local-only `doctor` command with redacted diagnostics.
