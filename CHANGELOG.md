# Changelog

All notable changes to this public, versioned package are documented here.

## Unreleased

- Add transactional, versioned SQLite persistence for runs, leases, workers, attempts,
  and idempotent events.
- Add exclusive ticket claims, owner-checked lease renewal and release, safe expired
  lease recovery, resumable run identity, and process-identity-aware worker capacity.
- Fence worker, attempt, and event writes with monotonic lease epochs; make capacity
  owner-aware while retaining an explicitly host-global diagnostic count.
- Fail closed when Linux process identity cannot be probed, include boot identity, and
  parse non-ASCII process names safely.
- Require explicit human authorization to resume `NEEDS_INPUT` runs, normalize public
  SQLite errors, and complete the event outbox retrieval/delivery lifecycle.
- Migrate pre-release schema version 1 databases transactionally to version 2.

## 0.1.0 - 2026-09-05

- Add strict, secret-safe YAML configuration validation.
- Add independent Codex/Claude session routing contracts.
- Add a local-only `doctor` command with redacted diagnostics.
