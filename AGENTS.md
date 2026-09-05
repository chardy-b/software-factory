# Contributor rules

- Work test-first: add a failing regression test before each behavior change.
- Never store, print, resolve, or commit secrets; configuration accepts references only.
- Deliver changes through pull requests only. Do not push directly to protected branches.
- Only humans may merge, release, or deploy this package.
- Keep WIL-131 limited to configuration validation and local diagnostics; do not add
  scheduling, SQLite, claim logic, workers, or pull-request automation.
