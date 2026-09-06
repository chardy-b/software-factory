# software-factory

`software-factory` is an installable Python 3.12 command-line foundation for
validating a single-host software factory's configuration and local prerequisites.

WIL-131 provides strict configuration parsing and the local-only `doctor` command.
WIL-132 adds a local SQLite persistence boundary for versioned run records, exclusive
ticket leases, worker liveness, attempts, and idempotent events. It does **not** add a
scheduler, agent execution, pull-request automation, live Linear validation, merging,
releasing, or deployment.

The store uses SQLite WAL mode, foreign-key enforcement, bounded lease heartbeats, and
PID plus process-start identity checks. New and pre-existing database files opened by
the store are forced to mode `0600`. SQLite sidecars are created under SQLite's
restrictive derived permissions where the platform supports them; this is not a claim
of protection against hostile same-user pathname replacement races.
Worker capacity counts all unfinished worker ledger rows whose PID and process-start identity
exactly match a live process, regardless of lease owner or whether the lease still exists.

## Install and use

```console
python3.12 -m venv .venv
.venv/bin/pip install .
.venv/bin/software-factory doctor --config config.yaml
```

Copy `config.example.yaml`, which contains the Pacenotes pilot repository and verified
Linear state identities, then replace its local absolute paths as needed. Configuration
accepts credential references only: `api_key_env` is an environment-variable name and
`keyholder_grant` is a Keyholder grant name. Secret values and credentialed URLs are
rejected, and diagnostics never read or print credential values.

Repository, artifact, worktree, and daemon state paths must be absolute. Routing uses
independent implementer and reviewer session objects, each with `tool` (`codex` or
`claude`), `model`, and a unique nonempty `session_namespace`; see the example config.
Model names containing `max` or `ultra` are rejected.

`doctor` validates the YAML contract, checks configured executables on `PATH`, verifies
each local Git repository and its `origin` (normalizing safe HTTPS and optional `.git`
forms), and checks state/artifact/worktree writability against each target or its nearest
existing parent without creating files or directories. Configured paths containing
existing symlink components are rejected. It does not mutate Git or call Linear, GitHub,
or any live service.

## Development

```console
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy
.venv/bin/python -m build
```

CI installs the Python 3.12 dependency set from `requirements-dev.lock` with hash
verification, installs this repository without dependency resolution or build isolation,
and builds without an isolated dependency download. Regenerate the lock deliberately
after reviewing dependency updates.
