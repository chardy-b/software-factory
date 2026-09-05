# software-factory

`software-factory` is an installable Python 3.12 command-line foundation for
validating a single-host software factory's configuration and local prerequisites.

This WIL-131 scope intentionally includes only strict configuration parsing and the
local-only `doctor` command. It does **not** include a scheduler, SQLite storage,
ticket claiming, agent execution, pull-request automation, live Linear validation,
merging, releasing, or deployment.

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
