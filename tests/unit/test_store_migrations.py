import os
import sqlite3
from pathlib import Path

import pytest

from software_factory import db
from software_factory.db import (
    SCHEMA_VERSION,
    ProcessProbeError,
    Store,
    StoreError,
    UnsupportedSchemaVersion,
)
from software_factory.models import ClaimRequest, MachineState


def test_process_start_identity_rejects_undecodable_starttime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stat = b"123 (present process) S " + b" ".join([b"0"] * 18 + [b"\xff"])
    reads = {
        Path("/proc/sys/kernel/random/boot_id"): b"boot-id\n",
        Path("/proc/123/stat"): stat,
    }
    monkeypatch.setattr(Path, "read_bytes", lambda path: reads[path])

    with pytest.raises(ProcessProbeError, match="malformed"):
        db._process_start_identity(123)


def test_initialize_creates_versioned_schema_and_is_repeatable(tmp_path: Path) -> None:
    path = tmp_path / "factory.db"
    store = Store(path, busy_timeout_ms=4321)

    store.initialize()
    store.initialize()

    assert path.stat().st_mode & 0o777 == 0o600
    with store.connect() as connection:
        assert (
            connection.execute("SELECT version FROM schema_version").fetchone()[0] == SCHEMA_VERSION
        )
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert {"runs", "leases", "workers", "attempts", "events"} <= tables
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == 4321
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_future_schema_is_refused_without_downgrade(tmp_path: Path) -> None:
    path = tmp_path / "future.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE schema_version(version INTEGER NOT NULL)")
    connection.execute("INSERT INTO schema_version VALUES (?)", (SCHEMA_VERSION + 1,))
    connection.commit()
    connection.close()

    with pytest.raises(UnsupportedSchemaVersion):
        Store(path).initialize()

    assert (
        sqlite3.connect(path).execute("SELECT version FROM schema_version").fetchone()[0]
        == SCHEMA_VERSION + 1
    )


def test_version_one_database_is_migrated_for_fencing(tmp_path: Path) -> None:
    path = tmp_path / "v1.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE schema_version(version INTEGER NOT NULL);
        INSERT INTO schema_version VALUES(1);
        CREATE TABLE runs(
          run_id TEXT PRIMARY KEY, linear_issue_id TEXT UNIQUE, repository_key TEXT,
          branch TEXT, worktree TEXT, pr_number INTEGER, machine_state TEXT,
          cycle_count INTEGER, created_at TEXT, updated_at TEXT, last_head_sha TEXT,
          implementer TEXT, reviewer TEXT
        );
        CREATE TABLE leases(
          linear_issue_id TEXT PRIMARY KEY, run_id TEXT UNIQUE, owner_instance TEXT,
          owner_pid INTEGER, owner_pid_started_at TEXT, acquired_at TEXT,
          heartbeat_at TEXT, expires_at TEXT
        );
        CREATE TABLE workers(
          worker_id TEXT PRIMARY KEY, run_id TEXT, kind TEXT, tool TEXT, pid INTEGER,
          pid_started_at TEXT, started_at TEXT, heartbeat_at TEXT, finished_at TEXT,
          exit_code INTEGER, artifact_dir TEXT
        );
        CREATE TABLE attempts(
          attempt_id TEXT PRIMARY KEY, run_id TEXT, cycle INTEGER, kind TEXT, tool TEXT,
          session_id TEXT, exact_head_sha TEXT, outcome TEXT, started_at TEXT,
          finished_at TEXT, artifact_dir TEXT
        );
        CREATE TABLE events(
          event_id TEXT PRIMARY KEY, run_id TEXT, type TEXT, payload_json TEXT,
          idempotency_key TEXT, created_at TEXT, delivered_at TEXT
        );
        """
    )
    connection.close()

    Store(path).initialize()

    with Store(path).connect() as migrated:
        assert migrated.execute("SELECT version FROM schema_version").fetchone()[0] == 2
        assert "epoch" in {row[1] for row in migrated.execute("PRAGMA table_info(leases)")}
        assert "lease_epoch" in {row[1] for row in migrated.execute("PRAGMA table_info(events)")}


def test_connect_corrects_preexisting_database_mode(tmp_path: Path) -> None:
    path = tmp_path / "factory.db"
    sqlite3.connect(path).close()
    path.chmod(0o644)

    Store(path).connect().close()

    assert path.stat().st_mode & 0o777 == 0o600


def test_connect_rejects_database_symlink_without_mutating_target(tmp_path: Path) -> None:
    target = tmp_path / "target.db"
    sqlite3.connect(target).close()
    target.chmod(0o644)
    link = tmp_path / "factory.db"
    link.symlink_to(target)

    with pytest.raises(StoreError, match="symlink"):
        Store(link).connect()

    assert target.stat().st_mode & 0o777 == 0o644


def test_foreign_keys_and_checks_are_enforced(tmp_path: Path) -> None:
    store = Store(tmp_path / "factory.db")
    store.initialize()
    with store.connect() as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO workers(worker_id, run_id, kind, tool, pid, pid_started_at, "
                "started_at, heartbeat_at, artifact_dir) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "w",
                    "missing",
                    "implementer",
                    "codex",
                    os.getpid(),
                    "x",
                    "t",
                    "t",
                    str(tmp_path / "a"),
                ),
            )


def test_worker_completion_fields_must_be_set_together(tmp_path: Path) -> None:
    store = Store(tmp_path / "factory.db")
    store.initialize()
    store.claim_ticket(ClaimRequest("issue", "run", "repo", "branch", "/work"), "owner", 1, "start")
    lease = store.get_lease("issue")
    with store.connect() as connection:
        connection.execute(
            "INSERT INTO workers(worker_id, run_id, kind, tool, pid, pid_started_at, "
            "started_at, heartbeat_at, artifact_dir, lease_epoch) "
            "VALUES('worker', 'run', 'implementer', 'codex', 1, 'start', 't', 't', "
            "'/artifacts', ?)",
            (lease.epoch,),
        )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE workers SET finished_at = 'finished' WHERE worker_id = 'worker'"
            )

    store.finish_worker("worker", 0, lease)
    with store.connect() as connection:
        row = connection.execute(
            "SELECT finished_at, exit_code FROM workers WHERE worker_id = 'worker'"
        ).fetchone()
        assert row[0] is not None
        assert row[1] == 0


def test_schema_accepts_every_machine_state_and_rejects_unknown(tmp_path: Path) -> None:
    store = Store(tmp_path / "factory.db")
    store.initialize()
    with store.connect() as connection:
        for index, state in enumerate(MachineState):
            connection.execute(
                "INSERT INTO runs(run_id, linear_issue_id, repository_key, branch, worktree, "
                "machine_state, cycle_count, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?, 0, 't', 't')",
                (f"r{index}", f"i{index}", "repo", "branch", "/work", state),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO runs(run_id, linear_issue_id, repository_key, branch, worktree, "
                "machine_state, cycle_count, created_at, updated_at) "
                "VALUES('bad', 'bad', 'repo', 'branch', '/work', 'UNKNOWN', 0, 't', 't')"
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO runs(run_id, linear_issue_id, repository_key, branch, worktree, "
                "machine_state, cycle_count, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("r", "i", "repo", "b", str(tmp_path / "w"), "bad-state", -1, "t", "t"),
            )
