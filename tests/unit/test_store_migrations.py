import os
import sqlite3
from pathlib import Path

import pytest

from software_factory import db
from software_factory.db import SCHEMA_VERSION, Store, StoreError, UnsupportedSchemaVersion
from software_factory.models import MachineState


def test_process_start_identity_treats_undecodable_stat_as_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_decode_error(_path: Path, *, encoding: str) -> str:
        assert encoding == "ascii"
        raise UnicodeDecodeError("ascii", b"\xff", 0, 1, "ordinal not in range")

    monkeypatch.setattr(Path, "read_text", raise_decode_error)

    assert db._process_start_identity(123) is None


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
    with store.connect() as connection:
        connection.execute(
            "INSERT INTO runs(run_id, linear_issue_id, repository_key, branch, worktree, "
            "machine_state, cycle_count, created_at, updated_at) "
            "VALUES('run', 'issue', 'repo', 'branch', '/work', 'CLAIMING', 0, 't', 't')"
        )
        connection.execute(
            "INSERT INTO workers(worker_id, run_id, kind, tool, pid, pid_started_at, "
            "started_at, heartbeat_at, artifact_dir) "
            "VALUES('worker', 'run', 'implementer', 'codex', 1, 'start', 't', 't', '/artifacts')"
        )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE workers SET finished_at = 'finished' WHERE worker_id = 'worker'"
            )

    store.finish_worker("worker", 0)
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
