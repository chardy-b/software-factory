"""Transactional SQLite persistence and local ticket leases."""

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from software_factory.models import Attempt, ClaimRequest, Event, Lease, MachineState, Run, Worker

SCHEMA_VERSION = 1
_SHA = re.compile(r"^[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?$")


class StoreError(RuntimeError):
    """Base class for durable-store errors."""


class UnsupportedSchemaVersion(StoreError):
    """The database is not at a schema version this package supports."""


class LeaseConflict(StoreError):
    """The issue is already owned or still has a live worker."""


class LeaseOwnershipError(StoreError):
    """A lease mutation was attempted by a stale or different owner."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _process_start_identity(pid: int) -> str | None:
    """Return the Linux kernel process start tick, without inspecting credentials."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        return stat[stat.rfind(")") + 2 :].split()[19]
    except (OSError, UnicodeDecodeError, IndexError):
        return None


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _validate_json(value: object, *, _depth: int = 0, _seen: set[int] | None = None) -> None:
    """Validate the strict JSON data model without exposing payload values in errors."""
    if _depth > 100:
        raise ValueError("event payload must be valid JSON")
    if value is None or isinstance(value, (bool, str, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("event payload must be valid JSON")
        return
    if not isinstance(value, (list, dict)):
        raise ValueError("event payload must be valid JSON")

    seen = _seen if _seen is not None else set()
    identity = id(value)
    if identity in seen:
        raise ValueError("event payload must be valid JSON")
    seen.add(identity)
    try:
        if isinstance(value, dict):
            if any(not isinstance(key, str) for key in value):
                raise ValueError("event payload must be valid JSON")
            for child in value.values():
                _validate_json(child, _depth=_depth + 1, _seen=seen)
        else:
            for child in value:
                _validate_json(child, _depth=_depth + 1, _seen=seen)
    finally:
        seen.remove(identity)


class Store:
    """One local controller database; the workers table is its process ownership ledger."""

    def __init__(
        self,
        path: Path,
        *,
        busy_timeout_ms: int = 5_000,
        clock: Callable[[], datetime] = _utc_now,
        process_probe: Callable[[int], str | None] = _process_start_identity,
        max_lease_ttl: timedelta = timedelta(minutes=5),
    ) -> None:
        if busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be nonnegative")
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms
        self.clock = clock
        self.process_probe = process_probe
        self.max_lease_ttl = max_lease_ttl

    def connect(self) -> sqlite3.Connection:
        if self.path.is_symlink():
            raise StoreError(f"database path is a symlink: {self.path}")
        flags = os.O_RDWR | os.O_CREAT
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.path, flags | nofollow, 0o600)
        except OSError as exc:
            if self.path.is_symlink():
                raise StoreError(f"database path is a symlink: {self.path}") from exc
            raise StoreError(f"cannot securely open database: {self.path}") from exc
        try:
            os.fchmod(descriptor, 0o600)
            connection = sqlite3.connect(self.path, isolation_level=None)
        except BaseException:
            os.close(descriptor)
            raise
        os.close(descriptor)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms:d}")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    @contextmanager
    def _transaction(self, mode: str = "IMMEDIATE") -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute(f"BEGIN {mode}")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._transaction() as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'"
            ).fetchone()
            if table:
                versions = connection.execute("SELECT version FROM schema_version").fetchall()
                if len(versions) != 1 or versions[0][0] != SCHEMA_VERSION:
                    version = versions[0][0] if versions else "missing"
                    raise UnsupportedSchemaVersion(f"unsupported schema version: {version}")
                return
            self._create_schema(connection)

    def _create_schema(self, connection: sqlite3.Connection) -> None:
        machine_states = ",".join(f"'{state.value}'" for state in MachineState)
        # Interpolation is safe: values come exclusively from the closed MachineState enum.
        schema = (""  # nosec B608  # noqa: S608
            f"""
            CREATE TABLE schema_version(version INTEGER NOT NULL CHECK(version >= 1));
            INSERT INTO schema_version VALUES (1);
            CREATE TABLE runs(
              run_id TEXT PRIMARY KEY, linear_issue_id TEXT NOT NULL UNIQUE,
              repository_key TEXT NOT NULL, branch TEXT NOT NULL, worktree TEXT NOT NULL,
              pr_number INTEGER CHECK(pr_number IS NULL OR pr_number > 0),
              machine_state TEXT NOT NULL
                CHECK(machine_state IN ({machine_states})),
              cycle_count INTEGER NOT NULL CHECK(cycle_count >= 0), created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL, last_head_sha TEXT,
              implementer TEXT, reviewer TEXT
            );
            CREATE TABLE leases(
              linear_issue_id TEXT PRIMARY KEY REFERENCES runs(linear_issue_id) ON DELETE CASCADE,
              run_id TEXT NOT NULL UNIQUE REFERENCES runs(run_id) ON DELETE CASCADE,
              owner_instance TEXT NOT NULL, owner_pid INTEGER NOT NULL CHECK(owner_pid > 0),
              owner_pid_started_at TEXT NOT NULL, acquired_at TEXT NOT NULL,
              heartbeat_at TEXT NOT NULL, expires_at TEXT NOT NULL
            );
            CREATE TABLE workers(
              worker_id TEXT PRIMARY KEY,
              run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
              kind TEXT NOT NULL CHECK(kind IN ('implementer','reviewer')), tool TEXT NOT NULL,
              pid INTEGER NOT NULL CHECK(pid > 0), pid_started_at TEXT NOT NULL,
              started_at TEXT NOT NULL, heartbeat_at TEXT NOT NULL, finished_at TEXT,
              exit_code INTEGER, artifact_dir TEXT NOT NULL,
              CHECK(
                (finished_at IS NULL AND exit_code IS NULL)
                OR (finished_at IS NOT NULL AND exit_code IS NOT NULL)
              )
            );
            CREATE TABLE attempts(
              attempt_id TEXT PRIMARY KEY,
              run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
              cycle INTEGER NOT NULL CHECK(cycle >= 0),
              kind TEXT NOT NULL CHECK(kind IN ('implementer','reviewer')), tool TEXT NOT NULL,
              session_id TEXT, exact_head_sha TEXT NOT NULL,
              outcome TEXT, started_at TEXT NOT NULL, finished_at TEXT, artifact_dir TEXT NOT NULL,
              CHECK(outcome IS NULL OR finished_at IS NOT NULL)
            );
            CREATE TABLE events(
              event_id TEXT PRIMARY KEY,
              run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
              type TEXT NOT NULL, payload_json TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
              created_at TEXT NOT NULL, delivered_at TEXT
            );
            CREATE INDEX workers_run_unfinished ON workers(run_id) WHERE finished_at IS NULL;
            CREATE INDEX attempts_run_cycle ON attempts(run_id, cycle);
            CREATE INDEX events_run_created ON events(run_id, created_at);
            CREATE INDEX events_undelivered ON events(created_at) WHERE delivered_at IS NULL;
            """  # noqa: S608
        )  # fmt: skip
        # sqlite3.executescript() can commit a pending transaction. Executing this
        # static migration statement-by-statement keeps the version and DDL atomic.
        for statement in schema.split(";"):
            if statement.strip():
                connection.execute(statement)

    def _ttl_expiry(self, now: datetime, ttl: timedelta) -> datetime:
        if ttl <= timedelta(0) or ttl > self.max_lease_ttl:
            raise ValueError("lease ttl must be positive and bounded")
        return now + ttl

    def _refuse_live_worker(self, connection: sqlite3.Connection, run_id: str) -> None:
        workers = connection.execute(
            "SELECT pid, pid_started_at FROM workers WHERE run_id = ? AND finished_at IS NULL",
            (run_id,),
        ).fetchall()
        if any(self.process_probe(row["pid"]) == row["pid_started_at"] for row in workers):
            raise LeaseConflict(
                "run has a live worker; finish it before removing or reacquiring the lease"
            )

    def claim_ticket(
        self,
        request: ClaimRequest,
        owner_instance: str,
        owner_pid: int,
        owner_pid_started_at: str,
        ttl: timedelta = timedelta(minutes=1),
    ) -> Run:
        now = self.clock()
        expires = self._ttl_expiry(now, ttl)
        with self._transaction("IMMEDIATE") as connection:
            run_row = connection.execute(
                "SELECT * FROM runs WHERE linear_issue_id = ?", (request.linear_issue_id,)
            ).fetchone()
            if run_row is not None:
                self._refuse_live_worker(connection, run_row["run_id"])
            existing_lease = connection.execute(
                "SELECT * FROM leases WHERE linear_issue_id = ?", (request.linear_issue_id,)
            ).fetchone()
            if existing_lease is not None:
                if cast(datetime, _datetime(existing_lease["expires_at"])) > now:
                    raise LeaseConflict("issue has an active lease")
                connection.execute(
                    "DELETE FROM leases WHERE linear_issue_id = ?", (request.linear_issue_id,)
                )
            if run_row is None:
                connection.execute(
                    "INSERT INTO runs(run_id, linear_issue_id, repository_key, branch, worktree, "
                    "machine_state, cycle_count, created_at, updated_at) "
                    "VALUES(?,?,?,?,?,'CLAIMING',0,?,?)",
                    (
                        request.run_id,
                        request.linear_issue_id,
                        request.repository_key,
                        request.branch,
                        request.worktree,
                        _timestamp(now),
                        _timestamp(now),
                    ),
                )
                run_row = connection.execute(
                    "SELECT * FROM runs WHERE run_id = ?", (request.run_id,)
                ).fetchone()
            elif run_row["machine_state"] in (MachineState.COMPLETE, MachineState.FAILED):
                raise LeaseConflict("terminal run cannot be reacquired")
            elif run_row["machine_state"] == MachineState.NEEDS_INPUT:
                connection.execute(
                    "UPDATE runs SET machine_state = ?, updated_at = ? WHERE run_id = ?",
                    (MachineState.CLAIMING, _timestamp(now), run_row["run_id"]),
                )
                run_row = connection.execute(
                    "SELECT * FROM runs WHERE run_id = ?", (run_row["run_id"],)
                ).fetchone()
            connection.execute(
                "INSERT INTO leases VALUES(?,?,?,?,?,?,?,?)",
                (
                    request.linear_issue_id,
                    run_row["run_id"],
                    owner_instance,
                    owner_pid,
                    owner_pid_started_at,
                    _timestamp(now),
                    _timestamp(now),
                    _timestamp(expires),
                ),
            )
            return self._run(run_row)

    def heartbeat_lease(
        self,
        linear_issue_id: str,
        owner_instance: str,
        owner_pid: int,
        owner_pid_started_at: str,
        ttl: timedelta,
    ) -> Lease:
        now = self.clock()
        expires = self._ttl_expiry(now, ttl)
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE leases SET heartbeat_at = ?, expires_at = ? "
                "WHERE linear_issue_id = ? AND owner_instance = ? AND owner_pid = ? "
                "AND owner_pid_started_at = ? AND expires_at > ?",
                (
                    _timestamp(now),
                    _timestamp(expires),
                    linear_issue_id,
                    owner_instance,
                    owner_pid,
                    owner_pid_started_at,
                    _timestamp(now),
                ),
            )
            if cursor.rowcount != 1:
                raise LeaseOwnershipError("lease is stale or belongs to another owner")
            row = connection.execute(
                "SELECT * FROM leases WHERE linear_issue_id = ?", (linear_issue_id,)
            ).fetchone()
            return self._lease(row)

    def release_lease(
        self,
        linear_issue_id: str,
        owner_instance: str,
        owner_pid: int,
        owner_pid_started_at: str,
    ) -> None:
        now = self.clock()
        with self._transaction() as connection:
            lease = connection.execute(
                "SELECT run_id FROM leases WHERE linear_issue_id = ? AND owner_instance = ? "
                "AND owner_pid = ? AND owner_pid_started_at = ? AND expires_at > ?",
                (
                    linear_issue_id,
                    owner_instance,
                    owner_pid,
                    owner_pid_started_at,
                    _timestamp(now),
                ),
            ).fetchone()
            if lease is None:
                raise LeaseOwnershipError("lease is stale, absent, or belongs to another owner")
            self._refuse_live_worker(connection, lease["run_id"])
            connection.execute(
                "DELETE FROM leases WHERE linear_issue_id = ?",
                (linear_issue_id,),
            )

    def park_lease_for_needs_input(
        self,
        linear_issue_id: str,
        owner_instance: str,
        owner_pid: int,
        owner_pid_started_at: str,
    ) -> None:
        now = self.clock()
        identity = (
            linear_issue_id,
            owner_instance,
            owner_pid,
            owner_pid_started_at,
            _timestamp(now),
        )
        with self._transaction("IMMEDIATE") as connection:
            lease = connection.execute(
                "SELECT run_id FROM leases WHERE linear_issue_id = ? "
                "AND owner_instance = ? AND owner_pid = ? AND owner_pid_started_at = ? "
                "AND expires_at > ?",
                identity,
            ).fetchone()
            if lease is None:
                raise LeaseOwnershipError("lease is stale, absent, or belongs to another owner")
            self._refuse_live_worker(connection, lease["run_id"])
            connection.execute(
                "UPDATE runs SET machine_state = ?, updated_at = ? WHERE run_id = ?",
                (MachineState.NEEDS_INPUT, _timestamp(now), lease["run_id"]),
            )
            connection.execute(
                "DELETE FROM leases WHERE linear_issue_id = ?",
                (linear_issue_id,),
            )

    def start_worker(self, worker: Worker) -> None:
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO workers VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    worker.worker_id,
                    worker.run_id,
                    worker.kind,
                    worker.tool,
                    worker.pid,
                    worker.pid_started_at,
                    _timestamp(worker.started_at),
                    _timestamp(worker.heartbeat_at),
                    _timestamp(worker.finished_at) if worker.finished_at else None,
                    worker.exit_code,
                    worker.artifact_dir,
                ),
            )

    def finish_worker(
        self, worker_id: str, exit_code: int, finished_at: datetime | None = None
    ) -> None:
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE workers SET finished_at = ?, exit_code = ? "
                "WHERE worker_id = ? AND finished_at IS NULL",
                (_timestamp(finished_at or self.clock()), exit_code, worker_id),
            )
            if cursor.rowcount != 1:
                raise StoreError("worker is absent or already finished")

    def active_worker_count(self) -> int:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT pid, pid_started_at FROM workers WHERE finished_at IS NULL"
            ).fetchall()
        return sum(self.process_probe(row["pid"]) == row["pid_started_at"] for row in rows)

    def start_attempt(self, attempt: Attempt) -> None:
        if not _SHA.fullmatch(attempt.exact_head_sha):
            raise ValueError("exact_head_sha must be an exact 40- or 64-character hexadecimal SHA")
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    attempt.attempt_id,
                    attempt.run_id,
                    attempt.cycle,
                    attempt.kind,
                    attempt.tool,
                    attempt.session_id,
                    attempt.exact_head_sha.lower(),
                    attempt.outcome,
                    _timestamp(attempt.started_at),
                    _timestamp(attempt.finished_at) if attempt.finished_at else None,
                    attempt.artifact_dir,
                ),
            )

    def finish_attempt(
        self, attempt_id: str, outcome: str, finished_at: datetime | None = None
    ) -> None:
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE attempts SET outcome = ?, finished_at = ? "
                "WHERE attempt_id = ? AND finished_at IS NULL",
                (outcome, _timestamp(finished_at or self.clock()), attempt_id),
            )
            if cursor.rowcount != 1:
                raise StoreError("attempt is absent or already finished")

    def get_attempt(self, attempt_id: str) -> Attempt:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id = ?", (attempt_id,)
            ).fetchone()
        if row is None:
            raise KeyError(attempt_id)
        return Attempt(
            attempt_id=row["attempt_id"],
            run_id=row["run_id"],
            cycle=row["cycle"],
            kind=row["kind"],
            tool=row["tool"],
            session_id=row["session_id"],
            exact_head_sha=row["exact_head_sha"],
            outcome=row["outcome"],
            started_at=cast(datetime, _datetime(row["started_at"])),
            finished_at=_datetime(row["finished_at"]),
            artifact_dir=row["artifact_dir"],
        )

    def record_event(self, event: Event) -> Event:
        _validate_json(event.payload)
        try:
            payload = json.dumps(
                event.payload, allow_nan=False, separators=(",", ":"), sort_keys=True
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("event payload must be valid JSON") from exc
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM events WHERE idempotency_key = ?", (event.idempotency_key,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["run_id"] != event.run_id
                    or existing["type"] != event.type
                    or existing["payload_json"] != payload
                ):
                    raise StoreError("idempotency key conflicts with an existing event")
                return self._event(existing)
            connection.execute(
                "INSERT INTO events VALUES(?,?,?,?,?,?,?)",
                (
                    event.event_id,
                    event.run_id,
                    event.type,
                    payload,
                    event.idempotency_key,
                    _timestamp(event.created_at),
                    _timestamp(event.delivered_at) if event.delivered_at else None,
                ),
            )
            return event

    @staticmethod
    def _run(row: sqlite3.Row) -> Run:
        return Run(
            run_id=row["run_id"],
            linear_issue_id=row["linear_issue_id"],
            repository_key=row["repository_key"],
            branch=row["branch"],
            worktree=row["worktree"],
            pr_number=row["pr_number"],
            machine_state=row["machine_state"],
            cycle_count=row["cycle_count"],
            created_at=cast(datetime, _datetime(row["created_at"])),
            updated_at=cast(datetime, _datetime(row["updated_at"])),
            last_head_sha=row["last_head_sha"],
            implementer=row["implementer"],
            reviewer=row["reviewer"],
        )

    @staticmethod
    def _lease(row: sqlite3.Row) -> Lease:
        return Lease(
            linear_issue_id=row["linear_issue_id"],
            run_id=row["run_id"],
            owner_instance=row["owner_instance"],
            owner_pid=row["owner_pid"],
            owner_pid_started_at=row["owner_pid_started_at"],
            acquired_at=cast(datetime, _datetime(row["acquired_at"])),
            heartbeat_at=cast(datetime, _datetime(row["heartbeat_at"])),
            expires_at=cast(datetime, _datetime(row["expires_at"])),
        )

    @staticmethod
    def _event(row: sqlite3.Row) -> Event:
        return Event(
            event_id=row["event_id"],
            run_id=row["run_id"],
            type=row["type"],
            payload=cast(Any, json.loads(row["payload_json"])),
            idempotency_key=row["idempotency_key"],
            created_at=cast(datetime, _datetime(row["created_at"])),
            delivered_at=_datetime(row["delivered_at"]),
        )
