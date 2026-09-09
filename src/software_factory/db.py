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

SCHEMA_VERSION = 2
_SHA = re.compile(r"^[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?$")


class StoreError(RuntimeError):
    """Base class for durable-store errors."""


class UnsupportedSchemaVersion(StoreError):
    """The database is not at a schema version this package supports."""


class LeaseConflict(StoreError):
    """The issue is already owned or still has a live worker."""


class LeaseOwnershipError(StoreError):
    """A lease mutation was attempted by a stale or different owner."""


class ConstraintStoreError(StoreError):
    """A public write violated a database uniqueness, foreign-key, or check constraint."""


class RetriableStoreError(StoreError):
    """A database lock or busy timeout prevented an operation and may be retried."""


class ProcessProbeError(StoreError):
    """Process liveness could not be determined safely on this host."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _process_start_identity(pid: int) -> str | None:
    """Return boot ID plus Linux process start tick; None means the PID is absent."""
    try:
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_bytes().strip().decode("ascii")
    except (OSError, UnicodeDecodeError) as exc:
        raise ProcessProbeError("Linux /proc boot identity is unavailable") from exc
    try:
        stat = Path(f"/proc/{pid}/stat").read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ProcessProbeError(
            f"Linux /proc process identity is unreadable for PID {pid}"
        ) from exc
    closing = stat.rfind(b")")
    fields = stat[closing + 2 :].split() if closing >= 0 else []
    if len(fields) <= 19 or not boot_id:
        raise ProcessProbeError(f"Linux /proc process identity is malformed for PID {pid}")
    try:
        starttime = fields[19].decode("ascii")
    except UnicodeDecodeError as exc:
        raise ProcessProbeError(f"Linux /proc process identity is malformed for PID {pid}") from exc
    return f"{boot_id}:{starttime}"


def _raise_store_error(exc: sqlite3.Error) -> None:
    if isinstance(exc, sqlite3.IntegrityError):
        raise ConstraintStoreError("database constraint rejected the operation") from exc
    if isinstance(exc, sqlite3.OperationalError) and any(
        word in str(exc).lower() for word in ("locked", "busy")
    ):
        raise RetriableStoreError("database is busy; the operation may be retried") from exc
    raise StoreError("database operation failed") from exc


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
            # Python's sqlite3 API does not expose SQLITE_OPEN_NOFOLLOW, and
            # ``nofollow`` is not a SQLite URI query parameter. Keep the
            # O_NOFOLLOW-opened file pinned and have SQLite open that exact
            # descriptor through Linux procfs, eliminating a second lookup of
            # the caller-controlled final path component.
            database_uri = Path(f"/proc/self/fd/{descriptor}").as_uri() + "?mode=rw"
            connection = sqlite3.connect(database_uri, isolation_level=None, uri=True)
        except sqlite3.Error as exc:
            os.close(descriptor)
            _raise_store_error(exc)
        except BaseException:
            os.close(descriptor)
            raise
        os.close(descriptor)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms:d}")
            connection.execute("PRAGMA journal_mode = WAL")
        except sqlite3.Error as exc:
            connection.close()
            _raise_store_error(exc)
        except BaseException:
            connection.close()
            raise
        return connection

    @contextmanager
    def _transaction(self, mode: str = "IMMEDIATE") -> Iterator[sqlite3.Connection]:
        try:
            connection = self.connect()
        except sqlite3.Error as exc:
            _raise_store_error(exc)
        try:
            connection.execute(f"BEGIN {mode}")
            yield connection
            connection.commit()
        except sqlite3.Error as exc:
            connection.rollback()
            _raise_store_error(exc)
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
                if len(versions) == 1 and versions[0][0] == 1:
                    self._migrate_v1(connection)
                    return
                if len(versions) != 1 or versions[0][0] != SCHEMA_VERSION:
                    version = versions[0][0] if versions else "missing"
                    raise UnsupportedSchemaVersion(f"unsupported schema version: {version}")
                return
            self._create_schema(connection)

    @staticmethod
    def _migrate_v1(connection: sqlite3.Connection) -> None:
        statements = (
            "ALTER TABLE runs ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK(lease_epoch >= 0)",  # noqa: E501
            "ALTER TABLE runs ADD COLUMN input_resume_authorized_at TEXT",
            "ALTER TABLE runs ADD COLUMN input_resume_authorized_by TEXT",
            "ALTER TABLE leases ADD COLUMN epoch INTEGER NOT NULL DEFAULT 1 CHECK(epoch > 0)",
            "ALTER TABLE workers ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 1 CHECK(lease_epoch > 0)",  # noqa: E501
            "ALTER TABLE attempts ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 1 CHECK(lease_epoch > 0)",  # noqa: E501
            "ALTER TABLE events ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 1 CHECK(lease_epoch > 0)",  # noqa: E501
            "UPDATE runs SET lease_epoch = COALESCE((SELECT epoch FROM leases WHERE leases.run_id = runs.run_id), 1)",  # noqa: E501
            "UPDATE schema_version SET version = 2",
        )
        for statement in statements:
            connection.execute(statement)

    def _create_schema(self, connection: sqlite3.Connection) -> None:
        machine_states = ",".join(f"'{state.value}'" for state in MachineState)
        # Interpolation is safe: values come exclusively from the closed MachineState enum.
        schema = (""  # nosec B608  # noqa: S608
            f"""
            CREATE TABLE schema_version(version INTEGER NOT NULL CHECK(version >= 1));
            INSERT INTO schema_version VALUES (2);
            CREATE TABLE runs(
              run_id TEXT PRIMARY KEY, linear_issue_id TEXT NOT NULL UNIQUE,
              repository_key TEXT NOT NULL, branch TEXT NOT NULL, worktree TEXT NOT NULL,
              pr_number INTEGER CHECK(pr_number IS NULL OR pr_number > 0),
              machine_state TEXT NOT NULL
                CHECK(machine_state IN ({machine_states})),
              cycle_count INTEGER NOT NULL CHECK(cycle_count >= 0), created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL, last_head_sha TEXT,
              implementer TEXT, reviewer TEXT
              , lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK(lease_epoch >= 0)
              , input_resume_authorized_at TEXT, input_resume_authorized_by TEXT
            );
            CREATE TABLE leases(
              linear_issue_id TEXT PRIMARY KEY REFERENCES runs(linear_issue_id) ON DELETE CASCADE,
              run_id TEXT NOT NULL UNIQUE REFERENCES runs(run_id) ON DELETE CASCADE,
              owner_instance TEXT NOT NULL, owner_pid INTEGER NOT NULL CHECK(owner_pid > 0),
              owner_pid_started_at TEXT NOT NULL, acquired_at TEXT NOT NULL,
              heartbeat_at TEXT NOT NULL, expires_at TEXT NOT NULL,
              epoch INTEGER NOT NULL CHECK(epoch > 0)
            );
            CREATE TABLE workers(
              worker_id TEXT PRIMARY KEY,
              run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
              kind TEXT NOT NULL CHECK(kind IN ('implementer','reviewer')), tool TEXT NOT NULL,
              pid INTEGER NOT NULL CHECK(pid > 0), pid_started_at TEXT NOT NULL,
              started_at TEXT NOT NULL, heartbeat_at TEXT NOT NULL, finished_at TEXT,
              exit_code INTEGER, artifact_dir TEXT NOT NULL,
              lease_epoch INTEGER NOT NULL CHECK(lease_epoch > 0),
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
              lease_epoch INTEGER NOT NULL CHECK(lease_epoch > 0),
              CHECK(outcome IS NULL OR finished_at IS NOT NULL)
            );
            CREATE TABLE events(
              event_id TEXT PRIMARY KEY,
              run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
              type TEXT NOT NULL, payload_json TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
              created_at TEXT NOT NULL, delivered_at TEXT,
              lease_epoch INTEGER NOT NULL CHECK(lease_epoch > 0)
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

    def _refuse_live_worker(self, connection: sqlite3.Connection, run_id: str, epoch: int) -> None:
        workers = connection.execute(
            "SELECT pid, pid_started_at FROM workers WHERE run_id = ? AND lease_epoch = ? "
            "AND finished_at IS NULL",
            (run_id, epoch),
        ).fetchall()
        if any(self.process_probe(row["pid"]) == row["pid_started_at"] for row in workers):
            raise LeaseConflict(
                "run has a live worker; finish it before removing or reacquiring the lease"
            )

    def _require_lease(self, connection: sqlite3.Connection, lease: Lease) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM leases WHERE run_id = ? AND owner_instance = ? AND owner_pid = ? "
            "AND owner_pid_started_at = ? AND epoch = ? AND expires_at > ?",
            (
                lease.run_id,
                lease.owner_instance,
                lease.owner_pid,
                lease.owner_pid_started_at,
                lease.epoch,
                _timestamp(self.clock()),
            ),
        ).fetchone()
        if row is None:
            raise LeaseOwnershipError(
                "lease fence is stale, expired, absent, or belongs to another owner"
            )
        return cast(sqlite3.Row, row)

    def get_lease(self, linear_issue_id: str) -> Lease:
        try:
            with closing(self.connect()) as connection:
                row = connection.execute(
                    "SELECT * FROM leases WHERE linear_issue_id = ?", (linear_issue_id,)
                ).fetchone()
        except sqlite3.Error as exc:
            _raise_store_error(exc)
        if row is None:
            raise KeyError(linear_issue_id)
        return self._lease(row)

    def get_run_for_issue(self, linear_issue_id: str) -> Run | None:
        """Return the persisted run identity for reconciliation, if one exists."""
        try:
            with closing(self.connect()) as connection:
                row = connection.execute(
                    "SELECT * FROM runs WHERE linear_issue_id = ?", (linear_issue_id,)
                ).fetchone()
        except sqlite3.Error as exc:
            _raise_store_error(exc)
        return self._run(row) if row is not None else None

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
            is_existing_run = run_row is not None
            existing_lease = connection.execute(
                "SELECT * FROM leases WHERE linear_issue_id = ?", (request.linear_issue_id,)
            ).fetchone()
            if existing_lease is not None:
                if cast(datetime, _datetime(existing_lease["expires_at"])) > now:
                    raise LeaseConflict("issue has an active lease")
                self._refuse_live_worker(
                    connection, existing_lease["run_id"], existing_lease["epoch"]
                )
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
                if run_row["input_resume_authorized_at"] is None:
                    raise LeaseConflict("NEEDS_INPUT run requires explicit human-authorized resume")
                connection.execute(
                    "UPDATE runs SET machine_state = ?, updated_at = ?, "
                    "input_resume_authorized_at = NULL, input_resume_authorized_by = NULL "
                    "WHERE run_id = ?",
                    (MachineState.CLAIMING, _timestamp(now), run_row["run_id"]),
                )
                run_row = connection.execute(
                    "SELECT * FROM runs WHERE run_id = ?", (run_row["run_id"],)
                ).fetchone()
            if is_existing_run and existing_lease is None:
                self._refuse_live_worker(connection, run_row["run_id"], run_row["lease_epoch"])
            epoch = int(run_row["lease_epoch"]) + 1
            connection.execute(
                "UPDATE runs SET lease_epoch = ? WHERE run_id = ?", (epoch, run_row["run_id"])
            )
            connection.execute(
                "INSERT INTO leases VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    request.linear_issue_id,
                    run_row["run_id"],
                    owner_instance,
                    owner_pid,
                    owner_pid_started_at,
                    _timestamp(now),
                    _timestamp(now),
                    _timestamp(expires),
                    epoch,
                ),
            )
            return self._run(run_row)

    def heartbeat_lease(
        self,
        linear_issue_id: str,
        owner_instance: str,
        owner_pid: int,
        owner_pid_started_at: str,
        epoch: int,
        ttl: timedelta,
    ) -> Lease:
        now = self.clock()
        expires = self._ttl_expiry(now, ttl)
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE leases SET heartbeat_at = ?, expires_at = ? "
                "WHERE linear_issue_id = ? AND owner_instance = ? AND owner_pid = ? "
                "AND owner_pid_started_at = ? AND epoch = ? AND expires_at > ?",
                (
                    _timestamp(now),
                    _timestamp(expires),
                    linear_issue_id,
                    owner_instance,
                    owner_pid,
                    owner_pid_started_at,
                    epoch,
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
        epoch: int,
    ) -> None:
        now = self.clock()
        with self._transaction() as connection:
            lease = connection.execute(
                "SELECT run_id, epoch FROM leases WHERE linear_issue_id = ? AND owner_instance = ? "
                "AND owner_pid = ? AND owner_pid_started_at = ? AND epoch = ? AND expires_at > ?",
                (
                    linear_issue_id,
                    owner_instance,
                    owner_pid,
                    owner_pid_started_at,
                    epoch,
                    _timestamp(now),
                ),
            ).fetchone()
            if lease is None:
                raise LeaseOwnershipError("lease is stale, absent, or belongs to another owner")
            self._refuse_live_worker(connection, lease["run_id"], lease["epoch"])
            connection.execute(
                "DELETE FROM leases WHERE linear_issue_id = ?",
                (linear_issue_id,),
            )

    def rollback_claim(self, lease: Lease) -> None:
        """Fence, record a failed external claim, and release its lease atomically."""
        now = self.clock()
        with self._transaction("IMMEDIATE") as connection:
            self._require_lease(connection, lease)
            self._refuse_live_worker(connection, lease.run_id, lease.epoch)
            cursor = connection.execute(
                "UPDATE runs SET machine_state = ?, updated_at = ? "
                "WHERE run_id = ? AND lease_epoch = ?",
                (MachineState.CLAIM_ROLLBACK, _timestamp(now), lease.run_id, lease.epoch),
            )
            if cursor.rowcount != 1:
                raise LeaseOwnershipError("run no longer matches the lease fence")
            connection.execute(
                "DELETE FROM leases WHERE run_id = ? AND epoch = ?", (lease.run_id, lease.epoch)
            )

    def park_lease_for_needs_input(
        self,
        linear_issue_id: str,
        owner_instance: str,
        owner_pid: int,
        owner_pid_started_at: str,
        epoch: int,
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
                "AND epoch = ? AND expires_at > ?",
                (*identity[:-1], epoch, identity[-1]),
            ).fetchone()
            if lease is None:
                raise LeaseOwnershipError("lease is stale, absent, or belongs to another owner")
            self._refuse_live_worker(connection, lease["run_id"], epoch)
            connection.execute(
                "UPDATE runs SET machine_state = ?, updated_at = ? WHERE run_id = ?",
                (MachineState.NEEDS_INPUT, _timestamp(now), lease["run_id"]),
            )
            connection.execute(
                "DELETE FROM leases WHERE linear_issue_id = ?",
                (linear_issue_id,),
            )

    def resume_after_input(self, linear_issue_id: str, authorized_by: str) -> None:
        if not authorized_by.strip():
            raise ValueError("authorized_by must be nonempty")
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE runs SET input_resume_authorized_at = ?, input_resume_authorized_by = ?, "
                "updated_at = ? WHERE linear_issue_id = ? AND machine_state = ?",
                (
                    _timestamp(self.clock()),
                    authorized_by,
                    _timestamp(self.clock()),
                    linear_issue_id,
                    MachineState.NEEDS_INPUT,
                ),
            )
            if cursor.rowcount != 1:
                raise StoreError("run is absent or is not waiting for input")

    def start_worker(self, worker: Worker, lease: Lease) -> None:
        with self._transaction() as connection:
            self._require_lease(connection, lease)
            if worker.run_id != lease.run_id:
                raise LeaseOwnershipError("worker run does not match lease fence")
            connection.execute(
                "INSERT INTO workers VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
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
                    lease.epoch,
                ),
            )

    def finish_worker(
        self, worker_id: str, exit_code: int, lease: Lease, finished_at: datetime | None = None
    ) -> None:
        with self._transaction() as connection:
            self._require_lease(connection, lease)
            cursor = connection.execute(
                "UPDATE workers SET finished_at = ?, exit_code = ? "
                "WHERE worker_id = ? AND run_id = ? AND lease_epoch = ? "
                "AND finished_at IS NULL",
                (
                    _timestamp(finished_at or self.clock()),
                    exit_code,
                    worker_id,
                    lease.run_id,
                    lease.epoch,
                ),
            )
            if cursor.rowcount != 1:
                raise StoreError("worker is absent or already finished")

    def host_active_worker_count(self) -> int:
        try:
            with closing(self.connect()) as connection:
                rows = connection.execute(
                    "SELECT pid, pid_started_at FROM workers WHERE finished_at IS NULL"
                ).fetchall()
        except sqlite3.Error as exc:
            _raise_store_error(exc)
        return sum(self.process_probe(row["pid"]) == row["pid_started_at"] for row in rows)

    def active_worker_count_for_owner(
        self, owner_instance: str, owner_pid: int, owner_pid_started_at: str
    ) -> int:
        try:
            with closing(self.connect()) as connection:
                rows = connection.execute(
                    "SELECT w.pid, w.pid_started_at FROM workers w JOIN leases l "
                    "ON l.run_id = w.run_id AND l.epoch = w.lease_epoch "
                    "WHERE w.finished_at IS NULL AND l.owner_instance = ? AND l.owner_pid = ? "
                    "AND l.owner_pid_started_at = ? AND l.expires_at > ?",
                    (owner_instance, owner_pid, owner_pid_started_at, _timestamp(self.clock())),
                ).fetchall()
        except sqlite3.Error as exc:
            _raise_store_error(exc)
        return sum(self.process_probe(row["pid"]) == row["pid_started_at"] for row in rows)

    def start_attempt(self, attempt: Attempt, lease: Lease) -> None:
        if not _SHA.fullmatch(attempt.exact_head_sha):
            raise ValueError("exact_head_sha must be an exact 40- or 64-character hexadecimal SHA")
        with self._transaction() as connection:
            self._require_lease(connection, lease)
            if attempt.run_id != lease.run_id:
                raise LeaseOwnershipError("attempt run does not match lease fence")
            connection.execute(
                "INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
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
                    lease.epoch,
                ),
            )

    def finish_attempt(
        self, attempt_id: str, outcome: str, lease: Lease, finished_at: datetime | None = None
    ) -> None:
        with self._transaction() as connection:
            self._require_lease(connection, lease)
            cursor = connection.execute(
                "UPDATE attempts SET outcome = ?, finished_at = ? "
                "WHERE attempt_id = ? AND run_id = ? AND lease_epoch = ? "
                "AND finished_at IS NULL",
                (
                    outcome,
                    _timestamp(finished_at or self.clock()),
                    attempt_id,
                    lease.run_id,
                    lease.epoch,
                ),
            )
            if cursor.rowcount != 1:
                raise StoreError("attempt is absent or already finished")

    def get_attempt(self, attempt_id: str) -> Attempt:
        try:
            with closing(self.connect()) as connection:
                row = connection.execute(
                    "SELECT * FROM attempts WHERE attempt_id = ?", (attempt_id,)
                ).fetchone()
        except sqlite3.Error as exc:
            _raise_store_error(exc)
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

    def record_event(self, event: Event, lease: Lease) -> Event:
        _validate_json(event.payload)
        try:
            payload = json.dumps(
                event.payload, allow_nan=False, separators=(",", ":"), sort_keys=True
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("event payload must be valid JSON") from exc
        with self._transaction() as connection:
            self._require_lease(connection, lease)
            if event.run_id != lease.run_id:
                raise LeaseOwnershipError("event run does not match lease fence")
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
                "INSERT INTO events VALUES(?,?,?,?,?,?,?,?)",
                (
                    event.event_id,
                    event.run_id,
                    event.type,
                    payload,
                    event.idempotency_key,
                    _timestamp(event.created_at),
                    _timestamp(event.delivered_at) if event.delivered_at else None,
                    lease.epoch,
                ),
            )
            return event

    def undelivered_events(self, lease: Lease, *, limit: int = 100) -> list[Event]:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        with self._transaction() as connection:
            self._require_lease(connection, lease)
            # Unlike worker and attempt rows, the outbox is deliberately run-scoped:
            # a current owner may drain events left by an earlier epoch after a crash.
            rows = connection.execute(
                "SELECT * FROM events WHERE run_id = ? "
                "AND delivered_at IS NULL ORDER BY created_at, event_id LIMIT ?",
                (lease.run_id, limit),
            ).fetchall()
            return [self._event(row) for row in rows]

    def mark_event_delivered(
        self, event_id: str, lease: Lease, delivered_at: datetime | None = None
    ) -> Event:
        with self._transaction() as connection:
            self._require_lease(connection, lease)
            # Keep delivery run-scoped across epochs so crash recovery can finish the outbox.
            row = connection.execute(
                "SELECT * FROM events WHERE event_id = ? AND run_id = ?",
                (event_id, lease.run_id),
            ).fetchone()
            if row is None:
                raise StoreError("event is absent or belongs to another lease epoch")
            if row["delivered_at"] is None:
                connection.execute(
                    "UPDATE events SET delivered_at = ? WHERE event_id = ?",
                    (_timestamp(delivered_at or self.clock()), event_id),
                )
                row = connection.execute(
                    "SELECT * FROM events WHERE event_id = ?", (event_id,)
                ).fetchone()
            return self._event(row)

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
            epoch=row["epoch"],
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
