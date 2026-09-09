import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from software_factory import db
from software_factory.db import (
    ConstraintStoreError,
    LeaseConflict,
    LeaseOwnershipError,
    ProcessProbeError,
    RetriableStoreError,
    Store,
    StoreError,
)
from software_factory.models import Attempt, ClaimRequest, Event, Worker


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


def claim(store: Store, issue: str = "I-1", run: str = "r1", owner: str = "one"):
    store.claim_ticket(
        ClaimRequest(issue, run, "repo", "branch", "/work"),
        owner,
        1,
        "boot:start",
        timedelta(seconds=1),
    )
    return store.get_lease(issue)


def test_stale_epoch_cannot_start_or_finish_and_does_not_wedge_owner(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "db", clock=clock, process_probe=lambda _pid: None)
    store.initialize()
    old = claim(store)
    old_worker = Worker(
        "old-w", "r1", "implementer", "codex", 19, "boot:old-w", clock.now, clock.now, "/old"
    )
    store.start_worker(old_worker, old)
    clock.now += timedelta(seconds=2)
    current_run = store.claim_ticket(
        ClaimRequest("I-1", "ignored", "repo", "branch", "/work"), "two", 2, "boot:new"
    )
    current = store.get_lease("I-1")
    worker = Worker(
        "w", current_run.run_id, "implementer", "codex", 20, "boot:w", clock.now, clock.now, "/a"
    )

    with pytest.raises(LeaseOwnershipError):
        store.finish_worker("old-w", 0, old)
    with pytest.raises(StoreError):
        store.finish_worker("old-w", 0, current)
    with pytest.raises(LeaseOwnershipError):
        store.start_worker(worker, old)
    store.start_worker(worker, current)
    with pytest.raises(LeaseOwnershipError):
        store.finish_worker("w", 0, old)
    store.finish_worker("w", 0, current)


def test_later_epoch_cannot_finish_prior_attempt_but_can_finish_its_own(
    tmp_path: Path,
) -> None:
    clock = Clock()
    store = Store(tmp_path / "db", clock=clock, process_probe=lambda _pid: None)
    store.initialize()
    old = claim(store)
    now = clock.now
    store.start_attempt(
        Attempt("old-a", "r1", 0, "implementer", "codex", None, "a" * 40, None, now, None, "/old"),
        old,
    )
    clock.now += timedelta(seconds=2)
    store.claim_ticket(
        ClaimRequest("I-1", "ignored", "repo", "branch", "/work"), "two", 2, "boot:new"
    )
    current = store.get_lease("I-1")

    with pytest.raises(StoreError):
        store.finish_attempt("old-a", "passed", current)

    store.start_attempt(
        Attempt(
            "current-a",
            "r1",
            1,
            "implementer",
            "codex",
            None,
            "b" * 40,
            None,
            clock.now,
            None,
            "/current",
        ),
        current,
    )
    store.finish_attempt("current-a", "passed", current)
    assert store.get_attempt("current-a").outcome == "passed"


def test_owner_aware_capacity_excludes_other_owner(tmp_path: Path) -> None:
    store = Store(tmp_path / "db", process_probe=lambda pid: f"boot:w{pid}")
    store.initialize()
    one = claim(store, "I-1", "r1", "one")
    two = claim(store, "I-2", "r2", "two")
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store.start_worker(
        Worker("w1", "r1", "implementer", "codex", 11, "boot:w11", now, now, "/a"), one
    )
    store.start_worker(
        Worker("w2", "r2", "implementer", "codex", 12, "boot:w12", now, now, "/b"), two
    )
    assert store.active_worker_count_for_owner("one", 1, "boot:start") == 1
    assert store.active_worker_count_for_owner("two", 1, "boot:start") == 1
    assert store.host_active_worker_count() == 2


def test_needs_input_requires_explicit_resume(tmp_path: Path) -> None:
    store = Store(tmp_path / "db", process_probe=lambda _pid: None)
    store.initialize()
    lease = claim(store)
    store.park_lease_for_needs_input("I-1", "one", 1, "boot:start", lease.epoch)
    request = ClaimRequest("I-1", "ignored", "repo", "branch", "/work")
    with pytest.raises(LeaseConflict, match="human-authorized"):
        store.claim_ticket(request, "two", 2, "boot:new")
    store.resume_after_input("I-1", "human@example.com")
    assert store.claim_ticket(request, "two", 2, "boot:new").run_id == "r1"


def test_process_identity_handles_non_ascii_and_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stat = b"123 (\xff name) S " + b" ".join([b"0"] * 18 + [b"987"])
    reads = {Path("/proc/sys/kernel/random/boot_id"): b"boot\n", Path("/proc/123/stat"): stat}
    monkeypatch.setattr(Path, "read_bytes", lambda path: reads[path])
    assert db._process_start_identity(123) == "boot:987"
    monkeypatch.setattr(Path, "read_bytes", lambda _path: (_ for _ in ()).throw(PermissionError()))
    with pytest.raises(ProcessProbeError):
        db._process_start_identity(123)


def test_public_sqlite_errors_are_normalized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = Store(tmp_path / "db")
    store.initialize()
    claim(store)
    with pytest.raises(ConstraintStoreError):
        store.claim_ticket(ClaimRequest("I-2", "r1", "repo", "branch", "/work"), "x", 2, "x")
    lease = store.get_lease("I-1")
    now = datetime(2026, 1, 1, tzinfo=UTC)
    bad = Attempt("a", "r1", 0, "invalid", "codex", None, "a" * 40, None, now, None, "/a")
    with pytest.raises(ConstraintStoreError):
        store.start_attempt(bad, lease)
    monkeypatch.setattr(
        store,
        "connect",
        lambda: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked")),
    )
    with pytest.raises(RetriableStoreError):
        store.get_attempt("a")


class FailingConnection:
    def __init__(self, error: sqlite3.Error) -> None:
        self.error = error
        self.closed = False
        self.row_factory = None

    def execute(self, _statement: str, _parameters: object = None):
        raise self.error

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (sqlite3.OperationalError("database is locked"), RetriableStoreError),
        (sqlite3.DatabaseError("malformed database"), StoreError),
    ],
)
def test_connect_closes_and_normalizes_pragma_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error: sqlite3.Error,
    expected: type[StoreError],
) -> None:
    connection = FailingConnection(error)
    monkeypatch.setattr(sqlite3, "connect", lambda *_args, **_kwargs: connection)

    with pytest.raises(expected):
        Store(tmp_path / "db").connect()

    assert connection.closed


@pytest.mark.parametrize(
    ("method", "error", "expected"),
    [
        (
            "host_active_worker_count",
            sqlite3.OperationalError("database is busy"),
            RetriableStoreError,
        ),
        ("host_active_worker_count", sqlite3.DatabaseError("malformed database"), StoreError),
        (
            "active_worker_count_for_owner",
            sqlite3.OperationalError("database is locked"),
            RetriableStoreError,
        ),
        ("active_worker_count_for_owner", sqlite3.DatabaseError("malformed database"), StoreError),
    ],
)
def test_worker_counts_normalize_sqlite_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    error: sqlite3.Error,
    expected: type[StoreError],
) -> None:
    store = Store(tmp_path / "db")
    connection = FailingConnection(error)
    monkeypatch.setattr(store, "connect", lambda: connection)

    with pytest.raises(expected):
        if method == "host_active_worker_count":
            store.host_active_worker_count()
        else:
            store.active_worker_count_for_owner("owner", 1, "boot:start")

    assert connection.closed


def test_outbox_is_bounded_idempotent_and_fenced(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "db", clock=clock, process_probe=lambda _pid: None)
    store.initialize()
    lease = claim(store)
    now = clock.now
    store.record_event(Event("e1", "r1", "kind", {}, "k1", now), lease)
    store.record_event(Event("e2", "r1", "kind", {}, "k2", now + timedelta(seconds=1)), lease)
    assert [event.event_id for event in store.undelivered_events(lease, limit=1)] == ["e1"]
    delivered = store.mark_event_delivered("e1", lease, now + timedelta(seconds=2))
    assert store.mark_event_delivered("e1", lease, now + timedelta(seconds=3)) == delivered
    with pytest.raises(ValueError):
        store.undelivered_events(lease, limit=0)

    clock.now += timedelta(seconds=2)
    store.claim_ticket(
        ClaimRequest("I-1", "ignored", "repo", "branch", "/work"), "two", 2, "boot:new"
    )
    current = store.get_lease("I-1")
    with pytest.raises(LeaseOwnershipError):
        store.undelivered_events(lease)
    assert [event.event_id for event in store.undelivered_events(current)] == ["e2"]
    assert store.mark_event_delivered("e2", current).delivered_at == clock.now
