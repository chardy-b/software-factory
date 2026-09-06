import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from software_factory.db import Store, StoreError
from software_factory.models import Attempt, ClaimRequest, Event


def _store_with_run(tmp_path: Path) -> Store:
    store = Store(tmp_path / "factory.db")
    store.initialize()
    store.claim_ticket(ClaimRequest("I-1", "r1", "repo", "branch", "/work"), "owner", 1, "start")
    return store


def test_attempt_can_be_started_and_durably_finished(tmp_path: Path) -> None:
    store = _store_with_run(tmp_path)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    attempt = Attempt(
        "a1", "r1", 0, "implementer", "codex", "session", "a" * 40, None, now, None, "/artifacts"
    )
    store.start_attempt(attempt)
    store.finish_attempt("a1", "passed", now)

    assert store.get_attempt("a1").outcome == "passed"
    assert Store(store.path).get_attempt("a1").finished_at == now


def test_exact_head_and_json_are_validated_and_events_are_idempotent(tmp_path: Path) -> None:
    store = _store_with_run(tmp_path)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="exact_head_sha"):
        store.start_attempt(
            Attempt("a", "r1", 0, "reviewer", "codex", None, "not-a-sha", None, now, None, "/a")
        )
    with pytest.raises(ValueError, match="JSON"):
        store.record_event(Event("e0", "r1", "bad", {"value": object()}, "bad", now))
    with pytest.raises(ValueError, match="JSON"):
        store.record_event(Event("e0", "r1", "bad", {1: "coerced"}, "bad-key", now))  # type: ignore[dict-item]
    with pytest.raises(ValueError, match="JSON"):
        store.record_event(Event("e0", "r1", "bad", {"value": float("nan")}, "nan", now))
    cyclic: list[Any] = []
    cyclic.append(cyclic)
    with pytest.raises(ValueError, match="JSON"):
        store.record_event(Event("e0", "r1", "bad", cyclic, "cycle", now))

    event = Event("e1", "r1", "completed", {"ok": True}, "run-r1-completed", now)
    first = store.record_event(event)
    second = store.record_event(
        Event("e2", "r1", "completed", {"ok": True}, "run-r1-completed", now)
    )
    assert first.event_id == second.event_id == "e1"


@pytest.mark.parametrize(
    ("run_id", "event_type", "payload"),
    [
        ("r2", "completed", {"ok": True}),
        ("r1", "failed", {"ok": True}),
        ("r1", "completed", {"ok": False}),
    ],
)
def test_event_idempotency_key_rejects_conflicting_semantics(
    tmp_path: Path, run_id: str, event_type: str, payload: dict[str, bool]
) -> None:
    store = _store_with_run(tmp_path)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    if run_id == "r2":
        store.claim_ticket(
            ClaimRequest("I-2", "r2", "repo", "branch-2", "/work-2"), "owner", 2, "start"
        )
    store.record_event(Event("e1", "r1", "completed", {"ok": True}, "same-key", now))

    with pytest.raises(StoreError, match="idempotency"):
        store.record_event(Event("e2", run_id, event_type, payload, "same-key", now))


def test_semantically_equal_event_payload_is_idempotent_despite_mapping_order(
    tmp_path: Path,
) -> None:
    store = _store_with_run(tmp_path)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    first = store.record_event(Event("e1", "r1", "kind", {"a": 1, "b": 2}, "same", now))
    duplicate = store.record_event(Event("e2", "r1", "kind", {"b": 2, "a": 1}, "same", now))

    assert duplicate.event_id == first.event_id == "e1"


@pytest.mark.parametrize("method", ["active_worker_count", "get_attempt"])
def test_store_owned_read_connections_close_deterministically(tmp_path: Path, method: str) -> None:
    store = _store_with_run(tmp_path)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store.start_attempt(
        Attempt("a1", "r1", 0, "implementer", "codex", None, "a" * 40, None, now, None, "/a")
    )
    opened = []
    original_connect = store.connect

    def tracking_connect():
        connection = original_connect()
        opened.append(connection)
        return connection

    store.connect = tracking_connect  # type: ignore[method-assign]

    getattr(store, method)(*("a1",) if method == "get_attempt" else ())

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        opened[-1].execute("SELECT 1")
