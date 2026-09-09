from datetime import UTC, datetime
from pathlib import Path

import pytest

from software_factory.db import Store
from software_factory.linear import (
    ClaimCoordinator,
    ClaimError,
    LinearAdapter,
    LinearIssue,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


class FakeLinear:
    def __init__(self, issue: LinearIssue) -> None:
        self.issue = issue
        self.markers: list[str] = []
        self.moves = 0

    def get_issue(self, issue_id: str, ready_state_id: str) -> LinearIssue:
        assert issue_id == self.issue.id
        assert ready_state_id == "ready"
        return self.issue

    def move_to_state(self, issue_id: str, state_id: str) -> None:
        self.moves += 1
        self.issue = self.issue.with_state(state_id)

    def add_comment(self, issue_id: str, body: str) -> None:
        self.markers.append(body)
        self.issue = self.issue.with_comments((*self.issue.comments, body))


def issue(**changes: object) -> LinearIssue:
    values = dict(
        id="issue-1",
        project_id="project",
        state_id="ready",
        priority=1,
        ready_at=NOW,
        comments=(),
        blockers=(),
        comments_complete=True,
        relations_complete=True,
    )
    values.update(changes)
    return LinearIssue(**values)  # type: ignore[arg-type]


def test_discovery_filters_server_side_and_caps_over_return() -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def transport(query: str, variables: dict[str, object]) -> object:
        calls.append((query, variables))
        nodes = [
            {
                "id": str(n),
                "priority": 2,
                "project": {"id": "p"},
                "state": {"id": "r"},
                "history": {
                    "nodes": [{
                        "createdAt": f"2026-01-{n + 1:02}T00:00:00Z",
                        "toStateId": "r",
                    }],
                    "pageInfo": {"hasNextPage": False},
                },
            }
            for n in range(4)
        ]
        return {"data": {"issues": {"nodes": nodes, "pageInfo": {"hasNextPage": False}}}}

    found = LinearAdapter(transport).discover("p", "r", limit=2)
    assert len(found) == 2
    assert calls[0][1] == {"projectId": "p", "stateId": "r", "first": 2}
    assert "filter" in calls[0][0]
    assert "history(first: 50)" in calls[0][0]
    assert "history(filter:" not in calls[0][0]


def test_discovery_rejects_truncated_or_unmatched_ready_history() -> None:
    def response(history: object) -> object:
        return {
            "data": {
                "issues": {
                    "nodes": [{
                        "id": "i",
                        "priority": 1,
                        "project": {"id": "p"},
                        "state": {"id": "r"},
                        "history": history,
                    }],
                    "pageInfo": {"hasNextPage": False},
                }
            }
        }

    with pytest.raises(ClaimError, match="history is truncated"):
        LinearAdapter(lambda _q, _v: response({
            "nodes": [{"createdAt": "2026-01-01T00:00:00Z", "toStateId": "r"}],
            "pageInfo": {"hasNextPage": True},
        })).discover("p", "r", limit=1)

    with pytest.raises(ClaimError, match="missing or ambiguous"):
        LinearAdapter(lambda _q, _v: response({
            "nodes": [{"createdAt": "2026-01-01T00:00:00Z", "toStateId": "other"}],
            "pageInfo": {"hasNextPage": False},
        })).discover("p", "r", limit=1)


def test_issue_snapshot_uses_inverse_blockers_and_matching_ready_transition() -> None:
    def transport(query: str, _variables: dict[str, object]) -> object:
        assert "inverseRelations" in query
        assert "issue { id state { type } }" in query
        return {"data": {"issue": {
            "id": "issue-1",
            "priority": 1,
            "project": {"id": "project"},
            "state": {"id": "ready"},
            "history": {
                "nodes": [
                    {"createdAt": "2024-01-01T00:00:00Z", "toStateId": "ready"},
                    {"createdAt": "2025-01-01T00:00:00Z", "toStateId": "other"},
                    {"createdAt": "2026-01-01T00:00:00Z", "toStateId": "ready"},
                ],
                "pageInfo": {"hasNextPage": False},
            },
            "comments": {"nodes": [], "pageInfo": {"hasNextPage": False}},
            "inverseRelations": {
                "nodes": [{
                    "type": "blocks",
                    "issue": {"id": "blocker", "state": {"type": "started"}},
                }],
                "pageInfo": {"hasNextPage": False},
            },
        }}}

    snapshot = LinearAdapter(transport).get_issue("issue-1", "ready")
    assert snapshot.ready_at == NOW
    assert snapshot.blockers == ("blocker",)


def test_issue_snapshot_rejects_malformed_or_truncated_history() -> None:
    base = {
        "id": "issue-1",
        "priority": 1,
        "project": {"id": "project"},
        "state": {"id": "ready"},
        "comments": {"nodes": [], "pageInfo": {"hasNextPage": False}},
        "inverseRelations": {"nodes": [], "pageInfo": {"hasNextPage": False}},
    }

    for history in (
        {"nodes": [{"createdAt": "not-a-date", "toStateId": "ready"}],
         "pageInfo": {"hasNextPage": False}},
        {"nodes": [{"createdAt": "2026-01-01T00:00:00Z", "toStateId": "ready"}],
         "pageInfo": {"hasNextPage": True}},
    ):
        raw = {**base, "history": history}
        with pytest.raises(ClaimError):
            LinearAdapter(lambda _q, _v, raw=raw: {"data": {"issue": raw}}).get_issue(
                "issue-1", "ready"
            )


@pytest.mark.parametrize("response", [{}, {"errors": [{"message": "no"}]}, {"data": None}])
def test_adapter_rejects_malformed_or_error_responses(response: object) -> None:
    with pytest.raises(ClaimError):
        LinearAdapter(lambda _q, _v: response).discover("p", "r", limit=1)


def test_ordering_is_priority_then_ready_time_then_id() -> None:
    items = [
        issue(id="z", priority=2),
        issue(id="b", priority=1),
        issue(id="a", priority=1),
        issue(id="none", priority=0),
    ]
    assert [item.id for item in LinearAdapter.order(items)] == ["a", "b", "z", "none"]


def test_claim_rolls_back_and_releases_on_linear_failure(tmp_path: Path) -> None:
    store = Store(tmp_path / "db", clock=lambda: NOW, process_probe=lambda _pid: "start")
    store.initialize()
    remote = FakeLinear(issue())
    remote.move_to_state = lambda _issue, _state: (_ for _ in ()).throw(RuntimeError("fail"))
    coordinator = ClaimCoordinator(
        store,
        remote,
        project_id="project",
        ready_state_id="ready",
        in_progress_state_id="progress",
        repository_key="repo",
        daemon_instance="d",
        owner_pid=1,
        owner_pid_started_at="start",
        clock=lambda: NOW,
    )
    with pytest.raises(ClaimError):
        coordinator.claim(issue())
    with store.connect() as connection:
        assert (
            connection.execute("SELECT machine_state FROM runs").fetchone()[0] == "CLAIM_ROLLBACK"
        )
        assert connection.execute("SELECT count(*) FROM leases").fetchone()[0] == 0


def test_claim_creates_one_marker_and_restart_reuses_run(tmp_path: Path) -> None:
    store = Store(tmp_path / "db", clock=lambda: NOW, process_probe=lambda _pid: "start")
    store.initialize()
    remote = FakeLinear(issue())
    coordinator = ClaimCoordinator(
        store,
        remote,
        project_id="project",
        ready_state_id="ready",
        in_progress_state_id="progress",
        repository_key="repo",
        daemon_instance="d",
        owner_pid=1,
        owner_pid_started_at="start",
        clock=lambda: NOW,
    )
    first = coordinator.claim(issue())
    lease = store.get_lease("issue-1")
    store.release_lease("issue-1", "d", 1, "start", lease.epoch)
    second = coordinator.claim(remote.issue)
    assert first.run_id == second.run_id
    assert len(remote.markers) == 1


def test_claim_rejects_blockers_mapping_and_human_in_progress(tmp_path: Path) -> None:
    store = Store(tmp_path / "db", clock=lambda: NOW)
    store.initialize()
    remote = FakeLinear(issue())
    coordinator = ClaimCoordinator(
        store,
        remote,
        project_id="project",
        ready_state_id="ready",
        in_progress_state_id="progress",
        repository_key="repo",
        daemon_instance="d",
        owner_pid=1,
        owner_pid_started_at="start",
    )
    for candidate in (
        issue(blockers=("x",)),
        issue(project_id="other"),
        issue(state_id="progress"),
    ):
        with pytest.raises(ClaimError):
            coordinator.claim(candidate)


def test_reconcile_finishes_existing_run_after_state_change_crash(tmp_path: Path) -> None:
    store = Store(tmp_path / "db", clock=lambda: NOW, process_probe=lambda _pid: "start")
    store.initialize()
    remote = FakeLinear(issue())
    coordinator = ClaimCoordinator(
        store,
        remote,
        project_id="project",
        ready_state_id="ready",
        in_progress_state_id="progress",
        repository_key="repo",
        daemon_instance="d",
        owner_pid=1,
        owner_pid_started_at="start",
        clock=lambda: NOW,
    )
    original_add_comment = remote.add_comment
    remote.add_comment = lambda _issue, _body: (_ for _ in ()).throw(TimeoutError())

    with pytest.raises(ClaimError, match="mutation failed"):
        coordinator.claim(issue())
    persisted = store.get_run_for_issue("issue-1")
    assert persisted is not None
    assert persisted.machine_state == "CLAIM_ROLLBACK"
    assert remote.issue.state_id == "progress"

    remote.add_comment = original_add_comment
    recovered = coordinator.reconcile("issue-1")
    assert recovered.run_id == persisted.run_id
    assert len(remote.markers) == 1


def test_reconcile_rejects_duplicate_own_markers(tmp_path: Path) -> None:
    store = Store(tmp_path / "db", clock=lambda: NOW, process_probe=lambda _pid: "start")
    store.initialize()
    remote = FakeLinear(issue())
    coordinator = ClaimCoordinator(
        store,
        remote,
        project_id="project",
        ready_state_id="ready",
        in_progress_state_id="progress",
        repository_key="repo",
        daemon_instance="d",
        owner_pid=1,
        owner_pid_started_at="start",
        clock=lambda: NOW,
    )
    run = coordinator.claim(issue())
    lease = store.get_lease("issue-1")
    store.release_lease("issue-1", "d", 1, "start", lease.epoch)
    marker = remote.markers[0]
    remote.issue = remote.issue.with_comments((marker, marker))

    with pytest.raises(ClaimError, match="duplicate claim markers"):
        coordinator.reconcile("issue-1")
    persisted = store.get_run_for_issue("issue-1")
    assert persisted is not None
    assert persisted.run_id == run.run_id
    assert persisted.machine_state == "CLAIM_ROLLBACK"
