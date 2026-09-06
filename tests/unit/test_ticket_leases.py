import multiprocessing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from queue import Empty
from time import monotonic
from traceback import format_exc
from typing import Any

import pytest

from software_factory.db import LeaseConflict, LeaseOwnershipError, Store
from software_factory.models import ClaimRequest, MachineState, Worker


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


def _race_claim(path: str, run_id: str, gate: Any, output: Any) -> None:
    try:
        gate.wait()
        store = Store(Path(path), process_probe=lambda _pid: "start")
        run = store.claim_ticket(
            ClaimRequest("ISSUE-1", run_id, "repo", f"branch-{run_id}", f"/work/{run_id}"),
            owner_instance=run_id,
            owner_pid=123,
            owner_pid_started_at="start",
            ttl=timedelta(minutes=1),
        )
        output.put(("won", run.run_id))
    except LeaseConflict:
        output.put(("blocked", run_id))
    except BaseException as error:
        output.put(("error", run_id, type(error).__name__, str(error), format_exc()))


def _collect_process_results(processes: list[Any], output: Any) -> list[tuple[str, ...]]:
    deadline = monotonic() + 10
    results: list[tuple[str, ...]] = []
    try:
        for process in processes:
            process.join(timeout=max(0, deadline - monotonic()))

        hung = [process.name for process in processes if process.is_alive()]
        if hung:
            pytest.fail(f"claim child processes did not exit: {hung}")

        bad_exits = {
            process.name: process.exitcode for process in processes if process.exitcode != 0
        }
        if bad_exits:
            pytest.fail(f"claim child processes had nonzero exit codes: {bad_exits}")

        for _ in processes:
            try:
                results.append(output.get(timeout=max(0, deadline - monotonic())))
            except Empty:
                pytest.fail(f"claim children produced {len(results)} of {len(processes)} results")

        try:
            extra = output.get_nowait()
        except Empty:
            pass
        else:
            pytest.fail(f"claim child produced an unexpected extra result: {extra!r}")

        errors = [result for result in results if result[0] == "error"]
        if errors:
            details = "\n".join(
                f"child {run_id} raised {error_type}: {message}\n{traceback}"
                for _, run_id, error_type, message, traceback in errors
            )
            pytest.fail(details)
        return results
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
        for process in processes:
            process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
        output.close()
        output.join_thread()


def test_multiprocess_claim_has_exactly_one_owner(tmp_path: Path) -> None:
    path = tmp_path / "factory.db"
    Store(path).initialize()
    context = multiprocessing.get_context("spawn")
    gate = context.Event()
    output = context.Queue()
    processes = [
        context.Process(target=_race_claim, args=(str(path), str(n), gate, output))
        for n in range(2)
    ]
    for process in processes:
        process.start()
    gate.set()
    results = _collect_process_results(processes, output)
    assert sorted(result[0] for result in results) == ["blocked", "won"]


def test_owner_checked_heartbeat_release_and_resume(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "factory.db", clock=clock, process_probe=lambda _pid: "start")
    store.initialize()
    request = ClaimRequest("ISSUE-1", "run-1", "repo", "wil-132", "/work/wil-132")
    run = store.claim_ticket(request, "daemon", 10, "start", timedelta(seconds=30))
    assert run.machine_state == MachineState.CLAIMING

    with pytest.raises(LeaseOwnershipError):
        store.heartbeat_lease("ISSUE-1", "other", 10, "start", timedelta(seconds=30))
    clock.now += timedelta(seconds=5)
    lease = store.heartbeat_lease("ISSUE-1", "daemon", 10, "start", timedelta(seconds=30))
    assert lease.expires_at == clock.now + timedelta(seconds=30)
    with pytest.raises(LeaseOwnershipError):
        store.release_lease("ISSUE-1", "other", 10, "start")
    clock.now += timedelta(seconds=5)
    park_time = clock.now
    store.park_lease_for_needs_input("ISSUE-1", "daemon", 10, "start")

    with store.connect() as connection:
        parked = connection.execute(
            "SELECT machine_state, updated_at FROM runs WHERE run_id = ?", (run.run_id,)
        ).fetchone()
        assert parked["machine_state"] == MachineState.NEEDS_INPUT
        assert datetime.fromisoformat(parked["updated_at"]) == park_time
        assert datetime.fromisoformat(parked["updated_at"]) > run.updated_at
        assert connection.execute("SELECT count(*) FROM leases").fetchone()[0] == 0

    clock.now += timedelta(seconds=5)
    resumed = store.claim_ticket(
        ClaimRequest("ISSUE-1", "ignored", "repo", "ignored", "/ignored"),
        "daemon-2",
        11,
        "start",
        timedelta(seconds=30),
    )
    assert (resumed.run_id, resumed.branch, resumed.worktree) == (
        run.run_id,
        run.branch,
        run.worktree,
    )
    assert resumed.repository_key == run.repository_key
    assert resumed.created_at == run.created_at
    assert resumed.machine_state == MachineState.CLAIMING
    with store.connect() as connection:
        assert connection.execute("SELECT count(*) FROM runs").fetchone()[0] == 1
        lease = connection.execute("SELECT * FROM leases").fetchone()
        assert (lease["owner_instance"], lease["owner_pid"], lease["owner_pid_started_at"]) == (
            "daemon-2",
            11,
            "start",
        )


@pytest.mark.parametrize("ttl", [timedelta(0), timedelta(seconds=-1), timedelta(seconds=11)])
def test_claim_rejects_unbounded_ttl_without_creating_run_or_lease(
    tmp_path: Path, ttl: timedelta
) -> None:
    store = Store(tmp_path / "factory.db", max_lease_ttl=timedelta(seconds=10))
    store.initialize()

    with pytest.raises(ValueError, match="positive and bounded"):
        store.claim_ticket(
            ClaimRequest("ISSUE-1", "run-1", "repo", "branch", "/work"),
            "owner",
            10,
            "start",
            ttl,
        )

    with store.connect() as connection:
        assert connection.execute("SELECT count(*) FROM runs").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM leases").fetchone()[0] == 0


@pytest.mark.parametrize("ttl", [timedelta(0), timedelta(seconds=-1), timedelta(seconds=11)])
def test_heartbeat_rejects_unbounded_ttl_without_mutating_timestamps(
    tmp_path: Path, ttl: timedelta
) -> None:
    clock = Clock()
    store = Store(tmp_path / "factory.db", clock=clock, max_lease_ttl=timedelta(seconds=10))
    store.initialize()
    store.claim_ticket(
        ClaimRequest("ISSUE-1", "run-1", "repo", "branch", "/work"),
        "owner",
        10,
        "start",
        timedelta(seconds=10),
    )
    with store.connect() as connection:
        before = tuple(connection.execute("SELECT heartbeat_at, expires_at FROM leases").fetchone())
    clock.now += timedelta(seconds=1)

    with pytest.raises(ValueError, match="positive and bounded"):
        store.heartbeat_lease("ISSUE-1", "owner", 10, "start", ttl)

    with store.connect() as connection:
        after = tuple(connection.execute("SELECT heartbeat_at, expires_at FROM leases").fetchone())
    assert after == before


@pytest.mark.parametrize(
    ("mutation", "owner_pid", "owner_pid_started_at"),
    [
        ("heartbeat", 11, "start"),
        ("heartbeat", 10, "restarted"),
        ("release", 11, "start"),
        ("release", 10, "restarted"),
        ("park", 11, "start"),
        ("park", 10, "restarted"),
    ],
)
def test_lease_mutations_require_complete_process_identity(
    tmp_path: Path, mutation: str, owner_pid: int, owner_pid_started_at: str
) -> None:
    store = Store(tmp_path / "factory.db")
    store.initialize()
    store.claim_ticket(
        ClaimRequest("ISSUE-1", "run-1", "repo", "branch", "/work"),
        "daemon",
        10,
        "start",
    )

    with pytest.raises(LeaseOwnershipError):
        if mutation == "heartbeat":
            store.heartbeat_lease(
                "ISSUE-1", "daemon", owner_pid, owner_pid_started_at, timedelta(seconds=30)
            )
        elif mutation == "release":
            store.release_lease("ISSUE-1", "daemon", owner_pid, owner_pid_started_at)
        else:
            store.park_lease_for_needs_input("ISSUE-1", "daemon", owner_pid, owner_pid_started_at)
    with store.connect() as connection:
        assert connection.execute("SELECT count(*) FROM leases").fetchone()[0] == 1
        assert (
            connection.execute("SELECT machine_state FROM runs").fetchone()[0]
            == MachineState.CLAIMING
        )


@pytest.mark.parametrize("terminal_state", [MachineState.COMPLETE, MachineState.FAILED])
def test_terminal_run_cannot_be_reacquired(tmp_path: Path, terminal_state: MachineState) -> None:
    store = Store(tmp_path / "factory.db")
    store.initialize()
    request = ClaimRequest("ISSUE-1", "run-1", "repo", "branch", "/work")
    store.claim_ticket(request, "daemon", 10, "start")
    store.release_lease("ISSUE-1", "daemon", 10, "start")
    with store.connect() as connection:
        connection.execute(
            "UPDATE runs SET machine_state = ? WHERE run_id = ?", (terminal_state, "run-1")
        )

    with pytest.raises(LeaseConflict, match="terminal"):
        store.claim_ticket(request, "new", 20, "new-start")


def test_expiry_pid_reuse_live_worker_and_capacity(tmp_path: Path) -> None:
    clock = Clock()
    starts = {10: "reused", 20: "worker-start", 30: "other-start"}
    store = Store(tmp_path / "factory.db", clock=clock, process_probe=starts.get)
    store.initialize()
    request = ClaimRequest("ISSUE-1", "run-1", "repo", "branch", "/work")
    store.claim_ticket(request, "owner", 10, "original", timedelta(seconds=1))
    store.start_worker(
        Worker(
            "w1", "run-1", "implementer", "codex", 20, "worker-start", clock.now, clock.now, "/a"
        )
    )
    store.start_worker(
        Worker("w2", "run-1", "reviewer", "codex", 30, "wrong-start", clock.now, clock.now, "/b")
    )
    clock.now += timedelta(seconds=2)

    assert store.active_worker_count() == 1
    with pytest.raises(LeaseConflict, match="live worker"):
        store.claim_ticket(request, "new", 40, "new-start", timedelta(seconds=5))
    store.finish_worker("w1", 0)
    assert store.active_worker_count() == 0
    resumed = store.claim_ticket(request, "new", 40, "new-start", timedelta(seconds=5))
    assert resumed.run_id == "run-1"


@pytest.mark.parametrize("mutation", ["release", "park"])
def test_lease_removal_refuses_live_worker_and_preserves_state(
    tmp_path: Path, mutation: str
) -> None:
    clock = Clock()
    store = Store(tmp_path / "factory.db", clock=clock, process_probe=lambda _pid: "worker-start")
    store.initialize()
    store.claim_ticket(
        ClaimRequest("ISSUE-1", "run-1", "repo", "branch", "/work"),
        "owner",
        10,
        "owner-start",
    )
    store.start_worker(
        Worker(
            "live-worker",
            "run-1",
            "implementer",
            "codex",
            20,
            "worker-start",
            clock.now,
            clock.now,
            "/artifacts",
        )
    )

    with pytest.raises(LeaseConflict, match="live worker"):
        if mutation == "release":
            store.release_lease("ISSUE-1", "owner", 10, "owner-start")
        else:
            store.park_lease_for_needs_input("ISSUE-1", "owner", 10, "owner-start")

    with store.connect() as connection:
        assert connection.execute("SELECT count(*) FROM leases").fetchone()[0] == 1
        assert (
            connection.execute("SELECT machine_state FROM runs WHERE run_id = 'run-1'").fetchone()[
                0
            ]
            == MachineState.CLAIMING
        )


def test_existing_run_without_lease_refuses_live_worker_then_resumes(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "factory.db", clock=clock, process_probe=lambda _pid: "worker-start")
    store.initialize()
    request = ClaimRequest("ISSUE-1", "run-1", "repo", "branch", "/work")
    store.claim_ticket(request, "owner", 10, "owner-start")
    store.start_worker(
        Worker(
            "live-worker",
            "run-1",
            "implementer",
            "codex",
            20,
            "worker-start",
            clock.now,
            clock.now,
            "/artifacts",
        )
    )
    with store.connect() as connection:
        connection.execute("DELETE FROM leases WHERE run_id = 'run-1'")

    with pytest.raises(LeaseConflict, match="live worker"):
        store.claim_ticket(request, "new-owner", 30, "new-owner-start")
    with store.connect() as connection:
        assert connection.execute("SELECT count(*) FROM leases").fetchone()[0] == 0
        assert (
            connection.execute("SELECT machine_state FROM runs WHERE run_id = 'run-1'").fetchone()[
                0
            ]
            == MachineState.CLAIMING
        )

    store.finish_worker("live-worker", 0)
    resumed = store.claim_ticket(request, "new-owner", 30, "new-owner-start")
    assert resumed.run_id == "run-1"


def test_capacity_counts_only_live_unfinished_workers_across_all_lease_owners(
    tmp_path: Path,
) -> None:
    clock = Clock()
    starts = {
        20: "worker-one-start",
        30: "worker-two-start",
        40: "reused-start",
        50: "finished-start",
        60: "unowned-process-start",
    }
    store = Store(tmp_path / "factory.db", clock=clock, process_probe=starts.get)
    store.initialize()
    for number, owner in ((1, "old-controller"), (2, "current-controller")):
        store.claim_ticket(
            ClaimRequest(f"ISSUE-{number}", f"run-{number}", "repo", f"branch-{number}", "/work"),
            owner,
            10 + number,
            f"controller-{number}-start",
        )

    store.start_worker(
        Worker(
            "live-one",
            "run-1",
            "implementer",
            "codex",
            20,
            "worker-one-start",
            clock.now,
            clock.now,
            "/one",
        )
    )
    store.start_worker(
        Worker(
            "live-two",
            "run-2",
            "reviewer",
            "claude",
            30,
            "worker-two-start",
            clock.now,
            clock.now,
            "/two",
        )
    )
    store.start_worker(
        Worker(
            "pid-reused",
            "run-2",
            "implementer",
            "codex",
            40,
            "original-start",
            clock.now,
            clock.now,
            "/mismatch",
        )
    )
    store.start_worker(
        Worker(
            "finished",
            "run-2",
            "reviewer",
            "claude",
            50,
            "finished-start",
            clock.now,
            clock.now,
            "/finished",
        )
    )
    store.finish_worker("finished", 0)

    assert store.active_worker_count() == 2

    # Model an abnormal-recovery no-lease run directly: public release must not
    # orphan a live worker, while capacity remains intentionally lease-independent.
    with store.connect() as connection:
        connection.execute("DELETE FROM leases WHERE run_id = 'run-1'")

    assert store.active_worker_count() == 2


def test_expired_owner_cannot_release_stale_lease(tmp_path: Path) -> None:
    clock = Clock()
    store = Store(tmp_path / "factory.db", clock=clock)
    store.initialize()
    store.claim_ticket(
        ClaimRequest("ISSUE-1", "run-1", "repo", "branch", "/work"),
        "owner",
        10,
        "start",
        timedelta(seconds=1),
    )
    clock.now += timedelta(seconds=1)

    with pytest.raises(LeaseOwnershipError, match="stale"):
        store.release_lease("ISSUE-1", "owner", 10, "start")


@pytest.mark.parametrize("mutation", ["heartbeat", "park"])
def test_expired_lease_mutation_is_refused_without_state_change(
    tmp_path: Path, mutation: str
) -> None:
    clock = Clock()
    store = Store(tmp_path / "factory.db", clock=clock)
    store.initialize()
    store.claim_ticket(
        ClaimRequest("ISSUE-1", "run-1", "repo", "branch", "/work"),
        "owner",
        10,
        "start",
        timedelta(seconds=1),
    )
    with store.connect() as connection:
        lease_before = tuple(connection.execute("SELECT * FROM leases").fetchone())
        run_before = tuple(connection.execute("SELECT * FROM runs").fetchone())
    clock.now += timedelta(seconds=1)

    with pytest.raises(LeaseOwnershipError, match="stale"):
        if mutation == "heartbeat":
            store.heartbeat_lease("ISSUE-1", "owner", 10, "start", timedelta(seconds=30))
        else:
            store.park_lease_for_needs_input("ISSUE-1", "owner", 10, "start")

    with store.connect() as connection:
        assert tuple(connection.execute("SELECT * FROM leases").fetchone()) == lease_before
        assert tuple(connection.execute("SELECT * FROM runs").fetchone()) == run_before
