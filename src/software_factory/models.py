"""Typed records used by the local durable store."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]


class WorkerKind(StrEnum):
    IMPLEMENTER = "implementer"
    REVIEWER = "reviewer"


class MachineState(StrEnum):
    DISCOVERED = "DISCOVERED"
    CLAIMING = "CLAIMING"
    CONTEXT_BUILDING = "CONTEXT_BUILDING"
    IMPLEMENTING = "IMPLEMENTING"
    VERIFYING = "VERIFYING"
    PR_SYNC = "PR_SYNC"
    CI_WAIT = "CI_WAIT"
    REVIEWING = "REVIEWING"
    REPAIRING = "REPAIRING"
    MERGE_READY = "MERGE_READY"
    MERGED = "MERGED"
    CLEANUP = "CLEANUP"
    COMPLETE = "COMPLETE"
    NEEDS_INPUT = "NEEDS_INPUT"
    RECOVERING = "RECOVERING"
    CLAIM_ROLLBACK = "CLAIM_ROLLBACK"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ClaimRequest:
    linear_issue_id: str
    run_id: str
    repository_key: str
    branch: str
    worktree: str


@dataclass(frozen=True)
class Run:
    run_id: str
    linear_issue_id: str
    repository_key: str
    branch: str
    worktree: str
    pr_number: int | None
    machine_state: str
    cycle_count: int
    created_at: datetime
    updated_at: datetime
    last_head_sha: str | None = None
    implementer: str | None = None
    reviewer: str | None = None


@dataclass(frozen=True)
class Lease:
    linear_issue_id: str
    run_id: str
    owner_instance: str
    owner_pid: int
    owner_pid_started_at: str
    acquired_at: datetime
    heartbeat_at: datetime
    expires_at: datetime
    epoch: int


@dataclass(frozen=True)
class Worker:
    worker_id: str
    run_id: str
    kind: str
    tool: str
    pid: int
    pid_started_at: str
    started_at: datetime
    heartbeat_at: datetime
    artifact_dir: str
    finished_at: datetime | None = None
    exit_code: int | None = None


@dataclass(frozen=True)
class Attempt:
    attempt_id: str
    run_id: str
    cycle: int
    kind: str
    tool: str
    session_id: str | None
    exact_head_sha: str
    outcome: str | None
    started_at: datetime
    finished_at: datetime | None
    artifact_dir: str


@dataclass(frozen=True)
class Event:
    event_id: str
    run_id: str
    type: str
    payload: JsonValue
    idempotency_key: str
    created_at: datetime
    delivered_at: datetime | None = None
