"""Narrow Linear GraphQL adapter and fenced claim coordination."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast
from uuid import uuid4

from software_factory.db import LeaseConflict, Store
from software_factory.models import ClaimRequest, Run

JsonObject = dict[str, object]


class ClaimError(RuntimeError):
    """Linear data or a claim transition was unsafe or unsuccessful."""


@dataclass(frozen=True)
class RetryMetadata:
    retry_after: float
    attempts: int


class RateLimited(ClaimError):
    def __init__(self, metadata: RetryMetadata) -> None:
        super().__init__("Linear rate limit retry budget exhausted")
        self.metadata = metadata


@dataclass(frozen=True)
class LinearIssue:
    id: str
    project_id: str
    state_id: str
    priority: int
    ready_at: datetime
    comments: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    comments_complete: bool = True
    relations_complete: bool = True

    def with_state(self, state_id: str) -> LinearIssue:
        return replace(self, state_id=state_id)

    def with_comments(self, comments: tuple[str, ...]) -> LinearIssue:
        return replace(self, comments=comments)


class LinearClient(Protocol):
    def get_issue(self, issue_id: str, ready_state_id: str) -> LinearIssue: ...
    def move_to_state(self, issue_id: str, state_id: str) -> None: ...
    def add_comment(self, issue_id: str, body: str) -> None: ...


DISCOVER_QUERY = """query Discover($projectId: ID!, $stateId: ID!, $first: Int!) {
  issues(filter: {project: {id: {eq: $projectId}}, state: {id: {eq: $stateId}}}, first: $first) {
    nodes { id priority project { id } state { id }
      history(first: 50) { nodes { createdAt toStateId } pageInfo { hasNextPage } }
    } pageInfo { hasNextPage }
  }
}"""

ISSUE_QUERY = """query Issue($id: String!, $commentFirst: Int!, $relationFirst: Int!) {
  issue(id: $id) { id priority project { id } state { id }
    history(first: 50) { nodes { createdAt toStateId } pageInfo { hasNextPage } }
    comments(first: $commentFirst) { nodes { body } pageInfo { hasNextPage } }
    inverseRelations(first: $relationFirst) { nodes { type issue { id state { type } } }
      pageInfo { hasNextPage } }
  }
}"""


class LinearAdapter:
    """Strict adapter over an injected authenticated GraphQL transport."""

    def __init__(
        self,
        transport: Callable[[str, JsonObject], object],
        *,
        max_retries: int = 2,
        sleeper: Callable[[float], None] = time.sleep,
        max_backoff: float = 30.0,
        comment_limit: int = 50,
        relation_limit: int = 50,
    ) -> None:
        if max_retries < 0 or max_backoff <= 0 or comment_limit <= 0 or relation_limit <= 0:
            raise ValueError("retry and page bounds must be positive (retries may be zero)")
        self._transport = transport
        self._max_retries = max_retries
        self._sleeper = sleeper
        self._max_backoff = max_backoff
        self._comment_limit = comment_limit
        self._relation_limit = relation_limit

    def _execute(self, query: str, variables: JsonObject) -> Mapping[str, object]:
        for attempt in range(self._max_retries + 1):
            raw = self._transport(query, variables)
            if not isinstance(raw, Mapping):
                raise ClaimError("Linear response must be an object")
            errors = raw.get("errors")
            if errors:
                retry = self._retry_after(errors)
                if retry is not None:
                    delay = min(retry, self._max_backoff)
                    if attempt < self._max_retries:
                        self._sleeper(delay)
                        continue
                    raise RateLimited(RetryMetadata(delay, attempt + 1))
                raise ClaimError("Linear returned GraphQL errors")
            data = raw.get("data")
            if not isinstance(data, Mapping):
                raise ClaimError("Linear response has no object data payload")
            return cast(Mapping[str, object], data)
        raise AssertionError("unreachable")

    @staticmethod
    def _retry_after(errors: object) -> float | None:
        if not isinstance(errors, Sequence) or isinstance(errors, (str, bytes)):
            raise ClaimError("Linear errors payload is malformed")
        for error in errors:
            if not isinstance(error, Mapping):
                raise ClaimError("Linear errors payload is malformed")
            extensions = error.get("extensions")
            if isinstance(extensions, Mapping) and extensions.get("code") in {
                "RATELIMITED",
                "RATE_LIMITED",
            }:
                value = extensions.get("retryAfter", 1)
                if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
                    raise ClaimError("Linear retry metadata is malformed")
                return float(value)
        return None

    def discover(self, project_id: str, ready_state_id: str, *, limit: int) -> list[LinearIssue]:
        if limit <= 0:
            raise ValueError("discovery limit must be positive")
        data = self._execute(
            DISCOVER_QUERY, {"projectId": project_id, "stateId": ready_state_id, "first": limit}
        )
        connection = self._connection(data, "issues")
        nodes = cast(list[object], connection["nodes"])
        parsed = [self._parse_discovery(node, project_id, ready_state_id) for node in nodes[:limit]]
        return self.order(parsed)

    @staticmethod
    def order(issues: Iterable[LinearIssue]) -> list[LinearIssue]:
        return sorted(
            issues,
            key=lambda issue: (issue.priority or 5, issue.ready_at, issue.id),
        )

    @staticmethod
    def _connection(parent: Mapping[str, object], key: str) -> Mapping[str, object]:
        value = parent.get(key)
        if not isinstance(value, Mapping) or set(value) != {"nodes", "pageInfo"}:
            raise ClaimError(f"Linear {key} connection has an unexpected shape")
        nodes, page = value["nodes"], value["pageInfo"]
        if (
            not isinstance(nodes, list)
            or not isinstance(page, Mapping)
            or set(page) != {"hasNextPage"}
        ):
            raise ClaimError(f"Linear {key} connection has an unexpected shape")
        if not isinstance(page["hasNextPage"], bool):
            raise ClaimError(f"Linear {key} pageInfo is malformed")
        return cast(Mapping[str, object], value)

    @staticmethod
    def _parse_discovery(raw: object, project: str, state: str) -> LinearIssue:
        if not isinstance(raw, Mapping) or set(raw) != {
            "id",
            "priority",
            "project",
            "state",
            "history",
        }:
            raise ClaimError("Linear issue has an unexpected shape")
        project_obj, state_obj, history = raw["project"], raw["state"], raw["history"]
        if (
            project_obj != {"id": project}
            or state_obj != {"id": state}
            or not isinstance(history, Mapping)
        ):
            raise ClaimError("Linear issue escaped the configured server-side filter")
        ready_at = LinearAdapter._ready_at_from_history(history, state)
        identifier, priority = raw["id"], raw["priority"]
        if (
            not isinstance(identifier, str)
            or not isinstance(priority, int)
            or isinstance(priority, bool)
        ):
            raise ClaimError("Linear issue scalar is malformed")
        return LinearIssue(identifier, project, state, priority, ready_at)

    @staticmethod
    def _ready_at_from_history(history: Mapping[str, object], ready_state_id: str) -> datetime:
        connection = LinearAdapter._connection({"history": history}, "history")
        page = cast(Mapping[str, object], connection["pageInfo"])
        if cast(bool, page["hasNextPage"]):
            raise ClaimError("Linear state history is truncated")
        ready_times: list[datetime] = []
        for node in cast(list[object], connection["nodes"]):
            if not isinstance(node, Mapping) or set(node) != {"createdAt", "toStateId"}:
                raise ClaimError("Linear state history entry is malformed")
            created, to_state_id = node["createdAt"], node["toStateId"]
            if not isinstance(created, str) or (
                to_state_id is not None and not isinstance(to_state_id, str)
            ):
                raise ClaimError("Linear state history entry is malformed")
            if to_state_id != ready_state_id:
                continue
            try:
                value = datetime.fromisoformat(created.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ClaimError("Ready-entry timestamp is malformed") from exc
            if value.tzinfo is None:
                raise ClaimError("Ready-entry timestamp lacks timezone")
            ready_times.append(value)
        if not ready_times:
            raise ClaimError("Ready-entry history is missing or ambiguous")
        return max(ready_times)

    def get_issue(self, issue_id: str, ready_state_id: str) -> LinearIssue:
        data = self._execute(
            ISSUE_QUERY,
            {
                "id": issue_id,
                "commentFirst": self._comment_limit,
                "relationFirst": self._relation_limit,
            },
        )
        raw = data.get("issue")
        if not isinstance(raw, Mapping) or raw.get("id") != issue_id:
            raise ClaimError("Linear issue snapshot is missing or mismatched")
        comments = self._connection(raw, "comments")
        relations = self._connection(raw, "inverseRelations")
        history = self._connection(raw, "history")
        project, state = raw.get("project"), raw.get("state")
        if (
            not isinstance(project, Mapping)
            or not isinstance(project.get("id"), str)
            or not isinstance(state, Mapping)
            or not isinstance(state.get("id"), str)
        ):
            raise ClaimError("Linear issue ownership is malformed")
        comment_bodies: list[str] = []
        for node in cast(list[object], comments["nodes"]):
            if (
                not isinstance(node, Mapping)
                or set(node) != {"body"}
                or not isinstance(node["body"], str)
            ):
                raise ClaimError("Linear comment is malformed")
            comment_bodies.append(node["body"])
        blockers: list[str] = []
        for node in cast(list[object], relations["nodes"]):
            if not isinstance(node, Mapping):
                raise ClaimError("Linear relation is malformed")
            if node.get("type") == "blocks":
                related = node.get("issue")
                if not isinstance(related, Mapping) or not isinstance(related.get("id"), str):
                    raise ClaimError("Linear blocker is malformed")
                related_state = related.get("state")
                if (
                    not isinstance(related_state, Mapping)
                    or related_state.get("type") != "completed"
                ):
                    blockers.append(cast(str, related["id"]))
        ready_at = self._ready_at_from_history(history, ready_state_id)
        priority = raw.get("priority")
        if not isinstance(priority, int) or isinstance(priority, bool):
            raise ClaimError("Linear priority is malformed")
        comment_page = cast(Mapping[str, object], comments["pageInfo"])
        relation_page = cast(Mapping[str, object], relations["pageInfo"])
        return LinearIssue(
            issue_id,
            cast(str, project["id"]),
            cast(str, state["id"]),
            priority,
            ready_at,
            tuple(comment_bodies),
            tuple(blockers),
            not cast(bool, comment_page["hasNextPage"]),
            not cast(bool, relation_page["hasNextPage"]),
        )

    def _mutation(self, query: str, variables: JsonObject, field: str) -> None:
        data = self._execute(query, variables)
        payload = data.get(field)
        if (
            not isinstance(payload, Mapping)
            or set(payload) != {"success"}
            or payload["success"] is not True
        ):
            raise ClaimError(f"Linear {field} mutation did not report success true")

    def move_to_state(self, issue_id: str, state_id: str) -> None:
        self._mutation(
            "mutation Update($id:String!,$stateId:String!){"
            "issueUpdate(id:$id,input:{stateId:$stateId}){success}}",
            {"id": issue_id, "stateId": state_id},
            "issueUpdate",
        )

    def add_comment(self, issue_id: str, body: str) -> None:
        self._mutation(
            "mutation Comment($id:String!,$body:String!){"
            "commentCreate(input:{issueId:$id,body:$body}){success}}",
            {"id": issue_id, "body": body},
            "commentCreate",
        )


MARKER_PREFIX = "<!-- software-factory-claim "


class ClaimCoordinator:
    def __init__(
        self,
        store: Store,
        linear: LinearClient,
        *,
        project_id: str,
        ready_state_id: str,
        in_progress_state_id: str,
        repository_key: str,
        daemon_instance: str,
        owner_pid: int,
        owner_pid_started_at: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        lease_ttl: timedelta = timedelta(minutes=1),
    ) -> None:
        if (
            not all(
                value.strip()
                for value in (
                    project_id,
                    ready_state_id,
                    in_progress_state_id,
                    repository_key,
                    daemon_instance,
                    owner_pid_started_at,
                )
            )
            or owner_pid <= 0
        ):
            raise ValueError("claim configuration values must be nonempty and PID positive")
        self.store, self.linear, self.project_id = store, linear, project_id
        self.ready_state_id, self.in_progress_state_id = ready_state_id, in_progress_state_id
        self.repository_key, self.daemon_instance = repository_key, daemon_instance
        self.owner_pid, self.owner_pid_started_at = owner_pid, owner_pid_started_at
        self.clock, self.lease_ttl = clock, lease_ttl

    @staticmethod
    def _marker(run_id: str, daemon: str, claimed_at: datetime) -> str:
        payload = {
            "claimed_at": claimed_at.astimezone(UTC).isoformat(),
            "daemon_instance": daemon,
            "roles": ["implementer", "reviewer"],
            "run_id": run_id,
        }
        return MARKER_PREFIX + json.dumps(payload, sort_keys=True, separators=(",", ":")) + " -->"

    @staticmethod
    def _marker_counts(comments: Iterable[str]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for body in comments:
            if not body.startswith(MARKER_PREFIX) or not body.endswith(" -->"):
                continue
            try:
                value = json.loads(body[len(MARKER_PREFIX) : -4])
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict) and isinstance(value.get("run_id"), str):
                run_id = cast(str, value["run_id"])
                counts[run_id] = counts.get(run_id, 0) + 1
        return counts

    @classmethod
    def _marker_run_ids(cls, comments: Iterable[str]) -> set[str]:
        return set(cls._marker_counts(comments))

    @classmethod
    def _marked(cls, comments: Iterable[str], run_id: str) -> bool:
        return run_id in cls._marker_run_ids(comments)

    def claim(self, candidate: LinearIssue) -> Run:
        if candidate.project_id != self.project_id:
            raise ClaimError("issue does not match configured project-to-repository mapping")
        if (
            candidate.blockers
            or not candidate.comments_complete
            or not candidate.relations_complete
        ):
            raise ClaimError("issue has blockers or truncated ownership data")
        persisted = self.store.get_run_for_issue(candidate.id)
        if candidate.state_id == self.in_progress_state_id and persisted is None:
            raise ClaimError("unmarked or unknown In Progress work is human-owned")
        request = ClaimRequest(
            candidate.id,
            uuid4().hex,
            self.repository_key,
            f"agent/{candidate.id}",
            f"worktrees/{candidate.id}",
        )
        try:
            run = self.store.claim_ticket(
                request,
                self.daemon_instance,
                self.owner_pid,
                self.owner_pid_started_at,
                self.lease_ttl,
            )
        except LeaseConflict:
            raise
        lease = self.store.get_lease(candidate.id)
        try:
            fresh = self.linear.get_issue(candidate.id, self.ready_state_id)
            if (
                fresh.project_id != self.project_id
                or not fresh.comments_complete
                or not fresh.relations_complete
            ):
                raise ClaimError("issue ownership or dependency data is truncated or mismatched")
            if fresh.blockers:
                raise ClaimError("issue has unresolved blockers")
            marker_counts = self._marker_counts(fresh.comments)
            marker_run_ids = set(marker_counts)
            if marker_run_ids - {run.run_id}:
                raise ClaimError("issue contains a claim marker owned by another run")
            if marker_counts.get(run.run_id, 0) > 1:
                raise ClaimError("issue contains duplicate claim markers for this run")
            marked = run.run_id in marker_run_ids
            if fresh.state_id == self.in_progress_state_id and not marked and persisted is None:
                raise ClaimError("unmarked In Progress work is human-owned")
            if fresh.state_id not in (self.ready_state_id, self.in_progress_state_id):
                raise ClaimError("fresh issue state is not claimable")
            if fresh.state_id == self.ready_state_id:
                self.linear.move_to_state(fresh.id, self.in_progress_state_id)
            if not marked:
                self.linear.add_comment(
                    fresh.id, self._marker(run.run_id, self.daemon_instance, self.clock())
                )
            verified = self.linear.get_issue(fresh.id, self.ready_state_id)
            if verified.state_id != self.in_progress_state_id or not self._marked(
                verified.comments, run.run_id
            ):
                raise ClaimError("post-mutation readback conflicted")
            if self._marker_counts(verified.comments).get(run.run_id, 0) != 1:
                raise ClaimError("post-mutation readback found duplicate claim markers")
            return run
        except Exception as exc:
            failure = exc if isinstance(exc, ClaimError) else ClaimError(
                "Linear claim mutation failed"
            )
            try:
                self.store.rollback_claim(lease)
            except Exception as rollback_exc:
                failure.add_note(
                    "The fenced local rollback also failed; startup reconciliation is required."
                )
                failure.add_note(f"Rollback error type: {type(rollback_exc).__name__}")
            if failure is exc:
                raise
            raise failure from exc

    def reconcile(self, issue_id: str) -> Run:
        """Resume one persisted claim without adopting unknown Linear work."""
        persisted = self.store.get_run_for_issue(issue_id)
        if persisted is None:
            raise ClaimError("cannot reconcile an issue without a persisted local run")
        snapshot = self.linear.get_issue(issue_id, self.ready_state_id)
        return self.claim(snapshot)
