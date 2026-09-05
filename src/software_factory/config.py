"""Strict, secret-safe configuration models."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit
from uuid import UUID

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode

# Verified through WIL-143 and the Software Factory operating contract on 2026-09-05.
# Dispatch authority remains the configured `Ready for Agent` identity, not a Linear type.
NEEDS_TRIAGE_ID = UUID("e0d094ca-9a01-4724-a9b5-4093b3dcc1a7")
READY_FOR_AGENT_ID = UUID("1d48ba48-29fb-4073-b3ad-ea8ab8f240b6")
IN_PROGRESS_ID = UUID("ba60d968-082b-4b92-bd30-2cc8072c54dc")
NEEDS_INPUT_ID = UUID("6210ccaf-3590-4869-8b58-2dde2abf831e")
DONE_ID = UUID("4747de0e-f178-415c-8e37-287f2869b8b1")

REQUIRED_STATES = {
    "Needs Triage": NEEDS_TRIAGE_ID,
    "Ready for Agent": READY_FOR_AGENT_ID,
    "In Progress": IN_PROGRESS_ID,
    "Needs Input": NEEDS_INPUT_ID,
    "Done": DONE_ID,
}
ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")
GRANT_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.:/-]{0,127}$")
SECRET_KEY = re.compile(
    r"(?:^|[_-])(?:password|passwd|secret|token|credential|private[_-]?key|access[_-]?key|api[_-]?key)(?:$|[_-])",
    re.IGNORECASE,
)
ALLOWED_REFERENCE_KEYS = {"api_key_env", "keyholder_grant"}
TOKEN_VALUE = re.compile(
    r"(?:github_pat_|gh[pousr]_|sk-|lin_api_|xox[baprs]-|"
    r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+|"
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----)",
    re.IGNORECASE,
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _absolute_path(value: object) -> Path:
    path = Path(value)  # type: ignore[arg-type]
    if not path.is_absolute():
        raise ValueError("path must be absolute")
    normalized = Path(os.path.abspath(os.fspath(path.expanduser())))
    current = Path(normalized.anchor)
    for component in normalized.parts[1:]:
        current /= component
        if current.is_symlink():
            raise ValueError("path must not contain symlink components")
    return normalized


class UnsafeConfigurationError(ValueError):
    """A safely printable configuration security failure."""


def _reject_secret_material(value: object, location: tuple[str, ...] = ()) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            name = str(key)
            child_location = (*location, name)
            if name not in ALLOWED_REFERENCE_KEYS and SECRET_KEY.search(name):
                raise UnsafeConfigurationError(
                    f"{'.'.join(child_location)}: secret-bearing key name is not allowed"
                )
            _reject_secret_material(item, child_location)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            _reject_secret_material(item, (*location, str(index)))
    elif isinstance(value, str) and TOKEN_VALUE.search(value):
        label = ".".join(location) or "configuration"
        raise UnsafeConfigurationError(f"{label}: token-shaped value is not allowed")


def _reject_duplicate_mapping_keys(node: Node | None, seen_nodes: set[int] | None = None) -> None:
    if node is None:
        return
    if seen_nodes is None:
        seen_nodes = set()
    node_identity = id(node)
    if node_identity in seen_nodes:
        raise UnsafeConfigurationError("YAML aliases or recursive structures are not allowed")
    seen_nodes.add(node_identity)

    if isinstance(node, MappingNode):
        seen_keys: set[str] = set()
        for key_node, value_node in node.value:
            if not isinstance(key_node, ScalarNode) or key_node.tag != "tag:yaml.org,2002:str":
                raise UnsafeConfigurationError("YAML mapping keys must be strings")
            if key_node.value in seen_keys:
                line = key_node.start_mark.line + 1
                column = key_node.start_mark.column + 1
                raise UnsafeConfigurationError(
                    f"duplicate mapping key at line {line}, column {column}"
                )
            seen_keys.add(key_node.value)
            _reject_duplicate_mapping_keys(key_node, seen_nodes)
            _reject_duplicate_mapping_keys(value_node, seen_nodes)
    elif isinstance(node, SequenceNode):
        for item in node.value:
            _reject_duplicate_mapping_keys(item, seen_nodes)


class DaemonConfig(StrictModel):
    capacity: Annotated[int, Field(ge=1)]
    state_dir: Path

    _normalize_state_dir = field_validator("state_dir")(_absolute_path)


class LinearConfig(StrictModel):
    api_key_env: str
    states: dict[str, UUID]

    @field_validator("api_key_env")
    @classmethod
    def validate_environment_name(cls, value: str) -> str:
        if not ENV_NAME.fullmatch(value):
            raise ValueError("api_key_env must be an environment-variable name")
        return value

    @field_validator("states")
    @classmethod
    def validate_states(cls, value: dict[str, UUID]) -> dict[str, UUID]:
        if value != REQUIRED_STATES or len(set(value.values())) != len(value):
            raise ValueError("state mappings must exactly match required unique UUIDs")
        return value


class RepositoryConfig(StrictModel):
    path: Path
    origin: str
    artifact_dir: Path
    worktree_dir: Path

    _normalize_paths = field_validator("path", "artifact_dir", "worktree_dir")(_absolute_path)

    @field_validator("origin")
    @classmethod
    def reject_credentials(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("repository origin must not contain embedded credentials")
        if parsed.scheme.lower() != "https" or parsed.hostname is None:
            raise ValueError("repository origin must be an absolute HTTPS URL")
        if parsed.query or parsed.fragment:
            raise ValueError("repository origin must not contain a query or fragment")
        return value


class RouteConfig(StrictModel):
    tool: Literal["codex", "claude"]
    model: str = Field(min_length=1)
    session_namespace: str = Field(min_length=1)

    @field_validator("model", "session_namespace")
    @classmethod
    def normalize_nonempty_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value must be nonempty")
        return value.strip()

    @field_validator("model")
    @classmethod
    def reject_extreme_models(cls, value: str) -> str:
        if re.search(r"(?:max|ultra)", value, re.IGNORECASE):
            raise ValueError("model names must not contain max or ultra")
        return value


class RoutingConfig(StrictModel):
    implementer: RouteConfig
    reviewer: RouteConfig
    repair_cycles: Annotated[int, Field(ge=0, le=5)]

    @model_validator(mode="after")
    def separate_roles(self) -> RoutingConfig:
        if self.implementer == self.reviewer:
            raise ValueError("implementer and reviewer routes must be different")
        if self.implementer.session_namespace == self.reviewer.session_namespace:
            raise ValueError("implementer and reviewer session namespaces must be different")
        return self


class VerificationConfig(StrictModel):
    executables: list[str] = Field(min_length=1)

    @field_validator("executables")
    @classmethod
    def executable_names_only(cls, value: list[str]) -> list[str]:
        if any(not item or Path(item).name != item for item in value):
            raise ValueError("executables must be bare command names")
        return value


class NotificationConfig(StrictModel):
    keyholder_grant: str

    @field_validator("keyholder_grant")
    @classmethod
    def grant_names_only(cls, value: str) -> str:
        if not GRANT_NAME.fullmatch(value):
            raise ValueError("keyholder_grant must be a grant name, not a credential")
        return value


class FactoryConfig(StrictModel):
    daemon: DaemonConfig
    linear: LinearConfig
    repositories: dict[str, RepositoryConfig] = Field(min_length=1)
    routing: RoutingConfig
    verification: VerificationConfig
    notification: NotificationConfig

    def is_dispatch_eligible(self, state_id: UUID | str) -> bool:
        """Return true only for the exact configured Ready for Agent UUID identity."""
        try:
            return UUID(str(state_id)) == self.linear.states["Ready for Agent"]
        except ValueError:
            return False


def load_config(path: Path) -> FactoryConfig:
    """Parse YAML and validate it without resolving any credential values."""
    try:
        source = path.read_text(encoding="utf-8")
        syntax_tree = yaml.compose(source, Loader=yaml.SafeLoader)
        _reject_duplicate_mapping_keys(syntax_tree)
        raw = yaml.safe_load(source)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        location = f" at line {mark.line + 1}, column {mark.column + 1}" if mark else ""
        raise ValueError(f"YAML parsing failed{location}") from None
    if not isinstance(raw, dict):
        raise ValueError("configuration root must be a mapping")
    _reject_secret_material(raw)
    return FactoryConfig.model_validate(raw)
