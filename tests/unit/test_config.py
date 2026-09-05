from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from software_factory.config import READY_FOR_AGENT_ID, FactoryConfig, load_config


def valid_config(tmp_path: Path) -> dict[str, object]:
    repo = tmp_path / "repo"
    repo.mkdir()
    return {
        "daemon": {"capacity": 1, "state_dir": str(tmp_path / "state")},
        "linear": {
            "api_key_env": "LINEAR_API_KEY",  # pragma: allowlist secret
            "states": {
                "Needs Triage": "e0d094ca-9a01-4724-a9b5-4093b3dcc1a7",
                "Ready for Agent": str(READY_FOR_AGENT_ID),
                "In Progress": "ba60d968-082b-4b92-bd30-2cc8072c54dc",
                "Needs Input": "6210ccaf-3590-4869-8b58-2dde2abf831e",
                "Done": "4747de0e-f178-415c-8e37-287f2869b8b1",
            },
        },
        "repositories": {
            "widgets": {
                "path": str(repo),
                "origin": "https://github.com/acme/widgets.git",
                "artifact_dir": str(tmp_path / "artifacts"),
                "worktree_dir": str(tmp_path / "worktrees"),
            }
        },
        "routing": {
            "implementer": {
                "tool": "codex",
                "model": "codex-medium",
                "session_namespace": "wil-implementer",
            },
            "reviewer": {
                "tool": "claude",
                "model": "claude-sonnet",
                "session_namespace": "wil-reviewer",
            },
            "repair_cycles": 2,
        },
        "verification": {"executables": ["git"]},
        "notification": {"keyholder_grant": "factory-notifications"},
    }


def test_load_config_normalizes_absolute_paths(tmp_path: Path) -> None:
    data = valid_config(tmp_path)
    config_file = tmp_path / "factory.yaml"
    config_file.write_text(__import__("yaml").safe_dump(data), encoding="utf-8")

    config = load_config(config_file)

    assert config.daemon.state_dir.is_absolute()
    assert config.repositories["widgets"].path == (tmp_path / "repo").resolve()
    assert config.repositories["widgets"].artifact_dir.is_absolute()
    assert config.repositories["widgets"].worktree_dir.is_absolute()
    assert config.is_dispatch_eligible(READY_FOR_AGENT_ID)
    assert not config.is_dispatch_eligible("Ready for Agent")


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda d: d["daemon"].update(capacity=0), "capacity"),
        (lambda d: d["routing"].update(repair_cycles=6), "repair_cycles"),
        (
            lambda d: d["routing"]["reviewer"].update(session_namespace="wil-implementer"),
            "session namespaces",
        ),
        (lambda d: d["routing"]["implementer"].update(model="gpt-max"), "max or ultra"),
        (lambda d: d["linear"].update(api_key_env="secret-value"), "environment-variable"),
        (
            lambda d: d["repositories"]["widgets"].update(origin="https://u:p@example.com/x"),
            "credentials",
        ),
        (
            lambda d: d["repositories"]["widgets"].update(
                origin="http://github.com/acme/widgets.git"
            ),
            "HTTPS",
        ),
        (
            lambda d: d["repositories"]["widgets"].update(
                origin="https://github.com/acme/widgets.git?ref=unsafe"
            ),
            "query or fragment",
        ),
        (lambda d: d["daemon"].update(unknown=True), "Extra inputs"),
    ],
)
def test_rejects_invalid_or_secret_bearing_config(tmp_path: Path, mutate, message: str) -> None:
    data = valid_config(tmp_path)
    mutate(data)
    with pytest.raises(ValidationError, match=message):
        FactoryConfig.model_validate(data)


def test_state_mapping_is_exact_and_unique(tmp_path: Path) -> None:
    data = valid_config(tmp_path)
    data["linear"]["states"]["Ready for Agent"] = data["linear"]["states"]["Done"]
    with pytest.raises(ValidationError, match="state mappings"):
        FactoryConfig.model_validate(data)


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("daemon", "state_dir"),
        ("repository", "path"),
        ("repository", "artifact_dir"),
        ("repository", "worktree_dir"),
    ],
)
def test_rejects_relative_paths(tmp_path: Path, section: str, field: str) -> None:
    data = valid_config(tmp_path)
    target = data["daemon"] if section == "daemon" else data["repositories"]["widgets"]
    target[field] = "relative/path"

    with pytest.raises(ValidationError, match="absolute"):
        FactoryConfig.model_validate(data)


@pytest.mark.parametrize("tool", ["shell", "openai", ""])
def test_rejects_unknown_or_empty_route_tool(tmp_path: Path, tool: str) -> None:
    data = valid_config(tmp_path)
    data["routing"]["implementer"]["tool"] = tool
    with pytest.raises(ValidationError):
        FactoryConfig.model_validate(data)


def test_rejects_empty_route_namespace(tmp_path: Path) -> None:
    data = valid_config(tmp_path)
    data["routing"]["reviewer"]["session_namespace"] = "  "
    with pytest.raises(ValidationError, match="session_namespace"):
        FactoryConfig.model_validate(data)


def test_loaded_yaml_rejects_secret_key_without_echo(tmp_path: Path) -> None:
    data = valid_config(tmp_path)
    sentinel = "github_pat_DO_NOT_ECHO_1234567890"
    data["mystery_token"] = sentinel
    path = tmp_path / "secret.yaml"
    path.write_text(__import__("yaml").safe_dump(data), encoding="utf-8")

    with pytest.raises(ValueError) as caught:
        load_config(path)

    assert sentinel not in str(caught.value)
    assert "mystery_token" in str(caught.value)


def test_token_shaped_keyholder_grant_is_rejected_without_echo(tmp_path: Path) -> None:
    data = valid_config(tmp_path)
    sentinel = "xoxb-123456789012-DO_NOT_ECHO"
    data["notification"]["keyholder_grant"] = sentinel
    path = tmp_path / "secret.yaml"
    path.write_text(__import__("yaml").safe_dump(data), encoding="utf-8")

    with pytest.raises(ValueError) as caught:
        load_config(path)

    assert sentinel not in str(caught.value)
    assert "keyholder_grant" in str(caught.value)


def test_route_namespaces_are_normalized_before_independence_check(tmp_path: Path) -> None:
    data = valid_config(tmp_path)
    data["routing"]["implementer"]["session_namespace"] = " shared-session "
    data["routing"]["reviewer"]["session_namespace"] = "shared-session"

    with pytest.raises(ValidationError, match="session namespaces"):
        FactoryConfig.model_validate(data)


def test_example_configuration_targets_pacenotes_pilot() -> None:
    config = load_config(Path(__file__).parents[2] / "config.example.yaml")

    assert set(config.repositories) == {"pacenotes"}
    assert config.repositories["pacenotes"].origin == (
        "https://github.com/chardy-b/pacenotes-android.git"
    )
    assert config.routing.implementer.tool == "codex"
    assert config.routing.reviewer.tool == "claude"


def test_duplicate_yaml_keys_are_rejected_without_echoing_values(tmp_path: Path) -> None:
    source = (Path(__file__).parents[2] / "config.example.yaml").read_text(encoding="utf-8")
    sentinel = "DO_NOT_ECHO_DUPLICATE_VALUE"
    path = tmp_path / "duplicate.yaml"
    path.write_text(
        f"daemon:\n  capacity: 0\n  state_dir: /tmp/{sentinel}\n{source}",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate mapping key") as caught:
        load_config(path)

    assert sentinel not in str(caught.value)


@pytest.mark.parametrize("yaml_key", ["1", "01", "true", "True"])
def test_non_string_yaml_keys_are_rejected_before_key_collapse(
    tmp_path: Path, yaml_key: str
) -> None:
    source = (Path(__file__).parents[2] / "config.example.yaml").read_text(encoding="utf-8")
    path = tmp_path / "non-string-key.yaml"
    path.write_text(f"{yaml_key}: ignored\n{source}", encoding="utf-8")

    with pytest.raises(ValueError, match="mapping keys must be strings"):
        load_config(path)


def test_recursive_yaml_alias_is_rejected_without_recursion_error(tmp_path: Path) -> None:
    source = (Path(__file__).parents[2] / "config.example.yaml").read_text(encoding="utf-8")
    recursive = source.replace(
        "verification:\n  executables:\n    - git\n    - pytest",
        "verification:\n  executables: &commands\n    - git\n    - *commands",
    )
    path = tmp_path / "recursive-alias.yaml"
    path.write_text(recursive, encoding="utf-8")

    with pytest.raises(ValueError, match="aliases or recursive structures"):
        load_config(path)


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("daemon", "state_dir"),
        ("repository", "path"),
        ("repository", "artifact_dir"),
        ("repository", "worktree_dir"),
    ],
)
def test_rejects_paths_with_existing_symlink_components(
    tmp_path: Path, section: str, field: str
) -> None:
    real_root = tmp_path / "real-root"
    real_root.mkdir()
    linked_root = tmp_path / "linked-root"
    linked_root.symlink_to(real_root, target_is_directory=True)
    fixture_root = tmp_path / "fixture"
    fixture_root.mkdir()
    data = valid_config(fixture_root)
    target = data["daemon"] if section == "daemon" else data["repositories"]["widgets"]
    target[field] = str(linked_root / "child")

    with pytest.raises(ValidationError, match="symlink"):
        FactoryConfig.model_validate(data)


def test_dispatch_eligibility_uses_validated_configured_ready_state(tmp_path: Path) -> None:
    config = FactoryConfig.model_validate(valid_config(tmp_path))
    alternate_ready = UUID("2f4b39db-5471-4c2a-aa3d-28c13f7abc18")
    altered_states = {**config.linear.states, "Ready for Agent": alternate_ready}
    altered_config = config.model_copy(
        update={"linear": config.linear.model_copy(update={"states": altered_states})}
    )

    assert altered_config.is_dispatch_eligible(alternate_ready)
    assert not altered_config.is_dispatch_eligible(config.linear.states["Ready for Agent"])
