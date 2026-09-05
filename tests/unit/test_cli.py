from pathlib import Path
from subprocess import run

import yaml
from test_config import valid_config
from typer.testing import CliRunner

from software_factory.cli import app

runner = CliRunner()


def write_config(tmp_path: Path, data: dict[str, object]) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def init_git_repository(path: Path, origin: str) -> None:
    run(["git", "init", "-q", str(path)], check=True)
    run(["git", "-C", str(path), "remote", "add", "origin", origin], check=True)


def test_doctor_succeeds_without_creating_missing_directories(tmp_path: Path, monkeypatch) -> None:
    data = valid_config(tmp_path)
    repo = Path(data["repositories"]["widgets"]["path"])
    init_git_repository(repo, "https://github.com/acme/widgets")
    config_path = write_config(tmp_path, data)

    def reject_mkdir(*args, **kwargs) -> None:
        raise AssertionError("doctor must not create configured directories")

    monkeypatch.setattr(Path, "mkdir", reject_mkdir)

    result = runner.invoke(app, ["doctor", "--config", str(config_path)])

    assert result.exit_code == 0, result.output
    assert "configuration: ok" in result.output
    assert "live Linear validation: not performed" in result.output
    assert not Path(data["daemon"]["state_dir"]).exists()
    repository = data["repositories"]["widgets"]
    assert not Path(repository["artifact_dir"]).exists()
    assert not Path(repository["worktree_dir"]).exists()


def test_doctor_reports_all_local_failures_without_secret_value(tmp_path: Path) -> None:
    data = valid_config(tmp_path)
    secret = "DO_NOT_PRINT_THIS"  # pragma: allowlist secret -- redaction sentinel
    data["verification"]["executables"] = ["definitely-not-installed-wil131"]
    data["repositories"]["widgets"]["origin"] = "https://example.com/wrong.git"
    data["notification"] = {"keyholder_grant": secret}
    config_path = write_config(tmp_path, data)

    result = runner.invoke(app, ["doctor", "--config", str(config_path)])

    assert result.exit_code != 0
    assert "executable not found" in result.output
    assert "not a Git repository" in result.output
    assert secret not in result.output


def test_doctor_rejects_unknown_config_field(tmp_path: Path) -> None:
    data = valid_config(tmp_path)
    data["verification"]["surprise"] = True
    result = runner.invoke(app, ["doctor", "--config", str(write_config(tmp_path, data))])
    assert result.exit_code != 0
    assert "surprise" in result.output
    assert "Extra inputs are not permitted" in result.output


def test_doctor_validation_output_omits_pydantic_input_value(tmp_path: Path) -> None:
    data = valid_config(tmp_path)
    sentinel = "DO_NOT_ECHO-invalid-env"
    data["linear"]["api_key_env"] = sentinel

    result = runner.invoke(app, ["doctor", "--config", str(write_config(tmp_path, data))])

    assert result.exit_code != 0
    assert "linear.api_key_env" in result.output
    assert "environment-variable name" in result.output
    assert sentinel not in result.output


def test_doctor_does_not_treat_http_origin_as_configured_https(tmp_path: Path) -> None:
    data = valid_config(tmp_path)
    repo = Path(data["repositories"]["widgets"]["path"])
    init_git_repository(repo, "http://github.com/acme/widgets.git")

    result = runner.invoke(app, ["doctor", "--config", str(write_config(tmp_path, data))])

    assert result.exit_code != 0
    assert "origin does not match configuration" in result.output
