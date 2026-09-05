"""Command-line entry point and local-only doctor diagnostics."""

from __future__ import annotations

import os
import shutil
import subprocess  # nosec B404
from pathlib import Path
from typing import Annotated
from urllib.parse import urlsplit, urlunsplit

import typer
from pydantic import ValidationError

from software_factory.config import FactoryConfig, load_config

app = typer.Typer(help="Validate software-factory configuration and local prerequisites.")


def _validation_message(error: ValidationError) -> str:
    lines: list[str] = []
    for detail in error.errors(include_url=False, include_context=False, include_input=False):
        location = ".".join(str(part) for part in detail["loc"])
        lines.append(f"{location}: {detail['msg']}")
    return "\n".join(lines)


@app.callback()
def root() -> None:
    """software-factory local administration commands."""


def _normalized_origin(value: str) -> str:
    value = value.strip()
    parsed = urlsplit(value)
    path = parsed.path.rstrip("/")
    if path.endswith(".git"):
        path = path[:-4]
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, "", ""))


def _git_origin(path: Path) -> tuple[str | None, str | None]:
    if not (path / ".git").exists():
        return None, "not a Git repository"
    git = shutil.which("git")
    if git is None:
        return None, "git executable not found"
    # The executable is resolved from PATH and receives a fixed argument vector.
    result = subprocess.run(  # noqa: S603  # nosec B603
        [git, "-C", os.fspath(path), "remote", "get-url", "origin"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        return None, "Git origin is not configured"
    return result.stdout.strip(), None


def _check_writable(path: Path) -> str | None:
    cursor = path
    while not cursor.exists():
        parent = cursor.parent
        if parent == cursor:
            return "has no existing writable ancestor"
        cursor = parent
    if not cursor.is_dir():
        return "path or nearest existing ancestor is not a directory"
    if not os.access(cursor, os.W_OK | os.X_OK, effective_ids=True):
        return "directory is not writable"
    return None


def _diagnose(config: FactoryConfig) -> list[str]:
    failures: list[str] = []
    for executable in config.verification.executables:
        if shutil.which(executable) is None:
            failures.append(f"executable not found: {executable}")
    for name, repository in config.repositories.items():
        actual, error = _git_origin(repository.path)
        if error:
            failures.append(f"repository {name}: {error}")
        elif actual is not None and _normalized_origin(actual) != _normalized_origin(
            repository.origin
        ):
            failures.append(f"repository {name}: origin does not match configuration")
        for label, path in (
            ("artifact", repository.artifact_dir),
            ("worktree", repository.worktree_dir),
        ):
            if error := _check_writable(path):
                failures.append(f"repository {name} {label} {error}")
    if error := _check_writable(config.daemon.state_dir):
        failures.append(f"state {error}")
    return failures


@app.command()
def doctor(
    config: Annotated[Path, typer.Option("--config", exists=True, dir_okay=False)],
) -> None:
    """Validate configuration and local resources; never contact live services."""
    try:
        parsed = load_config(config)
    except ValidationError as exc:
        typer.echo(f"configuration: failed\n{_validation_message(exc)}")
        raise typer.Exit(code=1) from None
    except (OSError, ValueError) as exc:
        typer.echo(f"configuration: failed\n{exc}")
        raise typer.Exit(code=1) from None

    typer.echo("configuration: ok")
    typer.echo("required Linear state IDs: present and unique")
    typer.echo("live Linear validation: not performed")
    failures = _diagnose(parsed)
    for failure in failures:
        typer.echo(f"failed: {failure}")
    if failures:
        raise typer.Exit(code=1)
    typer.echo("local diagnostics: ok")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
