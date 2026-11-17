import importlib.util
import os
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELEASE = ROOT / "scripts" / "release"


def load(name: str):
    """Import scripts/release/<name>.py, which is a script and not a package."""
    spec = importlib.util.spec_from_file_location(f"release_{name}", RELEASE / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fake_bin(directory: pathlib.Path, name: str, body: str) -> dict:
    """Write an executable `name` into directory and return an env with it first on PATH."""
    directory.mkdir(exist_ok=True)
    path = directory / name
    path.write_text("#!/usr/bin/env bash\n" + body)
    path.chmod(0o755)
    return {**os.environ, "PATH": f"{directory}:{os.environ['PATH']}"}


def git(repo: pathlib.Path, *args: str, env: dict | None = None) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], env=env, check=True, capture_output=True, text=True
    ).stdout.strip()


def init_repo(repo: pathlib.Path, env: dict | None = None) -> None:
    """An empty repository with a fixed identity and signing off by default."""
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], env=env, check=True)
    for key, value in {
        "user.name": "Release test",
        "user.email": "release-test@example.invalid",
        "commit.gpgsign": "false",
        "tag.gpgSign": "false",
    }.items():
        git(repo, "config", key, value, env=env)
