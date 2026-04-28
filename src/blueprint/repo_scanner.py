"""Lightweight repository scanner for plan-generation context."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


IGNORED_DIRS = {
    ".cache",
    ".git",
    ".hg",
    ".mypy_cache",
    ".nox",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    ".svn",
    "build",
    "coverage",
    "dist",
    "htmlcov",
    "node_modules",
    "target",
    "vendor",
    "venv",
}

IMPORTANT_FILENAMES = {
    ".blueprint.yaml",
    ".cursorrules",
    ".env.example",
    ".github",
    "AGENTS.md",
    "Dockerfile",
    "LICENSE",
    "Makefile",
    "README.md",
    "docker-compose.yml",
    "package-lock.json",
    "package.json",
    "pnpm-lock.yaml",
    "poetry.lock",
    "pyproject.toml",
    "requirements.txt",
    "setup.cfg",
    "setup.py",
    "tox.ini",
    "yarn.lock",
}

PYTHON_EXTENSIONS = {".py"}
NODE_EXTENSIONS = {".js", ".jsx", ".mjs", ".cjs", ".ts", ".tsx"}


def scan_repository(repo_path: str | Path) -> dict[str, Any]:
    """Return a compact summary of repository signals for planning prompts."""
    root = Path(repo_path).expanduser().resolve()
    if not root.exists():
        raise ValueError(f"Repository path does not exist: {repo_path}")
    if not root.is_dir():
        raise ValueError(f"Repository path is not a directory: {repo_path}")

    root_entries = _safe_iterdir(root)
    top_level = _top_level_structure(root_entries)
    all_files = _shallow_files(root)
    root_file_names = {entry.name for entry in root_entries if entry.is_file()}
    root_dir_names = {entry.name for entry in root_entries if entry.is_dir()}

    languages = _detect_languages(root_file_names, all_files)
    package_managers = _detect_package_managers(root, root_file_names)
    important_files = _important_files(root, root_entries, all_files)
    test_commands = _detect_test_commands(root, root_file_names, package_managers)

    return {
        "path": str(root),
        "languages": languages,
        "package_managers": package_managers,
        "important_files": important_files,
        "test_commands": test_commands,
        "top_level_structure": top_level,
        "ignored_directories": sorted(name for name in IGNORED_DIRS if name in root_dir_names),
    }


def format_repository_context(summary: dict[str, Any]) -> str:
    """Render scanner output as stable prompt context."""
    lines = [
        f"Path: {summary['path']}",
        f"Languages: {_format_list(summary.get('languages'))}",
        f"Package managers: {_format_list(summary.get('package_managers'))}",
        f"Important files: {_format_list(summary.get('important_files'))}",
        f"Likely test commands: {_format_list(summary.get('test_commands'))}",
        f"Ignored directories: {_format_list(summary.get('ignored_directories'))}",
        "Top-level structure:",
    ]
    structure = summary.get("top_level_structure") or []
    if structure:
        lines.extend(f"  - {entry}" for entry in structure)
    else:
        lines.append("  - Not detected")
    return "\n".join(lines)


def _safe_iterdir(path: Path) -> list[Path]:
    try:
        return sorted(path.iterdir(), key=lambda item: item.name.lower())
    except OSError as exc:
        raise ValueError(f"Could not read repository path {path}: {exc}") from exc


def _shallow_files(root: Path, max_depth: int = 2, max_files: int = 300) -> list[Path]:
    files: list[Path] = []
    stack: list[tuple[Path, int]] = [(root, 0)]

    while stack and len(files) < max_files:
        current, depth = stack.pop()
        for entry in _safe_iterdir(current):
            if entry.is_dir():
                if depth < max_depth and entry.name not in IGNORED_DIRS:
                    stack.append((entry, depth + 1))
                continue
            if entry.is_file():
                files.append(entry)
                if len(files) >= max_files:
                    break

    return files


def _top_level_structure(root_entries: list[Path], max_entries: int = 40) -> list[str]:
    structure: list[str] = []
    for entry in root_entries:
        if entry.name in IGNORED_DIRS:
            continue
        suffix = "/" if entry.is_dir() else ""
        structure.append(f"{entry.name}{suffix}")
        if len(structure) >= max_entries:
            break
    return structure


def _detect_languages(root_file_names: set[str], files: list[Path]) -> list[str]:
    languages: list[str] = []
    extensions = {path.suffix for path in files}

    if (
        "pyproject.toml" in root_file_names
        or "setup.py" in root_file_names
        or "requirements.txt" in root_file_names
        or extensions & PYTHON_EXTENSIONS
    ):
        languages.append("Python")

    if "package.json" in root_file_names or extensions & NODE_EXTENSIONS:
        languages.append("Node/JavaScript")

    if "Dockerfile" in root_file_names or any(path.name == "Dockerfile" for path in files):
        languages.append("Docker")

    if "Makefile" in root_file_names:
        languages.append("Make")

    return languages or ["Generic"]


def _detect_package_managers(root: Path, root_file_names: set[str]) -> list[str]:
    managers: list[str] = []
    pyproject = root / "pyproject.toml"

    if "poetry.lock" in root_file_names or _file_contains(pyproject, "[tool.poetry]"):
        managers.append("poetry")
    if "requirements.txt" in root_file_names:
        managers.append("pip")
    if "package-lock.json" in root_file_names:
        managers.append("npm")
    if "pnpm-lock.yaml" in root_file_names:
        managers.append("pnpm")
    if "yarn.lock" in root_file_names:
        managers.append("yarn")

    return managers


def _important_files(root: Path, root_entries: list[Path], files: list[Path]) -> list[str]:
    root_names = {entry.name for entry in root_entries}
    important = [
        name for name in sorted(IMPORTANT_FILENAMES, key=str.lower) if name in root_names
    ]

    for candidate in files:
        relative = candidate.relative_to(root).as_posix()
        if relative.startswith(".github/workflows/") or candidate.name in {
            "pytest.ini",
            "vitest.config.js",
            "vitest.config.ts",
        }:
            important.append(relative)

    return _dedupe(important)[:30]


def _detect_test_commands(
    root: Path,
    root_file_names: set[str],
    package_managers: list[str],
) -> list[str]:
    commands: list[str] = []

    if "poetry" in package_managers:
        commands.append("poetry run pytest")
    elif _has_python_test_signal(root, root_file_names):
        commands.append("pytest")

    package_json = _read_package_json(root / "package.json")
    scripts = package_json.get("scripts", {}) if isinstance(package_json, dict) else {}
    if isinstance(scripts, dict) and "test" in scripts:
        if "pnpm" in package_managers:
            commands.append("pnpm test")
        elif "yarn" in package_managers:
            commands.append("yarn test")
        else:
            commands.append("npm test")

    makefile_text = _read_text(root / "Makefile")
    if "Makefile" in root_file_names and (
        makefile_text.startswith("test:") or "\ntest:" in makefile_text
    ):
        commands.append("make test")

    return _dedupe(commands)


def _has_python_test_signal(root: Path, root_file_names: set[str]) -> bool:
    if {"pyproject.toml", "setup.cfg", "tox.ini", "pytest.ini"} & root_file_names:
        return True
    return (root / "tests").is_dir()


def _read_package_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _file_contains(path: Path, needle: str) -> bool:
    return needle in _read_text(path)


def _read_text(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        return path.read_text(errors="ignore")
    except OSError:
        return ""


def _format_list(values: Any) -> str:
    if not values:
        return "None detected"
    return ", ".join(str(value) for value in values)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped
