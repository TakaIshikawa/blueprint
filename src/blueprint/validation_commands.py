"""Repository validation command recommendations."""

from __future__ import annotations

import re
from typing import Any, Mapping


VALIDATION_CATEGORIES = ("test", "lint", "format", "typecheck", "build")


def recommend_validation_commands(summary: Mapping[str, Any]) -> dict[str, list[str]]:
    """Derive likely validation commands from repository scanner output."""
    package_managers = _as_string_set(summary.get("package_managers"))
    languages = _as_string_set(summary.get("languages"))
    important_files = _as_string_set(summary.get("important_files"))
    node_scripts = _as_string_set(summary.get("node_scripts"))
    python_tools = _as_string_set(summary.get("python_tools"))

    commands = {category: [] for category in VALIDATION_CATEGORIES}

    if "Python" in languages:
        _add_python_commands(commands, package_managers, important_files, python_tools, summary)

    if "Node/JavaScript" in languages:
        _add_node_commands(commands, package_managers, node_scripts)

    if "Makefile" in important_files:
        _add(commands["test"], "make test")

    return {category: _dedupe(values) for category, values in commands.items()}


def flatten_validation_commands(commands: Mapping[str, list[str]] | None) -> list[str]:
    """Return validation commands in a stable execution order."""
    if not commands:
        return []

    flattened: list[str] = []
    for category in VALIDATION_CATEGORIES:
        flattened.extend(commands.get(category, []))
    return _dedupe(flattened)


def extract_validation_commands_from_context(repo_context: str | None) -> dict[str, list[str]]:
    """Parse formatted repository context back into validation command groups."""
    if not repo_context:
        return {}

    commands = {category: [] for category in VALIDATION_CATEGORIES}
    for raw_line in repo_context.splitlines():
        line = raw_line.strip()
        match = re.match(r"-?\s*(test|lint|format|typecheck|build):\s*(.+)", line)
        if not match:
            continue
        category, values = match.groups()
        if values == "None detected":
            continue
        for value in values.split(", "):
            command = value.strip().strip("`")
            if command:
                commands[category].append(command)

    return {
        category: _dedupe(values)
        for category, values in commands.items()
        if values
    }


def format_validation_commands(commands: Mapping[str, list[str]] | None) -> str:
    """Render validation command groups for prompts and markdown exports."""
    if not commands or not flatten_validation_commands(commands):
        return "None detected"

    lines: list[str] = []
    for category in VALIDATION_CATEGORIES:
        values = commands.get(category, [])
        if values:
            rendered = ", ".join(f"`{command}`" for command in values)
            lines.append(f"  - {category}: {rendered}")
    return "\n".join(lines)


def _add_python_commands(
    commands: dict[str, list[str]],
    package_managers: set[str],
    important_files: set[str],
    python_tools: set[str],
    summary: Mapping[str, Any],
) -> None:
    prefix = "poetry run " if "poetry" in package_managers else ""

    for command in _as_string_list(summary.get("test_commands")):
        if command.startswith(("npm ", "pnpm ", "yarn ", "make ")):
            continue
        _add(commands["test"], command)

    if not commands["test"] and (
        {"pytest", "pyproject.toml", "setup.cfg", "tox.ini", "pytest.ini"} & important_files
        or "pytest" in python_tools
    ):
        _add(commands["test"], f"{prefix}pytest")

    if "ruff" in python_tools:
        _add(commands["lint"], f"{prefix}ruff check")
        _add(commands["format"], f"{prefix}ruff format --check")
    if "black" in python_tools:
        _add(commands["format"], f"{prefix}black --check .")
    if "mypy" in python_tools:
        _add(commands["typecheck"], f"{prefix}mypy .")
    if "pyright" in python_tools:
        _add(commands["typecheck"], f"{prefix}pyright")
    if "poetry" in package_managers:
        _add(commands["build"], "poetry build")


def _add_node_commands(
    commands: dict[str, list[str]],
    package_managers: set[str],
    node_scripts: set[str],
) -> None:
    manager = _node_package_manager(package_managers)

    script_categories = {
        "test": "test",
        "lint": "lint",
        "format": "format",
        "typecheck": "typecheck",
        "build": "build",
    }
    for category, script in script_categories.items():
        if script in node_scripts:
            _add(commands[category], _node_script_command(manager, script))


def _node_package_manager(package_managers: set[str]) -> str:
    if "pnpm" in package_managers:
        return "pnpm"
    if "yarn" in package_managers:
        return "yarn"
    return "npm"


def _node_script_command(manager: str, script: str) -> str:
    if script == "test":
        return f"{manager} test"
    if manager == "npm":
        return f"npm run {script}"
    return f"{manager} {script}"


def _add(commands: list[str], command: str) -> None:
    if command and command not in commands:
        commands.append(command)


def _as_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _as_string_set(value: Any) -> set[str]:
    return set(_as_string_list(value))


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped
