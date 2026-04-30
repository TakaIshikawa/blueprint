"""Build compact smoke-test matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


SmokeTestArea = Literal[
    "user_flow",
    "api_backend",
    "data",
    "integration",
    "cli",
    "regression",
]
SmokeTestPriority = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_AREA_ORDER: dict[SmokeTestArea, int] = {
    "user_flow": 0,
    "api_backend": 1,
    "data": 2,
    "integration": 3,
    "cli": 4,
    "regression": 5,
}
_PRIORITY_RANK: dict[SmokeTestPriority, int] = {"low": 0, "medium": 1, "high": 2}

_USER_FLOW_TEXT_RE = re.compile(
    r"\b(?:frontend|ui|ux|browser|component|page|screen|view|form|"
    r"checkout|onboarding|workflow|user flow|accessibility)\b",
    re.IGNORECASE,
)
_BACKEND_TEXT_RE = re.compile(
    r"\b(?:api|backend|server|endpoint|route|controller|handler|service|"
    r"request|response|graphql|grpc|rest)\b",
    re.IGNORECASE,
)
_DATA_TEXT_RE = re.compile(
    r"\b(?:data|database|db|sql|schema|migration|migrations|alembic|model|"
    r"table|column|backfill|seed|persist|repository|index)\b",
    re.IGNORECASE,
)
_INTEGRATION_TEXT_RE = re.compile(
    r"\b(?:integration|third[- ]?party|external|vendor|provider|webhook|"
    r"callback|sync|api client|slack|github|stripe|salesforce)\b",
    re.IGNORECASE,
)
_CLI_TEXT_RE = re.compile(
    r"\b(?:cli|command line|terminal|argparse|click|typer|console|script|"
    r"subcommand|flag|option)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class PlanSmokeTestMatrixRow:
    """One smoke-test row covering a small set of execution tasks."""

    area: SmokeTestArea
    name: str
    covered_task_ids: tuple[str, ...] = field(default_factory=tuple)
    priority: SmokeTestPriority = "low"
    rationale: str = ""
    suggested_command: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "area": self.area,
            "name": self.name,
            "covered_task_ids": list(self.covered_task_ids),
            "priority": self.priority,
            "rationale": self.rationale,
            "suggested_command": self.suggested_command,
        }


@dataclass(frozen=True, slots=True)
class PlanSmokeTestMatrix:
    """Compact smoke-test matrix for post-implementation validation."""

    plan_id: str | None = None
    rows: tuple[PlanSmokeTestMatrixRow, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return smoke-test rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Smoke Test Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"

        lines = [title]
        if not self.rows:
            lines.append("")
            lines.append("No smoke tests were derived.")
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Area | Priority | Covered Tasks | Suggested Command | Rationale |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            tasks = ", ".join(row.covered_task_ids) if row.covered_task_ids else "plan"
            command = f"`{row.suggested_command}`" if row.suggested_command else "None"
            lines.append(
                "| "
                + " | ".join(
                    (
                        _markdown_cell(row.name),
                        row.priority,
                        _markdown_cell(tasks),
                        _markdown_cell(command),
                        _markdown_cell(row.rationale),
                    )
                )
                + " |"
            )
        return "\n".join(lines)


def build_plan_smoke_test_matrix(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanSmokeTestMatrix:
    """Derive a minimal post-implementation smoke-test matrix from an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    plan_commands = _plan_validation_commands(plan)
    plan_command_set = set(plan_commands)

    builders: dict[SmokeTestArea, _RowBuilder] = {}
    all_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        all_task_ids.append(task_id)
        areas = _task_areas(task)
        if not areas:
            continue
        task_commands = [
            command
            for command in _task_validation_commands(task)
            if command not in plan_command_set
        ]
        for area in areas:
            builder = builders.setdefault(area, _RowBuilder(area=area))
            builder.task_ids.append(task_id)
            builder.priority = _max_priority(builder.priority, _task_area_priority(task, area))
            builder.commands.extend(task_commands)
            builder.signals.extend(_task_signals(task, area))

    if plan_commands or tasks:
        regression = builders.setdefault("regression", _RowBuilder(area="regression"))
        regression.task_ids.extend(all_task_ids)
        regression.commands.extend(plan_commands)
        regression.priority = _max_priority(
            regression.priority,
            (
                "high"
                if any(_risk_level(task.get("risk_level")) == "high" for task in tasks)
                else "medium"
            ),
        )
        if plan_commands:
            regression.signals.append("plan-level validation command")
        if _optional_text(plan.get("test_strategy")):
            regression.signals.append("test_strategy")

    rows = tuple(
        _row_from_builder(builder)
        for builder in sorted(builders.values(), key=lambda item: _AREA_ORDER[item.area])
        if builder.task_ids or builder.commands
    )
    return PlanSmokeTestMatrix(plan_id=_optional_text(plan.get("id")), rows=rows)


def plan_smoke_test_matrix_to_dict(
    matrix: PlanSmokeTestMatrix,
) -> dict[str, Any]:
    """Serialize a smoke-test matrix to a plain dictionary."""
    return matrix.to_dict()


plan_smoke_test_matrix_to_dict.__test__ = False


@dataclass(slots=True)
class _RowBuilder:
    area: SmokeTestArea
    task_ids: list[str] = field(default_factory=list)
    priority: SmokeTestPriority = "low"
    commands: list[str] = field(default_factory=list)
    signals: list[str] = field(default_factory=list)


def _row_from_builder(builder: _RowBuilder) -> PlanSmokeTestMatrixRow:
    task_ids = tuple(_dedupe(builder.task_ids))
    command = next(iter(_dedupe(builder.commands)), None)
    return PlanSmokeTestMatrixRow(
        area=builder.area,
        name=_area_name(builder.area),
        covered_task_ids=task_ids,
        priority=builder.priority,
        rationale=_rationale(builder.area, task_ids, builder.priority, builder.signals),
        suggested_command=command,
    )


def _task_areas(task: Mapping[str, Any]) -> list[SmokeTestArea]:
    files = _strings(task.get("files_or_modules"))
    context = _task_context(task)
    areas: list[SmokeTestArea] = []

    if any(_is_user_flow_path(path) for path in files) or _USER_FLOW_TEXT_RE.search(context):
        areas.append("user_flow")
    if any(_is_backend_path(path) for path in files) or (
        _BACKEND_TEXT_RE.search(context) and not _INTEGRATION_TEXT_RE.search(context)
    ):
        areas.append("api_backend")
    if any(_is_data_path(path) for path in files) or _DATA_TEXT_RE.search(context):
        areas.append("data")
    if any(_is_integration_path(path) for path in files) or _INTEGRATION_TEXT_RE.search(context):
        areas.append("integration")
    if any(_is_cli_path(path) for path in files) or _CLI_TEXT_RE.search(context):
        areas.append("cli")

    return [area for area in _AREA_ORDER if area in areas and area != "regression"]


def _task_area_priority(task: Mapping[str, Any], area: SmokeTestArea) -> SmokeTestPriority:
    if _risk_level(task.get("risk_level")) == "high":
        return "high"
    if area in {"data", "integration", "api_backend"}:
        return "medium"
    if _strings(task.get("depends_on")) or _strings(task.get("dependencies")):
        return "medium"
    return "low"


def _task_signals(task: Mapping[str, Any], area: SmokeTestArea) -> list[str]:
    signals: list[str] = []
    title = _optional_text(task.get("title"))
    if title:
        signals.append(f"title: {title}")
    files = _strings(task.get("files_or_modules"))
    if files:
        signals.append(f"files: {', '.join(files[:3])}")
    criteria = _strings(task.get("acceptance_criteria"))
    if criteria:
        signals.append(f"acceptance: {criteria[0]}")
    if area == "regression":
        return signals
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("smoke_test", "smoke_tests", "validation", "validation_scope"):
            values = _strings(metadata.get(key))
            if values:
                signals.append(f"metadata.{key}: {values[0]}")
    return signals


def _rationale(
    area: SmokeTestArea,
    task_ids: tuple[str, ...],
    priority: SmokeTestPriority,
    signals: Iterable[str],
) -> str:
    count = len(task_ids)
    task_phrase = f"{count} task" if count == 1 else f"{count} tasks"
    base = {
        "user_flow": f"Smoke the primary user-visible path across {task_phrase}.",
        "api_backend": (
            f"Exercise changed backend request or service behavior across {task_phrase}."
        ),
        "data": f"Verify data persistence, migration, or backfill behavior across {task_phrase}.",
        "integration": f"Check external boundary or cross-system behavior across {task_phrase}.",
        "cli": f"Run command-line entry point coverage across {task_phrase}.",
        "regression": (
            f"Reuse plan-level validation as the compact regression smoke for {task_phrase}."
        ),
    }[area]
    evidence = next(iter(_dedupe(signals)), None)
    if evidence:
        base = f"{base} Signal: {evidence}."
    if priority == "high":
        return f"{base} Prioritized high because high-risk work is covered."
    if priority == "medium":
        return f"{base} Prioritized medium because shared runtime behavior is covered."
    return base


def _area_name(area: SmokeTestArea) -> str:
    return {
        "user_flow": "User Flow",
        "api_backend": "API/Backend",
        "data": "Data",
        "integration": "Integration",
        "cli": "CLI",
        "regression": "Regression",
    }[area]


def _is_user_flow_path(path: str) -> bool:
    pure_path = _pure_path(path)
    return pure_path.suffix in {
        ".css",
        ".scss",
        ".sass",
        ".tsx",
        ".jsx",
        ".vue",
        ".svelte",
    } or bool(
        {"app", "client", "components", "frontend", "pages", "screens", "ui", "views"}
        & set(pure_path.parts)
    )


def _is_backend_path(path: str) -> bool:
    pure_path = _pure_path(path)
    return bool(
        {"api", "backend", "controllers", "handlers", "routes", "server", "services"}
        & set(pure_path.parts)
    )


def _is_data_path(path: str) -> bool:
    pure_path = _pure_path(path)
    return pure_path.suffix == ".sql" or bool(
        {"alembic", "database", "db", "migrations", "models", "repositories", "schema", "store"}
        & set(pure_path.parts)
    )


def _is_integration_path(path: str) -> bool:
    pure_path = _pure_path(path)
    parts = set(pure_path.parts)
    return bool({"clients", "connectors", "integrations", "providers", "webhooks"} & parts) or (
        "integration" in pure_path.name or "webhook" in pure_path.name
    )


def _is_cli_path(path: str) -> bool:
    pure_path = _pure_path(path)
    parts = set(pure_path.parts)
    return bool({"bin", "cli", "commands", "scripts"} & parts) or (
        pure_path.name in {"cli.py", "commands.py"} or pure_path.name.endswith("_cli.py")
    )


def _pure_path(path: str) -> PurePosixPath:
    return PurePosixPath(path.strip().replace("\\", "/").lower().strip("/"))


def _task_context(task: Mapping[str, Any]) -> str:
    values = [
        _text(task.get("title")),
        _text(task.get("description")),
        *_strings(task.get("files_or_modules")),
        *_strings(task.get("acceptance_criteria")),
        *_task_validation_commands(task),
    ]
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        values.extend(_strings(metadata))
    return " ".join(value for value in values if value)


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        command = _optional_text(task.get(key))
        if command:
            commands.append(command)
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
            commands.extend(_commands_from_value(metadata.get(key)))
    return _dedupe(commands)


def _plan_validation_commands(plan: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    test_strategy = _optional_text(plan.get("test_strategy"))
    if test_strategy:
        commands.append(_command_text(test_strategy))
    metadata = plan.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
            commands.extend(_commands_from_value(metadata.get(key)))
    return _dedupe(commands)


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    return _strings(value)


def _command_text(value: str) -> str:
    text = _text(value)
    match = re.match(r"^(?:run|execute)\s+(.+)$", text, re.IGNORECASE)
    return match.group(1).strip() if match else text


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            dumped = item.model_dump(mode="python")
            if isinstance(dumped, Mapping):
                tasks.append(dict(dumped))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _risk_level(value: Any) -> SmokeTestPriority:
    text = (_optional_text(value) or "").lower()
    if text in {"critical", "blocker", "high"}:
        return "high"
    if text in {"medium", "moderate"}:
        return "medium"
    return "low"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _max_priority(
    left: SmokeTestPriority,
    right: SmokeTestPriority,
) -> SmokeTestPriority:
    return left if _PRIORITY_RANK[left] >= _PRIORITY_RANK[right] else right


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|")


__all__ = [
    "PlanSmokeTestMatrix",
    "PlanSmokeTestMatrixRow",
    "SmokeTestArea",
    "SmokeTestPriority",
    "build_plan_smoke_test_matrix",
    "plan_smoke_test_matrix_to_dict",
]
