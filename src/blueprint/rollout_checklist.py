"""Build release-oriented rollout checklists from implementation plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.validation_commands import flatten_validation_commands


SECTION_ORDER = ("preflight", "implementation", "validation", "rollout", "rollback")


@dataclass(frozen=True, slots=True)
class RolloutChecklistSection:
    """A named group of release checklist items."""

    name: str
    items: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "name": self.name,
            "items": list(self.items),
        }


@dataclass(frozen=True, slots=True)
class RolloutChecklist:
    """Release checklist grouped into stable rollout sections."""

    brief_id: str | None
    plan_id: str | None
    sections: tuple[RolloutChecklistSection, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "plan_id": self.plan_id,
            "sections": [section.to_dict() for section in self.sections],
        }

    def to_markdown(self) -> str:
        """Render the checklist as deterministic markdown."""
        title_parts = ["Rollout Checklist"]
        if self.plan_id:
            title_parts.append(f"for {self.plan_id}")
        lines = [f"# {' '.join(title_parts)}", ""]

        for section in self.sections:
            lines.append(f"## {section.name.title()}")
            if section.items:
                lines.extend(f"- [ ] {item}" for item in section.items)
            else:
                lines.append("- [ ] No checklist items generated.")
            lines.append("")

        return "\n".join(lines).rstrip() + "\n"


def build_rollout_checklist(
    implementation_brief: Mapping[str, Any] | ImplementationBrief,
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> RolloutChecklist:
    """Build a release-oriented checklist from a brief and execution plan."""
    brief = _brief_payload(implementation_brief)
    plan = _plan_payload(execution_plan)
    tasks = _task_payloads(plan.get("tasks"))
    files = _task_files(tasks)
    risk_notes = _risk_notes(brief, plan)
    high_risk_tasks = _high_risk_tasks(tasks)
    migration_tasks = _migration_tasks(tasks)
    validation_commands = _validation_commands(tasks, plan)
    rollback_hints = _rollback_hints(tasks, plan)

    section_items = {
        "preflight": _preflight_items(brief, plan, files, risk_notes),
        "implementation": _implementation_items(tasks),
        "validation": _validation_items(brief, plan, tasks, validation_commands),
        "rollout": _rollout_items(brief, high_risk_tasks, migration_tasks),
        "rollback": _rollback_items(
            high_risk_tasks=high_risk_tasks,
            migration_tasks=migration_tasks,
            rollback_hints=rollback_hints,
        ),
    }

    return RolloutChecklist(
        brief_id=_optional_text(brief.get("id")),
        plan_id=_optional_text(plan.get("id")),
        sections=tuple(
            RolloutChecklistSection(name=section, items=tuple(section_items[section]))
            for section in SECTION_ORDER
        ),
    )


def rollout_checklist_to_dict(checklist: RolloutChecklist) -> dict[str, Any]:
    """Serialize a rollout checklist to a dictionary."""
    return checklist.to_dict()


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(brief)


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _preflight_items(
    brief: Mapping[str, Any],
    plan: Mapping[str, Any],
    files: list[str],
    risk_notes: list[str],
) -> list[str]:
    items = [
        "Confirm the implementation brief and execution plan are approved for release.",
        "Confirm the release owner, communication channel, and deployment window.",
    ]
    if brief.get("scope"):
        items.append("Review release scope: " + "; ".join(_strings(brief.get("scope"))) + ".")
    if plan.get("target_repo"):
        items.append(f"Confirm target repository is `{_text(plan.get('target_repo'))}`.")
    if files:
        items.append("Confirm affected files or modules: " + ", ".join(files) + ".")
    if risk_notes:
        items.append("Review known release risks: " + "; ".join(risk_notes) + ".")
    return _dedupe(items)


def _implementation_items(tasks: list[dict[str, Any]]) -> list[str]:
    items = ["Execute implementation tasks in execution-plan order."]
    for index, task in enumerate(tasks, start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        title = _text(task.get("title")) or task_id
        files = _strings(task.get("files_or_modules"))
        item = f"Complete `{task_id}`: {title}."
        if files:
            item += " Scope: " + ", ".join(files) + "."
        items.append(item)
    if not tasks:
        items.append("No execution tasks are listed; define release tasks before implementation.")
    return _dedupe(items)


def _validation_items(
    brief: Mapping[str, Any],
    plan: Mapping[str, Any],
    tasks: list[dict[str, Any]],
    validation_commands: list[str],
) -> list[str]:
    items: list[str] = []
    validation_plan = _optional_text(brief.get("validation_plan"))
    if validation_plan:
        items.append(f"Follow implementation validation plan: {validation_plan}")
    if plan.get("test_strategy"):
        items.append(f"Apply execution plan test strategy: {_text(plan.get('test_strategy'))}")
    items.extend(f"Run validation command: `{command}`." for command in validation_commands)
    items.extend(_acceptance_validation_items(tasks))
    if not items:
        items.append("Define and run release validation before rollout.")
    return _dedupe(items)


def _rollout_items(
    brief: Mapping[str, Any],
    high_risk_tasks: list[dict[str, Any]],
    migration_tasks: list[dict[str, Any]],
) -> list[str]:
    items = ["Release only after preflight, implementation, and validation items are complete."]
    integration_points = _strings(brief.get("integration_points"))
    if integration_points:
        items.append(
            "Coordinate rollout with integration points: "
            + ", ".join(integration_points)
            + "."
        )
    if high_risk_tasks:
        items.append(
            "Roll out high-risk tasks with a staged release, explicit approval checkpoint, "
            "and active monitoring: "
            + ", ".join(_task_refs(high_risk_tasks))
            + "."
        )
    if migration_tasks:
        items.append(
            "Apply migration or data tasks with a verified backup/snapshot and post-migration "
            "health check: "
            + ", ".join(_task_refs(migration_tasks))
            + "."
        )
    if not high_risk_tasks and not migration_tasks:
        items.append("Use the normal release path after validation passes.")
    return _dedupe(items)


def _rollback_items(
    *,
    high_risk_tasks: list[dict[str, Any]],
    migration_tasks: list[dict[str, Any]],
    rollback_hints: list[str],
) -> list[str]:
    items = ["Confirm the rollback owner and go/no-go decision point before release."]
    if rollback_hints:
        items.extend(f"Use rollback evidence: {hint}" for hint in rollback_hints)
    if high_risk_tasks:
        items.append(
            "Prepare rollback steps for high-risk tasks before rollout begins: "
            + ", ".join(_task_refs(high_risk_tasks))
            + "."
        )
    if migration_tasks:
        items.append(
            "Verify migration rollback, down migration, or data restore evidence before release: "
            + ", ".join(_task_refs(migration_tasks))
            + "."
        )
    if (high_risk_tasks or migration_tasks) and not rollback_hints:
        items.append(
            "Missing rollback evidence for high-risk or migration work; capture rollback steps "
            "or restore checkpoints before release."
        )
    if not high_risk_tasks and not migration_tasks:
        items.append(
            "Use version-control rollback for task-scoped code changes if release "
            "validation fails."
        )
    return _dedupe(items)


def _validation_commands(tasks: list[dict[str, Any]], plan: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for task in tasks:
        commands.extend(_task_validation_commands(task))

    metadata = plan.get("metadata")
    if isinstance(metadata, Mapping):
        commands.extend(_commands_from_value(metadata.get("validation_commands")))
        commands.extend(_commands_from_value(metadata.get("validation_command")))
        commands.extend(_commands_from_value(metadata.get("test_commands")))

    return _dedupe(commands)


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        if _optional_text(task.get(key)):
            commands.append(_text(task.get(key)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        commands.extend(_commands_from_value(metadata.get("validation_commands")))
        commands.extend(_commands_from_value(metadata.get("validation_command")))
        commands.extend(_commands_from_value(metadata.get("test_commands")))
    return commands


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    return _strings(value)


def _acceptance_validation_items(tasks: list[dict[str, Any]]) -> list[str]:
    items: list[str] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        criteria = _strings(task.get("acceptance_criteria"))
        if criteria:
            items.append(
                f"Verify acceptance criteria for `{task_id}`: "
                + "; ".join(criteria)
                + "."
            )
        else:
            items.append(
                f"Verify expected behavior for `{task_id}` because no acceptance "
                "criteria are listed."
            )
    return items


def _risk_notes(brief: Mapping[str, Any], plan: Mapping[str, Any]) -> list[str]:
    notes = _strings(brief.get("risks"))
    metadata = plan.get("metadata")
    if isinstance(metadata, Mapping):
        notes.extend(_strings(metadata.get("risks")))
        notes.extend(_strings(metadata.get("risk")))
        notes.extend(_strings(metadata.get("risk_notes")))
    return _dedupe(notes)


def _high_risk_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [task for task in tasks if _is_high_risk(task)]


def _migration_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [task for task in tasks if _is_migration_task(task)]


def _is_high_risk(task: Mapping[str, Any]) -> bool:
    risk_level = _text(task.get("risk_level") or task.get("risk")).lower()
    if risk_level in {"high", "critical", "blocker"}:
        return True
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        metadata_risk = " ".join(_strings(metadata.get("risks")) + _strings(metadata.get("risk")))
        return _has_token(metadata_risk, {"high", "critical", "destructive", "blocker"})
    return False


def _is_migration_task(task: Mapping[str, Any]) -> bool:
    files = _strings(task.get("files_or_modules"))
    context = " ".join(
        [
            _text(task.get("title")),
            _text(task.get("description")),
            *files,
            *_strings(task.get("acceptance_criteria")),
        ]
    )
    if _has_token(
        context,
        {"alembic", "backfill", "database", "migration", "migrations", "schema", "sql"},
    ):
        return True
    return any(_is_migration_path(file_path) for file_path in files)


def _is_migration_path(path: str) -> bool:
    normalized = path.strip().replace("\\", "/").lower().strip("/")
    if not normalized:
        return False
    pure_path = PurePosixPath(normalized)
    return pure_path.suffix == ".sql" or bool(
        {"alembic", "db", "migrations", "schema"} & set(pure_path.parts)
    )


def _rollback_hints(tasks: list[dict[str, Any]], plan: Mapping[str, Any]) -> list[str]:
    hints: list[str] = []
    metadata = plan.get("metadata")
    if isinstance(metadata, Mapping):
        hints.extend(_rollback_values(metadata))
    for task in tasks:
        metadata = task.get("metadata")
        if isinstance(metadata, Mapping):
            hints.extend(_rollback_values(metadata))
    return _dedupe(hints)


def _rollback_values(metadata: Mapping[str, Any]) -> list[str]:
    hints: list[str] = []
    for key in (
        "rollback",
        "rollback_hint",
        "rollback_hints",
        "rollback_plan",
        "rollback_strategy",
        "rollback_steps",
        "restore_plan",
    ):
        hints.extend(_strings(metadata.get(key)))
    return hints


def _task_files(tasks: list[dict[str, Any]]) -> list[str]:
    files: list[str] = []
    for task in tasks:
        files.extend(_strings(task.get("files_or_modules")))
    return _dedupe(files)


def _task_refs(tasks: list[dict[str, Any]]) -> list[str]:
    refs: list[str] = []
    for index, task in enumerate(tasks, start=1):
        refs.append(f"`{_text(task.get('id')) or f'task-{index}'}`")
    return refs


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for item in value.values():
            strings.extend(_strings(item))
        return strings
    if isinstance(value, (list, tuple, set)):
        strings: list[str] = []
        for item in value:
            strings.extend(_strings(item))
        return strings
    text = _text(value)
    return [text] if text else []


def _has_token(text: str, tokens: set[str]) -> bool:
    return bool(set(re.findall(r"[a-z0-9][a-z0-9-]*", text.lower())) & tokens)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            deduped.append(value)
    return deduped


__all__ = [
    "SECTION_ORDER",
    "RolloutChecklist",
    "RolloutChecklistSection",
    "build_rollout_checklist",
    "rollout_checklist_to_dict",
]
