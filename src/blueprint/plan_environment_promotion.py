"""Derive environment promotion maps for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


PromotionStageName = Literal[
    "local",
    "ci",
    "preview",
    "staging",
    "data-migration",
    "rollback",
    "production",
]

_STAGE_ORDER: dict[PromotionStageName, int] = {
    "local": 0,
    "ci": 1,
    "preview": 2,
    "staging": 3,
    "data-migration": 4,
    "rollback": 5,
    "production": 6,
}
_COMPLETE_STATUSES = {"completed", "skipped"}

_CI_RE = re.compile(
    r"\b(?:ci|continuous integration|pytest|test command|test_command|lint|"
    r"typecheck|type check|build|github actions?|workflow|pipeline)\b",
    re.IGNORECASE,
)
_PREVIEW_RE = re.compile(
    r"\b(?:preview|review app|review-app|vercel|netlify|storybook|ephemeral env|"
    r"temporary environment)\b",
    re.IGNORECASE,
)
_STAGING_RE = re.compile(
    r"\b(?:staging|stage env|preprod|pre-prod|pre production|pre-production|"
    r"canary|smoke test|smoke-test|uat)\b",
    re.IGNORECASE,
)
_PRODUCTION_RE = re.compile(
    r"\b(?:production|prod|go-live|go live|release|rollout|launch|live traffic|"
    r"customer traffic|post deploy|post-deploy)\b",
    re.IGNORECASE,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrations|migrate|schema|ddl|alembic|liquibase|flyway|"
    r"backfill|data migration|sql|database|db table|column|index)\b",
    re.IGNORECASE,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|revert|restore|down migration|undo|feature flag off|"
    r"disable flag|kill switch|recovery plan)\b",
    re.IGNORECASE,
)
_RISK_RE = re.compile(
    r"\b(?:high|critical|risky|destructive|manual|security|auth|secret|payment|"
    r"billing|customer[- ]facing|production|production data|live traffic)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class PromotionBlocker:
    """A dependency that must clear before a task can move through a later stage."""

    task_id: str
    blocked_by: str
    blocker_stage: PromotionStageName
    status: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "blocked_by": self.blocked_by,
            "blocker_stage": self.blocker_stage,
            "status": self.status,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class PromotionStage:
    """One ordered environment promotion stage."""

    name: PromotionStageName
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    blockers: tuple[PromotionBlocker, ...] = field(default_factory=tuple)
    required_evidence: tuple[str, ...] = field(default_factory=tuple)
    risk_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "name": self.name,
            "task_ids": list(self.task_ids),
            "blockers": [blocker.to_dict() for blocker in self.blockers],
            "required_evidence": list(self.required_evidence),
            "risk_notes": list(self.risk_notes),
        }


@dataclass(frozen=True, slots=True)
class PlanEnvironmentPromotionMap:
    """Environment promotion stages for an execution plan."""

    plan_id: str | None = None
    stages: tuple[PromotionStage, ...] = field(default_factory=tuple)
    task_stage_map: dict[str, list[PromotionStageName]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "stages": [stage.to_dict() for stage in self.stages],
            "task_stage_map": {
                task_id: list(stages) for task_id, stages in self.task_stage_map.items()
            },
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return promotion stages as plain dictionaries."""
        return [stage.to_dict() for stage in self.stages]

    def to_markdown(self) -> str:
        """Render the promotion map as deterministic Markdown."""
        title = "# Plan Environment Promotion Map"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.stages:
            lines.extend(["", "No environment promotion stages were derived."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Stage | Tasks | Blockers | Required Evidence | Risk Notes |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for stage in self.stages:
            blockers = "; ".join(
                f"{blocker.task_id} waits for {blocker.blocked_by} "
                f"({blocker.blocker_stage}, {blocker.status})"
                for blocker in stage.blockers
            )
            lines.append(
                "| "
                f"{stage.name} | "
                f"{_markdown_cell(', '.join(stage.task_ids) or 'none')} | "
                f"{_markdown_cell(blockers or 'none')} | "
                f"{_markdown_cell('; '.join(stage.required_evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(stage.risk_notes) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_environment_promotion_map(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanEnvironmentPromotionMap:
    """Derive ordered environment promotion stages from an execution plan."""
    plan_id, tasks = _source_payload(source)
    task_ids = [
        _optional_text(task.get("id")) or f"task-{index}"
        for index, task in enumerate(tasks, start=1)
    ]
    normalized_tasks = [
        {**task, "id": task_id} for task, task_id in zip(tasks, task_ids, strict=True)
    ]
    task_by_id = {task_id: task for task_id, task in zip(task_ids, normalized_tasks, strict=True)}

    task_stage_map: dict[str, list[PromotionStageName]] = {}
    stage_task_ids: dict[PromotionStageName, list[str]] = {stage: [] for stage in _STAGE_ORDER}
    stage_evidence: dict[PromotionStageName, list[str]] = {stage: [] for stage in _STAGE_ORDER}
    stage_risks: dict[PromotionStageName, list[str]] = {stage: [] for stage in _STAGE_ORDER}

    for task in normalized_tasks:
        task_id = _optional_text(task.get("id")) or "task"
        stages = _task_stages(task)
        task_stage_map[task_id] = list(stages)
        for stage in stages:
            stage_task_ids[stage].append(task_id)
        for stage, note in _task_required_evidence(task_id, task):
            stage_evidence[stage].append(note)
        for stage, note in _task_risk_notes(task_id, task, stages):
            stage_risks[stage].append(note)

    _add_production_gate_evidence(stage_evidence, task_stage_map)
    task_stage_lookup = {
        task_id: tuple(stages) for task_id, stages in task_stage_map.items()
    }
    stages = tuple(
        PromotionStage(
            name=stage,
            task_ids=tuple(_dedupe(stage_task_ids[stage])),
            blockers=tuple(
                _stage_blockers(stage, stage_task_ids[stage], task_by_id, task_stage_lookup)
            ),
            required_evidence=tuple(_dedupe(stage_evidence[stage])),
            risk_notes=tuple(_dedupe(stage_risks[stage])),
        )
        for stage in _STAGE_ORDER
        if stage_task_ids[stage] or stage_evidence[stage] or stage_risks[stage]
    )

    return PlanEnvironmentPromotionMap(
        plan_id=plan_id,
        stages=stages,
        task_stage_map={task_id: list(stages) for task_id, stages in task_stage_map.items()},
    )


def derive_plan_environment_promotion_map(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanEnvironmentPromotionMap:
    """Compatibility alias for building an environment promotion map."""
    return build_plan_environment_promotion_map(source)


def plan_environment_promotion_map_to_dict(
    promotion_map: PlanEnvironmentPromotionMap,
) -> dict[str, Any]:
    """Serialize an environment promotion map to a plain dictionary."""
    return promotion_map.to_dict()


plan_environment_promotion_map_to_dict.__test__ = False


def plan_environment_promotion_map_to_markdown(
    promotion_map: PlanEnvironmentPromotionMap,
) -> str:
    """Render an environment promotion map as Markdown."""
    return promotion_map.to_markdown()


plan_environment_promotion_map_to_markdown.__test__ = False


def _task_stages(task: Mapping[str, Any]) -> tuple[PromotionStageName, ...]:
    stages: list[PromotionStageName] = ["local"]
    files = _strings(task.get("files_or_modules") or task.get("files"))
    context = _task_context(task)
    commands = _task_validation_commands(task)

    if commands or _CI_RE.search(context) or any(_is_ci_path(path) for path in files):
        stages.append("ci")
    if _PREVIEW_RE.search(context) or any(_is_preview_path(path) for path in files):
        stages.append("preview")
    if _STAGING_RE.search(context) or any(_is_staging_path(path) for path in files):
        stages.append("staging")
    if _is_migration_task(files, context):
        stages.append("data-migration")
    if _is_rollback_task(files, context):
        stages.append("rollback")
    if _PRODUCTION_RE.search(context) or any(_is_production_path(path) for path in files):
        stages.append("production")

    return tuple(stage for stage in _STAGE_ORDER if stage in set(stages))


def _task_required_evidence(
    task_id: str,
    task: Mapping[str, Any],
) -> list[tuple[PromotionStageName, str]]:
    files = _strings(task.get("files_or_modules") or task.get("files"))
    context = _task_context(task)
    evidence: list[tuple[PromotionStageName, str]] = []
    if _is_migration_task(files, context):
        evidence.extend(
            [
                (
                    "data-migration",
                    f"{task_id}: migration dry-run or non-production apply result is recorded.",
                ),
                (
                    "data-migration",
                    f"{task_id}: backup, restore, or down-migration path is documented.",
                ),
            ]
        )
    if _is_rollback_task(files, context):
        evidence.extend(
            [
                ("rollback", f"{task_id}: rollback procedure is rehearsed or reviewed."),
                (
                    "rollback",
                    f"{task_id}: post-rollback verification command or checklist is recorded.",
                ),
            ]
        )
    return evidence


def _add_production_gate_evidence(
    stage_evidence: dict[PromotionStageName, list[str]],
    task_stage_map: Mapping[str, list[PromotionStageName]],
) -> None:
    for task_id, stages in task_stage_map.items():
        if "data-migration" in stages:
            stage_evidence["production"].append(
                f"{task_id}: migration evidence must be approved before production promotion."
            )
        if "rollback" in stages:
            stage_evidence["production"].append(
                f"{task_id}: rollback evidence must be approved before production promotion."
            )


def _task_risk_notes(
    task_id: str,
    task: Mapping[str, Any],
    stages: tuple[PromotionStageName, ...],
) -> list[tuple[PromotionStageName, str]]:
    context = _task_context(task)
    risk_level = _optional_text(task.get("risk_level"))
    notes: list[tuple[PromotionStageName, str]] = []
    if risk_level and risk_level.lower() not in {"low", "none"}:
        for stage in stages:
            notes.append((stage, f"{task_id}: risk level is {risk_level}."))
    if _RISK_RE.search(context):
        target_stages = [stage for stage in stages if stage != "local"] or list(stages)
        for stage in target_stages:
            notes.append((stage, f"{task_id}: promotion context contains elevated risk signals."))
    return notes


def _stage_blockers(
    stage: PromotionStageName,
    stage_task_ids: list[str],
    task_by_id: Mapping[str, Mapping[str, Any]],
    task_stage_lookup: Mapping[str, tuple[PromotionStageName, ...]],
) -> list[PromotionBlocker]:
    blockers: list[PromotionBlocker] = []
    stage_index = _STAGE_ORDER[stage]
    for task_id in _dedupe(stage_task_ids):
        task = task_by_id.get(task_id, {})
        for dependency_id in _strings(task.get("depends_on") or task.get("dependencies")):
            dependency = task_by_id.get(dependency_id)
            if not dependency:
                continue
            dependency_status = (_optional_text(dependency.get("status")) or "pending").lower()
            if dependency_status in _COMPLETE_STATUSES:
                continue
            earlier_dependency_stages = [
                dependency_stage
                for dependency_stage in task_stage_lookup.get(dependency_id, ())
                if _STAGE_ORDER[dependency_stage] < stage_index
            ]
            if not earlier_dependency_stages:
                continue
            blocker_stage = earlier_dependency_stages[-1]
            blockers.append(
                PromotionBlocker(
                    task_id=task_id,
                    blocked_by=dependency_id,
                    blocker_stage=blocker_stage,
                    status=dependency_status,
                    reason=(
                        f"{task_id} cannot enter {stage} until {dependency_id} "
                        f"clears {blocker_stage}."
                    ),
                )
            )
    return _dedupe_blockers(blockers)


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))
        return None, [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))

    tasks: list[dict[str, Any]] = []
    for item in source:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    return _object_payload(value)


def _object_payload(value: Any) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "status",
        "tags",
        "labels",
        "metadata",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _task_context(task: Mapping[str, Any]) -> str:
    values: list[str] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "status",
    ):
        if text := _optional_text(task.get(field_name)):
            values.append(text)
    values.extend(_strings(task.get("files_or_modules") or task.get("files")))
    values.extend(_strings(task.get("acceptance_criteria")))
    values.extend(_strings(task.get("tags")))
    values.extend(_strings(task.get("labels")))
    values.extend(_metadata_texts(task.get("metadata")))
    return " ".join(values)


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        if text := _optional_text(task.get(key)):
            commands.append(text)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        commands.extend(_commands_from_value(metadata.get("validation_commands")))
        commands.extend(_commands_from_value(metadata.get("validation_command")))
        commands.extend(_commands_from_value(metadata.get("test_commands")))
        commands.extend(_commands_from_value(metadata.get("test_command")))
    return _dedupe(commands)


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    return _strings(value)


def _metadata_texts(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        texts: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            texts.extend(_metadata_texts(value[key]))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[str] = []
        for item in items:
            texts.extend(_metadata_texts(item))
        return texts
    if text := _optional_text(value):
        return [text]
    return []


def _is_migration_task(files: list[str], context: str) -> bool:
    return _MIGRATION_RE.search(context) is not None or any(_is_migration_path(path) for path in files)


def _is_rollback_task(files: list[str], context: str) -> bool:
    return _ROLLBACK_RE.search(context) is not None or any(_is_rollback_path(path) for path in files)


def _is_ci_path(path: str) -> bool:
    folded = _normalized_path(path).casefold()
    return folded.startswith(".github/workflows/") or any(
        token in PurePosixPath(folded).parts for token in ("ci", ".circleci", ".buildkite")
    )


def _is_preview_path(path: str) -> bool:
    folded = _normalized_path(path).casefold()
    parts = set(PurePosixPath(folded).parts)
    return bool({"preview", "storybook", "vercel", "netlify"} & parts)


def _is_staging_path(path: str) -> bool:
    folded = _normalized_path(path).casefold()
    parts = set(PurePosixPath(folded).parts)
    return bool({"staging", "preprod", "pre-production", "uat"} & parts)


def _is_production_path(path: str) -> bool:
    folded = _normalized_path(path).casefold()
    parts = set(PurePosixPath(folded).parts)
    name = PurePosixPath(folded).name
    return bool({"production", "prod", "deploy", "release", "rollout"} & parts) or name.startswith(
        ("prod.", "production.")
    )


def _is_migration_path(path: str) -> bool:
    folded = _normalized_path(path).casefold()
    parts = set(PurePosixPath(folded).parts)
    name = PurePosixPath(folded).name
    return bool({"migrations", "migration", "alembic", "db", "database", "schema"} & parts) or any(
        token in name for token in ("migration", "migrate", "schema", "backfill")
    )


def _is_rollback_path(path: str) -> bool:
    folded = _normalized_path(path).casefold()
    name = PurePosixPath(folded).name
    parts = set(PurePosixPath(folded).parts)
    return bool({"rollback", "rollbacks", "recovery"} & parts) or any(
        token in name for token in ("rollback", "revert", "restore", "down")
    )


def _normalized_path(path: str) -> str:
    return path.replace("\\", "/").strip()


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
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
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped


def _dedupe_blockers(values: Iterable[PromotionBlocker]) -> list[PromotionBlocker]:
    seen: set[tuple[str, str, PromotionStageName]] = set()
    deduped: list[PromotionBlocker] = []
    for value in values:
        key = (value.task_id, value.blocked_by, value.blocker_stage)
        if key not in seen:
            deduped.append(value)
            seen.add(key)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")
