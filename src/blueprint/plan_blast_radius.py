"""Estimate implementation blast radius for execution plans."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


@dataclass(frozen=True, slots=True)
class TaskBlastRadius:
    """Blast-radius estimate for one execution task."""

    task_id: str
    title: str
    score: int
    severity: str
    reasons: tuple[str, ...] = field(default_factory=tuple)
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    file_paths: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "score": self.score,
            "severity": self.severity,
            "reasons": list(self.reasons),
            "affected_task_ids": list(self.affected_task_ids),
            "file_paths": list(self.file_paths),
        }


@dataclass(frozen=True, slots=True)
class PlanBlastRadius:
    """Plan-level blast-radius summary."""

    plan_id: str | None
    aggregate_score: int
    severity: str
    reasons: tuple[str, ...]
    affected_task_ids: tuple[str, ...]
    task_summaries: tuple[TaskBlastRadius, ...]
    severity_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "aggregate_score": self.aggregate_score,
            "severity": self.severity,
            "reasons": list(self.reasons),
            "affected_task_ids": list(self.affected_task_ids),
            "task_summaries": [summary.to_dict() for summary in self.task_summaries],
            "severity_counts": dict(self.severity_counts),
        }


def estimate_plan_blast_radius(plan: Mapping[str, Any] | ExecutionPlan) -> PlanBlastRadius:
    """Return deterministic task and plan blast-radius summaries."""
    payload = _plan_payload(plan)
    tasks = _task_records(_task_payloads(payload.get("tasks")))
    dependents_by_id = _dependents_by_id(tasks)
    milestones_by_id = {
        record["task_id"]: _text(record["task"].get("milestone")) for record in tasks
    }

    task_summaries = tuple(
        _estimate_task(
            record,
            dependents_by_id=dependents_by_id,
            milestones_by_id=milestones_by_id,
        )
        for record in tasks
    )
    plan_reasons = _plan_reasons(payload, task_summaries)
    aggregate_score = _aggregate_score(task_summaries, plan_reasons)
    severity = _severity(aggregate_score)
    affected_task_ids = tuple(
        summary.task_id for summary in task_summaries if summary.severity != "low"
    )

    return PlanBlastRadius(
        plan_id=_optional_text(payload.get("id")),
        aggregate_score=aggregate_score,
        severity=severity,
        reasons=tuple(plan_reasons),
        affected_task_ids=affected_task_ids,
        task_summaries=task_summaries,
        severity_counts=_severity_counts(task_summaries),
    )


def plan_blast_radius_to_dict(summary: PlanBlastRadius) -> dict[str, Any]:
    """Serialize a plan blast-radius estimate to a dictionary."""
    return summary.to_dict()


def _estimate_task(
    record: dict[str, Any],
    *,
    dependents_by_id: dict[str, list[str]],
    milestones_by_id: dict[str, str],
) -> TaskBlastRadius:
    task = record["task"]
    task_id = record["task_id"]
    title = record["title"]
    paths = tuple(_task_paths(task))
    reasons: list[str] = []
    score = 1

    if not paths:
        score += 1
        reasons.append("missing file path scope")

    if len(paths) >= 3:
        score += min(3, len(paths) - 2)
        reasons.append(f"touches {len(paths)} file paths")

    for path in paths:
        for category, weight in _path_categories(path):
            score += weight
            reasons.append(f"{category} path: {path}")

    risk_level = _text(task.get("risk_level")).lower()
    if risk_level in _RISK_LEVEL_POINTS:
        score += _RISK_LEVEL_POINTS[risk_level]
        reasons.append(f"{risk_level} risk task metadata")

    metadata = task.get("metadata")
    metadata_level = _metadata_blast_radius(metadata)
    if metadata_level:
        score += _METADATA_BLAST_RADIUS_POINTS[metadata_level]
        reasons.append(f"{metadata_level} blast radius metadata")

    dependencies = _string_list(task.get("depends_on"))
    if dependencies:
        score += min(3, len(dependencies))
        reasons.append(f"dependency fan-out to {len(dependencies)} task(s)")

    dependents = dependents_by_id.get(task_id, [])
    if dependents:
        score += min(4, len(dependents) * 2)
        reasons.append(f"dependency fan-in from {len(dependents)} task(s)")

    cross_milestone_count = _cross_milestone_count(
        task_id, dependencies, dependents, milestones_by_id
    )
    if cross_milestone_count:
        score += min(2, cross_milestone_count)
        reasons.append(f"cross-milestone dependency reach to {cross_milestone_count} task(s)")

    if _is_broad_validation_command(task.get("test_command")):
        score += 2
        reasons.append("broad validation command")

    return TaskBlastRadius(
        task_id=task_id,
        title=title,
        score=score,
        severity=_severity(score),
        reasons=tuple(_dedupe(reasons)),
        affected_task_ids=tuple(_dedupe([*dependencies, *dependents])),
        file_paths=paths,
    )


def _plan_reasons(
    payload: dict[str, Any], task_summaries: tuple[TaskBlastRadius, ...]
) -> list[str]:
    reasons: list[str] = []
    milestones = {_text(task.get("milestone")) for task in _task_payloads(payload.get("tasks"))}
    milestones.discard("")
    if len(milestones) >= 3:
        reasons.append(f"plan spans {len(milestones)} milestones")

    elevated = [summary for summary in task_summaries if summary.severity in {"high", "critical"}]
    if elevated:
        reasons.append(f"{len(elevated)} task(s) have high or critical blast radius")

    if _is_broad_validation_command(payload.get("test_strategy")):
        reasons.append("plan validation strategy is broad")

    return reasons


def _aggregate_score(task_summaries: tuple[TaskBlastRadius, ...], plan_reasons: list[str]) -> int:
    if not task_summaries:
        return 0
    max_score = max(summary.score for summary in task_summaries)
    average_score = sum(summary.score for summary in task_summaries) / len(task_summaries)
    return round((max_score * 0.7) + (average_score * 0.3) + len(plan_reasons))


def _task_records(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        records.append(
            {
                "task": task,
                "task_id": task_id,
                "title": _optional_text(task.get("title")) or task_id,
            }
        )
    return records


def _dependents_by_id(records: list[dict[str, Any]]) -> dict[str, list[str]]:
    known_ids = {record["task_id"] for record in records}
    dependents: dict[str, list[str]] = {task_id: [] for task_id in known_ids}
    for record in records:
        task_id = record["task_id"]
        for dependency_id in _string_list(record["task"].get("depends_on")):
            if dependency_id in known_ids:
                dependents.setdefault(dependency_id, []).append(task_id)
    return dependents


def _task_paths(task: Mapping[str, Any]) -> list[str]:
    paths = [
        *_strings(task.get("files_or_modules")),
        *_strings(task.get("expectedFiles")),
        *_strings(task.get("expected_files")),
    ]
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        paths.extend(_strings(metadata.get("expectedFiles")))
        paths.extend(_strings(metadata.get("expected_files")))
        paths.extend(_strings(metadata.get("files")))
    return _dedupe([_normalized_path(path) for path in paths])


def _path_categories(path: str) -> list[tuple[str, int]]:
    normalized = path.lower()
    path_obj = PurePosixPath(normalized)
    parts = set(path_obj.parts)
    categories: list[tuple[str, int]] = []

    if path_obj.name in _CONFIG_FILENAMES or path_obj.suffix in _CONFIG_SUFFIXES:
        categories.append(("config", 3))
    if parts & _SCHEMA_PARTS or _has_token(normalized, _SCHEMA_TOKENS):
        categories.append(("schema", 3))
    if parts & _DATABASE_PARTS or _has_token(normalized, _DATABASE_TOKENS):
        categories.append(("database", 4))
    if parts & _CLI_PARTS or path_obj.name in _CLI_FILENAMES:
        categories.append(("CLI", 2))
    if "export" in normalized or parts & _EXPORTER_PARTS:
        categories.append(("exporter registry", 2))
    if parts & _SHARED_PARTS:
        categories.append(("shared infrastructure", 2))

    return categories


def _cross_milestone_count(
    task_id: str,
    dependencies: list[str],
    dependents: list[str],
    milestones_by_id: dict[str, str],
) -> int:
    milestone = milestones_by_id.get(task_id, "")
    if not milestone:
        return 0
    linked_ids = [*dependencies, *dependents]
    return sum(
        1
        for linked_id in linked_ids
        if milestones_by_id.get(linked_id, "") and milestones_by_id.get(linked_id) != milestone
    )


def _metadata_blast_radius(value: Any) -> str:
    if not isinstance(value, Mapping):
        return ""
    for key in ("blast_radius", "blastRadius", "impact", "impact_level"):
        level = _text(value.get(key)).lower()
        if level in _METADATA_BLAST_RADIUS_POINTS:
            return level
    return ""


def _is_broad_validation_command(value: Any) -> bool:
    command = _text(value).lower()
    if not command:
        return False
    if any(token in command for token in _FOCUSED_VALIDATION_TOKENS):
        return False
    return any(re.search(pattern, command) for pattern in _BROAD_VALIDATION_PATTERNS)


def _severity(score: int) -> str:
    if score >= 12:
        return "critical"
    if score >= 8:
        return "high"
    if score >= 4:
        return "medium"
    return "low"


def _severity_counts(task_summaries: tuple[TaskBlastRadius, ...]) -> dict[str, int]:
    counts = Counter(summary.severity for summary in task_summaries)
    return {severity: counts.get(severity, 0) for severity in ("low", "medium", "high", "critical")}


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


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


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _text(value)
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        return [text for item in items if (text := _text(item))]
    return []


def _string_list(value: Any) -> list[str]:
    return _strings(value)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if isinstance(value, str):
        return " ".join(value.split())
    return ""


def _normalized_path(value: str) -> str:
    return value.strip().replace("\\", "/").strip("/")


def _has_token(value: str, tokens: set[str]) -> bool:
    return bool(set(_TOKEN_RE.findall(value)) & tokens)


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9-]*")
_CONFIG_FILENAMES = {
    ".env",
    ".env.local",
    ".env.production",
    "dockerfile",
    "package.json",
    "pyproject.toml",
    "settings.py",
}
_CONFIG_SUFFIXES = (".cfg", ".conf", ".env", ".ini", ".json", ".toml", ".yaml", ".yml")
_SCHEMA_PARTS = {"schema", "schemas"}
_SCHEMA_TOKENS = {"schema", "schemas", "graphql", "openapi", "proto", "protobuf"}
_DATABASE_PARTS = {"db", "database", "databases", "migration", "migrations", "sql"}
_DATABASE_TOKENS = {"database", "migration", "migrations", "sql", "sqlite"}
_CLI_PARTS = {"cli", "commands"}
_CLI_FILENAMES = {"cli.py", "command.py", "commands.py"}
_EXPORTER_PARTS = {"exporter", "exporters", "exports"}
_SHARED_PARTS = {"common", "core", "infra", "infrastructure", "shared"}
_RISK_LEVEL_POINTS = {"medium": 1, "high": 3, "critical": 5}
_METADATA_BLAST_RADIUS_POINTS = {"medium": 1, "high": 3, "critical": 5}
_BROAD_VALIDATION_PATTERNS = (
    r"\bpytest\b$",
    r"\bpytest\s+-?q\b$",
    r"\bpoetry\s+run\s+pytest\b$",
    r"\bnpm\s+(run\s+)?test\b$",
    r"\byarn\s+test\b$",
    r"\bpnpm\s+test\b$",
    r"\bmake\s+test\b$",
)
_FOCUSED_VALIDATION_TOKENS = (" tests/", " tests\\", " -k ", "::", "test_")


__all__ = [
    "PlanBlastRadius",
    "TaskBlastRadius",
    "estimate_plan_blast_radius",
    "plan_blast_radius_to_dict",
]
