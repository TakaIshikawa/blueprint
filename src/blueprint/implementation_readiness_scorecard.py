"""Compute an implementation-plan readiness scorecard."""

from __future__ import annotations

from dataclasses import dataclass, field
from numbers import Real
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


ReadinessStatus = Literal["ready", "needs_attention", "blocked"]
FindingSeverity = Literal["info", "warning", "blocking"]
ReadinessCategory = Literal[
    "task_completeness",
    "dependency_clarity",
    "validation_coverage",
    "acceptance_criteria_quality",
    "blocker_status",
    "ownership_coverage",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CATEGORY_WEIGHTS: dict[ReadinessCategory, float] = {
    "task_completeness": 0.20,
    "dependency_clarity": 0.15,
    "validation_coverage": 0.20,
    "acceptance_criteria_quality": 0.15,
    "blocker_status": 0.15,
    "ownership_coverage": 0.15,
}
_SEVERE_BLOCKER_RISKS = {"high", "critical", "blocker", "severe"}
_WEAK_ACCEPTANCE_TEXT = {
    "done",
    "works",
    "complete",
    "completed",
    "implemented",
    "tests pass",
    "test passes",
}


@dataclass(frozen=True, slots=True)
class ReadinessFinding:
    """Actionable issue or note contributing to the readiness score."""

    category: ReadinessCategory
    severity: FindingSeverity
    message: str
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    recommendation: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "severity": self.severity,
            "message": self.message,
            "task_ids": list(self.task_ids),
            "recommendation": self.recommendation,
        }


@dataclass(frozen=True, slots=True)
class ReadinessCategoryScore:
    """Weighted category score used in the aggregate readiness calculation."""

    category: ReadinessCategory
    score: int
    weight: float
    weighted_score: float

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "score": self.score,
            "weight": self.weight,
            "weighted_score": self.weighted_score,
        }


@dataclass(frozen=True, slots=True)
class ImplementationReadinessScorecard:
    """Plan-level implementation readiness scorecard."""

    plan_id: str | None = None
    status: ReadinessStatus = "needs_attention"
    overall_score: int = 0
    category_scores: tuple[ReadinessCategoryScore, ...] = field(default_factory=tuple)
    findings: tuple[ReadinessFinding, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "status": self.status,
            "overall_score": self.overall_score,
            "category_scores": [score.to_dict() for score in self.category_scores],
            "findings": [finding.to_dict() for finding in self.findings],
            "summary": dict(self.summary),
        }


def build_implementation_readiness_scorecard(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
) -> ImplementationReadinessScorecard:
    """Compute a deterministic readiness scorecard for an implementation plan."""
    plan_id, plan_payload, tasks = _source_payload(source)
    records = tuple(_task_record(task, index) for index, task in enumerate(tasks, start=1))
    known_task_ids = {record["task_id"] for record in records}
    findings: list[ReadinessFinding] = []
    raw_scores = {
        "task_completeness": _task_completeness(records, findings),
        "dependency_clarity": _dependency_clarity(records, known_task_ids, findings),
        "validation_coverage": _validation_coverage(plan_payload, records, findings),
        "acceptance_criteria_quality": _acceptance_criteria_quality(records, findings),
        "blocker_status": _blocker_status(records, findings),
        "ownership_coverage": _ownership_coverage(records, findings),
    }
    category_scores = tuple(
        ReadinessCategoryScore(
            category=category,
            score=score,
            weight=_CATEGORY_WEIGHTS[category],
            weighted_score=_stable_number(score * _CATEGORY_WEIGHTS[category]),
        )
        for category, score in raw_scores.items()
    )
    overall_score = int(round(sum(score.weighted_score for score in category_scores)))
    status = _status(overall_score, findings, raw_scores["blocker_status"])

    return ImplementationReadinessScorecard(
        plan_id=plan_id,
        status=status,
        overall_score=overall_score,
        category_scores=category_scores,
        findings=tuple(findings),
        summary={
            "task_count": len(records),
            "blocked_task_count": sum(1 for record in records if record["is_blocked"]),
            "severe_blocked_task_count": sum(
                1 for record in records if record["is_severe_blocked"]
            ),
            "owned_task_count": sum(1 for record in records if record["owner"]),
            "validated_task_count": sum(1 for record in records if record["validation_commands"]),
            "acceptance_criteria_count": sum(
                len(record["acceptance_criteria"]) for record in records
            ),
            "category_weights": dict(_CATEGORY_WEIGHTS),
        },
    )


def implementation_readiness_scorecard_to_dict(
    result: ImplementationReadinessScorecard,
) -> dict[str, Any]:
    """Serialize an implementation readiness scorecard to a plain dictionary."""
    return result.to_dict()


implementation_readiness_scorecard_to_dict.__test__ = False


def score_implementation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
) -> ImplementationReadinessScorecard:
    """Compatibility alias for building an implementation readiness scorecard."""
    return build_implementation_readiness_scorecard(source)


def _task_completeness(
    records: tuple[dict[str, Any], ...],
    findings: list[ReadinessFinding],
) -> int:
    if not records:
        findings.append(
            ReadinessFinding(
                category="task_completeness",
                severity="blocking",
                message="Plan has no implementation tasks.",
                recommendation="Add executable tasks before dispatching implementation agents.",
            )
        )
        return 0

    missing_title = [record["task_id"] for record in records if not record["title"]]
    missing_description = [record["task_id"] for record in records if not record["description"]]
    missing_scope = [record["task_id"] for record in records if not record["files_or_modules"]]
    missing_estimate = [record["task_id"] for record in records if record["estimate_missing"]]
    penalty = (
        len(missing_title) * 25
        + len(missing_description) * 25
        + len(missing_scope) * 15
        + len(missing_estimate) * 10
    )
    score = _bounded_score(100 - round(penalty / len(records)))
    if missing_title or missing_description or missing_scope or missing_estimate:
        findings.append(
            ReadinessFinding(
                category="task_completeness",
                severity="warning",
                message="Some tasks are missing dispatch details.",
                task_ids=tuple(_dedupe(missing_title + missing_description + missing_scope + missing_estimate)),
                recommendation="Fill in titles, descriptions, affected files or modules, and effort estimates.",
            )
        )
    return score


def _dependency_clarity(
    records: tuple[dict[str, Any], ...],
    known_task_ids: set[str],
    findings: list[ReadinessFinding],
) -> int:
    if not records:
        return 0
    unknown_dependency_tasks: list[str] = []
    blocked_dependency_tasks: list[str] = []
    blocked_task_ids = {
        record["task_id"] for record in records if record["is_blocked"]
    }
    for record in records:
        dependencies = record["depends_on"]
        unknown = [dependency for dependency in dependencies if dependency not in known_task_ids]
        blocked = [dependency for dependency in dependencies if dependency in blocked_task_ids]
        if unknown:
            unknown_dependency_tasks.append(record["task_id"])
        if blocked:
            blocked_dependency_tasks.append(record["task_id"])

    penalty = (len(unknown_dependency_tasks) * 35) + (len(blocked_dependency_tasks) * 25)
    score = _bounded_score(100 - round(penalty / len(records)))
    if unknown_dependency_tasks:
        findings.append(
            ReadinessFinding(
                category="dependency_clarity",
                severity="blocking",
                message="Some tasks depend on task ids that are not present in the plan.",
                task_ids=tuple(unknown_dependency_tasks),
                recommendation="Correct dependency ids or add the missing prerequisite tasks.",
            )
        )
    if blocked_dependency_tasks:
        findings.append(
            ReadinessFinding(
                category="dependency_clarity",
                severity="warning",
                message="Some tasks depend on currently blocked work.",
                task_ids=tuple(blocked_dependency_tasks),
                recommendation="Resolve blockers or split these tasks so ready work can proceed.",
            )
        )
    return score


def _validation_coverage(
    plan_payload: Mapping[str, Any],
    records: tuple[dict[str, Any], ...],
    findings: list[ReadinessFinding],
) -> int:
    if not records:
        return 0
    tasks_with_validation = [record for record in records if record["validation_commands"]]
    plan_has_strategy = bool(_optional_text(plan_payload.get("test_strategy"))) or bool(
        _plan_validation_commands(plan_payload)
    )
    score = round((len(tasks_with_validation) / len(records)) * 100)
    if plan_has_strategy:
        score = min(100, score + 10)
    score = _bounded_score(score)
    if score < 100:
        missing = [record["task_id"] for record in records if not record["validation_commands"]]
        findings.append(
            ReadinessFinding(
                category="validation_coverage",
                severity="warning" if score >= 60 else "blocking",
                message="Validation coverage is incomplete across implementation tasks.",
                task_ids=tuple(missing),
                recommendation="Add focused test or validation commands for every task.",
            )
        )
    return score


def _acceptance_criteria_quality(
    records: tuple[dict[str, Any], ...],
    findings: list[ReadinessFinding],
) -> int:
    if not records:
        return 0
    weak_task_ids: list[str] = []
    task_scores: list[int] = []
    for record in records:
        criteria = record["acceptance_criteria"]
        if not criteria:
            task_scores.append(0)
            weak_task_ids.append(record["task_id"])
            continue
        score = 60
        if len(criteria) >= 2:
            score += 20
        if all(_criterion_is_specific(criterion) for criterion in criteria):
            score += 20
        else:
            weak_task_ids.append(record["task_id"])
        task_scores.append(score)
    score = _bounded_score(round(sum(task_scores) / len(task_scores)))
    if weak_task_ids:
        findings.append(
            ReadinessFinding(
                category="acceptance_criteria_quality",
                severity="warning",
                message="Some tasks have missing or weak acceptance criteria.",
                task_ids=tuple(weak_task_ids),
                recommendation="Add observable outcomes, validation evidence, or user-visible behavior to each criterion.",
            )
        )
    return score


def _blocker_status(
    records: tuple[dict[str, Any], ...],
    findings: list[ReadinessFinding],
) -> int:
    if not records:
        return 0
    blocked = [record for record in records if record["is_blocked"]]
    if not blocked:
        return 100
    severe = [record for record in blocked if record["is_severe_blocked"]]
    findings.append(
        ReadinessFinding(
            category="blocker_status",
            severity="blocking" if severe else "warning",
            message="Blocked tasks are present in the implementation plan.",
            task_ids=tuple(record["task_id"] for record in blocked),
            recommendation="Resolve blocker reasons before dispatching dependent or high-risk tasks.",
        )
    )
    if severe:
        return 0
    return _bounded_score(100 - round((60 * len(blocked)) / len(records)))


def _ownership_coverage(
    records: tuple[dict[str, Any], ...],
    findings: list[ReadinessFinding],
) -> int:
    if not records:
        return 0
    missing = [record["task_id"] for record in records if not record["owner"]]
    score = _bounded_score(round(((len(records) - len(missing)) / len(records)) * 100))
    if missing:
        findings.append(
            ReadinessFinding(
                category="ownership_coverage",
                severity="warning" if score >= 50 else "blocking",
                message="Some tasks do not have an implementation owner.",
                task_ids=tuple(missing),
                recommendation="Assign owner, owner_type, assignee, or metadata owner before dispatch.",
            )
        )
    return score


def _status(
    overall_score: int,
    findings: list[ReadinessFinding],
    blocker_score: int,
) -> ReadinessStatus:
    if blocker_score == 0 or any(finding.severity == "blocking" for finding in findings):
        return "blocked"
    if blocker_score < 100:
        return "needs_attention"
    if overall_score >= 85:
        return "ready"
    return "needs_attention"


def _task_record(task: Mapping[str, Any], index: int) -> dict[str, Any]:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    status = (_optional_text(task.get("status")) or "pending").casefold()
    risk_level = _risk_level(task)
    effort = _non_negative_number(
        task.get("estimated_hours")
        or task.get("estimate")
        or task.get("effort")
        or _metadata_value(task.get("metadata"), "estimated_hours")
        or _metadata_value(task.get("metadata"), "estimate")
        or _metadata_value(task.get("metadata"), "effort")
    )
    complexity = _optional_text(
        task.get("estimated_complexity")
        or task.get("complexity")
        or _metadata_value(task.get("metadata"), "estimated_complexity")
        or _metadata_value(task.get("metadata"), "complexity")
    )
    validation_commands = _validation_commands(task)
    return {
        "task_id": task_id,
        "title": _optional_text(task.get("title")),
        "description": _optional_text(task.get("description")),
        "files_or_modules": _strings(task.get("files_or_modules") or task.get("files")),
        "owner": _owner(task),
        "depends_on": _strings(task.get("depends_on")),
        "acceptance_criteria": _strings(task.get("acceptance_criteria")),
        "validation_commands": validation_commands,
        "status": status,
        "risk_level": risk_level,
        "is_blocked": status == "blocked" or bool(_optional_text(task.get("blocked_reason"))),
        "is_severe_blocked": (
            status == "blocked" or bool(_optional_text(task.get("blocked_reason")))
        )
        and risk_level in _SEVERE_BLOCKER_RISKS,
        "estimate_missing": effort is None and not complexity,
    }


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        if command := _optional_text(task.get(key)):
            commands.append(command)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "validation_commands",
            "validation_command",
            "test_commands",
            "test_command",
        ):
            value = metadata.get(key)
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


def _plan_validation_commands(plan_payload: Mapping[str, Any]) -> list[str]:
    metadata = plan_payload.get("metadata")
    if isinstance(metadata, Mapping):
        value = metadata.get("validation_commands") or metadata.get("test_commands")
        if isinstance(value, Mapping):
            return flatten_validation_commands(value)
        return _strings(value)
    return []


def _criterion_is_specific(value: str) -> bool:
    text = value.casefold().strip().strip(".")
    if text in _WEAK_ACCEPTANCE_TEXT:
        return False
    return len(text.split()) >= 4 or any(
        token in text
        for token in (
            "when ",
            "then ",
            "given ",
            "verify",
            "assert",
            "shows",
            "returns",
            "records",
            "prevents",
            "fails",
            "passes",
        )
    )


def _owner(task: Mapping[str, Any]) -> str | None:
    metadata = task.get("metadata")
    value = (
        task.get("owner")
        or task.get("owner_id")
        or task.get("assignee")
        or _metadata_value(metadata, "owner")
        or _metadata_value(metadata, "owner_id")
        or _metadata_value(metadata, "assignee")
        or _first_string(task.get("assignees"))
        or _first_string(_metadata_value(metadata, "assignees"))
        or task.get("owner_type")
        or _metadata_value(metadata, "owner_type")
    )
    return _optional_text(value)


def _risk_level(task: Mapping[str, Any]) -> str:
    metadata = task.get("metadata")
    value = (
        task.get("risk_level")
        or task.get("risk")
        or _metadata_value(metadata, "risk_level")
        or _metadata_value(metadata, "risk")
    )
    return (_optional_text(value) or "unspecified").casefold()


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
) -> tuple[str | None, dict[str, Any], list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, {}, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return _optional_text(source.id), payload, [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), payload, _task_payloads(payload.get("tasks"))
        return None, {}, [dict(source)]
    if hasattr(source, "tasks"):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), payload, _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)
    except TypeError:
        task = _task_like_payload(source)
        return (None, {}, [task]) if task else (None, {}, [])

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_like_payload(item):
            tasks.append(task)
    return None, {}, tasks


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
        if task := _task_like_payload(item):
            tasks.append(task)
    return tasks


def _task_like_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if hasattr(value, "dict"):
        task = value.dict()
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
        "owner",
        "owner_id",
        "owner_type",
        "assignee",
        "suggested_engine",
        "risk_level",
        "test_command",
        "validation_command",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "depends_on",
        "status",
        "blocked_reason",
        "estimated_hours",
        "estimated_complexity",
        "metadata",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _metadata_value(value: Any, key: str) -> Any:
    return value.get(key) if isinstance(value, Mapping) else None


def _first_string(value: Any) -> str | None:
    values = _strings(value)
    return values[0] if values else None


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


def _non_negative_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, Real):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    return number if number >= 0 else None


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _bounded_score(value: int) -> int:
    return max(0, min(100, value))


def _stable_number(value: float) -> float:
    rounded = round(value, 6)
    if rounded == 0:
        return 0.0
    return rounded


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "FindingSeverity",
    "ImplementationReadinessScorecard",
    "ReadinessCategory",
    "ReadinessCategoryScore",
    "ReadinessFinding",
    "ReadinessStatus",
    "build_implementation_readiness_scorecard",
    "implementation_readiness_scorecard_to_dict",
    "score_implementation_readiness",
]
