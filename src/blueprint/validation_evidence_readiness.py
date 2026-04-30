"""Score validation evidence readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


ReadinessGrade = Literal["ready", "partial", "insufficient"]
RiskLevel = Literal["low", "medium", "high"]
EvidenceCategory = Literal[
    "acceptance_criteria",
    "test_command",
    "validation_artifact",
    "risk_review",
    "completed_without_evidence",
]
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class TaskValidationEvidenceReadiness:
    """Validation evidence completeness for one execution task."""

    task_id: str
    title: str
    risk_level: RiskLevel
    status: str
    score: int
    grade: ReadinessGrade
    missing_evidence: tuple[EvidenceCategory, ...] = field(default_factory=tuple)
    suggested_next_evidence: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_level": self.risk_level,
            "status": self.status,
            "score": self.score,
            "grade": self.grade,
            "missing_evidence": list(self.missing_evidence),
            "suggested_next_evidence": list(self.suggested_next_evidence),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class ValidationEvidenceReadinessResult:
    """Plan-level validation evidence readiness result."""

    plan_id: str | None = None
    task_count: int = 0
    summary_counts: dict[str, int] = field(default_factory=dict)
    tasks: tuple[TaskValidationEvidenceReadiness, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "summary_counts": dict(self.summary_counts),
            "tasks": [task.to_dict() for task in self.tasks],
        }


def build_validation_evidence_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> ValidationEvidenceReadinessResult:
    """Score whether each task has enough validation evidence for review."""
    plan_id, tasks = _source_payload(source)
    records = tuple(_task_readiness(task, index) for index, task in enumerate(tasks, start=1))
    return ValidationEvidenceReadinessResult(
        plan_id=plan_id,
        task_count=len(records),
        summary_counts=_summary_counts(record.grade for record in records),
        tasks=records,
    )


def validation_evidence_readiness_to_dict(
    result: ValidationEvidenceReadinessResult,
) -> dict[str, Any]:
    """Serialize validation evidence readiness to a plain dictionary."""
    return result.to_dict()


validation_evidence_readiness_to_dict.__test__ = False


def _task_readiness(task: Mapping[str, Any], index: int) -> TaskValidationEvidenceReadiness:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    risk_level = _risk_level(task.get("risk_level") or task.get("risk"))
    status = (_optional_text(task.get("status")) or "pending").lower()
    evidence_by_category = _evidence_by_category(task)
    missing = tuple(_missing_evidence(risk_level, status, evidence_by_category))
    score = _score(evidence_by_category, missing, risk_level)
    grade = _grade(score, missing, risk_level)
    return TaskValidationEvidenceReadiness(
        task_id=task_id,
        title=title,
        risk_level=risk_level,
        status=status,
        score=score,
        grade=grade,
        missing_evidence=missing,
        suggested_next_evidence=tuple(_suggestions(missing, risk_level, title)),
        evidence=tuple(
            _dedupe(
                evidence
                for category in (
                    "acceptance_criteria",
                    "test_command",
                    "validation_artifact",
                    "risk_review",
                )
                for evidence in evidence_by_category.get(category, ())
            )
        ),
    )


def _evidence_by_category(task: Mapping[str, Any]) -> dict[EvidenceCategory, tuple[str, ...]]:
    evidence: dict[EvidenceCategory, list[str]] = {}
    acceptance = _strings(task.get("acceptance_criteria"))
    if acceptance:
        _extend_evidence(
            evidence,
            "acceptance_criteria",
            (
                f"acceptance_criteria[{index}]: {criterion}"
                for index, criterion in enumerate(acceptance)
            ),
        )

    commands = _validation_commands(task)
    if commands:
        _extend_evidence(
            evidence,
            "test_command",
            (f"validation command: {command}" for command in commands),
        )

    artifacts = _validation_artifacts(task)
    if artifacts:
        _extend_evidence(evidence, "validation_artifact", artifacts)

    risk_review = _risk_review_evidence(task)
    if risk_review:
        _extend_evidence(evidence, "risk_review", risk_review)

    return {category: tuple(_dedupe(values)) for category, values in evidence.items()}


def _missing_evidence(
    risk_level: RiskLevel,
    status: str,
    evidence: Mapping[EvidenceCategory, tuple[str, ...]],
) -> list[EvidenceCategory]:
    missing: list[EvidenceCategory] = []
    for category in ("acceptance_criteria", "test_command", "validation_artifact"):
        if category not in evidence:
            missing.append(category)
    if risk_level == "high" and "risk_review" not in evidence:
        missing.append("risk_review")
    if _is_completed(status) and not _has_validation_proof(evidence):
        missing.append("completed_without_evidence")
    return _dedupe(missing)


def _score(
    evidence: Mapping[EvidenceCategory, tuple[str, ...]],
    missing: tuple[EvidenceCategory, ...],
    risk_level: RiskLevel,
) -> int:
    score = 0
    if "acceptance_criteria" in evidence:
        score += 20
    if "test_command" in evidence:
        score += 30
    if "validation_artifact" in evidence:
        score += 35
    if "risk_review" in evidence:
        score += 15 if risk_level == "high" else 10
    if "completed_without_evidence" in missing:
        score = min(score, 30)
    return min(score, 100)


def _grade(
    score: int,
    missing: tuple[EvidenceCategory, ...],
    risk_level: RiskLevel,
) -> ReadinessGrade:
    ready_threshold = {"low": 80, "medium": 85, "high": 95}[risk_level]
    ready_blockers = set(missing) & {
        "test_command",
        "validation_artifact",
        "completed_without_evidence",
    }
    if risk_level == "high":
        ready_blockers.update(set(missing) & {"risk_review"})
    if score >= ready_threshold and not ready_blockers:
        return "ready"
    if score >= 50 and "completed_without_evidence" not in missing:
        return "partial"
    return "insufficient"


def _suggestions(
    missing: tuple[EvidenceCategory, ...],
    risk_level: RiskLevel,
    title: str,
) -> list[str]:
    suggestions: list[str] = []
    if "acceptance_criteria" in missing:
        suggestions.append(f"Add acceptance criteria that define observable completion for {title}.")
    if "test_command" in missing:
        suggestions.append(f"Name the focused validation command reviewers should trust for {title}.")
    if "validation_artifact" in missing:
        suggestions.append(
            f"Attach validation output, logs, screenshots, or artifact links for {title}."
        )
    if "risk_review" in missing:
        suggestions.append(
            f"Add reviewer, rollback, rollout, or risk mitigation evidence for high-risk {title}."
        )
    if "completed_without_evidence" in missing:
        suggestions.append(f"Reopen or block review for {title} until validation proof is recorded.")
    if risk_level == "high" and "risk_review" not in missing:
        suggestions.append(f"Keep high-risk evidence for {title} tied to reviewer-visible artifacts.")
    return _dedupe(suggestions)


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        command = _optional_text(task.get(key))
        if command:
            commands.append(command)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "validation_commands",
            "validation_command",
            "test_commands",
            "test_command",
        ):
            commands.extend(_commands_from_value(metadata.get(key)))
    return _dedupe(commands)


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    if isinstance(value, (list, tuple, set)):
        return _strings(value)
    command = _optional_text(value)
    return [command] if command else []


def _validation_artifacts(task: Mapping[str, Any]) -> list[str]:
    artifacts: list[str] = []
    for key in (
        "evidence",
        "completion_evidence",
        "validation_evidence",
        "artifacts",
        "validation_artifacts",
    ):
        artifacts.extend(_labeled_values(key, task.get(key)))

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "evidence",
            "completion_evidence",
            "validation_evidence",
            "artifacts",
            "validation_artifacts",
            "artifact_links",
            "screenshots",
            "logs",
        ):
            artifacts.extend(_labeled_values(f"metadata.{key}", metadata.get(key)))
    return _dedupe(artifacts)


def _risk_review_evidence(task: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    for key in (
        "review_notes",
        "reviewer_notes",
        "rollback_plan",
        "risk_mitigation",
        "rollout_notes",
    ):
        evidence.extend(_labeled_values(key, task.get(key)))

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "review_notes",
            "reviewer_notes",
            "approval",
            "approvals",
            "rollback_plan",
            "risk_mitigation",
            "rollout_notes",
        ):
            evidence.extend(_labeled_values(f"metadata.{key}", metadata.get(key)))
    return _dedupe(evidence)


def _labeled_values(label: str, value: Any) -> list[str]:
    values: list[str] = []
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            nested_label = f"{label}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                values.extend(_labeled_values(nested_label, child))
            elif (text := _optional_text(child)):
                values.append(f"{nested_label}: {text}")
        return values
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            nested_label = f"{label}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                values.extend(_labeled_values(nested_label, item))
            elif (text := _optional_text(item)):
                values.append(f"{nested_label}: {text}")
        return values
    text = _optional_text(value)
    return [f"{label}: {text}"] if text else []


def _has_validation_proof(evidence: Mapping[EvidenceCategory, tuple[str, ...]]) -> bool:
    return bool(evidence.get("test_command") or evidence.get("validation_artifact"))


def _is_completed(status: str) -> bool:
    return status in {"complete", "completed", "done", "merged", "closed"}


def _risk_level(value: Any) -> RiskLevel:
    text = (_optional_text(value) or "").lower()
    if text in {"critical", "blocker", "high"}:
        return "high"
    if text in {"medium", "moderate"}:
        return "medium"
    return "low"


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]

    tasks: list[dict[str, Any]] = []
    for item in source:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
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
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _summary_counts(values: Iterable[ReadinessGrade]) -> dict[str, int]:
    counts = {"ready": 0, "partial": 0, "insufficient": 0}
    for value in values:
        counts[value] += 1
    return counts


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


def _extend_evidence(
    evidence: dict[EvidenceCategory, list[str]],
    category: EvidenceCategory,
    values: Iterable[str],
) -> None:
    evidence.setdefault(category, []).extend(values)


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "TaskValidationEvidenceReadiness",
    "ValidationEvidenceReadinessResult",
    "build_validation_evidence_readiness",
    "validation_evidence_readiness_to_dict",
]
