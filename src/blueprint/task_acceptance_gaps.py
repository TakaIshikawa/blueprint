"""Suggest observable acceptance criteria for weak execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


Severity = Literal["high", "medium"]

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_DIMENSION_ORDER = (
    "acceptance_criteria",
    "observable_behavior",
    "validation_evidence",
    "rollback_or_risk_check",
)
_IMPLEMENTATION_VERBS = {
    "add",
    "build",
    "change",
    "configure",
    "create",
    "implement",
    "refactor",
    "remove",
    "rename",
    "replace",
    "update",
    "wire",
}
_OBSERVABLE_TERMS = {
    "assert",
    "confirm",
    "display",
    "emit",
    "fail",
    "log",
    "persist",
    "reject",
    "render",
    "return",
    "show",
    "verify",
    "write",
}
_VALIDATION_TERMS = {
    "assert",
    "check",
    "ci",
    "coverage",
    "e2e",
    "integration",
    "lint",
    "manual",
    "pass",
    "pytest",
    "regression",
    "smoke",
    "test",
    "validate",
    "verify",
}
_RISK_TERMS = {
    "fallback",
    "mitigate",
    "recover",
    "recovery",
    "rollback",
    "rollout",
    "safe",
}
_HIGH_RISK_LEVELS = {"high", "critical"}


@dataclass(frozen=True, slots=True)
class TaskAcceptanceGapFinding:
    """Acceptance criteria gaps detected for one execution task."""

    task_id: str
    title: str
    severity: Severity
    missing_dimensions: tuple[str, ...]
    suggested_criteria: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "severity": self.severity,
            "missing_dimensions": list(self.missing_dimensions),
            "suggested_criteria": list(self.suggested_criteria),
        }


def suggest_task_acceptance_gaps(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> tuple[TaskAcceptanceGapFinding, ...]:
    """Suggest stronger acceptance criteria for tasks with weak completion checks."""
    payload = _plan_payload(plan)
    findings: list[TaskAcceptanceGapFinding] = []

    for index, task in enumerate(_task_payloads(payload.get("tasks")), start=1):
        task_id = _task_id(task, index)
        title = _optional_text(task.get("title")) or task_id
        criteria = _string_list(task.get("acceptance_criteria"))
        missing_dimensions = _missing_dimensions(task, criteria)

        if not missing_dimensions:
            continue

        findings.append(
            TaskAcceptanceGapFinding(
                task_id=task_id,
                title=title,
                severity=_severity(missing_dimensions),
                missing_dimensions=missing_dimensions,
                suggested_criteria=_suggested_criteria(title, missing_dimensions),
            )
        )

    return tuple(findings)


def task_acceptance_gap_findings_to_dict(
    findings: tuple[TaskAcceptanceGapFinding, ...] | list[TaskAcceptanceGapFinding],
) -> list[dict[str, Any]]:
    """Serialize task acceptance gap findings to dictionaries."""
    return [finding.to_dict() for finding in findings]


task_acceptance_gap_findings_to_dict.__test__ = False


def _missing_dimensions(
    task: Mapping[str, Any],
    criteria: list[str],
) -> tuple[str, ...]:
    missing: set[str] = set()

    if not criteria:
        missing.update(
            {
                "acceptance_criteria",
                "observable_behavior",
                "validation_evidence",
            }
        )
    else:
        if not _has_observable_behavior(criteria) or _is_implementation_only(criteria):
            missing.add("observable_behavior")
        if not _has_validation_evidence(criteria):
            missing.add("validation_evidence")

    if _is_high_risk(task.get("risk_level") or task.get("risk")) and not _has_risk_check(
        criteria
    ):
        missing.add("rollback_or_risk_check")

    return tuple(dimension for dimension in _DIMENSION_ORDER if dimension in missing)


def _suggested_criteria(
    title: str,
    missing_dimensions: tuple[str, ...],
) -> tuple[str, ...]:
    suggestions: list[str] = []

    if "acceptance_criteria" in missing_dimensions:
        suggestions.append(f"Verify {title} produces the expected observable outcome.")
    elif "observable_behavior" in missing_dimensions:
        suggestions.append(f"Verify the user-visible or API behavior for {title}.")

    if "validation_evidence" in missing_dimensions:
        suggestions.append(f"Add test or validation evidence proving {title} is complete.")

    if "rollback_or_risk_check" in missing_dimensions:
        suggestions.append(
            f"Validate rollback, fallback, or risk mitigation for {title} before release."
        )

    return tuple(suggestions)


def _severity(missing_dimensions: tuple[str, ...]) -> Severity:
    if (
        "acceptance_criteria" in missing_dimensions
        or "observable_behavior" in missing_dimensions
        or "rollback_or_risk_check" in missing_dimensions
    ):
        return "high"
    return "medium"


def _has_observable_behavior(criteria: list[str]) -> bool:
    return any(_contains_token_family(criterion, _OBSERVABLE_TERMS) for criterion in criteria)


def _has_validation_evidence(criteria: list[str]) -> bool:
    return any(_contains_token_family(criterion, _VALIDATION_TERMS) for criterion in criteria)


def _has_risk_check(criteria: list[str]) -> bool:
    return any(_contains_token_family(criterion, _RISK_TERMS) for criterion in criteria)


def _is_implementation_only(criteria: list[str]) -> bool:
    return bool(criteria) and all(_is_implementation_step(criterion) for criterion in criteria)


def _is_implementation_step(criterion: str) -> bool:
    tokens = _tokens(criterion)
    if not tokens:
        return False
    non_implementation_terms = _OBSERVABLE_TERMS | _VALIDATION_TERMS | _RISK_TERMS
    if any(_matches_term(token, non_implementation_terms) for token in tokens):
        return False
    return any(_matches_term(token, _IMPLEMENTATION_VERBS) for token in tokens)


def _contains_token_family(value: str, terms: set[str]) -> bool:
    return any(_matches_term(token, terms) for token in _tokens(value))


def _matches_term(token: str, terms: set[str]) -> bool:
    if token in terms:
        return True
    for term in terms:
        if token.startswith(term) and len(token) >= len(term) + 1:
            return True
    return False


def _tokens(value: str) -> list[str]:
    return _TOKEN_RE.findall(value.lower())


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


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _is_high_risk(value: Any) -> bool:
    return isinstance(value, str) and value.strip().lower() in _HIGH_RISK_LEVELS


__all__ = [
    "TaskAcceptanceGapFinding",
    "suggest_task_acceptance_gaps",
    "task_acceptance_gap_findings_to_dict",
]
