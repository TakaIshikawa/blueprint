"""Autonomous-agent task prompt budget audit."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any, Literal

from blueprint.llm.estimator import count_words, estimate_tokens


DEFAULT_WARNING_TOKEN_THRESHOLD = 6_000
DEFAULT_ERROR_TOKEN_THRESHOLD = 8_000
DEFAULT_LARGEST_TASK_LIMIT = 5

Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class TaskPromptBudgetEstimate:
    """Deterministic prompt size estimate for one execution task."""

    task_id: str
    title: str
    estimated_tokens: int
    characters: int
    words: int
    field_token_estimates: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "estimated_tokens": self.estimated_tokens,
            "characters": self.characters,
            "words": self.words,
            "field_token_estimates": self.field_token_estimates,
        }


@dataclass(frozen=True)
class TaskPromptBudgetFinding:
    """A task whose autonomous-agent prompt estimate crosses a budget threshold."""

    severity: Severity
    code: str
    task_id: str
    title: str
    estimated_tokens: int
    threshold: int
    largest_fields: list[str] = field(default_factory=list)
    message: str = ""
    remediation: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "task_id": self.task_id,
            "title": self.title,
            "estimated_tokens": self.estimated_tokens,
            "threshold": self.threshold,
            "largest_fields": self.largest_fields,
            "message": self.message,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class TaskPromptBudgetResult:
    """Task prompt budget audit result for an execution plan."""

    plan_id: str
    total_tasks_checked: int
    warning_threshold: int
    error_threshold: int
    findings: list[TaskPromptBudgetFinding] = field(default_factory=list)
    largest_tasks: list[TaskPromptBudgetEstimate] = field(default_factory=list)
    suggested_remediation: str = (
        "Split oversized tasks by file/module group or acceptance-criteria cluster, "
        "move background detail into linked source material, and keep the handoff "
        "prompt focused on the immediate implementation surface and validation command."
    )

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "warning")

    @property
    def error_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "error")

    @property
    def ok(self) -> bool:
        return self.error_count == 0

    @property
    def largest_task_ids(self) -> list[str]:
        return [estimate.task_id for estimate in self.largest_tasks]

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "ok": self.ok,
            "summary": {
                "total_tasks_checked": self.total_tasks_checked,
                "warnings": self.warning_count,
                "errors": self.error_count,
                "warning_threshold": self.warning_threshold,
                "error_threshold": self.error_threshold,
                "largest_task_ids": self.largest_task_ids,
            },
            "suggested_remediation": self.suggested_remediation,
            "largest_tasks": [estimate.to_dict() for estimate in self.largest_tasks],
            "findings": [finding.to_dict() for finding in self.findings],
        }


def audit_task_prompt_budget(
    plan: dict[str, Any],
    *,
    warning_threshold: int = DEFAULT_WARNING_TOKEN_THRESHOLD,
    error_threshold: int = DEFAULT_ERROR_TOKEN_THRESHOLD,
    largest_task_limit: int = DEFAULT_LARGEST_TASK_LIMIT,
) -> TaskPromptBudgetResult:
    """Flag tasks whose deterministic handoff prompt estimate exceeds thresholds."""
    _validate_thresholds(warning_threshold, error_threshold, largest_task_limit)
    tasks = [_task_mapping(task) for task in _task_items(plan)]
    estimates = [_estimate_task_prompt(task) for task in tasks]
    largest_tasks = sorted(
        estimates,
        key=lambda estimate: (-estimate.estimated_tokens, estimate.task_id),
    )[:largest_task_limit]

    return TaskPromptBudgetResult(
        plan_id=str(plan.get("id") or ""),
        total_tasks_checked=len(estimates),
        warning_threshold=warning_threshold,
        error_threshold=error_threshold,
        findings=_findings(estimates, warning_threshold, error_threshold),
        largest_tasks=largest_tasks,
    )


def estimate_task_prompt(task: Any) -> TaskPromptBudgetEstimate:
    """Estimate the autonomous-agent prompt size for one task without calling an LLM."""
    return _estimate_task_prompt(_task_mapping(task))


def build_task_prompt_budget_text(task: Any) -> str:
    """Return the deterministic text used for prompt budget estimation."""
    return _task_prompt_text(_task_mapping(task))


def _findings(
    estimates: list[TaskPromptBudgetEstimate],
    warning_threshold: int,
    error_threshold: int,
) -> list[TaskPromptBudgetFinding]:
    findings: list[TaskPromptBudgetFinding] = []
    for estimate in sorted(
        estimates,
        key=lambda item: (-item.estimated_tokens, item.task_id),
    ):
        if estimate.estimated_tokens >= error_threshold:
            severity: Severity = "error"
            threshold = error_threshold
            code = "task_prompt_budget_error"
        elif estimate.estimated_tokens >= warning_threshold:
            severity = "warning"
            threshold = warning_threshold
            code = "task_prompt_budget_warning"
        else:
            continue

        findings.append(
            TaskPromptBudgetFinding(
                severity=severity,
                code=code,
                task_id=estimate.task_id,
                title=estimate.title,
                estimated_tokens=estimate.estimated_tokens,
                threshold=threshold,
                largest_fields=_largest_fields(estimate.field_token_estimates),
                message=(
                    f"Task {estimate.task_id} is estimated at "
                    f"{estimate.estimated_tokens} tokens, crossing the "
                    f"{severity} threshold of {threshold} tokens."
                ),
                remediation=(
                    "Split or trim this task before autonomous handoff. Start with "
                    f"the largest prompt fields: "
                    f"{', '.join(_largest_fields(estimate.field_token_estimates))}."
                ),
            )
        )
    return findings


def _estimate_task_prompt(task: dict[str, Any]) -> TaskPromptBudgetEstimate:
    prompt_text = _task_prompt_text(task)
    return TaskPromptBudgetEstimate(
        task_id=str(task.get("id") or ""),
        title=str(task.get("title") or ""),
        estimated_tokens=estimate_tokens(prompt_text),
        characters=len(prompt_text),
        words=count_words(prompt_text),
        field_token_estimates={
            field_name: (
                estimate_tokens(_render_field(field_name, task.get(field_name)))
                if _has_budget_value(task.get(field_name))
                else 0
            )
            for field_name in _BUDGETED_FIELDS
        },
    )


_BUDGETED_FIELDS = (
    "description",
    "acceptance_criteria",
    "files_or_modules",
    "metadata",
    "test_command",
)


def _task_prompt_text(task: dict[str, Any]) -> str:
    sections = [
        _render_field(field_name, task.get(field_name))
        for field_name in _BUDGETED_FIELDS
        if _has_budget_value(task.get(field_name))
    ]
    return "\n\n".join(sections)


def _render_field(field_name: str, value: Any) -> str:
    label = field_name.replace("_", " ").title()
    if isinstance(value, str):
        rendered_value = value.strip()
    else:
        rendered_value = json.dumps(
            _json_safe(value),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    return f"{label}:\n{rendered_value}"


def _largest_fields(field_token_estimates: dict[str, int]) -> list[str]:
    return [
        field_name
        for field_name, tokens in sorted(
            field_token_estimates.items(),
            key=lambda item: (-item[1], item[0]),
        )
        if tokens > 0
    ][:3]


def _validate_thresholds(
    warning_threshold: int,
    error_threshold: int,
    largest_task_limit: int,
) -> None:
    if warning_threshold <= 0:
        raise ValueError("warning_threshold must be greater than zero")
    if error_threshold < warning_threshold:
        raise ValueError("error_threshold must be greater than or equal to warning_threshold")
    if largest_task_limit < 0:
        raise ValueError("largest_task_limit must be greater than or equal to zero")


def _task_items(plan: dict[str, Any]) -> list[Any]:
    tasks = plan.get("tasks")
    if not isinstance(tasks, list):
        return []
    return tasks


def _task_mapping(task: Any) -> dict[str, Any]:
    if isinstance(task, dict):
        return task
    if hasattr(task, "model_dump"):
        return task.model_dump(mode="json")
    if hasattr(task, "dict"):
        return task.dict()
    return {}


def _has_budget_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
    except TypeError:
        return str(value)
    return value
