"""Acceptance criteria quality audit for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal


Severity = Literal["high", "medium"]

DEFAULT_MIN_LENGTH = 12
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_OBSERVABLE_TERMS = {
    "assert",
    "display",
    "fail",
    "pass",
    "render",
    "return",
    "verify",
    "write",
}
_VAGUE_PHRASE_PATTERNS = {
    "works": re.compile(r"\bworks\b", re.IGNORECASE),
    "done": re.compile(r"\bdone\b", re.IGNORECASE),
    "as needed": re.compile(r"\bas\s+needed\b", re.IGNORECASE),
}


@dataclass(frozen=True)
class AcceptanceQualityFinding:
    """A single weak acceptance criteria finding."""

    task_id: str
    task_title: str
    criterion_text: str
    severity: Severity
    code: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "criterion_text": self.criterion_text,
            "severity": self.severity,
            "code": self.code,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class AcceptanceQualityTaskResult:
    """Acceptance criteria findings for one task."""

    task_id: str
    title: str
    findings: list[AcceptanceQualityFinding] = field(default_factory=list)

    @property
    def high_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "high")

    @property
    def medium_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "medium")

    @property
    def passed(self) -> bool:
        return self.high_count == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "passed": self.passed,
            "summary": {
                "high": self.high_count,
                "medium": self.medium_count,
            },
            "findings": [finding.to_dict() for finding in self.findings],
        }


@dataclass(frozen=True)
class AcceptanceQualityResult:
    """Acceptance criteria quality audit result for an execution plan."""

    plan_id: str
    min_length: int
    tasks: list[AcceptanceQualityTaskResult] = field(default_factory=list)

    @property
    def high_count(self) -> int:
        return sum(task.high_count for task in self.tasks)

    @property
    def medium_count(self) -> int:
        return sum(task.medium_count for task in self.tasks)

    @property
    def passed(self) -> bool:
        return self.high_count == 0

    @property
    def findings(self) -> list[AcceptanceQualityFinding]:
        return [finding for task in self.tasks for finding in task.findings]

    def findings_by_task(self) -> dict[str, list[AcceptanceQualityFinding]]:
        return {
            task.task_id: list(task.findings)
            for task in self.tasks
            if task.findings
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "passed": self.passed,
            "min_length": self.min_length,
            "summary": {
                "high": self.high_count,
                "medium": self.medium_count,
                "tasks": len(self.tasks),
            },
            "tasks": [task.to_dict() for task in self.tasks],
            "findings": [finding.to_dict() for finding in self.findings],
        }


def audit_acceptance_quality(
    plan_dict: dict[str, Any],
    *,
    min_length: int = DEFAULT_MIN_LENGTH,
) -> AcceptanceQualityResult:
    """Find weak or non-observable task acceptance criteria."""
    plan_id = str(plan_dict.get("id") or "")
    tasks: list[AcceptanceQualityTaskResult] = []

    for task in _list_of_dicts(plan_dict.get("tasks")):
        findings = _task_findings(task, min_length=max(1, min_length))
        tasks.append(
            AcceptanceQualityTaskResult(
                task_id=str(task.get("id") or ""),
                title=str(task.get("title") or ""),
                findings=findings,
            )
        )

    return AcceptanceQualityResult(
        plan_id=plan_id,
        min_length=max(1, min_length),
        tasks=tasks,
    )


def _task_findings(
    task: dict[str, Any],
    *,
    min_length: int,
) -> list[AcceptanceQualityFinding]:
    task_id = str(task.get("id") or "")
    task_title = str(task.get("title") or "")
    criteria = _string_list(task.get("acceptance_criteria"))
    findings: list[AcceptanceQualityFinding] = []

    if not criteria:
        return [
            AcceptanceQualityFinding(
                task_id=task_id,
                task_title=task_title,
                criterion_text="",
                severity="high",
                code="missing_acceptance_criteria",
                reason="Task has no acceptance criteria to validate completion.",
            )
        ]

    seen: set[str] = set()
    for criterion in criteria:
        normalized = _normalized_phrase(criterion)
        if normalized in seen:
            findings.append(
                AcceptanceQualityFinding(
                    task_id=task_id,
                    task_title=task_title,
                    criterion_text=criterion,
                    severity="medium",
                    code="duplicate_criterion",
                    reason="Criterion duplicates another acceptance criterion on the task.",
                )
            )
        seen.add(normalized)

        if len(criterion.strip()) < min_length:
            findings.append(
                AcceptanceQualityFinding(
                    task_id=task_id,
                    task_title=task_title,
                    criterion_text=criterion,
                    severity="high",
                    code="criterion_too_short",
                    reason=(
                        f"Criterion is shorter than the configured minimum of "
                        f"{min_length} characters."
                    ),
                )
            )

        vague_phrase = _matching_vague_phrase(criterion)
        if vague_phrase:
            findings.append(
                AcceptanceQualityFinding(
                    task_id=task_id,
                    task_title=task_title,
                    criterion_text=criterion,
                    severity="high",
                    code="vague_phrase",
                    reason=f"Criterion uses vague completion language: {vague_phrase}.",
                )
            )

        if not _has_observable_language(criterion):
            findings.append(
                AcceptanceQualityFinding(
                    task_id=task_id,
                    task_title=task_title,
                    criterion_text=criterion,
                    severity="high",
                    code="non_observable_criterion",
                    reason=(
                        "Criterion does not include observable validation language "
                        "such as verify, assert, render, return, write, fail, pass, "
                        "or display."
                    ),
                )
            )

    return findings


def _has_observable_language(criterion: str) -> bool:
    tokens = _TOKEN_RE.findall(criterion.lower())
    return any(_matches_observable_term(token) for token in tokens)


def _matches_observable_term(token: str) -> bool:
    if token in _OBSERVABLE_TERMS:
        return True
    variants = {
        "asserts",
        "asserted",
        "asserting",
        "displays",
        "displayed",
        "displaying",
        "fails",
        "failed",
        "failing",
        "passes",
        "passed",
        "passing",
        "renders",
        "rendered",
        "rendering",
        "returns",
        "returned",
        "returning",
        "verifies",
        "verified",
        "verifying",
        "writes",
        "wrote",
        "written",
        "writing",
    }
    return token in variants


def _matching_vague_phrase(criterion: str) -> str | None:
    for phrase, pattern in _VAGUE_PHRASE_PATTERNS.items():
        if pattern.search(criterion):
            return phrase
    return None


def _normalized_phrase(value: str) -> str:
    return " ".join(_TOKEN_RE.findall(value.lower()))


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
