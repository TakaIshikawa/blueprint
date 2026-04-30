"""Trace task acceptance criteria back to implementation brief commitments."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping


Severity = Literal["high", "medium"]
BriefItemType = Literal["goal", "scope", "requirement"]

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_GENERIC_PHRASES = {
    "as expected",
    "as needed",
    "done",
    "handled",
    "implemented",
    "meets requirements",
    "tested",
    "works",
}
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "be",
    "by",
    "can",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "then",
    "to",
    "with",
}


@dataclass(frozen=True)
class AcceptanceTraceabilityFinding:
    """A single acceptance traceability finding."""

    code: str
    severity: Severity
    message: str
    task_id: str | None = None
    criterion_text: str | None = None
    brief_item_code: str | None = None
    brief_item_type: BriefItemType | None = None
    brief_item_text: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "task_id": self.task_id,
            "criterion_text": self.criterion_text,
            "brief_item_code": self.brief_item_code,
            "brief_item_type": self.brief_item_type,
            "brief_item_text": self.brief_item_text,
        }


@dataclass(frozen=True)
class AcceptanceTraceabilityCoverage:
    """Coverage status for one implementation brief commitment."""

    brief_item_code: str
    brief_item_type: BriefItemType
    brief_item_text: str
    covered: bool
    task_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "brief_item_code": self.brief_item_code,
            "brief_item_type": self.brief_item_type,
            "brief_item_text": self.brief_item_text,
            "covered": self.covered,
            "task_ids": list(self.task_ids),
        }


@dataclass(frozen=True)
class AcceptanceTraceabilityResult:
    """Acceptance traceability audit result for a brief and execution plan."""

    brief_id: str
    plan_id: str
    findings: list[AcceptanceTraceabilityFinding] = field(default_factory=list)
    coverage: list[AcceptanceTraceabilityCoverage] = field(default_factory=list)

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
            "brief_id": self.brief_id,
            "plan_id": self.plan_id,
            "passed": self.passed,
            "summary": {
                "high": self.high_count,
                "medium": self.medium_count,
                "brief_items": len(self.coverage),
                "covered_brief_items": sum(1 for item in self.coverage if item.covered),
            },
            "findings": [finding.to_dict() for finding in self.findings],
            "coverage": [item.to_dict() for item in self.coverage],
        }


@dataclass(frozen=True)
class _BriefItem:
    code: str
    item_type: BriefItemType
    text: str
    tokens: set[str]


def audit_acceptance_traceability(
    implementation_brief: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> AcceptanceTraceabilityResult:
    """Audit whether task acceptance criteria trace to brief commitments."""
    brief = _payload(implementation_brief)
    plan_payload = _payload(plan)
    brief_items = _brief_items(brief)
    tasks = _list_of_dicts(plan_payload.get("tasks"))
    findings: list[AcceptanceTraceabilityFinding] = []

    if not tasks:
        findings.append(
            AcceptanceTraceabilityFinding(
                code="empty_plan",
                severity="high",
                message="Plan has no tasks with acceptance criteria to trace.",
            )
        )

    traceable_tasks: list[tuple[str, set[str]]] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        criteria = _string_list(task.get("acceptance_criteria"))
        if not criteria:
            findings.append(
                AcceptanceTraceabilityFinding(
                    code="missing_acceptance_criteria",
                    severity="high",
                    message="Task has no acceptance criteria to trace to brief commitments.",
                    task_id=task_id,
                )
            )
            continue

        traceable_text: list[str] = []
        for criterion in criteria:
            if _is_generic_criterion(criterion):
                findings.append(
                    AcceptanceTraceabilityFinding(
                        code="generic_acceptance_criterion",
                        severity="medium",
                        message=(
                            "Acceptance criterion is too generic to provide a reliable "
                            "traceability signal."
                        ),
                        task_id=task_id,
                        criterion_text=criterion,
                    )
                )
                continue
            traceable_text.append(criterion)

        if traceable_text:
            traceable_tasks.append((task_id, _tokens(" ".join(traceable_text))))

    coverage = _coverage(brief_items, traceable_tasks)
    for item in coverage:
        if not item.covered:
            findings.append(
                AcceptanceTraceabilityFinding(
                    code=f"uncovered_{item.brief_item_type}",
                    severity="high",
                    message="No task acceptance criteria appear to cover this brief commitment.",
                    brief_item_code=item.brief_item_code,
                    brief_item_type=item.brief_item_type,
                    brief_item_text=item.brief_item_text,
                )
            )

    return AcceptanceTraceabilityResult(
        brief_id=_text(brief.get("id")),
        plan_id=_text(plan_payload.get("id")),
        findings=findings,
        coverage=coverage,
    )


def _coverage(
    brief_items: list[_BriefItem],
    traceable_tasks: list[tuple[str, set[str]]],
) -> list[AcceptanceTraceabilityCoverage]:
    coverage: list[AcceptanceTraceabilityCoverage] = []
    for item in brief_items:
        task_ids = [
            task_id
            for task_id, task_tokens in traceable_tasks
            if _covers(item.tokens, task_tokens)
        ]
        coverage.append(
            AcceptanceTraceabilityCoverage(
                brief_item_code=item.code,
                brief_item_type=item.item_type,
                brief_item_text=item.text,
                covered=bool(task_ids),
                task_ids=task_ids,
            )
        )
    return coverage


def _covers(brief_tokens: set[str], task_tokens: set[str]) -> bool:
    if not brief_tokens:
        return False
    overlap = brief_tokens & task_tokens
    if len(brief_tokens) <= 2:
        return len(overlap) == len(brief_tokens)
    return len(overlap) >= min(3, len(brief_tokens))


def _brief_items(brief: Mapping[str, Any]) -> list[_BriefItem]:
    items: list[_BriefItem] = []
    goal = _text(brief.get("mvp_goal"))
    if goal:
        items.append(_BriefItem("goal:mvp_goal", "goal", goal, _tokens(goal)))

    for index, scope_item in enumerate(_string_list(brief.get("scope")), start=1):
        items.append(
            _BriefItem(
                f"scope:{index:03d}",
                "scope",
                scope_item,
                _tokens(scope_item),
            )
        )

    for index, requirement in enumerate(_requirements(brief), start=1):
        items.append(
            _BriefItem(
                f"requirement:{index:03d}",
                "requirement",
                requirement,
                _tokens(requirement),
            )
        )
    return items


def _requirements(brief: Mapping[str, Any]) -> list[str]:
    requirements: list[str] = []
    for key in ("requirements", "functional_requirements", "user_requirements"):
        value = brief.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, Mapping):
                    requirement = " ".join(
                        part
                        for part in (
                            _text(item.get("id")),
                            _text(item.get("title")),
                            _text(item.get("description")),
                        )
                        if part
                    )
                    if requirement:
                        requirements.append(requirement)
                elif _text(item):
                    requirements.append(_text(item))
        elif _text(value):
            requirements.append(_text(value))
    return _dedupe(requirements)


def _is_generic_criterion(criterion: str) -> bool:
    normalized = _normalized_phrase(criterion)
    if normalized in _GENERIC_PHRASES:
        return True
    if any(phrase in normalized for phrase in ("as expected", "as needed")):
        return True
    tokens = _tokens(criterion)
    return len(tokens) < 2


def _tokens(value: str) -> set[str]:
    return {
        _stem(token)
        for token in _TOKEN_RE.findall(value.lower())
        if token not in _STOPWORDS and len(token) > 1
    }


def _stem(token: str) -> str:
    for suffix in ("ing", "ed", "es", "s"):
        if len(token) > len(suffix) + 2 and token.endswith(suffix):
            return token[: -len(suffix)]
    return token


def _normalized_phrase(value: str) -> str:
    return " ".join(_TOKEN_RE.findall(value.lower()))


def _payload(value: Mapping[str, Any]) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="python")
    return dict(value)


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    return ""


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


__all__ = [
    "AcceptanceTraceabilityCoverage",
    "AcceptanceTraceabilityFinding",
    "AcceptanceTraceabilityResult",
    "audit_acceptance_traceability",
]
