"""Ambiguity audit for implementation briefs before task generation."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal

from blueprint.domain import ImplementationBrief


Severity = Literal["high", "medium"]

_AUDITED_FIELDS = (
    "problem_statement",
    "mvp_goal",
    "scope",
    "assumptions",
    "validation_plan",
    "definition_of_done",
)
_HIGH_SEVERITY_PENALTY = 20
_MEDIUM_SEVERITY_PENALTY = 10
_AMBIGUOUS_PHRASES: tuple[tuple[str, Severity, str, re.Pattern[str]], ...] = (
    (
        "owner TBD",
        "high",
        "Name the responsible owner or owner lane before generating tasks.",
        re.compile(r"\bowner\s+(?:is\s+)?tbd\b", re.IGNORECASE),
    ),
    (
        "someone",
        "high",
        "Name the responsible owner or owner lane before generating tasks.",
        re.compile(r"\bsomeone\b", re.IGNORECASE),
    ),
    (
        "appropriate team",
        "high",
        "Name the responsible team or role instead of deferring ownership.",
        re.compile(r"\bappropriate\s+team\b", re.IGNORECASE),
    ),
    (
        "TBD",
        "high",
        "Replace the placeholder with the actual decision, owner, or constraint.",
        re.compile(r"\btbd\b", re.IGNORECASE),
    ),
    (
        "various",
        "medium",
        "Name the specific surfaces, cases, or inputs that the plan must cover.",
        re.compile(r"\bvarious\b", re.IGNORECASE),
    ),
    (
        "as needed",
        "medium",
        "State the concrete condition or threshold that triggers this work.",
        re.compile(r"\bas\s+needed\b", re.IGNORECASE),
    ),
    (
        "user-friendly",
        "medium",
        "Define the observable usability behavior or acceptance criteria.",
        re.compile(r"\buser[-\s]+friendly\b", re.IGNORECASE),
    ),
    (
        "fast",
        "medium",
        "Define a target latency, runtime, throughput, or benchmark.",
        re.compile(r"\bfast\b", re.IGNORECASE),
    ),
    (
        "scalable",
        "medium",
        "Define the expected load, data volume, concurrency, or growth target.",
        re.compile(r"\bscalable\b", re.IGNORECASE),
    ),
)


@dataclass(frozen=True)
class BriefAmbiguityIssue:
    """A single ambiguous phrase found in an implementation brief."""

    field: str
    phrase: str
    severity: Severity
    message: str
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "phrase": self.phrase,
            "severity": self.severity,
            "message": self.message,
            "recommendation": self.recommendation,
        }


@dataclass(frozen=True)
class BriefAmbiguityResult:
    """Ambiguity score for an implementation brief."""

    brief_id: str
    score: int
    issues: list[BriefAmbiguityIssue] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.issues

    @property
    def high_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "high")

    @property
    def medium_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "medium")

    def to_dict(self) -> dict[str, Any]:
        return {
            "brief_id": self.brief_id,
            "score": self.score,
            "passed": self.passed,
            "summary": {
                "high": self.high_count,
                "medium": self.medium_count,
                "issues": len(self.issues),
            },
            "issues": [issue.to_dict() for issue in self.issues],
        }


def audit_brief_ambiguity(
    implementation_brief: ImplementationBrief | dict[str, Any],
) -> BriefAmbiguityResult:
    """Score an ImplementationBrief for vague language that weakens task planning."""
    brief = _validated_brief(implementation_brief)
    issues: list[BriefAmbiguityIssue] = []

    for field_name in _AUDITED_FIELDS:
        for text in _field_texts(brief, field_name):
            issues.extend(_text_issues(field_name, text))

    return BriefAmbiguityResult(
        brief_id=brief.id,
        score=_score(issues),
        issues=issues,
    )


def _validated_brief(
    implementation_brief: ImplementationBrief | dict[str, Any],
) -> ImplementationBrief:
    if isinstance(implementation_brief, ImplementationBrief):
        return implementation_brief
    return ImplementationBrief.model_validate(implementation_brief)


def _field_texts(brief: ImplementationBrief, field_name: str) -> list[str]:
    value = getattr(brief, field_name)
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)]
    if isinstance(value, str):
        return [value]
    return []


def _text_issues(field_name: str, text: str) -> list[BriefAmbiguityIssue]:
    issues: list[BriefAmbiguityIssue] = []
    seen: set[tuple[str, str]] = set()
    matched_spans: list[tuple[int, int]] = []
    for phrase, severity, recommendation, pattern in _AMBIGUOUS_PHRASES:
        for match in pattern.finditer(text):
            if any(_overlaps(match.span(), span) for span in matched_spans):
                continue
            key = (phrase.casefold(), match.group(0).casefold())
            if key in seen:
                continue
            seen.add(key)
            matched_spans.append(match.span())
            issues.append(
                BriefAmbiguityIssue(
                    field=field_name,
                    phrase=match.group(0),
                    severity=severity,
                    message=(
                        f"{field_name} contains ambiguous planning language: "
                        f"{match.group(0)}."
                    ),
                    recommendation=recommendation,
                )
            )
    return issues


def _overlaps(left: tuple[int, int], right: tuple[int, int]) -> bool:
    return left[0] < right[1] and right[0] < left[1]


def _score(issues: list[BriefAmbiguityIssue]) -> int:
    penalty = 0
    for issue in issues:
        if issue.severity == "high":
            penalty += _HIGH_SEVERITY_PENALTY
        else:
            penalty += _MEDIUM_SEVERITY_PENALTY
    return max(0, 100 - penalty)


__all__ = [
    "BriefAmbiguityIssue",
    "BriefAmbiguityResult",
    "audit_brief_ambiguity",
]
