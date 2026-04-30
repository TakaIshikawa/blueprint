"""Surface unresolved planning questions from implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief


QuestionSeverity = Literal["high", "medium", "low"]

_CONCERN_ORDER = (
    "scope",
    "assumptions",
    "integration_points",
    "data_requirements",
    "architecture_notes",
    "validation_plan",
    "definition_of_done",
)
_PLANNING_LIST_FIELDS = {"scope", "assumptions", "definition_of_done"}
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_VAGUE_PATTERNS: tuple[tuple[str, QuestionSeverity, re.Pattern[str]], ...] = (
    ("placeholder", "high", re.compile(r"\b(?:tbd|todo|unknown|unsure|n/?a)\b", re.IGNORECASE)),
    (
        "deferred owner",
        "high",
        re.compile(r"\b(?:someone|appropriate team|owner\s+(?:is\s+)?tbd)\b", re.IGNORECASE),
    ),
    (
        "undefined condition",
        "medium",
        re.compile(r"\b(?:as needed|if needed|where appropriate|etc\.?)\b", re.IGNORECASE),
    ),
    (
        "undefined quality target",
        "medium",
        re.compile(r"\b(?:fast|scalable|robust|user[-\s]+friendly|simple|easy)\b", re.IGNORECASE),
    ),
)
_GENERIC_VALIDATION_PATTERNS = (
    re.compile(r"^(?:test it|verify it works|make sure it works|run tests|qa)$", re.IGNORECASE),
    re.compile(r"\b(?:test it|verify it works|make sure it works)\b", re.IGNORECASE),
)
_QUESTION_BY_CONCERN = {
    "scope": "Which concrete deliverables and boundaries should execution tasks cover?",
    "assumptions": "Which assumptions must be confirmed before implementation starts?",
    "integration_points": "Which external systems, APIs, or services does the implementation depend on?",
    "data_requirements": "What data contracts, fields, migrations, or retention rules are required?",
    "architecture_notes": "What technical approach or constraints should guide the implementation?",
    "validation_plan": "What specific automated or manual checks will prove the implementation works?",
    "definition_of_done": "What observable completion criteria must be met before handoff?",
}
_OWNER_BY_CONCERN = {
    "scope": "product_owner",
    "assumptions": "product_owner",
    "integration_points": "technical_lead",
    "data_requirements": "data_owner",
    "architecture_notes": "technical_lead",
    "validation_plan": "qa_owner",
    "definition_of_done": "delivery_lead",
}


@dataclass(frozen=True, slots=True)
class BriefOpenQuestion:
    """One unresolved planning question inferred from a brief."""

    topic: str
    question: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    severity: QuestionSeverity = "medium"
    suggested_owner: str = "product_owner"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "topic": self.topic,
            "question": self.question,
            "evidence": list(self.evidence),
            "severity": self.severity,
            "suggested_owner": self.suggested_owner,
        }


def extract_open_questions(
    brief: Mapping[str, Any] | ImplementationBrief,
) -> tuple[BriefOpenQuestion, ...]:
    """Extract deterministic unresolved planning questions from a brief."""
    payload = _brief_payload(brief)
    builder = _QuestionBuilder()

    for concern in _CONCERN_ORDER:
        values = _field_values(payload.get(concern), concern)
        if not values:
            builder.add(
                concern,
                evidence=f"{concern}: missing",
                severity=_missing_severity(concern),
            )
            continue

        for source, text in values:
            for reason, severity in _vague_reasons(concern, text):
                builder.add(
                    concern,
                    evidence=f"{source}: {text} ({reason})",
                    severity=severity,
                )

        if concern == "validation_plan" and not _has_specific_validation(values):
            builder.add(
                concern,
                evidence="validation_plan lacks a concrete check, command, metric, or scenario",
                severity="medium",
            )

    return builder.records()


def open_questions_to_dict(
    questions: tuple[BriefOpenQuestion, ...] | list[BriefOpenQuestion],
) -> list[dict[str, Any]]:
    """Serialize open questions to dictionaries."""
    return [question.to_dict() for question in questions]


open_questions_to_dict.__test__ = False


class _QuestionBuilder:
    def __init__(self) -> None:
        self._drafts: dict[str, dict[str, Any]] = {}

    def add(self, concern: str, *, evidence: str, severity: QuestionSeverity) -> None:
        draft = self._drafts.setdefault(
            concern,
            {
                "evidence": [],
                "severity": "low",
            },
        )
        draft["evidence"].append(evidence)
        draft["severity"] = _highest_severity(draft["severity"], severity)

    def records(self) -> tuple[BriefOpenQuestion, ...]:
        records: list[BriefOpenQuestion] = []
        for concern in _CONCERN_ORDER:
            draft = self._drafts.get(concern)
            if draft is None:
                continue
            records.append(
                BriefOpenQuestion(
                    topic=concern,
                    question=_QUESTION_BY_CONCERN[concern],
                    evidence=tuple(_dedupe(draft["evidence"])),
                    severity=draft["severity"],
                    suggested_owner=_OWNER_BY_CONCERN[concern],
                )
            )
        return tuple(records)


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(brief)


def _field_values(value: Any, field_name: str) -> list[tuple[str, str]]:
    if isinstance(value, str):
        text = _optional_text(value)
        return [(field_name, text)] if text else []
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        values: list[tuple[str, str]] = []
        for index, item in enumerate(items, start=1):
            text = _optional_text(item)
            if text:
                values.append((f"{field_name}.{index}", text))
        return values
    return []


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _vague_reasons(concern: str, text: str) -> list[tuple[str, QuestionSeverity]]:
    reasons: list[tuple[str, QuestionSeverity]] = []
    for label, severity, pattern in _VAGUE_PATTERNS:
        if pattern.search(text):
            reasons.append((label, severity))

    token_count = len(_TOKEN_RE.findall(text))
    if concern in _PLANNING_LIST_FIELDS and token_count <= 2:
        reasons.append(("too terse to guide task planning", "medium"))
    if concern in {"architecture_notes", "data_requirements"} and token_count <= 4:
        reasons.append(("too terse to define implementation constraints", "medium"))
    if concern == "validation_plan":
        for pattern in _GENERIC_VALIDATION_PATTERNS:
            if pattern.search(text.strip()):
                reasons.append(("generic validation language", "medium"))
                break

    return reasons


def _has_specific_validation(values: list[tuple[str, str]]) -> bool:
    text = " ".join(value for _, value in values)
    tokens = _TOKEN_RE.findall(text)
    if len(tokens) < 5:
        return False
    return bool(
        re.search(
            r"\b(?:pytest|unit|integration|e2e|manual|scenario|metric|latency|"
            r"acceptance|regression|snapshot|contract|coverage|verify|assert)\b",
            text,
            re.IGNORECASE,
        )
    )


def _missing_severity(concern: str) -> QuestionSeverity:
    if concern in {"scope", "validation_plan", "definition_of_done"}:
        return "high"
    if concern in {"integration_points", "data_requirements", "architecture_notes"}:
        return "medium"
    return "low"


def _highest_severity(left: QuestionSeverity, right: QuestionSeverity) -> QuestionSeverity:
    rank = {"low": 0, "medium": 1, "high": 2}
    return left if rank[left] >= rank[right] else right


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
    "BriefOpenQuestion",
    "QuestionSeverity",
    "extract_open_questions",
    "open_questions_to_dict",
]
