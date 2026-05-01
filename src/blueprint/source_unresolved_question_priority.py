"""Rank unresolved source brief questions by implementation-planning impact."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


QuestionPriority = Literal["critical", "high", "medium", "low"]
_T = TypeVar("_T")

_QUESTION_FIELDS = {
    "open_questions",
    "questions",
    "unresolved_questions",
    "unknowns",
    "ambiguities",
}
_CONTEXT_FIELDS = {
    "risks",
    "constraints",
    "acceptance_criteria",
    "acceptance",
    "criteria",
    "summary",
    "metadata",
}
_QUESTION_RE = re.compile(r"([^?\n]*(?:\?+))")
_PROMPT_RE = re.compile(
    r"\b(?:open question|question|unresolved|unknown|tbd|todo|clarify|confirm)\s*:\s*(.+)",
    re.IGNORECASE,
)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")

_BLOCKER_RE = re.compile(
    r"\b(?:block|blocked|blocking|must|need|needed|required|before|cannot|can't|"
    r"decide|decision|confirm|clarify|unknown|tbd|waiting|depends?|prerequisite)\b",
    re.IGNORECASE,
)
_ACCEPTANCE_RE = re.compile(
    r"\b(?:acceptance|criteria|requirement|done when|definition of done|expected|"
    r"pass|fail|validate|verify|observable|success metric)\b",
    re.IGNORECASE,
)
_DEPENDENCY_RE = re.compile(
    r"\b(?:dependency|depends?|blocked by|waiting on|vendor|partner|external|api|"
    r"integration|contract|prerequisite|owner|approval|handoff)\b",
    re.IGNORECASE,
)
_SECURITY_RE = re.compile(
    r"\b(?:security|auth|authentication|authorization|permission|role|access|pii|"
    r"privacy|compliance|secret|token|encryption|audit)\b",
    re.IGNORECASE,
)
_DATA_RE = re.compile(
    r"\b(?:data|database|db|schema|migration|backfill|retention|analytics|event|"
    r"metric|field|record|payload|dataset|reporting)\b",
    re.IGNORECASE,
)
_ROLLOUT_RE = re.compile(
    r"\b(?:rollout|launch|release|deploy|deployment|canary|feature flag|flag|"
    r"rollback|production|staging|migration window|go-live|go live)\b",
    re.IGNORECASE,
)
_SCOPE_RE = re.compile(
    r"\b(?:all users|customers?|admins?|support|mobile|web|api|backend|frontend|"
    r"service|services|multiple|global|tenant|environment|production)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class SourceUnresolvedQuestionPriorityRecord:
    """One unresolved source question ranked for planning impact."""

    source_brief_id: str
    question: str
    priority: QuestionPriority
    score: int
    score_components: dict[str, int] = field(default_factory=dict)
    rationale: str = ""
    suggested_owner_role: str = "product_owner"
    evidence_needed: tuple[str, ...] = field(default_factory=tuple)
    source_fields: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "question": self.question,
            "priority": self.priority,
            "score": self.score,
            "score_components": dict(self.score_components),
            "rationale": self.rationale,
            "suggested_owner_role": self.suggested_owner_role,
            "evidence_needed": list(self.evidence_needed),
            "source_fields": list(self.source_fields),
        }


@dataclass(frozen=True, slots=True)
class SourceUnresolvedQuestionPriorityReport:
    """Ranked unresolved questions from one or more source briefs."""

    records: tuple[SourceUnresolvedQuestionPriorityRecord, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {"records": [record.to_dict() for record in self.records]}

    def to_markdown(self) -> str:
        """Render ranked unresolved questions as deterministic Markdown."""
        lines = ["# Source Unresolved Question Priority"]
        if not self.records:
            lines.extend(["", "No unresolved source questions found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Priority | Score | Question | Owner | Evidence Needed | Rationale |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{record.priority} | "
                f"{record.score} | "
                f"{_markdown_cell(record.question)} | "
                f"{record.suggested_owner_role} | "
                f"{_markdown_cell('; '.join(record.evidence_needed))} | "
                f"{_markdown_cell(record.rationale)} |"
            )
        return "\n".join(lines)


def build_source_unresolved_question_priority(
    source: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief],
) -> SourceUnresolvedQuestionPriorityReport:
    """Rank unresolved questions found in normalized source briefs."""
    records: list[SourceUnresolvedQuestionPriorityRecord] = []
    for brief in _source_briefs(source):
        records.extend(_brief_records(brief))
    records.sort(
        key=lambda record: (
            -record.score,
            _PRIORITY_RANK[record.priority],
            record.source_brief_id,
            _dedupe_key(record.question),
        )
    )
    return SourceUnresolvedQuestionPriorityReport(records=tuple(records))


def derive_source_unresolved_question_priority(
    source: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief],
) -> SourceUnresolvedQuestionPriorityReport:
    """Compatibility alias for ranking unresolved source questions."""
    return build_source_unresolved_question_priority(source)


def source_unresolved_question_priority_to_dict(
    report: SourceUnresolvedQuestionPriorityReport,
) -> dict[str, Any]:
    """Serialize unresolved question priorities to a plain dictionary."""
    return report.to_dict()


source_unresolved_question_priority_to_dict.__test__ = False


def source_unresolved_question_priority_to_markdown(
    report: SourceUnresolvedQuestionPriorityReport,
) -> str:
    """Render unresolved question priorities as Markdown."""
    return report.to_markdown()


source_unresolved_question_priority_to_markdown.__test__ = False


_PRIORITY_RANK = {"critical": 0, "high": 1, "medium": 2, "low": 3}


@dataclass(frozen=True, slots=True)
class _Candidate:
    question: str
    source_fields: tuple[str, ...]
    context: str


def _brief_records(brief: Mapping[str, Any]) -> list[SourceUnresolvedQuestionPriorityRecord]:
    source_brief_id = (
        _optional_text(brief.get("id"))
        or _optional_text(brief.get("source_id"))
        or "source-brief"
    )
    candidates_by_key: dict[str, _Candidate] = {}

    for source_field, value in _brief_values(brief):
        field_key = _field_key(source_field)
        if field_key in _QUESTION_FIELDS:
            for question in _questions_from_value(value):
                _merge_candidate(candidates_by_key, question, source_field, question)
            continue
        if field_key in _CONTEXT_FIELDS:
            for question in _questions_from_texts(value):
                _merge_candidate(candidates_by_key, question, source_field, question)

    return [
        _record(source_brief_id, candidate)
        for candidate in candidates_by_key.values()
    ]


def _record(
    source_brief_id: str,
    candidate: _Candidate,
) -> SourceUnresolvedQuestionPriorityRecord:
    text = f"{candidate.question} {candidate.context} {' '.join(candidate.source_fields)}"
    components = {
        "implementation_impact": _implementation_impact(text),
        "blocker_likelihood": _blocker_likelihood(text, candidate.source_fields),
        "affected_scope": _affected_scope(text, candidate.source_fields),
        "acceptance_ambiguity": _acceptance_ambiguity(text, candidate.source_fields),
        "dependency_risk": _dependency_risk(text),
    }
    score = sum(components.values())
    priority = _priority(score)
    signals = _signals(text, candidate.source_fields)
    return SourceUnresolvedQuestionPriorityRecord(
        source_brief_id=source_brief_id,
        question=candidate.question,
        priority=priority,
        score=score,
        score_components=components,
        rationale=_rationale(signals, components),
        suggested_owner_role=_owner_role(text, candidate.source_fields),
        evidence_needed=tuple(_evidence_needed(text, signals, candidate.source_fields)),
        source_fields=candidate.source_fields,
    )


def _implementation_impact(text: str) -> int:
    score = 1
    if any(pattern.search(text) for pattern in (_SECURITY_RE, _DATA_RE, _ROLLOUT_RE)):
        score += 2
    if any(pattern.search(text) for pattern in (_DEPENDENCY_RE, _ACCEPTANCE_RE)):
        score += 1
    return min(score, 4)


def _blocker_likelihood(text: str, source_fields: tuple[str, ...]) -> int:
    score = 1 if _BLOCKER_RE.search(text) else 0
    if any(_field_key(field) in _QUESTION_FIELDS for field in source_fields):
        score += 1
    if any(pattern.search(text) for pattern in (_DEPENDENCY_RE, _SECURITY_RE, _ROLLOUT_RE)):
        score += 1
    return min(score, 3)


def _affected_scope(text: str, source_fields: tuple[str, ...]) -> int:
    score = 1 if _SCOPE_RE.search(text) else 0
    if any(pattern.search(text) for pattern in (_SECURITY_RE, _DATA_RE, _ROLLOUT_RE)):
        score += 1
    if any("risk" in field.casefold() or "constraint" in field.casefold() for field in source_fields):
        score += 1
    return min(score, 3)


def _acceptance_ambiguity(text: str, source_fields: tuple[str, ...]) -> int:
    score = 0
    if _ACCEPTANCE_RE.search(text):
        score += 2
    if any("acceptance" in field.casefold() or "criteria" in field.casefold() for field in source_fields):
        score += 2
    if _QUESTION_RE.search(text):
        score += 1
    return min(score, 4)


def _dependency_risk(text: str) -> int:
    score = 0
    if _DEPENDENCY_RE.search(text):
        score += 2
    if any(pattern.search(text) for pattern in (_ROLLOUT_RE, _SECURITY_RE, _DATA_RE)):
        score += 1
    return min(score, 3)


def _priority(score: int) -> QuestionPriority:
    if score >= 14:
        return "critical"
    if score >= 9:
        return "high"
    if score >= 6:
        return "medium"
    return "low"


def _signals(text: str, source_fields: tuple[str, ...]) -> tuple[str, ...]:
    signals: list[str] = []
    if _ACCEPTANCE_RE.search(text) or any(
        "acceptance" in field.casefold() or "criteria" in field.casefold()
        for field in source_fields
    ):
        signals.append("acceptance ambiguity")
    if _DEPENDENCY_RE.search(text):
        signals.append("dependency risk")
    if _SECURITY_RE.search(text):
        signals.append("security impact")
    if _DATA_RE.search(text):
        signals.append("data impact")
    if _ROLLOUT_RE.search(text):
        signals.append("rollout impact")
    if _BLOCKER_RE.search(text):
        signals.append("likely blocker")
    return tuple(_dedupe(signals)) or ("informational",)


def _rationale(signals: tuple[str, ...], components: Mapping[str, int]) -> str:
    strongest = max(components, key=lambda key: (components[key], key))
    return (
        "Ranked from "
        + ", ".join(signals)
        + f"; strongest score component is {strongest.replace('_', ' ')}."
    )


def _owner_role(text: str, source_fields: tuple[str, ...]) -> str:
    if _SECURITY_RE.search(text):
        return "security_owner"
    if _ROLLOUT_RE.search(text):
        return "release_manager"
    if _DEPENDENCY_RE.search(text):
        return "technical_lead"
    if _DATA_RE.search(text):
        return "data_owner"
    if _ACCEPTANCE_RE.search(text) or any(
        "acceptance" in field.casefold() or "criteria" in field.casefold()
        for field in source_fields
    ):
        return "product_owner"
    return "product_owner"


def _evidence_needed(
    text: str,
    signals: tuple[str, ...],
    source_fields: tuple[str, ...],
) -> list[str]:
    evidence: list[str] = []
    if "acceptance ambiguity" in signals:
        evidence.append("Concrete acceptance criteria and pass/fail examples.")
    if "dependency risk" in signals:
        evidence.append("Confirmed dependency owner, contract, and availability timeline.")
    if "security impact" in signals:
        evidence.append("Security requirements, approval notes, and threat or permission model.")
    if "data impact" in signals:
        evidence.append("Data contract, schema, migration, retention, or metric definition.")
    if "rollout impact" in signals:
        evidence.append("Rollout plan, environment target, rollback path, and launch window.")
    if "likely blocker" in signals:
        evidence.append("Named decision owner and decision deadline before planning.")
    if not evidence or "informational" in signals:
        evidence.append("Answer from the relevant source owner or stakeholder.")
    evidence.extend(f"Source field: {field}" for field in source_fields[:3])
    return _dedupe(evidence)


def _merge_candidate(
    candidates_by_key: dict[str, _Candidate],
    question: str,
    source_field: str,
    raw_context: Any,
) -> None:
    text = _clean_question(question)
    if not text:
        return
    key = _dedupe_key(text)
    context = " ".join(_strings(raw_context))
    existing = candidates_by_key.get(key)
    if existing is None:
        candidates_by_key[key] = _Candidate(
            question=text,
            source_fields=(source_field,),
            context=context,
        )
        return
    candidates_by_key[key] = _Candidate(
        question=existing.question,
        source_fields=tuple(_dedupe([*existing.source_fields, source_field])),
        context=" ".join(_dedupe([existing.context, context])),
    )


def _questions_from_value(value: Any) -> list[str]:
    questions: list[str] = []
    if isinstance(value, Mapping):
        text = (
            _optional_text(value.get("question"))
            or _optional_text(value.get("text"))
            or _optional_text(value.get("title"))
            or _optional_text(value.get("description"))
        )
        if text:
            questions.extend(_questions_from_text(text) or [text])
        return questions
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for item in items:
            questions.extend(_questions_from_value(item))
        return questions
    text = _optional_text(value)
    return _questions_from_text(text) if text else []


def _questions_from_texts(value: Any) -> list[str]:
    questions: list[str] = []
    for text in _strings(value):
        questions.extend(_questions_from_text(text))
    return questions


def _questions_from_text(text: str) -> list[str]:
    questions: list[str] = []
    for match in _QUESTION_RE.finditer(text):
        candidate = _clean_question(match.group(1))
        if candidate:
            questions.append(candidate)
    for line in text.splitlines():
        prompt = _PROMPT_RE.search(line)
        if prompt is None:
            continue
        prompt_text = prompt.group(1)
        prompt_questions = [
            question for question in _QUESTION_RE.findall(prompt_text) if _clean_question(question)
        ]
        if prompt_questions:
            questions.extend(_clean_question(question) for question in prompt_questions if _clean_question(question))
            continue
        candidate = _clean_question(prompt_text)
        if candidate:
            questions.append(candidate)
    return _dedupe(questions)


def _brief_values(brief: Mapping[str, Any]) -> list[tuple[str, Any]]:
    values: list[tuple[str, Any]] = []
    for field_name in ("open_questions", "questions", "risks", "constraints", "acceptance_criteria", "summary", "metadata"):
        if field_name in brief:
            values.append((field_name, brief[field_name]))
    payload = _mapping(brief.get("source_payload"))
    for field_name in (
        "open_questions",
        "questions",
        "unresolved_questions",
        "risks",
        "constraints",
        "acceptance_criteria",
        "acceptance",
        "criteria",
        "summary",
        "metadata",
        "normalized",
    ):
        if field_name in payload:
            values.append((f"source_payload.{field_name}", payload[field_name]))
    normalized = _mapping(payload.get("normalized"))
    for field_name in (
        "open_questions",
        "questions",
        "unresolved_questions",
        "risks",
        "constraints",
        "acceptance_criteria",
        "acceptance",
        "criteria",
        "summary",
        "metadata",
    ):
        if field_name in normalized:
            values.append((f"source_payload.normalized.{field_name}", normalized[field_name]))
    return values


def _source_briefs(
    source: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief],
) -> list[dict[str, Any]]:
    if source is None:
        return []
    if isinstance(source, SourceBrief):
        return [source.model_dump(mode="python")]
    if isinstance(source, Mapping):
        return [_source_brief_payload(source)]
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        return [dict(value)] if isinstance(value, Mapping) else []
    briefs: list[dict[str, Any]] = []
    for item in source:
        payload = _source_brief_payload(item)
        if payload:
            briefs.append(payload)
    return briefs


def _source_brief_payload(source_brief: Mapping[str, Any] | SourceBrief) -> dict[str, Any]:
    if isinstance(source_brief, SourceBrief):
        return source_brief.model_dump(mode="python")
    if hasattr(source_brief, "model_dump"):
        value = source_brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = SourceBrief.model_validate(source_brief).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(source_brief) if isinstance(source_brief, Mapping) else {}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


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


def _field_key(source_field: str) -> str:
    field = source_field.split(".")[-1]
    return re.sub(r"\[\d+\]$", "", field).casefold()


def _clean_question(value: str) -> str | None:
    text = _BULLET_RE.sub("", value.strip())
    text = _PROMPT_RE.sub(r"\1", text)
    text = text.strip(" \t\r\n-:*`\"'")
    text = " ".join(text.split())
    if not text:
        return None
    if not text.endswith("?"):
        text = text.rstrip(".") + "?"
    if len(_TOKEN_RE.findall(text)) < 2:
        return None
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _dedupe_key(value: str) -> str:
    return " ".join(_TOKEN_RE.findall(value.casefold()))


def _dedupe(values: Iterable[_T | None]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "QuestionPriority",
    "SourceUnresolvedQuestionPriorityRecord",
    "SourceUnresolvedQuestionPriorityReport",
    "build_source_unresolved_question_priority",
    "derive_source_unresolved_question_priority",
    "source_unresolved_question_priority_to_dict",
    "source_unresolved_question_priority_to_markdown",
]
