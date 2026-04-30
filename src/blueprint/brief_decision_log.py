"""Extract settled implementation decisions from implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief


DecisionCategory = Literal["architecture", "data", "scope", "validation", "operations"]
DecisionConfidence = Literal["high", "medium", "low"]

_FIELD_NAMES = (
    "architecture_notes",
    "data_requirements",
    "scope",
    "non_goals",
    "assumptions",
    "definition_of_done",
)
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|(?:\r?\n|;)+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_EXPLICIT_DECISION_RE = re.compile(
    r"\b(?:we\s+will|will\s+use|must\s+use|must|should|shall|use|adopt|"
    r"choose|chosen|decided|decision|require|requires|required|only|exclude|"
    r"out\s+of\s+scope|not\s+include|do\s+not|avoid|validate|verify|assume)\b",
    re.IGNORECASE,
)
_HIGH_CONFIDENCE_RE = re.compile(
    r"\b(?:decided|decision|chosen|must|shall|required|requires|only|will\s+use|"
    r"we\s+will|out\s+of\s+scope|non[-\s]?goal)\b",
    re.IGNORECASE,
)
_MEDIUM_CONFIDENCE_RE = re.compile(
    r"\b(?:should|use|adopt|prefer|assume|validate|verify|definition\s+of\s+done)\b",
    re.IGNORECASE,
)
_DATA_RE = re.compile(
    r"\b(?:data|database|schema|migration|table|field|payload|record|storage|"
    r"cache|persist|retention|dataset)\b",
    re.IGNORECASE,
)
_VALIDATION_RE = re.compile(
    r"\b(?:test|validate|validation|verify|acceptance|definition\s+of\s+done|"
    r"assert|pytest|coverage|smoke|regression)\b",
    re.IGNORECASE,
)
_OPERATIONS_RE = re.compile(
    r"\b(?:deploy|deployment|rollout|monitor|alert|logging|logs|metrics|"
    r"observability|rollback|feature\s+flag|runbook|operator|operations?)\b",
    re.IGNORECASE,
)
_ARCHITECTURE_RE = re.compile(
    r"\b(?:architecture|service|api|component|module|adapter|interface|contract|"
    r"library|framework|backend|frontend|queue|event|worker|integration)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class BriefDecisionRecord:
    """One implementation decision extracted from a brief."""

    decision_id: str
    category: DecisionCategory
    decision_text: str
    source_field: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: DecisionConfidence = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "id": self.decision_id,
            "category": self.category,
            "decision_text": self.decision_text,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
        }


@dataclass(slots=True)
class _DecisionDraft:
    decision_id: str
    category: DecisionCategory
    decision_text: str
    source_field: str
    evidence: list[str]
    confidence: DecisionConfidence


def extract_brief_decision_log(
    brief: Mapping[str, Any] | ImplementationBrief,
) -> tuple[BriefDecisionRecord, ...]:
    """Extract deterministic implementation decision records from a brief-like object."""
    payload = _brief_payload(brief)
    drafts: dict[str, _DecisionDraft] = {}

    for field_name in _FIELD_NAMES:
        for source_field, text in _field_texts(payload.get(field_name), field_name):
            decision_text = _decision_text(text, field_name)
            if not decision_text:
                continue

            category = _category(decision_text, field_name)
            dedupe_key = _dedupe_key(decision_text)
            evidence = f"{source_field}: {text}"
            confidence = _confidence(text, field_name)
            draft = drafts.get(dedupe_key)
            if draft is None:
                drafts[dedupe_key] = _DecisionDraft(
                    decision_id=f"decision-{category}-{_slug(decision_text) or len(drafts) + 1}",
                    category=category,
                    decision_text=decision_text,
                    source_field=source_field,
                    evidence=[evidence],
                    confidence=confidence,
                )
                continue

            draft.evidence.append(evidence)
            draft.confidence = _highest_confidence(draft.confidence, confidence)

    return tuple(
        BriefDecisionRecord(
            decision_id=draft.decision_id,
            category=draft.category,
            decision_text=draft.decision_text,
            source_field=draft.source_field,
            evidence=tuple(_dedupe(draft.evidence)),
            confidence=draft.confidence,
        )
        for draft in drafts.values()
    )


def brief_decision_log_to_dicts(
    decisions: tuple[BriefDecisionRecord, ...] | list[BriefDecisionRecord],
) -> list[dict[str, Any]]:
    """Serialize brief decision records to plain dictionaries."""
    return [decision.to_dict() for decision in decisions]


brief_decision_log_to_dicts.__test__ = False


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        if isinstance(brief, Mapping):
            return dict(brief)
    return {}


def _field_texts(value: Any, field_name: str) -> list[tuple[str, str]]:
    if isinstance(value, str):
        return [
            (field_name, text)
            for part in _SPLIT_RE.split(value)
            if (text := _clean_text(_BULLET_RE.sub("", part)))
        ]
    if isinstance(value, (list, tuple)):
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(value):
            if not isinstance(item, str):
                continue
            for part in _SPLIT_RE.split(item):
                text = _clean_text(_BULLET_RE.sub("", part))
                if text:
                    texts.append((f"{field_name}[{index}]", text))
        return texts
    if isinstance(value, set):
        return [
            (f"{field_name}[{index}]", text)
            for index, item in enumerate(sorted(value, key=str))
            if isinstance(item, str)
            if (text := _clean_text(_BULLET_RE.sub("", item)))
        ]
    return []


def _decision_text(text: str, field_name: str) -> str | None:
    if field_name == "non_goals":
        return _scope_exclusion_text(text)
    if field_name in {"scope", "definition_of_done"}:
        return text
    if _EXPLICIT_DECISION_RE.search(text):
        return text
    return None


def _scope_exclusion_text(text: str) -> str:
    lowered = text.casefold()
    if lowered.startswith(("exclude ", "do not ", "don't ", "avoid ")):
        return text
    return f"Exclude {text[0].lower()}{text[1:]}" if text else text


def _category(text: str, field_name: str) -> DecisionCategory:
    if field_name in {"scope", "non_goals"}:
        return "scope"
    if field_name == "data_requirements" or _DATA_RE.search(text):
        return "data"
    if field_name == "definition_of_done" or _VALIDATION_RE.search(text):
        return "validation"
    if _OPERATIONS_RE.search(text):
        return "operations"
    if field_name == "architecture_notes" or _ARCHITECTURE_RE.search(text):
        return "architecture"
    return "operations"


def _confidence(text: str, field_name: str) -> DecisionConfidence:
    if field_name == "non_goals" or _HIGH_CONFIDENCE_RE.search(text):
        return "high"
    if field_name in {"scope", "definition_of_done"} or _MEDIUM_CONFIDENCE_RE.search(text):
        return "medium"
    return "low"


def _highest_confidence(
    current: DecisionConfidence, candidate: DecisionConfidence
) -> DecisionConfidence:
    order = {"low": 0, "medium": 1, "high": 2}
    return candidate if order[candidate] > order[current] else current


def _dedupe_key(text: str) -> str:
    tokens = [
        _normalize_token(token)
        for token in _TOKEN_RE.findall(text.casefold())
        if token
        not in {
            "a",
            "an",
            "the",
            "we",
            "will",
            "must",
            "should",
            "shall",
            "use",
            "using",
            "require",
            "requires",
            "required",
            "decision",
            "decided",
            "choose",
            "chosen",
        }
    ]
    if tokens[:1] == ["exclude"]:
        tokens = tokens[1:]
    return " ".join(tokens)


def _normalize_token(token: str) -> str:
    synonyms = {
        "excluded": "exclude",
        "excluding": "exclude",
        "non": "exclude",
        "goal": "",
        "goals": "",
        "validated": "validate",
        "validates": "validate",
        "verified": "verify",
        "verifies": "verify",
        "tests": "test",
    }
    return synonyms.get(token, token)


def _slug(text: str) -> str:
    tokens = [_normalize_token(token) for token in _TOKEN_RE.findall(text.casefold())]
    tokens = [token for token in tokens if token and token not in {"we", "will", "must"}]
    return "-".join(tokens[:9])


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = _SPACE_RE.sub(" ", value).strip()
    return text or None


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "BriefDecisionRecord",
    "DecisionCategory",
    "DecisionConfidence",
    "brief_decision_log_to_dicts",
    "extract_brief_decision_log",
]
