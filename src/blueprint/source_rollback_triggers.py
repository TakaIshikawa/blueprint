"""Extract rollback trigger criteria from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


RollbackTriggerCategory = Literal[
    "error_rate",
    "latency",
    "data_integrity",
    "revenue_impact",
    "support_volume",
    "security_signal",
    "customer_complaints",
    "manual_decision",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_TRIGGER_RE = re.compile(
    r"\b(?:rollback|roll back|revert|abort|stop[- ]?loss|kill switch|disable|"
    r"turn off|halt|pause(?: rollout| launch| deploy(?:ment)?)?|back out|"
    r"no[- ]?go|do not ship|must not ship|exit criteria)\b",
    re.I,
)
_STRUCTURED_TRIGGER_FIELD_RE = re.compile(
    r"(?:rollback|roll_back|revert|abort|stop_loss|kill_switch|no_go|exit_criteria)",
    re.I,
)
_STRUCTURED_CONTEXT_FIELD_RE = re.compile(
    r"(?:acceptance|criteria|definition_of_done|done|risk|risks|validation|launch|rollout)",
    re.I,
)
_THRESHOLD_RE = re.compile(
    r"(?:[<>]=?|>=|<=|\bat\b|\babove\b|\bbelow\b|\bexceeds?\b|\breaches?\b|"
    r"\bmore than\b|\bless than\b|\bover\b|\bunder\b|\bspikes?\b|\bdrops?\b)"
    r"[^.\n;:]{0,48}?(?:\d+(?:\.\d+)?\s*(?:%|ms|s|sec|seconds?|x|k|m|bps|pts?)?|\bp\d{2}\b)",
    re.I,
)

_CATEGORY_ORDER: dict[RollbackTriggerCategory, int] = {
    "error_rate": 0,
    "latency": 1,
    "data_integrity": 2,
    "revenue_impact": 3,
    "support_volume": 4,
    "security_signal": 5,
    "customer_complaints": 6,
    "manual_decision": 7,
}
_CATEGORY_PATTERNS: dict[RollbackTriggerCategory, re.Pattern[str]] = {
    "error_rate": re.compile(
        r"\b(?:error rate|errors?|5xx|4xx|exception(?:s)?|failure rate|failed requests?|crash(?:es)?)\b",
        re.I,
    ),
    "latency": re.compile(
        r"\b(?:latency|p95|p99|response time|timeout(?:s)?|time to first byte|ttfb|slow(?:down)?)\b",
        re.I,
    ),
    "data_integrity": re.compile(
        r"\b(?:data integrity|data loss|corrupt(?:ion|ed)?|incorrect data|bad data|"
        r"duplicate(?:s)?|mismatch(?:ed|es)?|reconciliation|orphan(?:ed)? records?)\b",
        re.I,
    ),
    "revenue_impact": re.compile(
        r"\b(?:revenue|conversion|checkout|payment(?:s)?|billing|purchase(?:s)?|"
        r"bookings?|gmv|arr|mrr|paid signup(?:s)?)\b",
        re.I,
    ),
    "support_volume": re.compile(
        r"\b(?:support volume|support tickets?|ticket volume|helpdesk|escalation(?:s)?|"
        r"contact rate|case volume)\b",
        re.I,
    ),
    "security_signal": re.compile(
        r"\b(?:security|auth(?:entication|orization)?|permission(?:s)?|fraud|abuse|"
        r"vulnerabilit(?:y|ies)|suspicious|incident|credential(?:s)?|token(?:s)?)\b",
        re.I,
    ),
    "customer_complaints": re.compile(
        r"\b(?:customer complaints?|complaints?|negative feedback|csat|nps|churn risk|"
        r"angry customers?|social complaints?)\b",
        re.I,
    ),
    "manual_decision": re.compile(
        r"\b(?:manual decision|manual approval|human approval|go/no[- ]go|go no[- ]go|"
        r"launch review|exec(?:utive)? review|product approval|security approval|sign[- ]?off)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[RollbackTriggerCategory, str] = {
    "error_rate": "engineering_oncall",
    "latency": "engineering_oncall",
    "data_integrity": "data_owner",
    "revenue_impact": "product_owner",
    "support_volume": "support_lead",
    "security_signal": "security_owner",
    "customer_complaints": "customer_success_lead",
    "manual_decision": "release_owner",
}
_PLAN_NOTE_BY_CATEGORY: dict[RollbackTriggerCategory, str] = {
    "error_rate": "Add a rollback gate for error-rate regression and assign monitoring ownership.",
    "latency": "Add a rollback gate for latency regression and define the watched percentile.",
    "data_integrity": "Add a rollback gate for data-integrity anomalies with reconciliation checks.",
    "revenue_impact": "Add a rollback gate for revenue or conversion impact during rollout.",
    "support_volume": "Add a rollback gate for support-volume spikes and escalation routing.",
    "security_signal": "Add an abort gate for security signals and incident-response ownership.",
    "customer_complaints": "Add a rollback gate for customer complaint trends and feedback review.",
    "manual_decision": "Add a manual go/no-go checkpoint before continuing rollout.",
}


@dataclass(frozen=True, slots=True)
class SourceRollbackTrigger:
    """One rollback trigger category found in brief evidence."""

    category: RollbackTriggerCategory
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_owner: str = ""
    suggested_plan_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "recommended_owner": self.recommended_owner,
            "suggested_plan_note": self.suggested_plan_note,
        }


@dataclass(frozen=True, slots=True)
class SourceRollbackTriggerReport:
    """Source-brief rollback trigger report before implementation planning."""

    brief_id: str | None = None
    triggers: tuple[SourceRollbackTrigger, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "triggers": [trigger.to_dict() for trigger in self.triggers],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return rollback triggers as plain dictionaries."""
        return [trigger.to_dict() for trigger in self.triggers]

    @property
    def records(self) -> tuple[SourceRollbackTrigger, ...]:
        """Compatibility view matching reports that name extracted items records."""
        return self.triggers


def build_source_rollback_trigger_report(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceRollbackTriggerReport:
    """Build a rollback trigger report from brief-shaped input."""
    brief_id, payload = _source_payload(source)
    triggers = tuple(_merge_triggers(_trigger_candidates(payload)))
    return SourceRollbackTriggerReport(
        brief_id=brief_id,
        triggers=triggers,
        summary={
            "trigger_count": len(triggers),
            "high_confidence_count": sum(1 for trigger in triggers if trigger.confidence >= 0.85),
            "category_counts": {
                category: sum(1 for trigger in triggers if trigger.category == category)
                for category in _CATEGORY_ORDER
            },
            "categories": [trigger.category for trigger in triggers],
        },
    )


def extract_source_rollback_triggers(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceRollbackTrigger, ...]:
    """Return rollback triggers extracted from brief-shaped input."""
    return build_source_rollback_trigger_report(source).triggers


def source_rollback_trigger_report_to_dict(
    result: SourceRollbackTriggerReport,
) -> dict[str, Any]:
    """Serialize a source rollback trigger report to a plain dictionary."""
    return result.to_dict()


source_rollback_trigger_report_to_dict.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: RollbackTriggerCategory
    confidence: float
    evidence: str


def _trigger_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_field, value in _brief_values(payload):
        field_context = _field_context(source_field)
        for segment, section_context in _segments(value):
            categories = _categories(segment)
            if not categories:
                continue
            trigger_context = _trigger_context(segment, field_context, section_context)
            if not trigger_context:
                continue
            evidence = _evidence_snippet(source_field, segment)
            for category in categories:
                candidates.append(
                    _Candidate(
                        category=category,
                        confidence=_confidence(segment, field_context, section_context),
                        evidence=evidence,
                    )
                )
    return candidates


def _merge_triggers(candidates: Iterable[_Candidate]) -> list[SourceRollbackTrigger]:
    by_category: dict[RollbackTriggerCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    triggers: list[SourceRollbackTrigger] = []
    for category in _CATEGORY_ORDER:
        category_candidates = by_category.get(category, [])
        if not category_candidates:
            continue
        evidence = tuple(_dedupe_evidence(candidate.evidence for candidate in category_candidates))[:4]
        confidence = round(max(candidate.confidence for candidate in category_candidates), 2)
        triggers.append(
            SourceRollbackTrigger(
                category=category,
                confidence=confidence,
                evidence=evidence,
                recommended_owner=_OWNER_BY_CATEGORY[category],
                suggested_plan_note=_PLAN_NOTE_BY_CATEGORY[category],
            )
        )
    return triggers


def _source_payload(source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = source.model_dump(mode="python")
        return _brief_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _brief_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                value = model.model_validate(source).model_dump(mode="python")
                return _brief_id(value), dict(value)
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _brief_id(payload), payload
    return None, {}


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return _optional_text(payload.get("id")) or _optional_text(payload.get("source_id"))


def _brief_values(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "problem_statement",
        "mvp_goal",
        "workflow_context",
        "architecture_notes",
        "data_requirements",
        "validation_plan",
    ):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for field_name in (
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "risks",
        "definition_of_done",
        "acceptance",
        "acceptance_criteria",
        "criteria",
    ):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for field_name in ("source_payload", "metadata"):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited:
            continue
        _append_value(values, str(key), payload[key])
    return values


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            _append_value(values, f"{source_field}.{key}", value[key])
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        values.append((source_field, text))


def _segments(value: str) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    in_rollback_section = False
    for raw_line in value.splitlines() or [value]:
        line = raw_line.strip()
        if not line:
            continue
        heading = _HEADING_RE.match(line)
        if heading:
            title = _clean_text(heading.group("title"))
            in_rollback_section = bool(_TRIGGER_RE.search(title) or _STRUCTURED_TRIGGER_FIELD_RE.search(title))
            if title:
                segments.append((title, in_rollback_section))
            continue
        bullet_text = _clean_text(line)
        parts = [bullet_text] if _BULLET_RE.match(line) else _sentence_parts(bullet_text)
        for part in parts:
            if part:
                segments.append((part, in_rollback_section))
    if not segments:
        text = _clean_text(value)
        return [(text, False)] if text else []
    return segments


def _sentence_parts(value: str) -> list[str]:
    return [_clean_text(part) for part in _SENTENCE_SPLIT_RE.split(value) if _clean_text(part)]


def _categories(text: str) -> list[RollbackTriggerCategory]:
    return [category for category, pattern in _CATEGORY_PATTERNS.items() if pattern.search(text)]


def _field_context(source_field: str) -> tuple[bool, bool]:
    normalized = source_field.replace("-", "_")
    return (
        bool(_STRUCTURED_TRIGGER_FIELD_RE.search(normalized)),
        bool(_STRUCTURED_CONTEXT_FIELD_RE.search(normalized)),
    )


def _trigger_context(text: str, field_context: tuple[bool, bool], section_context: bool) -> bool:
    has_trigger_field, has_structured_context = field_context
    return bool(
        _TRIGGER_RE.search(text)
        or section_context
        or has_trigger_field
        or (has_structured_context and _TRIGGER_RE.search(text))
    )


def _confidence(text: str, field_context: tuple[bool, bool], section_context: bool) -> float:
    has_trigger_field, has_structured_context = field_context
    score = 0.68
    if _TRIGGER_RE.search(text) or has_trigger_field:
        score += 0.12
    if _THRESHOLD_RE.search(text):
        score += 0.1
    if section_context:
        score += 0.06
    if has_structured_context:
        score += 0.04
    return round(min(score, 0.95), 2)


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _SPACE_RE.sub(" ", str(value)).strip()
    return text or None


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "RollbackTriggerCategory",
    "SourceRollbackTrigger",
    "SourceRollbackTriggerReport",
    "build_source_rollback_trigger_report",
    "extract_source_rollback_triggers",
    "source_rollback_trigger_report_to_dict",
]
