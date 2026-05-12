"""Extract source-level incident communication requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


IncidentCommunicationRequirementType = Literal[
    "audience",
    "channel",
    "severity_threshold",
    "notification_timing",
    "status_page",
    "customer_support_handoff",
    "regulatory_notice",
    "owner",
    "template",
    "post_incident_communication",
]
IncidentCommunicationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[IncidentCommunicationRequirementType, ...] = (
    "audience",
    "channel",
    "severity_threshold",
    "notification_timing",
    "status_page",
    "customer_support_handoff",
    "regulatory_notice",
    "owner",
    "template",
    "post_incident_communication",
)
_CONFIDENCE_ORDER: dict[IncidentCommunicationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_INCIDENT_CONTEXT_RE = re.compile(r"\b(?:incident|outage|degradation|sev[ -]?[0-4]|postmortem|post-incident|breach)\b", re.I)
_NO_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:incident|outage|notification|status page|regulatory notice)\b"
    r".{0,100}\b(?:scope|required|needed|changes?|impact)\b",
    re.I,
)
_SCANNED_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "constraints",
    "risks",
    "support",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_TYPE_PATTERNS: dict[IncidentCommunicationRequirementType, re.Pattern[str]] = {
    "audience": re.compile(r"\b(?:audience|customers?|tenants?|partners?|internal stakeholders?|subscribers?|affected users?)\b", re.I),
    "channel": re.compile(r"\b(?:email|sms|slack|pagerduty|in-app|push notification|support ticket|communication channel|notify via)\b", re.I),
    "severity_threshold": re.compile(r"\b(?:severity threshold|sev[ -]?[0-4]|p[0-4]|critical incident|major incident|severity level|customer-impacting)\b", re.I),
    "notification_timing": re.compile(r"\b(?:within \d+\s*(?:minutes?|hours?|days?)|notify within|initial notice|update cadence|every \d+\s*(?:minutes?|hours?)|timing|sla)\b", re.I),
    "status_page": re.compile(r"\b(?:status page|public status|statuspage|incident page|component status)\b", re.I),
    "customer_support_handoff": re.compile(r"\b(?:support handoff|customer support|support macro|support runbook|cs handoff|ticket routing|support queue)\b", re.I),
    "regulatory_notice": re.compile(r"\b(?:regulatory notice|regulator|breach notification|legal notice|gdpr|hipaa|sec notification|compliance notice|authority)\b", re.I),
    "owner": re.compile(r"\b(?:incident commander|communications owner|comms owner|owner|responsible team|dri|on-call|approver)\b", re.I),
    "template": re.compile(r"\b(?:templates?|message templates?|notification copy|pre-approved copy|macro|customer email draft)\b", re.I),
    "post_incident_communication": re.compile(r"\b(?:post-incident|post incident|postmortem|rca|root cause|incident report|follow-up communication|closure notice)\b", re.I),
}
_VALUE_RE = re.compile(
    r"\b(?:sev[ -]?[0-4]|p[0-4]|\d+\s*(?:minutes?|hours?|days?)|email|sms|slack|status page|gdpr|hipaa|postmortem)\b",
    re.I,
)
_FOLLOW_UP: dict[IncidentCommunicationRequirementType, str] = {
    "audience": "Confirm which customer, partner, regulator, and internal audiences receive each incident communication.",
    "channel": "Define primary and fallback channels for incident updates.",
    "severity_threshold": "Map severity levels to communication obligations.",
    "notification_timing": "Specify initial notification deadlines and update cadence.",
    "status_page": "Clarify status page ownership, component mapping, and publication rules.",
    "customer_support_handoff": "Document support handoff, macros, and escalation paths.",
    "regulatory_notice": "Confirm legal review and jurisdiction-specific notice deadlines.",
    "owner": "Assign a communications owner and backup approver.",
    "template": "Create approved templates for initial, update, resolved, and post-incident messages.",
    "post_incident_communication": "Define post-incident report timing, audience, and approval flow.",
}


@dataclass(frozen=True, slots=True)
class SourceIncidentCommunicationRequirement:
    requirement_type: IncidentCommunicationRequirementType
    source_field: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: IncidentCommunicationConfidence = "medium"
    value: str | None = None
    recommended_follow_up: tuple[str, ...] = field(default_factory=tuple)

    @property
    def category(self) -> IncidentCommunicationRequirementType:
        return self.requirement_type

    def to_dict(self) -> dict[str, Any]:
        return {
            "requirement_type": self.requirement_type,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "recommended_follow_up": list(self.recommended_follow_up),
        }


@dataclass(frozen=True, slots=True)
class SourceIncidentCommunicationRequirementsReport:
    source_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceIncidentCommunicationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceIncidentCommunicationRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceIncidentCommunicationRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [item.to_dict() for item in self.requirements],
            "records": [item.to_dict() for item in self.records],
            "findings": [item.to_dict() for item in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source Incident Communication Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Timing coverage: {self.summary.get('timing_coverage', 0)}%",
            f"- Audience coverage: {self.summary.get('audience_coverage', 0)}%",
            f"- Channel coverage: {self.summary.get('channel_coverage', 0)}%",
            f"- Ownership coverage: {self.summary.get('ownership_coverage', 0)}%",
            f"- Regulatory notice coverage: {self.summary.get('regulatory_notice_coverage', 0)}%",
        ]
        if not self.requirements:
            lines.extend(["", "No incident communication requirements were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Type | Confidence | Source | Evidence |", "| --- | --- | --- | --- |"])
        for item in self.requirements:
            lines.append(f"| {item.requirement_type} | {item.confidence} | {_markdown_cell(item.source_field)} | {_markdown_cell('; '.join(item.evidence))} |")
        return "\n".join(lines)


def build_source_incident_communication_requirements(source: Any) -> SourceIncidentCommunicationRequirementsReport:
    source_id, title, payload = _source_payload(source)
    grouped = _group(payload)
    requirements = tuple(_merge(grouped))
    return SourceIncidentCommunicationRequirementsReport(
        source_id=source_id,
        title=title,
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_incident_communication_requirements(source: Any) -> tuple[SourceIncidentCommunicationRequirement, ...]:
    return build_source_incident_communication_requirements(source).requirements


def derive_source_incident_communication_requirements(source: Any) -> SourceIncidentCommunicationRequirementsReport:
    return build_source_incident_communication_requirements(source)


def generate_source_incident_communication_requirements(source: Any) -> SourceIncidentCommunicationRequirementsReport:
    return build_source_incident_communication_requirements(source)


def summarize_source_incident_communication_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceIncidentCommunicationRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_incident_communication_requirements(source_or_report).summary


def source_incident_communication_requirements_to_dict(report: SourceIncidentCommunicationRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_incident_communication_requirements_to_dict.__test__ = False


def source_incident_communication_requirements_to_dicts(items: Any) -> list[dict[str, Any]]:
    if isinstance(items, SourceIncidentCommunicationRequirementsReport):
        return items.to_dicts()
    return [item.to_dict() for item in items]


source_incident_communication_requirements_to_dicts.__test__ = False


def source_incident_communication_requirements_to_markdown(report: SourceIncidentCommunicationRequirementsReport) -> str:
    return report.to_markdown()


source_incident_communication_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: IncidentCommunicationRequirementType
    source_field: str
    evidence: str
    confidence: IncidentCommunicationConfidence
    value: str | None


def _source_payload(source: Any) -> tuple[str | None, str | None, Mapping[str, Any]]:
    if isinstance(source, str):
        return None, None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), _optional_text(payload.get("title")), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _source_id(payload), _optional_text(payload.get("title")), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
        return _source_id(payload), _optional_text(payload.get("title")), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = {name: getattr(source, name) for name in _SCANNED_FIELDS + ("id", "source_id", "source_brief_id") if hasattr(source, name)}
        return _source_id(payload), _optional_text(payload.get("title")), payload
    return None, None, {}


def _group(payload: Mapping[str, Any]) -> dict[IncidentCommunicationRequirementType, list[_Candidate]]:
    grouped: dict[IncidentCommunicationRequirementType, list[_Candidate]] = {}
    for field, text in _candidate_texts(payload):
        for segment in _segments(text):
            if _NO_SCOPE_RE.search(segment):
                continue
            matched = [name for name in _TYPE_ORDER if _TYPE_PATTERNS[name].search(segment)]
            if not matched:
                continue
            confidence: IncidentCommunicationConfidence = "high" if _INCIDENT_CONTEXT_RE.search(segment) else "medium"
            for requirement_type in matched:
                grouped.setdefault(requirement_type, []).append(
                    _Candidate(requirement_type, field, _evidence(field, segment), confidence, _value(segment))
                )
    return grouped


def _merge(grouped: Mapping[IncidentCommunicationRequirementType, list[_Candidate]]) -> list[SourceIncidentCommunicationRequirement]:
    records: list[SourceIncidentCommunicationRequirement] = []
    for requirement_type in _TYPE_ORDER:
        candidates = grouped.get(requirement_type, [])
        if not candidates:
            continue
        fields = sorted(_dedupe(candidate.source_field for candidate in candidates), key=str.casefold)
        confidence = sorted((candidate.confidence for candidate in candidates), key=lambda item: _CONFIDENCE_ORDER[item])[0]
        values = _dedupe(candidate.value for candidate in candidates if candidate.value)
        records.append(
            SourceIncidentCommunicationRequirement(
                requirement_type=requirement_type,
                source_field=fields[0],
                evidence=tuple(_dedupe_evidence(candidate.evidence for candidate in candidates))[:5],
                confidence=confidence,
                value=values[0] if values else None,
                recommended_follow_up=(_FOLLOW_UP[requirement_type],),
            )
        )
    return records


def _summary(requirements: tuple[SourceIncidentCommunicationRequirement, ...]) -> dict[str, Any]:
    types = {item.requirement_type for item in requirements}
    return {
        "requirement_count": len(requirements),
        "requirement_types": [item.requirement_type for item in requirements],
        "type_counts": {name: sum(1 for item in requirements if item.requirement_type == name) for name in _TYPE_ORDER},
        "confidence_counts": {name: sum(1 for item in requirements if item.confidence == name) for name in _CONFIDENCE_ORDER},
        "timing_coverage": 100 if "notification_timing" in types else 0,
        "audience_coverage": 100 if "audience" in types else 0,
        "channel_coverage": 100 if "channel" in types else 0,
        "ownership_coverage": 100 if "owner" in types else 0,
        "regulatory_notice_coverage": 100 if "regulatory_notice" in types else 0,
        "status": "ready_for_planning" if requirements else "no_incident_communication_language",
    }


def _candidate_texts(payload: Mapping[str, Any], prefix: str | None = None) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for key in sorted(payload, key=str):
        if prefix is None and key not in _SCANNED_FIELDS:
            continue
        value = payload[key]
        field = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, str):
            if text := _optional_text(value):
                texts.append((field, text))
        elif isinstance(value, Mapping):
            texts.extend(_candidate_texts(value, field))
        elif isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
            for index, item in enumerate(value):
                if isinstance(item, Mapping):
                    texts.extend(_candidate_texts(item, f"{field}[{index}]"))
                elif text := _optional_text(item):
                    texts.append((f"{field}[{index}]", text))
    return texts


def _segments(text: str) -> list[str]:
    return [cleaned for part in _SPLIT_RE.split(text) if (cleaned := _clean(part))]


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return _optional_text(payload.get("id") or payload.get("source_brief_id") or payload.get("source_id"))


def _value(text: str) -> str | None:
    match = _VALUE_RE.search(text)
    return _clean(match.group(0)).casefold() if match else None


def _evidence(field: str, text: str) -> str:
    cleaned = _clean(text)
    if len(cleaned) > 220:
        cleaned = f"{cleaned[:217].rstrip()}..."
    return f"{field}: {cleaned}"


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean(str(value))
    return text or None


def _clean(value: str) -> str:
    return _SPACE_RE.sub(" ", _BULLET_RE.sub("", str(value).strip())).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[str] = set()
    for value in values:
        key = str(value).casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        _, _, statement = value.partition(": ")
        key = _clean(statement or value).casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return sorted(result, key=str.casefold)


def _markdown_cell(value: str) -> str:
    return _clean(value).replace("|", "\\|").replace("\n", " ")


__all__ = [
    "SourceIncidentCommunicationRequirement",
    "SourceIncidentCommunicationRequirementsReport",
    "build_source_incident_communication_requirements",
    "derive_source_incident_communication_requirements",
    "extract_source_incident_communication_requirements",
    "generate_source_incident_communication_requirements",
    "source_incident_communication_requirements_to_dict",
    "source_incident_communication_requirements_to_dicts",
    "source_incident_communication_requirements_to_markdown",
    "summarize_source_incident_communication_requirements",
]
