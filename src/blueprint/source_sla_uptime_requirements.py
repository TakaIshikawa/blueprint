"""Extract source-level SLA, uptime, and availability requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


SlaUptimeRequirementLabel = Literal[
    "uptime_percentage",
    "availability_expectation",
    "sla_credit",
    "incident_credit",
    "response_time_commitment",
    "maintenance_window",
]
SlaUptimeConfidence = Literal["high", "medium", "low"]

_LABEL_ORDER: tuple[SlaUptimeRequirementLabel, ...] = (
    "uptime_percentage",
    "availability_expectation",
    "sla_credit",
    "incident_credit",
    "response_time_commitment",
    "maintenance_window",
)
_CONFIDENCE_ORDER: dict[SlaUptimeConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"guarantee|commit(?:ted|ment)?|target|objective|support|provide|meet|within|"
    r"no more than|under|scheduled|published|excluded)\b",
    re.I,
)
_SLA_CONTEXT_RE = re.compile(
    r"\b(?:sla|service level agreement|service level objective|slo|availability|"
    r"available|uptime|downtime|high availability|ha|24/7|24x7|always on|"
    r"maintenance window|maintenance windows|scheduled maintenance|planned maintenance|"
    r"service credit|sla credit|credits?|refund|penalty|incident credit|"
    r"response time|respond within|initial response|acknowledged?|acknowledgement|"
    r"acknowledgment|p1|p2|sev(?:erity)?\s*[0-9]|critical incident)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:sla|slo|uptime|availability|maintenance|support|incident|response|"
    r"service_level|service level|requirements?|constraints?|acceptance|criteria|operations?)",
    re.I,
)
_PERCENT_RE = re.compile(
    r"\b(?P<value>99(?:\.\d+)?\s*%|100\s*%|\d+(?:\.\d+)?\s*percent)",
    re.I,
)
_TIME_VALUE_RE = re.compile(
    r"\b(?P<value>(?:within|under|less than|no more than|<=?|up to)?\s*"
    r"\d+(?:\.\d+)?\s*(?:ms|milliseconds?|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?))\b",
    re.I,
)
_WINDOW_VALUE_RE = re.compile(
    r"\b(?P<value>(?:(?:sunday|monday|tuesday|wednesday|thursday|friday|saturday)\s+)?"
    r"(?:\d{1,2}(?::\d{2})?\s*(?:am|pm)?\s*(?:-|to|through)\s*"
    r"\d{1,2}(?::\d{2})?\s*(?:am|pm)?(?:\s*[A-Z]{2,4})?|"
    r"(?:daily|weekly|monthly|sunday|monday|tuesday|wednesday|thursday|friday|saturday)"
    r"[^.?!]{0,50}(?:window|maintenance)))\b",
    re.I,
)
_LABEL_PATTERNS: dict[SlaUptimeRequirementLabel, re.Pattern[str]] = {
    "uptime_percentage": re.compile(
        r"\b(?:uptime|availability|sla|slo|service level objective|service level agreement)"
        r"\b[^.?!]{0,90}\b(?:99(?:\.\d+)?\s*%|100\s*%|\d+(?:\.\d+)?\s*percent)|"
        r"\b(?:99(?:\.\d+)?\s*%|100\s*%|\d+(?:\.\d+)?\s*percent)[^.?!]{0,60}"
        r"\b(?:uptime|availability|sla|slo)\b",
        re.I,
    ),
    "availability_expectation": re.compile(
        r"\b(?:availability|available|high availability|ha|uptime|downtime|always on|"
        r"24/7|24x7|business critical|mission critical)\b",
        re.I,
    ),
    "sla_credit": re.compile(
        r"\b(?:sla credit|service credits?|service level credits?|availability credits?|"
        r"credits? for downtime|refunds?|penalt(?:y|ies))\b",
        re.I,
    ),
    "incident_credit": re.compile(
        r"\b(?:incident credits?|credits? for incidents?|outage credits?|downtime credits?|"
        r"post[- ]incident credits?)\b",
        re.I,
    ),
    "response_time_commitment": re.compile(
        r"\b(?:response time|respond within|initial response|acknowledged?|"
        r"acknowledgement|acknowledgment|time to respond|p1|p2|sev(?:erity)?\s*[0-9]|"
        r"critical incident)\b",
        re.I,
    ),
    "maintenance_window": re.compile(
        r"\b(?:maintenance windows?|scheduled maintenance|planned maintenance|"
        r"maintenance period|downtime window|change window|release window)\b",
        re.I,
    ),
}
_PLANNING_NOTES: dict[SlaUptimeRequirementLabel, str] = {
    "uptime_percentage": "Translate the uptime target into measurable SLIs, error budgets, monitoring, and reporting.",
    "availability_expectation": "Confirm availability scope, excluded dependencies, redundancy, and operational ownership.",
    "sla_credit": "Define credit triggers, calculation rules, exclusions, approval flow, and customer communication.",
    "incident_credit": "Define outage or incident credit eligibility, evidence, approval, and support workflow.",
    "response_time_commitment": "Define response targets by severity, escalation paths, timers, and audit evidence.",
    "maintenance_window": "Document allowed windows, customer notice timing, exclusions, and deployment constraints.",
}
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "risks",
    "operations",
    "support",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_IGNORED_FIELDS = {
    "id",
    "source_brief_id",
    "source_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
    "source_links",
}


@dataclass(frozen=True, slots=True)
class SourceSlaUptimeRequirement:
    """One source-backed SLA, uptime, or availability requirement."""

    requirement_label: SlaUptimeRequirementLabel
    value: str | None = None
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: SlaUptimeConfidence = "medium"
    planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_label": self.requirement_label,
            "value": self.value,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceSlaUptimeRequirementsReport:
    """Source-level SLA and uptime requirements report."""

    source_brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceSlaUptimeRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSlaUptimeRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return SLA and uptime requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source SLA Uptime Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        label_counts = self.summary.get("requirement_label_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement label counts: "
            + ", ".join(f"{label} {label_counts.get(label, 0)}" for label in _LABEL_ORDER),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source SLA uptime requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Label | Value | Confidence | Source Field | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_label} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} |"
            )
        return "\n".join(lines)


def build_source_sla_uptime_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceSlaUptimeRequirementsReport:
    """Build an SLA and uptime requirements report from a source brief-like payload."""
    source_brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceSlaUptimeRequirementsReport(
        source_brief_id=source_brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_sla_uptime_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceSlaUptimeRequirementsReport:
    """Compatibility helper for callers that use summarize_* naming."""
    return build_source_sla_uptime_requirements(source)


def derive_source_sla_uptime_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceSlaUptimeRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_sla_uptime_requirements(source)


def generate_source_sla_uptime_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceSlaUptimeRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_sla_uptime_requirements(source)


def extract_source_sla_uptime_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceSlaUptimeRequirement, ...]:
    """Return SLA and uptime requirement records from brief-shaped input."""
    return build_source_sla_uptime_requirements(source).requirements


def source_sla_uptime_requirements_to_dict(
    report: SourceSlaUptimeRequirementsReport,
) -> dict[str, Any]:
    """Serialize an SLA and uptime requirements report to a plain dictionary."""
    return report.to_dict()


source_sla_uptime_requirements_to_dict.__test__ = False


def source_sla_uptime_requirements_to_dicts(
    requirements: (
        tuple[SourceSlaUptimeRequirement, ...]
        | list[SourceSlaUptimeRequirement]
        | SourceSlaUptimeRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize SLA and uptime requirement records to dictionaries."""
    if isinstance(requirements, SourceSlaUptimeRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_sla_uptime_requirements_to_dicts.__test__ = False


def source_sla_uptime_requirements_to_markdown(
    report: SourceSlaUptimeRequirementsReport,
) -> str:
    """Render an SLA and uptime requirements report as Markdown."""
    return report.to_markdown()


source_sla_uptime_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_label: SlaUptimeRequirementLabel
    value: str | None
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: SlaUptimeConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        try:
            value = SourceBrief.model_validate(source).model_dump(mode="python")
            payload = dict(value)
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
        return _source_id(payload), payload
    return None, _object_payload(source)


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_field, segment in _candidate_segments(payload):
        if not _is_sla_signal(source_field, segment):
            continue
        labels = _requirement_labels(source_field, segment)
        if not labels:
            continue
        if source_field == "title" and not (
            _REQUIRED_RE.search(segment) or _PERCENT_RE.search(segment)
        ):
            continue
        evidence = _evidence_snippet(source_field, segment)
        for label in labels:
            value = _value(segment, (label,))
            candidates.append(
                _Candidate(
                    requirement_label=label,
                    value=value,
                    source_field=source_field,
                    evidence=evidence,
                    matched_terms=_matched_terms(label, source_field, segment),
                    confidence=_confidence(source_field, segment, value),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceSlaUptimeRequirement]:
    grouped: dict[SlaUptimeRequirementLabel, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.requirement_label, []).append(candidate)

    requirements: list[SourceSlaUptimeRequirement] = []
    for label, items in grouped.items():
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda item: item.casefold(),
        )[0]
        confidence = min(
            (item.confidence for item in items), key=lambda item: _CONFIDENCE_ORDER[item]
        )
        requirements.append(
            SourceSlaUptimeRequirement(
                requirement_label=label,
                value=_best_value(items),
                source_field=source_field,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                matched_terms=tuple(
                    sorted(
                        _dedupe(term for item in items for term in item.matched_terms),
                        key=str.casefold,
                    )
                ),
                confidence=confidence,
                planning_note=_PLANNING_NOTES[label],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CONFIDENCE_ORDER[requirement.confidence],
            _LABEL_ORDER.index(requirement.requirement_label),
            requirement.value or "",
            requirement.source_field.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key], False)
    return [(field, segment) for field, segment in values if segment]


def _append_value(
    values: list[tuple[str, str]],
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _SLA_CONTEXT_RE.search(key_text)
            )
            if child_context and _SLA_CONTEXT_RE.search(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            if field_context or _SLA_CONTEXT_RE.search(segment) or _PERCENT_RE.search(segment):
                values.append((source_field, segment))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for raw_line in value.splitlines() or [value]:
        cleaned = _clean_text(raw_line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(raw_line) or _CHECKBOX_RE.match(raw_line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _is_sla_signal(source_field: str, text: str) -> bool:
    searchable = f"{_field_words(source_field)} {text}"
    if not _SLA_CONTEXT_RE.search(searchable):
        return False
    if _PERCENT_RE.search(text) and re.search(
        r"\b(?:uptime|availability|sla|slo)\b", searchable, re.I
    ):
        return True
    if _LABEL_PATTERNS["maintenance_window"].search(searchable):
        return True
    if _LABEL_PATTERNS["sla_credit"].search(searchable):
        return True
    if _LABEL_PATTERNS["incident_credit"].search(searchable):
        return True
    if _LABEL_PATTERNS["response_time_commitment"].search(searchable):
        return bool(_TIME_VALUE_RE.search(text) or _REQUIRED_RE.search(text))
    return bool(
        _REQUIRED_RE.search(text) or _STRUCTURED_FIELD_RE.search(_field_words(source_field))
    )


def _requirement_labels(source_field: str, text: str) -> tuple[SlaUptimeRequirementLabel, ...]:
    searchable = f"{_field_words(source_field)} {text}"
    labels = [label for label in _LABEL_ORDER if _LABEL_PATTERNS[label].search(searchable)]
    return tuple(_dedupe(labels))


def _value(text: str, labels: Iterable[SlaUptimeRequirementLabel]) -> str | None:
    label_set = set(labels)
    if "uptime_percentage" in label_set:
        if match := _PERCENT_RE.search(text):
            return _clean_text(match.group("value"))
    if "response_time_commitment" in label_set:
        if match := _TIME_VALUE_RE.search(text):
            return _clean_text(match.group("value"))
    if "maintenance_window" in label_set:
        if match := _WINDOW_VALUE_RE.search(text):
            return _clean_text(match.group("value"))
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (0 if re.search(r"\d", value) else 1, len(value), value.casefold()),
    )
    return values[0] if values else None


def _matched_terms(
    label: SlaUptimeRequirementLabel,
    source_field: str,
    text: str,
) -> tuple[str, ...]:
    searchable = f"{_field_words(source_field)} {text}"
    return tuple(
        _dedupe(
            _clean_text(match.group(0)) for match in _LABEL_PATTERNS[label].finditer(searchable)
        )
    )


def _confidence(source_field: str, text: str, value: str | None) -> SlaUptimeConfidence:
    normalized_field = source_field.replace("-", "_").casefold()
    if value and (
        _REQUIRED_RE.search(text)
        or any(
            marker in normalized_field
            for marker in ("success_criteria", "acceptance_criteria", "constraint", "sla")
        )
    ):
        return "high"
    if _REQUIRED_RE.search(text) or value:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceSlaUptimeRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "requirement_labels": [requirement.requirement_label for requirement in requirements],
        "requirement_label_counts": {
            label: sum(1 for requirement in requirements if requirement.requirement_label == label)
            for label in _LABEL_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "status": "ready_for_planning" if requirements else "no_sla_uptime_language",
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
        "problem",
        "problem_statement",
        "goal",
        "goals",
        "mvp_goal",
        "context",
        "workflow_context",
        "requirements",
        "constraints",
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "risks",
        "operations",
        "support",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


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
    return sorted(deduped, key=lambda item: item.casefold())


def _dedupe(values: Iterable[Any]) -> list[Any]:
    deduped: list[Any] = []
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


__all__ = [
    "SlaUptimeConfidence",
    "SlaUptimeRequirementLabel",
    "SourceSlaUptimeRequirement",
    "SourceSlaUptimeRequirementsReport",
    "build_source_sla_uptime_requirements",
    "derive_source_sla_uptime_requirements",
    "extract_source_sla_uptime_requirements",
    "generate_source_sla_uptime_requirements",
    "summarize_source_sla_uptime_requirements",
    "source_sla_uptime_requirements_to_dict",
    "source_sla_uptime_requirements_to_dicts",
    "source_sla_uptime_requirements_to_markdown",
]
