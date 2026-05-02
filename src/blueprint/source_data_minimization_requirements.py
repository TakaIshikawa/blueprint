"""Extract source-level data minimization requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


DataMinimizationRequirementCategory = Literal[
    "necessary_fields",
    "optional_attributes",
    "masking_redaction",
    "raw_payload_storage",
    "purpose_limitation",
    "field_level_retention",
    "telemetry_minimization",
    "default_off_tracking",
]
DataMinimizationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[DataMinimizationRequirementCategory, ...] = (
    "necessary_fields",
    "optional_attributes",
    "masking_redaction",
    "raw_payload_storage",
    "purpose_limitation",
    "field_level_retention",
    "telemetry_minimization",
    "default_off_tracking",
)
_CONFIDENCE_ORDER: dict[DataMinimizationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_OWNER_SUGGESTIONS: dict[DataMinimizationRequirementCategory, str] = {
    "necessary_fields": "Product owner with privacy engineering review",
    "optional_attributes": "Product owner and UX owner",
    "masking_redaction": "Privacy engineering and platform engineering",
    "raw_payload_storage": "Backend owner and data platform owner",
    "purpose_limitation": "Privacy counsel and product owner",
    "field_level_retention": "Data governance owner and backend owner",
    "telemetry_minimization": "Analytics owner and observability owner",
    "default_off_tracking": "Product owner and analytics owner",
}
_PLANNING_NOTES: dict[DataMinimizationRequirementCategory, str] = {
    "necessary_fields": "Inventory required fields and remove collection of fields that are not needed for the stated workflow.",
    "optional_attributes": "Keep optional profile attributes out of mandatory flows and define skip/default behavior.",
    "masking_redaction": "Add masking, redaction, or tokenization tasks for unused or sensitive data in UI, logs, and exports.",
    "raw_payload_storage": "Avoid storing raw request, webhook, event, or vendor payloads unless a documented purpose and retention bound exists.",
    "purpose_limitation": "Tie collected data to explicit purposes and prevent reuse for unrelated analytics, marketing, or personalization.",
    "field_level_retention": "Define retention and purge behavior at field level, including shorter lifetimes for sensitive or unused fields.",
    "telemetry_minimization": "Limit events, properties, logs, and traces to the minimum needed for operations and measurement.",
    "default_off_tracking": "Make tracking, profiling, personalization, or analytics default off until the required user or tenant choice is present.",
}
_CATEGORY_PATTERNS: dict[DataMinimizationRequirementCategory, re.Pattern[str]] = {
    "necessary_fields": re.compile(
        r"\b(?:collect|capture|request|ask for|require|store|process)\b.{0,90}"
        r"\b(?:only|minimum|minimal|necessary|needed|essential)\b.{0,80}\b(?:fields?|attributes?|data|PII|personal data|profile)\b|"
        r"\b(?:only|minimum|minimal|necessary|needed|essential)\b.{0,80}"
        r"\b(?:fields?|attributes?|data|PII|personal data|profile)\b",
        re.I,
    ),
    "optional_attributes": re.compile(
        r"\b(?:optional|non[- ]?required|nice[- ]?to[- ]?have|supplemental|extra)\b.{0,80}"
        r"\b(?:attributes?|fields?|profile data|demographics?|metadata)\b.{0,80}\b(?:should|must|skippable|skip|omit|unless|not required)\b|"
        r"\b(?:attributes?|profile data|demographics?|metadata)\b.{0,80}\b(?:optional|not required|required|skippable|skip|omit)\b",
        re.I,
    ),
    "masking_redaction": re.compile(
        r"\b(?:mask|masked|redact|redacted|redaction|tokeni[sz]e|hash|truncate|obfuscate|hide)\b.{0,90}"
        r"\b(?:unused|unneeded|unnecessary|PII|personal data|sensitive|raw|payload|fields?|data|logs?|exports?)\b|"
        r"\b(?:unused|unneeded|unnecessary|PII|personal data|sensitive|raw|payload|fields?|data|logs?|exports?)\b.{0,90}"
        r"\b(?:mask|masked|redact|redacted|redaction|tokeni[sz]e|hash|truncate|obfuscate|hide)\b",
        re.I,
    ),
    "raw_payload_storage": re.compile(
        r"\b(?:do not|don't|avoid|prevent|must not|never|disable)\b.{0,80}"
        r"\b(?:store|persist|retain|save|archive|log)\b.{0,80}\b(?:raw|full|entire|complete)\b.{0,40}\b(?:payloads?|requests?|webhooks?|events?|responses?)\b|"
        r"\b(?:raw|full|entire|complete)\b.{0,40}\b(?:payloads?|requests?|webhooks?|events?|responses?)\b.{0,90}"
        r"\b(?:not stored|not persisted|must not be stored|must not be persisted|avoid storing|must not store|redact before storage)\b",
        re.I,
    ),
    "purpose_limitation": re.compile(
        r"\b(?:purpose limitation|specific purpose|declared purpose|stated purpose|limited purpose|compatible purpose)\b|"
        r"\b(?:use|reuse|process|share|analy[sz]e|personalize|market)\b.{0,90}"
        r"\b(?:only for|solely for|limited to|not for unrelated|not reuse|no secondary use)\b",
        re.I,
    ),
    "field_level_retention": re.compile(
        r"\b(?:field[- ]level retention|per[- ]field retention|field retention|attribute retention)\b|"
        r"\b(?:retain|retention|expire|delete|purge|ttl)\b.{0,90}\b(?:fields?|attributes?|columns?|properties?)\b|"
        r"\b(?:fields?|attributes?|columns?|properties?)\b.{0,90}\b(?:retain|retention|expire|delete|purge|ttl)\b",
        re.I,
    ),
    "telemetry_minimization": re.compile(
        r"\b(?:telemetry|analytics events?|event properties|logs?|traces?|metrics|observability)\b.{0,100}"
        r"\b(?:minimum|minimal|minimi[sz]e|only necessary|drop|exclude|omit|redact|no PII|without PII)\b|"
        r"\b(?:minimum|minimal|minimi[sz]e|only necessary|drop|exclude|omit|redact|no PII|without PII)\b.{0,100}"
        r"\b(?:telemetry|analytics events?|event properties|logs?|traces?|metrics|observability)\b",
        re.I,
    ),
    "default_off_tracking": re.compile(
        r"\b(?:tracking|analytics|profiling|personalization|advertising|marketing pixels?|session replay)\b.{0,90}"
        r"\b(?:default[- ]?off|off by default|disabled by default|opt[- ]?in|not enabled by default|until consent|until enabled)\b|"
        r"\b(?:default[- ]?off|off by default|disabled by default|opt[- ]?in|not enabled by default)\b.{0,90}"
        r"\b(?:tracking|analytics|profiling|personalization|advertising|marketing pixels?|session replay)\b",
        re.I,
    ),
}
_FIELD_CATEGORY_PATTERNS: dict[DataMinimizationRequirementCategory, re.Pattern[str]] = {
    "necessary_fields": re.compile(r"\b(?:necessary fields?|minimum fields?|required fields?)\b", re.I),
    "optional_attributes": re.compile(r"\b(?:optional attributes?|optional fields?|supplemental fields?)\b", re.I),
    "masking_redaction": re.compile(r"\b(?:masking|redaction|redacted|masked)\b", re.I),
    "raw_payload_storage": re.compile(r"\b(?:raw payload|payload storage|webhook payload|request payload)\b", re.I),
    "purpose_limitation": re.compile(r"\b(?:purpose limitation|purpose)\b", re.I),
    "field_level_retention": re.compile(r"\b(?:field retention|field level retention|retention)\b", re.I),
    "telemetry_minimization": re.compile(r"\b(?:telemetry minimization|telemetry|analytics events?|logs?)\b", re.I),
    "default_off_tracking": re.compile(r"\b(?:default off tracking|tracking default|opt in tracking)\b", re.I),
}
_MINIMIZATION_CONTEXT_RE = re.compile(
    r"\b(?:data minimization|minimi[sz]e data|minimum necessary|necessary fields?|optional attributes?|"
    r"redact|mask|raw payload|purpose limitation|field[- ]level retention|telemetry minimization|"
    r"default[- ]?off tracking|privacy by default|collect only|avoid storing)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:data[_ -]?minimi[sz]ation|minimum[_ -]?necessary|necessary[_ -]?fields?|optional[_ -]?attributes?|"
    r"mask(?:ing)?|redact(?:ion)?|raw[_ -]?payload|payload[_ -]?storage|purpose[_ -]?limitation|"
    r"field[_ -]?level[_ -]?retention|retention|telemetry|tracking|privacy)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|limit|avoid|prevent|"
    r"disable|make|keep|collect|store|retain|redact|mask|purge|delete|only|default[- ]?off|opt[- ]?in|"
    r"acceptance|done when|definition of done)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,90}\b(?:new|additional|extra)?\s*(?:data collection|PII collection|personal data collection|"
    r"fields?|attributes?|tracking|telemetry|analytics)\b.{0,90}\b(?:required|needed|in scope|planned|changes?|impact)\b|"
    r"\b(?:data collection|tracking|telemetry|analytics|new fields?)\b.{0,90}\b(?:not required|not needed|out of scope|no changes?)\b",
    re.I,
)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SPACE_RE = re.compile(r"\s+")
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
    "scope",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "risks",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
    "id",
    "source_id",
    "source_brief_id",
    "domain",
    "status",
}


@dataclass(frozen=True, slots=True)
class SourceDataMinimizationRequirement:
    """One source-backed data minimization requirement."""

    source_brief_id: str | None
    category: DataMinimizationRequirementCategory
    requirement_text: str
    owner_suggestion: str
    planning_note: str
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: DataMinimizationConfidence = "medium"
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> DataMinimizationRequirementCategory:
        """Compatibility alias for callers expecting a longer category field name."""
        return self.category

    @property
    def planning_notes(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting plural planning notes."""
        return (self.planning_note,)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "requirement_text": self.requirement_text,
            "owner_suggestion": self.owner_suggestion,
            "planning_note": self.planning_note,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "unresolved_questions": list(self.unresolved_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceDataMinimizationRequirementsReport:
    """Source-level data minimization requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceDataMinimizationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceDataMinimizationRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceDataMinimizationRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return data minimization requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Data Minimization Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No data minimization requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Requirement | Owner Suggestion | Planning Note | Source Field | Confidence | Unresolved Questions | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.category)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.owner_suggestion)} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell('; '.join(requirement.unresolved_questions))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_data_minimization_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceDataMinimizationRequirementsReport:
    """Extract source-level data minimization requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceDataMinimizationRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_data_minimization_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceDataMinimizationRequirementsReport:
    """Compatibility alias for building a data minimization requirements report."""
    return build_source_data_minimization_requirements(source)


def generate_source_data_minimization_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceDataMinimizationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_data_minimization_requirements(source)


def derive_source_data_minimization_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceDataMinimizationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_data_minimization_requirements(source)


def summarize_source_data_minimization_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceDataMinimizationRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted data minimization requirements."""
    if isinstance(source_or_result, SourceDataMinimizationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_data_minimization_requirements(source_or_result).summary


def source_data_minimization_requirements_to_dict(
    report: SourceDataMinimizationRequirementsReport,
) -> dict[str, Any]:
    """Serialize a data minimization requirements report to a plain dictionary."""
    return report.to_dict()


source_data_minimization_requirements_to_dict.__test__ = False


def source_data_minimization_requirements_to_dicts(
    requirements: (
        tuple[SourceDataMinimizationRequirement, ...]
        | list[SourceDataMinimizationRequirement]
        | SourceDataMinimizationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize data minimization requirement records to dictionaries."""
    if isinstance(requirements, SourceDataMinimizationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_data_minimization_requirements_to_dicts.__test__ = False


def source_data_minimization_requirements_to_markdown(
    report: SourceDataMinimizationRequirementsReport,
) -> str:
    """Render a data minimization requirements report as Markdown."""
    return report.to_markdown()


source_data_minimization_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: DataMinimizationRequirementCategory
    requirement_text: str
    source_field: str
    evidence: str
    confidence: DataMinimizationConfidence


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(
        source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)
    ) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_id(payload), payload
    return None, {}


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            if _NEGATED_RE.search(_searchable_text(segment.source_field, segment.text)):
                continue
            for category in _categories(segment):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        requirement_text=_requirement_text(segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(category, segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceDataMinimizationRequirement]:
    grouped: dict[tuple[str | None, DataMinimizationRequirementCategory, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_brief_id,
                candidate.category,
                _dedupe_requirement_key(candidate.requirement_text, candidate.category),
            ),
            [],
        ).append(candidate)

    requirements: list[SourceDataMinimizationRequirement] = []
    for (_source_brief_id, category, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceDataMinimizationRequirement(
                source_brief_id=best.source_brief_id,
                category=category,
                requirement_text=best.requirement_text,
                owner_suggestion=_OWNER_SUGGESTIONS[category],
                planning_note=_PLANNING_NOTES[category],
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                unresolved_questions=_unresolved_questions(category, items),
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.requirement_text.casefold(),
            requirement.source_field or "",
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        if _has_structured_shape(value):
            for evidence in _structured_segments(value):
                segments.append(_Segment(source_field, evidence, True))
            return
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _MINIMIZATION_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, f"{source_field}.{key}", value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        raw_text = str(value) if isinstance(value, str) else text
        for segment_text, segment_context in _segments(raw_text, field_context):
            segments.append(_Segment(source_field, segment_text, segment_context))


def _segments(value: str, inherited_context: bool) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    section_context = inherited_context
    for raw_line in value.splitlines() or [value]:
        line = raw_line.strip()
        if not line:
            continue
        heading = _HEADING_RE.match(line)
        if heading:
            title = _clean_text(heading.group("title"))
            section_context = inherited_context or bool(_MINIMIZATION_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text and not _NEGATED_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _categories(segment: _Segment) -> tuple[DataMinimizationRequirementCategory, ...]:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    if not (
        _MINIMIZATION_CONTEXT_RE.search(searchable)
        or _STRUCTURED_FIELD_RE.search(field_words)
        or any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    ):
        return ()
    if not (
        _REQUIREMENT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(field_words)
    ):
        return ()
    field_categories = [
        category
        for category in _CATEGORY_ORDER
        if _FIELD_CATEGORY_PATTERNS[category].search(field_words)
    ]
    categories = [
        category
        for category in _CATEGORY_ORDER
        if _CATEGORY_PATTERNS[category].search(searchable)
    ]
    return tuple(_dedupe(field_categories + categories))


def _confidence(category: DataMinimizationRequirementCategory, segment: _Segment) -> DataMinimizationConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(segment.text))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    has_category = bool(_CATEGORY_PATTERNS[category].search(searchable))
    if has_category and has_explicit_requirement and has_structured_context:
        return "high"
    if has_category and (has_explicit_requirement or has_structured_context):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceDataMinimizationRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "categories": [
            category
            for category in _CATEGORY_ORDER
            if any(requirement.category == category for requirement in requirements)
        ],
    }


def _unresolved_questions(
    category: DataMinimizationRequirementCategory, items: Iterable[_Candidate]
) -> tuple[str, ...]:
    candidates = tuple(items)
    text = " ".join(candidate.requirement_text for candidate in candidates)
    questions: list[str] = []
    if category in {"necessary_fields", "optional_attributes"} and not re.search(r"\b(?:field|attribute|email|name|phone|address|dob|metadata)\b", text, re.I):
        questions.append("Which fields or attributes are in scope?")
    if category == "field_level_retention" and not re.search(r"\b(?:\d+\s*(?:days?|months?|years?)|ttl|purge|delete|expire)\b", text, re.I):
        questions.append("What retention period applies to each field?")
    if category == "purpose_limitation" and not re.search(r"\b(?:for|purpose|solely|limited to)\b", text, re.I):
        questions.append("Which purpose authorizes the data use?")
    return tuple(questions)


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    if keys <= {"data_minimization", "privacy", "requirements"} and any(
        isinstance(value, (Mapping, list, tuple, set)) for value in item.values()
    ):
        return False
    return bool(
        keys
        & {
            "category",
            "data_minimization",
            "minimum_necessary",
            "necessary_fields",
            "optional_attributes",
            "masking",
            "redaction",
            "raw_payload_storage",
            "purpose_limitation",
            "field_level_retention",
            "telemetry_minimization",
            "default_off_tracking",
            "owner",
            "retention",
            "purpose",
        }
    )


def _structured_segments(item: Mapping[str, Any]) -> list[str]:
    segments: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(value)
        if text:
            segments.append(f"{key}: {text}")
    return segments


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "target_user",
        "buyer",
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
        "product_surface",
        "requirements",
        "constraints",
        "scope",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "data_requirements",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, str]:
    return (
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int("[" in candidate.source_field),
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        candidate.evidence,
    )


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _clean_text(value)
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
    text = _clean_text(value)
    return [text] if text else []


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _searchable_text(source_field: str, text: str) -> str:
    return f"{_field_words(source_field)} {text}".replace("_", " ").replace("-", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(value)
    return text or None


def _requirement_text(value: str) -> str:
    text = _clean_text(value)
    if len(text) > 220:
        text = f"{text[:217].rstrip()}..."
    return text


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
    return deduped


def _dedupe_requirement_key(value: str, category: DataMinimizationRequirementCategory) -> str:
    text = _clean_text(value).casefold()
    return f"{category}:{_SPACE_RE.sub(' ', text).strip()}"


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


__all__ = [
    "DataMinimizationConfidence",
    "DataMinimizationRequirementCategory",
    "SourceDataMinimizationRequirement",
    "SourceDataMinimizationRequirementsReport",
    "build_source_data_minimization_requirements",
    "derive_source_data_minimization_requirements",
    "extract_source_data_minimization_requirements",
    "generate_source_data_minimization_requirements",
    "source_data_minimization_requirements_to_dict",
    "source_data_minimization_requirements_to_dicts",
    "source_data_minimization_requirements_to_markdown",
    "summarize_source_data_minimization_requirements",
]
