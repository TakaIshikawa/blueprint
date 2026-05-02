"""Extract source-level data portability requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


DataPortabilityCategory = Literal[
    "user_export",
    "account_data_download",
    "machine_readable_format",
    "tenant_export",
    "gdpr_portability",
    "export_retention_window",
    "async_export_delivery",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[DataPortabilityCategory, ...] = (
    "user_export",
    "account_data_download",
    "machine_readable_format",
    "tenant_export",
    "gdpr_portability",
    "export_retention_window",
    "async_export_delivery",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_PORTABILITY_CONTEXT_RE = re.compile(
    r"\b(?:data portability|portability|portable data|export|exports|exported|"
    r"download(?:able)? data|data download|account download|account data|"
    r"machine[- ]readable|csv|json|xml|zip|archive|tenant export|workspace export|"
    r"organization export|gdpr|data subject|article 20|retention window|"
    r"download link|async export|background export|email notification)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:portability|portable|export|download|gdpr|data[-_ ]?subject|machine[-_ ]?readable|"
    r"tenant|workspace|organization|retention|expiry|expiration|async|background|"
    r"delivery|data[-_ ]?requirements|requirements?|constraints?|acceptance|"
    r"definition[-_ ]?of[-_ ]?done|privacy|compliance|support)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|provide|allow|enable|deliver|generate|include|retain|expire|delete|"
    r"notify|email|before launch|cannot ship|done when|acceptance)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:data\s+)?(?:portability|export|download|gdpr|"
    r"tenant export|machine[- ]readable|retention|async).*?"
    r"\b(?:in scope|required|requirements?|needed|changes?|impact)\b",
    re.I,
)
_RETENTION_RE = re.compile(
    r"\b(?:retention window|retain(?:ed)? for|available for|expires? after|expire after|"
    r"download link expires?|delete exports? after|purge exports? after|"
    r"\d+(?:\.\d+)?\s*(?:minutes?|hours?|days?|weeks?)\b)",
    re.I,
)
_SPECIFIC_PORTABILITY_RE = re.compile(
    r"\b(?:data portability|gdpr|article 20|data subject|account export|account data download|"
    r"user export|export my data|download my data|tenant export|workspace export|"
    r"organization export|machine[- ]readable|csv|json|xml|zip archive|download link|"
    r"async export|background export)\b",
    re.I,
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
}

_CATEGORY_PATTERNS: dict[DataPortabilityCategory, re.Pattern[str]] = {
    "user_export": re.compile(
        r"\b(?:user export|export my data|download my data|personal data export|"
        r"users? can export|users? export(?:s|ed)?|self[- ]service export|"
        r"individual export|profile export)\b",
        re.I,
    ),
    "account_data_download": re.compile(
        r"\b(?:account data download|account download|download account data|account export|"
        r"export account data|customer data download|download all account data|"
        r"account archive|data download package)\b",
        re.I,
    ),
    "machine_readable_format": re.compile(
        r"\b(?:machine[- ]readable|structured format|portable format|csv|json|xml|"
        r"ndjson|zip archive|open format|schema(?:d)? export)\b",
        re.I,
    ),
    "tenant_export": re.compile(
        r"\b(?:tenant export|workspace export|organization export|org export|enterprise export|"
        r"export tenant|export workspace|export organization|multi[- ]tenant export|"
        r"full tenant archive)\b",
        re.I,
    ),
    "gdpr_portability": re.compile(
        r"\b(?:gdpr(?: data)? portability|article 20|right to data portability|"
        r"data subject portability|data subject export|privacy portability|"
        r"regulatory portability)\b",
        re.I,
    ),
    "export_retention_window": _RETENTION_RE,
    "async_export_delivery": re.compile(
        r"\b(?:async export|asynchronous export|background export|queued export|export job|"
        r"long[- ]running export|notify when (?:ready|complete)|email(?:ed)? download link|"
        r"download link delivery|webhook when export completes|export delivery)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[DataPortabilityCategory, str] = {
    "user_export": "product_engineering",
    "account_data_download": "product_engineering",
    "machine_readable_format": "data_platform",
    "tenant_export": "data_platform",
    "gdpr_portability": "privacy_owner",
    "export_retention_window": "security_owner",
    "async_export_delivery": "platform_engineering",
}
_PLANNING_NOTE_BY_CATEGORY: dict[DataPortabilityCategory, str] = {
    "user_export": "Plan self-service user export scope, authorization, and support handoff.",
    "account_data_download": "Define account data coverage, packaging, and access controls before implementation.",
    "machine_readable_format": "Confirm export schemas, machine-readable formats, and compatibility expectations.",
    "tenant_export": "Plan tenant-scale export boundaries, storage access, and operational safeguards.",
    "gdpr_portability": "Coordinate GDPR portability scope, privacy review, and data subject request handling.",
    "export_retention_window": "Define export artifact retention, expiry, deletion, and audit behavior.",
    "async_export_delivery": "Plan async export jobs, completion notifications, and download delivery states.",
}


@dataclass(frozen=True, slots=True)
class SourceDataPortabilityRequirement:
    """One source-backed data portability requirement category."""

    category: DataPortabilityCategory
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_owner: str = ""
    suggested_planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "suggested_owner": self.suggested_owner,
            "suggested_planning_note": self.suggested_planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceDataPortabilityRequirementsReport:
    """Source-level data portability requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceDataPortabilityRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceDataPortabilityRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
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
        """Return data portability requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Data Portability Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        owner_counts = self.summary.get("owner_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Owner counts: "
            + (", ".join(f"{owner} {owner_counts[owner]}" for owner in sorted(owner_counts)) or "none"),
        ]
        if not self.requirements:
            lines.extend(["", "No data portability requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Confidence | Evidence | Suggested Owner | Suggested Planning Note |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{requirement.confidence:.2f} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{requirement.suggested_owner} | "
                f"{_markdown_cell(requirement.suggested_planning_note)} |"
            )
        return "\n".join(lines)


def build_source_data_portability_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceDataPortabilityRequirementsReport:
    """Build a data portability requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceDataPortabilityRequirementsReport(
        source_id=source_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def generate_source_data_portability_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceDataPortabilityRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_data_portability_requirements(source)


def derive_source_data_portability_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceDataPortabilityRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_data_portability_requirements(source)


def extract_source_data_portability_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceDataPortabilityRequirement, ...]:
    """Return data portability requirement records extracted from brief-shaped input."""
    return build_source_data_portability_requirements(source).requirements


def summarize_source_data_portability_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceDataPortabilityRequirementsReport
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic data portability requirements summary."""
    if isinstance(source_or_result, SourceDataPortabilityRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_data_portability_requirements(source_or_result).summary


def source_data_portability_requirements_to_dict(
    report: SourceDataPortabilityRequirementsReport,
) -> dict[str, Any]:
    """Serialize a data portability requirements report to a plain dictionary."""
    return report.to_dict()


source_data_portability_requirements_to_dict.__test__ = False


def source_data_portability_requirements_to_dicts(
    requirements: (
        tuple[SourceDataPortabilityRequirement, ...]
        | list[SourceDataPortabilityRequirement]
        | SourceDataPortabilityRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source data portability requirement records to dictionaries."""
    if isinstance(requirements, SourceDataPortabilityRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_data_portability_requirements_to_dicts.__test__ = False


def source_data_portability_requirements_to_markdown(
    report: SourceDataPortabilityRequirementsReport,
) -> str:
    """Render a data portability requirements report as Markdown."""
    return report.to_markdown()


source_data_portability_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: DataPortabilityCategory
    confidence: float
    evidence: str


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _brief_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _brief_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _brief_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _brief_id(payload), payload
    if not isinstance(source, (str, bytes, bytearray)):
        payload = _object_payload(source)
        return _brief_id(payload), payload
    return None, {}


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = [
            category
            for category in _CATEGORY_ORDER
            if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        if not categories:
            continue
        if not _is_requirement(segment.text, segment.source_field, segment.section_context):
            continue
        evidence = _evidence_snippet(segment.source_field, segment.text)
        confidence = _confidence(segment.text, segment.source_field, segment.section_context)
        for category in categories:
            candidates.append(_Candidate(category=category, confidence=confidence, evidence=evidence))
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceDataPortabilityRequirement]:
    by_category: dict[DataPortabilityCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceDataPortabilityRequirement] = []
    for category in _CATEGORY_ORDER:
        items = by_category.get(category, [])
        if not items:
            continue
        requirements.append(
            SourceDataPortabilityRequirement(
                category=category,
                confidence=round(max(item.confidence for item in items), 2),
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                suggested_owner=_OWNER_BY_CATEGORY[category],
                suggested_planning_note=_PLANNING_NOTE_BY_CATEGORY[category],
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "problem",
        "problem_statement",
        "mvp_goal",
        "workflow_context",
        "requirements",
        "constraints",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "privacy",
        "compliance",
        "exports",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(
    segments: list[_Segment],
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
                _STRUCTURED_FIELD_RE.search(key_text) or _PORTABILITY_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text, segment_context in _segments(text, field_context):
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
            section_context = inherited_context or bool(
                _PORTABILITY_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(text: str, source_field: str, section_context: bool) -> bool:
    if _NEGATED_SCOPE_RE.search(text):
        return False
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if (field_context or section_context) and _PORTABILITY_CONTEXT_RE.search(text):
        return True
    if _REQUIREMENT_RE.search(text) and (
        _PORTABILITY_CONTEXT_RE.search(text) or _SPECIFIC_PORTABILITY_RE.search(text)
    ):
        return True
    if _SPECIFIC_PORTABILITY_RE.search(text) and _PORTABILITY_CONTEXT_RE.search(text):
        return True
    if _RETENTION_RE.search(text) and (field_context or section_context):
        return True
    return False


def _confidence(text: str, source_field: str, section_context: bool) -> float:
    score = 0.68
    if _STRUCTURED_FIELD_RE.search(_field_words(source_field)):
        score += 0.08
    if section_context or _PORTABILITY_CONTEXT_RE.search(text):
        score += 0.07
    if _REQUIREMENT_RE.search(text):
        score += 0.07
    if _SPECIFIC_PORTABILITY_RE.search(text) or _RETENTION_RE.search(text):
        score += 0.05
    return round(min(score, 0.95), 2)


def _summary(requirements: tuple[SourceDataPortabilityRequirement, ...]) -> dict[str, Any]:
    owner_counts = {
        owner: sum(1 for requirement in requirements if requirement.suggested_owner == owner)
        for owner in sorted({requirement.suggested_owner for requirement in requirements})
    }
    return {
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "high_confidence_count": sum(
            1 for requirement in requirements if requirement.confidence >= 0.85
        ),
        "categories": [requirement.category for requirement in requirements],
        "owner_counts": owner_counts,
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
        "workflow_context",
        "problem",
        "problem_statement",
        "mvp_goal",
        "requirements",
        "constraints",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "privacy",
        "compliance",
        "exports",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = str(value).strip()
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
    return deduped


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
    "DataPortabilityCategory",
    "SourceDataPortabilityRequirement",
    "SourceDataPortabilityRequirementsReport",
    "build_source_data_portability_requirements",
    "derive_source_data_portability_requirements",
    "extract_source_data_portability_requirements",
    "generate_source_data_portability_requirements",
    "summarize_source_data_portability_requirements",
    "source_data_portability_requirements_to_dict",
    "source_data_portability_requirements_to_dicts",
    "source_data_portability_requirements_to_markdown",
]
