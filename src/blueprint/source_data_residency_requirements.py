"""Extract source-level data residency requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceDataResidencyRequirementCategory = Literal[
    "eu_only",
    "us_only",
    "region_pinning",
    "cross_border_transfer",
    "regional_failover",
    "data_localization",
    "tenant_region_routing",
    "customer_selectable_region",
    "data_sovereignty",
    "subprocessor_residency",
    "residency_audit_evidence",
]
SourceDataResidencyConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SourceDataResidencyRequirementCategory, ...] = (
    "eu_only",
    "us_only",
    "region_pinning",
    "cross_border_transfer",
    "regional_failover",
    "data_localization",
    "tenant_region_routing",
    "customer_selectable_region",
    "data_sovereignty",
    "subprocessor_residency",
    "residency_audit_evidence",
)
_CONFIDENCE_ORDER: dict[SourceDataResidencyConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_RESIDENCY_CONTEXT_RE = re.compile(
    r"\b(?:data residency|residency|regional storage|region pinning|pinned region|"
    r"home region|tenant region|tenant[- ]region|data localization|data localisation|"
    r"localized storage|localised storage|cross[- ]border|data transfer|transfer impact|"
    r"regional failover|failover region|disaster recovery region|dr region|"
    r"sovereign(?:ty)?|sovereign cloud|regional routing|geo[- ]routing|allowed regions?|"
    r"customer[- ]selectable region|customer selected region|subprocessors?|"
    r"residency evidence|residency audit|attestation|audit evidence)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:residen(?:cy|t)|region|regional|geo|geograph|jurisdiction|locali[sz]ation|"
    r"sovereign|transfer|routing|tenant|compliance|privacy|data[-_ ]?requirements|"
    r"subprocessor|processor|failover|disaster|dr|evidence|audit|attestation|"
    r"requirements?|constraints?|acceptance|metadata)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|only|cannot|must not|"
    r"should|ensure|keep|kept|store|stored|stay|remain|reside|host|pin|pinned|route|"
    r"failover|replicate|recover|select|choose|subprocessor|audit|evidence|attest|"
    r"before launch|compliance|policy|blocked|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:data\s+)?(?:residency|regional|region|locali[sz]ation|"
    r"cross[- ]border|transfer|sovereign|tenant[- ]region).*?"
    r"\b(?:in scope|required|needed|changes?|impact|requirements?)\b",
    re.I,
)
_NEGATED_RESIDENCY_LIST_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,160}\b(?:data residency|regional failover|cross[- ]border|"
    r"transfer|subprocessors?|residency|locali[sz]ation|sovereign).{0,160}"
    r"\b(?:required|needed|in scope|changes?|requirements?)\b",
    re.I,
)
_REGION_RE = re.compile(
    r"\b(?:eu[-_ ]?west[-_ ]?\d+|eu[-_ ]?central[-_ ]?\d+|"
    r"us[-_ ]?east[-_ ]?\d+|us[-_ ]?west[-_ ]?\d+|ca[-_ ]?central[-_ ]?\d+|"
    r"ap[-_ ](?:southeast|northeast|south)[-_ ]?\d+|eu|europe|european union|eea|"
    r"gdpr|us|usa|united states|north america|uk|united kingdom|apac|asia[- ]?pacific|"
    r"canada|ca|australia|au|germany|france|ireland|japan|singapore|india|"
    r"switzerland|ch|brazil|mexico|south korea|korea|china|uae|united arab emirates|"
    r"saudi arabia)\b",
    re.I,
)
_SCOPE_RE = re.compile(
    r"\b(?:customer|tenant|user|account|workspace|organization|personal|sensitive|"
    r"regulated|payment|billing|invoice|authentication|session|analytics|telemetry|"
    r"logs?|audit|backup|export|profile|support|public sector|government)"
    r"(?:[- ](?:data|records?|fields?|files?|uploads?|backups?|logs?|events?|exports?|"
    r"tenants?|accounts?|workspaces?|metadata|payloads?))*\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[SourceDataResidencyRequirementCategory, re.Pattern[str]] = {
    "eu_only": re.compile(
        r"\b(?:eu[- ]only|eu only|european union only|eea only|gdpr region only|"
        r"(?:only|solely|exclusively)\s+(?:in|within)\s+(?:the\s+)?(?:eu|european union|eea)|"
        r"(?:eu|european union|eea)\s+(?:residents?|customers?|tenants?)?.{0,50}\b(?:only|must remain|must stay|must be stored))\b",
        re.I,
    ),
    "us_only": re.compile(
        r"\b(?:us[- ]only|us only|usa only|united states only|"
        r"(?:only|solely|exclusively)\s+(?:in|within)\s+(?:the\s+)?(?:us|usa|united states)|"
        r"(?:us|usa|united states)\s+(?:customers?|tenants?)?.{0,50}\b(?:only|must remain|must stay|must be stored))\b",
        re.I,
    ),
    "region_pinning": re.compile(
        r"\b(?:region pinning|pinned region|pin(?:ned)?\s+(?:to|in)\s+(?:a\s+)?region|"
        r"fixed region|storage region|"
        r"host(?:ed)?\s+in\s+(?:eu|us|uk|ca|apac|[a-z]{2}[-_ ][a-z]+[-_ ]\d+)|"
        r"(?:eu|us|ca|ap|uk)[-_ ][a-z]+[-_ ]\d+)\b",
        re.I,
    ),
    "cross_border_transfer": re.compile(
        r"\b(?:cross[- ]border|data transfer|transfer data|transfer outside|leave the region|"
        r"outside (?:the )?(?:eu|us|region|jurisdiction|country)|replicate outside|"
        r"export outside|standard contractual clauses|sccs?|transfer impact assessment|tia|"
        r"adequacy decision|international transfer)\b",
        re.I,
    ),
    "regional_failover": re.compile(
        r"\b(?:(?:regional|same[- ]region|in[- ]region|in country|same country|same jurisdiction)\s+failover|"
        r"failover\s+(?:must|shall|should|needs?|requires?|only|within|inside|to|in).{0,80}\b(?:region|country|jurisdiction|eu|us|uk|canada|germany|france|japan|singapore|india)|"
        r"(?:disaster recovery|dr|backup|replica|replication|warm standby|hot standby).{0,80}\b(?:same|resident|residency|region|country|jurisdiction)|"
        r"(?:do not|must not|cannot|never)\s+fail\s*over\s+(?:outside|across|to another).{0,60}\b(?:region|country|jurisdiction|eu|us))\b",
        re.I,
    ),
    "data_localization": re.compile(
        r"\b(?:data locali[sz]ation|locali[sz]ed storage|local storage|in-country storage|"
        r"country[- ]specific storage|stored locally|kept locally|remain in country|"
        r"(?:must\s+)?remain(?:s)? in (?:germany|france|canada|australia|india|japan|singapore|switzerland|brazil|china|uae|united arab emirates)|"
        r"(?:host|hosted|hosting|store|stored|storage|process|processed|processing)\s+(?:only\s+)?in (?:germany|france|canada|australia|india|japan|singapore|switzerland|brazil|china|uae|united arab emirates))\b",
        re.I,
    ),
    "tenant_region_routing": re.compile(
        r"\b(?:tenant[- ]region routing|tenant region|tenant home region|tenant selected region|"
        r"tenant's region|customer region|workspace region|route tenants?|route data|"
        r"regional endpoint|geo[- ]routing|route requests?.{0,40}\bregion|"
        r"region based routing|region-aware routing)\b",
        re.I,
    ),
    "customer_selectable_region": re.compile(
        r"\b(?:customer[- ]selectable region|customer selected region|customer chosen region|"
        r"customers? can (?:select|choose|set).{0,80}\bregion|"
        r"allow customers? to (?:select|choose|set) (?:their )?(?:hosting|storage|data|tenant )?region|"
        r"tenant selected region|tenant chosen region|workspace selected region|"
        r"region selector|selectable hosting regions?|bring your own region)\b",
        re.I,
    ),
    "data_sovereignty": re.compile(
        r"\b(?:data sovereignty|sovereign cloud|sovereign region|sovereign controls?|"
        r"sovereignty requirement|government cloud|public sector cloud|national cloud|"
        r"jurisdictional control)\b",
        re.I,
    ),
    "subprocessor_residency": re.compile(
        r"\b(?:subprocessors?|third[- ]party processors?|vendors?|processors?).{0,100}"
        r"\b(?:resident|residency|region|regional|country|jurisdiction|eu[- ]only|us[- ]only|cross[- ]border|"
        r"transfer|outside|inside|approved regions?|data location)|"
        r"\b(?:resident|residency|regional|country|jurisdiction|approved regions?).{0,100}"
        r"\b(?:subprocessors?|third[- ]party processors?|vendors?|processors?)\b",
        re.I,
    ),
    "residency_audit_evidence": re.compile(
        r"\b(?:residency|regional hosting|data location|data residency).{0,100}"
        r"\b(?:audit evidence|evidence|attestation|audit logs?|reports?|proof|certificate|certification|"
        r"customer audit|compliance report|soc 2|iso 27001)|"
        r"\b(?:audit evidence|attestation|proof|compliance report|customer audit).{0,100}"
        r"\b(?:residency|regional hosting|data location|region commitments?)\b",
        re.I,
    ),
}
_PLANNING_NOTES: dict[SourceDataResidencyRequirementCategory, str] = {
    "eu_only": "Preserve the EU-only constraint when planning storage, processing, logs, backups, analytics, and subprocessors.",
    "us_only": "Preserve the US-only constraint when planning storage, processing, logs, backups, analytics, and subprocessors.",
    "region_pinning": "Carry the pinned or selected region into architecture, provisioning, migration, and validation tasks.",
    "cross_border_transfer": "Confirm allowed transfer paths, legal basis, subprocessors, and evidence before implementation planning.",
    "regional_failover": "Keep failover, disaster recovery, replicas, and backup routing within allowed residency boundaries.",
    "data_localization": "Translate localization language into explicit storage, processing, backup, and export location controls.",
    "tenant_region_routing": "Plan tenant-region attribution, routing, failover, and operational diagnostics as source constraints.",
    "customer_selectable_region": "Preserve customer-selected region choices in provisioning, routing, migration, support, and billing workflows.",
    "data_sovereignty": "Preserve sovereignty controls and jurisdictional ownership requirements in planning and vendor review.",
    "subprocessor_residency": "Validate subprocessor and vendor data locations against residency, transfer, and disclosure commitments.",
    "residency_audit_evidence": "Plan auditable evidence, reports, logs, or attestations proving residency commitments are met.",
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
    "acceptance",
    "acceptance_criteria",
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
    "security",
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
}


@dataclass(frozen=True, slots=True)
class SourceDataResidencyRequirement:
    """One source-backed data residency requirement category."""

    category: SourceDataResidencyRequirementCategory
    confidence: SourceDataResidencyConfidence = "medium"
    region_signals: tuple[str, ...] = field(default_factory=tuple)
    data_scope: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "confidence": self.confidence,
            "region_signals": list(self.region_signals),
            "data_scope": self.data_scope,
            "evidence": list(self.evidence),
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceDataResidencyRequirementsReport:
    """Source-level data residency requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceDataResidencyRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceDataResidencyRequirement, ...]:
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
        """Return data residency requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Data Residency Requirements Report"
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
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source data residency requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Confidence | Regions | Data Scope | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(', '.join(requirement.region_signals))} | "
                f"{_markdown_cell(requirement.data_scope or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} |"
            )
        return "\n".join(lines)


def build_source_data_residency_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceDataResidencyRequirementsReport:
    """Build a source data residency requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceDataResidencyRequirementsReport(
        source_id=source_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def build_source_data_residency_requirements_report(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceDataResidencyRequirementsReport:
    """Compatibility helper for callers that use explicit report naming."""
    return build_source_data_residency_requirements(source)


def extract_source_data_residency_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceDataResidencyRequirement, ...]:
    """Return data residency requirement records extracted from brief-shaped input."""
    return build_source_data_residency_requirements(source).requirements


def summarize_source_data_residency_requirements(
    source_or_report: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceDataResidencyRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for source data residency requirements."""
    if isinstance(source_or_report, SourceDataResidencyRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_data_residency_requirements(source_or_report).summary


def source_data_residency_requirements_to_dict(
    report: SourceDataResidencyRequirementsReport,
) -> dict[str, Any]:
    """Serialize a source data residency requirements report to a plain dictionary."""
    return report.to_dict()


source_data_residency_requirements_to_dict.__test__ = False


def source_data_residency_requirements_to_dicts(
    requirements: (
        tuple[SourceDataResidencyRequirement, ...]
        | list[SourceDataResidencyRequirement]
        | SourceDataResidencyRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source data residency requirement records to dictionaries."""
    if isinstance(requirements, SourceDataResidencyRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_data_residency_requirements_to_dicts.__test__ = False


def source_data_residency_requirements_to_markdown(
    report: SourceDataResidencyRequirementsReport,
) -> str:
    """Render a source data residency requirements report as Markdown."""
    return report.to_markdown()


source_data_residency_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: SourceDataResidencyRequirementCategory
    confidence: SourceDataResidencyConfidence
    region_signals: tuple[str, ...]
    data_scope: str | None
    evidence: str


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
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


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        categories = _categories(segment)
        if not categories:
            continue
        evidence = _evidence_snippet(segment.source_field, segment.text)
        for category in categories:
            candidates.append(
                _Candidate(
                    category=category,
                    confidence=_confidence(category, segment),
                    region_signals=tuple(_region_tokens(segment.text)),
                    data_scope=_data_scope(segment.text, segment.source_field),
                    evidence=evidence,
                )
            )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceDataResidencyRequirement]:
    grouped: dict[SourceDataResidencyRequirementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceDataResidencyRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        region_signals = tuple(sorted(_dedupe(region for item in items for region in item.region_signals), key=str.casefold))
        data_scope = next((item.data_scope for item in items if item.data_scope), None)
        requirements.append(
            SourceDataResidencyRequirement(
                category=category,
                confidence=confidence,
                region_signals=region_signals,
                data_scope=data_scope,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:6],
                planning_note=_PLANNING_NOTES[category],
            )
        )
    return requirements


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
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text))
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
            section_context = inherited_context or bool(_RESIDENCY_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            keep_whole_clause = (
                bool(
                    re.search(r"\bcustomers? can (?:select|choose|set)\b", part, re.I)
                    and re.search(r"\bregion\b", part, re.I)
                )
                or bool(re.search(r"\b(?:no|not|without)\b", part, re.I) and _RESIDENCY_CONTEXT_RE.search(part))
            )
            clauses = [part] if keep_whole_clause else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _categories(segment: _Segment) -> tuple[SourceDataResidencyRequirementCategory, ...]:
    if _NEGATED_SCOPE_RE.search(segment.text) or _NEGATED_RESIDENCY_LIST_RE.search(segment.text):
        return ()
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    categories = [
        category
        for category in _CATEGORY_ORDER
        if _CATEGORY_PATTERNS[category].search(searchable)
    ]
    if not categories:
        return ()
    if _is_requirement(segment, categories):
        return tuple(_dedupe(categories))
    return ()


def _is_requirement(
    segment: _Segment,
    categories: Iterable[SourceDataResidencyRequirementCategory],
) -> bool:
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    if field_context or segment.section_context or _RESIDENCY_CONTEXT_RE.search(segment.text):
        return True
    if _REQUIREMENT_RE.search(segment.text) and (_REGION_RE.search(segment.text) or _RESIDENCY_CONTEXT_RE.search(segment.text)):
        return True
    if any(category == "tenant_region_routing" for category in categories) and _REQUIREMENT_RE.search(segment.text):
        return True
    if any(category in {"data_sovereignty", "cross_border_transfer", "data_localization"} for category in categories):
        return True
    return False


def _confidence(
    category: SourceDataResidencyRequirementCategory,
    segment: _Segment,
) -> SourceDataResidencyConfidence:
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    has_region = bool(_region_tokens(segment.text))
    if _REQUIREMENT_RE.search(segment.text) and (field_context or segment.section_context or has_region):
        return "high"
    if category in {"eu_only", "us_only", "cross_border_transfer", "data_localization", "data_sovereignty"}:
        return "high" if _REQUIREMENT_RE.search(segment.text) else "medium"
    if field_context or segment.section_context or has_region:
        return "medium"
    return "low"


def _region_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    for match in _REGION_RE.finditer(text):
        token = match.group(0).replace("_", "-").lower()
        token = re.sub(r"\s+", "-", token)
        aliases = {
            "europe": "eu",
            "european-union": "eu",
            "eea": "eu",
            "gdpr": "eu",
            "usa": "us",
            "united-states": "us",
            "north-america": "us",
            "united-kingdom": "uk",
            "asia-pacific": "apac",
            "canada": "ca",
            "australia": "au",
        }
        tokens.append(aliases.get(token, token))
    return _dedupe(tokens)


def _data_scope(text: str, source_field: str) -> str | None:
    generic_scopes = {
        "customer",
        "tenant",
        "user",
        "account",
        "workspace",
        "organization",
        "logs",
        "backup",
        "export",
    }
    matches = [
        _clean_scope(match.group(0))
        for match in _SCOPE_RE.finditer(text)
        if _clean_scope(match.group(0)) not in generic_scopes
    ]
    if matches:
        return _dedupe(matches)[0]
    field_tail = source_field.rsplit(".", 1)[-1].replace("_", " ").replace("-", " ")
    field_matches = [
        _clean_scope(match.group(0))
        for match in _SCOPE_RE.finditer(field_tail)
        if _clean_scope(match.group(0)) not in generic_scopes
    ]
    return _dedupe(field_matches)[0] if field_matches else None


def _summary(requirements: tuple[SourceDataResidencyRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "categories": [requirement.category for requirement in requirements],
        "region_signals": list(sorted(_dedupe(region for requirement in requirements for region in requirement.region_signals), key=str.casefold)),
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
        "problem_statement",
        "mvp_goal",
        "requirements",
        "constraints",
        "acceptance",
        "acceptance_criteria",
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
        "security",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _any_signal(text: str) -> bool:
    return bool(_RESIDENCY_CONTEXT_RE.search(text) or _STRUCTURED_FIELD_RE.search(text)) or any(
        pattern.search(text) for pattern in _CATEGORY_PATTERNS.values()
    )


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_scope(text: str) -> str:
    return _SPACE_RE.sub(" ", text.strip(" .,:;")).casefold()


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
    seen: dict[str, int] = {}
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            index = seen[key]
            if _evidence_priority(value) < _evidence_priority(deduped[index]):
                deduped[index] = value
            continue
        deduped.append(value)
        seen[key] = len(deduped) - 1
    return deduped


def _evidence_priority(value: str) -> int:
    source_field, _, _ = value.partition(": ")
    if ".requirements" in source_field or ".constraints" in source_field or ".acceptance" in source_field:
        return 0
    if ".metadata" in source_field or ".brief_metadata" in source_field:
        return 2
    return 1


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
    "SourceDataResidencyConfidence",
    "SourceDataResidencyRequirement",
    "SourceDataResidencyRequirementCategory",
    "SourceDataResidencyRequirementsReport",
    "build_source_data_residency_requirements",
    "build_source_data_residency_requirements_report",
    "extract_source_data_residency_requirements",
    "source_data_residency_requirements_to_dict",
    "source_data_residency_requirements_to_dicts",
    "source_data_residency_requirements_to_markdown",
    "summarize_source_data_residency_requirements",
]
