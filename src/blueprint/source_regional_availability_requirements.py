"""Extract regional availability and rollout constraints from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


RegionalAvailabilityConfidence = Literal["high", "medium", "low"]
RegionalAvailabilityMissingDetailFlag = Literal[
    "missing_region_list",
    "missing_launch_phase",
    "missing_fallback_behavior",
    "missing_owner",
    "missing_legal_compliance_basis",
]
_T = TypeVar("_T")

_CONFIDENCE_ORDER: dict[RegionalAvailabilityConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_MISSING_FLAG_ORDER: tuple[RegionalAvailabilityMissingDetailFlag, ...] = (
    "missing_region_list",
    "missing_launch_phase",
    "missing_fallback_behavior",
    "missing_owner",
    "missing_legal_compliance_basis",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_IGNORED_FIELDS = {
    "id",
    "source_id",
    "source_brief_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
    "status",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
}

_AVAILABILITY_FIELD_RE = re.compile(
    r"(?:availability|available|regional|region|geo|geograph|market|country|territor|"
    r"locale|locali[sz]ation|currency|rollout|launch|phase|wave|feature[-_ ]?flag|"
    r"blocked|excluded|restricted|unsupported|waitlist)",
    re.I,
)
_AVAILABILITY_SIGNAL_RE = re.compile(
    r"\b(?:available|availability|launch(?:es|ed|ing)?|roll(?:\s|-)?out|release|ship|"
    r"enable(?:d)?|feature[- ]?flag|gate(?:d)?|market(?:s)?|country|countries|region(?:s|al)?|"
    r"geo(?:graphy|graphic)?|territor(?:y|ies)|locale(?:s)?|locali[sz]ation|currency|"
    r"unsupported|unavailable|not\s+available|exclude(?:d|s|ing)?|block(?:ed|ing)?|"
    r"restrict(?:ed|ion|ions)?|embargo|sanction|waitlist)\b",
    re.I,
)
_USER_FACING_RE = re.compile(
    r"\b(?:user(?:s)?|customer(?:s)?|tenant(?:s)?|market(?:s)?|country|countries|region(?:s)?|"
    r"locale(?:s)?|currency|checkout|signup|sign[- ]?up|onboarding|billing|payment(?:s)?|"
    r"app|dashboard|portal|feature|ui|workflow|support|merchant(?:s)?|buyer(?:s)?)\b",
    re.I,
)
_DATA_RESIDENCY_ONLY_RE = re.compile(
    r"\b(?:data residency|data localisation|data localization|store data|stored in|process data|"
    r"processing in|eu data|regional data|cross[- ]border)\b",
    re.I,
)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|need(?:s)?|acceptance|done when|before launch|cannot|never|block)\b",
    re.I,
)
_EXCLUSION_RE = re.compile(
    r"\b(?:exclude(?:d|s|ing)?|except|not\s+available\s+in|unavailable\s+in|unsupported\s+in|"
    r"block(?:ed)?\s+in|restrict(?:ed)?\s+in|do\s+not\s+launch\s+in|no\s+access\s+in|"
    r"outside\s+of|except\s+for)\b",
    re.I,
)
_INCLUSION_RE = re.compile(
    r"\b(?:available\s+in|available\s+to|launch(?:es|ed|ing)?\s+in|launch(?:es|ed|ing)?\s+to|"
    r"roll(?:\s|-)?out\s+(?:in|to)|release\s+(?:in|to)|ship\s+(?:in|to)|enable(?:d)?\s+(?:in|for|to)|"
    r"support(?:ed)?\s+in|include(?:d|s|ing)?|market(?:s)?\:?)\b",
    re.I,
)
_PHASE_RE = re.compile(
    r"\b(?:phase\s*\d+|wave\s*\d+|pilot|beta|preview|early access|limited launch|soft launch|"
    r"initial(?:ly)?|first|then|later|ga|general availability|after [A-Za-z0-9 /_-]{2,60}|"
    r"before [A-Za-z0-9 /_-]{2,60})\b",
    re.I,
)
_FALLBACK_RE = re.compile(
    r"\b(?:fallback|waitlist|waiting list|show (?:an? )?(?:unavailable|unsupported|blocked) message|"
    r"unavailable message|unsupported message|hide (?:the )?feature|disable(?:d)?|redirect|"
    r"404|geo[- ]?block|block page|manual review|contact support|legal review|compliance review|"
    r"approval|allowlist|denylist|locali[sz]e|translation|currency|tax|vat|gdpr|ccpa|export control|"
    r"sanction(?:s)?|embargo|license|licensing)\b",
    re.I,
)
_COMPLIANCE_BLOCK_RE = re.compile(
    r"\b(?:legal|compliance|regulatory|gdpr|ccpa|export control|sanction(?:s)?|embargo|tax|vat|"
    r"license|licensing|privacy|consumer protection|age gate)\b",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:owner|owned by|legal|compliance|product|pm|growth|ops|operations|regional team|market lead)\b",
    re.I,
)
_NO_AVAILABILITY_CHANGE_RE = re.compile(
    r"\b(?:no|without)\s+(?:user[- ]facing\s+)?(?:launch|rollout|market|regional|availability)(?:\s+or\s+\w+)*\s+changes?\b|"
    r"\bno\b.{0,80}\bavailability\s+changes?\b",
    re.I,
)
_STRUCTURED_INCLUDE_RE = re.compile(r"(?:include|included|allow|allowed|available|launch|market|region|country)", re.I)
_STRUCTURED_EXCLUDE_RE = re.compile(r"(?:exclude|excluded|deny|denied|block|blocked|restrict|unsupported|unavailable)", re.I)

_REGION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("United States", re.compile(r"\b(?:United States|U\.S\.|US|USA)\b", re.I)),
    ("Canada", re.compile(r"\bCanada\b", re.I)),
    ("Quebec", re.compile(r"\bQuebec\b", re.I)),
    ("European Union", re.compile(r"\b(?:European Union|EU)\b", re.I)),
    ("United Kingdom", re.compile(r"\b(?:United Kingdom|UK|U\.K\.)\b", re.I)),
    ("Europe", re.compile(r"\bEurope\b", re.I)),
    ("Germany", re.compile(r"\bGermany\b", re.I)),
    ("France", re.compile(r"\bFrance\b", re.I)),
    ("Ireland", re.compile(r"\bIreland\b", re.I)),
    ("Netherlands", re.compile(r"\bNetherlands\b", re.I)),
    ("Spain", re.compile(r"\bSpain\b", re.I)),
    ("Italy", re.compile(r"\bItaly\b", re.I)),
    ("APAC", re.compile(r"\bAPAC\b", re.I)),
    ("EMEA", re.compile(r"\bEMEA\b", re.I)),
    ("LATAM", re.compile(r"\bLATAM\b", re.I)),
    ("Japan", re.compile(r"\bJapan\b", re.I)),
    ("Australia", re.compile(r"\bAustralia\b", re.I)),
    ("New Zealand", re.compile(r"\bNew Zealand\b", re.I)),
    ("India", re.compile(r"\bIndia\b", re.I)),
    ("Singapore", re.compile(r"\bSingapore\b", re.I)),
    ("South Korea", re.compile(r"\b(?:South Korea|Korea)\b", re.I)),
    ("China", re.compile(r"\bChina\b", re.I)),
    ("Brazil", re.compile(r"\bBrazil\b", re.I)),
    ("Mexico", re.compile(r"\bMexico\b", re.I)),
    ("California", re.compile(r"\bCalifornia\b", re.I)),
    ("North America", re.compile(r"\bNorth America\b", re.I)),
    ("Global", re.compile(r"\bglobal(?:ly)?\b", re.I)),
)
_SURFACE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("checkout", re.compile(r"\bcheckout\b", re.I)),
    ("payments", re.compile(r"\bpayments?\b", re.I)),
    ("billing", re.compile(r"\bbilling\b", re.I)),
    ("signup", re.compile(r"\bsign[- ]?up\b|\bregistration\b", re.I)),
    ("onboarding", re.compile(r"\bonboarding\b", re.I)),
    ("mobile app", re.compile(r"\bmobile app\b|\biOS\b|\bAndroid\b", re.I)),
    ("web app", re.compile(r"\bweb app\b|\bweb\b", re.I)),
    ("dashboard", re.compile(r"\bdashboards?\b", re.I)),
    ("reports", re.compile(r"\breports?\b|\bexports?\b", re.I)),
    ("marketplace", re.compile(r"\bmarketplace\b", re.I)),
    ("customer support", re.compile(r"\bcustomer support\b|\bsupport\b", re.I)),
    ("feature", re.compile(r"\bfeatures?\b|\bworkflow\b", re.I)),
)


@dataclass(frozen=True, slots=True)
class SourceRegionalAvailabilityRequirement:
    """One source-backed regional availability or market restriction requirement."""

    source_brief_id: str | None
    availability_surface: str
    included_regions: tuple[str, ...] = field(default_factory=tuple)
    excluded_regions: tuple[str, ...] = field(default_factory=tuple)
    rollout_phase_hints: tuple[str, ...] = field(default_factory=tuple)
    fallback_compliance_hints: tuple[str, ...] = field(default_factory=tuple)
    missing_detail_flags: tuple[RegionalAvailabilityMissingDetailFlag, ...] = field(default_factory=tuple)
    confidence: RegionalAvailabilityConfidence = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "availability_surface": self.availability_surface,
            "included_regions": list(self.included_regions),
            "excluded_regions": list(self.excluded_regions),
            "rollout_phase_hints": list(self.rollout_phase_hints),
            "fallback_compliance_hints": list(self.fallback_compliance_hints),
            "missing_detail_flags": list(self.missing_detail_flags),
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceRegionalAvailabilityRequirementsReport:
    """Brief-level regional availability requirements report."""

    source_brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceRegionalAvailabilityRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceRegionalAvailabilityRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return regional availability requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Regional Availability Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        confidence_counts = self.summary.get("confidence_counts", {})
        missing_detail_counts = self.summary.get("missing_detail_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Included regions: {self.summary.get('included_region_count', 0)}",
            f"- Excluded regions: {self.summary.get('excluded_region_count', 0)}",
            "- Confidence counts: "
            + ", ".join(
                f"{confidence} {confidence_counts.get(confidence, 0)}"
                for confidence in _CONFIDENCE_ORDER
            ),
            "- Missing detail counts: "
            + ", ".join(
                f"{flag} {missing_detail_counts.get(flag, 0)}" for flag in _MISSING_FLAG_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source regional availability requirements were found in the brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Surface | Included Regions | Excluded Regions | Phase Hints | Fallback/Compliance | Missing Details | Confidence | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.availability_surface)} | "
                f"{_markdown_cell(', '.join(requirement.included_regions) or 'unspecified')} | "
                f"{_markdown_cell(', '.join(requirement.excluded_regions) or 'none')} | "
                f"{_markdown_cell('; '.join(requirement.rollout_phase_hints) or 'unspecified')} | "
                f"{_markdown_cell('; '.join(requirement.fallback_compliance_hints) or 'none')} | "
                f"{_markdown_cell(', '.join(requirement.missing_detail_flags) or 'none')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    availability_surface: str
    included_regions: tuple[str, ...]
    excluded_regions: tuple[str, ...]
    rollout_phase_hints: tuple[str, ...]
    fallback_compliance_hints: tuple[str, ...]
    evidence: str
    confidence: RegionalAvailabilityConfidence
    owner_present: bool


def build_source_regional_availability_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceRegionalAvailabilityRequirementsReport:
    """Extract regional availability requirements from source or implementation brief input."""
    source_brief_id, payload = _source_payload(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_requirement_candidates(source_brief_id, payload)),
            key=lambda requirement: (
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.availability_surface.casefold(),
                requirement.included_regions,
                requirement.excluded_regions,
                requirement.rollout_phase_hints,
                requirement.evidence,
            ),
        )
    )
    return SourceRegionalAvailabilityRequirementsReport(
        source_brief_id=source_brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def generate_source_regional_availability_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceRegionalAvailabilityRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_regional_availability_requirements(source)


def extract_source_regional_availability_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceRegionalAvailabilityRequirement, ...]:
    """Return regional availability requirement records from brief-shaped input."""
    return build_source_regional_availability_requirements(source).requirements


def source_regional_availability_requirements_to_dict(
    report: SourceRegionalAvailabilityRequirementsReport,
) -> dict[str, Any]:
    """Serialize a regional availability requirements report to a plain dictionary."""
    return report.to_dict()


source_regional_availability_requirements_to_dict.__test__ = False


def source_regional_availability_requirements_to_dicts(
    requirements: (
        tuple[SourceRegionalAvailabilityRequirement, ...]
        | list[SourceRegionalAvailabilityRequirement]
        | SourceRegionalAvailabilityRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize regional availability requirement records to dictionaries."""
    if isinstance(requirements, SourceRegionalAvailabilityRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_regional_availability_requirements_to_dicts.__test__ = False


def source_regional_availability_requirements_to_markdown(
    report: SourceRegionalAvailabilityRequirementsReport,
) -> str:
    """Render a regional availability requirements report as Markdown."""
    return report.to_markdown()


source_regional_availability_requirements_to_markdown.__test__ = False


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = source.model_dump(mode="python")
        return _source_brief_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _source_brief_id(payload), payload
    if isinstance(source, Mapping):
        payload = _validated_payload(source)
        return _source_brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_brief_id(payload), payload
    return None, {}


def _validated_payload(source: Mapping[str, Any]) -> dict[str, Any]:
    for model in (SourceBrief, ImplementationBrief):
        try:
            return dict(model.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            continue
    return dict(source)


def _source_brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(source_brief_id: str | None, payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    owner_present = _payload_has_owner_hint(payload)
    for source_field, text in _candidate_segments(payload):
        if not _is_availability_requirement(text, source_field):
            continue
        included, excluded = _regions_by_status(text, source_field)
        phases = _phase_hints(text)
        fallback = _fallback_compliance_hints(text)
        candidates.append(
            _Candidate(
                source_brief_id=source_brief_id,
                availability_surface=_availability_surface(text, source_field),
                included_regions=included,
                excluded_regions=excluded,
                rollout_phase_hints=phases,
                fallback_compliance_hints=fallback,
                evidence=_evidence_snippet(source_field, text),
                confidence=_confidence(text, source_field, included, excluded, phases, fallback),
                owner_present=owner_present or bool(_OWNER_RE.search(f"{source_field} {text}")),
            )
        )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceRegionalAvailabilityRequirement]:
    grouped: dict[tuple[str | None, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.availability_surface), []).append(candidate)
    for source_brief_id in {key[0] for key in grouped}:
        default_key = (source_brief_id, "product availability")
        concrete_keys = [
            key for key in grouped if key[0] == source_brief_id and key[1] != "product availability"
        ]
        if default_key in grouped and len(concrete_keys) == 1:
            grouped[concrete_keys[0]].extend(grouped.pop(default_key))

    requirements: list[SourceRegionalAvailabilityRequirement] = []
    for (source_brief_id, surface), items in grouped.items():
        excluded = tuple(sorted(_dedupe(region for item in items for region in item.excluded_regions), key=str.casefold))
        included = tuple(
            region
            for region in sorted(
                _dedupe(region for item in items for region in item.included_regions),
                key=str.casefold,
            )
            if region not in excluded
        )
        phases = tuple(_dedupe(hint for item in items for hint in item.rollout_phase_hints))
        fallback = tuple(_dedupe(hint for item in items for hint in item.fallback_compliance_hints))
        evidence = tuple(sorted(_dedupe(item.evidence for item in items), key=str.casefold))[:8]
        flags = _missing_detail_flags(items, included, excluded, phases, fallback)
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        if flags and confidence == "high":
            confidence = "medium"
        requirements.append(
            SourceRegionalAvailabilityRequirement(
                source_brief_id=source_brief_id,
                availability_surface=surface,
                included_regions=included,
                excluded_regions=excluded,
                rollout_phase_hints=phases,
                fallback_compliance_hints=fallback,
                missing_detail_flags=flags,
                confidence=confidence,
                evidence=evidence,
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    segments: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "problem_statement",
        "mvp_goal",
        "workflow_context",
        "product_surface",
        "scope",
        "requirements",
        "constraints",
        "implementation_constraints",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "data_requirements",
        "architecture_notes",
        "implementation_notes",
        "risks",
        "assumptions",
        "open_questions",
        "questions",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key])
    return [(field, segment) for field, segment in segments if segment]


def _append_value(segments: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _any_signal(key_text):
                segments.append((child_field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                _append_value(segments, child_field, child)
            elif text := _optional_text(child):
                for segment in _segments(text):
                    segments.append((child_field, segment))
                if _any_signal(key_text):
                    for segment in _segments(f"{key_text}: {text}"):
                        segments.append((child_field, segment))
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            segments.append((source_field, segment))


def _segments(value: str) -> list[str]:
    parts: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        sentence_parts = (
            [_clean_text(cleaned)]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for sentence in sentence_parts:
            parts.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in parts if _clean_text(part)]


def _is_availability_requirement(text: str, source_field: str) -> bool:
    searchable = f"{source_field.replace('_', ' ').replace('-', ' ')} {text}"
    if _NO_AVAILABILITY_CHANGE_RE.search(text):
        return False
    has_availability_signal = bool(_AVAILABILITY_SIGNAL_RE.search(searchable))
    has_region = bool(_regions(text))
    has_user_surface = bool(_USER_FACING_RE.search(searchable))
    if not has_availability_signal:
        return False
    if _is_data_residency_only(text, searchable):
        return False
    if has_region:
        return True
    if source_field == "title":
        return False
    return has_user_surface or bool(_AVAILABILITY_FIELD_RE.search(source_field))


def _is_data_residency_only(text: str, searchable: str) -> bool:
    if not _DATA_RESIDENCY_ONLY_RE.search(text):
        return False
    user_availability = re.search(
        r"\b(?:available|availability|launch|roll(?:\s|-)?out|enable|block users|unavailable|unsupported|"
        r"market|locale|currency|checkout|signup|feature)\b",
        searchable,
        re.I,
    )
    return not bool(user_availability)


def _regions_by_status(text: str, source_field: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    regions = _regions(text)
    if not regions:
        return (), ()
    structured_exclusion = bool(_STRUCTURED_EXCLUDE_RE.search(source_field))
    structured_inclusion = bool(_STRUCTURED_INCLUDE_RE.search(source_field))
    excluded: list[str] = []
    included: list[str] = []
    for region in regions:
        context = _region_context(text, region)
        if structured_exclusion or _EXCLUSION_RE.search(context):
            excluded.append(region)
        elif structured_inclusion or _INCLUSION_RE.search(context) or _INCLUSION_RE.search(text):
            included.append(region)
        elif _EXCLUSION_RE.search(text) and not _INCLUSION_RE.search(text):
            excluded.append(region)
        else:
            included.append(region)
    return (
        tuple(sorted(_dedupe(included), key=str.casefold)),
        tuple(sorted(_dedupe(excluded), key=str.casefold)),
    )


def _regions(text: str) -> tuple[str, ...]:
    return tuple(_dedupe(region for region, pattern in _REGION_PATTERNS if pattern.search(text)))


def _region_context(text: str, region: str) -> str:
    pattern = next((compiled for name, compiled in _REGION_PATTERNS if name == region), None)
    if pattern is None:
        return text
    match = pattern.search(text)
    if not match:
        return text
    start = max(0, match.start() - 80)
    end = min(len(text), match.end() + 80)
    return text[start:end]


def _phase_hints(text: str) -> tuple[str, ...]:
    return tuple(_dedupe(_clean_text(match.group(0)) for match in _PHASE_RE.finditer(text)))


def _fallback_compliance_hints(text: str) -> tuple[str, ...]:
    return tuple(_dedupe(_clean_text(match.group(0)) for match in _FALLBACK_RE.finditer(text)))


def _availability_surface(text: str, source_field: str) -> str:
    searchable = f"{source_field.replace('_', ' ').replace('-', ' ')} {text}"
    for surface, pattern in _SURFACE_PATTERNS:
        if pattern.search(searchable):
            return surface
    parts = [
        part
        for part in re.split(r"[.\[\]_\-\s]+", source_field)
        if part
        and not part.isdigit()
        and part
        not in {
            "source",
            "payload",
            "metadata",
            "brief",
            "requirements",
            "availability",
            "regional",
            "regions",
        }
    ]
    if parts and _AVAILABILITY_FIELD_RE.search(source_field):
        return _clean_text(" ".join(parts[-2:]))
    return "product availability"


def _missing_detail_flags(
    items: list[_Candidate],
    included: tuple[str, ...],
    excluded: tuple[str, ...],
    phases: tuple[str, ...],
    fallback: tuple[str, ...],
) -> tuple[RegionalAvailabilityMissingDetailFlag, ...]:
    combined = " ".join(item.evidence for item in items)
    flags: list[RegionalAvailabilityMissingDetailFlag] = []
    phased_signal = bool(re.search(r"\b(?:roll(?:\s|-)?out|launch|phase|wave|pilot|beta|market expansion)\b", combined, re.I))
    restriction_signal = bool(_EXCLUSION_RE.search(combined) or re.search(r"\b(?:blocked|unavailable|unsupported|restricted)\b", combined, re.I))
    compliance_signal = bool(_COMPLIANCE_BLOCK_RE.search(combined))
    if not included and not excluded:
        flags.append("missing_region_list")
    if phased_signal and not phases:
        flags.append("missing_launch_phase")
    if (restriction_signal or phased_signal) and not fallback:
        flags.append("missing_fallback_behavior")
    if not any(item.owner_present for item in items) and not _OWNER_RE.search(combined):
        flags.append("missing_owner")
    if compliance_signal and not fallback:
        flags.append("missing_legal_compliance_basis")
    return tuple(flag for flag in _MISSING_FLAG_ORDER if flag in flags)


def _confidence(
    text: str,
    source_field: str,
    included: tuple[str, ...],
    excluded: tuple[str, ...],
    phases: tuple[str, ...],
    fallback: tuple[str, ...],
) -> RegionalAvailabilityConfidence:
    structured_field = bool(_AVAILABILITY_FIELD_RE.search(source_field))
    if (included or excluded) and (_REQUIRED_RE.search(text) or structured_field) and (phases or fallback):
        return "high"
    if (included or excluded) and (_REQUIRED_RE.search(text) or structured_field or phases or fallback):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceRegionalAvailabilityRequirement, ...]) -> dict[str, Any]:
    included_regions = tuple(
        sorted(_dedupe(region for requirement in requirements for region in requirement.included_regions), key=str.casefold)
    )
    excluded_regions = tuple(
        sorted(_dedupe(region for requirement in requirements for region in requirement.excluded_regions), key=str.casefold)
    )
    surfaces = sorted({requirement.availability_surface for requirement in requirements}, key=str.casefold)
    return {
        "requirement_count": len(requirements),
        "included_region_count": len(included_regions),
        "excluded_region_count": len(excluded_regions),
        "included_regions": list(included_regions),
        "excluded_regions": list(excluded_regions),
        "availability_surfaces": surfaces,
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "missing_detail_counts": {
            flag: sum(1 for requirement in requirements if flag in requirement.missing_detail_flags)
            for flag in _MISSING_FLAG_ORDER
        },
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
        "source_project",
        "source_entity_type",
        "source_payload",
        "source_links",
        "target_user",
        "buyer",
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "product_surface",
        "scope",
        "non_goals",
        "assumptions",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "validation_plan",
        "definition_of_done",
        "requirements",
        "constraints",
        "acceptance_criteria",
        "metadata",
        "brief_metadata",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _payload_has_owner_hint(payload: Mapping[str, Any]) -> bool:
    for source_field, text in _candidate_segments(payload):
        if _OWNER_RE.search(f"{source_field} {text}"):
            return True
    return False


def _any_signal(text: str) -> bool:
    return bool(
        _AVAILABILITY_SIGNAL_RE.search(text)
        or _AVAILABILITY_FIELD_RE.search(text)
        or _FALLBACK_RE.search(text)
        or _PHASE_RE.search(text)
    )


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


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[Any] = set()
    for value in values:
        key = value.casefold() if isinstance(value, str) else value
        if not value or key in seen:
            continue
        result.append(value)
        seen.add(key)
    return result


__all__ = [
    "RegionalAvailabilityConfidence",
    "RegionalAvailabilityMissingDetailFlag",
    "SourceRegionalAvailabilityRequirement",
    "SourceRegionalAvailabilityRequirementsReport",
    "build_source_regional_availability_requirements",
    "extract_source_regional_availability_requirements",
    "generate_source_regional_availability_requirements",
    "source_regional_availability_requirements_to_dict",
    "source_regional_availability_requirements_to_dicts",
    "source_regional_availability_requirements_to_markdown",
]
