"""Extract source-level SAML SSO and SCIM provisioning requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceSAMLSCIMRequirementType = Literal[
    "saml_sso",
    "saml_metadata",
    "idp_initiated_login",
    "sp_initiated_login",
    "acs_url",
    "entity_id",
    "certificate_rotation",
    "jit_provisioning",
    "scim_users",
    "scim_groups",
    "deprovisioning",
    "group_mapping",
    "tenant_identity_settings",
]
SourceSAMLSCIMConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[SourceSAMLSCIMRequirementType, ...] = (
    "saml_sso",
    "saml_metadata",
    "idp_initiated_login",
    "sp_initiated_login",
    "acs_url",
    "entity_id",
    "certificate_rotation",
    "jit_provisioning",
    "scim_users",
    "scim_groups",
    "deprovisioning",
    "group_mapping",
    "tenant_identity_settings",
)
_CONFIDENCE_ORDER: dict[SourceSAMLSCIMConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_DIRECTIVE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|enable|configure|provide|document|maps?|sync|provision|deprovision|"
    r"rotate|acceptance|done when|before launch|cannot ship)\b",
    re.I,
)
_IDENTITY_CONTEXT_RE = re.compile(
    r"\b(?:identity|sso|single sign[- ]?on|saml|idp|identity provider|scim|directory|"
    r"provision(?:ing)?|deprovision(?:ing)?|jit|just[- ]in[- ]time|login|sign[- ]?in|"
    r"certificate|x\.?509|tenant|workspace|organization|groups?|roles?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:identity|sso|saml|scim|idp|provision|deprovision|directory|groups?|roles?|"
    r"tenant|workspace|organization|requirements?|acceptance|criteria|constraints?|"
    r"metadata|source[_ -]?payload|implementation[_ -]?notes)",
    re.I,
)
_OUT_OF_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|outside scope|non[- ]?goal|defer|deferred)\b"
    r".{0,140}\b(?:identity provisioning|identity setup|saml|sso|scim|directory sync|"
    r"user provisioning|group provisioning|deprovisioning|group mapping|role mapping)\b"
    r".{0,140}\b(?:required|needed|in scope|supported|support|work|changes?|planned|"
    r"for this release)?\b|"
    r"\b(?:identity provisioning|identity setup|saml|sso|scim|directory sync|"
    r"user provisioning|group provisioning|deprovisioning|group mapping|role mapping)\b"
    r".{0,140}\b(?:out of scope|outside scope|not required|not needed|no support|"
    r"unsupported|no work|non[- ]?goal|deferred)\b",
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
    "non_goals",
    "assumptions",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "identity",
    "authentication",
    "auth",
    "sso",
    "saml",
    "scim",
    "security",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_TYPE_PATTERNS: dict[SourceSAMLSCIMRequirementType, re.Pattern[str]] = {
    "saml_sso": re.compile(r"\b(?:saml(?:\s*2(?:\.0)?)?|saml sso|single sign[- ]?on|sso)\b", re.I),
    "saml_metadata": re.compile(r"\b(?:saml metadata|metadata xml|idp metadata|sp metadata|metadata url|metadata file)\b", re.I),
    "idp_initiated_login": re.compile(r"\b(?:idp[- ]initiated|identity provider[- ]initiated|idp initiated login|idp initiated sso)\b", re.I),
    "sp_initiated_login": re.compile(r"\b(?:sp[- ]initiated|service provider[- ]initiated|sp initiated login|sp initiated sso)\b", re.I),
    "acs_url": re.compile(r"\b(?:acs url|assertion consumer service|assertion consumer service url|reply url|callback url)\b", re.I),
    "entity_id": re.compile(r"\b(?:entity id|entityid|sp entity|idp entity|audience uri|audience restriction)\b", re.I),
    "certificate_rotation": re.compile(r"\b(?:certificate rotation|cert rotation|x\.?509|signing certificate|saml certificate|certificate expiry|certificate expiration|rotate certificates?)\b", re.I),
    "jit_provisioning": re.compile(r"\b(?:jit provisioning|just[- ]in[- ]time provisioning|just in time provisioning|auto[- ]provision|create users? on first login|provision users? on first login)\b", re.I),
    "scim_users": re.compile(r"\b(?:scim(?:\s*2(?:\.0)?)?|scim users?|user provisioning|provision users?|user lifecycle|directory sync)\b", re.I),
    "scim_groups": re.compile(r"\b(?:scim groups?|group provisioning|provision groups?|group sync|directory groups?)\b", re.I),
    "deprovisioning": re.compile(r"\b(?:deprovision(?:ing)?|disable users?|suspend users?|remove users?|user offboarding|offboard users?|revoke access)\b", re.I),
    "group_mapping": re.compile(r"\b(?:group mapping|group mappings|role mapping|map groups?|map idp groups?|claims? mapping|groups? claim|attribute mapping|map roles?)\b", re.I),
    "tenant_identity_settings": re.compile(r"\b(?:tenant[- ]specific|per[- ]tenant|workspace[- ]specific|organization[- ]specific|org[- ]specific|tenant identity|identity settings|sso settings|scim token|tenant domain)\b", re.I),
}
_PLANNING_NOTES: dict[SourceSAMLSCIMRequirementType, str] = {
    "saml_sso": "Plan customer SAML SSO setup, enforcement rules, and launch validation.",
    "saml_metadata": "Collect IdP/SP metadata exchange format, owner, and validation steps.",
    "idp_initiated_login": "Confirm IdP-initiated login support and relay state behavior.",
    "sp_initiated_login": "Confirm SP-initiated login routing and tenant discovery behavior.",
    "acs_url": "Provide ACS/reply URL values for each environment and tenant.",
    "entity_id": "Provide SP and IdP entity ID/audience values for setup guides.",
    "certificate_rotation": "Plan certificate expiry monitoring, overlap, and rotation runbooks.",
    "jit_provisioning": "Define first-login user creation, default role, and eligibility rules.",
    "scim_users": "Define SCIM user lifecycle operations, schema fields, and token handling.",
    "scim_groups": "Define SCIM group sync scope and conflict behavior.",
    "deprovisioning": "Define deprovisioning effects for sessions, ownership, and access revocation.",
    "group_mapping": "Map IdP groups or claims to application roles and conflict handling.",
    "tenant_identity_settings": "Model tenant-specific identity settings, secrets, and admin controls.",
}


@dataclass(frozen=True, slots=True)
class SourceSAMLSCIMRequirement:
    """One source-backed SAML SSO or SCIM provisioning requirement."""

    source_brief_id: str | None
    requirement_type: SourceSAMLSCIMRequirementType
    requirement_text: str
    source_field: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SourceSAMLSCIMConfidence = "medium"
    planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "requirement_text": self.requirement_text,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceSAMLSCIMRequirementsReport:
    """Source-level SAML SSO and SCIM provisioning requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceSAMLSCIMRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSAMLSCIMRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceSAMLSCIMRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return SAML/SCIM requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source SAML SCIM Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Source count: {self.summary.get('source_count', 0)}",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Requirement type counts: "
            + ", ".join(
                f"{requirement_type} {type_counts.get(requirement_type, 0)}"
                for requirement_type in _TYPE_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source SAML/SCIM requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Confidence | Source Field | Requirement | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_type} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} |"
            )
        return "\n".join(lines)


def build_source_saml_scim_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSAMLSCIMRequirementsReport:
    """Extract SAML SSO and SCIM provisioning requirement signals from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceSAMLSCIMRequirementsReport(
        source_brief_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_saml_scim_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSAMLSCIMRequirementsReport:
    """Compatibility alias for building a SAML/SCIM requirements report."""
    return build_source_saml_scim_requirements(source)


def generate_source_saml_scim_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSAMLSCIMRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_saml_scim_requirements(source)


def derive_source_saml_scim_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSAMLSCIMRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_saml_scim_requirements(source)


def summarize_source_saml_scim_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceSAMLSCIMRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic SAML/SCIM requirements summary."""
    if isinstance(source_or_result, SourceSAMLSCIMRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_saml_scim_requirements(source_or_result).summary


def source_saml_scim_requirements_to_dict(
    report: SourceSAMLSCIMRequirementsReport,
) -> dict[str, Any]:
    """Serialize a SAML/SCIM requirements report to a plain dictionary."""
    return report.to_dict()


source_saml_scim_requirements_to_dict.__test__ = False


def source_saml_scim_requirements_to_dicts(
    requirements: (
        tuple[SourceSAMLSCIMRequirement, ...]
        | list[SourceSAMLSCIMRequirement]
        | SourceSAMLSCIMRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source SAML/SCIM requirement records to dictionaries."""
    if isinstance(requirements, SourceSAMLSCIMRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_saml_scim_requirements_to_dicts.__test__ = False


def source_saml_scim_requirements_to_markdown(
    report: SourceSAMLSCIMRequirementsReport,
) -> str:
    """Render a SAML/SCIM requirements report as Markdown."""
    return report.to_markdown()


source_saml_scim_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_type: SourceSAMLSCIMRequirementType
    requirement_text: str
    source_field: str
    evidence: str
    confidence: SourceSAMLSCIMConfidence


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


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        segments = _candidate_segments(payload)
        if _has_global_no_signal(segments):
            continue
        for segment in segments:
            if not _is_requirement(segment):
                continue
            searchable = _searchable_text(segment.source_field, segment.text)
            for requirement_type in _requirement_types(searchable):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        requirement_type=requirement_type,
                        requirement_text=_requirement_text(segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(requirement_type, segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceSAMLSCIMRequirement]:
    grouped: dict[tuple[str | None, SourceSAMLSCIMRequirementType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.requirement_type), []).append(candidate)

    requirements: list[SourceSAMLSCIMRequirement] = []
    for (source_brief_id, requirement_type), items in grouped.items():
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        requirements.append(
            SourceSAMLSCIMRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                requirement_text=_best_requirement_text(items),
                source_field=_best_source_field(items),
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                confidence=confidence,
                planning_note=_PLANNING_NOTES[requirement_type],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _TYPE_ORDER.index(requirement.requirement_type),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field.casefold(),
            requirement.requirement_text.casefold(),
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
        if key not in visited and str(key) not in _IGNORED_FIELDS:
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
                _STRUCTURED_FIELD_RE.search(key_text) or _IDENTITY_CONTEXT_RE.search(key_text)
            )
            child = value[key]
            if child_context and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    for segment_text in _segments(f"{key_text}: {text}"):
                        segments.append(_Segment(child_field, segment_text, child_context))
                continue
            _append_value(segments, child_field, child, child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text in _segments(text):
            segments.append(_Segment(source_field, segment_text, field_context))


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
            clauses = [part] if _OUT_OF_SCOPE_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _has_global_no_signal(segments: Iterable[_Segment]) -> bool:
    return any(
        _OUT_OF_SCOPE_RE.search(_searchable_text(segment.source_field, segment.text))
        for segment in segments
    )


def _is_requirement(segment: _Segment) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    if _OUT_OF_SCOPE_RE.search(searchable):
        return False
    if not _IDENTITY_CONTEXT_RE.search(searchable):
        return False
    if not any(pattern.search(searchable) for pattern in _TYPE_PATTERNS.values()):
        return False
    return segment.section_context or bool(_DIRECTIVE_RE.search(searchable))


def _requirement_types(searchable: str) -> tuple[SourceSAMLSCIMRequirementType, ...]:
    return tuple(
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(searchable)
    )


def _confidence(
    requirement_type: SourceSAMLSCIMRequirementType,
    segment: _Segment,
) -> SourceSAMLSCIMConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    has_directive = bool(_DIRECTIVE_RE.search(searchable))
    has_structured_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    has_specific_detail = requirement_type in {
        "saml_metadata",
        "idp_initiated_login",
        "sp_initiated_login",
        "acs_url",
        "entity_id",
        "certificate_rotation",
        "jit_provisioning",
        "scim_users",
        "scim_groups",
        "deprovisioning",
        "group_mapping",
        "tenant_identity_settings",
    }
    if has_directive and (has_specific_detail or has_structured_context):
        return "high"
    if has_directive or has_structured_context or has_specific_detail:
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceSAMLSCIMRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    status = "ready_for_planning" if requirements else "no_saml_scim_language"
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "type_counts": {
            requirement_type: sum(
                1
                for requirement in requirements
                if requirement.requirement_type == requirement_type
            )
            for requirement_type in _TYPE_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "requirement_types": [
            requirement_type
            for requirement_type in _TYPE_ORDER
            if any(requirement.requirement_type == requirement_type for requirement in requirements)
        ],
        "status": status,
    }


def _best_requirement_text(items: Iterable[_Candidate]) -> str:
    candidates = sorted(
        items,
        key=lambda item: (
            _CONFIDENCE_ORDER[item.confidence],
            -len(item.requirement_text),
            item.requirement_text.casefold(),
        ),
    )
    return candidates[0].requirement_text if candidates else ""


def _best_source_field(items: Iterable[_Candidate]) -> str:
    fields = sorted(_dedupe(item.source_field for item in items), key=str.casefold)
    return fields[0] if fields else ""


def _requirement_text(text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 220:
        cleaned = f"{cleaned[:217].rstrip()}..."
    return cleaned


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
        "non_goals",
        "assumptions",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "identity",
        "authentication",
        "auth",
        "sso",
        "saml",
        "scim",
        "security",
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
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = _clean_text(str(value)).casefold()
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
    return sorted(deduped, key=str.casefold)


__all__ = [
    "SourceSAMLSCIMConfidence",
    "SourceSAMLSCIMRequirement",
    "SourceSAMLSCIMRequirementType",
    "SourceSAMLSCIMRequirementsReport",
    "build_source_saml_scim_requirements",
    "derive_source_saml_scim_requirements",
    "extract_source_saml_scim_requirements",
    "generate_source_saml_scim_requirements",
    "source_saml_scim_requirements_to_dict",
    "source_saml_scim_requirements_to_dicts",
    "source_saml_scim_requirements_to_markdown",
    "summarize_source_saml_scim_requirements",
]
