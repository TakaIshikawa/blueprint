"""Extract source-level SCIM and identity lifecycle provisioning requirements."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SCIMProvisioningAction = Literal[
    "provisioning",
    "deprovisioning",
    "group_sync",
    "group_push",
    "role_mapping",
    "jit_provisioning",
    "directory_sync",
    "suspension",
    "external_id_mapping",
    "idp_constraint",
]
SCIMProvisioningConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_ACTION_ORDER: tuple[SCIMProvisioningAction, ...] = (
    "provisioning",
    "deprovisioning",
    "group_sync",
    "group_push",
    "role_mapping",
    "jit_provisioning",
    "directory_sync",
    "suspension",
    "external_id_mapping",
    "idp_constraint",
)
_CONFIDENCE_ORDER: dict[SCIMProvisioningConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_PLANNING_NOTES: dict[SCIMProvisioningAction, str] = {
    "provisioning": "Define SCIM user create/update operations, required attributes, token scope, and idempotency.",
    "deprovisioning": "Define offboarding effects for access, sessions, owned resources, and audit trails.",
    "group_sync": "Define group membership sync scope, conflict behavior, and reconciliation cadence.",
    "group_push": "Confirm IdP group push support, naming rules, and downstream membership ownership.",
    "role_mapping": "Map IdP groups, claims, or directory attributes to application roles and precedence rules.",
    "jit_provisioning": "Define first-login user creation, eligibility checks, default role, and fallback handling.",
    "directory_sync": "Define directory sync source of truth, sync cadence, partial failures, and drift handling.",
    "suspension": "Define suspended-user state, login blocking, reactivation, and retained data behavior.",
    "external_id_mapping": "Persist externalId/nameID mappings and uniqueness rules for lifecycle correlation.",
    "idp_constraint": "Capture enterprise IdP constraints, supported providers, tenant settings, and setup ownership.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_DIRECTIVE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|enable|configure|provide|document|map|maps?|sync|provision|"
    r"deprovision|suspend|disable|reactivate|revoke|create|update|delete|store|"
    r"before launch|cannot ship|done when|acceptance)\b",
    re.I,
)
_IDENTITY_CONTEXT_RE = re.compile(
    r"\b(?:scim|identity|idp|identity provider|okta|azure ad|entra|onelogin|ping|"
    r"google workspace|directory|directory sync|group push|group sync|user provisioning|"
    r"provisioning|deprovisioning|jit|just[- ]in[- ]time|suspended users?|externalid|"
    r"external id|nameid|roles?|groups?|claims?|enterprise)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:scim|identity|idp|provision|deprovision|directory|lifecycle|groups?|roles?|"
    r"jit|suspend|external[_ -]?id|mapping|enterprise|requirements?|acceptance|"
    r"constraints?|metadata|source[_ -]?payload|implementation[_ -]?notes)",
    re.I,
)
_OUT_OF_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|outside scope|non[- ]?goal|defer|deferred)\b"
    r".{0,140}\b(?:scim|identity lifecycle|identity provisioning|user provisioning|"
    r"deprovisioning|directory sync|group sync|group push|role mapping|jit provisioning)\b"
    r".{0,140}\b(?:required|needed|in scope|supported|support|work|changes?|planned|"
    r"for this release)?\b|"
    r"\b(?:scim|identity lifecycle|identity provisioning|user provisioning|"
    r"deprovisioning|directory sync|group sync|group push|role mapping|jit provisioning)\b"
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
    "identity_lifecycle",
    "authentication",
    "auth",
    "sso",
    "saml",
    "scim",
    "directory",
    "security",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_ACTION_PATTERNS: dict[SCIMProvisioningAction, re.Pattern[str]] = {
    "provisioning": re.compile(r"\b(?:scim(?:\s*2(?:\.0)?)?.{0,60}(?:provision|users?)|user provisioning|provision users?|create users?|update users?|user lifecycle)\b", re.I),
    "deprovisioning": re.compile(r"\b(?:deprovision(?:ing)?|remove users?|delete users?|disable users?|user offboarding|offboard users?|revoke access)\b", re.I),
    "group_sync": re.compile(r"\b(?:scim groups?|group provisioning|group sync|sync groups?|directory groups?|group membership sync)\b", re.I),
    "group_push": re.compile(r"\b(?:group push|push groups?|pushed groups?|idp group push|okta group push)\b", re.I),
    "role_mapping": re.compile(r"\b(?:role mapping|map roles?|mapped roles?|group mapping|map idp groups?|claims? mapping|groups? claim|attribute mapping|entitlement mapping)\b", re.I),
    "jit_provisioning": re.compile(r"\b(?:jit provisioning|just[- ]in[- ]time provisioning|just in time provisioning|auto[- ]provision|create users? on first login|provision users? on first login)\b", re.I),
    "directory_sync": re.compile(r"\b(?:directory sync|directory synchronization|sync from directory|directory groups?|active directory|ldap|hris|workday|directory as source of truth)\b", re.I),
    "suspension": re.compile(r"\b(?:suspend users?|suspended users?|user suspension|disable login|blocked users?|inactive users?|reactivat(?:e|ion))\b", re.I),
    "external_id_mapping": re.compile(r"\b(?:externalid|external id|external identifier|nameid|name id|idp user id|directory id|immutable id|subject id)\b", re.I),
    "idp_constraint": re.compile(r"\b(?:enterprise idp|identity provider|idp constraints?|tenant identity|scim token|per[- ]tenant)\b", re.I),
}
_PROVIDER_RE = re.compile(
    r"\b(?:Okta|Azure AD|Microsoft Entra|Entra ID|OneLogin|Ping(?:Identity)?|Google Workspace|"
    r"Google Cloud Identity|Azure|ADFS|Active Directory|LDAP|Workday|HRIS|enterprise IdP|IdP|identity provider)\b",
    re.I,
)
_TIMING_RE = re.compile(
    r"\b(?:before launch|at launch|on first login|first login|real[- ]?time|near real[- ]?time|"
    r"hourly|daily|nightly|within \d+\s*(?:minutes?|hours?|days?)|after termination|on offboarding)\b",
    re.I,
)
_ENTITY_RE = re.compile(
    r"\b(?:users?|members?|accounts?|groups?|roles?|claims?|attributes?|externalId|external ID|"
    r"nameID|directory ID|entitlements?|suspended users?|inactive users?)\b",
    re.I,
)
_EXPLICIT_PROVISIONING_RE = re.compile(
    r"\b(?:user provisioning|provision users?|create users?|update users?|scim(?:\s*2(?:\.0)?)?.{0,60}provision)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SourceSCIMProvisioningRequirement:
    """One source-backed SCIM or identity lifecycle provisioning requirement."""

    lifecycle_action: SCIMProvisioningAction
    identity_provider: str = ""
    mapped_entity: str = ""
    timing: str = ""
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SCIMProvisioningConfidence = "medium"
    planning_note: str = ""
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)

    @property
    def category(self) -> SCIMProvisioningAction:
        """Compatibility view for extractors that expose category."""
        return self.lifecycle_action

    @property
    def requirement_category(self) -> SCIMProvisioningAction:
        """Compatibility view for extractors that expose requirement_category."""
        return self.lifecycle_action

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "lifecycle_action": self.lifecycle_action,
            "identity_provider": self.identity_provider,
            "mapped_entity": self.mapped_entity,
            "timing": self.timing,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
            "unresolved_questions": list(self.unresolved_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceSCIMProvisioningRequirementsReport:
    """Source-level SCIM and identity lifecycle provisioning requirements report."""

    source_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceSCIMProvisioningRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSCIMProvisioningRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceSCIMProvisioningRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "title": self.title,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return SCIM provisioning requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source SCIM Provisioning Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        action_counts = self.summary.get("action_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Lifecycle action counts: "
            + ", ".join(f"{action} {action_counts.get(action, 0)}" for action in _ACTION_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No SCIM provisioning requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Lifecycle Action | Provider | Mapped Entity | Timing | Confidence | Source | Evidence | Planning Notes | Unresolved Questions |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.lifecycle_action} | "
                f"{_markdown_cell(requirement.identity_provider)} | "
                f"{_markdown_cell(requirement.mapped_entity)} | "
                f"{_markdown_cell(requirement.timing)} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell('; '.join(requirement.unresolved_questions))} |"
            )
        return "\n".join(lines)


def build_source_scim_provisioning_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceSCIMProvisioningRequirementsReport:
    """Build a SCIM provisioning requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    requirements = tuple(_merge_candidates(candidates))
    return SourceSCIMProvisioningRequirementsReport(
        source_id=source_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def generate_source_scim_provisioning_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceSCIMProvisioningRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_scim_provisioning_requirements(source)


def derive_source_scim_provisioning_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceSCIMProvisioningRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_scim_provisioning_requirements(source)


def extract_source_scim_provisioning_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceSCIMProvisioningRequirement, ...]:
    """Return SCIM provisioning requirement records extracted from brief-shaped input."""
    return build_source_scim_provisioning_requirements(source).requirements


def summarize_source_scim_provisioning_requirements(
    source_or_result: (
        str
        | Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceSCIMProvisioningRequirementsReport
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic SCIM provisioning requirements summary."""
    if isinstance(source_or_result, SourceSCIMProvisioningRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_scim_provisioning_requirements(source_or_result).summary


def source_scim_provisioning_requirements_to_dict(
    report: SourceSCIMProvisioningRequirementsReport,
) -> dict[str, Any]:
    """Serialize a SCIM provisioning requirements report to a plain dictionary."""
    return report.to_dict()


source_scim_provisioning_requirements_to_dict.__test__ = False


def source_scim_provisioning_requirements_to_dicts(
    requirements: tuple[SourceSCIMProvisioningRequirement, ...]
    | list[SourceSCIMProvisioningRequirement]
    | SourceSCIMProvisioningRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize source SCIM provisioning requirement records to dictionaries."""
    if isinstance(requirements, SourceSCIMProvisioningRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_scim_provisioning_requirements_to_dicts.__test__ = False


def source_scim_provisioning_requirements_to_markdown(
    report: SourceSCIMProvisioningRequirementsReport,
) -> str:
    """Render a SCIM provisioning requirements report as Markdown."""
    return report.to_markdown()


source_scim_provisioning_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    lifecycle_action: SCIMProvisioningAction
    identity_provider: str
    mapped_entity: str
    timing: str
    source_field: str
    evidence: str
    confidence: SCIMProvisioningConfidence


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
        if not _is_requirement(segment):
            continue
        searchable = _searchable_text(segment.source_field, segment.text)
        for action in _dedupe(action for action in _ACTION_ORDER if _ACTION_PATTERNS[action].search(searchable)):
            if action == "provisioning" and _ACTION_PATTERNS["jit_provisioning"].search(searchable):
                continue
            if action == "provisioning" and _ACTION_PATTERNS["directory_sync"].search(searchable) and not _EXPLICIT_PROVISIONING_RE.search(searchable):
                continue
            candidates.append(
                _Candidate(
                    lifecycle_action=action,
                    identity_provider=_extract_provider(segment.text),
                    mapped_entity=_extract_entity(action, segment.text),
                    timing=_extract_timing(segment.text),
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    confidence=_confidence(action, segment),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceSCIMProvisioningRequirement]:
    by_action: dict[SCIMProvisioningAction, list[_Candidate]] = {}
    for candidate in candidates:
        by_action.setdefault(candidate.lifecycle_action, []).append(candidate)

    requirements: list[SourceSCIMProvisioningRequirement] = []
    for action in _ACTION_ORDER:
        items = by_action.get(action, [])
        if not items:
            continue
        best = min(
            items,
            key=lambda item: (
                _CONFIDENCE_ORDER[item.confidence],
                _field_action_rank(action, item.source_field),
                item.source_field.casefold(),
                item.evidence.casefold(),
            ),
        )
        evidence = tuple(_dedupe_evidence(item.evidence for item in sorted(items, key=_candidate_sort_key)))[:5]
        requirements.append(
            SourceSCIMProvisioningRequirement(
                lifecycle_action=action,
                identity_provider=_merge_values(item.identity_provider for item in items),
                mapped_entity=_merge_values(item.mapped_entity for item in items),
                timing=_merge_values(item.timing for item in items),
                source_field=best.source_field,
                evidence=evidence,
                confidence=best.confidence,
                planning_note=_PLANNING_NOTES[action],
                unresolved_questions=tuple(_unresolved_questions(action, items)),
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


def _append_value(
    segments: list[_Segment],
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            if str(key) in _IGNORED_FIELDS:
                continue
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _IDENTITY_CONTEXT_RE.search(key_text)
            )
            child = value[key]
            if child_context and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    for segment_text, segment_context in _segments(f"{key_text}: {text}", child_context):
                        segments.append(_Segment(child_field, segment_text, segment_context))
                continue
            _append_value(segments, child_field, child, child_context)
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
            section_context = inherited_context or bool(
                _IDENTITY_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if _OUT_OF_SCOPE_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    if _OUT_OF_SCOPE_RE.search(searchable):
        return False
    if not (_IDENTITY_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    if not any(pattern.search(searchable) for pattern in _ACTION_PATTERNS.values()):
        return False
    if _DIRECTIVE_RE.search(segment.text):
        return True
    return bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))


def _has_global_no_scope(payload: Mapping[str, Any]) -> bool:
    for segment in _candidate_segments(payload):
        root_field = segment.source_field.split("[", 1)[0].split(".", 1)[0]
        if root_field not in {"title", "summary", "body", "description", "scope", "non_goals", "constraints", "source_payload"}:
            continue
        if _OUT_OF_SCOPE_RE.search(_searchable_text(segment.source_field, segment.text)):
            return True
    return False


def _confidence(
    action: SCIMProvisioningAction,
    segment: _Segment,
) -> SCIMProvisioningConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    score = 0
    if _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        score += 1
    if segment.section_context or _IDENTITY_CONTEXT_RE.search(searchable):
        score += 1
    if _DIRECTIVE_RE.search(segment.text):
        score += 1
    if _ACTION_PATTERNS[action].search(searchable):
        score += 1
    if _extract_provider(segment.text) or _extract_entity(action, segment.text):
        score += 1
    return "high" if score >= 4 else "medium" if score >= 2 else "low"


def _extract_provider(text: str) -> str:
    providers = _dedupe(_clean_text(match.group(0)) for match in _PROVIDER_RE.finditer(text))
    if len(providers) > 1:
        providers = [
            provider
            for provider in providers
            if provider.casefold() not in {"idp", "identity provider", "enterprise idp"}
        ]
    return _merge_values(providers)


def _extract_timing(text: str) -> str:
    return _merge_values(_clean_text(match.group(0)) for match in _TIMING_RE.finditer(text))


def _extract_entity(action: SCIMProvisioningAction, text: str) -> str:
    values = _dedupe(_clean_text(match.group(0)) for match in _ENTITY_RE.finditer(text))
    lower_values = {value.casefold() for value in values}
    values = [
        value
        for value in values
        if not (
            (value.casefold() == "role" and "roles" in lower_values)
            or (value.casefold() == "group" and "groups" in lower_values)
            or (value.casefold() == "user" and "users" in lower_values)
            or (action == "role_mapping" and value.casefold() in {"member", "members"} and "roles" in lower_values)
        )
    ]
    if values:
        return ", ".join(values[:3])
    defaults: dict[SCIMProvisioningAction, str] = {
        "provisioning": "users",
        "deprovisioning": "users",
        "group_sync": "groups",
        "group_push": "groups",
        "role_mapping": "roles",
        "jit_provisioning": "users",
        "directory_sync": "users, groups",
        "suspension": "suspended users",
        "external_id_mapping": "externalId",
        "idp_constraint": "identity provider",
    }
    return defaults[action]


def _unresolved_questions(
    action: SCIMProvisioningAction,
    items: Iterable[_Candidate],
) -> list[str]:
    item_list = list(items)
    questions: list[str] = []
    if not any(item.identity_provider for item in item_list):
        questions.append("Which identity provider or directory is the source of truth?")
    if action in {"provisioning", "deprovisioning", "directory_sync", "group_sync", "group_push"} and not any(
        item.timing for item in item_list
    ):
        questions.append("What sync timing or lifecycle SLA is required?")
    if action in {"role_mapping", "external_id_mapping"} and not any(item.mapped_entity for item in item_list):
        questions.append("Which IdP attribute, claim, or identifier should be mapped?")
    return questions[:3]


def _summary(requirements: tuple[SourceSCIMProvisioningRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "action_counts": {
            action: sum(1 for requirement in requirements if requirement.lifecycle_action == action)
            for action in _ACTION_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "lifecycle_actions": [requirement.lifecycle_action for requirement in requirements],
        "status": "ready_for_planning" if requirements else "no_scim_provisioning_requirements_found",
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = ("id", "source_brief_id", "source_id", *_SCANNED_FIELDS)
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _searchable_text(source_field: str, text: str) -> str:
    return f"{_field_words(source_field)} {text}".replace("_", " ").replace("-", " ")


def _field_action_rank(action: SCIMProvisioningAction, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[SCIMProvisioningAction, tuple[str, ...]] = {
        "provisioning": ("provision", "scim", "users"),
        "deprovisioning": ("deprovision", "offboard", "revoke"),
        "group_sync": ("group", "sync"),
        "group_push": ("group", "push"),
        "role_mapping": ("role", "mapping", "claim"),
        "jit_provisioning": ("jit", "first login"),
        "directory_sync": ("directory", "sync"),
        "suspension": ("suspend", "inactive", "disabled"),
        "external_id_mapping": ("external", "identifier", "nameid"),
        "idp_constraint": ("idp", "identity provider", "enterprise"),
    }
    return 0 if any(marker in field_words for marker in markers[action]) else 1


def _candidate_sort_key(item: _Candidate) -> tuple[int, int, str, str]:
    return (
        _CONFIDENCE_ORDER[item.confidence],
        _field_action_rank(item.lifecycle_action, item.source_field),
        item.source_field.casefold(),
        item.evidence.casefold(),
    )


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


def _merge_values(values: Iterable[str]) -> str:
    parts: list[str] = []
    for value in values:
        parts.extend(item.strip() for item in value.split(",") if item.strip())
    return ", ".join(_dedupe(parts)[:3])


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


__all__ = [
    "SCIMProvisioningAction",
    "SCIMProvisioningConfidence",
    "SourceSCIMProvisioningRequirement",
    "SourceSCIMProvisioningRequirementsReport",
    "build_source_scim_provisioning_requirements",
    "derive_source_scim_provisioning_requirements",
    "extract_source_scim_provisioning_requirements",
    "generate_source_scim_provisioning_requirements",
    "summarize_source_scim_provisioning_requirements",
    "source_scim_provisioning_requirements_to_dict",
    "source_scim_provisioning_requirements_to_dicts",
    "source_scim_provisioning_requirements_to_markdown",
]
