"""Extract source-level delegated access and role administration requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


RoleDelegationRequirementType = Literal[
    "role_catalog",
    "delegation_scope",
    "approver_workflow",
    "time_bound_access",
    "break_glass_access",
    "privilege_review",
    "revocation_flow",
    "audit_logging",
]
RoleDelegationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_REQUIREMENT_ORDER: tuple[RoleDelegationRequirementType, ...] = (
    "role_catalog",
    "delegation_scope",
    "approver_workflow",
    "time_bound_access",
    "break_glass_access",
    "privilege_review",
    "revocation_flow",
    "audit_logging",
)
_CONFIDENCE_ORDER: dict[RoleDelegationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_ROLE_CONTEXT_RE = re.compile(
    r"\b(?:delegated admin|admin delegation|delegated access|role delegation|privileged access|"
    r"privilege|privileges|permission|permissions|rbac|roles?|role catalog|role management|"
    r"access grant|grant access|temporary access|break[- ]?glass|just[- ]?in[- ]?time|jit|"
    r"access request|access approval|approver|approval workflow|privilege review|access review|"
    r"revoke|revocation|deprovision|delegation scope|administrator roles?)\b",
    re.I,
)
_ASSIGNEE_ONLY_RE = re.compile(
    r"\b(?:owner|owners|assignee|assignees|assigned to|task owner|ticket owner|story owner|"
    r"project owner|feature owner|dris?|responsible person)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|support|"
    r"allow|assign|grant|delegate|approve|expire|review|revoke|remove|audit|log|track|"
    r"record|restrict|limit|scope|enforce|cannot ship|before launch|done when)\b",
    re.I,
)
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,140}"
    r"\b(?:delegated admin|admin delegation|role management|roles?|permissions?|privileged access|"
    r"access approval|access review|revocation|break[- ]?glass)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|"
    r"impact|for this release)\b|"
    r"\b(?:delegated admin|admin delegation|role management|roles?|permissions?|privileged access|"
    r"access approval|access review|revocation|break[- ]?glass)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|"
    r"no work|no impact|non-goal|non goal)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:role|roles|rbac|permission|permissions|privilege|privileged|admin|administrator|"
    r"delegat|access|approval|approver|expiry|expiration|temporary|jit|break[_ -]?glass|"
    r"review|recertification|revocation|revoke|audit|logging|requirements?|acceptance|"
    r"criteria|definition[_ -]?of[_ -]?done|security|identity|source[_ -]?payload|metadata)",
    re.I,
)
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
    "success_criteria",
    "acceptance",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "security",
    "identity",
    "roles",
    "permissions",
    "rbac",
    "access",
    "delegation",
    "privileged_access",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_IGNORED_FIELDS = {
    "id",
    "source_id",
    "source_brief_id",
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
}
_TYPE_PATTERNS: dict[RoleDelegationRequirementType, re.Pattern[str]] = {
    "role_catalog": re.compile(
        r"\b(?:role catalog|role catalogue|role definitions?|role list|predefined roles?|"
        r"built[- ]?in roles?|custom roles?|rbac matrix|permission matrix|admin role types?|"
        r"role management|viewer|editor|billing admin|security admin|workspace admin|owner role)\b",
        re.I,
    ),
    "delegation_scope": re.compile(
        r"\b(?:delegate|delegation|delegated admin|delegated access|grant access|assign roles?|"
        r"role assignment|scope|scoped to|limited to|restrict(?:ed)? to|tenant|workspace|project|"
        r"organization|org|environment|resource scope|least privilege)\b",
        re.I,
    ),
    "approver_workflow": re.compile(
        r"\b(?:approv(?:e|al|er|ers|ing)|access request|request workflow|manager approval|"
        r"security approval|two[- ]person|four[- ]eyes|dual control|review before grant)\b",
        re.I,
    ),
    "time_bound_access": re.compile(
        r"\b(?:temporary access|time[- ]?bound|expires?|expiry|expiration|ttl|time to live|"
        r"just[- ]?in[- ]?time|jit|limited duration|auto[- ]?expire|access window)\b",
        re.I,
    ),
    "break_glass_access": re.compile(
        r"\b(?:break[- ]?glass|emergency access|emergency admin|incident access|override access|"
        r"last resort access|emergency role)\b",
        re.I,
    ),
    "privilege_review": re.compile(
        r"\b(?:privilege review|access review|role review|periodic review|quarterly review|"
        r"recertification|certification campaign|attestation|least privilege review|stale access)\b",
        re.I,
    ),
    "revocation_flow": re.compile(
        r"\b(?:revoke|revocation|remove access|access removal|deprovision|deprovisioning|"
        r"offboard(?:ing)?|terminate access|downgrade role|role removal|auto[- ]?remove)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit|audited|audit log|audit trail|logged|logging|record|track|history)"
        r".{0,120}\b(?:role|permission|privilege|delegat|grant|approval|revocation|break[- ]?glass|access)\b|"
        r"\b(?:role|permission|privilege|delegat|grant|approval|revocation|break[- ]?glass|access)"
        r".{0,120}\b(?:audit|audited|audit log|audit trail|logged|logging|record|track|history)\b",
        re.I,
    ),
}
_DURATION_RE = re.compile(
    r"\b(?P<value>(?:within|after|for|up to|no more than|less than|every)?\s*"
    r"\d+(?:\.\d+)?\s*(?:minutes?|mins?|hours?|days?|weeks?|months?|quarters?|years?))\b|"
    r"\b(?P<named>same day|next business day|quarterly|annually|yearly|monthly|weekly)\b",
    re.I,
)
_SCOPE_RE = re.compile(
    r"\b(?:scoped to|limited to|restricted to|only for|per|within|for)\s+"
    r"(?P<scope>(?:tenant|workspace|project|organization|org|environment|resource|team|"
    r"department|customer account|account|group)s?)\b",
    re.I,
)
_ROLE_NAME_RE = re.compile(
    r"\b(?:security admin|billing admin|workspace admin|organization admin|org admin|"
    r"project admin|viewer|editor|owner|approver|auditor|support admin|read[- ]?only admin)\b",
    re.I,
)
_UNRESOLVED_QUESTIONS: dict[RoleDelegationRequirementType, tuple[str, ...]] = {
    "role_catalog": (
        "Confirm the canonical role catalog, permission boundaries, and custom-role support.",
    ),
    "delegation_scope": (
        "Confirm which resources, tenants, and admin groups can grant delegated roles.",
    ),
    "approver_workflow": (
        "Confirm approval steps, approver eligibility, escalation paths, and rejection behavior.",
    ),
    "time_bound_access": (
        "Confirm access duration defaults, extension rules, and expiry enforcement timing.",
    ),
    "break_glass_access": (
        "Confirm emergency access eligibility, activation checks, monitoring, and post-use review.",
    ),
    "privilege_review": (
        "Confirm review cadence, reviewer ownership, remediation deadlines, and evidence needs.",
    ),
    "revocation_flow": (
        "Confirm revocation triggers, propagation timing, notifications, and session invalidation.",
    ),
    "audit_logging": (
        "Confirm audit event schema, retention expectations, exports, and privileged-access visibility.",
    ),
}
_PLAN_IMPACTS: dict[RoleDelegationRequirementType, tuple[str, ...]] = {
    "role_catalog": (
        "Model role catalog records, permission mappings, admin UI copy, and migration defaults.",
    ),
    "delegation_scope": (
        "Add scoped role assignment checks and enforce least-privilege boundaries in authorization.",
    ),
    "approver_workflow": (
        "Plan access request states, approver queues, notifications, and grant finalization.",
    ),
    "time_bound_access": (
        "Add expiry timestamps, scheduled cleanup, extension flows, and expired-access UX.",
    ),
    "break_glass_access": (
        "Design emergency activation, strong verification, alerting, and post-incident review tasks.",
    ),
    "privilege_review": (
        "Generate review campaigns, reviewer assignments, evidence capture, and remediation tracking.",
    ),
    "revocation_flow": (
        "Implement role removal, downstream deprovisioning, session refresh, and notification handling.",
    ),
    "audit_logging": (
        "Emit audit events for role grants, approvals, expiries, reviews, revocations, and emergency access.",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceRoleDelegationRequirement:
    """One source-backed delegated access or role administration requirement."""

    source_brief_id: str | None
    source_field: str
    requirement_type: RoleDelegationRequirementType
    role_scope: str | None = None
    value: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: RoleDelegationConfidence = "medium"
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "source_field": self.source_field,
            "requirement_type": self.requirement_type,
            "role_scope": self.role_scope,
            "value": self.value,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "unresolved_questions": list(self.unresolved_questions),
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourceRoleDelegationRequirementsReport:
    """Source-level role delegation requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceRoleDelegationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceRoleDelegationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceRoleDelegationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return role delegation requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Role Delegation Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("requirement_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement types: "
            + ", ".join(f"{kind} {type_counts.get(kind, 0)}" for kind in _REQUIREMENT_ORDER),
            "- Role scopes: " + (", ".join(self.summary.get("role_scopes", [])) or "none"),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source role delegation requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Source Field | Type | Scope | Value | Confidence | Unresolved Questions | Suggested Plan Impacts | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell(requirement.role_scope or '')} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.unresolved_questions))} | "
                f"{_markdown_cell('; '.join(requirement.suggested_plan_impacts))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_role_delegation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceRoleDelegationRequirementsReport:
    """Extract source-level role delegation requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceRoleDelegationRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_role_delegation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceRoleDelegationRequirementsReport:
    """Compatibility alias for building a role delegation requirements report."""
    return build_source_role_delegation_requirements(source)


def generate_source_role_delegation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceRoleDelegationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_role_delegation_requirements(source)


def derive_source_role_delegation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceRoleDelegationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_role_delegation_requirements(source)


def summarize_source_role_delegation_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceRoleDelegationRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted role delegation requirements."""
    if isinstance(source_or_result, SourceRoleDelegationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_role_delegation_requirements(source_or_result).summary


def source_role_delegation_requirements_to_dict(
    report: SourceRoleDelegationRequirementsReport,
) -> dict[str, Any]:
    """Serialize a role delegation requirements report to a plain dictionary."""
    return report.to_dict()


source_role_delegation_requirements_to_dict.__test__ = False


def source_role_delegation_requirements_to_dicts(
    requirements: (
        tuple[SourceRoleDelegationRequirement, ...]
        | list[SourceRoleDelegationRequirement]
        | SourceRoleDelegationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize role delegation requirement records to dictionaries."""
    if isinstance(requirements, SourceRoleDelegationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_role_delegation_requirements_to_dicts.__test__ = False


def source_role_delegation_requirements_to_markdown(
    report: SourceRoleDelegationRequirementsReport,
) -> str:
    """Render a role delegation requirements report as Markdown."""
    return report.to_markdown()


source_role_delegation_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    source_field: str
    requirement_type: RoleDelegationRequirementType
    role_scope: str | None
    value: str | None
    evidence: str
    confidence: RoleDelegationConfidence


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
        if any(
            _NO_IMPACT_RE.search(f"{_field_words(segment.source_field)} {segment.text}")
            for segment in segments
        ):
            continue
        for segment in segments:
            if not _is_requirement(segment):
                continue
            searchable = f"{_leaf_field_words(segment.source_field)} {segment.text}"
            for requirement_type in _requirement_types(searchable):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        source_field=segment.source_field,
                        requirement_type=requirement_type,
                        role_scope=_role_scope(searchable, segment.source_field),
                        value=_value(requirement_type, searchable),
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(segment, requirement_type),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceRoleDelegationRequirement]:
    grouped: dict[tuple[str | None, RoleDelegationRequirementType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.requirement_type), []).append(
            candidate
        )

    requirements: list[SourceRoleDelegationRequirement] = []
    for (source_brief_id, requirement_type), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        requirements.append(
            SourceRoleDelegationRequirement(
                source_brief_id=source_brief_id,
                source_field=_best_source_field(items),
                requirement_type=requirement_type,
                role_scope=_joined_details(item.role_scope for item in items),
                value=(
                    _joined_details(item.value for item in items)
                    if requirement_type == "role_catalog"
                    else _best_value(item.value for item in items)
                ),
                evidence=tuple(
                    sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
                )[:6],
                confidence=confidence,
                unresolved_questions=_UNRESOLVED_QUESTIONS[requirement_type],
                suggested_plan_impacts=_PLAN_IMPACTS[requirement_type],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _REQUIREMENT_ORDER.index(requirement.requirement_type),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.role_scope or "",
            requirement.value or "",
            requirement.source_field.casefold(),
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


def _append_value(
    segments: list[_Segment],
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        if _has_structured_shape(value) and (
            _is_list_item_field(source_field) or _has_role_key(value)
        ):
            evidence = _structured_text(value)
            if evidence:
                segments.append(_Segment(source_field, evidence, True))
            return
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _ROLE_CONTEXT_RE.search(key_text)
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
                _STRUCTURED_FIELD_RE.search(title) or _ROLE_CONTEXT_RE.search(title)
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
            clauses = (
                [part]
                if _NO_IMPACT_RE.search(part) and _ROLE_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NO_IMPACT_RE.search(searchable):
        return False
    if _ASSIGNEE_ONLY_RE.search(searchable) and not _ROLE_CONTEXT_RE.search(searchable):
        return False
    structured = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    if not (_ROLE_CONTEXT_RE.search(searchable) or structured):
        return False
    if _requirement_types(searchable):
        return True
    return bool(_REQUIREMENT_RE.search(segment.text) and (segment.section_context or structured))


def _requirement_types(text: str) -> tuple[RoleDelegationRequirementType, ...]:
    values = [kind for kind in _REQUIREMENT_ORDER if _TYPE_PATTERNS[kind].search(text)]
    if (
        "delegation_scope" in values
        and _ASSIGNEE_ONLY_RE.search(text)
        and not _ROLE_CONTEXT_RE.search(text)
    ):
        values.remove("delegation_scope")
    return tuple(_dedupe(values))


def _role_scope(text: str, source_field: str) -> str | None:
    if match := _SCOPE_RE.search(text):
        return _detail(match.group("scope"))
    role_names = _dedupe(_detail(match.group(0)) for match in _ROLE_NAME_RE.finditer(text))
    if role_names:
        return ", ".join(role_name for role_name in role_names if role_name)
    field_parts = [
        part
        for part in re.split(r"[.\[\]_\-\s]+", source_field)
        if part
        and not part.isdigit()
        and part not in {"source", "payload", "metadata", "requirements"}
    ]
    if field_parts and _STRUCTURED_FIELD_RE.search(source_field):
        return _clean_text(" ".join(field_parts[-2:]))
    return None


def _value(requirement_type: RoleDelegationRequirementType, text: str) -> str | None:
    if requirement_type in {"time_bound_access", "privilege_review", "revocation_flow"}:
        if match := _DURATION_RE.search(text):
            return _detail(match.group("value") or match.group("named"))
    if requirement_type == "role_catalog":
        role_names = _dedupe(_detail(match.group(0)) for match in _ROLE_NAME_RE.finditer(text))
        return ", ".join(role_name for role_name in role_names if role_name) or None
    if requirement_type == "delegation_scope":
        return _role_scope(text, "")
    return None


def _confidence(
    segment: _Segment, requirement_type: RoleDelegationRequirementType
) -> RoleDelegationConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    structured = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    has_detail = bool(
        _DURATION_RE.search(searchable)
        or _SCOPE_RE.search(searchable)
        or _ROLE_NAME_RE.search(searchable)
        or requirement_type in {"break_glass_access", "audit_logging", "approver_workflow"}
    )
    if (
        _REQUIREMENT_RE.search(segment.text) or structured or segment.section_context
    ) and has_detail:
        return "high"
    if _REQUIREMENT_RE.search(segment.text) or _ROLE_CONTEXT_RE.search(searchable) or structured:
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceRoleDelegationRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_type_counts": {
            kind: sum(1 for requirement in requirements if requirement.requirement_type == kind)
            for kind in _REQUIREMENT_ORDER
        },
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "role_scopes": _dedupe(
            requirement.role_scope for requirement in requirements if requirement.role_scope
        ),
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "requires_approval": any(
            requirement.requirement_type == "approver_workflow" for requirement in requirements
        ),
        "requires_expiry": any(
            requirement.requirement_type == "time_bound_access" for requirement in requirements
        ),
        "requires_review": any(
            requirement.requirement_type == "privilege_review" for requirement in requirements
        ),
        "requires_revocation": any(
            requirement.requirement_type == "revocation_flow" for requirement in requirements
        ),
        "requires_audit_logging": any(
            requirement.requirement_type == "audit_logging" for requirement in requirements
        ),
        "status": (
            "ready_for_role_delegation_planning" if requirements else "no_role_delegation_language"
        ),
    }


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    return bool(
        keys
        & {
            "role",
            "roles",
            "role_catalog",
            "permission_matrix",
            "delegation_scope",
            "scope",
            "approver",
            "approvers",
            "approval",
            "expiry",
            "expires",
            "duration",
            "break_glass",
            "review",
            "recertification",
            "revocation",
            "revoke",
            "audit",
            "audit_logging",
        }
    )


def _has_role_key(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    return bool(keys & {"role", "roles"})


def _is_list_item_field(source_field: str) -> bool:
    return bool(re.search(r"\[\d+\]$", source_field))


def _structured_text(item: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = (
            ", ".join(_strings(value))
            if isinstance(value, (list, tuple, set))
            else _clean_text(value)
        )
        if text:
            parts.append(f"{key}: {text}")
    return "; ".join(parts)


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
        "security",
        "identity",
        "roles",
        "permissions",
        "rbac",
        "access",
        "delegation",
        "privileged_access",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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


def _best_source_field(items: Iterable[_Candidate]) -> str:
    candidates = list(items)
    return sorted(
        {item.source_field for item in candidates if item.source_field},
        key=lambda field: (
            min(
                _CONFIDENCE_ORDER[item.confidence]
                for item in candidates
                if item.source_field == field
            ),
            -sum(1 for item in candidates if item.source_field == field),
            field.casefold(),
        ),
    )[0]


def _joined_details(values: Iterable[str | None]) -> str | None:
    details = _dedupe(value for value in values if value)
    return ", ".join(details) if details else None


def _best_value(values: Iterable[str | None]) -> str | None:
    candidates = sorted(
        _dedupe(value for value in values if value),
        key=lambda value: (0 if re.search(r"\d", value) else 1, len(value), value.casefold()),
    )
    return candidates[0] if candidates else None


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _leaf_field_words(source_field: str) -> str:
    leaf = source_field.rsplit(".", 1)[-1]
    leaf = re.sub(r"\[\d+\]$", "", leaf)
    return _field_words(leaf)


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


def _detail(value: Any) -> str | None:
    text = _clean_text(value).strip("`'\" ;,.")
    return text[:140].rstrip() if text else None


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
    "RoleDelegationConfidence",
    "RoleDelegationRequirementType",
    "SourceRoleDelegationRequirement",
    "SourceRoleDelegationRequirementsReport",
    "build_source_role_delegation_requirements",
    "derive_source_role_delegation_requirements",
    "extract_source_role_delegation_requirements",
    "generate_source_role_delegation_requirements",
    "source_role_delegation_requirements_to_dict",
    "source_role_delegation_requirements_to_dicts",
    "source_role_delegation_requirements_to_markdown",
    "summarize_source_role_delegation_requirements",
]
