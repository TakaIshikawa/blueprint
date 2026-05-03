"""Extract source-level SSO enforcement requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SSOEnforcementCategory = Literal[
    "identity_providers",
    "enforcement_scope",
    "fallback_access",
    "domain_rules",
    "session_behavior",
    "audit_logging",
    "rollout_constraints",
]
SSOEnforcementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SSOEnforcementCategory, ...] = (
    "identity_providers",
    "enforcement_scope",
    "fallback_access",
    "domain_rules",
    "session_behavior",
    "audit_logging",
    "rollout_constraints",
)
_CONFIDENCE_ORDER: dict[SSOEnforcementConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SSO_CONTEXT_RE = re.compile(
    r"\b(?:sso|single sign[- ]?on|single sign on|federated login|federated auth|"
    r"saml|oidc|openid connect|idp|identity provider|okta|azure ad|microsoft entra|"
    r"entra id|google workspace|onelogin|ping identity|sso enforcement|enforced sso|"
    r"require sso|mandatory sso|password fallback|break[- ]?glass|emergency admin|"
    r"bypass|claimed domain|domain claim|verified domain|domain ownership|"
    r"session lifetime|reauth|single logout|audit log|security event|rollout|phased rollout)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:sso|single[-_ ]?sign[-_ ]?on|identity|idp|auth|authentication|security|"
    r"enterprise|compliance|requirements?|acceptance|criteria|constraints?|scope|"
    r"fallback|break[-_ ]?glass|bypass|domain|session|audit|rollout|metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"enforce|enforced|mandatory|force|block|deny|disable|allow|support|provide|enable|use|uses|"
    r"permit|exempt|bypass|fallback|break[- ]?glass|claim|verify|match|map|expire|"
    r"reauth(?:enticate|entication)?|logout|log|record|audit|track|roll out|rollout|"
    r"pilot|phase|feature flag|allowlist|before launch|acceptance|done when)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:sso enforcement|enforced sso|mandatory sso|require sso|single sign[- ]?on enforcement|"
    r"break[- ]?glass|password fallback|domain claim|session behavior|sso audit|sso rollout)\b|"
    r"\b(?:sso enforcement|enforced sso|mandatory sso|require sso|single sign[- ]?on enforcement|"
    r"break[- ]?glass|password fallback|domain claim|session behavior|sso audit|sso rollout)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|excluded)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:okta|azure ad|microsoft entra|entra id|google workspace|onelogin|ping identity|"
    r"saml|oidc|openid connect|all users?|admins?|owners?|employees?|contractors?|"
    r"enterprise tenants?|managed users?|invited users?|password fallback|break[- ]?glass|"
    r"emergency admin|verified domains?|claimed domains?|domain claim|email domains?|"
    r"\d+\s*(?:minutes?|hours?|days?|weeks?)|reauthentication|single logout|audit log|"
    r"security events?|phased rollout|pilot|feature flag|allowlist|grace period)\b|"
    r"@[a-z0-9.-]+\.[a-z]{2,}\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:minutes?|hours?|days?|weeks?)\b", re.I)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
    "updated_by",
    "created_by",
    "owner",
    "last_editor",
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
    "integration_points",
    "risks",
    "non_goals",
    "assumptions",
    "authentication",
    "auth_requirements",
    "security",
    "identity",
    "sso",
    "sso_enforcement",
    "identity_provider",
    "domain_rules",
    "session",
    "audit",
    "rollout",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[SSOEnforcementCategory, re.Pattern[str]] = {
    "identity_providers": re.compile(
        r"\b(?:identity providers?|idps?|okta|azure ad|microsoft entra|entra id|google workspace|"
        r"onelogin|ping identity|saml|oidc|openid connect|provider metadata|issuer url|"
        r"entity id|acs url|federated login)\b",
        re.I,
    ),
    "enforcement_scope": re.compile(
        r"\b(?:(?:sso enforcement|enforced sso|mandatory sso|require sso|requires sso|force sso)"
        r".{0,100}(?:all managed users?|all users?|admins?|owners?|employees?|contractors?|"
        r"enterprise tenants?|specific groups?|password login)|"
        r"password login disabled|disable password login|block password login|managed users?|"
        r"all managed users?|all users?|enterprise tenants?|specific groups?|"
        r"enforcement population|population|tenant scope)\b",
        re.I,
    ),
    "fallback_access": re.compile(
        r"\b(?:fallback access|password fallback|local password|break[- ]?glass|emergency admin|"
        r"backup admin|bypass|exempt(?:ion|ed)?|recovery access|support override|"
        r"admin override|temporary access)\b",
        re.I,
    ),
    "domain_rules": re.compile(
        r"\b(?:domain rules?|domain claim|claimed domains?|verified domains?|verify domains?|"
        r"domain ownership|email domains?|managed domains?|allowed domains?|domain match|"
        r"domain conflict|multiple tenants?|jit domain|@[a-z0-9.-]+\.[a-z]{2,})\b",
        re.I,
    ),
    "session_behavior": re.compile(
        r"\b(?:session behavior|session lifetime|session duration|session timeout|idle timeout|"
        r"max session|reauth(?:enticate|entication)?|step[- ]?up|single logout|slo|"
        r"global sign[- ]?out|idp[- ]initiated logout|logout)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit logging|audit log|audited|logging|logged|security events?|"
        r"enforcement events?|login events?|bypass events?|break[- ]?glass events?|"
        r"actor and timestamp|compliance evidence)\b",
        re.I,
    ),
    "rollout_constraints": re.compile(
        r"\b(?:rollout|roll out|phased rollout|pilot|beta|gradual|migration|grace period|"
        r"feature flag|allowlist|ring|cohort|communications?|deadline|before launch|"
        r"tenant by tenant|staged enforcement)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[SSOEnforcementCategory, tuple[str, ...]] = {
    "identity_providers": ("identity", "enterprise_integrations"),
    "enforcement_scope": ("identity", "product"),
    "fallback_access": ("security", "support"),
    "domain_rules": ("identity", "tenant_management"),
    "session_behavior": ("identity", "security"),
    "audit_logging": ("security", "compliance"),
    "rollout_constraints": ("product", "customer_success"),
}
_PLAN_IMPACTS: dict[SSOEnforcementCategory, tuple[str, ...]] = {
    "identity_providers": ("Identify supported IdPs, protocols, metadata, issuer, certificate, and setup requirements.",),
    "enforcement_scope": ("Define which tenants, domains, roles, users, and groups must use SSO and where password login is blocked.",),
    "fallback_access": ("Specify break-glass, bypass, password fallback, support override, and recovery approval rules.",),
    "domain_rules": ("Define domain claim, verification, matching, conflict, and managed-domain enforcement behavior.",),
    "session_behavior": ("Set SSO session lifetime, reauthentication, logout, and IdP session handling expectations.",),
    "audit_logging": ("Record enforcement, login, bypass, configuration, actor, timestamp, and compliance evidence events.",),
    "rollout_constraints": ("Plan pilot, staged rollout, feature flags, grace periods, deadlines, and customer communications.",),
}


@dataclass(frozen=True, slots=True)
class SourceSSOEnforcementRequirement:
    """One source-backed SSO enforcement requirement."""

    category: SSOEnforcementCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SSOEnforcementConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> SSOEnforcementCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> SSOEnforcementCategory:
        """Compatibility view for extractors that expose concern naming."""
        return self.category

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourceSSOEnforcementRequirementsReport:
    """Source-level SSO enforcement requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceSSOEnforcementRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSSOEnforcementRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceSSOEnforcementRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return SSO enforcement requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source SSO Enforcement Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
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
            lines.extend(["", "No source SSO enforcement requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Confidence | Source Field | Owners | Evidence | Suggested Plan Impacts |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell(', '.join(requirement.suggested_owners))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.suggested_plan_impacts))} |"
            )
        return "\n".join(lines)


def build_source_sso_enforcement_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceSSOEnforcementRequirementsReport:
    """Build an SSO enforcement requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    requirements = () if _has_global_no_scope(payload) else tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceSSOEnforcementRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_sso_enforcement_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceSSOEnforcementRequirementsReport
        | str
        | object
    ),
) -> SourceSSOEnforcementRequirementsReport | dict[str, Any]:
    """Compatibility helper for callers that use summarize_* naming."""
    if isinstance(source, SourceSSOEnforcementRequirementsReport):
        return dict(source.summary)
    return build_source_sso_enforcement_requirements(source)


def derive_source_sso_enforcement_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceSSOEnforcementRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_sso_enforcement_requirements(source)


def generate_source_sso_enforcement_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceSSOEnforcementRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_sso_enforcement_requirements(source)


def extract_source_sso_enforcement_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceSSOEnforcementRequirement, ...]:
    """Return SSO enforcement requirement records from brief-shaped input."""
    return build_source_sso_enforcement_requirements(source).requirements


def source_sso_enforcement_requirements_to_dict(report: SourceSSOEnforcementRequirementsReport) -> dict[str, Any]:
    """Serialize an SSO enforcement requirements report to a plain dictionary."""
    return report.to_dict()


source_sso_enforcement_requirements_to_dict.__test__ = False


def source_sso_enforcement_requirements_to_dicts(
    requirements: (
        tuple[SourceSSOEnforcementRequirement, ...]
        | list[SourceSSOEnforcementRequirement]
        | SourceSSOEnforcementRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize SSO enforcement requirement records to dictionaries."""
    if isinstance(requirements, SourceSSOEnforcementRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_sso_enforcement_requirements_to_dicts.__test__ = False


def source_sso_enforcement_requirements_to_markdown(report: SourceSSOEnforcementRequirementsReport) -> str:
    """Render an SSO enforcement requirements report as Markdown."""
    return report.to_markdown()


source_sso_enforcement_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: SSOEnforcementCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: SSOEnforcementConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
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
    if not isinstance(source, (bytes, bytearray)):
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
        if not _is_requirement(segment):
            continue
        searchable = f"{_category_field_words(segment.source_field)} {segment.text}"
        categories = [
            category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        for category in _dedupe(categories):
            candidates.append(
                _Candidate(
                    category=category,
                    value=_value(category, segment.text),
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    confidence=_confidence(segment),
                )
            )
    return candidates


def _has_global_no_scope(payload: Mapping[str, Any]) -> bool:
    for segment in _candidate_segments(payload):
        if segment.source_field.split("[", 1)[0].split(".", 1)[0] not in {
            "title",
            "summary",
            "body",
            "description",
            "scope",
            "non_goals",
            "constraints",
            "source_payload",
        }:
            continue
        if _NEGATED_SCOPE_RE.search(f"{_field_words(segment.source_field)} {segment.text}"):
            return True
    return False


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceSSOEnforcementRequirement]:
    grouped: dict[SSOEnforcementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceSSOEnforcementRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(_CONFIDENCE_ORDER[item.confidence] for item in items if item.source_field == field),
                _field_category_rank(category, field),
                field.casefold(),
            ),
        )[0]
        requirements.append(
            SourceSSOEnforcementRequirement(
                category=category,
                source_field=source_field,
                evidence=tuple(
                    sorted(
                        _dedupe_evidence(
                            item.evidence
                            for item in sorted(
                                items,
                                key=lambda item: (
                                    _field_category_rank(category, item.source_field),
                                    item.source_field.casefold(),
                                ),
                            )
                        ),
                        key=str.casefold,
                    )
                )[:5],
                confidence=confidence,
                value=_best_value(items),
                suggested_owners=_OWNER_SUGGESTIONS[category],
                suggested_plan_impacts=_PLAN_IMPACTS[category],
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
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            if str(key) in _IGNORED_FIELDS:
                continue
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text) or _SSO_CONTEXT_RE.search(key_text))
            _append_value(segments, child_field, value[key], child_context)
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
            section_context = inherited_context or bool(_SSO_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_words = _field_words(segment.source_field)
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    if not (_SSO_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    if not any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values()):
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(_SSO_CONTEXT_RE.search(segment.text))


def _value(category: SSOEnforcementCategory, text: str) -> str | None:
    if category == "domain_rules":
        if match := re.search(r"@[a-z0-9.-]+\.[a-z]{2,}\b", text, re.I):
            return _clean_text(match.group(0)).casefold()
    if category == "session_behavior":
        if match := _DURATION_RE.search(text):
            return _clean_text(match.group(0)).casefold()
    patterns: dict[SSOEnforcementCategory, tuple[str, ...]] = {
        "identity_providers": (
            r"\b(?:okta|azure ad|microsoft entra|entra id|google workspace|onelogin|ping identity|saml|oidc|openid connect)\b",
        ),
        "enforcement_scope": (
            r"\b(?:all managed users?|all users?|admins?|owners?|employees?|contractors?|enterprise tenants?|managed users?|specific groups?|password login disabled)\b",
        ),
        "fallback_access": (
            r"\b(?:password fallback|break[- ]?glass|emergency admin|backup admin|bypass|support override|temporary access|local password)\b",
        ),
        "domain_rules": (
            r"\b(?:verified domains?|claimed domains?|domain claim|domain ownership|email domains?|managed domains?|allowed domains?|@[a-z0-9.-]+\.[a-z]{2,})\b",
        ),
        "session_behavior": (
            r"\b(?:\d+\s*(?:minutes?|hours?|days?|weeks?)|session lifetime|session timeout|idle timeout|reauthentication|single logout|slo|global sign[- ]?out)\b",
        ),
        "audit_logging": (
            r"\b(?:audit log|security events?|enforcement events?|login events?|bypass events?|actor and timestamp|compliance evidence)\b",
        ),
        "rollout_constraints": (
            r"\b(?:phased rollout|rollout|pilot|beta|feature flag|allowlist|grace period|ring|cohort|deadline)\b",
        ),
    }
    for pattern in patterns[category]:
        if match := re.search(pattern, text, re.I):
            return _clean_text(match.group(0)).casefold()
    if match := _VALUE_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (
            0 if _DURATION_RE.search(value) else 1,
            0 if _VALUE_RE.search(value) else 1,
            len(value),
            value.casefold(),
        ),
    )
    return values[0] if values else None


def _confidence(segment: _Segment) -> SSOEnforcementConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _REQUIREMENT_RE.search(segment.text) and (
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "security",
                "identity",
                "authentication",
                "sso",
                "domain",
                "session",
                "audit",
                "rollout",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _SSO_CONTEXT_RE.search(searchable):
        return "medium"
    if _SSO_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(requirements: tuple[SourceSSOEnforcementRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "categories": [requirement.category for requirement in requirements],
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "status": "ready_for_planning" if requirements else "no_sso_enforcement_language",
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
        "context",
        "workflow_context",
        "requirements",
        "constraints",
        "scope",
        "non_goals",
        "assumptions",
        "acceptance",
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "authentication",
        "auth_requirements",
        "security",
        "identity",
        "sso",
        "sso_enforcement",
        "identity_provider",
        "domain_rules",
        "session",
        "audit",
        "rollout",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _category_field_words(source_field: str) -> str:
    leaf = source_field.rsplit(".", 1)[-1].split("[", 1)[0]
    return leaf.replace("_", " ").replace("-", " ").replace("/", " ")


def _field_category_rank(category: SSOEnforcementCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[SSOEnforcementCategory, tuple[str, ...]] = {
        "identity_providers": ("provider", "idp", "saml", "oidc", "okta", "entra"),
        "enforcement_scope": ("scope", "population", "enforcement", "tenant", "users"),
        "fallback_access": ("fallback", "break", "glass", "bypass", "override"),
        "domain_rules": ("domain", "claim", "verified"),
        "session_behavior": ("session", "reauth", "logout"),
        "audit_logging": ("audit", "log", "event", "evidence"),
        "rollout_constraints": ("rollout", "pilot", "flag", "grace", "deadline"),
    }
    return 0 if any(marker in field_words for marker in markers[category]) else 1


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
    "SSOEnforcementCategory",
    "SSOEnforcementConfidence",
    "SourceSSOEnforcementRequirement",
    "SourceSSOEnforcementRequirementsReport",
    "build_source_sso_enforcement_requirements",
    "derive_source_sso_enforcement_requirements",
    "extract_source_sso_enforcement_requirements",
    "generate_source_sso_enforcement_requirements",
    "summarize_source_sso_enforcement_requirements",
    "source_sso_enforcement_requirements_to_dict",
    "source_sso_enforcement_requirements_to_dicts",
    "source_sso_enforcement_requirements_to_markdown",
]
