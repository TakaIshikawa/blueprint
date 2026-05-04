"""Extract source-level API IP allowlist requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


IPAllowlistCategory = Literal[
    "allowlist_configuration",
    "cidr_range_support",
    "per_tenant_customization",
    "ip_based_authentication",
    "endpoint_bypass",
    "dynamic_updates",
    "geo_blocking",
    "violation_logging",
]
IPAllowlistMissingDetail = Literal["missing_cidr_validation", "missing_tenant_isolation"]
IPAllowlistConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[IPAllowlistCategory, ...] = (
    "allowlist_configuration",
    "cidr_range_support",
    "per_tenant_customization",
    "ip_based_authentication",
    "endpoint_bypass",
    "dynamic_updates",
    "geo_blocking",
    "violation_logging",
)
_MISSING_DETAIL_ORDER: tuple[IPAllowlistMissingDetail, ...] = (
    "missing_cidr_validation",
    "missing_tenant_isolation",
)
_CONFIDENCE_ORDER: dict[IPAllowlistConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_IP_ALLOWLIST_CONTEXT_RE = re.compile(
    r"\b(?:ip allowlist|ip whitelist|allowlist|whitelist|"
    r"cidr|cidr range|network access|ip restriction|"
    r"ip filter|ip address|ip-based|"
    r"geo-?block|geo-?fenc|geo-?restriction|"
    r"tenant isolation|network security)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:ip|allowlist|whitelist|cidr|network|access|"
    r"security|geo|tenant|isolation|"
    r"header|headers?|api|rest|requirements?)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|include|return|expose|follow|implement|"
    r"ip|allowlist|whitelist|cidr|network|geo|"
    r"acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:ip allowlist|ip whitelist|allowlist|whitelist|"
    r"cidr|network restriction|ip restriction|geo-?block)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:ip allowlist|ip whitelist|allowlist|whitelist|"
    r"cidr|network restriction|ip restriction|geo-?block)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_IP_ALLOWLIST_RE = re.compile(
    r"\b(?:no ip allowlist|no ip whitelist|no allowlist|"
    r"no cidr|no network restriction|no ip restriction|"
    r"allowlist is out of scope|no geo-?block)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:shopping allowlist|wishlist|allow listing|listing allowed)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:cidr|cidr range|ipv4|ipv6|"
    r"10\.\d+\.\d+\.\d+|192\.168\.\d+\.\d+|172\.1[6-9]\.\d+\.\d+|"
    r"/\d{1,2}|subnet mask|network mask)\b",
    re.I,
)
_CIDR_VALIDATION_RE = re.compile(
    r"\b(?:cidr|cidr range|cidr validation|validate cidr|"
    r"subnet|subnet mask|network mask|ip range)\b",
    re.I,
)
_TENANT_ISOLATION_RE = re.compile(
    r"\b(?:tenant|multi-?tenant|per-?tenant|tenant-?specific|"
    r"tenant isolation|tenant separation|customer isolation)\b",
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
    "id",
    "source_id",
    "source_brief_id",
    "status",
    "created_by",
    "updated_by",
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
    "api",
    "rest",
    "security",
    "authentication",
    "ip",
    "allowlist",
    "whitelist",
    "network",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[IPAllowlistCategory, re.Pattern[str]] = {
    "allowlist_configuration": re.compile(
        r"\b(?:(?:ip |network )?(?:allowlist|whitelist)(?: config(?:uration)?| setting)?|"
        r"configure (?:ip )?(?:allowlist|whitelist)|"
        r"(?:allowed|permitted|trusted) ip)\b",
        re.I,
    ),
    "cidr_range_support": re.compile(
        r"\b(?:cidr(?: range| block| notation| support| validation)?|"
        r"(?:ip|address) range|subnet(?: mask)?|network mask|"
        r"ipv[46]|/\d{1,2})\b",
        re.I,
    ),
    "per_tenant_customization": re.compile(
        r"\b(?:(?:per|tenant)[- ]?tenant|tenant[- ]?(?:specific|level|customiz)|"
        r"(?:custom(?:er)?|tenant) allowlist|multi[- ]?tenant|"
        r"tenant isolation|(?:customer|organization)[- ]?specific)\b",
        re.I,
    ),
    "ip_based_authentication": re.compile(
        r"\b(?:ip[- ]?based (?:auth(?:entication)?|verification)|"
        r"(?:authenticate|verify|validate|check) (?:by |source )?ip|"
        r"ip (?:verification|validation|check|authentication))\b",
        re.I,
    ),
    "endpoint_bypass": re.compile(
        r"\b(?:(?:endpoint|allowlist)[- ]?(?:level )?(?:bypass|exemption)|"
        r"(?:bypass|skip|ignore|exempt) (?:(?:ip )?(?:allowlist|check)|endpoint)|"
        r"(?:allowlist|ip) bypass|(?:exempt|public|unauthenticated) endpoint|"
        r"no ip check)\b",
        re.I,
    ),
    "dynamic_updates": re.compile(
        r"\b(?:dynamic(?: update| allowlist)?|"
        r"(?:update|modify|change) allowlist|runtime (?:update|allowlist)|"
        r"(?:add|remove) ip|live update|on[- ]?the[- ]?fly)\b",
        re.I,
    ),
    "geo_blocking": re.compile(
        r"\b(?:geo[- ]?(?:block(?:ing)?|fenc(?:ing)?|restrict(?:ion)?)|"
        r"geographic(?: restriction| block)?|"
        r"(?:country|region|location) (?:block|restriction))\b",
        re.I,
    ),
    "violation_logging": re.compile(
        r"\b(?:violation (?:log(?:ging)?|record)|"
        r"(?:log(?:ged)?|record) (?:violation|denied|blocked|unauthorized|access attempt)|"
        r"(?:denied|blocked|rejected|unauthorized) (?:access|request|ip|attempt)|"
        r"(?:audit|access|security) (?:log(?:ging)?|trail))\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[IPAllowlistCategory, tuple[str, ...]] = {
    "allowlist_configuration": ("security", "backend", "api_platform"),
    "cidr_range_support": ("backend", "security"),
    "per_tenant_customization": ("backend", "platform"),
    "ip_based_authentication": ("security", "backend"),
    "endpoint_bypass": ("security", "backend"),
    "dynamic_updates": ("backend", "platform"),
    "geo_blocking": ("security", "backend"),
    "violation_logging": ("security", "backend", "observability"),
}
_PLANNING_NOTES: dict[IPAllowlistCategory, tuple[str, ...]] = {
    "allowlist_configuration": ("Define IP allowlist configuration structure, storage mechanism, and default behavior.",),
    "cidr_range_support": ("Specify CIDR notation support, validation rules, IPv4/IPv6 handling, and range parsing.",),
    "per_tenant_customization": ("Plan tenant-specific allowlist management, isolation guarantees, and multi-tenancy architecture.",),
    "ip_based_authentication": ("Document IP-based authentication flow, source IP extraction, and proxy header handling.",),
    "endpoint_bypass": ("Specify which endpoints bypass allowlist checks, public endpoint classification, and exemption rules.",),
    "dynamic_updates": ("Plan dynamic allowlist update mechanism, API endpoints, cache invalidation, and propagation strategy.",),
    "geo_blocking": ("Define geo-blocking strategy, GeoIP database integration, country/region targeting, and accuracy requirements.",),
    "violation_logging": ("Document violation logging format, audit trail requirements, retention policies, and alerting strategy.",),
}
_GAP_MESSAGES: dict[IPAllowlistMissingDetail, str] = {
    "missing_cidr_validation": "Specify CIDR validation requirements, supported formats, and error handling for invalid ranges.",
    "missing_tenant_isolation": "Define tenant isolation requirements for allowlists, preventing cross-tenant access.",
}


@dataclass(frozen=True, slots=True)
class SourceAPIIPAllowlistRequirement:
    """One source-backed API IP allowlist requirement."""

    category: IPAllowlistCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: IPAllowlistConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> IPAllowlistCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> IPAllowlistCategory:
        """Compatibility view for extractors that expose concern naming."""
        return self.category

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        """Compatibility view matching adjacent source extractors."""
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "planning_notes": list(self.planning_notes),
            "gap_messages": list(self.gap_messages),
        }


@dataclass(frozen=True, slots=True)
class SourceAPIIPAllowlistRequirementsReport:
    """Source-level API IP allowlist requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceAPIIPAllowlistRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAPIIPAllowlistRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAPIIPAllowlistRequirement, ...]:
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
        """Return API IP allowlist requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API IP Allowlist Requirements Report"
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
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source API IP allowlist requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
            ]
        )
        for req in self.requirements:
            lines.extend(
                [
                    f"### {req.category}",
                    "",
                    f"- Source field: `{req.source_field}`",
                    f"- Confidence: {req.confidence}",
                ]
            )
            if req.value:
                lines.append(f"- Value: {req.value}")
            if req.evidence:
                lines.extend(["- Evidence:", *[f"  - {ev}" for ev in req.evidence]])
            if req.suggested_owners:
                lines.append(f"- Suggested owners: {', '.join(req.suggested_owners)}")
            if req.planning_notes:
                lines.extend(["- Planning notes:", *[f"  - {note}" for note in req.planning_notes]])
            if req.gap_messages:
                lines.extend(["- Gaps:", *[f"  - {gap}" for gap in req.gap_messages]])
            lines.append("")
        return "\n".join(lines)


def extract_source_api_ip_allowlist_requirements(
    brief: SourceBrief | ImplementationBrief | Mapping[str, Any] | str | object,
) -> SourceAPIIPAllowlistRequirementsReport:
    """Extract source API IP allowlist requirements from a source or implementation brief."""
    brief_id, title, payload = _brief_payload(brief)
    if _has_negated_scope(payload):
        return SourceAPIIPAllowlistRequirementsReport(
            brief_id=brief_id,
            title=title,
            requirements=tuple(),
            summary=_empty_summary(),
        )

    requirements: list[SourceAPIIPAllowlistRequirement] = []
    seen_categories: set[IPAllowlistCategory] = set()

    for category in _CATEGORY_ORDER:
        if category in seen_categories:
            continue
        matches = _find_category_matches(payload, category)
        if not matches:
            continue
        seen_categories.add(category)
        evidence, source_field, confidence, value = _best_match(matches, category)
        gaps = _detect_gaps(payload, category)
        requirements.append(
            SourceAPIIPAllowlistRequirement(
                category=category,
                source_field=source_field,
                evidence=evidence,
                confidence=confidence,
                value=value,
                suggested_owners=_OWNER_SUGGESTIONS.get(category, tuple()),
                planning_notes=_PLANNING_NOTES.get(category, tuple()),
                gap_messages=tuple(_GAP_MESSAGES[g] for g in gaps),
            )
        )

    return SourceAPIIPAllowlistRequirementsReport(
        brief_id=brief_id,
        title=title,
        requirements=tuple(requirements),
        summary=_compute_summary(requirements),
    )


def _brief_payload(
    brief: SourceBrief | ImplementationBrief | Mapping[str, Any] | str | object,
) -> tuple[str | None, str | None, dict[str, Any]]:
    if isinstance(brief, (SourceBrief, ImplementationBrief)):
        return brief.id, getattr(brief, "title", None), dict(brief.model_dump(mode="python"))
    if isinstance(brief, str):
        return None, None, {"body": brief}
    if isinstance(brief, Mapping):
        try:
            validated = SourceBrief.model_validate(brief)
            return validated.id, getattr(validated, "title", None), dict(validated.model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            pass
        try:
            validated = ImplementationBrief.model_validate(brief)
            return validated.id, getattr(validated, "title", None), dict(validated.model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            pass
        return brief.get("id"), brief.get("title"), dict(brief)
    if hasattr(brief, "id"):
        payload = {}
        for field in _SCANNED_FIELDS:
            if hasattr(brief, field):
                payload[field] = getattr(brief, field)
        return getattr(brief, "id", None), getattr(brief, "title", None), payload
    return None, None, {}


def _has_negated_scope(payload: Mapping[str, Any]) -> bool:
    searchable = " ".join(str(v) for v in payload.values() if v)
    return bool(_NO_IP_ALLOWLIST_RE.search(searchable)) or bool(_NEGATED_SCOPE_RE.search(searchable))


def _find_category_matches(payload: Mapping[str, Any], category: IPAllowlistCategory) -> list[tuple[str, str, str]]:
    pattern = _CATEGORY_PATTERNS[category]
    matches: list[tuple[str, str, str]] = []

    def _scan_value(field_name: str, value: Any, parent_has_context: bool = False) -> None:
        if isinstance(value, dict):
            # Recursively scan nested dictionaries
            # Check if this dict level has IP allowlist context
            dict_text = " ".join(str(v) for v in value.values() if v)
            has_context = parent_has_context or bool(_IP_ALLOWLIST_CONTEXT_RE.search(dict_text))
            for nested_key, nested_value in value.items():
                nested_field = f"{field_name}.{nested_key}" if field_name else nested_key
                _scan_value(nested_field, nested_value, has_context)
        elif isinstance(value, (list, tuple)):
            # Scan list/tuple items
            for item in value:
                _scan_value(field_name, item, parent_has_context)
        elif value:
            text = str(value)
            if _UNRELATED_RE.search(text):
                return
            # Only require context if parent doesn't have it and text is long enough
            if not parent_has_context and len(text) > 50 and not _IP_ALLOWLIST_CONTEXT_RE.search(text):
                return

            for match in pattern.finditer(text):
                snippet = text[max(0, match.start() - 40) : min(len(text), match.end() + 40)]
                snippet = _SPACE_RE.sub(" ", snippet).strip()
                matches.append((field_name, snippet, match.group(0)))

    for field_name in _SCANNED_FIELDS:
        if field_name in _IGNORED_FIELDS:
            continue
        value = payload.get(field_name)
        if value:
            _scan_value(field_name, value)

    return matches


def _best_match(
    matches: list[tuple[str, str, str]], category: IPAllowlistCategory
) -> tuple[tuple[str, ...], str, IPAllowlistConfidence, str | None]:
    if not matches:
        return tuple(), "", "low", None

    field_name, snippet, keyword = matches[0]
    evidence = tuple(f"{field_name}: ...{snippet}..." for field_name, snippet, _ in matches[:3])

    confidence: IPAllowlistConfidence = "medium"
    if _REQUIREMENT_RE.search(snippet):
        confidence = "high"
    elif not _STRUCTURED_FIELD_RE.search(field_name):
        confidence = "low"

    value = None
    if category == "cidr_range_support":
        value_match = _VALUE_RE.search(snippet)
        if value_match:
            value = value_match.group(0)

    return evidence, field_name, confidence, value


def _detect_gaps(payload: Mapping[str, Any], category: IPAllowlistCategory) -> list[IPAllowlistMissingDetail]:
    gaps: list[IPAllowlistMissingDetail] = []
    searchable = " ".join(str(v) for v in payload.values() if v)

    if category == "cidr_range_support":
        if not _CIDR_VALIDATION_RE.search(searchable):
            gaps.append("missing_cidr_validation")

    if category == "per_tenant_customization":
        if not _TENANT_ISOLATION_RE.search(searchable):
            gaps.append("missing_tenant_isolation")

    return gaps


def _compute_summary(requirements: list[SourceAPIIPAllowlistRequirement]) -> dict[str, Any]:
    category_counts = {category: 0 for category in _CATEGORY_ORDER}
    confidence_counts = {"high": 0, "medium": 0, "low": 0}
    missing_detail_flags: set[str] = set()

    for req in requirements:
        category_counts[req.category] += 1
        confidence_counts[req.confidence] += 1
        for gap_msg in req.gap_messages:
            for detail, msg in _GAP_MESSAGES.items():
                if msg == gap_msg:
                    missing_detail_flags.add(detail)

    return {
        "requirement_count": len(requirements),
        "category_counts": category_counts,
        "confidence_counts": confidence_counts,
        "missing_detail_flags": sorted(missing_detail_flags),
    }


def _empty_summary() -> dict[str, Any]:
    return {
        "requirement_count": 0,
        "category_counts": {category: 0 for category in _CATEGORY_ORDER},
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
    }


__all__ = [
    "IPAllowlistCategory",
    "IPAllowlistMissingDetail",
    "IPAllowlistConfidence",
    "SourceAPIIPAllowlistRequirement",
    "SourceAPIIPAllowlistRequirementsReport",
    "extract_source_api_ip_allowlist_requirements",
]
