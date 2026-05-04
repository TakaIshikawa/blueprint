"""Extract source-level marketplace listing requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


MarketplaceListingCategory = Literal[
    "listing_copy",
    "screenshots_assets",
    "oauth_review",
    "privacy_policy_link",
    "terms_link",
    "support_contact",
    "category_tags",
    "pricing_disclosure",
    "review_submission_deadline",
    "approval_status_tracking",
    "partner_owner",
]
MarketplaceListingMissingDetail = Literal["missing_review_deadline", "missing_approval_tracking"]
MarketplaceListingConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[MarketplaceListingCategory, ...] = (
    "listing_copy",
    "screenshots_assets",
    "oauth_review",
    "privacy_policy_link",
    "terms_link",
    "support_contact",
    "category_tags",
    "pricing_disclosure",
    "review_submission_deadline",
    "approval_status_tracking",
    "partner_owner",
)
_CONFIDENCE_ORDER: dict[MarketplaceListingConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_MARKETPLACE_CONTEXT_RE = re.compile(
    r"\b(?:marketplace|app directory|app store|listing|"
    r"store listing|app listing|directory listing|"
    r"partner portal|partner directory|"
    r"publish|publication|launch|"
    r"review process|approval|oauth review|"
    r"app submission|submit app)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:marketplace|listing|store|directory|app|"
    r"launch|review|approval|oauth|"
    r"privacy|terms|support|contact|"
    r"screenshot|asset|copy|description|"
    r"metadata|requirements?)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"provide|include|submit|upload|prepare|create|write|"
    r"before launch|prior to|deadline|due|"
    r"acceptance|done when)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:marketplace|app directory|store listing|app listing|"
    r"publish|publication|launch|review|submission)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:marketplace|app directory|store listing|app listing|"
    r"publish|publication|launch|review|submission)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_MARKETPLACE_RE = re.compile(
    r"\b(?:no marketplace|no app directory|no store listing|"
    r"no app listing|no publication|no launch|"
    r"marketplace is out of scope|store listing is out of scope)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:shopping list|wish list|to-do list)\b",
    re.I,
)
_URL_RE = re.compile(
    r"\b(?:https?://|www\.)[^\s]+",
    re.I,
)
_DEADLINE_RE = re.compile(
    r"\b(?:deadline|due date|submit by|submission date|"
    r"review by|approval by|launch date|go-?live date)\b",
    re.I,
)
_APPROVAL_STATUS_RE = re.compile(
    r"\b(?:approval status|review status|submission status|"
    r"approved|rejected|pending review|in review|under review)\b",
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
    "launch",
    "launch_notes",
    "launch_plan",
    "marketplace",
    "listing",
    "store",
    "directory",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[MarketplaceListingCategory, re.Pattern[str]] = {
    "listing_copy": re.compile(
        r"\b(?:listing (?:copy|text|description|content)|"
        r"app description|store description|"
        r"marketplace description|directory description|"
        r"product description|app copy|"
        r"short description|long description|"
        r"tagline|elevator pitch)\b",
        re.I,
    ),
    "screenshots_assets": re.compile(
        r"\b(?:screenshot|screen shot|screen capture|"
        r"app screenshot|product screenshot|"
        r"asset|image|graphic|icon|logo|banner|"
        r"promo(?:tional)? (?:image|graphic|asset)|"
        r"app icon|app logo|marketing asset)\b",
        re.I,
    ),
    "oauth_review": re.compile(
        r"\b(?:oauth review|oauth approval|"
        r"oauth verification|oauth submission|"
        r"oauth scope|oauth permission|"
        r"authorization review|auth review|"
        r"security review|api permission)\b",
        re.I,
    ),
    "privacy_policy_link": re.compile(
        r"\b(?:privacy policy|privacy url|privacy link|"
        r"data privacy|privacy notice|"
        r"privacy statement|user privacy)\b",
        re.I,
    ),
    "terms_link": re.compile(
        r"\b(?:terms (?:of service|of use|and conditions)|"
        r"tos|service terms|terms url|terms link|"
        r"user agreement|end user license|eula)\b",
        re.I,
    ),
    "support_contact": re.compile(
        r"\b(?:support (?:contact|email|url|link|channel)|"
        r"customer support|technical support|"
        r"help (?:desk|center|email|url)|"
        r"contact (?:email|information|details))\b",
        re.I,
    ),
    "category_tags": re.compile(
        r"\b(?:category|categories|tag|tags|classification|"
        r"app category|marketplace category|"
        r"directory category|listing category|"
        r"product category|industry tag)\b",
        re.I,
    ),
    "pricing_disclosure": re.compile(
        r"\b(?:pricing|price|cost|fee|billing|subscription|"
        r"pricing model|pricing tier|pricing plan|"
        r"free trial|freemium|paid feature|"
        r"pricing disclosure|pricing information)\b",
        re.I,
    ),
    "review_submission_deadline": re.compile(
        r"\b(?:(?:review|submission|approval|launch) (?:deadline|due date|date)|"
        r"submit by|due by|deadline for|"
        r"review by|approval by|launch date|go-?live date|"
        r"target launch|planned launch)\b",
        re.I,
    ),
    "approval_status_tracking": re.compile(
        r"\b(?:approval status|review status|submission status|"
        r"track(?:ing)? (?:approval|review|status)|"
        r"status check|status update|"
        r"approved|rejected|pending|in review|under review)\b",
        re.I,
    ),
    "partner_owner": re.compile(
        r"\b(?:partner (?:owner|contact|manager|lead|rep)|"
        r"marketplace (?:owner|contact|manager|lead|rep)|"
        r"(?:account|partner) manager|partner success|"
        r"partner team|marketplace team)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[MarketplaceListingCategory, tuple[str, ...]] = {
    "listing_copy": ("product_marketing", "content", "product_manager"),
    "screenshots_assets": ("design", "marketing", "product_marketing"),
    "oauth_review": ("security", "backend", "platform"),
    "privacy_policy_link": ("legal", "compliance", "privacy"),
    "terms_link": ("legal", "compliance"),
    "support_contact": ("customer_support", "product_manager"),
    "category_tags": ("product_marketing", "product_manager"),
    "pricing_disclosure": ("product_manager", "finance", "legal"),
    "review_submission_deadline": ("product_manager", "project_manager"),
    "approval_status_tracking": ("product_manager", "project_manager"),
    "partner_owner": ("partnerships", "business_development", "product_manager"),
}
_PLANNING_NOTES: dict[MarketplaceListingCategory, tuple[str, ...]] = {
    "listing_copy": ("Define listing copy requirements, character limits, localization needs, and content approval process.",),
    "screenshots_assets": ("Specify screenshot dimensions, asset formats, branding guidelines, and review process.",),
    "oauth_review": ("Document OAuth scope requirements, security review process, and approval timeline.",),
    "privacy_policy_link": ("Ensure privacy policy URL is publicly accessible, compliant, and current.",),
    "terms_link": ("Ensure terms of service URL is publicly accessible, compliant, and current.",),
    "support_contact": ("Provide support contact email, help center URL, and expected response time SLA.",),
    "category_tags": ("Select appropriate marketplace categories, tags, and keywords for discoverability.",),
    "pricing_disclosure": ("Disclose pricing model, free trial details, paid features, and billing terms.",),
    "review_submission_deadline": ("Establish submission deadline, review timeline, and launch date constraints.",),
    "approval_status_tracking": ("Set up approval tracking process, status updates, and stakeholder communication plan.",),
    "partner_owner": ("Identify partner owner, establish communication channel, and define escalation path.",),
}
_GAP_MESSAGES: dict[MarketplaceListingMissingDetail, str] = {
    "missing_review_deadline": "Specify review submission deadline, expected turnaround time, and launch date target.",
    "missing_approval_tracking": "Define approval status tracking mechanism, notification process, and owner responsibilities.",
}


@dataclass(frozen=True, slots=True)
class SourceMarketplaceListingRequirement:
    """One source-backed marketplace listing requirement."""

    category: MarketplaceListingCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: MarketplaceListingConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> MarketplaceListingCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> MarketplaceListingCategory:
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
class SourceMarketplaceListingRequirementsReport:
    """Source-level marketplace listing requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceMarketplaceListingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceMarketplaceListingRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceMarketplaceListingRequirement, ...]:
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
        """Return marketplace listing requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Marketplace Listing Requirements Report"
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
            lines.extend(["", "No source marketplace listing requirements were inferred."])
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


def extract_source_marketplace_listing_requirements(
    brief: SourceBrief | ImplementationBrief | Mapping[str, Any] | str | object,
) -> SourceMarketplaceListingRequirementsReport:
    """Extract source marketplace listing requirements from a source or implementation brief."""
    brief_id, title, payload = _brief_payload(brief)
    if _has_negated_scope(payload):
        return SourceMarketplaceListingRequirementsReport(
            brief_id=brief_id,
            title=title,
            requirements=tuple(),
            summary=_empty_summary(),
        )

    requirements: list[SourceMarketplaceListingRequirement] = []
    seen_categories: set[MarketplaceListingCategory] = set()

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
            SourceMarketplaceListingRequirement(
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

    return SourceMarketplaceListingRequirementsReport(
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
    return bool(_NO_MARKETPLACE_RE.search(searchable)) or bool(_NEGATED_SCOPE_RE.search(searchable))


def _find_category_matches(payload: Mapping[str, Any], category: MarketplaceListingCategory) -> list[tuple[str, str, str]]:
    pattern = _CATEGORY_PATTERNS[category]
    matches: list[tuple[str, str, str]] = []

    # Check if the brief itself has marketplace context in key fields
    brief_has_context = False
    for key in ("domain", "title", "summary", "marketplace", "listing", "store", "directory", "launch_notes"):
        if key in payload and payload[key]:
            if _MARKETPLACE_CONTEXT_RE.search(str(payload[key])):
                brief_has_context = True
                break

    def _scan_value(field_name: str, value: Any, parent_has_context: bool = False) -> None:
        if isinstance(value, dict):
            # Recursively scan nested dictionaries
            # Check if this dict level has marketplace context
            dict_text = " ".join(str(v) for v in value.values() if v)
            has_context = parent_has_context or bool(_MARKETPLACE_CONTEXT_RE.search(dict_text))
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
            if not parent_has_context and len(text) > 50 and not _MARKETPLACE_CONTEXT_RE.search(text):
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
            _scan_value(field_name, value, brief_has_context)

    return matches


def _best_match(
    matches: list[tuple[str, str, str]], category: MarketplaceListingCategory
) -> tuple[tuple[str, ...], str, MarketplaceListingConfidence, str | None]:
    if not matches:
        return tuple(), "", "low", None

    field_name, snippet, _keyword = matches[0]
    evidence = tuple(f"{field_name}: ...{snippet}..." for field_name, snippet, _ in matches[:3])

    confidence: MarketplaceListingConfidence = "medium"
    if _REQUIREMENT_RE.search(snippet):
        confidence = "high"
    elif not _STRUCTURED_FIELD_RE.search(field_name):
        confidence = "low"

    value = None
    if category in ("privacy_policy_link", "terms_link", "support_contact"):
        url_match = _URL_RE.search(snippet)
        if url_match:
            value = url_match.group(0)
    elif category == "review_submission_deadline":
        # Try to extract deadline date/time mentions
        deadline_context = snippet[max(0, snippet.lower().find("deadline") - 20):]
        date_pattern = re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b", re.I)
        date_match = date_pattern.search(deadline_context)
        if date_match:
            value = date_match.group(0)

    return evidence, field_name, confidence, value


def _detect_gaps(payload: Mapping[str, Any], category: MarketplaceListingCategory) -> list[MarketplaceListingMissingDetail]:
    gaps: list[MarketplaceListingMissingDetail] = []
    searchable = " ".join(str(v) for v in payload.values() if v)

    if category == "review_submission_deadline":
        if not _DEADLINE_RE.search(searchable):
            gaps.append("missing_review_deadline")

    if category == "approval_status_tracking":
        if not _APPROVAL_STATUS_RE.search(searchable):
            gaps.append("missing_approval_tracking")

    return gaps


def _compute_summary(requirements: list[SourceMarketplaceListingRequirement]) -> dict[str, Any]:
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
    "MarketplaceListingCategory",
    "MarketplaceListingMissingDetail",
    "MarketplaceListingConfidence",
    "SourceMarketplaceListingRequirement",
    "SourceMarketplaceListingRequirementsReport",
    "extract_source_marketplace_listing_requirements",
]
