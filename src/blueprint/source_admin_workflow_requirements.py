"""Extract admin and back-office workflow requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


SourceAdminWorkflowRequirementCategory = Literal[
    "moderation",
    "impersonation",
    "approval",
    "override",
    "dashboard",
    "bulk_action",
    "support_operation",
    "audit_expectation",
]
SourceAdminWorkflowRequirementConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[SourceAdminWorkflowRequirementCategory, ...] = (
    "moderation",
    "impersonation",
    "approval",
    "override",
    "dashboard",
    "bulk_action",
    "support_operation",
    "audit_expectation",
)
_CONFIDENCE_ORDER: dict[SourceAdminWorkflowRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|cannot ship|workflow|process|policy)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:admin|back[- ]office|moderation|impersonation|approval|"
    r"override|dashboard|bulk|support|audit).*?\b(?:in scope|required|needed|changes?)\b",
    re.I,
)
_ADMIN_CONTEXT_RE = re.compile(
    r"\b(?:admin|administrator|internal|back[- ]office|backoffice|ops|operations|operator|"
    r"support|helpdesk|csr|customer success|moderator|moderation|review queue|approval|"
    r"override|dashboard|console|bulk|audit)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:admin|administrator|internal|back[-_ ]?office|ops|operations|support|helpdesk|"
    r"moderation|impersonation|approval|override|dashboard|console|bulk|audit|workflow|"
    r"requirements?|acceptance|controls?)",
    re.I,
)
_CATEGORY_PATTERNS: dict[SourceAdminWorkflowRequirementCategory, re.Pattern[str]] = {
    "moderation": re.compile(
        r"\b(?:moderation|moderator|review queue|content review|flagged content|reported "
        r"(?:content|post|message|user)|ban users?|suspend users?|remove content|approve content|"
        r"reject content|trust and safety)\b",
        re.I,
    ),
    "impersonation": re.compile(
        r"\b(?:admin impersonation|impersonat(?:e|es|ing|ion)|login[- ]?as(?:[- ]a)?[- ]user|"
        r"log in as (?:a )?user|support login|masquerade|assume user|act as (?:a )?"
        r"(?:user|customer)|switch user|view as user|delegated access|customer account access|"
        r"support access to customer accounts?)\b",
        re.I,
    ),
    "approval": re.compile(
        r"\b(?:approval workflow|approval required|requires approval|approve requests?|"
        r"approver|manager approval|security approval|two[- ]person|four[- ]eyes|peer approval|"
        r"review and approve|pending approval|approval queue)\b",
        re.I,
    ),
    "override": re.compile(
        r"\b(?:override|manual override|admin override|policy override|limit override|"
        r"override workflow|exception workflow|force approve|force publish|force unlock|"
        r"break[- ]glass|emergency access)\b",
        re.I,
    ),
    "dashboard": re.compile(
        r"\b(?:admin dashboard|internal dashboard|ops dashboard|operations dashboard|"
        r"support dashboard|moderation dashboard|back[- ]office dashboard|admin console|"
        r"internal console|operator console|management console)\b",
        re.I,
    ),
    "bulk_action": re.compile(
        r"\b(?:bulk action|bulk actions|bulk admin|bulk update|bulk edit|bulk import|"
        r"bulk export|bulk delete|bulk approve|bulk reject|mass update|batch action|"
        r"batch operation|select all)\b",
        re.I,
    ),
    "support_operation": re.compile(
        r"\b(?:support operation|support workflow|support tool(?:ing)?|helpdesk|support agents?|"
        r"customer success|csr|case management|ticket workflow|support ticket|refund customer|"
        r"adjust invoice|account recovery|unlock account|reset customer|resolve disputes?)\b",
        re.I,
    ),
    "audit_expectation": re.compile(
        r"\b(?:audit trail|activity log(?:s)?|admin action log|operator action log|"
        r"audit logs?\s+(?:must|should|need(?:s)? to|records?|captures?|tracks?)|"
        r"record admin actions?|log admin actions?|track admin changes?|who performed)\b",
        re.I,
    ),
}
_SUBJECT_RE = re.compile(
    r"\b(?:admin|administrator|moderation|content|post|message|user|customer|account|tenant|"
    r"workspace|organization|billing|invoice|refund|payment|subscription|role|permission|"
    r"policy|limit|feature flag|settings|dashboard|console|ticket|case|approval|override|"
    r"bulk|support|audit)(?:[- ](?:actions?|changes?|requests?|queue|workflow|operation|"
    r"operations|dashboard|console|accounts?|users?|content|posts?|messages?|settings|"
    r"roles?|permissions?|approvals?|overrides?|tickets?|cases?|logs?))*\b",
    re.I,
)
_MISSING_DETAILS: dict[SourceAdminWorkflowRequirementCategory, tuple[str, ...]] = {
    "moderation": ("queue criteria", "moderator roles", "decision outcomes", "appeal path"),
    "impersonation": ("access scope", "reason capture", "session limit", "audit visibility"),
    "approval": ("approver roles", "approval states", "escalation path", "notification behavior"),
    "override": ("eligible roles", "override reason", "expiration behavior", "post-review policy"),
    "dashboard": ("dashboard users", "visible metrics", "filters", "drill-down actions"),
    "bulk_action": ("selection criteria", "preview step", "failure handling", "undo or recovery"),
    "support_operation": ("support roles", "ticket linkage", "customer visibility", "resolution states"),
    "audit_expectation": ("actor identity", "timestamp source", "reason or ticket", "retention period"),
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
    "acceptance_criteria",
    "definition_of_done",
    "risks",
    "security",
    "compliance",
    "admin",
    "operations",
    "support",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_IGNORED_FIELDS = {
    "id",
    "source_brief_id",
    "source_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
    "source_links",
}


@dataclass(frozen=True, slots=True)
class SourceAdminWorkflowRequirement:
    """One source-backed admin or back-office workflow requirement."""

    source_brief_id: str | None
    category: SourceAdminWorkflowRequirementCategory
    subject_scope: str | None = None
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: SourceAdminWorkflowRequirementConfidence = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "subject_scope": self.subject_scope,
            "missing_details": list(self.missing_details),
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceAdminWorkflowRequirementsReport:
    """Source-level admin and back-office workflow requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceAdminWorkflowRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAdminWorkflowRequirement, ...]:
        """Compatibility view matching extractors that name findings records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return admin workflow requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Admin Workflow Requirements Report"
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
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
            "- Category counts: "
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No admin workflow requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Scope | Confidence | Missing Details | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.category} | "
                f"{_markdown_cell(requirement.subject_scope or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.missing_details))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_admin_workflow_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceAdminWorkflowRequirementsReport:
    """Extract admin workflow requirement records from SourceBrief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _category_index(requirement.category),
                requirement.subject_scope or "",
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceAdminWorkflowRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_admin_workflow_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceAdminWorkflowRequirementsReport:
    """Compatibility alias for building an admin workflow requirements report."""
    return build_source_admin_workflow_requirements(source)


def generate_source_admin_workflow_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceAdminWorkflowRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_admin_workflow_requirements(source)


def derive_source_admin_workflow_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceAdminWorkflowRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_admin_workflow_requirements(source)


def summarize_source_admin_workflow_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | SourceAdminWorkflowRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted admin workflow requirements."""
    if isinstance(source_or_result, SourceAdminWorkflowRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_admin_workflow_requirements(source_or_result).summary


def source_admin_workflow_requirements_to_dict(
    report: SourceAdminWorkflowRequirementsReport,
) -> dict[str, Any]:
    """Serialize an admin workflow requirements report to a plain dictionary."""
    return report.to_dict()


source_admin_workflow_requirements_to_dict.__test__ = False


def source_admin_workflow_requirements_to_dicts(
    requirements: (
        tuple[SourceAdminWorkflowRequirement, ...]
        | list[SourceAdminWorkflowRequirement]
        | SourceAdminWorkflowRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize admin workflow requirement records to dictionaries."""
    if isinstance(requirements, SourceAdminWorkflowRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_admin_workflow_requirements_to_dicts.__test__ = False


def source_admin_workflow_requirements_to_markdown(
    report: SourceAdminWorkflowRequirementsReport,
) -> str:
    """Render an admin workflow requirements report as Markdown."""
    return report.to_markdown()


source_admin_workflow_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: SourceAdminWorkflowRequirementCategory
    subject_scope: str | None
    missing_details: tuple[str, ...]
    confidence: SourceAdminWorkflowRequirementConfidence
    evidence: str


def _source_payloads(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief)) or hasattr(
        source, "model_dump"
    ):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        try:
            value = SourceBrief.model_validate(source).model_dump(mode="python")
            payload = dict(value)
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
        return _source_id(payload), payload
    return None, _object_payload(source)


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
        for source_field, segment in _candidate_segments(payload):
            categories = _categories(segment, source_field)
            if not categories:
                continue
            evidence = _evidence_snippet(source_field, segment)
            for category in categories:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        subject_scope=_subject_scope(category, segment, source_field),
                        missing_details=_missing_details(category, segment),
                        confidence=_confidence(category, segment, source_field),
                        evidence=evidence,
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAdminWorkflowRequirement]:
    grouped: dict[tuple[str | None, SourceAdminWorkflowRequirementCategory, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.source_brief_id, candidate.category, ""),
            [],
        ).append(candidate)

    requirements: list[SourceAdminWorkflowRequirement] = []
    for (source_brief_id, category, _), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        subject_scope = next((item.subject_scope for item in items if item.subject_scope), None)
        common_missing = set(items[0].missing_details)
        for item in items[1:]:
            common_missing.intersection_update(item.missing_details)
        requirements.append(
            SourceAdminWorkflowRequirement(
                source_brief_id=source_brief_id,
                category=category,
                subject_scope=subject_scope,
                missing_details=tuple(sorted(_dedupe(common_missing), key=str.casefold)),
                confidence=confidence,
                evidence=tuple(
                    sorted(
                        _dedupe(item.evidence for item in items),
                        key=lambda item: item.casefold(),
                    )
                )[:5],
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            if _any_signal(key_text) and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    values.append((child_field, _clean_text(f"{key_text}: {text}")))
                continue
            _append_value(values, child_field, child)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        values.extend((source_field, segment) for segment in _segments(text))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for sentence in parts:
            segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _categories(
    text: str, source_field: str
) -> tuple[SourceAdminWorkflowRequirementCategory, ...]:
    if _NEGATED_SCOPE_RE.search(text):
        return ()
    categories = [
        category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(text)
    ]
    field_text = source_field.replace("_", " ").replace("-", " ")
    for category in _CATEGORY_ORDER:
        if (
            category not in categories
            and _CATEGORY_PATTERNS[category].search(field_text)
            and _admin_context(text, source_field)
        ):
            categories.append(category)
    if (
        categories
        and "audit_expectation" not in categories
        and re.search(r"\b(?:who performed|record admin|log admin|track admin)\b", text, re.I)
        and _admin_context(text, source_field)
    ):
        categories.append("audit_expectation")
    if source_field == "title" and categories and not _REQUIRED_RE.search(text):
        return ()
    return tuple(_dedupe(categories))


def _subject_scope(
    category: SourceAdminWorkflowRequirementCategory, text: str, source_field: str
) -> str | None:
    if scoped := _category_scope(category, text):
        return scoped
    ignored = {
        "admin",
        "administrator",
        "internal",
        "support",
        "approval",
        "override",
        "bulk",
        "audit",
        "dashboard",
        "console",
    }
    matches = [
        _clean_scope(match.group(0))
        for match in _SUBJECT_RE.finditer(text)
        if _clean_scope(match.group(0)) not in ignored
    ]
    if matches:
        return _dedupe(matches)[0]
    field_tail = source_field.rsplit(".", 1)[-1].replace("_", " ").replace("-", " ")
    field_matches = [
        _clean_scope(match.group(0))
        for match in _SUBJECT_RE.finditer(field_tail)
        if _clean_scope(match.group(0)) not in ignored
    ]
    return _dedupe(field_matches)[0] if field_matches else None


def _category_scope(
    category: SourceAdminWorkflowRequirementCategory, text: str
) -> str | None:
    patterns: dict[SourceAdminWorkflowRequirementCategory, tuple[re.Pattern[str], ...]] = {
        "moderation": (
            re.compile(r"\b(flagged content|reported content|content review|review queue)\b", re.I),
        ),
        "impersonation": (
            re.compile(r"\b(customer accounts?|user accounts?|tenant accounts?|workspaces?)\b", re.I),
            re.compile(r"\blogin as (?:a )?([a-z ]+?user)\b", re.I),
        ),
        "approval": (
            re.compile(r"\b(approval queue|refund requests?|payout requests?|access requests?)\b", re.I),
        ),
        "override": (
            re.compile(r"\b(admin override|manual override|policy override|limit override)\b", re.I),
        ),
        "dashboard": (
            re.compile(r"\b(internal console|admin console|ops dashboard|support dashboard|admin dashboard)\b", re.I),
        ),
        "bulk_action": (
            re.compile(r"\bbulk (?:update|edit|approve|reject|delete) of ([a-z ]+?)(?:\.|$)", re.I),
            re.compile(r"\b(bulk action|bulk approve|bulk reject|bulk update)\b", re.I),
        ),
        "support_operation": (
            re.compile(r"\b(support agents?|support tickets?|ticket workflow|support cases?)\b", re.I),
        ),
        "audit_expectation": (
            re.compile(r"\b(admin actions?|admin changes?|operator actions?)\b", re.I),
        ),
    }
    for pattern in patterns[category]:
        if match := pattern.search(text):
            return _clean_scope(match.group(1))
    return None


def _missing_details(
    category: SourceAdminWorkflowRequirementCategory, text: str
) -> tuple[str, ...]:
    missing = list(_MISSING_DETAILS[category])
    if re.search(r"\b(?:role|roles|rbac|permission|permissions|moderators?|approvers?|agents?)\b", text, re.I):
        _remove(missing, "moderator roles")
        _remove(missing, "approver roles")
        _remove(missing, "eligible roles")
        _remove(missing, "support roles")
        _remove(missing, "dashboard users")
    if re.search(r"\b(?:reason|justification|tickets?|case id|support cases?)\b", text, re.I):
        _remove(missing, "reason capture")
        _remove(missing, "override reason")
        _remove(missing, "reason or ticket")
        _remove(missing, "ticket linkage")
    if re.search(r"\b(?:audit|log|activity history|who performed|actor)\b", text, re.I):
        _remove(missing, "audit visibility")
        _remove(missing, "actor identity")
    if re.search(r"\b(?:timestamp|utc|time[- ]stamped|recorded at)\b", text, re.I):
        _remove(missing, "timestamp source")
    if re.search(r"\b(?:expire|expiration|ttl|time[- ]boxed|timeboxed|session limit)\b", text, re.I):
        _remove(missing, "session limit")
        _remove(missing, "expiration behavior")
    if re.search(r"\b(?:preview|confirm|dry[- ]run)\b", text, re.I):
        _remove(missing, "preview step")
    if re.search(r"\b(?:undo|rollback|recover|recovery)\b", text, re.I):
        _remove(missing, "undo or recovery")
    if re.search(r"\b(?:filter|filters|search)\b", text, re.I):
        _remove(missing, "filters")
    if re.search(r"\b(?:retain|retention|for \d+|one year|13 months|twelve months)\b", text, re.I):
        _remove(missing, "retention period")
    return tuple(missing)


def _confidence(
    category: SourceAdminWorkflowRequirementCategory, text: str, source_field: str
) -> SourceAdminWorkflowRequirementConfidence:
    field_text = source_field.replace("-", "_").casefold()
    if _REQUIRED_RE.search(text) or any(
        marker in field_text
        for marker in (
            "requirements",
            "acceptance_criteria",
            "success_criteria",
            "definition_of_done",
            "constraints",
        )
    ):
        return "high"
    if _CATEGORY_PATTERNS[category].search(source_field.replace("_", " ")):
        return "high"
    if _admin_context(text, source_field):
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceAdminWorkflowRequirement, ...], source_count: int
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "categories": [
            category
            for category in _CATEGORY_ORDER
            if any(requirement.category == category for requirement in requirements)
        ],
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
        "mvp_goal",
        "context",
        "workflow_context",
        "requirements",
        "constraints",
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "risks",
        "security",
        "compliance",
        "admin",
        "operations",
        "support",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _admin_context(text: str, source_field: str) -> bool:
    field_text = source_field.replace("_", " ").replace("-", " ")
    return _ADMIN_CONTEXT_RE.search(text) is not None or _STRUCTURED_FIELD_RE.search(field_text) is not None


def _any_signal(text: str) -> bool:
    return _ADMIN_CONTEXT_RE.search(text) is not None or any(
        pattern.search(text) for pattern in _CATEGORY_PATTERNS.values()
    )


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    text = _CHECKBOX_RE.sub("", _BULLET_RE.sub("", value.strip()))
    return _SPACE_RE.sub(" ", text).strip()


def _clean_scope(value: str) -> str:
    return _clean_text(value).casefold().strip(" .,:;")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _category_index(category: SourceAdminWorkflowRequirementCategory) -> int:
    return _CATEGORY_ORDER.index(category)


def _remove(values: list[str], value: str) -> None:
    while value in values:
        values.remove(value)


def _dedupe(values: Iterable[Any]) -> list[Any]:
    seen: set[str] = set()
    result: list[Any] = []
    for value in values:
        key = _dedupe_key(value)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _dedupe_key(value: Any) -> str:
    return _clean_text(str(value)).casefold() if value is not None else ""
