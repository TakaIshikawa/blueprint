"""Extract source-level webhook signing requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


WebhookSigningCategory = Literal[
    "signing_secret_creation",
    "signature_verification",
    "timestamp_tolerance",
    "replay_prevention",
    "secret_rotation",
    "multi_secret_grace_period",
    "failure_handling",
    "audit_logging",
    "customer_documentation",
]
WebhookSigningMissingDetail = Literal["missing_signature_verification", "missing_rotation_or_grace_period"]
WebhookSigningConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[WebhookSigningCategory, ...] = (
    "signing_secret_creation",
    "signature_verification",
    "timestamp_tolerance",
    "replay_prevention",
    "secret_rotation",
    "multi_secret_grace_period",
    "failure_handling",
    "audit_logging",
    "customer_documentation",
)
_MISSING_DETAIL_ORDER: tuple[WebhookSigningMissingDetail, ...] = (
    "missing_signature_verification",
    "missing_rotation_or_grace_period",
)
_CONFIDENCE_ORDER: dict[WebhookSigningConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_WEBHOOK_CONTEXT_RE = re.compile(
    r"\b(?:webhooks?|webhook endpoints?|webhook deliveries?|webhook events?|webhook payloads?|"
    r"webhook signatures?|webhook signing|webhook verification|signed webhooks?|"
    r"signing secrets?|signature headers?|signature verification|verify signatures?|"
    r"hmac|sha-?256|timestamp headers?|replay attacks?|replay prevention)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:webhooks?|webhook[_ -]?signatures?|signing[_ -]?secrets?|signature[_ -]?verification|"
    r"signature[_ -]?headers?|hmac|timestamp[_ -]?tolerance|replay|rotation|rotate|"
    r"grace[_ -]?period|dual[_ -]?secret|multi[_ -]?secret|failure|unauthorized|audit|"
    r"logging|security|integration|customer|docs?|documentation|source[_ -]?payload|requirements?)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|create|generate|issue|provision|verify|validate|"
    r"sign|signed|signature|hmac|timestamp|tolerance|prevent|replay|rotate|rotation|"
    r"grace|dual[- ]?secret|multi[- ]?secret|fail|reject|unauthorized|audit|log|"
    r"record|customer|docs?|documentation|guidance|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:webhook signatures?|webhook signing|signature verification|signing secrets?|"
    r"timestamp tolerance|replay prevention|secret rotation|multi[- ]?secret|audit logging|customer docs?)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:webhook signatures?|webhook signing|signature verification|signing secrets?|"
    r"timestamp tolerance|replay prevention|secret rotation|multi[- ]?secret|audit logging|customer docs?)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_WEBHOOK_SIGNING_RE = re.compile(
    r"\b(?:no webhook signing|no webhook signature verification|webhook signatures? are out of scope|"
    r"webhook signing is out of scope|signature verification is out of scope|no webhook signing work)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:keyboard shortcut|translation key|cache key|primary key|foreign key|key value|"
    r"legend key|map key|license key copy|key result|object key|sort key|document signature|"
    r"contract signature|signed contract|sign in|single sign[- ]?on)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:minutes?|hours?|days?|weeks?|months?|years?)|hmac[- ]?sha[- ]?256|"
    r"sha[- ]?256|signature header|timestamp header|webhook|audit log|customer docs?|"
    r"rotate|rotation window|grace period|dual[- ]?secret|multi[- ]?secret|401|403)\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:minutes?|hours?|days?|weeks?|months?|years?)\b", re.I)
_VERIFICATION_DETAIL_RE = re.compile(
    r"\b(?:verify|verification|validate|signature header|hmac|sha[- ]?256|signed payload|"
    r"constant[- ]?time|timestamp tolerance|replay)\b",
    re.I,
)
_ROTATION_GRACE_DETAIL_RE = re.compile(
    r"\b(?:rotate|rotation|rotated|regenerate|reissue|rollover|dual[- ]?secret|"
    r"multi[- ]?secret|multiple secrets?|grace period|overlap|old and new secrets?)\b",
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
    "authentication",
    "auth_requirements",
    "security",
    "authorization",
    "integrations",
    "api",
    "api_keys",
    "access_keys",
    "developer_keys",
    "integration_keys",
    "credentials",
    "tokens",
    "audit",
    "support",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[WebhookSigningCategory, re.Pattern[str]] = {
    "signing_secret_creation": re.compile(
        r"\b(?:create|creation|generate|generated|issue|issued|provision|mint|new signing secrets?|"
        r"webhook secrets?|signing secret|secret creation|show secret once)\b",
        re.I,
    ),
    "signature_verification": re.compile(
        r"\b(?:verify|verification|validate|validated|signature header|webhook signature|signed payload|"
        r"hmac|sha[- ]?256|constant[- ]?time|compare digest|signing algorithm)\b",
        re.I,
    ),
    "timestamp_tolerance": re.compile(
        r"\b(?:timestamp tolerance|timestamp header|timestamp skew|clock skew|freshness window|"
        r"older than|within|tolerance|valid for)\b",
        re.I,
    ),
    "replay_prevention": re.compile(
        r"\b(?:replay|replays|replayed|nonce|event id|delivery id|idempotency|dedupe|duplicate signature|"
        r"previously seen|prevent replay)\b",
        re.I,
    ),
    "secret_rotation": re.compile(
        r"\b(?:rotate|rotation|rotated|regenerate|reissue|rollover|roll secrets?|secret rollover|"
        r"compromised secret|old secret|new secret)\b",
        re.I,
    ),
    "multi_secret_grace_period": re.compile(
        r"\b(?:dual[- ]?secret|multi[- ]?secret|multiple secrets?|old and new secrets?|overlap window|"
        r"grace period|accept both|previous secret|current secret)\b",
        re.I,
    ),
    "failure_handling": re.compile(
        r"\b(?:reject|fail|failure|invalid signature|missing signature|unauthorized|forbidden|401|403|"
        r"error response|dead letter|do not process|drop delivery)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit log|audit trail|audited|logged|logging|signature events?|verification failure|"
        r"secret rotated by|record actor|actor and timestamp|security event)\b",
        re.I,
    ),
    "customer_documentation": re.compile(
        r"\b(?:customer docs?|customer documentation|developer docs?|documentation|help docs?|guide|guidance|"
        r"example|examples|sample code|integration docs?|verification instructions)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[WebhookSigningCategory, tuple[str, ...]] = {
    "signing_secret_creation": ("security", "integrations"),
    "signature_verification": ("security", "integrations"),
    "timestamp_tolerance": ("security", "platform"),
    "replay_prevention": ("security", "platform"),
    "secret_rotation": ("security", "integrations"),
    "multi_secret_grace_period": ("security", "integrations"),
    "failure_handling": ("security", "api_platform"),
    "audit_logging": ("security", "compliance"),
    "customer_documentation": ("developer_experience", "support"),
}
_PLANNING_NOTES: dict[WebhookSigningCategory, tuple[str, ...]] = {
    "signing_secret_creation": ("Define signing secret creation, storage, display-once behavior, and ownership metadata.",),
    "signature_verification": ("Specify signature headers, HMAC algorithm, payload canonicalization, and constant-time comparison.",),
    "timestamp_tolerance": ("Define timestamp header parsing, clock-skew tolerance, and stale request rejection.",),
    "replay_prevention": ("Plan replay protection using timestamp freshness, delivery IDs, nonces, or dedupe storage.",),
    "secret_rotation": ("Describe signing secret rotation, regeneration, compromise response, and customer migration expectations.",),
    "multi_secret_grace_period": ("Specify old/new secret overlap, grace-period duration, and retirement behavior.",),
    "failure_handling": ("Define invalid or missing signature behavior, status codes, delivery processing, and support diagnostics.",),
    "audit_logging": ("Record webhook signing and verification events with actor, endpoint, result, and timestamp.",),
    "customer_documentation": ("Provide customer-facing verification docs, examples, header names, and rotation guidance.",),
}
_GAP_MESSAGES: dict[WebhookSigningMissingDetail, str] = {
    "missing_signature_verification": "Specify webhook signature verification algorithm, headers, or comparison behavior.",
    "missing_rotation_or_grace_period": "Specify signing secret rotation and any multi-secret grace period.",
}


@dataclass(frozen=True, slots=True)
class SourceWebhookSigningRequirement:
    """One source-backed webhook signing requirement."""

    category: WebhookSigningCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: WebhookSigningConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> WebhookSigningCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> WebhookSigningCategory:
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
class SourceWebhookSigningRequirementsReport:
    """Source-level webhook signing requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceWebhookSigningRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceWebhookSigningRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceWebhookSigningRequirement, ...]:
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
        """Return webhook signing requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Webhook Signing Requirements Report"
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
            lines.extend(["", "No source webhook signing requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
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
                f"{_markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{_markdown_cell('; '.join(requirement.gap_messages))} |"
            )
        return "\n".join(lines)


def build_source_webhook_signing_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceWebhookSigningRequirementsReport:
    """Build an webhook signing requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    gap_flags = tuple(_missing_detail_flags(candidate.evidence for candidate in candidates))
    requirements = tuple(_merge_candidates(candidates, gap_flags))
    return SourceWebhookSigningRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements, gap_flags if requirements else ()),
    )


def summarize_source_webhook_signing_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceWebhookSigningRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted webhook signing requirements."""
    if isinstance(source, SourceWebhookSigningRequirementsReport):
        return dict(source.summary)
    return build_source_webhook_signing_requirements(source).summary


def derive_source_webhook_signing_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceWebhookSigningRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_webhook_signing_requirements(source)


def generate_source_webhook_signing_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceWebhookSigningRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_webhook_signing_requirements(source)


def extract_source_webhook_signing_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceWebhookSigningRequirement, ...]:
    """Return webhook signing requirement records from brief-shaped input."""
    return build_source_webhook_signing_requirements(source).requirements


def source_webhook_signing_requirements_to_dict(
    report: SourceWebhookSigningRequirementsReport,
) -> dict[str, Any]:
    """Serialize an webhook signing requirements report to a plain dictionary."""
    return report.to_dict()


source_webhook_signing_requirements_to_dict.__test__ = False


def source_webhook_signing_requirements_to_dicts(
    requirements: (
        tuple[SourceWebhookSigningRequirement, ...]
        | list[SourceWebhookSigningRequirement]
        | SourceWebhookSigningRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize webhook signing requirement records to dictionaries."""
    if isinstance(requirements, SourceWebhookSigningRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_webhook_signing_requirements_to_dicts.__test__ = False


def source_webhook_signing_requirements_to_markdown(
    report: SourceWebhookSigningRequirementsReport,
) -> str:
    """Render an webhook signing requirements report as Markdown."""
    return report.to_markdown()


source_webhook_signing_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: WebhookSigningCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: WebhookSigningConfidence


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
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = _categories(searchable)
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
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        if _NO_WEBHOOK_SIGNING_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_flags: tuple[WebhookSigningMissingDetail, ...],
) -> list[SourceWebhookSigningRequirement]:
    grouped: dict[WebhookSigningCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceWebhookSigningRequirement] = []
    gap_messages = tuple(_GAP_MESSAGES[flag] for flag in gap_flags)
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
            SourceWebhookSigningRequirement(
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
                planning_notes=_PLANNING_NOTES[category],
                gap_messages=gap_messages,
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
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
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _WEBHOOK_CONTEXT_RE.search(key_text)
            )
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
            section_context = inherited_context or bool(
                _WEBHOOK_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = (
                [part]
                if _NEGATED_SCOPE_RE.search(part) and _WEBHOOK_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_words = _field_words(segment.source_field)
    if _NO_WEBHOOK_SIGNING_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _UNRELATED_RE.search(searchable) and not _WEBHOOK_CONTEXT_RE.search(searchable):
        return False
    if not (_WEBHOOK_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _WEBHOOK_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:created|generated|issued|verified|validated|signed|rotated|logged|rejected|documented)\b",
            segment.text,
            re.I,
        )
    )


def _categories(searchable: str) -> list[WebhookSigningCategory]:
    categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
    if "secret_rotation" in categories and "customer_documentation" in categories:
        if re.search(r"\b(?:customer docs?|customer documentation|developer docs?|help docs?|guide|guidance|example|sample code)\b", searchable, re.I):
            categories.remove("secret_rotation")
    return categories


def _missing_detail_flags(evidence_values: Iterable[str]) -> list[WebhookSigningMissingDetail]:
    text = " ".join(evidence_values)
    if not text:
        return []
    flags: list[WebhookSigningMissingDetail] = []
    if not _VERIFICATION_DETAIL_RE.search(text):
        flags.append("missing_signature_verification")
    if not _ROTATION_GRACE_DETAIL_RE.search(text):
        flags.append("missing_rotation_or_grace_period")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(category: WebhookSigningCategory, text: str) -> str | None:
    if category == "timestamp_tolerance":
        if match := _DURATION_RE.search(text):
            return _clean_text(match.group(0)).casefold()
        if match := re.search(r"\b(?P<value>timestamp tolerance|clock skew|freshness window|tolerance)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "signature_verification":
        if match := re.search(r"\b(?P<value>hmac[- ]?sha[- ]?256|sha[- ]?256)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(
            r"\b(?P<value>hmac[- ]?sha[- ]?256|sha[- ]?256|signature header|constant[- ]?time|signed payload|verify)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category in {"secret_rotation", "signing_secret_creation"}:
        if match := re.search(r"\b(?P<value>signing secret|webhook secret|rotate|rotation|regenerate|reissue|rollover)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "multi_secret_grace_period":
        if match := re.search(r"\b(?P<value>dual[- ]?secret|multi[- ]?secret|multiple secrets?|grace period|overlap window)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "failure_handling":
        if match := re.search(r"\b(?P<value>401|403)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(r"\b(?P<value>401|403|unauthorized|forbidden|reject|invalid signature|missing signature)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "replay_prevention":
        if match := re.search(r"\b(?P<value>replay|nonce|event id|delivery id|dedupe|idempotency)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "audit_logging":
        if match := re.search(r"\b(?P<value>audit log|audit trail|verification failure|security event|actor and timestamp)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "customer_documentation":
        if match := re.search(r"\b(?P<value>customer docs?|developer docs?|documentation|examples?|sample code|guide)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    ranked_values = sorted(
        ((index, item.value) for index, item in enumerate(items) if item.value),
        key=lambda indexed_value: (
            0 if re.search(r"\d", indexed_value[1]) else 1,
            0 if _VALUE_RE.search(indexed_value[1]) or _DURATION_RE.search(indexed_value[1]) else 1,
            indexed_value[0],
            len(indexed_value[1]),
            indexed_value[1].casefold(),
        ),
    )
    values = _dedupe(value for _, value in ranked_values)
    return values[0] if values else None


def _confidence(segment: _Segment) -> WebhookSigningConfidence:
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
                "authorization",
                "security",
                "integration",
                "api",
                "webhook",
                "signature",
                "signing",
                "secret",
                "verification",
                "audit",
                "documentation",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _WEBHOOK_CONTEXT_RE.search(searchable):
        return "medium"
    if _WEBHOOK_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceWebhookSigningRequirement, ...],
    gap_flags: tuple[WebhookSigningMissingDetail, ...],
) -> dict[str, Any]:
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
        "missing_detail_flags": list(gap_flags),
        "missing_detail_counts": {
            flag: sum(1 for requirement in requirements if _GAP_MESSAGES[flag] in requirement.gap_messages)
            for flag in _MISSING_DETAIL_ORDER
        },
        "gap_messages": [_GAP_MESSAGES[flag] for flag in gap_flags],
        "status": "ready_for_planning" if requirements and not gap_flags else "needs_webhook_signing_details" if requirements else "no_webhook_signing_language",
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
        "authorization",
        "integrations",
        "webhook",
        "webhooks",
        "webhook_signing",
        "signature_verification",
        "signing_secrets",
        "api",
        "audit",
        "docs",
        "documentation",
        "support",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: WebhookSigningCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[WebhookSigningCategory, tuple[str, ...]] = {
        "signing_secret_creation": ("creation", "create", "generate", "issue", "provision", "secret"),
        "signature_verification": ("verification", "verify", "signature", "hmac", "algorithm"),
        "timestamp_tolerance": ("timestamp", "tolerance", "skew", "freshness"),
        "replay_prevention": ("replay", "nonce", "dedupe", "delivery id", "event id"),
        "secret_rotation": ("rotation", "rotate", "regenerate", "reissue", "rollover"),
        "multi_secret_grace_period": ("grace", "overlap", "dual secret", "multi secret"),
        "failure_handling": ("failure", "invalid", "reject", "unauthorized", "403", "401"),
        "audit_logging": ("audit", "log", "event", "timestamp"),
        "customer_documentation": ("documentation", "docs", "guidance", "example", "support"),
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
    "WebhookSigningCategory",
    "WebhookSigningConfidence",
    "WebhookSigningMissingDetail",
    "SourceWebhookSigningRequirement",
    "SourceWebhookSigningRequirementsReport",
    "build_source_webhook_signing_requirements",
    "derive_source_webhook_signing_requirements",
    "extract_source_webhook_signing_requirements",
    "generate_source_webhook_signing_requirements",
    "summarize_source_webhook_signing_requirements",
    "source_webhook_signing_requirements_to_dict",
    "source_webhook_signing_requirements_to_dicts",
    "source_webhook_signing_requirements_to_markdown",
]
