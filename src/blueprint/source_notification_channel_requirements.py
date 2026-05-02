"""Extract notification channel requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


NotificationChannelRequirement = Literal[
    "email",
    "sms",
    "push",
    "in_app",
    "slack_teams",
    "webhook",
    "status_page",
    "support_ticket",
]
NotificationChannelConfidence = Literal["high", "medium", "low"]

_CHANNEL_ORDER: tuple[NotificationChannelRequirement, ...] = (
    "email",
    "sms",
    "push",
    "in_app",
    "slack_teams",
    "webhook",
    "status_page",
    "support_ticket",
)
_CONFIDENCE_ORDER: dict[NotificationChannelConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_PLANNING_NOTES: dict[NotificationChannelRequirement, str] = {
    "email": "Plan email provider, template, preference, deliverability, and audit tasks for this notification.",
    "sms": "Plan SMS provider, consent, STOP handling, timing, and regional delivery tasks for this notification.",
    "push": "Plan device token, preference, copy, timing, and fallback behavior for this push notification.",
    "in_app": "Plan in-app surface, read state, targeting, and copy tasks for this notification.",
    "slack_teams": "Plan workspace app, channel routing, audience ownership, and escalation tasks for this operator notification.",
    "webhook": "Plan webhook payload, signing, retry, ordering, endpoint preference, and delivery visibility tasks.",
    "status_page": "Plan public status page update workflow, audience copy, timing, and incident communication tasks.",
    "support_ticket": "Plan support ticket creation, routing, ownership, SLA, and customer follow-up tasks.",
}
_CHANNEL_PATTERNS: dict[NotificationChannelRequirement, re.Pattern[str]] = {
    "email": re.compile(r"\b(?:email|e-mail|mail|newsletter|receipt|sendgrid|mailgun|postmark|ses|smtp)\b", re.I),
    "sms": re.compile(r"\b(?:sms|text message|texts?|twilio|short code|long code|10dlc|stop keyword)\b", re.I),
    "push": re.compile(r"\b(?:push notification|push notifications|mobile push|web push|apns|fcm|device token)\b", re.I),
    "in_app": re.compile(
        r"\b(?:in[- ]?app notification|in[- ]?product notification|notification center|notification inbox|toast|banner notification|activity feed)\b",
        re.I,
    ),
    "slack_teams": re.compile(r"\b(?:slack|teams|microsoft teams|operator channel|ops channel|chatops)\b", re.I),
    "webhook": re.compile(r"\b(?:webhook|webhooks|callback url|callback endpoint|event delivery|signed callback)\b", re.I),
    "status_page": re.compile(r"\b(?:status page|statuspage|incident page|public status|service status)\b", re.I),
    "support_ticket": re.compile(r"\b(?:support ticket|ticketing|zendesk|freshdesk|service desk|help desk|case creation|support case)\b", re.I),
}
_CONTEXT_RE = re.compile(
    r"\b(?:notify|notification|alert|message|announce|communication|comms|send|deliver|page|"
    r"contact|escalate|subscriber|recipient|audience|channel)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|should|need(?:s)? to|required|requires?|requirement|ensure|support|"
    r"allow|include|send|notify|alert|announce|page|create|open|post|publish|deliver|"
    r"before|after|within|when|if|on failure|on completion|trigger)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:notification|notify|channel|channels|email|sms|push|in[_ -]?app|slack|teams|"
    r"webhook|status[_ -]?page|support[_ -]?ticket|ticket|audience|recipient|timing|"
    r"template|copy|locale|unsubscribe|preference|opt[_ -]?out)",
    re.I,
)
_TIMING_RE = re.compile(
    r"\b(?:immediately|real[- ]?time|as soon as possible|asynchronously|before [^.;,\n]+|"
    r"after [^.;,\n]+|when [^.;,\n]+|within\s+(?:\d+(?:\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|fifteen|thirty|sixty|ninety)\s+"
    r"(?:seconds?|minutes?|hours?|days?)|daily|weekly|monthly|at least\s+\d+\s+(?:hours?|days?)\s+before)\b",
    re.I,
)
_AUDIENCE_RE = re.compile(
    r"\b(?:to|for|notify|alert|page)\s+(?:the\s+)?((?:affected\s+|eligible\s+|all\s+|"
    r"internal\s+|external\s+)?(?:customers?|users?|admins?|operators?|support agents?|"
    r"support team|on[- ]call|incident commanders?|account owners?|subscribers?|partners?))\b",
    re.I,
)
_TEMPLATE_RE = re.compile(
    r"\b(?:template|copy|subject line|message body|localized copy|content|wording|"
    r"customer-facing copy|notification text)\b(?:[:\s-]+([^.;\n]+))?",
    re.I,
)
_LOCALE_RE = re.compile(
    r"\b(?:locale|locales|localized|locali[sz]ation|translation|language|languages|"
    r"en[-_ ]US|en[-_ ]GB|fr[-_ ]FR|es[-_ ]ES|de[-_ ]DE|Japanese|French|Spanish|German)\b",
    re.I,
)
_PREFERENCE_RE = re.compile(
    r"\b(?:unsubscribe|opt[- ]?out|preference center|preferences|notification settings|"
    r"suppression|STOP keyword|quiet hours|consent)\b",
    re.I,
)
_NO_SIGNAL_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,70}\b(?:notification|notify|channel|email|sms|push|webhook|"
    r"status page|support ticket|slack|teams)\b.{0,70}\b(?:scope|required|needed|changes?)\b",
    re.I,
)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SPACE_RE = re.compile(r"\s+")
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
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "data_requirements",
    "risks",
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
class SourceNotificationChannelRequirement:
    """One source-backed notification channel requirement candidate."""

    source_brief_id: str | None
    channel: NotificationChannelRequirement
    requirement_text: str
    timing: str | None = None
    audience: str | None = None
    template_copy: str | None = None
    locale: str | None = None
    unsubscribe_or_preference: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: NotificationChannelConfidence = "medium"
    planning_note: str | None = None

    @property
    def notification_channel(self) -> NotificationChannelRequirement:
        """Compatibility alias for callers expecting a longer channel name."""
        return self.channel

    @property
    def planning_notes(self) -> str | None:
        """Compatibility alias for callers expecting plural planning notes."""
        return self.planning_note

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "channel": self.channel,
            "requirement_text": self.requirement_text,
            "timing": self.timing,
            "audience": self.audience,
            "template_copy": self.template_copy,
            "locale": self.locale,
            "unsubscribe_or_preference": self.unsubscribe_or_preference,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceNotificationChannelRequirementsReport:
    """Source-level notification channel requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceNotificationChannelRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceNotificationChannelRequirement, ...]:
        """Compatibility view matching reports that name extracted items records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceNotificationChannelRequirement, ...]:
        """Compatibility view matching reports that name extracted items findings."""
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
        """Return notification channel requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Notification Channel Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        channel_counts = self.summary.get("channel_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Channel counts: "
            + ", ".join(f"{channel} {channel_counts.get(channel, 0)}" for channel in _CHANNEL_ORDER),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No notification channel requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Channel | Requirement | Timing | Audience | Template / Copy | Locale | Preference | Source Field | Confidence | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.channel)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.timing or '')} | "
                f"{_markdown_cell(requirement.audience or '')} | "
                f"{_markdown_cell(requirement.template_copy or '')} | "
                f"{_markdown_cell(requirement.locale or '')} | "
                f"{_markdown_cell(requirement.unsubscribe_or_preference or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.planning_note or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_notification_channel_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceNotificationChannelRequirementsReport:
    """Extract source-level notification channel requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _CHANNEL_ORDER.index(requirement.channel),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.requirement_text.casefold(),
                requirement.source_field or "",
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceNotificationChannelRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_notification_channel_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceNotificationChannelRequirementsReport:
    """Compatibility alias for building a notification channel requirements report."""
    return build_source_notification_channel_requirements(source)


def generate_source_notification_channel_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceNotificationChannelRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_notification_channel_requirements(source)


def derive_source_notification_channel_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceNotificationChannelRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_notification_channel_requirements(source)


def summarize_source_notification_channel_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceNotificationChannelRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted notification channel requirements."""
    if isinstance(source_or_result, SourceNotificationChannelRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_notification_channel_requirements(source_or_result).summary


def source_notification_channel_requirements_to_dict(
    report: SourceNotificationChannelRequirementsReport,
) -> dict[str, Any]:
    """Serialize a notification channel requirements report to a plain dictionary."""
    return report.to_dict()


source_notification_channel_requirements_to_dict.__test__ = False


def source_notification_channel_requirements_to_dicts(
    requirements: (
        tuple[SourceNotificationChannelRequirement, ...]
        | list[SourceNotificationChannelRequirement]
        | SourceNotificationChannelRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize notification channel requirement records to dictionaries."""
    if isinstance(requirements, SourceNotificationChannelRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_notification_channel_requirements_to_dicts.__test__ = False


def source_notification_channel_requirements_to_markdown(
    report: SourceNotificationChannelRequirementsReport,
) -> str:
    """Render a notification channel requirements report as Markdown."""
    return report.to_markdown()


source_notification_channel_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    channel: NotificationChannelRequirement
    requirement_text: str
    timing: str | None
    audience: str | None
    template_copy: str | None
    locale: str | None
    unsubscribe_or_preference: str | None
    source_field: str
    evidence: str
    confidence: NotificationChannelConfidence


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
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                value = model.model_validate(source).model_dump(mode="python")
                payload = dict(value)
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
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
            channels = _channels(segment, source_field)
            if not channels:
                continue
            for channel in channels:
                candidates.append(_candidate(source_brief_id, source_field, segment, channel))
    return candidates


def _candidate(
    source_brief_id: str | None,
    source_field: str,
    text: str,
    channel: NotificationChannelRequirement,
) -> _Candidate:
    return _Candidate(
        source_brief_id=source_brief_id,
        channel=channel,
        requirement_text=_requirement_text(text),
        timing=_match_detail(_TIMING_RE, text),
        audience=_audience(text),
        template_copy=_template_copy(text),
        locale=_locale(text),
        unsubscribe_or_preference=_match_detail(_PREFERENCE_RE, text),
        source_field=source_field,
        evidence=_evidence_snippet(source_field, text),
        confidence=_confidence(channel, source_field, text),
    )


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceNotificationChannelRequirement]:
    grouped: dict[tuple[str | None, NotificationChannelRequirement, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_brief_id,
                candidate.channel,
                _dedupe_requirement_key(candidate.requirement_text, candidate.channel),
            ),
            [],
        ).append(candidate)

    requirements: list[SourceNotificationChannelRequirement] = []
    for (_source_brief_id, _channel, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceNotificationChannelRequirement(
                source_brief_id=best.source_brief_id,
                channel=best.channel,
                requirement_text=best.requirement_text,
                timing=_joined_details(item.timing for item in items),
                audience=_joined_details(item.audience for item in items),
                template_copy=_joined_details(item.template_copy for item in items),
                locale=_joined_details(item.locale for item in items),
                unsubscribe_or_preference=_joined_details(
                    item.unsubscribe_or_preference for item in items
                ),
                source_field=best.source_field,
                evidence=tuple(
                    sorted(
                        _dedupe(candidate.evidence for candidate in items),
                        key=lambda item: item.casefold(),
                    )
                )[:5],
                confidence=best.confidence,
                planning_note=_PLANNING_NOTES[best.channel],
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
        if _has_structured_notification_shape(value):
            evidence = _structured_evidence(value)
            if evidence:
                values.append((source_field, evidence))
            return
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            if _STRUCTURED_FIELD_RE.search(key_text) and not isinstance(
                child, (Mapping, list, tuple, set)
            ):
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


def _channels(text: str, source_field: str) -> tuple[NotificationChannelRequirement, ...]:
    searchable = _searchable_text(source_field, text)
    field_words = _field_words(source_field)
    if _generic_notification_statement(text) or _NO_SIGNAL_RE.search(searchable):
        return ()
    channel_matches = [
        channel for channel in _CHANNEL_ORDER if _CHANNEL_PATTERNS[channel].search(searchable)
    ]
    if not channel_matches:
        return ()
    if not (
        _CONTEXT_RE.search(searchable)
        or _STRUCTURED_FIELD_RE.search(field_words)
        or _REQUIREMENT_RE.search(searchable)
    ):
        return ()
    if not (
        _REQUIREMENT_RE.search(searchable)
        or _STRUCTURED_FIELD_RE.search(field_words)
        or _TIMING_RE.search(searchable)
        or _AUDIENCE_RE.search(searchable)
    ):
        return ()
    return tuple(_dedupe(channel_matches))


def _confidence(
    channel: NotificationChannelRequirement, source_field: str, text: str
) -> NotificationChannelConfidence:
    field_words = _field_words(source_field)
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(text))
    has_structured_context = bool(_STRUCTURED_FIELD_RE.search(field_words))
    has_detail = any(
        (
            _match_detail(_TIMING_RE, text),
            _audience(text),
            _template_copy(text),
            _locale(text),
            _match_detail(_PREFERENCE_RE, text),
        )
    )
    has_channel = bool(_CHANNEL_PATTERNS[channel].search(_searchable_text(source_field, text)))
    if has_channel and has_explicit_requirement and (has_structured_context or has_detail):
        return "high"
    if has_channel and (has_explicit_requirement or has_structured_context or has_detail):
        return "medium"
    return "low"


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, str]:
    detail_count = sum(
        bool(value)
        for value in (
            candidate.timing,
            candidate.audience,
            candidate.template_copy,
            candidate.locale,
            candidate.unsubscribe_or_preference,
        )
    )
    return (
        detail_count,
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        candidate.evidence,
    )


def _summary(
    requirements: tuple[SourceNotificationChannelRequirement, ...], source_count: int
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "channel_counts": {
            channel: sum(1 for requirement in requirements if requirement.channel == channel)
            for channel in _CHANNEL_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "channels": [
            channel
            for channel in _CHANNEL_ORDER
            if any(requirement.channel == channel for requirement in requirements)
        ],
    }


def _has_structured_notification_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    return bool(
        keys
        & {
            "notification",
            "notifications",
            "notification_channel",
            "channel",
            "channels",
            "audience",
            "audiences",
            "recipient",
            "recipients",
            "timing",
            "template",
            "copy",
            "locale",
            "locales",
            "unsubscribe",
            "preferences",
            "preference",
            "opt_out",
        }
    )


def _structured_evidence(item: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(str(value))
        if text:
            parts.append(f"{key}: {text}")
    return "; ".join(parts) or _clean_text(str(item))


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
    text = _clean_text(str(value))
    return [text] if text else []


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
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "data_requirements",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _requirement_text(value: str) -> str:
    text = _clean_text(value)
    if len(text) > 220:
        text = f"{text[:217].rstrip()}..."
    return text


def _generic_notification_statement(text: str) -> bool:
    return bool(
        re.fullmatch(
            r"(?:general\s+)?(?:notification|notification channel|customer communication)\s+requirements?\.?",
            _clean_text(text),
            re.I,
        )
    )


def _audience(text: str) -> str | None:
    if match := _AUDIENCE_RE.search(text):
        return _detail(match.group(1))
    return None


def _template_copy(text: str) -> str | None:
    if not (match := _TEMPLATE_RE.search(text)):
        return None
    return _detail(match.group(1) or match.group(0))


def _locale(text: str) -> str | None:
    matches = _dedupe(match.group(0) for match in _LOCALE_RE.finditer(text))
    return ", ".join(matches) if matches else None


def _match_detail(pattern: re.Pattern[str], text: str) -> str | None:
    if match := pattern.search(text):
        return _detail(match.group(0))
    return None


def _joined_details(values: Iterable[str | None]) -> str | None:
    details = _dedupe(value for value in values if value)
    return ", ".join(details) if details else None


def _detail(value: Any) -> str | None:
    text = _clean_text(str(value)) if value is not None else ""
    text = text.strip("`'\" ;,.")
    if not text:
        return None
    return text[:120].rstrip()


def _clean_text(value: str) -> str:
    text = _CHECKBOX_RE.sub("", _BULLET_RE.sub("", value.strip()))
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _searchable_text(source_field: str, text: str) -> str:
    value = f"{_field_words(source_field)} {text}"
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    return value.replace("/", " ").replace("_", " ").replace("-", " ")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[Any]) -> list[Any]:
    seen: set[str] = set()
    result: list[Any] = []
    for value in values:
        key = _dedupe_text_key(value)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _dedupe_requirement_key(value: str, channel: NotificationChannelRequirement) -> str:
    text = _clean_text(value).casefold()
    for pattern in (
        r"\be[- ]?mail\b",
        r"\b(?:sms|text messages?|texts?)\b",
        r"\b(?:push notifications?|mobile push|web push)\b",
        r"\bin[- ]?(?:app|product) notifications?\b",
        r"\b(?:slack|microsoft teams|teams)\b",
        r"\bwebhooks?\b",
        r"\b(?:statuspage|status page|public status)\b",
        r"\b(?:support tickets?|support cases?|ticketing)\b",
    ):
        text = re.sub(pattern, "notification", text)
    text = re.sub(r"\bnotification\s+(?:and|or)\s+notification\b", "notification", text)
    return f"{channel}:{_SPACE_RE.sub(' ', text).strip()}"


def _dedupe_text_key(value: Any) -> str:
    return _clean_text(str(value)).casefold() if value is not None else ""


__all__ = [
    "NotificationChannelConfidence",
    "NotificationChannelRequirement",
    "SourceNotificationChannelRequirement",
    "SourceNotificationChannelRequirementsReport",
    "build_source_notification_channel_requirements",
    "derive_source_notification_channel_requirements",
    "extract_source_notification_channel_requirements",
    "generate_source_notification_channel_requirements",
    "source_notification_channel_requirements_to_dict",
    "source_notification_channel_requirements_to_dicts",
    "source_notification_channel_requirements_to_markdown",
    "summarize_source_notification_channel_requirements",
]
