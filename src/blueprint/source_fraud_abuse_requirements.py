"""Extract source-level fraud and abuse-prevention requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


FraudAbuseSignal = Literal[
    "fraud_scoring",
    "account_abuse",
    "signup_abuse",
    "payment_fraud",
    "suspicious_activity",
    "velocity_limits",
    "device_fingerprinting",
    "ip_reputation",
    "manual_review",
    "chargeback_risk",
    "bot_detection",
    "abuse_reporting",
]
FraudAbuseConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SIGNAL_ORDER: tuple[FraudAbuseSignal, ...] = (
    "fraud_scoring",
    "account_abuse",
    "signup_abuse",
    "payment_fraud",
    "suspicious_activity",
    "velocity_limits",
    "device_fingerprinting",
    "ip_reputation",
    "manual_review",
    "chargeback_risk",
    "bot_detection",
    "abuse_reporting",
)
_CONFIDENCE_ORDER: dict[FraudAbuseConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_PLANNING_NOTES: dict[FraudAbuseSignal, str] = {
    "fraud_scoring": "Plan fraud score inputs, thresholds, reason codes, storage, alerting, and downstream decision paths.",
    "account_abuse": "Add account trust controls for takeover, fake accounts, shared identities, suspension, and support appeal workflows.",
    "signup_abuse": "Add signup rate limits, verification gates, duplicate checks, and instrumentation before account creation.",
    "payment_fraud": "Coordinate payment risk checks, authorization holds, billing events, and customer support handling.",
    "suspicious_activity": "Define suspicious activity rules, monitoring events, alert routing, and investigation ownership.",
    "velocity_limits": "Implement per-user, per-card, per-IP, or per-device velocity limits with clear reset and override behavior.",
    "device_fingerprinting": "Capture device signals with privacy review, matching rules, retention, and false-positive handling.",
    "ip_reputation": "Integrate IP reputation, proxy/VPN/Tor checks, geolocation risk, and allowlist/blocklist operations.",
    "manual_review": "Create review queues, reviewer roles, evidence views, SLAs, approve/deny outcomes, and audit logging.",
    "chargeback_risk": "Plan chargeback monitoring, dispute evidence, refund/hold policies, and billing/support escalation paths.",
    "bot_detection": "Add bot detection, CAPTCHA or challenge flows, automation telemetry, and accessibility fallback paths.",
    "abuse_reporting": "Provide report-abuse intake, triage queues, notifier decisions, enforcement status, and reporter feedback.",
}
_SIGNAL_PATTERNS: dict[FraudAbuseSignal, re.Pattern[str]] = {
    "fraud_scoring": re.compile(
        r"\b(?:fraud score|fraud scoring|risk score|risk scoring|risk model|risk engine|"
        r"fraud model|score threshold|risk threshold|risk tier|trust score)\b",
        re.I,
    ),
    "account_abuse": re.compile(
        r"\b(?:account abuse|fake accounts?|account takeover|ATO|compromised account|shared account|"
        r"duplicate accounts?|multi[- ]?account|ban evasion|trust and safety)\b",
        re.I,
    ),
    "signup_abuse": re.compile(
        r"\b(?:signup abuse|sign[- ]?up abuse|registration abuse|fake signup|bulk signup|"
        r"mass registration|new account abuse|account creation abuse|scripted signup|invite abuse)\b",
        re.I,
    ),
    "payment_fraud": re.compile(
        r"\b(?:payment fraud|card fraud|stolen card|fraudulent payment|billing fraud|"
        r"payment risk|payment abuse|card testing|carding|authorization abuse)\b",
        re.I,
    ),
    "suspicious_activity": re.compile(
        r"\b(?:suspicious activit(?:y|ies)|suspicious behavior|anomal(?:y|ies)|anomalous|"
        r"risk event|unusual activity|abnormal activity|flag suspicious|fraud alert)\b",
        re.I,
    ),
    "velocity_limits": re.compile(
        r"\b(?:velocity limit|velocity limits|rate limit|rate limits|rate limiting|throttle|throttling|"
        r"attempt limit|transaction limit|per\s+(?:minute|hour|day|account|user|card|device|ip)|"
        r"\d+\s+(?:attempts?|transactions?|signups?|requests?)\s+per\s+(?:minute|hour|day))\b",
        re.I,
    ),
    "device_fingerprinting": re.compile(
        r"\b(?:device fingerprint|device fingerprinting|device id|device identifier|device reputation|"
        r"device risk|device match|browser fingerprint|hardware fingerprint)\b",
        re.I,
    ),
    "ip_reputation": re.compile(
        r"\b(?:ip reputation|ip risk|proxy detection|vpn detection|tor exit|tor detection|"
        r"datacenter ip|geo velocity|geo[- ]?location risk|blocklisted ip|allowlisted ip)\b",
        re.I,
    ),
    "manual_review": re.compile(
        r"\b(?:manual review|human review|review queue|reviewer|risk review|fraud review|"
        r"trust review|ops review|escalate to review|investigation queue)\b",
        re.I,
    ),
    "chargeback_risk": re.compile(
        r"\b(?:chargeback|chargebacks|dispute risk|payment dispute|friendly fraud|"
        r"retrieval request|representment|dispute evidence)\b",
        re.I,
    ),
    "bot_detection": re.compile(
        r"\b(?:bot detection|bot mitigation|bot traffic|automation detection|automated abuse|"
        r"captcha|challenge|headless browser|scripted signup|credential stuffing)\b",
        re.I,
    ),
    "abuse_reporting": re.compile(
        r"\b(?:abuse report|report abuse|reporting abuse|abuse reporting|flag content|"
        r"report user|report listing|trust report|moderation report|report queue)\b",
        re.I,
    ),
}
_FIELD_SIGNAL_PATTERNS: dict[FraudAbuseSignal, re.Pattern[str]] = {
    signal: re.compile(signal.replace("_", r"[_ -]?"), re.I) for signal in _SIGNAL_ORDER
}
_ABUSE_CONTEXT_RE = re.compile(
    r"\b(?:fraud|abuse|risk|trust and safety|suspicious|velocity|chargeback|bot|captcha|"
    r"manual review|device fingerprint|ip reputation|payment risk|account takeover|signup abuse|moderation)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:fraud|abuse|risk|trust[_ -]?safety|suspicious|velocity|rate[_ -]?limit|"
    r"device[_ -]?fingerprint|device[_ -]?reputation|ip[_ -]?reputation|manual[_ -]?review|"
    r"chargeback|bot|captcha|payment[_ -]?fraud|signup[_ -]?abuse|account[_ -]?abuse|"
    r"report[_ -]?abuse|abuse[_ -]?report|moderation)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|define|"
    r"detect|flag|score|block|deny|suspend|limit|throttle|challenge|captcha|review|escalate|"
    r"hold|refund|monitor|alert|report|triage|enforce|prevent|mitigate|acceptance|done when)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:fraud|abuse|risk|chargeback|bot|captcha|manual review|rate limits?)\b"
    r".{0,100}\b(?:required|needed|in scope|planned|changes?|impact)\b|"
    r"\b(?:fraud|abuse|risk|chargeback|bot|captcha|manual review|rate limits?)\b"
    r".{0,100}\b(?:not required|not needed|out of scope|no changes?|non-goal|non goal)\b",
    re.I,
)
_FLOW_RE = re.compile(
    r"\b(?:signup|sign[- ]?up|registration|login|authentication|onboarding|checkout|payment|billing|"
    r"refund|payout|subscription|account creation|invite|posting|content upload|messaging|support|"
    r"moderation|reporting|appeal|admin)\b",
    re.I,
)
_ENFORCEMENT_RE = re.compile(
    r"\b(?:block|deny|decline|suspend|ban|disable|lock|hold|freeze|rate limits?|limit|throttle|challenge|captcha|"
    r"step[- ]?up|verify|refund|void|cancel|quarantine|remove|shadowban|flag|allowlist|blocklist)\b",
    re.I,
)
_REVIEW_RE = re.compile(
    r"\b(?:manual review|human review|review queue|reviewer|risk review|fraud review|trust review|"
    r"ops review|support review|moderation queue|investigation queue|appeal|escalat(?:e|ion))\b",
    re.I,
)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
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
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "security",
    "trust_safety",
    "fraud",
    "abuse",
    "moderation",
    "billing",
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
    "id",
    "source_id",
    "source_brief_id",
    "domain",
    "status",
}


@dataclass(frozen=True, slots=True)
class SourceFraudAbuseRequirement:
    """One source-backed fraud and abuse-prevention requirement."""

    source_brief_id: str | None
    abuse_signal: FraudAbuseSignal
    requirement_text: str
    protected_flow: str | None = None
    enforcement_action: str | None = None
    review_path: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: FraudAbuseConfidence = "medium"
    planning_note: str = ""
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)

    @property
    def category(self) -> FraudAbuseSignal:
        """Compatibility view for extractors that expose category naming."""
        return self.abuse_signal

    @property
    def requirement_category(self) -> FraudAbuseSignal:
        """Compatibility alias for callers expecting a longer category field name."""
        return self.abuse_signal

    @property
    def planning_notes(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting plural planning notes."""
        return (self.planning_note,)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "abuse_signal": self.abuse_signal,
            "requirement_text": self.requirement_text,
            "protected_flow": self.protected_flow,
            "enforcement_action": self.enforcement_action,
            "review_path": self.review_path,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
            "unresolved_questions": list(self.unresolved_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceFraudAbuseRequirementsReport:
    """Source-level fraud and abuse-prevention requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceFraudAbuseRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceFraudAbuseRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceFraudAbuseRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return fraud and abuse requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Fraud Abuse Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        signal_counts = self.summary.get("signal_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No fraud or abuse-prevention requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Abuse Signal | Requirement | Protected Flow | Enforcement Action | Review Path | Source Field | Confidence | Planning Note | Unresolved Questions | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.abuse_signal)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.protected_flow or '')} | "
                f"{_markdown_cell(requirement.enforcement_action or '')} | "
                f"{_markdown_cell(requirement.review_path or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell('; '.join(requirement.unresolved_questions))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_fraud_abuse_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFraudAbuseRequirementsReport:
    """Extract source-level fraud and abuse-prevention requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceFraudAbuseRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_fraud_abuse_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFraudAbuseRequirementsReport:
    """Compatibility alias for building a fraud and abuse requirements report."""
    return build_source_fraud_abuse_requirements(source)


def generate_source_fraud_abuse_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFraudAbuseRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_fraud_abuse_requirements(source)


def derive_source_fraud_abuse_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFraudAbuseRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_fraud_abuse_requirements(source)


def summarize_source_fraud_abuse_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceFraudAbuseRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted fraud and abuse requirements."""
    if isinstance(source_or_result, SourceFraudAbuseRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_fraud_abuse_requirements(source_or_result).summary


def source_fraud_abuse_requirements_to_dict(
    report: SourceFraudAbuseRequirementsReport,
) -> dict[str, Any]:
    """Serialize a fraud and abuse requirements report to a plain dictionary."""
    return report.to_dict()


source_fraud_abuse_requirements_to_dict.__test__ = False


def source_fraud_abuse_requirements_to_dicts(
    requirements: (
        tuple[SourceFraudAbuseRequirement, ...]
        | list[SourceFraudAbuseRequirement]
        | SourceFraudAbuseRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize fraud and abuse requirement records to dictionaries."""
    if isinstance(requirements, SourceFraudAbuseRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_fraud_abuse_requirements_to_dicts.__test__ = False


def source_fraud_abuse_requirements_to_markdown(
    report: SourceFraudAbuseRequirementsReport,
) -> str:
    """Render a fraud and abuse requirements report as Markdown."""
    return report.to_markdown()


source_fraud_abuse_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    abuse_signal: FraudAbuseSignal
    requirement_text: str
    protected_flow: str | None
    enforcement_action: str | None
    review_path: str | None
    source_field: str
    evidence: str
    confidence: FraudAbuseConfidence


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


def _source_payload(source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object) -> tuple[str | None, dict[str, Any]]:
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
        for segment in _candidate_segments(payload):
            if _NEGATED_RE.search(_searchable_text(segment.source_field, segment.text)):
                continue
            for abuse_signal in _signals(segment):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        abuse_signal=abuse_signal,
                        requirement_text=_requirement_text(segment.text),
                        protected_flow=_field_value_detail("protected_flow", segment.text) or _detail(_FLOW_RE, segment.text),
                        enforcement_action=_field_value_detail("enforcement_action", segment.text)
                        or _detail(_ENFORCEMENT_RE, segment.text),
                        review_path=_field_value_detail("review_path", segment.text) or _detail(_REVIEW_RE, segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(abuse_signal, segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceFraudAbuseRequirement]:
    grouped: dict[tuple[str | None, FraudAbuseSignal, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_brief_id,
                candidate.abuse_signal,
                _dedupe_requirement_key(candidate.requirement_text, candidate.abuse_signal),
            ),
            [],
        ).append(candidate)

    requirements: list[SourceFraudAbuseRequirement] = []
    for (_source_brief_id, abuse_signal, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceFraudAbuseRequirement(
                source_brief_id=best.source_brief_id,
                abuse_signal=abuse_signal,
                requirement_text=best.requirement_text,
                protected_flow=_first_detail(item.protected_flow for item in items),
                enforcement_action=_first_detail(item.enforcement_action for item in items),
                review_path=_first_detail(item.review_path for item in items),
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                planning_note=_PLANNING_NOTES[abuse_signal],
                unresolved_questions=_unresolved_questions(abuse_signal, items),
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _SIGNAL_ORDER.index(requirement.abuse_signal),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.requirement_text.casefold(),
            requirement.source_field or "",
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


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        if _has_structured_shape(value):
            for evidence in _structured_segments(value):
                segments.append(_Segment(source_field, evidence, True))
            return
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _ABUSE_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, f"{source_field}.{key}", value[key], child_context)
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
            section_context = inherited_context or bool(_ABUSE_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if any(pattern.search(part) for pattern in _SIGNAL_PATTERNS.values()) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text and not _NEGATED_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _signals(segment: _Segment) -> tuple[FraudAbuseSignal, ...]:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    explicit_signal = _explicit_abuse_signal(segment.text)
    if explicit_signal:
        return (explicit_signal,)
    if not (
        _ABUSE_CONTEXT_RE.search(searchable)
        or _STRUCTURED_FIELD_RE.search(field_words)
        or any(pattern.search(searchable) for pattern in _SIGNAL_PATTERNS.values())
    ):
        return ()
    if not (
        _REQUIREMENT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(field_words)
        or any(pattern.search(searchable) for pattern in _SIGNAL_PATTERNS.values())
    ):
        return ()
    field_signals = [
        signal
        for signal in _SIGNAL_ORDER
        if _FIELD_SIGNAL_PATTERNS[signal].search(field_words)
    ]
    signals = [
        signal
        for signal in _SIGNAL_ORDER
        if _SIGNAL_PATTERNS[signal].search(searchable)
    ]
    return tuple(_dedupe(field_signals + signals))


def _explicit_abuse_signal(text: str) -> FraudAbuseSignal | None:
    match = re.search(r"\babuse_signal:\s*([a-zA-Z0-9_ -]+?)(?:;|$)", text)
    if not match:
        return None
    value = match.group(1).strip().casefold().replace("-", "_").replace(" ", "_")
    return value if value in _SIGNAL_ORDER else None


def _confidence(abuse_signal: FraudAbuseSignal, segment: _Segment) -> FraudAbuseConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(segment.text))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    has_signal = bool(_SIGNAL_PATTERNS[abuse_signal].search(searchable))
    if has_signal and has_explicit_requirement and has_structured_context:
        return "high"
    if has_signal and (has_explicit_requirement or has_structured_context):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceFraudAbuseRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "signal_counts": {
            signal: sum(1 for requirement in requirements if requirement.abuse_signal == signal)
            for signal in _SIGNAL_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "signals": [
            signal
            for signal in _SIGNAL_ORDER
            if any(requirement.abuse_signal == signal for requirement in requirements)
        ],
    }


def _unresolved_questions(
    abuse_signal: FraudAbuseSignal, items: Iterable[_Candidate]
) -> tuple[str, ...]:
    candidates = tuple(items)
    text = " ".join(candidate.requirement_text for candidate in candidates)
    questions: list[str] = []
    if not any(candidate.protected_flow for candidate in candidates):
        questions.append("Which user or system flow must be protected?")
    if abuse_signal in {"fraud_scoring", "velocity_limits", "payment_fraud", "chargeback_risk"} and not re.search(
        r"\b(?:threshold|limit|score|tier|amount|\d+|percent|%)\b", text, re.I
    ):
        questions.append("What threshold or limit triggers the control?")
    if abuse_signal in {"manual_review", "abuse_reporting"} and not any(candidate.review_path for candidate in candidates):
        questions.append("Who reviews the case and what queue or SLA applies?")
    if abuse_signal in {"bot_detection", "signup_abuse", "ip_reputation", "device_fingerprinting"} and not any(
        candidate.enforcement_action for candidate in candidates
    ):
        questions.append("What enforcement action should happen after detection?")
    return tuple(questions)


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    if any(isinstance(value, Mapping) for value in item.values()):
        return False
    if keys <= {"fraud", "abuse", "risk", "trust_safety", "requirements"} and any(
        isinstance(value, (Mapping, list, tuple, set)) for value in item.values()
    ):
        return False
    return bool(
        keys
        & {
            "abuse_signal",
            "fraud_scoring",
            "account_abuse",
            "signup_abuse",
            "payment_fraud",
            "suspicious_activity",
            "velocity_limits",
            "device_fingerprinting",
            "ip_reputation",
            "manual_review",
            "chargeback_risk",
            "bot_detection",
            "abuse_reporting",
            "protected_flow",
            "enforcement_action",
            "review_path",
            "threshold",
        }
    )


def _structured_segments(item: Mapping[str, Any]) -> list[str]:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(value)
        if text:
            parts.append(f"{key}: {text}")
    return ["; ".join(parts)] if parts else []


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
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, int]:
    return (
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int("[" in candidate.source_field),
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        -_source_index(candidate.source_field),
    )


def _detail(pattern: re.Pattern[str], text: str) -> str | None:
    if not (match := pattern.search(text)):
        return None
    return _clean_text(match.group(0)).casefold()


def _field_value_detail(field_name: str, text: str) -> str | None:
    pattern = re.compile(rf"\b{re.escape(field_name)}:\s*([^;]+)", re.I)
    if not (match := pattern.search(text)):
        return None
    return _clean_text(match.group(1)).casefold()


def _first_detail(values: Iterable[str | None]) -> str | None:
    for value in values:
        if value:
            return value
    return None


def _source_index(source_field: str) -> int:
    match = re.search(r"\[(\d+)\]", source_field)
    return int(match.group(1)) if match else 0


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
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(value)
    return text or None


def _requirement_text(value: str) -> str:
    text = _clean_text(value)
    if len(text) > 220:
        text = f"{text[:217].rstrip()}..."
    return text


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


def _dedupe_requirement_key(value: str, abuse_signal: FraudAbuseSignal) -> str:
    text = _clean_text(value).casefold()
    return f"{abuse_signal}:{_SPACE_RE.sub(' ', text).strip()}"


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
    "FraudAbuseConfidence",
    "FraudAbuseSignal",
    "SourceFraudAbuseRequirement",
    "SourceFraudAbuseRequirementsReport",
    "build_source_fraud_abuse_requirements",
    "derive_source_fraud_abuse_requirements",
    "extract_source_fraud_abuse_requirements",
    "generate_source_fraud_abuse_requirements",
    "source_fraud_abuse_requirements_to_dict",
    "source_fraud_abuse_requirements_to_dicts",
    "source_fraud_abuse_requirements_to_markdown",
    "summarize_source_fraud_abuse_requirements",
]
