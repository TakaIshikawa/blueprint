"""Extract source-level fraud prevention and abuse-risk requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


FraudPreventionRequirementCategory = Literal[
    "suspicious_activity_detection",
    "velocity_limits",
    "payment_fraud",
    "account_takeover",
    "manual_review",
    "risk_scoring",
    "device_fingerprint",
    "chargeback_monitoring",
    "enforcement_actions",
]
FraudPreventionConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[FraudPreventionRequirementCategory, ...] = (
    "suspicious_activity_detection",
    "velocity_limits",
    "payment_fraud",
    "account_takeover",
    "manual_review",
    "risk_scoring",
    "device_fingerprint",
    "chargeback_monitoring",
    "enforcement_actions",
)
_CONFIDENCE_ORDER: dict[FraudPreventionConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"prevent|detect|flag|block|limit|review|monitor|score|challenge|suspend|ban|"
    r"hold|quarantine|investigate|escalate|verify|protect)\b",
    re.I,
)
_FRAUD_CONTEXT_RE = re.compile(
    r"\b(?:fraud|fraudulent|abuse|abusive|risk|trust and safety|trust\s*&\s*safety|"
    r"suspicious|anomal(?:y|ies|ous)|bot|spam|scam|ATO|account takeover|credential stuffing|"
    r"chargeback|dispute|payment risk|payment fraud|card testing|stolen card|"
    r"manual review|review queue|risk score|risk scoring|device fingerprint|fingerprint|"
    r"velocity|rate limit|rate limiting|signup limit|purchase limit|transaction limit|"
    r"IP reputation|device reputation|enforcement|suspend|ban|blocklist|blacklist|denylist|"
    r"account lock|step-up|MFA challenge|2FA challenge)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:fraud|abuse|risk|security|compliance|trust|safety|payment|billing|signup|"
    r"marketplace|account|identity|verification|chargeback|dispute|review|"
    r"requirements?|constraints?|acceptance|criteria|metadata|signals?)",
    re.I,
)
_CATEGORY_PATTERNS: dict[FraudPreventionRequirementCategory, re.Pattern[str]] = {
    "suspicious_activity_detection": re.compile(
        r"\b(?:suspicious activit(?:y|ies)|suspicious behavior|suspicious login|"
        r"fraud detection|abuse detection|detect(?:ing)? abuse|anomal(?:y|ies|ous)|"
        r"unusual activity|bot detection|spam detection|scam detection)\b",
        re.I,
    ),
    "velocity_limits": re.compile(
        r"\b(?:velocity limits?|velocity checks?|rate limits?|rate limiting|"
        r"attempt limits?|signup limits?|purchase limits?|transaction limits?|"
        r"per[- ](?:account|user|card|device|ip)|burst limits?|throttle|throttling)\b",
        re.I,
    ),
    "payment_fraud": re.compile(
        r"\b(?:payment fraud|fraudulent payments?|card testing|stolen cards?|"
        r"stolen payment|payment risk|billing fraud|transaction fraud|"
        r"payment abuse|unauthorized transactions?|high[- ]risk payments?)\b",
        re.I,
    ),
    "account_takeover": re.compile(
        r"\b(?:account takeover|ATO|credential stuffing|compromised accounts?|"
        r"account compromise|suspicious login|login risk|session hijack|"
        r"unauthorized account access|account lock(?:out)?)\b",
        re.I,
    ),
    "manual_review": re.compile(
        r"\b(?:manual review|human review|review queue|case review|investigation queue|"
        r"risk review|fraud review|analyst review|escalat(?:e|ion) to review)\b",
        re.I,
    ),
    "risk_scoring": re.compile(
        r"\b(?:risk scor(?:e|ing)|risk model|risk rating|risk tier|risk level|"
        r"risk threshold|fraud score|abuse score|confidence score)\b",
        re.I,
    ),
    "device_fingerprint": re.compile(
        r"\b(?:device fingerprint(?:ing)?|browser fingerprint(?:ing)?|device reputation|"
        r"IP reputation|ip reputation|device signals?|fingerprint signals?|"
        r"device intelligence|known device|new device)\b",
        re.I,
    ),
    "chargeback_monitoring": re.compile(
        r"\b(?:chargeback(?:s)?|chargeback monitoring|dispute monitoring|payment disputes?|"
        r"dispute rate|chargeback rate|retrieval requests?|representment)\b",
        re.I,
    ),
    "enforcement_actions": re.compile(
        r"\b(?:enforcement actions?|suspend(?:ed)? accounts?|account suspension|ban(?:ned)?|"
        r"block(?:ed)? accounts?|blocklist|blacklist|denylist|freeze accounts?|"
        r"hold payouts?|withhold payouts?|disable checkout|lock accounts?|step-up challenge|"
        r"MFA challenge|2FA challenge|captcha challenge)\b",
        re.I,
    ),
}
_OWNER_HINTS: dict[FraudPreventionRequirementCategory, str] = {
    "suspicious_activity_detection": "Trust and Safety",
    "velocity_limits": "Platform Security",
    "payment_fraud": "Payments Risk",
    "account_takeover": "Identity Security",
    "manual_review": "Risk Operations",
    "risk_scoring": "Risk Engineering",
    "device_fingerprint": "Fraud Platform",
    "chargeback_monitoring": "Payments Operations",
    "enforcement_actions": "Trust and Safety Operations",
}
_PLANNING_NOTES: dict[FraudPreventionRequirementCategory, str] = {
    "suspicious_activity_detection": "Define detection signals, alert thresholds, triage workflow, and false-positive handling.",
    "velocity_limits": "Specify counters, time windows, identities, exemptions, and user-facing limit behavior.",
    "payment_fraud": "Define risky payment signals, decisioning, processor integration, and audit evidence.",
    "account_takeover": "Define takeover signals, step-up verification, account recovery, and customer notification.",
    "manual_review": "Define queue routing, reviewer permissions, SLAs, evidence capture, and decision outcomes.",
    "risk_scoring": "Define score inputs, thresholds, overrides, explainability, and monitoring for drift.",
    "device_fingerprint": "Define allowed device signals, privacy constraints, retention, and matching behavior.",
    "chargeback_monitoring": "Define dispute ingestion, chargeback thresholds, reporting, and owner handoffs.",
    "enforcement_actions": "Define allowed actions, appeal paths, audit logging, and policy ownership.",
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
class SourceFraudPreventionRequirement:
    """One source-backed fraud prevention or abuse-risk requirement."""

    requirement_category: FraudPreventionRequirementCategory
    value: str | None = None
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: FraudPreventionConfidence = "medium"
    suggested_owner: str = ""
    planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_category": self.requirement_category,
            "value": self.value,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "suggested_owner": self.suggested_owner,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceFraudPreventionRequirementsReport:
    """Source-level fraud prevention and abuse-risk requirements report."""

    source_brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceFraudPreventionRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceFraudPreventionRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return fraud prevention requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Fraud Prevention Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        category_counts = self.summary.get("requirement_category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement category counts: "
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source fraud prevention requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Confidence | Owner | Source Field | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_category} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.suggested_owner)} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} |"
            )
        return "\n".join(lines)


def build_source_fraud_prevention_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceFraudPreventionRequirementsReport:
    """Build a fraud prevention requirements report from a source brief-like payload."""
    source_brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceFraudPreventionRequirementsReport(
        source_brief_id=source_brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_fraud_prevention_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceFraudPreventionRequirementsReport:
    """Compatibility helper for callers that use summarize_* naming."""
    return build_source_fraud_prevention_requirements(source)


def derive_source_fraud_prevention_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceFraudPreventionRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_fraud_prevention_requirements(source)


def generate_source_fraud_prevention_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceFraudPreventionRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_fraud_prevention_requirements(source)


def extract_source_fraud_prevention_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceFraudPreventionRequirement, ...]:
    """Return fraud prevention requirement records from brief-shaped input."""
    return build_source_fraud_prevention_requirements(source).requirements


def source_fraud_prevention_requirements_to_dict(
    report: SourceFraudPreventionRequirementsReport,
) -> dict[str, Any]:
    """Serialize a fraud prevention requirements report to a plain dictionary."""
    return report.to_dict()


source_fraud_prevention_requirements_to_dict.__test__ = False


def source_fraud_prevention_requirements_to_dicts(
    requirements: (
        tuple[SourceFraudPreventionRequirement, ...]
        | list[SourceFraudPreventionRequirement]
        | SourceFraudPreventionRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize fraud prevention requirement records to dictionaries."""
    if isinstance(requirements, SourceFraudPreventionRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_fraud_prevention_requirements_to_dicts.__test__ = False


def source_fraud_prevention_requirements_to_markdown(
    report: SourceFraudPreventionRequirementsReport,
) -> str:
    """Render a fraud prevention requirements report as Markdown."""
    return report.to_markdown()


source_fraud_prevention_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_category: FraudPreventionRequirementCategory
    value: str | None
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: FraudPreventionConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object,
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


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_field, segment in _candidate_segments(payload):
        if not _is_fraud_signal(source_field, segment):
            continue
        categories = _requirement_categories(source_field, segment)
        if not categories:
            continue
        if source_field == "title" and not _REQUIRED_RE.search(segment):
            continue
        evidence = _evidence_snippet(source_field, segment)
        for category in categories:
            candidates.append(
                _Candidate(
                    requirement_category=category,
                    value=_value(segment),
                    source_field=source_field,
                    evidence=evidence,
                    matched_terms=_matched_terms(category, source_field, segment),
                    confidence=_confidence(source_field, segment),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceFraudPreventionRequirement]:
    grouped: dict[FraudPreventionRequirementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.requirement_category, []).append(candidate)

    requirements: list[SourceFraudPreventionRequirement] = []
    for category, items in grouped.items():
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda item: item.casefold(),
        )[0]
        confidence = min(
            (item.confidence for item in items), key=lambda item: _CONFIDENCE_ORDER[item]
        )
        requirements.append(
            SourceFraudPreventionRequirement(
                requirement_category=category,
                value=_best_value(items),
                source_field=source_field,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                matched_terms=tuple(
                    sorted(
                        _dedupe(term for item in items for term in item.matched_terms),
                        key=str.casefold,
                    )
                ),
                confidence=confidence,
                suggested_owner=_OWNER_HINTS[category],
                planning_note=_PLANNING_NOTES[category],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CONFIDENCE_ORDER[requirement.confidence],
            _CATEGORY_ORDER.index(requirement.requirement_category),
            requirement.value or "",
            requirement.source_field.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key], False)
    return [(field, segment) for field, segment in values if segment]


def _append_value(
    values: list[tuple[str, str]],
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
                _STRUCTURED_FIELD_RE.search(key_text) or _FRAUD_CONTEXT_RE.search(key_text)
            )
            if child_context and _FRAUD_CONTEXT_RE.search(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            if field_context or _FRAUD_CONTEXT_RE.search(segment):
                values.append((source_field, segment))


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
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _is_fraud_signal(source_field: str, text: str) -> bool:
    searchable = f"{_field_words(source_field)} {text}"
    if not _FRAUD_CONTEXT_RE.search(searchable):
        return False
    if any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values()):
        return True
    return bool(
        _REQUIRED_RE.search(text) and _STRUCTURED_FIELD_RE.search(_field_words(source_field))
    )


def _requirement_categories(
    source_field: str,
    text: str,
) -> tuple[FraudPreventionRequirementCategory, ...]:
    searchable = f"{_field_words(source_field)} {text}"
    categories = [
        category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)
    ]
    return tuple(_dedupe(categories))


def _value(text: str) -> str | None:
    if match := re.search(
        r"\b(?P<value>(?:within|under|less than|no more than|up to|at least|after)?\s*"
        r"\d+(?:\.\d+)?\s*(?:signup attempts?|login attempts?|payment attempts?|"
        r"purchase attempts?|transactions?|orders?|attempts?|signups?|accounts?|cards?|"
        r"devices?|ips?|minutes?|hours?|days?|%|percent))\b",
        text,
        re.I,
    ):
        return _clean_text(match.group("value"))
    if match := re.search(r"\b(?P<value>(?:high|medium|low)[- ]risk)\b", text, re.I):
        return _clean_text(match.group("value"))
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (0 if re.search(r"\d", value) else 1, len(value), value.casefold()),
    )
    return values[0] if values else None


def _matched_terms(
    category: FraudPreventionRequirementCategory,
    source_field: str,
    text: str,
) -> tuple[str, ...]:
    searchable = f"{_field_words(source_field)} {text}"
    return tuple(
        _dedupe(
            _clean_text(match.group(0))
            for match in _CATEGORY_PATTERNS[category].finditer(searchable)
        )
    )


def _confidence(source_field: str, text: str) -> FraudPreventionConfidence:
    normalized_field = source_field.replace("-", "_").casefold()
    if _REQUIRED_RE.search(text) and any(
        marker in normalized_field
        for marker in (
            "acceptance_criteria",
            "success_criteria",
            "security",
            "compliance",
            "risk",
            "fraud",
            "abuse",
        )
    ):
        return "high"
    if _REQUIRED_RE.search(text):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceFraudPreventionRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "requirement_categories": [
            requirement.requirement_category for requirement in requirements
        ],
        "requirement_category_counts": {
            category: sum(
                1 for requirement in requirements if requirement.requirement_category == category
            )
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "status": "ready_for_planning" if requirements else "no_fraud_prevention_language",
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
        "operations",
        "support",
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
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


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
    return sorted(deduped, key=lambda item: item.casefold())


def _dedupe(values: Iterable[Any]) -> list[Any]:
    deduped: list[Any] = []
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
    "FraudPreventionConfidence",
    "FraudPreventionRequirementCategory",
    "SourceFraudPreventionRequirement",
    "SourceFraudPreventionRequirementsReport",
    "build_source_fraud_prevention_requirements",
    "derive_source_fraud_prevention_requirements",
    "extract_source_fraud_prevention_requirements",
    "generate_source_fraud_prevention_requirements",
    "summarize_source_fraud_prevention_requirements",
    "source_fraud_prevention_requirements_to_dict",
    "source_fraud_prevention_requirements_to_dicts",
    "source_fraud_prevention_requirements_to_markdown",
]
