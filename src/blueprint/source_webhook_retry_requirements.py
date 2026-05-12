"""Extract source-level webhook retry requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


WebhookRetryRequirementType = Literal[
    "retry_policy",
    "max_attempts",
    "backoff_strategy",
    "idempotency_key",
    "dead_letter_handling",
    "delivery_status_visibility",
    "replay_window",
    "ordering_caveat",
    "provider_specific_evidence",
]
WebhookRetryConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[WebhookRetryRequirementType, ...] = (
    "retry_policy",
    "max_attempts",
    "backoff_strategy",
    "idempotency_key",
    "dead_letter_handling",
    "delivery_status_visibility",
    "replay_window",
    "ordering_caveat",
    "provider_specific_evidence",
)
_CONFIDENCE_ORDER: dict[WebhookRetryConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_WEBHOOK_CONTEXT_RE = re.compile(r"\b(?:webhooks?|webhook deliveries?|webhook events?|delivery attempts?)\b", re.I)
_NO_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:webhook|delivery|retry|replay)\b.{0,80}"
    r"\b(?:scope|required|needed|changes?|impact)\b",
    re.I,
)
_UNRELATED_RE = re.compile(r"\b(?:retry button|replay video|status page redesign only|email delivery)\b", re.I)
_SCANNED_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "constraints",
    "risks",
    "integration_points",
    "integrations",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_TYPE_PATTERNS: dict[WebhookRetryRequirementType, re.Pattern[str]] = {
    "retry_policy": re.compile(r"\b(?:retry policy|retry schedule|redeliver|delivery retry|automatic retries?|webhook retries?)\b", re.I),
    "max_attempts": re.compile(r"\b(?:max(?:imum)? attempts?|max(?:imum)? retries|retry limit|attempt limit|give up after|stop after|\d+\s+(?:attempts?|retries))\b", re.I),
    "backoff_strategy": re.compile(r"\b(?:backoff|exponential delay|progressive delay|jitter|retry interval|retry cadence)\b", re.I),
    "idempotency_key": re.compile(r"\b(?:idempotency key|idempotency token|idempotent|delivery id|event id|dedupe|duplicate delivery)\b", re.I),
    "dead_letter_handling": re.compile(r"\b(?:dead[- ]letter|dlq|failed delivery queue|poison|undeliverable|parking lot|failure queue)\b", re.I),
    "delivery_status_visibility": re.compile(r"\b(?:delivery status|delivery log|delivery history|attempt history|webhook status|success and failure|observability|dashboard|alert)\b", re.I),
    "replay_window": re.compile(r"\b(?:replay window|manual replay|redelivery window|replay for|replay within|replay retention|resend webhook|retry from history)\b", re.I),
    "ordering_caveat": re.compile(r"\b(?:ordering caveat|ordered delivery|out of order|order(?:ing)? is not guaranteed|ordering not guaranteed|fifo|sequence)\b", re.I),
    "provider_specific_evidence": re.compile(r"\b(?:stripe|github|shopify|slack|twilio|sendgrid|adyen|checkout\.com|webhook provider|provider-specific|vendor)\b", re.I),
}
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:attempts?|retries|seconds?|minutes?|hours?|days?)|"
    r"exponential backoff|linear backoff|full jitter|dlq|dead[- ]letter|"
    r"idempotency key|delivery id|event id|stripe|github|shopify|slack|twilio)\b",
    re.I,
)
_OWNER_SUGGESTIONS: dict[WebhookRetryRequirementType, tuple[str, ...]] = {
    "retry_policy": ("integrations", "platform"),
    "max_attempts": ("integrations", "platform"),
    "backoff_strategy": ("platform", "sre"),
    "idempotency_key": ("integrations", "data_platform"),
    "dead_letter_handling": ("platform", "sre"),
    "delivery_status_visibility": ("integrations", "support"),
    "replay_window": ("integrations", "support"),
    "ordering_caveat": ("integrations", "product"),
    "provider_specific_evidence": ("integrations", "partnerships"),
}
_NOTES: dict[WebhookRetryRequirementType, str] = {
    "retry_policy": "Define which webhook delivery failures are retried and which are terminal.",
    "max_attempts": "Specify the maximum delivery attempts before the endpoint is considered failed.",
    "backoff_strategy": "Capture retry delay, backoff, jitter, and maximum elapsed retry duration.",
    "idempotency_key": "Require stable delivery or event identifiers so receivers can dedupe retries.",
    "dead_letter_handling": "Define where exhausted deliveries go and how operators triage them.",
    "delivery_status_visibility": "Expose delivery attempt status, failure reason, and timestamps for support.",
    "replay_window": "Define the retention window and authorization model for manual webhook replay.",
    "ordering_caveat": "Document whether retry and replay can deliver events out of order.",
    "provider_specific_evidence": "Preserve provider-specific retry semantics as implementation evidence.",
}


@dataclass(frozen=True, slots=True)
class SourceWebhookRetryRequirement:
    """One source-backed webhook retry requirement."""

    requirement_type: WebhookRetryRequirementType
    source_field: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: WebhookRetryConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def category(self) -> WebhookRetryRequirementType:
        return self.requirement_type

    @property
    def requirement_category(self) -> WebhookRetryRequirementType:
        return self.requirement_type

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        return {
            "requirement_type": self.requirement_type,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "planning_notes": list(self.planning_notes),
        }


@dataclass(frozen=True, slots=True)
class SourceWebhookRetryRequirementsReport:
    """Source-level webhook retry requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceWebhookRetryRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceWebhookRetryRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceWebhookRetryRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [item.to_dict() for item in self.requirements],
            "records": [item.to_dict() for item in self.records],
            "findings": [item.to_dict() for item in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source Webhook Retry Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Delivery reliability coverage: {self.summary.get('delivery_reliability_coverage', 0)}%",
            f"- Replay coverage: {self.summary.get('replay_coverage', 0)}%",
            f"- Observability coverage: {self.summary.get('observability_coverage', 0)}%",
        ]
        if not self.requirements:
            lines.extend(["", "No webhook retry requirements were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Type | Confidence | Source | Evidence |", "| --- | --- | --- | --- |"])
        for item in self.requirements:
            lines.append(
                f"| {item.requirement_type} | {item.confidence} | {_markdown_cell(item.source_field)} | "
                f"{_markdown_cell('; '.join(item.evidence))} |"
            )
        return "\n".join(lines)


def build_source_webhook_retry_requirements(source: Any) -> SourceWebhookRetryRequirementsReport:
    """Extract webhook retry requirement records from SourceBrief-shaped input."""
    brief_id, title, payload = _source_payload(source)
    grouped = _group_candidates(payload)
    requirements = tuple(_merge(grouped))
    return SourceWebhookRetryRequirementsReport(
        brief_id=brief_id,
        title=title,
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_webhook_retry_requirements(source: Any) -> tuple[SourceWebhookRetryRequirement, ...]:
    return build_source_webhook_retry_requirements(source).requirements


def derive_source_webhook_retry_requirements(source: Any) -> SourceWebhookRetryRequirementsReport:
    return build_source_webhook_retry_requirements(source)


def generate_source_webhook_retry_requirements(source: Any) -> SourceWebhookRetryRequirementsReport:
    return build_source_webhook_retry_requirements(source)


def summarize_source_webhook_retry_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceWebhookRetryRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_webhook_retry_requirements(source_or_report).summary


def source_webhook_retry_requirements_to_dict(report: SourceWebhookRetryRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_webhook_retry_requirements_to_dict.__test__ = False


def source_webhook_retry_requirements_to_dicts(items: Any) -> list[dict[str, Any]]:
    if isinstance(items, SourceWebhookRetryRequirementsReport):
        return items.to_dicts()
    return [item.to_dict() for item in items]


source_webhook_retry_requirements_to_dicts.__test__ = False


def source_webhook_retry_requirements_to_markdown(report: SourceWebhookRetryRequirementsReport) -> str:
    return report.to_markdown()


source_webhook_retry_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: WebhookRetryRequirementType
    source_field: str
    evidence: str
    confidence: WebhookRetryConfidence
    value: str | None


def _source_payload(source: Any) -> tuple[str | None, str | None, Mapping[str, Any]]:
    if isinstance(source, str):
        return None, None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _brief_id(payload), _optional_text(payload.get("title")), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _brief_id(payload), _optional_text(payload.get("title")), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            try:
                payload = dict(ImplementationBrief.model_validate(source).model_dump(mode="python"))
            except (TypeError, ValueError, ValidationError):
                payload = dict(source)
        return _brief_id(payload), _optional_text(payload.get("title")), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = {name: getattr(source, name) for name in _SCANNED_FIELDS + ("id", "source_id", "source_brief_id") if hasattr(source, name)}
        return _brief_id(payload), _optional_text(payload.get("title")), payload
    return None, None, {}


def _group_candidates(payload: Mapping[str, Any]) -> dict[WebhookRetryRequirementType, list[_Candidate]]:
    grouped: dict[WebhookRetryRequirementType, list[_Candidate]] = {}
    for field, text in _candidate_texts(payload):
        for segment in _segments(text):
            if _NO_SCOPE_RE.search(segment) or _UNRELATED_RE.search(segment):
                continue
            matched_types = [name for name in _TYPE_ORDER if _TYPE_PATTERNS[name].search(segment)]
            if not matched_types:
                continue
            confidence: WebhookRetryConfidence = "high" if _WEBHOOK_CONTEXT_RE.search(segment) else "medium"
            for requirement_type in matched_types:
                candidate = _Candidate(
                    requirement_type=requirement_type,
                    source_field=field,
                    evidence=_evidence(field, segment),
                    confidence=confidence,
                    value=_value(segment),
                )
                grouped.setdefault(requirement_type, []).append(candidate)
    return grouped


def _merge(grouped: Mapping[WebhookRetryRequirementType, list[_Candidate]]) -> list[SourceWebhookRetryRequirement]:
    records: list[SourceWebhookRetryRequirement] = []
    for requirement_type in _TYPE_ORDER:
        candidates = grouped.get(requirement_type, [])
        if not candidates:
            continue
        evidence = tuple(_dedupe_evidence(candidate.evidence for candidate in candidates))[:5]
        fields = sorted(_dedupe(candidate.source_field for candidate in candidates), key=str.casefold)
        confidence = sorted((candidate.confidence for candidate in candidates), key=lambda item: _CONFIDENCE_ORDER[item])[0]
        values = _dedupe(candidate.value for candidate in candidates if candidate.value)
        records.append(
            SourceWebhookRetryRequirement(
                requirement_type=requirement_type,
                source_field=fields[0],
                evidence=evidence,
                confidence=confidence,
                value=values[0] if values else None,
                suggested_owners=_OWNER_SUGGESTIONS[requirement_type],
                planning_notes=(_NOTES[requirement_type],),
            )
        )
    return records


def _candidate_texts(payload: Mapping[str, Any], prefix: str | None = None) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for key in sorted(payload, key=str):
        if prefix is None and key not in _SCANNED_FIELDS:
            continue
        value = payload[key]
        field = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, str):
            text = _clean(value)
            if text:
                texts.append((field, text))
        elif isinstance(value, Mapping):
            texts.extend(_candidate_texts(value, field))
        elif isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
            for index, item in enumerate(value):
                if isinstance(item, Mapping):
                    texts.extend(_candidate_texts(item, f"{field}[{index}]"))
                else:
                    text = _optional_text(item)
                    if text:
                        texts.append((f"{field}[{index}]", text))
    return texts


def _segments(text: str) -> list[str]:
    return [cleaned for part in _SPLIT_RE.split(text) if (cleaned := _clean(part))]


def _summary(requirements: tuple[SourceWebhookRetryRequirement, ...]) -> dict[str, Any]:
    types = {item.requirement_type for item in requirements}
    reliability = {"retry_policy", "max_attempts", "backoff_strategy", "idempotency_key", "dead_letter_handling"}
    replay = {"replay_window", "ordering_caveat"}
    observability = {"delivery_status_visibility", "provider_specific_evidence"}
    return {
        "requirement_count": len(requirements),
        "requirement_types": [item.requirement_type for item in requirements],
        "type_counts": {name: sum(1 for item in requirements if item.requirement_type == name) for name in _TYPE_ORDER},
        "confidence_counts": {name: sum(1 for item in requirements if item.confidence == name) for name in _CONFIDENCE_ORDER},
        "delivery_reliability_coverage": int(100 * len(types & reliability) / len(reliability)),
        "replay_coverage": int(100 * len(types & replay) / len(replay)),
        "observability_coverage": int(100 * len(types & observability) / len(observability)),
        "status": "ready_for_planning" if requirements else "no_webhook_retry_language",
    }


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return _optional_text(payload.get("id") or payload.get("source_brief_id") or payload.get("source_id"))


def _value(text: str) -> str | None:
    match = _VALUE_RE.search(text)
    return _clean(match.group(0)).casefold() if match else None


def _evidence(field: str, text: str) -> str:
    cleaned = _clean(text)
    if len(cleaned) > 220:
        cleaned = f"{cleaned[:217].rstrip()}..."
    return f"{field}: {cleaned}"


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean(str(value))
    return text or None


def _clean(value: str) -> str:
    text = _BULLET_RE.sub("", str(value).strip())
    return _SPACE_RE.sub(" ", text).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    seen: set[str] = set()
    result: list[_T] = []
    for value in values:
        key = str(value).casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        _, _, statement = value.partition(": ")
        key = _clean(statement or value).casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return sorted(result, key=str.casefold)


def _markdown_cell(value: str) -> str:
    return _clean(value).replace("|", "\\|").replace("\n", " ")


__all__ = [
    "SourceWebhookRetryRequirement",
    "SourceWebhookRetryRequirementsReport",
    "build_source_webhook_retry_requirements",
    "derive_source_webhook_retry_requirements",
    "extract_source_webhook_retry_requirements",
    "generate_source_webhook_retry_requirements",
    "source_webhook_retry_requirements_to_dict",
    "source_webhook_retry_requirements_to_dicts",
    "source_webhook_retry_requirements_to_markdown",
    "summarize_source_webhook_retry_requirements",
]
