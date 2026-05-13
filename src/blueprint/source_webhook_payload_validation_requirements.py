"""Extract source-level webhook payload validation requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping


WebhookPayloadValidationSignal = Literal[
    "schema_validation",
    "required_fields",
    "unknown_field_handling",
    "versioned_payloads",
    "signature_timestamp_coupling",
    "malformed_payload_response",
    "validation_observability",
]

_SIGNALS: tuple[WebhookPayloadValidationSignal, ...] = (
    "schema_validation",
    "required_fields",
    "unknown_field_handling",
    "versioned_payloads",
    "signature_timestamp_coupling",
    "malformed_payload_response",
    "validation_observability",
)
_SPACE_RE = re.compile(r"\s+")
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_VALIDATION_CONTEXT_RE = re.compile(
    r"\b(?:webhook payloads?|payload validation|validate payloads?|schema validation|malformed payloads?|"
    r"invalid payload|required fields?|unknown fields?|payload version)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:retry|redeliver|backoff|dead[- ]letter|signature verification only|ordering guarantee|"
    r"ordered delivery|secret rotation|signing secret)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope)\b.{0,100}\b(?:payload validation|schema validation|malformed payload|webhook payload)\b|"
    r"\b(?:payload validation|schema validation|malformed payload|webhook payload)\b.{0,100}\b(?:not required|out of scope|no work)\b",
    re.I,
)
_PATTERNS: dict[WebhookPayloadValidationSignal, re.Pattern[str]] = {
    "schema_validation": re.compile(
        r"\b(?:json schema|schema validation|validate schema|payload schema|schema registry|contract validation)\b",
        re.I,
    ),
    "required_fields": re.compile(
        r"\b(?:required fields?|mandatory fields?|must include|required properties|missing fields?|event id|event type)\b",
        re.I,
    ),
    "unknown_field_handling": re.compile(
        r"\b(?:unknown fields?|additional properties|extra fields?|unexpected fields?|ignore unknown|reject unknown|"
        r"allowlisted fields?)\b",
        re.I,
    ),
    "versioned_payloads": re.compile(
        r"\b(?:payload version|versioned payload|schema version|api version|v\d+(?:\.\d+)*|version header|"
        r"backward compatible)\b",
        re.I,
    ),
    "signature_timestamp_coupling": re.compile(
        r"\b(?:signature|signed|hmac|timestamp|timestamp header|clock skew)\b.{0,120}"
        r"\b(?:payload validation|schema validation|canonical payload|raw body|validate payload|malformed payload)\b|"
        r"\b(?:payload validation|schema validation|canonical payload|raw body|validate payload)\b.{0,120}"
        r"\b(?:signature|signed|hmac|timestamp|timestamp header|clock skew)\b",
        re.I,
    ),
    "malformed_payload_response": re.compile(
        r"\b(?:malformed payload|invalid payload|bad request|400|422|reject payload|validation error|error response|"
        r"do not process)\b",
        re.I,
    ),
    "validation_observability": re.compile(
        r"\b(?:validation metrics?|validation logs?|log validation|audit validation|invalid payload count|"
        r"malformed count|dashboard|alert|observability|trace id)\b",
        re.I,
    ),
}
_VALUE_PATTERNS: dict[WebhookPayloadValidationSignal, re.Pattern[str]] = {
    "malformed_payload_response": re.compile(r"\b(?:400|422|bad request|validation error)\b", re.I),
    "versioned_payloads": re.compile(r"\bv\d+(?:\.\d+)*\b", re.I),
}
_WEAK_RE = re.compile(r"\b(?:webhook validation|validate webhooks?|validate payloads?|payload checks?)\b", re.I)
_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "constraints",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "metadata",
    "source_payload",
)


@dataclass(frozen=True, slots=True)
class SourceWebhookPayloadValidationRequirement:
    """One extracted webhook payload validation requirement signal."""

    signal: WebhookPayloadValidationSignal
    value: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: Literal["high", "medium"] = "high"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "signal": self.signal,
            "value": self.value,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SourceWebhookPayloadValidationRequirementsReport:
    """Structured webhook payload validation findings and gaps."""

    source_id: str | None = None
    requirements: tuple[SourceWebhookPayloadValidationRequirement, ...] = field(default_factory=tuple)
    missing_signals: tuple[WebhookPayloadValidationSignal, ...] = field(default_factory=tuple)
    weak_signals: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceWebhookPayloadValidationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [requirement.to_dict() for requirement in self.records],
            "missing_signals": list(self.missing_signals),
            "weak_signals": list(self.weak_signals),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return webhook payload validation records as dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]


def build_source_webhook_payload_validation_requirements(
    source: Mapping[str, Any] | str | object,
) -> SourceWebhookPayloadValidationRequirementsReport:
    """Build a deterministic webhook payload validation report from brief-like input."""
    source_id, payload = _payload(source)
    texts = _texts(payload)
    has_context = any(
        _VALIDATION_CONTEXT_RE.search(f"{_field_words(field)} {text}") for field, text in texts
    )
    found: dict[WebhookPayloadValidationSignal, list[str]] = {signal: [] for signal in _SIGNALS}
    values: dict[WebhookPayloadValidationSignal, str] = {}
    weak: list[str] = []

    for field, text in texts:
        if _NEGATED_RE.search(text):
            continue
        searchable = f"{_field_words(field)} {text}"
        unrelated_only = _UNRELATED_RE.search(text) and not _VALIDATION_CONTEXT_RE.search(searchable)
        matches = [] if unrelated_only else [signal for signal, pattern in _PATTERNS.items() if pattern.search(searchable)]
        if matches and (has_context or _field_context(field) or len(matches) > 1):
            for signal in matches:
                found[signal].append(f"{field}: {text}")
                if signal in _VALUE_PATTERNS and signal not in values:
                    if match := _VALUE_PATTERNS[signal].search(text):
                        values[signal] = _clean(match.group(0))
        elif field not in {"id", "source_id", "title"} and _WEAK_RE.search(text):
            weak.append(f"{field}: clarify concrete webhook payload validation rule for '{text}'")

    requirements = tuple(
        SourceWebhookPayloadValidationRequirement(
            signal=signal,
            value=values.get(signal),
            evidence=tuple(_dedupe(found[signal]))[:4],
            confidence="high" if signal in values or len(found[signal]) > 1 else "medium",
        )
        for signal in _SIGNALS
        if found[signal]
    )
    missing = tuple(signal for signal in _SIGNALS if not found[signal])
    weak_signals = () if not missing else tuple(_dedupe(weak))
    return SourceWebhookPayloadValidationRequirementsReport(
        source_id=source_id,
        requirements=requirements,
        missing_signals=missing,
        weak_signals=weak_signals,
        summary={
            "requirement_count": len(requirements),
            "missing_count": len(missing),
            "weak_count": len(weak_signals),
            "signals": [requirement.signal for requirement in requirements],
            "signal_counts": {signal: int(bool(found[signal])) for signal in _SIGNALS},
        },
    )


def derive_source_webhook_payload_validation_requirements(
    source: Mapping[str, Any] | str | object,
) -> SourceWebhookPayloadValidationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_webhook_payload_validation_requirements(source)


def generate_source_webhook_payload_validation_requirements(
    source: Mapping[str, Any] | str | object,
) -> SourceWebhookPayloadValidationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_webhook_payload_validation_requirements(source)


def extract_source_webhook_payload_validation_requirements(
    source: Mapping[str, Any] | str | object,
) -> tuple[SourceWebhookPayloadValidationRequirement, ...]:
    """Return extracted webhook payload validation records."""
    return build_source_webhook_payload_validation_requirements(source).requirements


def summarize_source_webhook_payload_validation_requirements(
    source: Mapping[str, Any] | SourceWebhookPayloadValidationRequirementsReport | str | object,
) -> dict[str, Any]:
    """Return deterministic counts for webhook payload validation requirements."""
    if isinstance(source, SourceWebhookPayloadValidationRequirementsReport):
        return dict(source.summary)
    return build_source_webhook_payload_validation_requirements(source).summary


def source_webhook_payload_validation_requirements_to_dict(
    report: SourceWebhookPayloadValidationRequirementsReport,
) -> dict[str, Any]:
    """Serialize a webhook payload validation requirements report."""
    return report.to_dict()


source_webhook_payload_validation_requirements_to_dict.__test__ = False


def source_webhook_payload_validation_requirements_to_dicts(
    requirements: (
        tuple[SourceWebhookPayloadValidationRequirement, ...]
        | list[SourceWebhookPayloadValidationRequirement]
        | SourceWebhookPayloadValidationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize webhook payload validation records to dictionaries."""
    if isinstance(requirements, SourceWebhookPayloadValidationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_webhook_payload_validation_requirements_to_dicts.__test__ = False


def _payload(source: Mapping[str, Any] | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
    elif isinstance(source, Mapping):
        payload = dict(source)
    else:
        payload = {
            key: getattr(source, key)
            for key in dir(source)
            if not key.startswith("_") and not callable(getattr(source, key))
        }
    return _optional(payload.get("id")) or _optional(payload.get("source_id")), payload


def _texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    seen: set[str] = set()
    for field in _FIELDS:
        if field in payload:
            _append(texts, field, payload[field])
            seen.add(field)
    for field in sorted(payload):
        if field not in seen:
            _append(texts, str(field), payload[field])
    return texts


def _append(texts: list[tuple[str, str]], field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value):
            _append(texts, f"{field}.{key}", value[key])
    elif isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append(texts, f"{field}[{index}]", item)
    elif text := _optional(value):
        for segment in _SENTENCE_RE.split(text):
            cleaned = _clean(segment)
            if cleaned:
                texts.append((field, cleaned))


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _field_context(field: str) -> bool:
    return bool(re.search(r"\b(?:webhook|payload|validation|schema|source payload)\b", _field_words(field), re.I))


def _optional(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean(str(value))
    return text or None


def _clean(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip(" -\t\r\n.")


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.casefold()
        if key not in seen:
            seen.add(key)
            result.append(value)
    return result


__all__ = [
    "WebhookPayloadValidationSignal",
    "SourceWebhookPayloadValidationRequirement",
    "SourceWebhookPayloadValidationRequirementsReport",
    "build_source_webhook_payload_validation_requirements",
    "derive_source_webhook_payload_validation_requirements",
    "extract_source_webhook_payload_validation_requirements",
    "generate_source_webhook_payload_validation_requirements",
    "source_webhook_payload_validation_requirements_to_dict",
    "source_webhook_payload_validation_requirements_to_dicts",
    "summarize_source_webhook_payload_validation_requirements",
]
