"""Extract source-level service decommission requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping


ServiceDecommissionSignal = Literal[
    "consumer_inventory",
    "traffic_drain",
    "data_archival_deletion",
    "dependency_removal",
    "communication",
    "monitoring_after_shutdown",
    "rollback_window",
    "ownership",
]

_SIGNALS: tuple[ServiceDecommissionSignal, ...] = (
    "consumer_inventory",
    "traffic_drain",
    "data_archival_deletion",
    "dependency_removal",
    "communication",
    "monitoring_after_shutdown",
    "rollback_window",
    "ownership",
)
_SPACE_RE = re.compile(r"\s+")
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_DECOM_CONTEXT_RE = re.compile(
    r"\b(?:decommission|deprecate|shutdown|shut down|retire|sunset|turn off|remove service|end of life|eol)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope)\b.{0,100}\b(?:decommission|shutdown|retire|sunset|end of life|eol)\b|"
    r"\b(?:decommission|shutdown|retire|sunset|end of life|eol)\b.{0,100}\b(?:not required|out of scope|no work)\b",
    re.I,
)
_PATTERNS: dict[ServiceDecommissionSignal, re.Pattern[str]] = {
    "consumer_inventory": re.compile(
        r"\b(?:consumer inventory|known consumers?|clients?|callers?|downstream|integrations?|usage inventory|service owners?)\b",
        re.I,
    ),
    "traffic_drain": re.compile(
        r"\b(?:traffic drain|drain traffic|route traffic|reroute|redirect|cut traffic|stop sending|zero traffic|"
        r"disable ingress|remove routing)\b",
        re.I,
    ),
    "data_archival_deletion": re.compile(
        r"\b(?:archive|archival|delete data|data deletion|purge|retention|backup|snapshot|export data|customer data)\b",
        re.I,
    ),
    "dependency_removal": re.compile(
        r"\b(?:dependency removal|remove dependencies|dependency graph|configs?|feature flags?|cron|jobs?|"
        r"dashboards?|runbooks?|dns|terraform|iam|secrets?)\b",
        re.I,
    ),
    "communication": re.compile(
        r"\b(?:communicat|notify|announcement|migration notice|stakeholders?|customers?|support|status page|email)\b",
        re.I,
    ),
    "monitoring_after_shutdown": re.compile(
        r"\b(?:monitor|monitoring|after shutdown|post[- ]shutdown|after decommission|watch|alerts?|logs?|"
        r"error rate|unexpected traffic)\b",
        re.I,
    ),
    "rollback_window": re.compile(
        r"\b(?:rollback|roll back|restore|re-enable|reenable|backout|back out|recovery window|rollback window)\b",
        re.I,
    ),
    "ownership": re.compile(
        r"\b(?:owner|owned by|responsible team|accountable|approver|escalation|on-call|pagerduty|slack channel)\b",
        re.I,
    ),
}
_VALUE_PATTERNS: dict[ServiceDecommissionSignal, re.Pattern[str]] = {
    "rollback_window": re.compile(r"\b\d+\s*(?:hour|day|week)s?\b", re.I),
    "traffic_drain": re.compile(r"\b(?:zero traffic|\d+\s*%|all traffic|no traffic)\b", re.I),
}
_WEAK_RE = re.compile(r"\b(?:decommission|shutdown|retire|sunset|turn off|remove service)\b", re.I)
_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "constraints",
    "acceptance_criteria",
    "definition_of_done",
    "risks",
    "metadata",
    "source_payload",
)


@dataclass(frozen=True, slots=True)
class SourceServiceDecommissionRequirement:
    """One extracted service decommission requirement signal."""

    signal: ServiceDecommissionSignal
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
class SourceServiceDecommissionRequirementsReport:
    """Structured service decommission findings and gaps."""

    source_id: str | None = None
    requirements: tuple[SourceServiceDecommissionRequirement, ...] = field(default_factory=tuple)
    missing_signals: tuple[ServiceDecommissionSignal, ...] = field(default_factory=tuple)
    weak_signals: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceServiceDecommissionRequirement, ...]:
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
        """Return service decommission records as dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]


def build_source_service_decommission_requirements(
    source: Mapping[str, Any] | str | object,
) -> SourceServiceDecommissionRequirementsReport:
    """Build a deterministic service decommission requirements report from brief-like input."""
    source_id, payload = _payload(source)
    texts = _texts(payload)
    has_context = any(_DECOM_CONTEXT_RE.search(f"{_field_words(field)} {text}") for field, text in texts)
    found: dict[ServiceDecommissionSignal, list[str]] = {signal: [] for signal in _SIGNALS}
    values: dict[ServiceDecommissionSignal, str] = {}
    weak: list[str] = []

    for field, text in texts:
        if _NEGATED_RE.search(text):
            continue
        searchable = f"{_field_words(field)} {text}"
        matches = [signal for signal, pattern in _PATTERNS.items() if pattern.search(searchable)]
        if matches and (has_context or _field_context(field) or len(matches) > 1):
            for signal in matches:
                found[signal].append(f"{field}: {text}")
                if signal in _VALUE_PATTERNS and signal not in values:
                    if match := _VALUE_PATTERNS[signal].search(text):
                        values[signal] = _clean(match.group(0))
        elif field not in {"id", "source_id", "title"} and _WEAK_RE.search(text):
            weak.append(f"{field}: clarify concrete decommission rule for '{text}'")

    requirements = tuple(
        SourceServiceDecommissionRequirement(
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
    return SourceServiceDecommissionRequirementsReport(
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


def derive_source_service_decommission_requirements(
    source: Mapping[str, Any] | str | object,
) -> SourceServiceDecommissionRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_service_decommission_requirements(source)


def generate_source_service_decommission_requirements(
    source: Mapping[str, Any] | str | object,
) -> SourceServiceDecommissionRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_service_decommission_requirements(source)


def extract_source_service_decommission_requirements(
    source: Mapping[str, Any] | str | object,
) -> tuple[SourceServiceDecommissionRequirement, ...]:
    """Return extracted service decommission requirement records."""
    return build_source_service_decommission_requirements(source).requirements


def summarize_source_service_decommission_requirements(
    source: Mapping[str, Any] | SourceServiceDecommissionRequirementsReport | str | object,
) -> dict[str, Any]:
    """Return deterministic counts for service decommission requirements."""
    if isinstance(source, SourceServiceDecommissionRequirementsReport):
        return dict(source.summary)
    return build_source_service_decommission_requirements(source).summary


def source_service_decommission_requirements_to_dict(
    report: SourceServiceDecommissionRequirementsReport,
) -> dict[str, Any]:
    """Serialize a service decommission requirements report."""
    return report.to_dict()


source_service_decommission_requirements_to_dict.__test__ = False


def source_service_decommission_requirements_to_dicts(
    requirements: (
        tuple[SourceServiceDecommissionRequirement, ...]
        | list[SourceServiceDecommissionRequirement]
        | SourceServiceDecommissionRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize service decommission records to dictionaries."""
    if isinstance(requirements, SourceServiceDecommissionRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_service_decommission_requirements_to_dicts.__test__ = False


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
    return bool(re.search(r"\b(?:decommission|shutdown|sunset|retire|source payload)\b", _field_words(field), re.I))


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
    "ServiceDecommissionSignal",
    "SourceServiceDecommissionRequirement",
    "SourceServiceDecommissionRequirementsReport",
    "build_source_service_decommission_requirements",
    "derive_source_service_decommission_requirements",
    "extract_source_service_decommission_requirements",
    "generate_source_service_decommission_requirements",
    "source_service_decommission_requirements_to_dict",
    "source_service_decommission_requirements_to_dicts",
    "summarize_source_service_decommission_requirements",
]
