"""Extract source-level release freeze requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping


ReleaseFreezeSignal = Literal[
    "freeze_window",
    "exception_approval",
    "affected_environments",
    "deployment_restrictions",
    "communication_requirements",
    "rollback_only_permissions",
    "owner_escalation",
]

_SIGNALS: tuple[ReleaseFreezeSignal, ...] = (
    "freeze_window",
    "exception_approval",
    "affected_environments",
    "deployment_restrictions",
    "communication_requirements",
    "rollback_only_permissions",
    "owner_escalation",
)
_SPACE_RE = re.compile(r"\s+")
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_FREEZE_CONTEXT_RE = re.compile(
    r"\b(?:release freeze|freeze window|code freeze|deployment freeze|change freeze|holiday freeze)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope)\b.{0,80}\b(?:release freeze|freeze window|code freeze|deployment freeze|change freeze)\b|"
    r"\b(?:release freeze|freeze window|code freeze|deployment freeze|change freeze)\b.{0,80}\b(?:not required|out of scope|no changes?)\b",
    re.I,
)
_PATTERNS: dict[ReleaseFreezeSignal, re.Pattern[str]] = {
    "freeze_window": re.compile(
        r"\b(?:freeze window|release freeze|code freeze|deployment freeze|change freeze|holiday freeze|"
        r"from\s+\w+\s+\d{1,2}|until\s+\w+\s+\d{1,2}|between\s+\w+\s+\d{1,2})\b",
        re.I,
    ),
    "exception_approval": re.compile(
        r"\b(?:exception|exemption|override|waiver|approval|approve|approved|change advisory board|cab)\b",
        re.I,
    ),
    "affected_environments": re.compile(
        r"\b(?:production|prod|staging|stage|uat|sandbox|demo|preprod|pre-prod|environment|environments|region|tenant)\b",
        re.I,
    ),
    "deployment_restrictions": re.compile(
        r"\b(?:deployments?|releases?|rollouts?|feature flags?|schema changes?|migrations?|changes?)\b.{0,80}"
        r"\b(?:blocked|paused|prohibited|frozen|not allowed|must not|only|restricted|stop)\b|"
        r"\b(?:blocked|paused|prohibited|frozen|not allowed|must not|restricted|stop)\b.{0,80}"
        r"\b(?:deployments?|releases?|rollouts?|feature flags?|schema changes?|migrations?|changes?)\b",
        re.I,
    ),
    "communication_requirements": re.compile(
        r"\b(?:announce|announcement|notify|notification|communicate|comms|status page|release calendar|"
        r"stakeholders?|customer success|support)\b",
        re.I,
    ),
    "rollback_only_permissions": re.compile(
        r"\b(?:rollback[- ]?only|rollbacks? only|rollback|roll back|revert|hotfix)\b.{0,100}"
        r"\b(?:only|allowed|permitted|approved|during freeze|freeze)\b|"
        r"\b(?:only|allowed|permitted|approved)\b.{0,100}\b(?:rollback|roll back|revert|hotfix)\b",
        re.I,
    ),
    "owner_escalation": re.compile(
        r"\b(?:owner|owned by|responsible team|release manager|incident commander|approver|escalation|"
        r"escalate|on-call|pagerduty|slack channel)\b",
        re.I,
    ),
}
_VALUE_PATTERNS: dict[ReleaseFreezeSignal, re.Pattern[str]] = {
    "freeze_window": re.compile(
        r"((?:from|between)\s+[^.]{3,80}?(?:to|and|through|-)\s+[^.]{3,80}|"
        r"(?:until|through)\s+\w+\s+\d{1,2}(?:,\s*\d{4})?|"
        r"\d{4}-\d{2}-\d{2}\s*(?:to|through|-)\s*\d{4}-\d{2}-\d{2}|"
        r"\d+\s*(?:hour|day|week)s?)",
        re.I,
    ),
    "affected_environments": re.compile(
        r"\b(production|prod|staging|stage|uat|sandbox|demo|preprod|pre-prod)"
        r"(?:\s*,\s*(?:and\s+)?(?:production|prod|staging|stage|uat|sandbox|demo|preprod|pre-prod)|"
        r"\s+and\s+(?:production|prod|staging|stage|uat|sandbox|demo|preprod|pre-prod))*\b",
        re.I,
    ),
}
_WEAK_RE = re.compile(r"\b(?:freeze|frozen|blackout|avoid releases|be careful with releases)\b", re.I)
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
class SourceReleaseFreezeRequirement:
    """One extracted release freeze requirement signal."""

    signal: ReleaseFreezeSignal
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
class SourceReleaseFreezeRequirementsReport:
    """Structured release freeze requirement findings and gaps."""

    source_id: str | None = None
    requirements: tuple[SourceReleaseFreezeRequirement, ...] = field(default_factory=tuple)
    missing_signals: tuple[ReleaseFreezeSignal, ...] = field(default_factory=tuple)
    weak_signals: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceReleaseFreezeRequirement, ...]:
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
        """Return release freeze requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]


def build_source_release_freeze_requirements(
    source: Mapping[str, Any] | str | object,
) -> SourceReleaseFreezeRequirementsReport:
    """Build a deterministic release freeze requirements report from brief-like input."""
    source_id, payload = _payload(source)
    found: dict[ReleaseFreezeSignal, list[str]] = {signal: [] for signal in _SIGNALS}
    values: dict[ReleaseFreezeSignal, str] = {}
    weak: list[str] = []
    texts = _texts(payload)
    has_freeze_context = any(
        _FREEZE_CONTEXT_RE.search(f"{_field_words(field)} {text}") for field, text in texts
    )

    for field, text in texts:
        if _NEGATED_RE.search(text):
            continue
        searchable = f"{_field_words(field)} {text}"
        matches = [
            signal
            for signal, pattern in _PATTERNS.items()
            if pattern.search(searchable)
            and (signal != "freeze_window" or bool(_VALUE_PATTERNS["freeze_window"].search(text)))
        ]
        if matches and (
            has_freeze_context or _FREEZE_CONTEXT_RE.search(searchable) or _field_context(field) or len(matches) > 1
        ):
            for signal in matches:
                found[signal].append(f"{field}: {text}")
                if signal in _VALUE_PATTERNS and signal not in values:
                    if match := _VALUE_PATTERNS[signal].search(text):
                        values[signal] = _clean(match.group(1) if signal == "freeze_window" else match.group(0))
        elif field not in {"id", "source_id", "title"} and _WEAK_RE.search(text):
            weak.append(f"{field}: clarify concrete release freeze rule for '{text}'")

    requirements = tuple(
        SourceReleaseFreezeRequirement(
            signal=signal,
            value=values.get(signal),
            evidence=tuple(_dedupe(found[signal]))[:4],
            confidence="high" if values.get(signal) or signal != "deployment_restrictions" else "medium",
        )
        for signal in _SIGNALS
        if found[signal]
    )
    missing = tuple(signal for signal in _SIGNALS if not found[signal])
    weak_signals = () if not missing else tuple(_dedupe(weak))
    return SourceReleaseFreezeRequirementsReport(
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


def derive_source_release_freeze_requirements(
    source: Mapping[str, Any] | str | object,
) -> SourceReleaseFreezeRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_release_freeze_requirements(source)


def generate_source_release_freeze_requirements(
    source: Mapping[str, Any] | str | object,
) -> SourceReleaseFreezeRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_release_freeze_requirements(source)


def extract_source_release_freeze_requirements(
    source: Mapping[str, Any] | str | object,
) -> tuple[SourceReleaseFreezeRequirement, ...]:
    """Return extracted release freeze requirement records."""
    return build_source_release_freeze_requirements(source).requirements


def summarize_source_release_freeze_requirements(
    source: Mapping[str, Any] | SourceReleaseFreezeRequirementsReport | str | object,
) -> dict[str, Any]:
    """Return deterministic counts for release freeze requirements."""
    if isinstance(source, SourceReleaseFreezeRequirementsReport):
        return dict(source.summary)
    return build_source_release_freeze_requirements(source).summary


def source_release_freeze_requirements_to_dict(
    report: SourceReleaseFreezeRequirementsReport,
) -> dict[str, Any]:
    """Serialize a release freeze requirements report."""
    return report.to_dict()


source_release_freeze_requirements_to_dict.__test__ = False


def source_release_freeze_requirements_to_dicts(
    requirements: (
        tuple[SourceReleaseFreezeRequirement, ...]
        | list[SourceReleaseFreezeRequirement]
        | SourceReleaseFreezeRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize release freeze requirement records to dictionaries."""
    if isinstance(requirements, SourceReleaseFreezeRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_release_freeze_requirements_to_dicts.__test__ = False


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
    source_id = _optional(payload.get("id")) or _optional(payload.get("source_id"))
    return source_id, payload


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
    return bool(re.search(r"\b(?:freeze|release|deployment|change|constraints?|source payload)\b", _field_words(field), re.I))


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
    "ReleaseFreezeSignal",
    "SourceReleaseFreezeRequirement",
    "SourceReleaseFreezeRequirementsReport",
    "build_source_release_freeze_requirements",
    "derive_source_release_freeze_requirements",
    "extract_source_release_freeze_requirements",
    "generate_source_release_freeze_requirements",
    "source_release_freeze_requirements_to_dict",
    "source_release_freeze_requirements_to_dicts",
    "summarize_source_release_freeze_requirements",
]
