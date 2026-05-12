"""Extract regional data residency requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping


RegionalResidencySignal = Literal[
    "allowed_regions",
    "prohibited_regions",
    "transfer_rules",
    "backup_placement",
    "processing_location",
    "compliance_driver",
    "owner",
    "verification_evidence",
]

_SIGNALS: tuple[RegionalResidencySignal, ...] = (
    "allowed_regions",
    "prohibited_regions",
    "transfer_rules",
    "backup_placement",
    "processing_location",
    "compliance_driver",
    "owner",
    "verification_evidence",
)
_SPACE_RE = re.compile(r"\s+")
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_REGION_RE = re.compile(
    r"\b(?:eu|eea|europe|us|usa|united states|uk|canada|germany|france|ireland|japan|"
    r"singapore|australia|india|china|brazil|eu[-_ ]west[-_ ]\d+|us[-_ ]east[-_ ]\d+|"
    r"us[-_ ]west[-_ ]\d+|ap[-_ ](?:southeast|northeast)[-_ ]\d+)\b",
    re.I,
)
_PATTERNS: dict[RegionalResidencySignal, re.Pattern[str]] = {
    "allowed_regions": re.compile(
        r"\b(?:allowed|approved|permitted|must remain|must stay|store(?:d)? only|host(?:ed)? only|within|in-region)\b.{0,100}"
        + _REGION_RE.pattern
        + r"|"
        + r"\b(?:store|stored|host|hosted|keep|kept|reside|process|processed)\b.{0,80}\b(?:only|solely|exclusively)\b.{0,40}"
        + _REGION_RE.pattern,
        re.I,
    ),
    "prohibited_regions": re.compile(
        r"\b(?:prohibit|prohibited|blocked|must not|cannot|never|outside|not leave|no transfer to)\b.{0,100}"
        + _REGION_RE.pattern,
        re.I,
    ),
    "transfer_rules": re.compile(
        r"\b(?:cross[- ]region|cross[- ]border|transfer|replicate outside|leave the region|scc|tia|adequacy)\b",
        re.I,
    ),
    "backup_placement": re.compile(
        r"\b(?:backup|backups|replica|replicas|replication|snapshot|disaster recovery|dr)\b.{0,100}"
        r"\b(?:region|same country|same jurisdiction|resident|in-region|within)\b",
        re.I,
    ),
    "processing_location": re.compile(
        r"\b(?:process|processing|compute|worker|job|analytics|logs?)\b.{0,100}"
        r"\b(?:in|within|only|region|country|jurisdiction)\b",
        re.I,
    ),
    "compliance_driver": re.compile(
        r"\b(?:gdpr|data residency|data localization|data localisation|sovereignty|hipaa|pci|public sector|contractual|regulatory)\b",
        re.I,
    ),
    "owner": re.compile(r"\b(?:owner|owned by|accountable owner|responsible team|privacy team|compliance team)\b", re.I),
    "verification_evidence": re.compile(
        r"\b(?:evidence|verify|verification|attestation|audit|report|proof|soc 2|iso 27001|customer audit)\b",
        re.I,
    ),
}
_WEAK_RE = re.compile(r"\b(?:residency|regional|region|geo|locali[sz]ation|sovereign)\b", re.I)
_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "constraints",
    "acceptance_criteria",
    "definition_of_done",
    "privacy",
    "compliance",
    "metadata",
    "source_payload",
)


@dataclass(frozen=True, slots=True)
class SourceRegionalResidencyRequirement:
    """One extracted regional residency requirement signal."""

    signal: RegionalResidencySignal
    regions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: Literal["high", "medium"] = "high"

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal": self.signal,
            "regions": list(self.regions),
            "evidence": list(self.evidence),
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SourceRegionalResidencyRequirementsReport:
    """Structured regional residency findings and gaps."""

    source_id: str | None = None
    requirements: tuple[SourceRegionalResidencyRequirement, ...] = field(default_factory=tuple)
    missing_signals: tuple[RegionalResidencySignal, ...] = field(default_factory=tuple)
    weak_signals: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceRegionalResidencyRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [requirement.to_dict() for requirement in self.records],
            "missing_signals": list(self.missing_signals),
            "weak_signals": list(self.weak_signals),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [requirement.to_dict() for requirement in self.requirements]


def build_source_regional_residency_requirements(
    source: Mapping[str, Any] | str | object,
) -> SourceRegionalResidencyRequirementsReport:
    """Build a deterministic regional residency report from brief-like input."""
    source_id, payload = _payload(source)
    found: dict[RegionalResidencySignal, list[str]] = {signal: [] for signal in _SIGNALS}
    regions: dict[RegionalResidencySignal, list[str]] = {signal: [] for signal in _SIGNALS}
    weak: list[str] = []

    for field, text in _texts(payload):
        matches = [signal for signal, pattern in _PATTERNS.items() if pattern.search(text)]
        for signal in matches:
            found[signal].append(f"{field}: {text}")
            regions[signal].extend(_clean(match.group(0)) for match in _REGION_RE.finditer(text))
        if field not in {"id", "source_id", "title"} and _WEAK_RE.search(text) and not matches:
            weak.append(f"{field}: clarify explicit regional residency rule for '{text}'")

    requirements = tuple(
        SourceRegionalResidencyRequirement(
            signal=signal,
            regions=tuple(_dedupe(regions[signal])),
            evidence=tuple(_dedupe(found[signal]))[:4],
        )
        for signal in _SIGNALS
        if found[signal]
    )
    missing = tuple(signal for signal in _SIGNALS if not found[signal])
    weak_values = tuple(_dedupe(weak))
    return SourceRegionalResidencyRequirementsReport(
        source_id=source_id,
        requirements=requirements,
        missing_signals=missing,
        weak_signals=weak_values,
        summary={
            "requirement_count": len(requirements),
            "missing_count": len(missing),
            "weak_count": len(weak_values),
            "signals": [requirement.signal for requirement in requirements],
            "signal_counts": {signal: int(bool(found[signal])) for signal in _SIGNALS},
        },
    )


def extract_source_regional_residency_requirements(
    source: Mapping[str, Any] | str | object,
) -> tuple[SourceRegionalResidencyRequirement, ...]:
    """Return extracted regional residency records."""
    return build_source_regional_residency_requirements(source).requirements


def source_regional_residency_requirements_to_dict(
    report: SourceRegionalResidencyRequirementsReport,
) -> dict[str, Any]:
    """Serialize a regional residency report."""
    return report.to_dict()


source_regional_residency_requirements_to_dict.__test__ = False


def _payload(source: Mapping[str, Any] | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
    elif isinstance(source, Mapping):
        payload = dict(source)
    else:
        payload = {}
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


def _optional(value: Any) -> str | None:
    if value is None:
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
