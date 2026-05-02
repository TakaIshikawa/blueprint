"""Extract data retention lifecycle requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


DataRetentionRequirementType = Literal[
    "retention",
    "deletion",
    "archival",
    "purge",
    "legal_hold",
]
DataRetentionRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[DataRetentionRequirementType, ...] = (
    "legal_hold",
    "retention",
    "deletion",
    "purge",
    "archival",
)
_CONFIDENCE_ORDER: dict[DataRetentionRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_TYPE_PATTERNS: dict[DataRetentionRequirementType, re.Pattern[str]] = {
    "retention": re.compile(
        r"\b(?:retain|retained|retention|keep|kept|store|stored|preserve|retention period)\b",
        re.I,
    ),
    "deletion": re.compile(
        r"\b(?:delete|deleted|deletion|remove|removed|erase|erasure|right to erasure|"
        r"forget|forgotten|redact|anonymi[sz]e)\b",
        re.I,
    ),
    "archival": re.compile(r"\b(?:archive|archived|archival|cold storage)\b", re.I),
    "purge": re.compile(
        r"\b(?:purge|purged|hard delete|permanent(?:ly)? delete|tombstone)\b",
        re.I,
    ),
    "legal_hold": re.compile(
        r"\b(?:legal hold|litigation hold|hold order|e[- ]?discovery|regulatory hold|"
        r"audit hold|do not delete|suspend deletion|preservation notice)\b",
        re.I,
    ),
}
_LIFECYCLE_RE = re.compile(
    r"\b(?:retain|retained|retention|keep|kept|delete|deleted|deletion|erase|erasure|"
    r"archive|archived|archival|purge|purged|hard delete|legal hold|litigation hold|"
    r"do not delete|suspend deletion|"
    r"right to erasure|anonymi[sz]e)\b",
    re.I,
)
_EXPLICIT_RE = re.compile(
    r"\b(?:must|shall|requires?|required|requirement|needs? to|should|ensure|policy|"
    r"after|within|until|unless|upon|when|on request|by request)\b",
    re.I,
)
_WINDOW_RE = re.compile(
    r"\b(?:(?:for|within|after|older than|no longer than|at least|minimum of|maximum of|"
    r"up to)\s+)?(?:\d+(?:\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|"
    r"eleven|twelve|thirteen|fourteen|fifteen|thirty|sixty|ninety)\s+"
    r"(?:calendar\s+)?(?:days?|weeks?|months?|quarters?|years?|hrs?|hours?)\b",
    re.I,
)
_UNTIL_RE = re.compile(
    r"\b(?:until|through)\s+(?:contract termination|account closure|case closure|"
    r"legal hold is lifted|hold release|subscription cancellation|audit completion)\b",
    re.I,
)
_TRIGGER_RE = re.compile(
    r"\b(?:upon|on|after|when|once|following)\s+(?:account closure|account deletion|"
    r"user request|customer request|deletion request|erasure request|contract termination|"
    r"subscription cancellation|case closure|offboarding|consent withdrawal|legal hold release|"
    r"hold release|audit completion)\b",
    re.I,
)
_LEGAL_HOLD_SIGNAL_RE = re.compile(
    r"\b(?:legal hold|litigation hold|hold order|regulatory hold|audit hold|"
    r"do not delete|suspend deletion|preservation notice|until legal hold is lifted)\b",
    re.I,
)
_SCOPE_RE = re.compile(
    r"\b(?:customer|user|account|billing|invoice|payment|audit|event|session|profile|"
    r"personal|pii|personal data|export|backup|log|analytics|support|case|workspace|tenant)"
    r"(?:[- ](?:customer|user|account|billing|invoice|payment|audit|event|session|profile|"
    r"personal|pii|data|records?|logs?|exports?|backups?|tokens?|files?|attachments?|case))*\b",
    re.I,
)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_SPACE_RE = re.compile(r"\s+")

_TOP_LEVEL_FIELDS = (
    "summary",
    "problem",
    "problem_statement",
    "goals",
    "constraints",
    "acceptance_criteria",
)
_SOURCE_PAYLOAD_FIELDS = (
    "requirements",
    "retention",
    "data_retention",
    "data_retention_requirements",
    "privacy",
    "compliance",
    "constraints",
    "acceptance_criteria",
    "body",
    "description",
    "markdown",
)


@dataclass(frozen=True, slots=True)
class SourceDataRetentionRequirement:
    """One source-backed data lifecycle requirement."""

    requirement_type: DataRetentionRequirementType
    data_scope: str | None = None
    retention_window: str | None = None
    deletion_trigger: str | None = None
    legal_hold_signal: str | None = None
    confidence: DataRetentionRequirementConfidence = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "data_scope": self.data_scope,
            "retention_window": self.retention_window,
            "deletion_trigger": self.deletion_trigger,
            "legal_hold_signal": self.legal_hold_signal,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceDataRetentionRequirements:
    """Inventory of data retention lifecycle requirements found in a source brief."""

    source_id: str | None = None
    requirements: tuple[SourceDataRetentionRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceDataRetentionRequirement, ...]:
        """Compatibility view matching extractors that name findings records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return retention requirements as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]


def build_source_data_retention_requirements(
    source: Mapping[str, Any] | SourceBrief | Any,
) -> SourceDataRetentionRequirements:
    """Build a data retention requirement inventory from a SourceBrief-shaped record."""
    brief = _source_brief_payload(source)
    if not brief:
        return SourceDataRetentionRequirements()

    detected: dict[tuple[Any, ...], SourceDataRetentionRequirement] = {}
    for source_field, value in _candidate_values(brief):
        for segment in _segments(value):
            if not _LIFECYCLE_RE.search(segment):
                continue
            for requirement_type in _requirement_types(segment, source_field):
                requirement = _requirement_for(requirement_type, source_field, segment)
                if (
                    requirement.confidence == "low"
                    and not requirement.data_scope
                    and not requirement.retention_window
                    and not requirement.deletion_trigger
                    and not requirement.legal_hold_signal
                ):
                    continue
                key = (
                    requirement.requirement_type,
                    _dedupe_key(requirement.data_scope),
                    _dedupe_key(requirement.retention_window),
                    _dedupe_key(requirement.deletion_trigger),
                    _dedupe_key(requirement.legal_hold_signal),
                )
                existing = detected.get(key)
                if existing is None:
                    detected[key] = requirement
                else:
                    detected[key] = _merge_requirement(existing, requirement)

    requirements = tuple(
        sorted(
            detected.values(),
            key=lambda requirement: (
                _TYPE_ORDER.index(requirement.requirement_type),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.data_scope or "",
                requirement.retention_window or "",
                requirement.deletion_trigger or "",
                requirement.legal_hold_signal or "",
                requirement.evidence,
            ),
        )
    )
    return SourceDataRetentionRequirements(
        source_id=_optional_text(brief.get("id") or brief.get("source_id")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_data_retention_requirements(
    source: Mapping[str, Any] | SourceBrief | Any,
) -> SourceDataRetentionRequirements:
    """Compatibility alias for building a data retention requirement inventory."""
    return build_source_data_retention_requirements(source)


def summarize_source_data_retention_requirements(
    source_or_result: Mapping[str, Any] | SourceBrief | SourceDataRetentionRequirements | Any,
) -> dict[str, Any]:
    """Return deterministic counts for extracted retention requirements."""
    if isinstance(source_or_result, SourceDataRetentionRequirements):
        requirements = source_or_result.requirements
    else:
        requirements = build_source_data_retention_requirements(source_or_result).requirements
    return _summary(requirements)


def source_data_retention_requirements_to_dict(
    result: SourceDataRetentionRequirements,
) -> dict[str, Any]:
    """Serialize a data retention requirement inventory to a plain dictionary."""
    return result.to_dict()


source_data_retention_requirements_to_dict.__test__ = False


def _requirement_for(
    requirement_type: DataRetentionRequirementType,
    source_field: str,
    text: str,
) -> SourceDataRetentionRequirement:
    retention_window = _match_text(_WINDOW_RE, text) or _match_text(_UNTIL_RE, text)
    legal_hold_signal = _match_text(_LEGAL_HOLD_SIGNAL_RE, text)
    deletion_trigger = _match_text(_TRIGGER_RE, text)
    if requirement_type == "legal_hold" and legal_hold_signal and not deletion_trigger:
        deletion_trigger = "until legal hold is lifted" if "lifted" in text.casefold() else None
    return SourceDataRetentionRequirement(
        requirement_type=requirement_type,
        data_scope=_data_scope(text),
        retention_window=retention_window,
        deletion_trigger=deletion_trigger,
        legal_hold_signal=legal_hold_signal,
        confidence=_confidence(
            text,
            source_field,
            retention_window,
            deletion_trigger,
            legal_hold_signal,
        ),
        evidence=(_evidence_snippet(source_field, text),),
    )


def _requirement_types(text: str, source_field: str) -> tuple[DataRetentionRequirementType, ...]:
    types = [
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    ]
    field_text = source_field.rsplit(".", 1)[-1].replace("_", " ")
    for requirement_type in _TYPE_ORDER:
        if (
            requirement_type not in types
            and _TYPE_PATTERNS[requirement_type].search(field_text)
        ):
            types.append(requirement_type)
    if "purge" in types and "deletion" in types:
        types.remove("deletion")
    if "legal_hold" in types and "deletion" in types and re.search(
        r"\b(?:do not delete|suspend deletion)\b",
        text,
        re.I,
    ):
        types.remove("deletion")
    if "legal_hold" in types and not _is_standalone_legal_hold(text):
        types.remove("legal_hold")
    return tuple(types)


def _is_standalone_legal_hold(text: str) -> bool:
    return bool(
        re.search(
            r"\b(?:do not delete|suspend deletion|litigation hold|regulatory hold|audit hold|"
            r"hold order|preservation notice|legal hold requires|until legal hold is lifted)\b",
            text,
            re.I,
        )
    )


def _confidence(
    text: str,
    source_field: str,
    retention_window: str | None,
    deletion_trigger: str | None,
    legal_hold_signal: str | None,
) -> DataRetentionRequirementConfidence:
    strong_field = any(
        token in source_field
        for token in ("requirements", "retention", "privacy", "compliance", "constraints")
    )
    has_detail = bool(retention_window or deletion_trigger or legal_hold_signal)
    if has_detail and (_EXPLICIT_RE.search(text) or strong_field):
        return "high"
    if has_detail or _EXPLICIT_RE.search(text) or strong_field:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceDataRetentionRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "type_counts": {
            requirement_type: sum(
                1
                for requirement in requirements
                if requirement.requirement_type == requirement_type
            )
            for requirement_type in _TYPE_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "legal_hold_count": sum(
            1 for requirement in requirements if requirement.legal_hold_signal
        ),
    }


def _candidate_values(brief: Mapping[str, Any]) -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = []
    for field_name in _TOP_LEVEL_FIELDS:
        if field_name in brief:
            _append_value(candidates, field_name, brief[field_name])

    payload = brief.get("source_payload")
    if isinstance(payload, Mapping):
        visited: set[str] = set()
        for field_name in _SOURCE_PAYLOAD_FIELDS:
            if field_name in payload:
                source_field = f"source_payload.{field_name}"
                _append_value(candidates, source_field, payload[field_name])
                visited.add(source_field)
        for source_field, value in _flatten_payload(payload, prefix="source_payload"):
            if _is_under_visited_field(source_field, visited):
                continue
            _append_value(candidates, source_field, value)
    return candidates


def _append_value(candidates: list[tuple[str, Any]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for child_field, child_value in _flatten_payload(value, prefix=source_field):
            _append_value(candidates, child_field, child_value)
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _append_value(candidates, f"{source_field}[{index}]", item)
        return
    if isinstance(value, set):
        for index, item in enumerate(sorted(value, key=lambda item: str(item))):
            _append_value(candidates, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        candidates.append((source_field, text))


def _flatten_payload(value: Any, *, prefix: str) -> list[tuple[str, Any]]:
    flattened: list[tuple[str, Any]] = []

    def append(current: Any, path: str) -> None:
        if isinstance(current, Mapping):
            for key in sorted(current, key=lambda item: str(item)):
                try:
                    append(current[key], f"{path}.{key}")
                except (KeyError, TypeError):
                    continue
            return
        if isinstance(current, (list, tuple)):
            for index, item in enumerate(current):
                append(item, f"{path}[{index}]")
            return
        if isinstance(current, set):
            for index, item in enumerate(sorted(current, key=lambda item: str(item))):
                append(item, f"{path}[{index}]")
            return
        flattened.append((path, current))

    append(value, prefix)
    return flattened


def _segments(value: str) -> tuple[str, ...]:
    segments: list[str] = []
    for line in value.splitlines():
        line_text = _clean_text(line)
        if not line_text:
            continue
        if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line):
            segments.append(line_text)
            continue
        for part in _SENTENCE_SPLIT_RE.split(line):
            text = _clean_text(part)
            if text:
                segments.append(text)
    return tuple(segments)


def _data_scope(text: str) -> str | None:
    matches = [
        _clean_scope(match.group(0))
        for match in _SCOPE_RE.finditer(text)
        if _clean_scope(match.group(0)) not in {"data", "records", "logs"}
    ]
    return _dedupe(matches)[0] if matches else None


def _clean_scope(text: str) -> str:
    return _SPACE_RE.sub(" ", text.strip(" .,:;")).casefold()


def _merge_requirement(
    left: SourceDataRetentionRequirement,
    right: SourceDataRetentionRequirement,
) -> SourceDataRetentionRequirement:
    confidence = min(
        (left.confidence, right.confidence),
        key=lambda item: _CONFIDENCE_ORDER[item],
    )
    return SourceDataRetentionRequirement(
        requirement_type=left.requirement_type,
        data_scope=left.data_scope or right.data_scope,
        retention_window=left.retention_window or right.retention_window,
        deletion_trigger=left.deletion_trigger or right.deletion_trigger,
        legal_hold_signal=left.legal_hold_signal or right.legal_hold_signal,
        confidence=confidence,
        evidence=tuple(_dedupe((*left.evidence, *right.evidence))),
    )


def _source_brief_payload(source_brief: Mapping[str, Any] | SourceBrief | Any) -> dict[str, Any]:
    if isinstance(source_brief, SourceBrief):
        return source_brief.model_dump(mode="python")
    if hasattr(source_brief, "model_dump"):
        value = source_brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = SourceBrief.model_validate(source_brief).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(source_brief, Mapping):
            return dict(source_brief)
    return {}


def _match_text(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    return _clean_text(match.group(0)) if match else None


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _dedupe_key(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", _clean_text(value).casefold()).strip()


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


def _is_under_visited_field(source_field: str, visited_fields: set[str]) -> bool:
    return any(
        source_field == visited
        or source_field.startswith(f"{visited}.")
        or source_field.startswith(f"{visited}[")
        for visited in visited_fields
    )


__all__ = [
    "DataRetentionRequirementConfidence",
    "DataRetentionRequirementType",
    "SourceDataRetentionRequirement",
    "SourceDataRetentionRequirements",
    "build_source_data_retention_requirements",
    "extract_source_data_retention_requirements",
    "source_data_retention_requirements_to_dict",
    "summarize_source_data_retention_requirements",
]
