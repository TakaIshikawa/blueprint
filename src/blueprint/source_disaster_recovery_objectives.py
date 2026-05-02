"""Extract disaster recovery objectives from source brief-shaped inputs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


DisasterRecoveryObjectiveType = Literal[
    "rto",
    "rpo",
    "backup",
    "restore",
    "failover",
    "region",
    "incident",
    "continuity",
]

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_MAX_EVIDENCE_PER_OBJECTIVE = 4
_SCANNED_FIELDS: tuple[str, ...] = (
    "summary",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
    "constraints",
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "risks",
    "metadata",
)
_OBJECTIVE_ORDER: dict[DisasterRecoveryObjectiveType, int] = {
    "rto": 0,
    "rpo": 1,
    "backup": 2,
    "restore": 3,
    "failover": 4,
    "region": 5,
    "incident": 6,
    "continuity": 7,
}
_EXPLICIT_VALUE_RE = re.compile(
    r"\b(?:rto|recovery time objective|rpo|recovery point objective)\b"
    r"\s*(?:is|of|:)?\s*"
    r"(?P<value>(?:less than|under|within|<=?\s*)?\d+(?:\.\d+)?\s*"
    r"(?:milliseconds?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d)|"
    r"zero\s+(?:data\s+)?loss|no\s+data\s+loss)",
    re.I,
)
_OBJECTIVE_PATTERNS: dict[DisasterRecoveryObjectiveType, tuple[re.Pattern[str], ...]] = {
    "rto": (
        re.compile(r"\b(?:rto|recovery time objective|restore within|recover within)\b", re.I),
    ),
    "rpo": (
        re.compile(
            r"\b(?:rpo|recovery point objective|data loss window|maximum data loss|"
            r"no data loss|zero data loss)\b",
            re.I,
        ),
    ),
    "backup": (
        re.compile(
            r"\b(?:backup|backups|snapshot|snapshots|point[- ]in[- ]time|pit[ r]?|"
            r"replication|replicated|archive|archival)\b",
            re.I,
        ),
    ),
    "restore": (
        re.compile(
            r"\b(?:restore|restores|restoration|recovery runbook|recover from|"
            r"rollback data|rebuild from backup|test restores?)\b",
            re.I,
        ),
    ),
    "failover": (
        re.compile(
            r"\b(?:failover|fail over|standby|hot standby|warm standby|active[- ]active|"
            r"active[- ]passive|secondary cluster|promote replica)\b",
            re.I,
        ),
    ),
    "region": (
        re.compile(
            r"\b(?:multi[- ]region|cross[- ]region|secondary region|regional outage|"
            r"region fail(?:ure|s)?|geo[- ]redundant|geographic redundancy|availability zone|az)\b",
            re.I,
        ),
    ),
    "incident": (
        re.compile(
            r"\b(?:incident|sev[ -]?\d|outage|disaster|dr drill|tabletop|"
            r"major failure|crisis|emergency response)\b",
            re.I,
        ),
    ),
    "continuity": (
        re.compile(
            r"\b(?:business continuity|continuity|disaster recovery|dr readiness|"
            r"service continuity|continue operating|keep running|critical operations)\b",
            re.I,
        ),
    ),
}
_FOLLOW_UPS: dict[DisasterRecoveryObjectiveType, str] = {
    "rto": "Confirm the recovery time objective owner, measurement start point, and validation drill.",
    "rpo": "Confirm the tolerated data loss window and backup or replication proof.",
    "backup": "Define backup cadence, retention, encryption, restore scope, and monitoring.",
    "restore": "Document restore runbooks, test cadence, dependencies, and acceptance checks.",
    "failover": "Define failover trigger, authority, automation level, and rollback criteria.",
    "region": "Confirm regional dependency assumptions, traffic routing, data residency, and capacity.",
    "incident": "Map disaster scenarios to incident severity, communications, and recovery ownership.",
    "continuity": "Define the minimum viable service during disruption and continuity validation steps.",
}


@dataclass(frozen=True, slots=True)
class SourceDisasterRecoveryObjective:
    """One disaster recovery objective found in source brief evidence."""

    objective_type: DisasterRecoveryObjectiveType
    value: str | None = None
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: float = 0.0
    recommended_follow_up: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "objective_type": self.objective_type,
            "value": self.value,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "recommended_follow_up": self.recommended_follow_up,
        }


@dataclass(frozen=True, slots=True)
class SourceDisasterRecoveryObjectivesReport:
    """Source-level disaster recovery objectives report."""

    source_brief_id: str | None = None
    title: str | None = None
    objectives: tuple[SourceDisasterRecoveryObjective, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceDisasterRecoveryObjective, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.objectives

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "objectives": [objective.to_dict() for objective in self.objectives],
            "records": [objective.to_dict() for objective in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return objective records as plain dictionaries."""
        return [objective.to_dict() for objective in self.objectives]


def build_source_disaster_recovery_objectives_report(
    source: Mapping[str, Any] | SourceBrief | object,
) -> SourceDisasterRecoveryObjectivesReport:
    """Build a disaster recovery objectives report from a source brief-like payload."""
    source_brief_id, payload = _source_payload(source)
    objectives = tuple(_merge_candidates(_objective_candidates(payload)))
    return SourceDisasterRecoveryObjectivesReport(
        source_brief_id=source_brief_id,
        title=_optional_text(payload.get("title")),
        objectives=objectives,
        summary=_summary(objectives),
    )


def derive_source_disaster_recovery_objectives(
    source: Mapping[str, Any] | SourceBrief | object,
) -> tuple[SourceDisasterRecoveryObjective, ...]:
    """Return disaster recovery objective records from brief-shaped input."""
    return build_source_disaster_recovery_objectives_report(source).objectives


def extract_source_disaster_recovery_objectives(
    source: Mapping[str, Any] | SourceBrief | object,
) -> tuple[SourceDisasterRecoveryObjective, ...]:
    """Alias for callers that use extract_* naming."""
    return derive_source_disaster_recovery_objectives(source)


def source_disaster_recovery_objectives_report_to_dict(
    report: SourceDisasterRecoveryObjectivesReport,
) -> dict[str, Any]:
    """Serialize a disaster recovery objectives report to a plain dictionary."""
    return report.to_dict()


source_disaster_recovery_objectives_report_to_dict.__test__ = False


def source_disaster_recovery_objectives_to_dicts(
    objectives: (
        tuple[SourceDisasterRecoveryObjective, ...]
        | list[SourceDisasterRecoveryObjective]
        | SourceDisasterRecoveryObjectivesReport
    ),
) -> list[dict[str, Any]]:
    """Serialize disaster recovery objective records to dictionaries."""
    if isinstance(objectives, SourceDisasterRecoveryObjectivesReport):
        return objectives.to_dicts()
    return [objective.to_dict() for objective in objectives]


source_disaster_recovery_objectives_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    objective_type: DisasterRecoveryObjectiveType
    value: str | None
    source_field: str
    evidence: str
    confidence: float


def _source_payload(source: Mapping[str, Any] | SourceBrief | object) -> tuple[str | None, dict[str, Any]]:
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


def _objective_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_field, segment in _candidate_segments(payload):
        objective_types = _objective_types(segment)
        if not objective_types:
            continue
        explicit_values = _explicit_values(segment)
        evidence = _evidence_snippet(source_field, segment)
        for objective_type in objective_types:
            value = explicit_values.get(objective_type)
            candidates.append(
                _Candidate(
                    objective_type=objective_type,
                    value=value,
                    source_field=source_field,
                    evidence=evidence,
                    confidence=_confidence(segment, source_field, value),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceDisasterRecoveryObjective]:
    grouped: dict[DisasterRecoveryObjectiveType, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.objective_type, []).append(candidate)

    objectives: list[SourceDisasterRecoveryObjective] = []
    for objective_type in _OBJECTIVE_ORDER:
        items = grouped.get(objective_type, [])
        if not items:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in items))[
            :_MAX_EVIDENCE_PER_OBJECTIVE
        ]
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda value: value.casefold(),
        )[0]
        value = _best_value(items)
        objectives.append(
            SourceDisasterRecoveryObjective(
                objective_type=objective_type,
                value=value,
                source_field=source_field,
                evidence=evidence,
                confidence=round(max(item.confidence for item in items), 2),
                recommended_follow_up=_FOLLOW_UPS[objective_type],
            )
        )
    return sorted(
        objectives,
        key=lambda objective: (
            -objective.confidence,
            _OBJECTIVE_ORDER[objective.objective_type],
            objective.source_field.casefold(),
            objective.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
    if isinstance(payload.get("source_payload"), Mapping):
        source_payload = payload["source_payload"]
        for field_name in _SCANNED_FIELDS:
            if field_name in source_payload:
                _append_value(values, f"source_payload.{field_name}", source_payload[field_name])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _any_signal(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, child)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            values.append((source_field, segment))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for sentence in _SENTENCE_SPLIT_RE.split(value):
        segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _objective_types(text: str) -> list[DisasterRecoveryObjectiveType]:
    return [
        objective_type
        for objective_type, patterns in _OBJECTIVE_PATTERNS.items()
        if any(pattern.search(text) for pattern in patterns)
    ]


def _explicit_values(text: str) -> dict[DisasterRecoveryObjectiveType, str]:
    values: dict[DisasterRecoveryObjectiveType, str] = {}
    for match in _EXPLICIT_VALUE_RE.finditer(text):
        label = match.group(0).casefold()
        value = _clean_text(match.group("value"))
        if label.startswith("rto") or "recovery time objective" in label:
            values.setdefault("rto", value)
        if label.startswith("rpo") or "recovery point objective" in label:
            values.setdefault("rpo", value)
    return values


def _confidence(text: str, source_field: str, value: str | None) -> float:
    score = 0.58
    normalized_field = source_field.replace("-", "_").casefold()
    if any(
        marker in normalized_field
        for marker in ("success_criteria", "acceptance_criteria", "constraint", "risk", "metadata")
    ):
        score += 0.1
    if re.search(r"\b(?:must|shall|required|needs?|ensure|acceptance|done when|critical)\b", text, re.I):
        score += 0.1
    if re.search(r"\b(?:disaster recovery|business continuity|outage|incident|failover)\b", text, re.I):
        score += 0.06
    if value:
        score += 0.16
    elif re.search(r"\d", text):
        score += 0.04
    return round(min(score, 0.97), 2)


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (len(value), value.casefold()),
    )
    return values[0] if values else None


def _summary(objectives: tuple[SourceDisasterRecoveryObjective, ...]) -> dict[str, Any]:
    return {
        "objective_count": len(objectives),
        "highest_confidence": max((objective.confidence for objective in objectives), default=0.0),
        "objective_types": [objective.objective_type for objective in objectives],
        "objective_type_counts": {
            objective_type: sum(
                1 for objective in objectives if objective.objective_type == objective_type
            )
            for objective_type in _OBJECTIVE_ORDER
        },
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "summary",
        "problem",
        "problem_statement",
        "goal",
        "goals",
        "mvp_goal",
        "context",
        "workflow_context",
        "constraints",
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "risks",
        "metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _any_signal(text: str) -> bool:
    return any(
        pattern.search(text)
        for patterns in _OBJECTIVE_PATTERNS.values()
        for pattern in patterns
    )


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


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


__all__ = [
    "DisasterRecoveryObjectiveType",
    "SourceDisasterRecoveryObjective",
    "SourceDisasterRecoveryObjectivesReport",
    "build_source_disaster_recovery_objectives_report",
    "derive_source_disaster_recovery_objectives",
    "extract_source_disaster_recovery_objectives",
    "source_disaster_recovery_objectives_report_to_dict",
    "source_disaster_recovery_objectives_to_dicts",
]
