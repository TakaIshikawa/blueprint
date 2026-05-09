"""Extract backup and recovery requirements from source brief-shaped inputs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


BackupRequirementType = Literal[
    "frequency",
    "retention",
    "backup_type",
    "verification",
    "encryption",
    "geo_redundancy",
    "restore_testing",
    "point_in_time",
    "compliance",
]

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_MAX_EVIDENCE_PER_REQUIREMENT = 4
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
_REQUIREMENT_ORDER: dict[BackupRequirementType, int] = {
    "frequency": 0,
    "retention": 1,
    "backup_type": 2,
    "verification": 3,
    "encryption": 4,
    "geo_redundancy": 5,
    "restore_testing": 6,
    "point_in_time": 7,
    "compliance": 8,
}
_EXPLICIT_VALUE_RE = re.compile(
    r"\b(?:backup frequency|backup cadence|backup schedule)\b"
    r"\s*(?:is|of|:)?\s*"
    r"(?P<value>(?:every|each)?\s*\d+\s*"
    r"(?:milliseconds?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|"
    r"weeks?|w|months?|mo|years?|y)|"
    r"hourly|daily|weekly|monthly|yearly|continuously?|real[- ]?time)",
    re.I,
)
_RETENTION_VALUE_RE = re.compile(
    r"\b(?:retain|retention|keep|preserve|store)\b"
    r"\s+(?:for|period of)?\s*"
    r"(?P<value>\d+\s*(?:days?|d|weeks?|w|months?|mo|years?|y))",
    re.I,
)
_REQUIREMENT_PATTERNS: dict[BackupRequirementType, tuple[re.Pattern[str], ...]] = {
    "frequency": (
        re.compile(
            r"\b(?:backup frequency|backup cadence|backup schedule|backup interval)\b",
            re.I,
        ),
        re.compile(
            r"\b(?:backup|backups)\b.{0,30}\b(?:hourly|daily|weekly|monthly|continuously?|real[- ]?time)\b",
            re.I,
        ),
        re.compile(
            r"\b(?:hourly|daily|weekly|monthly|continuously?|real[- ]?time)\b.{0,30}\b(?:backup|backups)\b",
            re.I,
        ),
    ),
    "retention": (
        re.compile(
            r"\b(?:retention|retain|keep backup|preserve backup|backup retention|"
            r"retention period|retention policy|archive period|backup lifecycle)\b",
            re.I,
        ),
    ),
    "backup_type": (
        re.compile(
            r"\b(?:full backup|incremental backup|differential backup|"
            r"snapshot|backup type|backup strategy|backup method|"
            r"continuous data protection|cdp)\b",
            re.I,
        ),
        re.compile(
            r"\b(?:full|incremental|differential)\b.{0,20}\bbackup",
            re.I,
        ),
    ),
    "verification": (
        re.compile(
            r"\b(?:verify backup|backup verification|backup integrity|"
            r"validate backup|backup validation|checksum|backup health|"
            r"backup monitoring)\b",
            re.I,
        ),
    ),
    "encryption": (
        re.compile(
            r"\b(?:encrypt|encryption|encrypted|encrypted backup|backup encryption|"
            r"encryption at rest|encrypted at rest|data at rest|encrypted storage|"
            r"backup security|secure backup)\b",
            re.I,
        ),
    ),
    "geo_redundancy": (
        re.compile(
            r"\b(?:geo[- ]?redundan|geographic redundancy|cross[- ]region backup|"
            r"multi[- ]region backup|off[- ]?site backup|remote backup|"
            r"geographic distribution|geo[- ]?replication)\b",
            re.I,
        ),
    ),
    "restore_testing": (
        re.compile(
            r"\b(?:restore test|test restore|backup drill|restore drill|"
            r"recovery test|test recovery|restore verification|"
            r"restore validation|disaster recovery drill|dr drill)\b",
            re.I,
        ),
        re.compile(
            r"\btest\b.{0,20}\b(?:restore|restores|restoring)\b",
            re.I,
        ),
        re.compile(
            r"\b(?:restore|restores|restoring)\b.{0,20}\btest",
            re.I,
        ),
        re.compile(
            r"\b(?:drill|drills)\b",
            re.I,
        ),
    ),
    "point_in_time": (
        re.compile(
            r"\b(?:point[- ]in[- ]time|pit recovery|pitr|point[- ]in[- ]time recovery|"
            r"time travel|historical restore|timestamp restore|"
            r"restore to point)\b",
            re.I,
        ),
    ),
    "compliance": (
        re.compile(
            r"\b(?:compliance|regulatory|regulation|gdpr|hipaa|sox|pci|"
            r"audit requirement|legal requirement|data governance|"
            r"backup compliance)\b",
            re.I,
        ),
    ),
}
_FOLLOW_UPS: dict[BackupRequirementType, str] = {
    "frequency": "Confirm backup frequency aligns with RPO, automation tooling, and monitoring alerts.",
    "retention": "Define retention policy enforcement, lifecycle transitions, and deletion verification.",
    "backup_type": "Document backup type strategy, chain management, storage optimization, and restore priority.",
    "verification": "Establish verification cadence, integrity checks, alert thresholds, and remediation workflow.",
    "encryption": "Confirm encryption method, key management, rotation policy, and access controls.",
    "geo_redundancy": "Define regional distribution, replication lag tolerance, failover procedure, and cost implications.",
    "restore_testing": "Establish test cadence, success criteria, environment requirements, and test documentation.",
    "point_in_time": "Confirm granularity requirements, retention of transaction logs, and restore procedure.",
    "compliance": "Map backup requirements to compliance frameworks, audit evidence, and retention mandates.",
}


@dataclass(frozen=True, slots=True)
class BackupRequirement:
    """One backup requirement found in source brief evidence."""

    requirement_type: BackupRequirementType
    value: str | None = None
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: float = 0.0
    recommended_follow_up: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "value": self.value,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "recommended_follow_up": self.recommended_follow_up,
        }


@dataclass(frozen=True, slots=True)
class BackupRequirementsReport:
    """Source-level backup requirements report."""

    source_brief_id: str | None = None
    title: str | None = None
    requirements: tuple[BackupRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[BackupRequirement, ...]:
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
        """Return requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]


def build_backup_requirements_report(
    source: Mapping[str, Any] | SourceBrief | object,
) -> BackupRequirementsReport:
    """Build a backup requirements report from a source brief-like payload."""
    source_brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return BackupRequirementsReport(
        source_brief_id=source_brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def derive_backup_requirements(
    source: Mapping[str, Any] | SourceBrief | object,
) -> tuple[BackupRequirement, ...]:
    """Return backup requirement records from brief-shaped input."""
    return build_backup_requirements_report(source).requirements


def extract_backup_requirements(
    source: Mapping[str, Any] | SourceBrief | object,
) -> tuple[BackupRequirement, ...]:
    """Alias for callers that use extract_* naming."""
    return derive_backup_requirements(source)


def backup_requirements_report_to_dict(
    report: BackupRequirementsReport,
) -> dict[str, Any]:
    """Serialize a backup requirements report to a plain dictionary."""
    return report.to_dict()


backup_requirements_report_to_dict.__test__ = False


def backup_requirements_to_dicts(
    requirements: (
        tuple[BackupRequirement, ...]
        | list[BackupRequirement]
        | BackupRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize backup requirement records to dictionaries."""
    if isinstance(requirements, BackupRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


backup_requirements_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: BackupRequirementType
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


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_field, segment in _candidate_segments(payload):
        requirement_types = _requirement_types(segment)
        if not requirement_types:
            continue
        explicit_values = _explicit_values(segment)
        evidence = _evidence_snippet(source_field, segment)
        for requirement_type in requirement_types:
            value = explicit_values.get(requirement_type)
            candidates.append(
                _Candidate(
                    requirement_type=requirement_type,
                    value=value,
                    source_field=source_field,
                    evidence=evidence,
                    confidence=_confidence(segment, source_field, value, requirement_type),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[BackupRequirement]:
    grouped: dict[BackupRequirementType, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.requirement_type, []).append(candidate)

    requirements: list[BackupRequirement] = []
    for requirement_type in _REQUIREMENT_ORDER:
        items = grouped.get(requirement_type, [])
        if not items:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in items))[
            :_MAX_EVIDENCE_PER_REQUIREMENT
        ]
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda value: value.casefold(),
        )[0]
        value = _best_value(items)
        requirements.append(
            BackupRequirement(
                requirement_type=requirement_type,
                value=value,
                source_field=source_field,
                evidence=evidence,
                confidence=round(max(item.confidence for item in items), 2),
                recommended_follow_up=_FOLLOW_UPS[requirement_type],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            -requirement.confidence,
            _REQUIREMENT_ORDER[requirement.requirement_type],
            requirement.source_field.casefold(),
            requirement.evidence,
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


def _requirement_types(text: str) -> list[BackupRequirementType]:
    return [
        requirement_type
        for requirement_type, patterns in _REQUIREMENT_PATTERNS.items()
        if any(pattern.search(text) for pattern in patterns)
    ]


def _explicit_values(text: str) -> dict[BackupRequirementType, str]:
    values: dict[BackupRequirementType, str] = {}

    # Extract frequency values
    for match in _EXPLICIT_VALUE_RE.finditer(text):
        value = _clean_text(match.group("value"))
        values.setdefault("frequency", value)

    # Extract retention values
    for match in _RETENTION_VALUE_RE.finditer(text):
        value = _clean_text(match.group("value"))
        values.setdefault("retention", value)

    return values


def _confidence(text: str, source_field: str, value: str | None, requirement_type: BackupRequirementType) -> float:
    score = 0.58
    normalized_field = source_field.replace("-", "_").casefold()
    if any(
        marker in normalized_field
        for marker in ("success_criteria", "acceptance_criteria", "constraint", "risk", "metadata")
    ):
        score += 0.1
    if re.search(r"\b(?:must|shall|required|needs?|ensure|acceptance|done when|critical)\b", text, re.I):
        score += 0.1
    if re.search(r"\b(?:backup|recovery|restore|disaster|resilience|continuity)\b", text, re.I):
        score += 0.06
    if value:
        score += 0.16
    elif re.search(r"\d", text):
        score += 0.04

    # Bonus for specific high-value requirements
    if requirement_type in ("encryption", "geo_redundancy") and re.search(
        r"\b(?:encryption|encrypt|geographic|geo[- ]?redundan|cross[- ]region)\b", text, re.I
    ):
        score += 0.05

    return round(min(score, 0.97), 2)


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (len(value), value.casefold()),
    )
    return values[0] if values else None


def _summary(requirements: tuple[BackupRequirement, ...]) -> dict[str, Any]:
    """Calculate summary including completeness score."""
    requirement_type_counts = {
        requirement_type: sum(
            1 for requirement in requirements if requirement.requirement_type == requirement_type
        )
        for requirement_type in _REQUIREMENT_ORDER
    }

    # Calculate completeness score based on coverage of key areas
    completeness_score = 0.0
    max_score = 0.0

    # Core requirements (weighted more heavily)
    core_requirements = {
        "frequency": 0.20,  # RPO/RTO alignment
        "retention": 0.15,
        "backup_type": 0.10,
    }

    # Security and reliability (weighted moderately)
    security_requirements = {
        "encryption": 0.15,
        "geo_redundancy": 0.10,
        "verification": 0.10,
    }

    # Testing and compliance (important but supplementary)
    supplementary_requirements = {
        "restore_testing": 0.10,
        "point_in_time": 0.05,
        "compliance": 0.05,
    }

    all_weights = {**core_requirements, **security_requirements, **supplementary_requirements}

    for req_type, weight in all_weights.items():
        max_score += weight
        if requirement_type_counts.get(req_type, 0) > 0:
            # Get highest confidence for this requirement type
            matching_reqs = [r for r in requirements if r.requirement_type == req_type]
            if matching_reqs:
                best_confidence = max(r.confidence for r in matching_reqs)
                completeness_score += weight * best_confidence

    # Normalize to 0-1 scale
    normalized_completeness = round(completeness_score / max_score, 2) if max_score > 0 else 0.0

    return {
        "requirement_count": len(requirements),
        "highest_confidence": round(max((requirement.confidence for requirement in requirements), default=0.0), 2),
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "requirement_type_counts": requirement_type_counts,
        "completeness_score": normalized_completeness,
        "has_rpo_rto_alignment": requirement_type_counts.get("frequency", 0) > 0,
        "has_testing_coverage": requirement_type_counts.get("restore_testing", 0) > 0,
        "has_automation": requirement_type_counts.get("verification", 0) > 0,
        "has_security_measures": (
            requirement_type_counts.get("encryption", 0) > 0
            or requirement_type_counts.get("geo_redundancy", 0) > 0
        ),
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
        for patterns in _REQUIREMENT_PATTERNS.values()
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
    "BackupRequirementType",
    "BackupRequirement",
    "BackupRequirementsReport",
    "build_backup_requirements_report",
    "derive_backup_requirements",
    "extract_backup_requirements",
    "backup_requirements_report_to_dict",
    "backup_requirements_to_dicts",
]
