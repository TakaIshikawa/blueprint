"""Extract source-level migration and cutover requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


MigrationCutoverConcernCategory = Literal[
    "data_migration",
    "backfill",
    "cutover",
    "dual_write",
    "import_export",
    "downtime",
    "rollback",
    "reconciliation",
]
MigrationCutoverConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[MigrationCutoverConcernCategory, ...] = (
    "data_migration",
    "backfill",
    "cutover",
    "dual_write",
    "import_export",
    "downtime",
    "rollback",
    "reconciliation",
)
_CONFIDENCE_ORDER: dict[MigrationCutoverConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|define|document|plan|sequence|schedule|coordinate|"
    r"validate|verify|monitor|before launch|cannot ship|done when|acceptance)\b",
    re.I,
)
_MIGRATION_CONTEXT_RE = re.compile(
    r"\b(?:migration|migrate|backfills?|backfilled|backfilling|cutover|cut over|dual[- ]?write|dual write|"
    r"shadow write|rollback|roll back|downtime|maintenance window|reconciliation|"
    r"reconcile|parity|import|export|csv import|data load|legacy data|legacy system|"
    r"source system|target system|switchover|go[- ]live|data copy)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:migration|cutover|backfill|dual[-_ ]?write|rollback|downtime|reconciliation|"
    r"import|export|legacy|data[-_ ]?migration|data[-_ ]?requirements?|constraints?|"
    r"requirements?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|"
    r"source[-_ ]?payload)",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,120}"
    r"\b(?:migration|backfill|cutover|dual[- ]?write|rollback|downtime|"
    r"reconciliation|import|export)\b.{0,120}"
    r"\b(?:required|needed|in scope|changes?|work|support|planned|for this release)\b|"
    r"\b(?:migration|backfill|cutover|dual[- ]?write|rollback|downtime|"
    r"reconciliation|import|export)\b.{0,120}"
    r"\b(?:out of scope|not required|not needed|no changes?|no work|non-goal|non goal)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:minutes?|hours?|days?|weeks?|months?)|"
    r"\d+(?:st|nd|rd|th)\s+(?:day|week|month)|"
    r"(?:zero|no)\s+downtime|read[- ]?only|maintenance window|"
    r"phase\s+\d+|v\d+(?:\.\d+)*|batch(?:es)?\s+of\s+\d+|"
    r"\d+\s*(?:records?|rows?|accounts?|tenants?|percent|%))\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:minutes?|hours?|days?|weeks?|months?)\b", re.I)
_NEGATION_CONTEXT_RE = re.compile(r"\b(?:no|not|without|out of scope|non-goal|non goal)\b", re.I)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
}

_CATEGORY_PATTERNS: dict[MigrationCutoverConcernCategory, re.Pattern[str]] = {
    "data_migration": re.compile(
        r"\b(?:data migration|migrate(?:s|d)? data|migration path|"
        r"legacy data|move data|copy data|data copy|source system.{0,80}target system|"
        r"target system.{0,80}source system|schema migration)\b",
        re.I,
    ),
    "backfill": re.compile(
        r"\b(?:backfill(?:s|ed|ing)?|historical fill|historical data load|"
        r"load historical|reprocess historical|catch[- ]?up job|catch up job)\b",
        re.I,
    ),
    "cutover": re.compile(
        r"\b(?:cutover|cut over|switchover|switch over|go[- ]live|launch window|"
        r"traffic shift|flip(?:ping)? the switch|promote.{0,60}new system)\b",
        re.I,
    ),
    "dual_write": re.compile(
        r"\b(?:dual[- ]?write|dual write|write to both|writes? to both|shadow write|"
        r"mirror writes?|parallel writes?|old and new stores?|new and old stores?)\b",
        re.I,
    ),
    "import_export": re.compile(
        r"\b(?:import/export|export/import|import(?:s|ed|ing)?|export(?:s|ed|ing)?|"
        r"csv import|csv export|bulk import|bulk export|data import|data export|"
        r"portable export|migration file)\b",
        re.I,
    ),
    "downtime": re.compile(
        r"\b(?:downtime|zero[- ]?downtime|no downtime|maintenance window|read[- ]?only window|"
        r"service interruption|offline window|freeze writes?|write freeze|availability impact)\b",
        re.I,
    ),
    "rollback": re.compile(
        r"\b(?:rollback|roll back|rollbacks|revert plan|restore previous|fall back|fallback plan|"
        r"abort migration|undo migration|backout|back out)\b",
        re.I,
    ),
    "reconciliation": re.compile(
        r"\b(?:reconciliation|reconcile|reconciled|parity check|data parity|checksum|"
        r"row counts?|record counts?|compare counts?|diff report|audit totals?|"
        r"validate migrated data|verify migrated data)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class SourceMigrationCutoverRequirement:
    """One source-backed migration or cutover planning requirement."""

    concern_category: MigrationCutoverConcernCategory
    value: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: MigrationCutoverConfidence = "medium"
    source_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "concern_category": self.concern_category,
            "value": self.value,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "source_id": self.source_id,
        }


@dataclass(frozen=True, slots=True)
class SourceMigrationCutoverRequirementsReport:
    """Source-level migration and cutover requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceMigrationCutoverRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceMigrationCutoverRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return migration cutover requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Migration Cutover Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Source count: {self.summary.get('source_count', 0)}",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Category counts: "
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}"
                for category in _CATEGORY_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}"
                for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No migration cutover requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Confidence | Source | Evidence |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.concern_category} | "
                f"{_markdown_cell(requirement.value)} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_id or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_migration_cutover_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMigrationCutoverRequirementsReport:
    """Extract source-level migration cutover requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceMigrationCutoverRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def generate_source_migration_cutover_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMigrationCutoverRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_migration_cutover_requirements(source)


def derive_source_migration_cutover_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMigrationCutoverRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_migration_cutover_requirements(source)


def extract_source_migration_cutover_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> tuple[SourceMigrationCutoverRequirement, ...]:
    """Return migration cutover requirement records extracted from brief-shaped input."""
    return build_source_migration_cutover_requirements(source).requirements


def summarize_source_migration_cutover_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceMigrationCutoverRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted migration cutover requirements."""
    if isinstance(source_or_result, SourceMigrationCutoverRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_migration_cutover_requirements(source_or_result).summary


def source_migration_cutover_requirements_to_dict(
    report: SourceMigrationCutoverRequirementsReport,
) -> dict[str, Any]:
    """Serialize a migration cutover requirements report to a plain dictionary."""
    return report.to_dict()


source_migration_cutover_requirements_to_dict.__test__ = False


def source_migration_cutover_requirements_to_dicts(
    requirements: (
        tuple[SourceMigrationCutoverRequirement, ...]
        | list[SourceMigrationCutoverRequirement]
        | SourceMigrationCutoverRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize migration cutover requirement records to dictionaries."""
    if isinstance(requirements, SourceMigrationCutoverRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_migration_cutover_requirements_to_dicts.__test__ = False


def source_migration_cutover_requirements_to_markdown(
    report: SourceMigrationCutoverRequirementsReport,
) -> str:
    """Render a migration cutover requirements report as Markdown."""
    return report.to_markdown()


source_migration_cutover_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    concern_category: MigrationCutoverConcernCategory
    value: str
    evidence: str
    confidence: MigrationCutoverConfidence
    source_id: str | None


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


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _brief_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _brief_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _brief_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _brief_id(payload), payload
    return None, {}


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            if not _is_requirement(segment):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            categories = [
                category
                for category in _CATEGORY_ORDER
                if _CATEGORY_PATTERNS[category].search(searchable)
            ]
            for category in _dedupe(categories):
                candidates.append(
                    _Candidate(
                        concern_category=category,
                        value=_value(segment.text),
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(segment),
                        source_id=source_id,
                    )
                )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceMigrationCutoverRequirement]:
    grouped: dict[tuple[str | None, MigrationCutoverConcernCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_id, candidate.concern_category), []).append(candidate)

    requirements: list[SourceMigrationCutoverRequirement] = []
    for (source_id, category), items in grouped.items():
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
        )[:5]
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        requirements.append(
            SourceMigrationCutoverRequirement(
                concern_category=category,
                value=_strongest_value(items),
                evidence=evidence,
                confidence=confidence,
                source_id=source_id,
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_id) or "",
            _CATEGORY_ORDER.index(requirement.concern_category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.value.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in (
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
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "migration",
        "cutover",
        "backfill",
        "rollback",
        "reconciliation",
        "import_export",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(
    segments: list[_Segment],
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _MIGRATION_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text, segment_context in _segments(text, field_context):
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
            section_context = inherited_context or bool(
                _MIGRATION_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            clauses = (
                [part]
                if _NEGATION_CONTEXT_RE.search(part) and _MIGRATION_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if _MIGRATION_CONTEXT_RE.search(segment.text) and re.search(
        r"\b(?:backfills?|backfilled|migrates?|migrated|reconciles?|reconciled|"
        r"exports?|exported|imports?|imported)\b",
        segment.text,
        re.I,
    ):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return True
    return False


def _value(text: str) -> str:
    if match := _DURATION_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    if match := _VALUE_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    return _clean_text(text)


def _strongest_value(items: Iterable[_Candidate]) -> str:
    ordered = sorted(
        items,
        key=lambda item: (
            _CONFIDENCE_ORDER[item.confidence],
            0 if _VALUE_RE.search(item.value) else 1,
            len(item.value),
            item.value.casefold(),
        ),
    )
    return ordered[0].value


def _confidence(segment: _Segment) -> MigrationCutoverConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _VALUE_RE.search(segment.text) and (_REQUIREMENT_RE.search(segment.text) or segment.section_context):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _MIGRATION_CONTEXT_RE.search(searchable):
        return "high"
    if _MIGRATION_CONTEXT_RE.search(searchable):
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceMigrationCutoverRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(
                1 for requirement in requirements if requirement.concern_category == category
            )
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "concern_categories": [requirement.concern_category for requirement in requirements],
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
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "requirements",
        "constraints",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "migration",
        "cutover",
        "backfill",
        "rollback",
        "reconciliation",
        "import_export",
        "metadata",
        "brief_metadata",
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
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = str(value).strip()
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
    return deduped


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
    "MigrationCutoverConcernCategory",
    "MigrationCutoverConfidence",
    "SourceMigrationCutoverRequirement",
    "SourceMigrationCutoverRequirementsReport",
    "build_source_migration_cutover_requirements",
    "derive_source_migration_cutover_requirements",
    "extract_source_migration_cutover_requirements",
    "generate_source_migration_cutover_requirements",
    "source_migration_cutover_requirements_to_dict",
    "source_migration_cutover_requirements_to_dicts",
    "source_migration_cutover_requirements_to_markdown",
    "summarize_source_migration_cutover_requirements",
]
