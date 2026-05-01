"""Build chronological timelines for requirement and task source evidence."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, SourceBrief


EvidenceReferenceType = Literal["requirement", "task", "source"]
EvidenceTimelineStatus = Literal["current", "superseded", "contradictory", "undated"]
_T = TypeVar("_T")

_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}(?:[T ][0-9:.+-]+Z?)?\b")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_DATE_KEYS = (
    "date",
    "evidence_date",
    "observed_at",
    "captured_at",
    "collected_at",
    "published_at",
    "reported_at",
    "updated_at",
    "created_at",
    "reviewed_at",
)
_EVIDENCE_KEYS = (
    "evidence",
    "source_evidence",
    "requirements",
    "requirement_evidence",
    "references",
)
_TEXT_KEYS = ("claim", "summary", "text", "title", "decision", "note", "description")
_REFERENCE_KEYS = (
    "requirement_id",
    "requirement_ref",
    "requirement",
    "task_id",
    "task_ref",
)
_NEGATORS = {
    "cannot",
    "disable",
    "disabled",
    "exclude",
    "excluded",
    "never",
    "no",
    "not",
    "remove",
    "removed",
    "skip",
    "skipped",
    "without",
    "wont",
    "won",
}
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "with",
}


@dataclass(frozen=True, slots=True)
class SourceEvidenceTimelineEntry:
    """One normalized source evidence item in a group timeline."""

    reference_id: str
    reference_type: EvidenceReferenceType
    source_id: str
    source_kind: str
    label: str
    text: str
    evidence_date: str | None = None
    status: EvidenceTimelineStatus = "current"
    supersedes_source_ids: tuple[str, ...] = field(default_factory=tuple)
    contradicted_by_source_ids: tuple[str, ...] = field(default_factory=tuple)
    contradicts_source_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "reference_id": self.reference_id,
            "reference_type": self.reference_type,
            "source_id": self.source_id,
            "source_kind": self.source_kind,
            "label": self.label,
            "text": self.text,
            "evidence_date": self.evidence_date,
            "status": self.status,
            "supersedes_source_ids": list(self.supersedes_source_ids),
            "contradicted_by_source_ids": list(self.contradicted_by_source_ids),
            "contradicts_source_ids": list(self.contradicts_source_ids),
        }


@dataclass(frozen=True, slots=True)
class SourceEvidenceTimelineGroup:
    """Timeline of evidence for one requirement, task, or source reference."""

    reference_id: str
    reference_type: EvidenceReferenceType
    entries: tuple[SourceEvidenceTimelineEntry, ...] = field(default_factory=tuple)
    current_source_ids: tuple[str, ...] = field(default_factory=tuple)
    superseded_source_ids: tuple[str, ...] = field(default_factory=tuple)
    contradictory_source_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "reference_id": self.reference_id,
            "reference_type": self.reference_type,
            "entries": [entry.to_dict() for entry in self.entries],
            "current_source_ids": list(self.current_source_ids),
            "superseded_source_ids": list(self.superseded_source_ids),
            "contradictory_source_ids": list(self.contradictory_source_ids),
        }


def build_source_evidence_timeline(
    source_briefs: (
        Mapping[str, Any]
        | SourceBrief
        | Iterable[Mapping[str, Any] | SourceBrief]
        | None
    ) = None,
    imported_notes: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    execution_plan: Mapping[str, Any] | ExecutionPlan | None = None,
    *,
    plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> tuple[SourceEvidenceTimelineGroup, ...]:
    """Build grouped evidence timelines from sources, notes, and plan metadata."""
    raw_entries: list[_RawEvidenceEntry] = []
    raw_entries.extend(_source_brief_entries(source_briefs))
    raw_entries.extend(_imported_note_entries(imported_notes))
    raw_entries.extend(_plan_entries(plan if plan is not None else execution_plan))

    groups: dict[tuple[str, EvidenceReferenceType], list[_RawEvidenceEntry]] = {}
    for entry in raw_entries:
        groups.setdefault((entry.reference_id, entry.reference_type), []).append(entry)

    timeline_groups = [
        _timeline_group(reference_id, reference_type, entries)
        for (reference_id, reference_type), entries in groups.items()
    ]
    timeline_groups.sort(key=lambda group: (group.reference_type, group.reference_id))
    return tuple(timeline_groups)


def source_evidence_timeline_to_dicts(
    timeline: Iterable[SourceEvidenceTimelineGroup],
) -> list[dict[str, Any]]:
    """Serialize evidence timeline groups to plain dictionaries."""
    return [group.to_dict() for group in timeline]


source_evidence_timeline_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _RawEvidenceEntry:
    reference_id: str
    reference_type: EvidenceReferenceType
    source_id: str
    source_kind: str
    label: str
    text: str
    evidence_date: date | None = None
    sequence: int = 0
    supersedes_source_ids: tuple[str, ...] = field(default_factory=tuple)
    contradicts_source_ids: tuple[str, ...] = field(default_factory=tuple)


def _source_brief_entries(
    source_briefs: (
        Mapping[str, Any]
        | SourceBrief
        | Iterable[Mapping[str, Any] | SourceBrief]
        | None
    ),
) -> list[_RawEvidenceEntry]:
    entries: list[_RawEvidenceEntry] = []
    for index, brief in enumerate(_source_briefs(source_briefs), start=1):
        source_id = _source_id(brief, fallback=f"source-brief-{index}")
        context = _SourceContext(
            source_id=source_id,
            source_kind=_text(brief.get("source_entity_type")) or "source_brief",
            default_reference_id=source_id,
            default_reference_type="source",
            sequence_base=index * 1000,
        )
        entries.extend(
            _evidence_entries(
                _evidence_container(brief),
                context=context,
                prefix="source_brief",
            )
        )
        payload = brief.get("source_payload")
        if isinstance(payload, Mapping):
            entries.extend(
                _evidence_entries(
                    _evidence_container(payload),
                    context=context,
                    prefix="source_payload",
                )
            )
    return entries


def _imported_note_entries(
    imported_notes: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None,
) -> list[_RawEvidenceEntry]:
    entries: list[_RawEvidenceEntry] = []
    for index, note in enumerate(_mappings(imported_notes), start=1):
        source_id = _text(note.get("source_id")) or _text(note.get("id")) or f"note-{index}"
        context = _SourceContext(
            source_id=source_id,
            source_kind=_text(note.get("source_kind")) or _text(note.get("type")) or "note",
            default_reference_id=_text(note.get("requirement_id"))
            or _text(note.get("task_id"))
            or source_id,
            default_reference_type=_reference_type(note, default="source"),
            sequence_base=100000 + index * 1000,
        )
        entries.extend(_evidence_entries(note, context=context, prefix="note"))
    return entries


def _plan_entries(plan: Mapping[str, Any] | ExecutionPlan | None) -> list[_RawEvidenceEntry]:
    if plan is None:
        return []
    payload = _plan_payload(plan)
    plan_id = _text(payload.get("id")) or "plan"
    entries: list[_RawEvidenceEntry] = []
    context = _SourceContext(
        source_id=plan_id,
        source_kind="plan",
        default_reference_id=plan_id,
        default_reference_type="source",
        sequence_base=200000,
    )
    entries.extend(
        _evidence_entries(
            payload.get("metadata"),
            context=context,
            prefix="plan.metadata",
        )
    )

    for index, task in enumerate(_task_payloads(payload.get("tasks")), start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        task_context = _SourceContext(
            source_id=f"{plan_id}:{task_id}",
            source_kind="plan_task",
            default_reference_id=task_id,
            default_reference_type="task",
            sequence_base=210000 + index * 1000,
        )
        entries.extend(
            _evidence_entries(
                task.get("metadata"),
                context=task_context,
                prefix=f"tasks[{index - 1}].metadata",
            )
        )
    return entries


@dataclass(frozen=True, slots=True)
class _SourceContext:
    source_id: str
    source_kind: str
    default_reference_id: str
    default_reference_type: EvidenceReferenceType
    sequence_base: int


def _evidence_entries(
    value: Any,
    *,
    context: _SourceContext,
    prefix: str,
) -> list[_RawEvidenceEntry]:
    records = _evidence_records(value, prefix=prefix)
    entries: list[_RawEvidenceEntry] = []
    for offset, (label, record) in enumerate(records):
        entry = _raw_entry(record, label=label, context=context, sequence=offset)
        if entry is not None:
            entries.append(entry)
    return entries


def _evidence_container(value: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value[key] for key in _EVIDENCE_KEYS if key in value}


def _evidence_records(value: Any, *, prefix: str) -> list[tuple[str, Any]]:
    records: list[tuple[str, Any]] = []

    def append(current: Any, path: str, *, force_record: bool = False) -> None:
        if isinstance(current, Mapping):
            if force_record or _is_evidence_record(current):
                records.append((path, dict(current)))
                return
            for key in sorted(current, key=str):
                append(current[key], f"{path}.{key}", force_record=str(key) in _EVIDENCE_KEYS)
            return
        if isinstance(current, (list, tuple)):
            for index, item in enumerate(current):
                append(item, f"{path}[{index}]", force_record=force_record)
            return
        if force_record and _optional_text(current):
            records.append((path, current))

    append(value, prefix)
    return records


def _raw_entry(
    value: Any,
    *,
    label: str,
    context: _SourceContext,
    sequence: int,
) -> _RawEvidenceEntry | None:
    record = value if isinstance(value, Mapping) else {}
    text = _entry_text(value)
    if not text:
        return None
    source_id = (
        _optional_text(record.get("source_id"))
        or _optional_text(record.get("id"))
        or context.source_id
    )
    reference_id = _reference_id(record) or context.default_reference_id
    reference_type = _reference_type(record, default=context.default_reference_type)
    return _RawEvidenceEntry(
        reference_id=reference_id,
        reference_type=reference_type,
        source_id=source_id,
        source_kind=_optional_text(record.get("source_kind")) or context.source_kind,
        label=label,
        text=text,
        evidence_date=_date_from_value(value),
        sequence=context.sequence_base + sequence,
        supersedes_source_ids=tuple(
            _dedupe(
                _strings(
                    _first_present(
                        record,
                        ("supersedes", "supersedes_source_ids", "superseded_source_ids"),
                    )
                )
            )
        ),
        contradicts_source_ids=tuple(
            _dedupe(
                _strings(
                    _first_present(
                        record,
                        ("contradicts", "contradicts_source_ids", "contradiction_of"),
                    )
                )
            )
        ),
    )


def _timeline_group(
    reference_id: str,
    reference_type: EvidenceReferenceType,
    entries: list[_RawEvidenceEntry],
) -> SourceEvidenceTimelineGroup:
    ordered = sorted(entries, key=_raw_sort_key)
    superseded_by_source_id: dict[str, list[str]] = {}
    contradicts_by_source_id: dict[str, list[str]] = {}
    contradicted_by_source_id: dict[str, list[str]] = {}

    for index, newer in enumerate(ordered):
        for older_id in newer.supersedes_source_ids:
            superseded_by_source_id.setdefault(older_id, []).append(newer.source_id)
        explicit_contradictions = list(newer.contradicts_source_ids)
        inferred_contradictions = [
            older.source_id
            for older in ordered[:index]
            if _contradicts(newer.text, older.text)
        ]
        for older_id in _dedupe([*explicit_contradictions, *inferred_contradictions]):
            contradicts_by_source_id.setdefault(newer.source_id, []).append(older_id)
            contradicted_by_source_id.setdefault(older_id, []).append(newer.source_id)

    timeline_entries: list[SourceEvidenceTimelineEntry] = []
    for entry in ordered:
        superseded_by = tuple(_dedupe(superseded_by_source_id.get(entry.source_id, [])))
        contradicts = tuple(_dedupe(contradicts_by_source_id.get(entry.source_id, [])))
        contradicted_by = tuple(_dedupe(contradicted_by_source_id.get(entry.source_id, [])))
        status = _status(
            entry,
            superseded_by=superseded_by,
            contradicts=contradicts,
            contradicted_by=contradicted_by,
        )
        timeline_entries.append(
            SourceEvidenceTimelineEntry(
                reference_id=entry.reference_id,
                reference_type=entry.reference_type,
                source_id=entry.source_id,
                source_kind=entry.source_kind,
                label=entry.label,
                text=entry.text,
                evidence_date=entry.evidence_date.isoformat() if entry.evidence_date else None,
                status=status,
                supersedes_source_ids=entry.supersedes_source_ids,
                contradicted_by_source_ids=contradicted_by,
                contradicts_source_ids=contradicts,
            )
        )

    return SourceEvidenceTimelineGroup(
        reference_id=reference_id,
        reference_type=reference_type,
        entries=tuple(timeline_entries),
        current_source_ids=tuple(
            entry.source_id for entry in timeline_entries if entry.status == "current"
        ),
        superseded_source_ids=tuple(
            entry.source_id for entry in timeline_entries if entry.status == "superseded"
        ),
        contradictory_source_ids=tuple(
            entry.source_id for entry in timeline_entries if entry.status == "contradictory"
        ),
    )


def _status(
    entry: _RawEvidenceEntry,
    *,
    superseded_by: tuple[str, ...],
    contradicts: tuple[str, ...],
    contradicted_by: tuple[str, ...],
) -> EvidenceTimelineStatus:
    if contradicts or contradicted_by:
        return "contradictory"
    if superseded_by:
        return "superseded"
    if entry.evidence_date is None:
        return "undated"
    return "current"


def _contradicts(newer_text: str, older_text: str) -> bool:
    newer_tokens = _tokens(newer_text)
    older_tokens = _tokens(older_text)
    if not newer_tokens or not older_tokens:
        return False
    if bool(newer_tokens & _NEGATORS) == bool(older_tokens & _NEGATORS):
        return False
    newer_signal = newer_tokens - _NEGATORS
    older_signal = older_tokens - _NEGATORS
    overlap = newer_signal & older_signal
    if len(overlap) >= 3:
        return True
    denominator = max(1, min(len(newer_signal), len(older_signal)))
    return len(overlap) / denominator >= 0.6


def _source_briefs(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | Iterable[Mapping[str, Any] | SourceBrief]
        | None
    ),
) -> list[dict[str, Any]]:
    if source is None:
        return []
    if isinstance(source, SourceBrief):
        return [source.model_dump(mode="python")]
    if isinstance(source, Mapping):
        if "source_briefs" in source:
            return _source_briefs(source.get("source_briefs"))  # type: ignore[arg-type]
        return [_source_brief_payload(source)]

    briefs: list[dict[str, Any]] = []
    try:
        iterator = iter(source)
    except TypeError:
        return []
    for item in iterator:
        if isinstance(item, SourceBrief):
            briefs.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            briefs.append(_source_brief_payload(item))
    return briefs


def _source_brief_payload(source_brief: Mapping[str, Any]) -> dict[str, Any]:
    try:
        value = SourceBrief.model_validate(source_brief).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(source_brief)


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _mappings(
    value: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        if "imported_notes" in value:
            return _mappings(value.get("imported_notes"))  # type: ignore[arg-type]
        return [dict(value)]
    mappings: list[dict[str, Any]] = []
    try:
        iterator = iter(value)
    except TypeError:
        return []
    for item in iterator:
        if isinstance(item, Mapping):
            mappings.append(dict(item))
    return mappings


def _is_evidence_record(value: Mapping[str, Any]) -> bool:
    keys = {str(key) for key in value}
    return bool(keys & {*_TEXT_KEYS, *_REFERENCE_KEYS, "supersedes", "contradicts"})


def _entry_text(value: Any) -> str:
    if isinstance(value, Mapping):
        for key in _TEXT_KEYS:
            if text := _optional_text(value.get(key)):
                return text
        return _optional_text(value) or ""
    return _optional_text(value) or ""


def _reference_id(value: Mapping[str, Any]) -> str | None:
    for key in _REFERENCE_KEYS:
        if text := _optional_text(value.get(key)):
            return text
    return None


def _reference_type(
    value: Mapping[str, Any],
    *,
    default: EvidenceReferenceType,
) -> EvidenceReferenceType:
    if _optional_text(value.get("task_id")) or _optional_text(value.get("task_ref")):
        return "task"
    if (
        _optional_text(value.get("requirement_id"))
        or _optional_text(value.get("requirement_ref"))
        or _optional_text(value.get("requirement"))
    ):
        return "requirement"
    declared = (_optional_text(value.get("reference_type")) or "").lower()
    if declared in {"requirement", "task", "source"}:
        return declared  # type: ignore[return-value]
    return default


def _date_from_value(value: Any) -> date | None:
    direct = _parse_date(value)
    if direct is not None:
        return direct
    if isinstance(value, Mapping):
        for key in _DATE_KEYS:
            if key in value and (parsed := _parse_date(value[key])) is not None:
                return parsed
        for key in sorted(value, key=str):
            child = value[key]
            if isinstance(child, Mapping | list | tuple):
                parsed = _date_from_value(child)
                if parsed is not None:
                    return parsed
    if isinstance(value, (list, tuple)):
        for item in value:
            parsed = _date_from_value(item)
            if parsed is not None:
                return parsed
    return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None

    for match in _DATE_RE.finditer(value.strip()):
        candidate = match.group(0)
        if candidate.endswith("Z"):
            candidate = f"{candidate[:-1]}+00:00"
        try:
            return datetime.fromisoformat(candidate).date()
        except ValueError:
            try:
                return date.fromisoformat(candidate[:10])
            except ValueError:
                continue
    return None


def _source_id(value: Mapping[str, Any], *, fallback: str) -> str:
    return (
        _text(value.get("source_id"))
        or _text(value.get("id"))
        or _text(value.get("url"))
        or fallback
    )


def _raw_sort_key(entry: _RawEvidenceEntry) -> tuple[Any, ...]:
    return (
        entry.evidence_date is None,
        entry.evidence_date or date.max,
        entry.sequence,
        entry.source_id,
        entry.label,
        entry.text,
    )


def _first_present(value: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in value:
            return value[key]
    return None


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=str):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _tokens(value: str) -> set[str]:
    return {
        token
        for token in _TOKEN_RE.findall(value.lower().replace("'", ""))
        if token not in _STOPWORDS
    }


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Mapping):
        return " ".join(
            part for key in sorted(value, key=str) if (part := _text(value[key]))
        )
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return " ".join(part for item in items if (part := _text(item)))
    return " ".join(str(value).split())


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "EvidenceReferenceType",
    "EvidenceTimelineStatus",
    "SourceEvidenceTimelineEntry",
    "SourceEvidenceTimelineGroup",
    "build_source_evidence_timeline",
    "source_evidence_timeline_to_dicts",
]
