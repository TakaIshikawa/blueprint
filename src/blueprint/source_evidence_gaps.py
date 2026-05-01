"""Find execution-plan tasks that lack traceable source evidence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SourceEvidenceGapType = Literal[
    "acceptance_criteria",
    "files_or_modules",
    "risks",
    "metadata_traceability",
]
_T = TypeVar("_T")

_GAP_ORDER: dict[SourceEvidenceGapType, int] = {
    "acceptance_criteria": 0,
    "files_or_modules": 1,
    "risks": 2,
    "metadata_traceability": 3,
}
_SOURCE_KEYS = (
    "source_ids",
    "source_id",
    "source_links",
    "source_link",
    "evidence",
    "source_evidence",
    "requirement_ids",
    "requirement_id",
    "requirement_refs",
    "requirement_ref",
    "traceability",
)
_EVIDENCE_RECORD_KEYS = {
    "field",
    "field_name",
    "category",
    "gap_type",
    "reference_type",
    "task_field",
}
_ALL_FIELD_ALIASES = frozenset(
    alias
    for aliases in (
        ("acceptance_criteria", "acceptance", "criteria"),
        ("files_or_modules", "files", "modules", "file"),
        ("risks", "risk", "risk_level", "risk_notes"),
        ("metadata", "metadata_traceability", "task"),
    )
    for alias in aliases
)


@dataclass(frozen=True, slots=True)
class SourceEvidenceGap:
    """One task field that lacks traceable source evidence."""

    task_id: str
    title: str
    gap_type: SourceEvidenceGapType
    field_path: str
    message: str
    checked_evidence_fields: tuple[str, ...] = field(default_factory=tuple)
    observed_values: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "gap_type": self.gap_type,
            "field_path": self.field_path,
            "message": self.message,
            "checked_evidence_fields": list(self.checked_evidence_fields),
            "observed_values": list(self.observed_values),
        }


@dataclass(frozen=True, slots=True)
class SourceEvidenceGapAnalysis:
    """Task-level source evidence gap analysis."""

    plan_id: str | None = None
    gaps: tuple[SourceEvidenceGap, ...] = field(default_factory=tuple)
    task_count: int = 0

    @property
    def gap_count(self) -> int:
        """Return the number of detected evidence gaps."""
        return len(self.gaps)

    @property
    def task_ids_with_gaps(self) -> tuple[str, ...]:
        """Return task IDs with at least one gap in deterministic order."""
        return tuple(_dedupe(gap.task_id for gap in self.gaps))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "gap_count": self.gap_count,
            "task_ids_with_gaps": list(self.task_ids_with_gaps),
            "gaps": [gap.to_dict() for gap in self.gaps],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return gap records as plain dictionaries."""
        return [gap.to_dict() for gap in self.gaps]

    def to_markdown(self) -> str:
        """Render evidence gaps as deterministic Markdown."""
        title = "# Source Evidence Gaps"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.gaps:
            lines.extend(["", "No source evidence gaps detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Gap Type | Field | Observed Values | Message |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for gap in self.gaps:
            lines.append(
                "| "
                f"{_markdown_cell(gap.task_id)} | "
                f"{gap.gap_type} | "
                f"{_markdown_cell(gap.field_path)} | "
                f"{_markdown_cell('; '.join(gap.observed_values) or 'None')} | "
                f"{_markdown_cell(gap.message)} |"
            )
        return "\n".join(lines)


def analyze_source_evidence_gaps(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> SourceEvidenceGapAnalysis:
    """Report execution tasks whose key fields lack source traceability."""
    plan_id, tasks = _source_payload(source)
    gaps: list[SourceEvidenceGap] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        title = _optional_text(task.get("title")) or task_id
        for gap_type in _GAP_ORDER:
            field_path = _field_path(gap_type)
            checked_fields = _checked_evidence_fields(gap_type)
            if _has_source_evidence(task, gap_type):
                continue
            gaps.append(
                SourceEvidenceGap(
                    task_id=task_id,
                    title=title,
                    gap_type=gap_type,
                    field_path=field_path,
                    message=_gap_message(gap_type),
                    checked_evidence_fields=checked_fields,
                    observed_values=tuple(_field_values(task, gap_type)[:5]),
                )
            )

    gaps.sort(key=lambda gap: (gap.task_id, _GAP_ORDER[gap.gap_type]))
    return SourceEvidenceGapAnalysis(
        plan_id=plan_id,
        gaps=tuple(gaps),
        task_count=len(tasks),
    )


def source_evidence_gap_analysis_to_dict(
    analysis: SourceEvidenceGapAnalysis,
) -> dict[str, Any]:
    """Serialize source evidence gap analysis to a plain dictionary."""
    return analysis.to_dict()


source_evidence_gap_analysis_to_dict.__test__ = False


def source_evidence_gaps_to_dicts(
    analysis: SourceEvidenceGapAnalysis | Iterable[SourceEvidenceGap],
) -> list[dict[str, Any]]:
    """Serialize source evidence gap records to plain dictionaries."""
    if isinstance(analysis, SourceEvidenceGapAnalysis):
        return analysis.to_dicts()
    return [gap.to_dict() for gap in analysis]


source_evidence_gaps_to_dicts.__test__ = False


def source_evidence_gap_analysis_to_markdown(
    analysis: SourceEvidenceGapAnalysis,
) -> str:
    """Render source evidence gap analysis as Markdown."""
    return analysis.to_markdown()


source_evidence_gap_analysis_to_markdown.__test__ = False


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [_task_payload(source)]

    tasks: list[dict[str, Any]] = []
    try:
        iterator = iter(source)
    except TypeError:
        return None, []
    for item in iterator:
        tasks.append(_task_payload(item))
    return None, [task for task in tasks if task]


def _plan_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        task = _task_payload(item)
        if task:
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        dumped = value.model_dump(mode="python")
        return dict(dumped) if isinstance(dumped, Mapping) else {}
    if isinstance(value, Mapping):
        try:
            dumped = ExecutionTask.model_validate(value).model_dump(mode="python")
            return dict(dumped) if isinstance(dumped, Mapping) else {}
        except (TypeError, ValueError, ValidationError):
            return dict(value)
    return {}


def _has_source_evidence(task: Mapping[str, Any], gap_type: SourceEvidenceGapType) -> bool:
    field_names = _field_aliases(gap_type)
    if _has_direct_evidence(task, field_names):
        return True

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        if _has_direct_evidence(metadata, field_names):
            return True
        for key in ("source_evidence", "traceability"):
            if _has_nested_evidence(metadata.get(key), field_names):
                return True

    for field_name in field_names:
        if _has_field_value_evidence(task.get(field_name), field_names):
            return True
    return False


def _has_direct_evidence(value: Mapping[str, Any], field_names: tuple[str, ...]) -> bool:
    for key in _SOURCE_KEYS:
        if _has_source_key_evidence(key, value.get(key), field_names):
            return True
    for field_name in field_names:
        for suffix in _SOURCE_KEYS:
            if _has_nested_evidence(value.get(f"{field_name}_{suffix}"), field_names):
                return True
    return False


def _has_source_key_evidence(
    key: str,
    value: Any,
    field_names: tuple[str, ...],
) -> bool:
    if key in {"source_evidence", "traceability"}:
        return _has_nested_evidence(value, field_names)
    return _has_non_empty_evidence(value, field_names)


def _has_non_empty_evidence(value: Any, field_names: tuple[str, ...]) -> bool:
    if value in (None, "", [], {}, ()):
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Mapping):
        field_keys = _field_like_keys(value)
        if field_keys:
            return any(
                _key_matches_field(key, field_names)
                and _has_non_empty_evidence(value[key], field_names)
                for key in field_keys
            )
        record_field = _record_field(value)
        if record_field is not None:
            return any(_field_name_matches(record_field, field_name) for field_name in field_names)
        return True
    if isinstance(value, (list, tuple, set)):
        return any(_has_non_empty_evidence(item, field_names) for item in value)
    return True


def _has_nested_evidence(value: Any, field_names: tuple[str, ...]) -> bool:
    if value in (None, "", [], {}, ()):
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Mapping):
        field_keys = _field_like_keys(value)
        if field_keys:
            return any(
                _key_matches_field(key, field_names)
                and _has_nested_evidence(value[key], field_names)
                for key in field_keys
            )

        record_field = _record_field(value)
        if record_field is not None and not any(
            _field_name_matches(record_field, field_name) for field_name in field_names
        ):
            return False

        if any(_has_source_key_evidence(key, value.get(key), field_names) for key in _SOURCE_KEYS):
            return True
        if _looks_like_source_record(value):
            return True
        return False
    if isinstance(value, (list, tuple, set)):
        return any(_has_nested_evidence(item, field_names) for item in value)
    return True


def _has_field_value_evidence(value: Any, field_names: tuple[str, ...]) -> bool:
    if isinstance(value, Mapping):
        return _has_nested_evidence(value, field_names)
    if isinstance(value, (list, tuple, set)):
        return any(_has_field_value_evidence(item, field_names) for item in value)
    return False


def _record_field(value: Mapping[str, Any]) -> str | None:
    for key in _EVIDENCE_RECORD_KEYS:
        if text := _optional_text(value.get(key)):
            return text
    return None


def _looks_like_source_record(value: Mapping[str, Any]) -> bool:
    return any(
        _optional_text(value.get(key))
        for key in (
            "source_ids",
            "source_id",
            "source_links",
            "source_link",
            "evidence",
            "requirement_ids",
            "requirement_id",
            "requirement_refs",
            "requirement_ref",
        )
    )


def _field_aliases(gap_type: SourceEvidenceGapType) -> tuple[str, ...]:
    if gap_type == "acceptance_criteria":
        return ("acceptance_criteria", "acceptance", "criteria")
    if gap_type == "files_or_modules":
        return ("files_or_modules", "files", "modules", "file")
    if gap_type == "risks":
        return ("risks", "risk", "risk_level", "risk_notes")
    return ("metadata", "metadata_traceability", "task")


def _field_path(gap_type: SourceEvidenceGapType) -> str:
    if gap_type == "risks":
        return "risk_level/metadata.risks"
    if gap_type == "metadata_traceability":
        return "metadata.source_evidence/metadata.traceability"
    return gap_type


def _checked_evidence_fields(gap_type: SourceEvidenceGapType) -> tuple[str, ...]:
    aliases = _field_aliases(gap_type)
    field_keys = tuple(f"{alias}_{key}" for alias in aliases for key in _SOURCE_KEYS)
    return (*_SOURCE_KEYS, *field_keys, "metadata.source_evidence", "metadata.traceability")


def _field_values(task: Mapping[str, Any], gap_type: SourceEvidenceGapType) -> list[str]:
    if gap_type == "acceptance_criteria":
        return _strings(task.get("acceptance_criteria"))
    if gap_type == "files_or_modules":
        return _strings(task.get("files_or_modules"))
    if gap_type == "risks":
        values = []
        for key in ("risk_level", "risk", "risks", "risk_notes"):
            values.extend(_strings(task.get(key)))
        metadata = task.get("metadata")
        if isinstance(metadata, Mapping):
            values.extend(_strings(metadata.get("risks")))
            values.extend(_strings(metadata.get("risk_notes")))
        return _dedupe(values)
    metadata = task.get("metadata")
    if not isinstance(metadata, Mapping):
        return []
    return [str(key) for key in sorted(metadata, key=str) if str(key) not in _SOURCE_KEYS]


def _gap_message(gap_type: SourceEvidenceGapType) -> str:
    if gap_type == "acceptance_criteria":
        return "Acceptance criteria are not backed by traceable source evidence."
    if gap_type == "files_or_modules":
        return "Files or modules are not backed by traceable source evidence."
    if gap_type == "risks":
        return "Risk details are not backed by traceable source evidence."
    return "Task metadata is not backed by source evidence or traceability metadata."


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _key_matches_field(key: Any, field_names: tuple[str, ...]) -> bool:
    key_text = str(key).strip().lower()
    return any(_field_name_matches(key_text, field_name) for field_name in field_names)


def _field_like_keys(value: Mapping[str, Any]) -> list[Any]:
    return [
        key
        for key in value
        if any(_field_name_matches(str(key), field_name) for field_name in _ALL_FIELD_ALIASES)
    ]


def _field_name_matches(value: str, field_name: str) -> bool:
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    expected = field_name.strip().lower().replace("-", "_").replace(" ", "_")
    return normalized == expected or normalized.endswith(f".{expected}")


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        values: list[str] = []
        for key in sorted(value, key=str):
            values.extend(_strings(value[key]))
        return values
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        values: list[str] = []
        for item in items:
            values.extend(_strings(item))
        return values
    text = _optional_text(value)
    return [text] if text else []


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _dedupe(values: Iterable[_T]) -> list[_T]:
    seen: set[_T] = set()
    result: list[_T] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "SourceEvidenceGap",
    "SourceEvidenceGapAnalysis",
    "SourceEvidenceGapType",
    "analyze_source_evidence_gaps",
    "source_evidence_gap_analysis_to_dict",
    "source_evidence_gap_analysis_to_markdown",
    "source_evidence_gaps_to_dicts",
]
