"""Small shared utilities for deterministic task-readiness helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


_SPACE_RE = re.compile(r"\s+")
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")


@dataclass(frozen=True, slots=True)
class SimpleReadinessRecord:
    """One deterministic task-readiness finding."""

    task_id: str
    title: str
    detected_signals: tuple[str, ...]
    present_criteria: tuple[str, ...]
    missing_criteria: tuple[str, ...]
    readiness: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_follow_up_actions: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[str, ...]:
        return self.detected_signals

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_criteria": list(self.present_criteria),
            "missing_criteria": list(self.missing_criteria),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "recommended_follow_up_actions": list(self.recommended_follow_up_actions),
        }


@dataclass(frozen=True, slots=True)
class SimpleReadinessPlan:
    """Plan-level deterministic task-readiness report."""

    plan_id: str | None = None
    records: tuple[SimpleReadinessRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)
    title: str = "Task Readiness"

    @property
    def findings(self) -> tuple[SimpleReadinessRecord, ...]:
        return self.records

    @property
    def recommendations(self) -> tuple[SimpleReadinessRecord, ...]:
        return self.records

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "impacted_task_ids": list(self.impacted_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        title = f"# {self.title}"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Impacted task count: {self.summary.get('impacted_task_count', 0)}",
            f"- Missing criterion count: {self.summary.get('missing_criterion_count', 0)}",
        ]
        if not self.records:
            lines.extend(["", f"No {self.title.lower()} records were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Task | Title | Readiness | Signals | Present | Missing | Evidence |", "| --- | --- | --- | --- | --- | --- | --- |"])
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.detected_signals))} | "
                f"{_markdown_cell(', '.join(record.present_criteria) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_simple_readiness_plan(
    source: Any,
    *,
    title: str,
    signal_patterns: Mapping[str, re.Pattern[str]],
    path_signal_patterns: Mapping[str, re.Pattern[str]],
    criteria_patterns: Mapping[str, re.Pattern[str]],
    criterion_guidance: Mapping[str, str],
    no_impact_pattern: re.Pattern[str] | None = None,
) -> SimpleReadinessPlan:
    """Build a readiness report from task-shaped input."""
    plan_id, tasks = _source_payload(source)
    records: list[SimpleReadinessRecord] = []
    for index, task in enumerate(tasks, start=1):
        record = _record(
            task,
            index,
            signal_patterns=signal_patterns,
            path_signal_patterns=path_signal_patterns,
            criteria_patterns=criteria_patterns,
            criterion_guidance=criterion_guidance,
            no_impact_pattern=no_impact_pattern,
        )
        if record is not None:
            records.append(record)
    records_tuple = tuple(
        sorted(
            records,
            key=lambda record: (
                {"needs_planning": 0, "partial": 1, "ready": 2}[record.readiness],
                -len(record.missing_criteria),
                record.task_id,
            ),
        )
    )
    impacted = tuple(record.task_id for record in records_tuple)
    impacted_set = set(impacted)
    ignored = tuple(_task_id(task, index) for index, task in enumerate(tasks, start=1) if _task_id(task, index) not in impacted_set)
    return SimpleReadinessPlan(
        plan_id=plan_id,
        records=records_tuple,
        impacted_task_ids=impacted,
        ignored_task_ids=ignored,
        summary=_summary(records_tuple, len(tasks), criteria_patterns),
        title=title,
    )


def _record(
    task: Mapping[str, Any],
    index: int,
    *,
    signal_patterns: Mapping[str, re.Pattern[str]],
    path_signal_patterns: Mapping[str, re.Pattern[str]],
    criteria_patterns: Mapping[str, re.Pattern[str]],
    criterion_guidance: Mapping[str, str],
    no_impact_pattern: re.Pattern[str] | None,
) -> SimpleReadinessRecord | None:
    signals: set[str] = set()
    present: set[str] = set()
    evidence: list[str] = []
    for field, text in _candidate_texts(task):
        if no_impact_pattern and no_impact_pattern.search(text):
            return None
        searchable = text.replace("_", " ").replace("-", " ")
        matched = False
        for name, pattern in signal_patterns.items():
            if pattern.search(searchable):
                signals.add(name)
                matched = True
        for name, pattern in criteria_patterns.items():
            if pattern.search(searchable):
                present.add(name)
                matched = True
        if matched:
            evidence.append(f"{field}: {text}")
    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("expected_files") or task.get("paths")):
        searchable = path.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for name, pattern in path_signal_patterns.items():
            if pattern.search(searchable):
                signals.add(name)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")
    if not signals:
        return None
    criteria = tuple(criteria_patterns)
    missing = tuple(name for name in criteria if name not in present)
    readiness = "ready" if not missing else ("partial" if present else "needs_planning")
    return SimpleReadinessRecord(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        detected_signals=tuple(name for name in signal_patterns if name in signals),
        present_criteria=tuple(name for name in criteria if name in present),
        missing_criteria=missing,
        readiness=readiness,
        evidence=tuple(_dedupe(evidence)),
        recommended_follow_up_actions=tuple(criterion_guidance[name] for name in missing),
    )


def _source_payload(source: Any) -> tuple[str | None, list[Mapping[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return _optional_text(payload.get("id") or payload.get("plan_id")), _tasks_from_payload(payload)
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            return _optional_text(value.get("id") or value.get("plan_id")), _tasks_from_payload(value)
    if isinstance(source, Mapping):
        try:
            payload = ExecutionPlan.model_validate(source).model_dump(mode="python")
            return _optional_text(payload.get("id") or payload.get("plan_id")), _tasks_from_payload(payload)
        except (TypeError, ValueError, ValidationError):
            return _optional_text(source.get("id") or source.get("plan_id")), _tasks_from_payload(source)
    if isinstance(source, str):
        return None, [{"id": "task-1", "title": source, "description": source}]
    if isinstance(source, Iterable):
        return None, [_task_payload(item, index) for index, item in enumerate(source, start=1)]
    return None, []


def _tasks_from_payload(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    tasks = payload.get("tasks") or payload.get("execution_tasks") or payload.get("items")
    if isinstance(tasks, Iterable) and not isinstance(tasks, (str, bytes, bytearray, Mapping)):
        return [_task_payload(item, index) for index, item in enumerate(tasks, start=1)]
    if any(key in payload for key in ("title", "description", "summary")):
        return [_task_payload(payload, 1)]
    return []


def _task_payload(item: Any, index: int) -> Mapping[str, Any]:
    if isinstance(item, ExecutionTask):
        return item.model_dump(mode="python")
    if hasattr(item, "model_dump"):
        value = item.model_dump(mode="python")
        if isinstance(value, Mapping):
            return value
    if isinstance(item, Mapping):
        return item
    if isinstance(item, str):
        return {"id": f"task-{index}", "title": item, "description": item}
    return {}


def _candidate_texts(task: Mapping[str, Any], prefix: str | None = None) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    for key, value in task.items():
        field = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, str):
            for part in _SPLIT_RE.split(value):
                text = _clean(part)
                if text:
                    fields.append((field, text))
        elif isinstance(value, Mapping):
            fields.extend(_candidate_texts(value, field))
        elif isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
            for index, item in enumerate(value):
                if isinstance(item, str):
                    text = _clean(item)
                    if text:
                        fields.append((f"{field}[{index}]", text))
                elif isinstance(item, Mapping):
                    fields.extend(_candidate_texts(item, f"{field}[{index}]"))
    return fields


def _summary(records: tuple[SimpleReadinessRecord, ...], task_count: int, criteria_patterns: Mapping[str, re.Pattern[str]]) -> dict[str, Any]:
    readiness_counts = {"needs_planning": 0, "partial": 0, "ready": 0}
    criterion_counts = {name: 0 for name in criteria_patterns}
    missing_counts = {name: 0 for name in criteria_patterns}
    for record in records:
        readiness_counts[record.readiness] += 1
        for name in record.present_criteria:
            criterion_counts[name] += 1
        for name in record.missing_criteria:
            missing_counts[name] += 1
    return {
        "task_count": task_count,
        "impacted_task_count": len(records),
        "readiness_counts": readiness_counts,
        "criterion_counts": criterion_counts,
        "missing_criterion_counts": missing_counts,
        "missing_criterion_count": sum(len(record.missing_criteria) for record in records),
    }


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id") or task.get("task_id") or task.get("key")) or f"task-{index}"


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, Mapping)):
        return [str(item) for item in value if item is not None]
    return []


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _clean(str(value))
    return text or None


def _clean(value: str) -> str:
    return _SPACE_RE.sub(" ", value.strip()).strip()


def _dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")

