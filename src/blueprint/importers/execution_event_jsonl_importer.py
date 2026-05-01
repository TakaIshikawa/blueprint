"""JSON Lines importer for downstream execution events."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Literal, Mapping


ExecutionEventType = Literal[
    "task_started",
    "task_completed",
    "task_failed",
    "verification_failed",
    "branch_created",
    "artifact_exported",
]

EVENT_TYPES: tuple[ExecutionEventType, ...] = (
    "task_started",
    "task_completed",
    "task_failed",
    "verification_failed",
    "branch_created",
    "artifact_exported",
)

TASK_SCOPED_EVENT_TYPES: frozenset[ExecutionEventType] = frozenset(
    {
        "task_started",
        "task_completed",
        "task_failed",
        "verification_failed",
    }
)

EVENT_REQUIRED_FIELDS: Mapping[ExecutionEventType, tuple[str, ...]] = {
    "task_started": ("plan_id", "task_id", "timestamp"),
    "task_completed": ("plan_id", "task_id", "timestamp"),
    "task_failed": ("plan_id", "task_id", "timestamp"),
    "verification_failed": ("plan_id", "task_id", "timestamp"),
    "branch_created": ("plan_id", "timestamp", "branch_name"),
    "artifact_exported": ("plan_id", "timestamp", "artifact_path"),
}


@dataclass(frozen=True, slots=True)
class ExecutionEventJsonlImportError:
    """One failed execution-event JSONL line with an actionable message."""

    line_number: int
    message: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "line_number": self.line_number,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class ExecutionEventRecord:
    """One validated downstream execution event."""

    line_number: int
    event_type: ExecutionEventType
    plan_id: str
    task_id: str | None
    timestamp: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "line_number": self.line_number,
            "event_type": self.event_type,
            "plan_id": self.plan_id,
            "task_id": self.task_id,
            "timestamp": self.timestamp,
            "payload": self.payload,
        }


@dataclass(frozen=True, slots=True)
class TaskExecutionEventGroup:
    """Execution events grouped for one task within a plan."""

    task_id: str
    events: tuple[ExecutionEventRecord, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "events": [event.to_dict() for event in self.events],
        }


@dataclass(frozen=True, slots=True)
class PlanExecutionEventGroup:
    """Execution events grouped for one plan."""

    plan_id: str
    events: tuple[ExecutionEventRecord, ...] = field(default_factory=tuple)
    task_groups: tuple[TaskExecutionEventGroup, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "events": [event.to_dict() for event in self.events],
            "task_groups": [group.to_dict() for group in self.task_groups],
        }


@dataclass(frozen=True, slots=True)
class ExecutionEventJsonlImportResult:
    """Complete result of an execution-event JSONL import."""

    records: tuple[ExecutionEventRecord, ...] = field(default_factory=tuple)
    errors: tuple[ExecutionEventJsonlImportError, ...] = field(default_factory=tuple)
    plan_groups: tuple[PlanExecutionEventGroup, ...] = field(default_factory=tuple)
    total_lines: int = 0

    @property
    def valid_count(self) -> int:
        """Return the number of valid event records."""
        return len(self.records)

    @property
    def error_count(self) -> int:
        """Return the number of failed lines."""
        return len(self.errors)

    @property
    def plan_ids(self) -> tuple[str, ...]:
        """Return imported plan IDs in stable first-seen order."""
        return tuple(group.plan_id for group in self.plan_groups)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "total_lines": self.total_lines,
            "valid_count": self.valid_count,
            "error_count": self.error_count,
            "records": [record.to_dict() for record in self.records],
            "errors": [error.to_dict() for error in self.errors],
            "plan_groups": [group.to_dict() for group in self.plan_groups],
        }


class ExecutionEventJsonlImporter:
    """Parse newline-delimited downstream execution events."""

    def import_file(
        self,
        file_path: str,
        *,
        continue_on_error: bool = False,
    ) -> ExecutionEventJsonlImportResult:
        """Validate execution events from a JSON Lines file."""
        path = Path(file_path).expanduser()
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError as exc:
            raise ImportError(f"Execution event JSONL file not found: {path}") from exc
        except OSError as exc:
            raise ImportError(f"Could not read execution event JSONL file: {path}") from exc

        return self.import_lines(lines, continue_on_error=continue_on_error)

    def import_lines(
        self,
        lines: list[str] | tuple[str, ...],
        *,
        continue_on_error: bool = False,
    ) -> ExecutionEventJsonlImportResult:
        """Validate execution events from already-loaded JSONL lines."""
        records: list[ExecutionEventRecord] = []
        errors: list[ExecutionEventJsonlImportError] = []

        for line_number, line in enumerate(lines, start=1):
            record, error = _parse_line(line, line_number=line_number)
            if error is not None:
                errors.append(error)
                if not continue_on_error:
                    return ExecutionEventJsonlImportResult(
                        records=(),
                        errors=tuple(errors),
                        plan_groups=(),
                        total_lines=len(lines),
                    )
                continue
            records.append(record)

        return ExecutionEventJsonlImportResult(
            records=tuple(records),
            errors=tuple(errors),
            plan_groups=_group_records(records),
            total_lines=len(lines),
        )


def execution_event_jsonl_import_to_dict(
    result: ExecutionEventJsonlImportResult,
) -> dict[str, Any]:
    """Serialize an execution-event import result to a plain dictionary."""
    return result.to_dict()


execution_event_jsonl_import_to_dict.__test__ = False


def _parse_line(
    line: str,
    *,
    line_number: int,
) -> tuple[ExecutionEventRecord, None] | tuple[None, ExecutionEventJsonlImportError]:
    if not line.strip():
        return None, ExecutionEventJsonlImportError(
            line_number=line_number,
            message="empty line is not an execution event JSON object",
        )

    try:
        payload = json.loads(line)
    except JSONDecodeError as exc:
        return None, ExecutionEventJsonlImportError(
            line_number=line_number,
            message=f"invalid JSON: {exc.msg}",
        )

    if not isinstance(payload, dict):
        return None, ExecutionEventJsonlImportError(
            line_number=line_number,
            message="expected a JSON object",
        )

    return _record_from_payload(payload, line_number=line_number)


def _record_from_payload(
    payload: dict[str, Any],
    *,
    line_number: int,
) -> tuple[ExecutionEventRecord, None] | tuple[None, ExecutionEventJsonlImportError]:
    event_type = _text(payload.get("event_type"))
    if not event_type:
        return None, _validation_error(line_number, "event_type is required")
    if event_type not in EVENT_TYPES:
        return None, _validation_error(
            line_number,
            f"unknown event_type: {event_type}; expected one of: {', '.join(EVENT_TYPES)}",
        )

    typed_event_type = event_type
    missing = [
        field_name
        for field_name in EVENT_REQUIRED_FIELDS[typed_event_type]
        if not _text(payload.get(field_name))
    ]
    if missing:
        return None, _validation_error(
            line_number,
            "missing required field(s): " + ", ".join(missing),
        )

    timestamp, timestamp_error = _normalize_timestamp(payload.get("timestamp"))
    if timestamp_error is not None:
        return None, _validation_error(line_number, timestamp_error)

    task_id = _text(payload.get("task_id"))
    if typed_event_type in TASK_SCOPED_EVENT_TYPES and task_id is None:
        return None, _validation_error(line_number, "task_id is required")

    return (
        ExecutionEventRecord(
            line_number=line_number,
            event_type=typed_event_type,
            plan_id=_text(payload.get("plan_id")) or "",
            task_id=task_id,
            timestamp=timestamp,
            payload={**payload, "event_type": typed_event_type, "timestamp": timestamp},
        ),
        None,
    )


def _validation_error(line_number: int, message: str) -> ExecutionEventJsonlImportError:
    return ExecutionEventJsonlImportError(
        line_number=line_number,
        message=f"validation failed: {message}",
    )


def _normalize_timestamp(value: Any) -> tuple[str, None] | tuple[None, str]:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        candidate = value.strip()
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError:
            return None, f"timestamp must be an ISO 8601 datetime: {value!r}"
    else:
        return None, "timestamp must be an ISO 8601 datetime"

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    normalized = parsed.astimezone(UTC).replace(tzinfo=None).isoformat(timespec="seconds")
    return f"{normalized}Z", None


def _group_records(records: list[ExecutionEventRecord]) -> tuple[PlanExecutionEventGroup, ...]:
    plan_order: list[str] = []
    events_by_plan: dict[str, list[ExecutionEventRecord]] = {}
    for record in records:
        if record.plan_id not in events_by_plan:
            plan_order.append(record.plan_id)
            events_by_plan[record.plan_id] = []
        events_by_plan[record.plan_id].append(record)

    groups: list[PlanExecutionEventGroup] = []
    for plan_id in plan_order:
        events = events_by_plan[plan_id]
        groups.append(
            PlanExecutionEventGroup(
                plan_id=plan_id,
                events=tuple(events),
                task_groups=_task_groups(events),
            )
        )
    return tuple(groups)


def _task_groups(events: list[ExecutionEventRecord]) -> tuple[TaskExecutionEventGroup, ...]:
    task_order: list[str] = []
    events_by_task: dict[str, list[ExecutionEventRecord]] = {}
    for event in events:
        if event.task_id is None:
            continue
        if event.task_id not in events_by_task:
            task_order.append(event.task_id)
            events_by_task[event.task_id] = []
        events_by_task[event.task_id].append(event)

    return tuple(
        TaskExecutionEventGroup(task_id=task_id, events=tuple(events_by_task[task_id]))
        for task_id in task_order
    )


def _text(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None
