"""Build dependency waiver registers for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DependencyWaiverStatus = Literal["active", "expired", "unresolved"]
DependencyWaiverType = Literal[
    "dependency_waiver",
    "dependency_exception",
    "dependency_override",
    "manual_sequencing",
    "missing_dependency_acceptance",
    "blocked_reason",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_ISO_DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2}|19\d{2}-\d{2}-\d{2})\b")
_DEPENDENCY_RE = re.compile(
    r"\b(?:dependenc(?:y|ies)|depends? on|prerequisite|blocked by|"
    r"sequenc(?:e|ing)|order(?:ed)?|upstream|downstream)\b",
    re.IGNORECASE,
)
_WAIVER_RE = re.compile(
    r"\b(?:waiv(?:e|er|ed)|exception|override|bypass|ignore|accepted risk|"
    r"manual(?:ly)? sequenc(?:e|ed|ing)|missing dependenc(?:y|ies)|"
    r"accept(?:ed|ance) missing)\b",
    re.IGNORECASE,
)
_EXPIRY_RE = re.compile(
    r"\b(?:expires?|expiry|until|after|review(?: by)?|revisit(?: by)?)"
    r"[:\s-]+([^.;\n]+)",
    re.IGNORECASE,
)
_ACTIVE_STATUSES = {
    "accepted",
    "active",
    "approved",
    "granted",
    "open",
    "waived",
}
_EXPIRED_STATUSES = {"expired", "lapsed", "stale"}
_UNRESOLVED_STATUSES = {
    "blocked",
    "draft",
    "missing",
    "pending",
    "proposed",
    "todo",
    "tbd",
    "unknown",
    "unresolved",
}
_EXPLICIT_WAIVER_KEYS = (
    "dependency_waivers",
    "dependency_waiver",
    "waivers",
    "waiver",
    "dependency_exceptions",
    "dependency_exception",
    "exceptions",
    "exception",
    "dependency_overrides",
    "dependency_override",
    "overrides",
    "override",
    "manual_sequencing",
    "manual_sequence",
    "missing_dependency_acceptance",
    "missing_dependency_accepted",
)


@dataclass(frozen=True, slots=True)
class DependencyWaiverRecord:
    """One accepted or inferred dependency exception for a task."""

    task_id: str
    waiver_type: DependencyWaiverType
    reason: str
    expiry_signal: str
    status: DependencyWaiverStatus
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "waiver_type": self.waiver_type,
            "reason": self.reason,
            "expiry_signal": self.expiry_signal,
            "status": self.status,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanDependencyWaiverRegister:
    """Plan-level dependency waiver register and rollup counts."""

    plan_id: str | None = None
    waivers: tuple[DependencyWaiverRecord, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "waivers": [waiver.to_dict() for waiver in self.waivers],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return waiver records as plain dictionaries."""
        return [waiver.to_dict() for waiver in self.waivers]

    def to_markdown(self) -> str:
        """Render the waiver register as deterministic Markdown."""
        title = "# Plan Dependency Waivers"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.waivers:
            lines.extend(["", "No dependency waivers were found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Type | Status | Expiry Signal | Reason | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for waiver in self.waivers:
            lines.append(
                "| "
                f"{_markdown_cell(waiver.task_id)} | "
                f"{waiver.waiver_type} | "
                f"{waiver.status} | "
                f"{_markdown_cell(waiver.expiry_signal)} | "
                f"{_markdown_cell(waiver.reason)} | "
                f"{_markdown_cell('; '.join(waiver.evidence))} |"
            )
        return "\n".join(lines)


def build_plan_dependency_waiver_register(
    source: Mapping[str, Any]
    | ExecutionPlan
    | ExecutionTask
    | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanDependencyWaiverRegister:
    """Extract explicit and inferred dependency waiver records from plan tasks."""
    plan_id, tasks = _source_payload(source)
    records: list[DependencyWaiverRecord] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        records.extend(_task_waivers(task, task_id))

    waivers = tuple(
        sorted(
            records,
            key=lambda waiver: (
                waiver.task_id,
                _WAIVER_TYPE_ORDER[waiver.waiver_type],
                waiver.reason,
                waiver.expiry_signal,
            ),
        )
    )
    summary = {
        "total": len(waivers),
        "active": sum(1 for waiver in waivers if waiver.status == "active"),
        "expired": sum(1 for waiver in waivers if waiver.status == "expired"),
        "unresolved": sum(1 for waiver in waivers if waiver.status == "unresolved"),
        "waiver_type_counts": {
            waiver_type: sum(1 for waiver in waivers if waiver.waiver_type == waiver_type)
            for waiver_type in _WAIVER_TYPE_ORDER
        },
    }
    return PlanDependencyWaiverRegister(plan_id=plan_id, waivers=waivers, summary=summary)


def plan_dependency_waiver_register_to_dict(
    result: PlanDependencyWaiverRegister,
) -> dict[str, Any]:
    """Serialize a dependency waiver register to a plain dictionary."""
    return result.to_dict()


plan_dependency_waiver_register_to_dict.__test__ = False


def plan_dependency_waiver_register_to_markdown(
    result: PlanDependencyWaiverRegister,
) -> str:
    """Render a dependency waiver register as Markdown."""
    return result.to_markdown()


plan_dependency_waiver_register_to_markdown.__test__ = False


def summarize_plan_dependency_waivers(
    source: Mapping[str, Any]
    | ExecutionPlan
    | ExecutionTask
    | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanDependencyWaiverRegister:
    """Compatibility alias for building dependency waiver registers."""
    return build_plan_dependency_waiver_register(source)


_WAIVER_TYPE_ORDER: dict[DependencyWaiverType, int] = {
    "dependency_waiver": 0,
    "dependency_exception": 1,
    "dependency_override": 2,
    "manual_sequencing": 3,
    "missing_dependency_acceptance": 4,
    "blocked_reason": 5,
}


def _task_waivers(task: Mapping[str, Any], task_id: str) -> list[DependencyWaiverRecord]:
    records: list[DependencyWaiverRecord] = []
    metadata = task.get("metadata")

    for key, value in _explicit_waiver_values(task, metadata):
        for item in _waiver_items(value):
            record = _record_from_explicit_item(task_id, key, item)
            if record is not None:
                records.append(record)

    for source_field, text in _candidate_texts(task):
        if not _is_inferred_waiver_text(text, source_field):
            continue
        records.append(
            _build_record(
                task_id=task_id,
                waiver_type=(
                    "blocked_reason"
                    if source_field.endswith("blocked_reason")
                    else _classify_waiver_type(source_field, text)
                ),
                reason=text,
                expiry_signal=_expiry_signal_from_value(None, text),
                explicit_status=None,
                evidence=(f"{source_field}: {text}",),
            )
        )

    return _dedupe_records(records)


def _explicit_waiver_values(
    task: Mapping[str, Any],
    metadata: Any,
) -> list[tuple[str, Any]]:
    values: list[tuple[str, Any]] = []
    for key in _EXPLICIT_WAIVER_KEYS:
        if key in task:
            values.append((key, task[key]))
        if isinstance(metadata, Mapping) and key in metadata:
            values.append((f"metadata.{key}", metadata[key]))
    return values


def _waiver_items(value: Any) -> list[Any]:
    if value in (None, False):
        return []
    if value is True:
        return [{"reason": "dependency waiver accepted"}]
    if isinstance(value, Mapping):
        if any(
            key in value
            for key in (
                "reason",
                "description",
                "rationale",
                "status",
                "expires",
                "expires_at",
                "expiry",
                "expiry_signal",
                "waiver_type",
                "type",
            )
        ):
            return [value]
        items: list[Any] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            if isinstance(child, Mapping):
                merged = {"id": str(key), **dict(child)}
                items.append(merged)
            else:
                items.append({"id": str(key), "reason": child})
        return items
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        expanded: list[Any] = []
        for item in items:
            expanded.extend(_waiver_items(item))
        return expanded
    text = _optional_text(value)
    return [{"reason": text}] if text else []


def _record_from_explicit_item(
    task_id: str,
    source_field: str,
    item: Any,
) -> DependencyWaiverRecord | None:
    if isinstance(item, Mapping):
        reason = _first_text(
            item,
            ("reason", "description", "rationale", "note", "notes", "id"),
        )
        if not reason:
            return None
        waiver_type = _classify_waiver_type(
            _optional_text(item.get("waiver_type"))
            or _optional_text(item.get("type"))
            or source_field,
            reason,
        )
        expiry_signal = _expiry_signal_from_value(
            _first_raw(
                item,
                (
                    "expiry_signal",
                    "expires",
                    "expires_at",
                    "expiry",
                    "until",
                    "review_by",
                    "revisit_by",
                ),
            ),
            reason,
        )
        status = _optional_text(item.get("status"))
        evidence = tuple(
            _dedupe(
                [
                    f"{source_field}: {reason}",
                    *[
                        f"{source_field}.{key}: {_text(item[key])}"
                        for key in (
                            "status",
                            "expiry_signal",
                            "expires",
                            "expires_at",
                            "until",
                            "review_by",
                        )
                        if key in item and _optional_text(item[key])
                    ],
                ]
            )
        )
        return _build_record(
            task_id=task_id,
            waiver_type=waiver_type,
            reason=reason,
            expiry_signal=expiry_signal,
            explicit_status=status,
            evidence=evidence,
        )

    reason = _optional_text(item)
    if not reason:
        return None
    return _build_record(
        task_id=task_id,
        waiver_type=_classify_waiver_type(source_field, reason),
        reason=reason,
        expiry_signal=_expiry_signal_from_value(None, reason),
        explicit_status=None,
        evidence=(f"{source_field}: {reason}",),
    )


def _build_record(
    *,
    task_id: str,
    waiver_type: DependencyWaiverType,
    reason: str,
    expiry_signal: str,
    explicit_status: str | None,
    evidence: tuple[str, ...],
) -> DependencyWaiverRecord:
    return DependencyWaiverRecord(
        task_id=task_id,
        waiver_type=waiver_type,
        reason=reason,
        expiry_signal=expiry_signal,
        status=_status(explicit_status, expiry_signal),
        evidence=evidence,
    )


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    if text := _optional_text(task.get("description")):
        texts.append(("description", text))
    if text := _optional_text(task.get("blocked_reason")):
        texts.append(("blocked_reason", text))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        if text := _optional_text(metadata.get("blocked_reason")):
            texts.append(("metadata.blocked_reason", text))
        for source_field, text in _metadata_texts(metadata):
            if _is_explicit_metadata_field(source_field):
                continue
            texts.append((source_field, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    return texts


def _is_explicit_metadata_field(source_field: str) -> bool:
    return any(
        source_field == f"metadata.{key}"
        or source_field.startswith(f"metadata.{key}.")
        or source_field.startswith(f"metadata.{key}[")
        for key in _EXPLICIT_WAIVER_KEYS
    )


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if not isinstance(value, Mapping):
        return []
    texts: list[tuple[str, str]] = []
    for key in sorted(value, key=lambda item: str(item)):
        child = value[key]
        field = f"{prefix}.{key}"
        if isinstance(child, Mapping):
            texts.extend(_metadata_texts(child, field))
        elif isinstance(child, (list, tuple, set)):
            items = sorted(child, key=lambda item: str(item)) if isinstance(child, set) else child
            for index, item in enumerate(items):
                item_field = f"{field}[{index}]"
                if isinstance(item, Mapping):
                    texts.extend(_metadata_texts(item, item_field))
                elif text := _optional_text(item):
                    texts.append((item_field, text))
        elif text := _optional_text(child):
            texts.append((field, text))
    return texts


def _is_inferred_waiver_text(text: str, source_field: str) -> bool:
    if not _WAIVER_RE.search(text):
        return False
    if _DEPENDENCY_RE.search(text):
        return True
    return any(
        token in source_field
        for token in ("dependency", "sequencing", "blocked_reason", "waiver", "override")
    )


def _classify_waiver_type(source: str, text: str) -> DependencyWaiverType:
    folded = f"{source} {text}".casefold()
    if "manual" in folded and "sequenc" in folded:
        return "manual_sequencing"
    if "missing dependenc" in folded or "acceptance" in folded and "missing" in folded:
        return "missing_dependency_acceptance"
    if "override" in folded or "bypass" in folded or "ignore" in folded:
        return "dependency_override"
    if "exception" in folded:
        return "dependency_exception"
    if "blocked_reason" in source:
        return "blocked_reason"
    return "dependency_waiver"


def _expiry_signal_from_value(value: Any, text: str) -> str:
    explicit = _optional_text(value)
    if explicit:
        return explicit
    match = _EXPIRY_RE.search(text)
    if match:
        return _text(match.group(1)).rstrip(" .")
    return "unspecified"


def _status(explicit_status: str | None, expiry_signal: str) -> DependencyWaiverStatus:
    folded_status = (explicit_status or "").casefold()
    if folded_status in _EXPIRED_STATUSES or _expiry_date(expiry_signal, date.today()):
        return "expired"
    if folded_status in _UNRESOLVED_STATUSES or expiry_signal == "unspecified":
        return "unresolved"
    if folded_status in _ACTIVE_STATUSES:
        return "active"
    return "active"


def _expiry_date(expiry_signal: str, today: date) -> bool:
    match = _ISO_DATE_RE.search(expiry_signal)
    if not match:
        return False
    try:
        return date.fromisoformat(match.group(1)) < today
    except ValueError:
        return False


def _source_payload(
    source: Mapping[str, Any]
    | ExecutionPlan
    | ExecutionTask
    | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, tasks


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


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _first_text(value: Mapping[str, Any], keys: Iterable[str]) -> str | None:
    raw = _first_raw(value, keys)
    return _optional_text(raw)


def _first_raw(value: Mapping[str, Any], keys: Iterable[str]) -> Any:
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
        for key in sorted(value, key=lambda item: str(item)):
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


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _dedupe_records(
    records: Iterable[DependencyWaiverRecord],
) -> list[DependencyWaiverRecord]:
    deduped: list[DependencyWaiverRecord] = []
    seen: set[tuple[str, str, str, str]] = set()
    for record in records:
        key = (
            record.task_id,
            record.waiver_type,
            record.reason.casefold(),
            record.expiry_signal.casefold(),
        )
        if key in seen:
            continue
        deduped.append(record)
        seen.add(key)
    return deduped


__all__ = [
    "DependencyWaiverRecord",
    "DependencyWaiverStatus",
    "DependencyWaiverType",
    "PlanDependencyWaiverRegister",
    "build_plan_dependency_waiver_register",
    "plan_dependency_waiver_register_to_dict",
    "plan_dependency_waiver_register_to_markdown",
    "summarize_plan_dependency_waivers",
]
