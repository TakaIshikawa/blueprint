"""Recommend deterministic git branch isolation for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


BranchIsolationSeverity = Literal["info", "warning", "error"]
_T = TypeVar("_T")

_HIGH_RISK_VALUES = {"blocker", "critical", "high"}
_LARGE_SCOPE_VALUES = {"epic", "extra-large", "large", "xl", "xxl"}
_SLUG_RE = re.compile(r"[^a-z0-9._/-]+")


@dataclass(frozen=True, slots=True)
class PlanBranchIsolationFinding:
    """One branch-isolation concern for autonomous task execution."""

    code: str
    severity: BranchIsolationSeverity
    reason: str
    suggested_remediation: str
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    file_paths: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "code": self.code,
            "severity": self.severity,
            "reason": self.reason,
            "suggested_remediation": self.suggested_remediation,
            "task_ids": list(self.task_ids),
            "file_paths": list(self.file_paths),
        }


@dataclass(frozen=True, slots=True)
class PlanBranchGroup:
    """A recommended branch assignment for one or more plan tasks."""

    group_id: str
    branch_name: str
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    files_or_modules: tuple[str, ...] = field(default_factory=tuple)
    owner: str | None = None
    owner_type: str | None = None
    isolation_level: Literal["parallel", "serialized"] = "parallel"
    serialization_reasons: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "group_id": self.group_id,
            "branch_name": self.branch_name,
            "task_ids": list(self.task_ids),
            "files_or_modules": list(self.files_or_modules),
            "owner": self.owner,
            "owner_type": self.owner_type,
            "isolation_level": self.isolation_level,
            "serialization_reasons": list(self.serialization_reasons),
        }


@dataclass(frozen=True, slots=True)
class PlanBranchIsolationAdvice:
    """Complete branch-isolation recommendation for an execution plan."""

    plan_id: str
    branch_groups: tuple[PlanBranchGroup, ...] = field(default_factory=tuple)
    findings: tuple[PlanBranchIsolationFinding, ...] = field(default_factory=tuple)

    @property
    def parallel_branch_names(self) -> tuple[str, ...]:
        """Branch names that are safe to dispatch independently."""
        return tuple(
            group.branch_name for group in self.branch_groups if group.isolation_level == "parallel"
        )

    @property
    def serialized_branch_names(self) -> tuple[str, ...]:
        """Branch names that should be executed as serialized task groups."""
        return tuple(
            group.branch_name
            for group in self.branch_groups
            if group.isolation_level == "serialized"
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "branch_groups": [group.to_dict() for group in self.branch_groups],
            "findings": [finding.to_dict() for finding in self.findings],
            "parallel_branch_names": list(self.parallel_branch_names),
            "serialized_branch_names": list(self.serialized_branch_names),
        }


def advise_plan_branch_isolation(
    plan: Mapping[str, Any] | ExecutionPlan,
    *,
    branch_prefix: str = "bp",
) -> PlanBranchIsolationAdvice:
    """Recommend branch groups without invoking git or inspecting the worktree."""
    payload = _plan_payload(plan)
    records = _task_records(_task_payloads(payload.get("tasks")))
    plan_id = _text(payload.get("id")) or "plan"
    task_ids = {record.task_id for record in records}
    serial_edges = _serial_edges(records, task_ids)
    components = _serial_components(records, serial_edges)

    findings = [
        *_dependency_findings(records, task_ids),
        *_file_contention_findings(records),
        *_large_or_risky_scope_findings(records),
    ]
    groups = tuple(
        _branch_group(
            plan_id=plan_id,
            branch_prefix=branch_prefix,
            records=component,
            serial_edges=serial_edges,
            group_index=index,
        )
        for index, component in enumerate(components, start=1)
    )

    return PlanBranchIsolationAdvice(
        plan_id=plan_id,
        branch_groups=groups,
        findings=tuple(findings),
    )


def plan_branch_isolation_to_dict(advice: PlanBranchIsolationAdvice) -> dict[str, Any]:
    """Serialize branch-isolation advice to a plain dictionary."""
    return advice.to_dict()


plan_branch_isolation_to_dict.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    depends_on: tuple[str, ...]
    files_or_modules: tuple[str, ...]
    owner: str | None
    owner_type: str | None
    risk_level: str
    estimated_complexity: str


@dataclass(frozen=True, slots=True)
class _SerialEdge:
    left_task_id: str
    right_task_id: str
    reason: str


def _serial_edges(records: tuple[_TaskRecord, ...], task_ids: set[str]) -> tuple[_SerialEdge, ...]:
    edges: list[_SerialEdge] = []
    for record in records:
        for dependency_id in record.depends_on:
            if dependency_id in task_ids:
                edges.append(
                    _SerialEdge(
                        left_task_id=dependency_id,
                        right_task_id=record.task_id,
                        reason=f"{record.task_id} depends on {dependency_id}",
                    )
                )

    for left_index, left in enumerate(records):
        for right in records[left_index + 1 :]:
            overlaps = _overlapping_paths(left.files_or_modules, right.files_or_modules)
            if overlaps:
                edges.append(
                    _SerialEdge(
                        left_task_id=left.task_id,
                        right_task_id=right.task_id,
                        reason=("shared files_or_modules: " + ", ".join(overlaps)),
                    )
                )
    return tuple(edges)


def _serial_components(
    records: tuple[_TaskRecord, ...],
    serial_edges: tuple[_SerialEdge, ...],
) -> tuple[tuple[_TaskRecord, ...], ...]:
    adjacent: dict[str, set[str]] = {record.task_id: set() for record in records}
    for edge in serial_edges:
        adjacent.setdefault(edge.left_task_id, set()).add(edge.right_task_id)
        adjacent.setdefault(edge.right_task_id, set()).add(edge.left_task_id)

    record_by_id = {record.task_id: record for record in records}
    seen: set[str] = set()
    components: list[tuple[_TaskRecord, ...]] = []
    for record in records:
        if record.task_id in seen:
            continue
        stack = [record.task_id]
        component_ids: list[str] = []
        while stack:
            task_id = stack.pop()
            if task_id in seen:
                continue
            seen.add(task_id)
            component_ids.append(task_id)
            stack.extend(sorted(adjacent.get(task_id, set()), reverse=True))
        component_ids.sort(key=lambda task_id: _record_index(records, task_id))
        components.append(tuple(record_by_id[task_id] for task_id in component_ids))
    return tuple(components)


def _branch_group(
    *,
    plan_id: str,
    branch_prefix: str,
    records: tuple[_TaskRecord, ...],
    serial_edges: tuple[_SerialEdge, ...],
    group_index: int,
) -> PlanBranchGroup:
    task_ids = tuple(record.task_id for record in records)
    branch_stem = task_ids[0] if len(task_ids) == 1 else "-".join(task_ids[:3])
    reasons = tuple(
        _dedupe(
            edge.reason
            for edge in serial_edges
            if edge.left_task_id in task_ids and edge.right_task_id in task_ids
        )
    )
    owners = _dedupe(record.owner for record in records if record.owner)
    owner_types = _dedupe(record.owner_type for record in records if record.owner_type)
    files = tuple(_dedupe(path for record in records for path in record.files_or_modules))

    return PlanBranchGroup(
        group_id=f"branch-group-{group_index}",
        branch_name=_branch_name(branch_prefix, plan_id, branch_stem),
        task_ids=task_ids,
        files_or_modules=files,
        owner=owners[0] if len(owners) == 1 else None,
        owner_type=owner_types[0] if len(owner_types) == 1 else None,
        isolation_level="serialized" if len(records) > 1 or reasons else "parallel",
        serialization_reasons=reasons,
    )


def _dependency_findings(
    records: tuple[_TaskRecord, ...],
    task_ids: set[str],
) -> list[PlanBranchIsolationFinding]:
    findings: list[PlanBranchIsolationFinding] = []
    for record in records:
        dependencies = [
            dependency_id for dependency_id in record.depends_on if dependency_id in task_ids
        ]
        if not dependencies:
            continue
        findings.append(
            PlanBranchIsolationFinding(
                code="dependency_serialization_required",
                severity="warning",
                reason=(
                    f"Task {record.task_id} depends on prerequisite task(s): "
                    + ", ".join(dependencies)
                    + "."
                ),
                suggested_remediation=(
                    "Dispatch these tasks sequentially on the same branch group, "
                    "or complete the prerequisite branch before starting the dependent branch."
                ),
                task_ids=(record.task_id, *dependencies),
            )
        )
    return findings


def _file_contention_findings(
    records: tuple[_TaskRecord, ...],
) -> list[PlanBranchIsolationFinding]:
    findings: list[PlanBranchIsolationFinding] = []
    for left_index, left in enumerate(records):
        for right in records[left_index + 1 :]:
            overlaps = _overlapping_paths(left.files_or_modules, right.files_or_modules)
            if not overlaps:
                continue
            findings.append(
                PlanBranchIsolationFinding(
                    code="file_contention",
                    severity="warning",
                    reason=(
                        f"Tasks {left.task_id} and {right.task_id} touch overlapping "
                        "files_or_modules: " + ", ".join(overlaps) + "."
                    ),
                    suggested_remediation=(
                        "Keep the tasks on one serialized branch group or split the file ownership "
                        "so autonomous agents do not edit the same paths in parallel."
                    ),
                    task_ids=(left.task_id, right.task_id),
                    file_paths=tuple(overlaps),
                )
            )
    return findings


def _large_or_risky_scope_findings(
    records: tuple[_TaskRecord, ...],
) -> list[PlanBranchIsolationFinding]:
    findings: list[PlanBranchIsolationFinding] = []
    for record in records:
        reasons: list[str] = []
        if record.risk_level in _HIGH_RISK_VALUES:
            reasons.append(f"{record.risk_level} risk")
        if record.estimated_complexity in _LARGE_SCOPE_VALUES:
            reasons.append(f"{record.estimated_complexity} estimated complexity")
        if not reasons:
            continue
        findings.append(
            PlanBranchIsolationFinding(
                code="manual_branch_review_recommended",
                severity="info",
                reason=f"Task {record.task_id} has " + " and ".join(reasons) + ".",
                suggested_remediation=(
                    "Review branch scope before dispatch and avoid combining this task with "
                    "unrelated parallel work."
                ),
                task_ids=(record.task_id,),
                file_paths=record.files_or_modules,
            )
        )
    return findings


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


def _task_records(tasks: list[dict[str, Any]]) -> tuple[_TaskRecord, ...]:
    records: list[_TaskRecord] = []
    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        if task_id in seen_ids:
            continue
        seen_ids.add(task_id)
        metadata = task.get("metadata")
        records.append(
            _TaskRecord(
                task=task,
                task_id=task_id,
                title=_optional_text(task.get("title")) or task_id,
                depends_on=_dependency_ids(task),
                files_or_modules=tuple(
                    _dedupe(
                        _normalized_path(path) for path in _strings(task.get("files_or_modules"))
                    )
                ),
                owner=_owner(task),
                owner_type=_optional_text(task.get("owner_type"))
                or _optional_text(_metadata_value(metadata, "owner_type")),
                risk_level=(
                    _optional_text(task.get("risk_level"))
                    or _optional_text(task.get("risk"))
                    or _optional_text(_metadata_value(metadata, "risk_level"))
                    or _optional_text(_metadata_value(metadata, "risk"))
                    or "unspecified"
                ).lower(),
                estimated_complexity=(
                    _optional_text(task.get("estimated_complexity"))
                    or _optional_text(_metadata_value(metadata, "estimated_complexity"))
                    or "unspecified"
                ).lower(),
            )
        )
    return tuple(records)


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


def _dependency_ids(task: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(_dedupe(_strings(task.get("depends_on"))))


def _owner(task: Mapping[str, Any]) -> str | None:
    metadata = task.get("metadata")
    return (
        _optional_text(task.get("owner"))
        or _optional_text(task.get("assignee"))
        or _optional_text(_metadata_value(metadata, "owner"))
        or _optional_text(_metadata_value(metadata, "assignee"))
    )


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


def _overlapping_paths(
    left_values: Iterable[str],
    right_values: Iterable[str],
) -> list[str]:
    overlaps: list[str] = []
    left_paths = [_normalized_path(value) for value in left_values]
    right_paths = [_normalized_path(value) for value in right_values]
    for left_path in left_paths:
        if not left_path:
            continue
        for right_path in right_paths:
            if not right_path:
                continue
            if _paths_overlap(left_path, right_path):
                overlaps.append(
                    left_path if left_path == right_path else f"{left_path} <-> {right_path}"
                )
    return _dedupe(overlaps)


def _paths_overlap(left_path: str, right_path: str) -> bool:
    if left_path == right_path:
        return True
    left_parts = PurePosixPath(left_path).parts
    right_parts = PurePosixPath(right_path).parts
    return (
        left_parts == right_parts[: len(left_parts)]
        or right_parts == left_parts[: len(right_parts)]
    )


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _branch_name(branch_prefix: str, plan_id: str, branch_stem: str) -> str:
    return "/".join(
        part
        for part in (
            _slug(branch_prefix) or "bp",
            _slug(plan_id) or "plan",
            _slug(branch_stem) or "tasks",
        )
        if part
    )


def _slug(value: str) -> str:
    slug = _SLUG_RE.sub("-", _text(value).lower().replace("\\", "/"))
    slug = re.sub(r"-{2,}", "-", slug).strip("-./")
    return slug[:80].strip("-./")


def _record_index(records: tuple[_TaskRecord, ...], task_id: str) -> int:
    for index, record in enumerate(records):
        if record.task_id == task_id:
            return index
    return len(records)


def _metadata_value(metadata: Any, key: str) -> Any:
    if not isinstance(metadata, Mapping):
        return None
    return metadata.get(key)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
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
    "BranchIsolationSeverity",
    "PlanBranchGroup",
    "PlanBranchIsolationAdvice",
    "PlanBranchIsolationFinding",
    "advise_plan_branch_isolation",
    "plan_branch_isolation_to_dict",
]
