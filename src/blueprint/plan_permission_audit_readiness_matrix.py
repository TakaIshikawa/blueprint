"""Build permission audit readiness matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


PermissionAuditArea = Literal[
    "role_inventory",
    "privileged_permission_review",
    "delegated_access",
    "migration_diff_evidence",
    "approval_owners",
    "post_launch_sampling",
]
PermissionAuditPriority = Literal["high", "medium", "low"]
PermissionAuditReadiness = Literal["ready", "partial", "blocked"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_AREA_ORDER: dict[PermissionAuditArea, int] = {
    "role_inventory": 0,
    "privileged_permission_review": 1,
    "delegated_access": 2,
    "migration_diff_evidence": 3,
    "approval_owners": 4,
    "post_launch_sampling": 5,
}
_PRIORITY_ORDER: dict[PermissionAuditPriority, int] = {"high": 0, "medium": 1, "low": 2}
_READINESS_ORDER: dict[PermissionAuditReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_PERMISSION_RE = re.compile(
    r"\b(?:permission|permissions|role|roles|rbac|access control|acl|privileged|admin|"
    r"delegated access|migration diff|approval owner|audit sample)\b",
    re.I,
)
_AREA_PATTERNS: dict[PermissionAuditArea, re.Pattern[str]] = {
    "role_inventory": re.compile(
        r"\b(?:role inventory|role catalog|roles? list|rbac inventory|permission inventory|"
        r"current roles?|target roles?|role matrix)\b",
        re.I,
    ),
    "privileged_permission_review": re.compile(
        r"\b(?:privileged permissions?|privileged access|admin permissions?|superuser|root access|"
        r"elevated access|dangerous permission|break[- ]?glass)\b",
        re.I,
    ),
    "delegated_access": re.compile(
        r"\b(?:delegated access|impersonation|act as|service account|oauth delegation|"
        r"delegated permission|shared mailbox|support access)\b",
        re.I,
    ),
    "migration_diff_evidence": re.compile(
        r"\b(?:migration diff|permission diff|rbac diff|role diff|access diff|before and after|"
        r"before/after|baseline diff|changed permissions?|diff evidence)\b",
        re.I,
    ),
    "approval_owners": re.compile(
        r"\b(?:approval owner|approval owners|approver|owner approval|security approval|"
        r"data owner approval|role owner|permission owner|sign[- ]?off|signoff)\b",
        re.I,
    ),
    "post_launch_sampling": re.compile(
        r"\b(?:post[- ]launch sampling|audit sample|access sample|sampling plan|sample review|"
        r"post launch review|after launch audit|spot check)\b",
        re.I,
    ),
}
_OWNER_KEYS = (
    "owner",
    "owners",
    "owner_hint",
    "owner_team",
    "team",
    "dri",
    "security_owner",
    "approval_owner",
    "approval_owners",
    "role_owner",
    "permission_owner",
    "access_owner",
)
_NEXT_ACTIONS: dict[PermissionAuditArea, str] = {
    "role_inventory": "Attach the source and target role inventory for every changed permission surface.",
    "privileged_permission_review": "Review elevated permissions with security and remove unnecessary privilege.",
    "delegated_access": "Document delegated access flows, actors, scopes, and revocation behavior.",
    "migration_diff_evidence": "Attach before-and-after permission diff evidence for the migration.",
    "approval_owners": "Assign explicit approval owners for role and permission changes.",
    "post_launch_sampling": "Define post-launch sampling of changed roles and privileged access grants.",
}
_GAP_MESSAGES: dict[PermissionAuditArea, str] = {
    "role_inventory": "Missing role inventory.",
    "privileged_permission_review": "Missing privileged permission review.",
    "delegated_access": "Missing delegated access review.",
    "migration_diff_evidence": "Missing permission migration diff evidence.",
    "approval_owners": "Missing approval owners.",
    "post_launch_sampling": "Missing post-launch sampling plan.",
}


@dataclass(frozen=True, slots=True)
class PlanPermissionAuditReadinessRow:
    """One permission audit readiness row."""

    area: PermissionAuditArea
    owner: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: PermissionAuditReadiness = "partial"
    priority: PermissionAuditPriority = "medium"
    next_action: str = ""
    task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "area": self.area,
            "owner": self.owner,
            "evidence": list(self.evidence),
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "priority": self.priority,
            "next_action": self.next_action,
            "task_ids": list(self.task_ids),
        }


@dataclass(frozen=True, slots=True)
class PlanPermissionAuditReadinessMatrix:
    """Plan-level permission audit readiness matrix and rollup counts."""

    plan_id: str | None = None
    rows: tuple[PlanPermissionAuditReadinessRow, ...] = field(default_factory=tuple)
    permission_task_ids: tuple[str, ...] = field(default_factory=tuple)
    high_priority_gap_areas: tuple[PermissionAuditArea, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanPermissionAuditReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "permission_task_ids": list(self.permission_task_ids),
            "high_priority_gap_areas": list(self.high_priority_gap_areas),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return permission audit rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Permission Audit Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        counts = self.summary.get("priority_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('ready_area_count', 0)} of "
                f"{self.summary.get('area_count', 0)} permission audit areas ready "
                f"(high: {counts.get('high', 0)}, medium: {counts.get('medium', 0)}, "
                f"low: {counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No permission audit readiness rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Area | Owner | Readiness | Priority | Evidence | Gaps | Next Action | Tasks |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.area} | "
                f"{_markdown_cell(row.owner)} | "
                f"{row.readiness} | "
                f"{row.priority} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell(row.next_action)} | "
                f"{_markdown_cell(', '.join(row.task_ids) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_permission_audit_readiness_matrix(source: Any) -> PlanPermissionAuditReadinessMatrix:
    """Build required permission audit readiness rows for an execution plan."""
    plan_id, tasks = _source_payload(source)
    area_evidence: dict[PermissionAuditArea, list[str]] = {area: [] for area in _AREA_ORDER}
    area_task_ids: dict[PermissionAuditArea, list[str]] = {area: [] for area in _AREA_ORDER}
    area_owners: dict[PermissionAuditArea, list[str]] = {area: [] for area in _AREA_ORDER}
    permission_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        texts = _candidate_texts(task)
        context = " ".join(text for _, text in texts)
        owners = _owner_hints(task)
        task_has_permission_signal = bool(_PERMISSION_RE.search(context))
        for area, pattern in _AREA_PATTERNS.items():
            matches = [
                _evidence_snippet(source_field, text)
                for source_field, text in texts
                if pattern.search(text)
            ]
            if matches:
                task_has_permission_signal = True
                area_evidence[area].extend(matches)
                area_task_ids[area].append(task_id)
                area_owners[area].extend(owners)
        if task_has_permission_signal:
            permission_task_ids.append(task_id)
            for area in _AREA_ORDER:
                area_owners[area].extend(owners)

    rows = tuple(_row(area, area_evidence[area], area_task_ids[area], area_owners[area]) for area in _AREA_ORDER)
    high_priority_gap_areas = tuple(row.area for row in rows if row.priority == "high" and row.gaps)
    return PlanPermissionAuditReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        permission_task_ids=tuple(_dedupe(permission_task_ids)),
        high_priority_gap_areas=high_priority_gap_areas,
        summary=_summary(len(tasks), rows, permission_task_ids),
    )


def generate_plan_permission_audit_readiness_matrix(source: Any) -> PlanPermissionAuditReadinessMatrix:
    """Generate a permission audit readiness matrix from a plan-like source."""
    return build_plan_permission_audit_readiness_matrix(source)


def plan_permission_audit_readiness_matrix_to_dict(
    matrix: PlanPermissionAuditReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a permission audit readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_permission_audit_readiness_matrix_to_dict.__test__ = False


def plan_permission_audit_readiness_matrix_to_markdown(
    matrix: PlanPermissionAuditReadinessMatrix,
) -> str:
    """Render a permission audit readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_permission_audit_readiness_matrix_to_markdown.__test__ = False


def _row(
    area: PermissionAuditArea,
    evidence: Iterable[str],
    task_ids: Iterable[str],
    owners: Iterable[str],
) -> PlanPermissionAuditReadinessRow:
    evidence_tuple = tuple(_dedupe(evidence))
    owner_values = _dedupe(owners)
    gaps: list[str] = []
    if not evidence_tuple:
        gaps.append(_GAP_MESSAGES[area])
    if not owner_values:
        gaps.append("Missing owner.")
    owner = next(iter(owner_values), "unassigned")
    readiness: PermissionAuditReadiness = "ready" if not gaps else "partial"
    priority: PermissionAuditPriority = "low" if not gaps else "medium"
    if not owner_values or area in {"migration_diff_evidence", "approval_owners"}:
        priority = "high" if gaps else "low"
    return PlanPermissionAuditReadinessRow(
        area=area,
        owner=owner,
        evidence=evidence_tuple,
        gaps=tuple(_dedupe(gaps)),
        readiness=readiness,
        priority=priority,
        next_action="Ready for permission audit handoff." if not gaps else _NEXT_ACTIONS[area],
        task_ids=tuple(_dedupe(task_ids)),
    )


def _summary(
    task_count: int,
    rows: Iterable[PlanPermissionAuditReadinessRow],
    permission_task_ids: Iterable[str],
) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "area_count": len(row_list),
        "ready_area_count": sum(1 for row in row_list if row.readiness == "ready"),
        "gap_area_count": sum(1 for row in row_list if row.gaps),
        "permission_task_count": len(tuple(_dedupe(permission_task_ids))),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "priority_counts": {
            priority: sum(1 for row in row_list if row.priority == priority)
            for priority in _PRIORITY_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
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
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "milestone", "owner_type", "risk_level", "test_command", "blocked_reason"):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("depends_on", "files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes", "risks"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            elif key_text:
                texts.append((field, key_text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _owner_hints(task: Mapping[str, Any]) -> list[str]:
    owners = []
    if owner := _optional_text(task.get("owner_type")):
        owners.append(owner)
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in _OWNER_KEYS:
            owners.extend(_strings(metadata.get(key)))
    return _dedupe(owners)


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "test_command",
        "status",
        "tags",
        "labels",
        "notes",
        "risks",
        "metadata",
        "blocked_reason",
        "tasks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


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


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


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


__all__ = [
    "PermissionAuditArea",
    "PermissionAuditPriority",
    "PermissionAuditReadiness",
    "PlanPermissionAuditReadinessMatrix",
    "PlanPermissionAuditReadinessRow",
    "build_plan_permission_audit_readiness_matrix",
    "generate_plan_permission_audit_readiness_matrix",
    "plan_permission_audit_readiness_matrix_to_dict",
    "plan_permission_audit_readiness_matrix_to_markdown",
]
