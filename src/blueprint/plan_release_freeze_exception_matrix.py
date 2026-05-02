"""Build release-freeze exception matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FreezeExceptionReason = Literal[
    "security_fix",
    "production_incident",
    "compliance_deadline",
    "customer_commitment",
    "data_integrity",
    "operational_unblock",
]
FreezeExceptionControl = Literal[
    "approver_named",
    "rollback_plan",
    "blast_radius",
    "validation_command",
    "communication_plan",
    "timebox",
    "post_release_review",
]
FreezeExceptionRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_REASON_ORDER: tuple[FreezeExceptionReason, ...] = (
    "security_fix",
    "production_incident",
    "compliance_deadline",
    "customer_commitment",
    "data_integrity",
    "operational_unblock",
)
_CONTROL_ORDER: tuple[FreezeExceptionControl, ...] = (
    "approver_named",
    "rollback_plan",
    "blast_radius",
    "validation_command",
    "communication_plan",
    "timebox",
    "post_release_review",
)
_RISK_ORDER: dict[FreezeExceptionRisk, int] = {"high": 0, "medium": 1, "low": 2}

_FREEZE_RE = re.compile(
    r"\b(?:release freeze|holiday freeze|incident freeze|production freeze|code freeze|"
    r"change freeze|freeze window|deployment freeze|change moratorium|deployment moratorium|"
    r"release moratorium|moratorium|blackout(?: period| window)?)\b",
    re.I,
)
_EXCEPTION_RE = re.compile(
    r"\b(?:freeze exception|exception to (?:the )?(?:release |holiday |incident |change )?freeze|"
    r"freeze waiver|waiver|override|break[- ]?glass|emergency change|expedited change|"
    r"ship during|deploy during|release during|must ship|must deploy|must release|"
    r"hotfix|emergency patch)\b",
    re.I,
)
_SHIP_RE = re.compile(r"\b(?:ship|deploy|release|rollout|launch|promote|hotfix|patch)\b", re.I)

_REASON_PATTERNS: dict[FreezeExceptionReason, re.Pattern[str]] = {
    "security_fix": re.compile(
        r"\b(?:security|vulnerabilit(?:y|ies)|cve|zero[- ]day|exploit|auth bypass|"
        r"permission bypass|security patch|security fix)\b",
        re.I,
    ),
    "production_incident": re.compile(
        r"\b(?:production incident|prod incident|incident|outage|sev\s*[0-2]|sev[- ]?[0-2]|"
        r"p0|p1|production down|prod down|service degradation|customer-impacting incident)\b",
        re.I,
    ),
    "compliance_deadline": re.compile(
        r"\b(?:compliance|regulatory|regulator|audit|legal deadline|statutory|sox|soc2|"
        r"gdpr|hipaa|pci|deadline)\b",
        re.I,
    ),
    "customer_commitment": re.compile(
        r"\b(?:customer commitment|customer promise|contractual commitment|contractual deadline|"
        r"enterprise commitment|customer deadline|sla commitment|launch commitment|committed date)\b",
        re.I,
    ),
    "data_integrity": re.compile(
        r"\b(?:data integrity|data corruption|corrupt(?:ed|ion)|data loss|reconciliation|"
        r"reconcile|backfill|data repair|incorrect data|stale data|duplicate records?)\b",
        re.I,
    ),
    "operational_unblock": re.compile(
        r"\b(?:operational unblock|unblock operations|unblock support|ops unblock|manual ops|"
        r"runbook unblock|on[- ]call unblock|support escalation|blocked operations?)\b",
        re.I,
    ),
}
_CONTROL_PATTERNS: dict[FreezeExceptionControl, re.Pattern[str]] = {
    "approver_named": re.compile(
        r"\b(?:approved by|approver(?: is|:)?|approval from|sign[- ]?off from|"
        r"authorized by|cab approval from|change approver)\s+[A-Z][A-Za-z0-9_.@ -]{1,80}\b",
        re.I,
    ),
    "rollback_plan": re.compile(
        r"\b(?:rollback plan|roll back|rollback|revert plan|revert|restore plan|backout plan|"
        r"abort plan|recovery plan)\b",
        re.I,
    ),
    "blast_radius": re.compile(
        r"\b(?:blast radius|impact radius|affected users?|affected customers?|scope of impact|"
        r"limited to|percentage of traffic|traffic slice|service impact)\b",
        re.I,
    ),
    "validation_command": re.compile(
        r"\b(?:validation command|test command|smoke test|pytest|npm test|poetry run|"
        r"verification command|validate with|runbook validation|health check)\b",
        re.I,
    ),
    "communication_plan": re.compile(
        r"\b(?:communication plan|comms plan|notify|notification|status page|stakeholder update|"
        r"customer communication|support brief|announce)\b",
        re.I,
    ),
    "timebox": re.compile(
        r"\b(?:timebox|time-box|within \d+\s*(?:minutes?|hours?|days?)|for \d+\s*(?:minutes?|hours?|days?)|"
        r"expires?|expiry|until \d{1,2}:\d{2}|maintenance window|change window)\b",
        re.I,
    ),
    "post_release_review": re.compile(
        r"\b(?:post[- ]release review|post release review|postmortem|retrospective|after-action|"
        r"after action|follow[- ]up review|review after release)\b",
        re.I,
    ),
}
_APPROVER_METADATA_KEYS = {
    "approver",
    "approvers",
    "approved_by",
    "approval_by",
    "approval_owner",
    "change_approver",
    "cab_approver",
    "release_approver",
    "freeze_exception_approver",
}
_PLAN_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "description",
    "goal",
    "goals",
    "scope",
    "constraints",
    "risks",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "release_plan",
    "metadata",
    "notes",
)


@dataclass(frozen=True, slots=True)
class PlanReleaseFreezeExceptionRow:
    """One task-level release-freeze exception record."""

    task_id: str
    task_title: str
    exception_reasons: tuple[FreezeExceptionReason, ...]
    present_controls: tuple[FreezeExceptionControl, ...] = field(default_factory=tuple)
    missing_controls: tuple[FreezeExceptionControl, ...] = field(default_factory=tuple)
    approver_evidence: tuple[str, ...] = field(default_factory=tuple)
    risk_level: FreezeExceptionRisk = "medium"
    exception_evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def exception_reason(self) -> FreezeExceptionReason | None:
        """Compatibility view for consumers that expect a primary reason."""
        return self.exception_reasons[0] if self.exception_reasons else None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "exception_reasons": list(self.exception_reasons),
            "present_controls": list(self.present_controls),
            "missing_controls": list(self.missing_controls),
            "approver_evidence": list(self.approver_evidence),
            "risk_level": self.risk_level,
            "exception_evidence": list(self.exception_evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanReleaseFreezeExceptionMatrix:
    """Plan-level release-freeze exception matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanReleaseFreezeExceptionRow, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanReleaseFreezeExceptionRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.rows],
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return freeze exception matrix rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Release Freeze Exception Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('exception_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks require freeze exceptions "
                f"(high: {risk_counts.get('high', 0)}, "
                f"medium: {risk_counts.get('medium', 0)}, "
                f"low: {risk_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No release-freeze exception tasks were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(
                    [
                        "",
                        f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}",
                    ]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Reasons | Present Controls | Missing Controls | Approver Evidence | Risk | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.task_title)} | "
                f"{_markdown_cell(', '.join(row.exception_reasons) or 'uncategorized')} | "
                f"{_markdown_cell(', '.join(row.present_controls) or 'none')} | "
                f"{_markdown_cell(', '.join(row.missing_controls) or 'none')} | "
                f"{_markdown_cell('; '.join(row.approver_evidence) or 'none')} | "
                f"{row.risk_level} | "
                f"{_markdown_cell('; '.join(row.exception_evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(
                [
                    "",
                    f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}",
                ]
            )
        return "\n".join(lines)


def build_plan_release_freeze_exception_matrix(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> PlanReleaseFreezeExceptionMatrix:
    """Build task-level release-freeze exception controls for an execution plan."""
    plan_id, plan_texts, tasks = _source_payload(source)
    rows: list[PlanReleaseFreezeExceptionRow] = []
    not_applicable_task_ids: list[str] = []
    plan_has_freeze = any(_FREEZE_RE.search(text) for _, text in plan_texts)

    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index, plan_texts, plan_has_freeze)
        if row:
            rows.append(row)
        else:
            not_applicable_task_ids.append(_task_id(task, index))

    rows.sort(
        key=lambda row: (
            _RISK_ORDER[row.risk_level],
            _reason_index(row.exception_reasons),
            row.task_id,
        )
    )
    return PlanReleaseFreezeExceptionMatrix(
        plan_id=plan_id,
        rows=tuple(rows),
        not_applicable_task_ids=tuple(not_applicable_task_ids),
        summary=_summary(len(tasks), rows),
    )


def extract_plan_release_freeze_exception_matrix(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> PlanReleaseFreezeExceptionMatrix:
    """Extract a release-freeze exception matrix from a plan-like source."""
    return build_plan_release_freeze_exception_matrix(source)


def summarize_plan_release_freeze_exception_matrix(
    matrix: PlanReleaseFreezeExceptionMatrix | Iterable[PlanReleaseFreezeExceptionRow],
) -> dict[str, Any]:
    """Return deterministic summary counts for a matrix or row iterable."""
    if isinstance(matrix, PlanReleaseFreezeExceptionMatrix):
        return dict(matrix.summary)
    rows = list(matrix)
    return _summary(len(rows), rows)


def plan_release_freeze_exception_matrix_to_dict(
    matrix: PlanReleaseFreezeExceptionMatrix,
) -> dict[str, Any]:
    """Serialize a release-freeze exception matrix to a plain dictionary."""
    return matrix.to_dict()


plan_release_freeze_exception_matrix_to_dict.__test__ = False


def plan_release_freeze_exception_matrix_to_dicts(
    matrix: PlanReleaseFreezeExceptionMatrix | Iterable[PlanReleaseFreezeExceptionRow],
) -> list[dict[str, Any]]:
    """Serialize release-freeze exception rows to plain dictionaries."""
    if isinstance(matrix, PlanReleaseFreezeExceptionMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_release_freeze_exception_matrix_to_dicts.__test__ = False


def plan_release_freeze_exception_matrix_to_markdown(
    matrix: PlanReleaseFreezeExceptionMatrix,
) -> str:
    """Render a release-freeze exception matrix as Markdown."""
    return matrix.to_markdown()


plan_release_freeze_exception_matrix_to_markdown.__test__ = False


def _task_row(
    task: Mapping[str, Any],
    index: int,
    plan_texts: Iterable[tuple[str, str]],
    plan_has_freeze: bool,
) -> PlanReleaseFreezeExceptionRow | None:
    task_id = _task_id(task, index)
    task_texts = _candidate_texts(task)
    task_has_freeze = any(_FREEZE_RE.search(text) for _, text in task_texts)
    task_has_exception = any(_EXCEPTION_RE.search(text) for _, text in task_texts)
    task_ships = any(_SHIP_RE.search(text) for _, text in task_texts)
    if not (
        (task_has_freeze and (task_has_exception or task_ships))
        or (plan_has_freeze and task_has_exception and task_ships)
    ):
        return None

    all_texts = list(plan_texts) + task_texts
    reasons = _reasons(all_texts)
    controls, control_evidence = _controls(task, all_texts)
    missing_controls = tuple(control for control in _CONTROL_ORDER if control not in controls)
    approver_evidence = tuple(control_evidence.get("approver_named", ()))
    exception_evidence = _exception_evidence(all_texts)
    return PlanReleaseFreezeExceptionRow(
        task_id=task_id,
        task_title=_optional_text(task.get("title")) or task_id,
        exception_reasons=reasons or ("operational_unblock",),
        present_controls=controls,
        missing_controls=missing_controls,
        approver_evidence=approver_evidence,
        risk_level=_risk_level(controls),
        exception_evidence=exception_evidence,
    )


def _summary(task_count: int, rows: Iterable[PlanReleaseFreezeExceptionRow]) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "exception_count": len(row_list),
        "reason_counts": {
            reason: sum(1 for row in row_list if reason in row.exception_reasons)
            for reason in _REASON_ORDER
        },
        "risk_counts": {
            risk: sum(1 for row in row_list if row.risk_level == risk) for risk in _RISK_ORDER
        },
        "missing_control_counts": {
            control: sum(1 for row in row_list if control in row.missing_controls)
            for control in _CONTROL_ORDER
        },
    }


def _reasons(texts: Iterable[tuple[str, str]]) -> tuple[FreezeExceptionReason, ...]:
    found: set[FreezeExceptionReason] = set()
    for _, text in texts:
        for reason, pattern in _REASON_PATTERNS.items():
            if pattern.search(text):
                found.add(reason)
    return tuple(reason for reason in _REASON_ORDER if reason in found)


def _controls(
    task: Mapping[str, Any],
    texts: Iterable[tuple[str, str]],
) -> tuple[tuple[FreezeExceptionControl, ...], dict[FreezeExceptionControl, tuple[str, ...]]]:
    found: set[FreezeExceptionControl] = set()
    evidence: dict[FreezeExceptionControl, list[str]] = {control: [] for control in _CONTROL_ORDER}

    if test_command := _optional_text(task.get("test_command")):
        found.add("validation_command")
        evidence["validation_command"].append(_evidence_snippet("test_command", test_command))

    for field, text in texts:
        for control, pattern in _CONTROL_PATTERNS.items():
            if pattern.search(text):
                found.add(control)
                evidence[control].append(_evidence_snippet(field, text))
    for field, value in _approver_metadata_values(task.get("metadata")):
        found.add("approver_named")
        evidence["approver_named"].append(_evidence_snippet(field, value))

    ordered = tuple(control for control in _CONTROL_ORDER if control in found)
    return ordered, {control: tuple(_dedupe(values)) for control, values in evidence.items()}


def _approver_metadata_values(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        values: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            normalized_key = str(key).casefold().replace("-", "_")
            if normalized_key in _APPROVER_METADATA_KEYS:
                values.extend((field, text) for text in _strings(child))
            if isinstance(child, (Mapping, list, tuple, set)):
                values.extend(_approver_metadata_values(child, field))
        return values
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        values: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            values.extend(_approver_metadata_values(item, f"{prefix}[{index}]"))
        return values
    return []


def _exception_evidence(texts: Iterable[tuple[str, str]]) -> tuple[str, ...]:
    evidence: list[str] = []
    for field, text in texts:
        if _FREEZE_RE.search(text) or _EXCEPTION_RE.search(text):
            evidence.append(_evidence_snippet(field, text))
    return tuple(_dedupe(evidence))


def _risk_level(controls: Iterable[FreezeExceptionControl]) -> FreezeExceptionRisk:
    control_set = set(controls)
    if "approver_named" not in control_set or "rollback_plan" not in control_set:
        return "high"
    if all(control in control_set for control in _CONTROL_ORDER):
        return "low"
    return "medium"


def _reason_index(reasons: tuple[FreezeExceptionReason, ...]) -> int:
    if not reasons:
        return len(_REASON_ORDER)
    return _REASON_ORDER.index(reasons[0])


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> tuple[str | None, list[tuple[str, str]], list[dict[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return _optional_text(source.id), _plan_texts(payload), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _plan_texts(payload), _task_payloads(payload.get("tasks"))
        return None, [], [dict(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _plan_texts(payload), _task_payloads(payload.get("tasks"))
    if _looks_like_task(source):
        return None, [], [_object_payload(source)]
    return None, [], []


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
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
    return tasks


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
        "summary",
        "description",
        "goal",
        "goals",
        "scope",
        "constraints",
        "risks",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "definition_of_done",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tags",
        "labels",
        "notes",
        "release_plan",
        "tasks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _plan_texts(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in _PLAN_FIELDS:
        if field_name == "metadata":
            texts.extend(_metadata_texts(plan.get(field_name), "plan.metadata"))
        else:
            for index, text in enumerate(_strings(plan.get(field_name))):
                suffix = f"[{index}]" if isinstance(plan.get(field_name), (list, tuple, set)) else ""
                texts.append((f"plan.{field_name}{suffix}", text))
    return texts


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "definition_of_done",
        "tags",
        "labels",
        "notes",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    texts.extend(_metadata_texts(task.get("metadata")))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.append((field, key_text))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            else:
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
    "FreezeExceptionControl",
    "FreezeExceptionReason",
    "FreezeExceptionRisk",
    "PlanReleaseFreezeExceptionMatrix",
    "PlanReleaseFreezeExceptionRow",
    "build_plan_release_freeze_exception_matrix",
    "extract_plan_release_freeze_exception_matrix",
    "plan_release_freeze_exception_matrix_to_dict",
    "plan_release_freeze_exception_matrix_to_dicts",
    "plan_release_freeze_exception_matrix_to_markdown",
    "summarize_plan_release_freeze_exception_matrix",
]
