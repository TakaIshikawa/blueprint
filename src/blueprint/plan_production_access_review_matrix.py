"""Build production access review matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ProductionAccessControl = Literal[
    "reviewer_named",
    "privileged_role_scope",
    "approval_evidence",
    "break_glass_handling",
    "audit_log_evidence",
    "revocation_path",
    "review_frequency",
]
ProductionAccessRisk = Literal["high", "medium", "low"]
ProductionAccessSignal = Literal[
    "production_access",
    "privileged_role",
    "break_glass_access",
    "admin_console_permission",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CONTROL_ORDER: tuple[ProductionAccessControl, ...] = (
    "reviewer_named",
    "privileged_role_scope",
    "approval_evidence",
    "break_glass_handling",
    "audit_log_evidence",
    "revocation_path",
    "review_frequency",
)
_SIGNAL_ORDER: tuple[ProductionAccessSignal, ...] = (
    "production_access",
    "privileged_role",
    "break_glass_access",
    "admin_console_permission",
)
_RISK_ORDER: dict[ProductionAccessRisk, int] = {"high": 0, "medium": 1, "low": 2}

_SIGNAL_PATTERNS: dict[ProductionAccessSignal, re.Pattern[str]] = {
    "production_access": re.compile(
        r"\b(?:production access|prod access|access to prod(?:uction)?|production data access|"
        r"live environment access|prod console|prod shell|production shell|production account)\b",
        re.I,
    ),
    "privileged_role": re.compile(
        r"\b(?:privileged role|privileged access|elevated role|elevated access|"
        r"superuser|root access|owner role|admin role|administrator role|iam admin|"
        r"security admin|database admin|db admin|platform admin)\b",
        re.I,
    ),
    "break_glass_access": re.compile(
        r"\b(?:break[- ]?glass|emergency access|emergency admin|emergency role|"
        r"just[- ]?in[- ]?time access|jit access|temporary elevated access)\b",
        re.I,
    ),
    "admin_console_permission": re.compile(
        r"\b(?:admin console|administrator console|management console|console permissions?|"
        r"console access|permission set|rbac|iam policy|admin permissions?|"
        r"workspace admin|tenant admin)\b",
        re.I,
    ),
}
_CONTROL_PATTERNS: dict[ProductionAccessControl, re.Pattern[str]] = {
    "reviewer_named": re.compile(
        r"\b(?:reviewer(?: is|:)?|reviewed by|access reviewer|access review owner|"
        r"owner review(?:er)?|security reviewer|iam reviewer|compliance reviewer)\s+"
        r"[A-Z][A-Za-z0-9_.@& /-]{1,80}\b",
        re.I,
    ),
    "privileged_role_scope": re.compile(
        r"\b(?:role scope|scoped role|scope of access|least privilege|least-privilege|"
        r"permission scope|rbac scope|iam scope|limited to|read[- ]?only|"
        r"write access to|admin scope|only for (?:the )?[A-Za-z0-9_. /-]+)\b",
        re.I,
    ),
    "approval_evidence": re.compile(
        r"\b(?:approved by|approval from|approval ticket|approval evidence|access approval|"
        r"manager approval|security approval|change approval|sign[- ]?off from|"
        r"authorized by|authorization from)\s+[A-Z#A-Za-z0-9_.@& /:-]{1,80}\b",
        re.I,
    ),
    "break_glass_handling": re.compile(
        r"\b(?:break[- ]?glass (?:runbook|procedure|handling|process)|emergency access runbook|"
        r"emergency access expires?|temporary access expires?|jit approval|"
        r"just[- ]?in[- ]?time approval|rotate credentials|credential rotation)\b",
        re.I,
    ),
    "audit_log_evidence": re.compile(
        r"\b(?:audit logs?|audit trail|audit evidence|logged in (?:the )?siem|siem logging|"
        r"access logs?|admin activity log|cloudtrail|splunk|datadog audit|"
        r"log every access|monitor privileged access)\b",
        re.I,
    ),
    "revocation_path": re.compile(
        r"\b(?:revocation path|revoke access|access revocation|remove access|disable access|"
        r"deprovision|offboard|expire access|access expires?|rollback permissions?|"
        r"remove role|remove permission)\b",
        re.I,
    ),
    "review_frequency": re.compile(
        r"\b(?:(?:daily|weekly|monthly|quarterly|annual|yearly|biweekly|bi-weekly)\s+"
        r"(?:access )?review|review cadence|review frequency|recertification|"
        r"access certification|every \d+\s*(?:days?|weeks?|months?)|"
        r"within \d+\s*(?:hours?|days?) of access)\b",
        re.I,
    ),
}
_METADATA_CONTROL_KEYS: dict[ProductionAccessControl, set[str]] = {
    "reviewer_named": {
        "reviewer",
        "reviewers",
        "access_reviewer",
        "access_review_owner",
        "security_reviewer",
        "compliance_reviewer",
    },
    "privileged_role_scope": {
        "role_scope",
        "privileged_role_scope",
        "permission_scope",
        "rbac_scope",
        "iam_scope",
        "access_scope",
    },
    "approval_evidence": {
        "approver",
        "approvers",
        "approved_by",
        "approval",
        "approval_ticket",
        "access_approval",
        "security_approval",
    },
    "break_glass_handling": {
        "break_glass",
        "break_glass_runbook",
        "emergency_access",
        "jit_access",
        "temporary_access",
    },
    "audit_log_evidence": {
        "audit_log",
        "audit_logs",
        "audit_evidence",
        "siem",
        "logging",
        "cloudtrail",
    },
    "revocation_path": {
        "revocation",
        "revocation_path",
        "revoke_access",
        "access_expiry",
        "expires_at",
        "deprovisioning",
    },
    "review_frequency": {
        "review_frequency",
        "review_cadence",
        "access_review_cadence",
        "recertification",
        "certification_frequency",
    },
}


@dataclass(frozen=True, slots=True)
class PlanProductionAccessReviewRow:
    """One task-level production access review record."""

    task_id: str
    task_title: str
    access_signals: tuple[ProductionAccessSignal, ...]
    present_controls: tuple[ProductionAccessControl, ...] = field(default_factory=tuple)
    missing_controls: tuple[ProductionAccessControl, ...] = field(default_factory=tuple)
    reviewer_evidence: tuple[str, ...] = field(default_factory=tuple)
    approval_evidence: tuple[str, ...] = field(default_factory=tuple)
    audit_log_evidence: tuple[str, ...] = field(default_factory=tuple)
    revocation_evidence: tuple[str, ...] = field(default_factory=tuple)
    review_frequency: str | None = None
    risk_level: ProductionAccessRisk = "medium"
    access_evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def access_signal(self) -> ProductionAccessSignal | None:
        """Compatibility view for consumers that expect a primary access signal."""
        return self.access_signals[0] if self.access_signals else None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "access_signals": list(self.access_signals),
            "present_controls": list(self.present_controls),
            "missing_controls": list(self.missing_controls),
            "reviewer_evidence": list(self.reviewer_evidence),
            "approval_evidence": list(self.approval_evidence),
            "audit_log_evidence": list(self.audit_log_evidence),
            "revocation_evidence": list(self.revocation_evidence),
            "review_frequency": self.review_frequency,
            "risk_level": self.risk_level,
            "access_evidence": list(self.access_evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanProductionAccessReviewMatrix:
    """Plan-level production access review matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanProductionAccessReviewRow, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanProductionAccessReviewRow, ...]:
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
        """Return production access review matrix rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Production Access Review Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('access_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks require production access review "
                f"(high: {risk_counts.get('high', 0)}, "
                f"medium: {risk_counts.get('medium', 0)}, "
                f"low: {risk_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No production access review rows were inferred."])
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
                (
                    "| Task | Title | Signals | Present Controls | Missing Controls | "
                    "Reviewer | Approval | Audit Logs | Revocation | Cadence | Risk | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.task_title)} | "
                f"{_markdown_cell(', '.join(row.access_signals))} | "
                f"{_markdown_cell(', '.join(row.present_controls) or 'none')} | "
                f"{_markdown_cell(', '.join(row.missing_controls) or 'none')} | "
                f"{_markdown_cell('; '.join(row.reviewer_evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.approval_evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.audit_log_evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.revocation_evidence) or 'none')} | "
                f"{_markdown_cell(row.review_frequency or 'none')} | "
                f"{row.risk_level} | "
                f"{_markdown_cell('; '.join(row.access_evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(
                [
                    "",
                    f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}",
                ]
            )
        return "\n".join(lines)


def build_plan_production_access_review_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanProductionAccessReviewMatrix:
    """Build task-level production access review controls for an execution plan."""
    plan_id, tasks = _source_payload(source)
    rows: list[PlanProductionAccessReviewRow] = []
    not_applicable_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index)
        if row:
            rows.append(row)
        else:
            not_applicable_task_ids.append(_task_id(task, index))

    rows.sort(
        key=lambda row: (
            _RISK_ORDER[row.risk_level],
            -len(row.missing_controls),
            _signal_index(row.access_signals),
            row.task_id,
        )
    )
    return PlanProductionAccessReviewMatrix(
        plan_id=plan_id,
        rows=tuple(rows),
        not_applicable_task_ids=tuple(not_applicable_task_ids),
        summary=_summary(len(tasks), rows),
    )


def generate_plan_production_access_review_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanProductionAccessReviewMatrix:
    """Generate a production access review matrix from a plan-like source."""
    return build_plan_production_access_review_matrix(source)


def derive_plan_production_access_review_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanProductionAccessReviewMatrix:
    """Derive a production access review matrix from a plan-like source."""
    return build_plan_production_access_review_matrix(source)


def extract_plan_production_access_review_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanProductionAccessReviewMatrix:
    """Extract a production access review matrix from a plan-like source."""
    return derive_plan_production_access_review_matrix(source)


def summarize_plan_production_access_review_matrix(
    matrix: PlanProductionAccessReviewMatrix | Iterable[PlanProductionAccessReviewRow],
) -> dict[str, Any]:
    """Return deterministic summary counts for a matrix or row iterable."""
    if isinstance(matrix, PlanProductionAccessReviewMatrix):
        return dict(matrix.summary)
    rows = list(matrix)
    return _summary(len(rows), rows)


def plan_production_access_review_matrix_to_dict(
    matrix: PlanProductionAccessReviewMatrix,
) -> dict[str, Any]:
    """Serialize a production access review matrix to a plain dictionary."""
    return matrix.to_dict()


plan_production_access_review_matrix_to_dict.__test__ = False


def plan_production_access_review_matrix_to_dicts(
    matrix: PlanProductionAccessReviewMatrix | Iterable[PlanProductionAccessReviewRow],
) -> list[dict[str, Any]]:
    """Serialize production access review rows to plain dictionaries."""
    if isinstance(matrix, PlanProductionAccessReviewMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_production_access_review_matrix_to_dicts.__test__ = False


def plan_production_access_review_matrix_to_markdown(
    matrix: PlanProductionAccessReviewMatrix,
) -> str:
    """Render a production access review matrix as Markdown."""
    return matrix.to_markdown()


plan_production_access_review_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanProductionAccessReviewRow | None:
    task_id = _task_id(task, index)
    texts = _candidate_texts(task)
    signals, signal_evidence = _signals(texts)
    if not signals:
        return None

    controls, control_evidence = _controls(task, texts)
    missing_controls = tuple(control for control in _CONTROL_ORDER if control not in controls)
    return PlanProductionAccessReviewRow(
        task_id=task_id,
        task_title=_optional_text(task.get("title")) or task_id,
        access_signals=signals,
        present_controls=controls,
        missing_controls=missing_controls,
        reviewer_evidence=tuple(control_evidence.get("reviewer_named", ())),
        approval_evidence=tuple(control_evidence.get("approval_evidence", ())),
        audit_log_evidence=tuple(control_evidence.get("audit_log_evidence", ())),
        revocation_evidence=tuple(control_evidence.get("revocation_path", ())),
        review_frequency=_review_frequency(control_evidence.get("review_frequency", ())),
        risk_level=_risk_level(signals, controls),
        access_evidence=signal_evidence,
    )


def _summary(task_count: int, rows: Iterable[PlanProductionAccessReviewRow]) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "access_task_count": len(row_list),
        "not_applicable_task_count": task_count - len(row_list),
        "signal_counts": {
            signal: sum(1 for row in row_list if signal in row.access_signals)
            for signal in _SIGNAL_ORDER
        },
        "risk_counts": {
            risk: sum(1 for row in row_list if row.risk_level == risk) for risk in _RISK_ORDER
        },
        "missing_control_counts": {
            control: sum(1 for row in row_list if control in row.missing_controls)
            for control in _CONTROL_ORDER
        },
    }


def _signals(
    texts: Iterable[tuple[str, str]],
) -> tuple[tuple[ProductionAccessSignal, ...], tuple[str, ...]]:
    found: set[ProductionAccessSignal] = set()
    evidence: list[str] = []
    for field, text in texts:
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text):
                found.add(signal)
                evidence.append(_evidence_snippet(field, text))
    return (
        tuple(signal for signal in _SIGNAL_ORDER if signal in found),
        tuple(_dedupe(evidence)),
    )


def _controls(
    task: Mapping[str, Any],
    texts: Iterable[tuple[str, str]],
) -> tuple[
    tuple[ProductionAccessControl, ...],
    dict[ProductionAccessControl, tuple[str, ...]],
]:
    found: set[ProductionAccessControl] = set()
    evidence: dict[ProductionAccessControl, list[str]] = {
        control: [] for control in _CONTROL_ORDER
    }

    for field, text in texts:
        for control, pattern in _CONTROL_PATTERNS.items():
            if pattern.search(text):
                found.add(control)
                evidence[control].append(_evidence_snippet(field, text))
    for control, field, value in _metadata_control_values(task.get("metadata")):
        found.add(control)
        evidence[control].append(_evidence_snippet(field, value))

    ordered = tuple(control for control in _CONTROL_ORDER if control in found)
    return ordered, {control: tuple(_dedupe(values)) for control, values in evidence.items()}


def _metadata_control_values(
    value: Any,
    prefix: str = "metadata",
) -> list[tuple[ProductionAccessControl, str, str]]:
    if isinstance(value, Mapping):
        values: list[tuple[ProductionAccessControl, str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            normalized_key = str(key).casefold().replace("-", "_")
            for control, keys in _METADATA_CONTROL_KEYS.items():
                if normalized_key in keys:
                    values.extend((control, field, text) for text in _strings(child))
            if isinstance(child, (Mapping, list, tuple, set)):
                values.extend(_metadata_control_values(child, field))
        return values
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        values: list[tuple[ProductionAccessControl, str, str]] = []
        for index, item in enumerate(items):
            values.extend(_metadata_control_values(item, f"{prefix}[{index}]"))
        return values
    return []


def _review_frequency(evidence: Iterable[str]) -> str | None:
    for item in evidence:
        text = _text(item)
        cadence = re.search(
            r"\b(daily|weekly|biweekly|bi-weekly|monthly|quarterly|annual|yearly)\b",
            text,
            re.I,
        )
        if cadence:
            return cadence.group(1).lower()
        interval = re.search(r"\bevery \d+\s*(?:days?|weeks?|months?)\b", text, re.I)
        if interval:
            return interval.group(0).lower()
        within = re.search(r"\bwithin \d+\s*(?:hours?|days?)\b", text, re.I)
        if within:
            return within.group(0).lower()
    return None


def _risk_level(
    signals: Iterable[ProductionAccessSignal],
    controls: Iterable[ProductionAccessControl],
) -> ProductionAccessRisk:
    signal_set = set(signals)
    control_set = set(controls)
    missing = set(_CONTROL_ORDER) - control_set
    if "break_glass_access" in signal_set and "break_glass_handling" in missing:
        return "high"
    if "privileged_role" in signal_set and (
        "approval_evidence" in missing
        or "audit_log_evidence" in missing
        or "revocation_path" in missing
    ):
        return "high"
    if "admin_console_permission" in signal_set and (
        "privileged_role_scope" in missing or "review_frequency" in missing
    ):
        return "high"
    if {"approval_evidence", "audit_log_evidence", "revocation_path"} & missing:
        return "high"
    if not missing:
        return "low"
    return "medium"


def _signal_index(signals: tuple[ProductionAccessSignal, ...]) -> int:
    if not signals:
        return len(_SIGNAL_ORDER)
    return _SIGNAL_ORDER.index(signals[0])


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
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
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
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
        if hasattr(item, "model_dump"):
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
        "description",
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
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
    "PlanProductionAccessReviewMatrix",
    "PlanProductionAccessReviewRow",
    "ProductionAccessControl",
    "ProductionAccessRisk",
    "ProductionAccessSignal",
    "build_plan_production_access_review_matrix",
    "derive_plan_production_access_review_matrix",
    "extract_plan_production_access_review_matrix",
    "generate_plan_production_access_review_matrix",
    "plan_production_access_review_matrix_to_dict",
    "plan_production_access_review_matrix_to_dicts",
    "plan_production_access_review_matrix_to_markdown",
    "summarize_plan_production_access_review_matrix",
]
