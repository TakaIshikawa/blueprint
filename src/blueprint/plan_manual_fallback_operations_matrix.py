"""Build plan-level manual fallback operations matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FallbackSignal = Literal[
    "manual_processing",
    "support_override",
    "offline_procedure",
    "break_glass_access",
    "manual_reconciliation",
    "customer_communication",
    "manual_rollback",
]
FallbackReadinessLevel = Literal["incomplete", "partial", "ready"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: dict[FallbackSignal, int] = {
    "manual_processing": 0,
    "support_override": 1,
    "offline_procedure": 2,
    "break_glass_access": 3,
    "manual_reconciliation": 4,
    "customer_communication": 5,
    "manual_rollback": 6,
}
_READINESS_ORDER: dict[FallbackReadinessLevel, int] = {
    "incomplete": 0,
    "partial": 1,
    "ready": 2,
}
_SIGNAL_PATTERNS: dict[FallbackSignal, tuple[re.Pattern[str], ...]] = {
    "manual_processing": (
        re.compile(
            r"\b(?:manual processing|manual process|manual fallback|fallback manually|"
            r"manual queue|manual review|hand process|process by hand|operator action|"
            r"manual intervention)\b",
            re.I,
        ),
    ),
    "support_override": (
        re.compile(
            r"\b(?:support[- ]assisted|support override|support can override|agent override|"
            r"admin override|operator override|override workflow|assisted override)\b",
            re.I,
        ),
    ),
    "offline_procedure": (
        re.compile(
            r"\b(?:offline procedure|offline runbook|spreadsheet|csv export|csv import|"
            r"paper form|out[- ]of[- ]band|phone order|email intake|manual file)\b",
            re.I,
        ),
    ),
    "break_glass_access": (
        re.compile(
            r"\b(?:break[- ]glass|emergency access|privileged access|elevated access|"
            r"temporary access|access token|production console)\b",
            re.I,
        ),
    ),
    "manual_reconciliation": (
        re.compile(
            r"\b(?:reconcile|reconciliation|ledger true[- ]up|true[- ]up|backfill after recovery|"
            r"replay after recovery|compare records|audit trail|post[- ]recovery check)\b",
            re.I,
        ),
    ),
    "customer_communication": (
        re.compile(
            r"\b(?:customer communication|customer comms|customer notice|notify customers|"
            r"status page|support macro|email customers|banner|incident update)\b",
            re.I,
        ),
    ),
    "manual_rollback": (
        re.compile(
            r"\b(?:rollback to manual|roll back to manual|manual rollback|rollback procedure|"
            r"revert to manual|switch to manual|disable automation)\b",
            re.I,
        ),
    ),
}
_PATH_SIGNAL_PATTERNS: dict[FallbackSignal, re.Pattern[str]] = {
    "manual_processing": re.compile(r"manual|operator|ops[_-]?queue|manual[_-]?fallback", re.I),
    "support_override": re.compile(r"support|override|admin", re.I),
    "offline_procedure": re.compile(r"offline|spreadsheet|csv|out[_-]?of[_-]?band", re.I),
    "break_glass_access": re.compile(r"break[_-]?glass|emergency[_-]?access|privileged", re.I),
    "manual_reconciliation": re.compile(r"reconcile|reconciliation|true[_-]?up|audit", re.I),
    "customer_communication": re.compile(r"customer|status[_-]?page|support[_-]?macro|incident[_-]?update", re.I),
    "manual_rollback": re.compile(r"manual[_-]?rollback|rollback[_-]?to[_-]?manual|disable[_-]?automation", re.I),
}
_FIELD_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "fallback_trigger": (
        re.compile(
            r"\b(?:trigger|when|if|automation fails|integration fails|provider fails|"
            r"system unavailable|outage|failure|timeout|error rate|threshold)\b",
            re.I,
        ),
    ),
    "manual_procedure": (
        re.compile(
            r"\b(?:procedure|runbook|step(?:s)?|manual process|operator action|"
            r"playbook|checklist|SOP|standard operating procedure)\b",
            re.I,
        ),
    ),
    "responsible_owner": (
        re.compile(
            r"\b(?:owner|dri|assignee|responsible|support lead|ops lead|operations owner|"
            r"on[- ]call|operator|incident commander)\b",
            re.I,
        ),
    ),
    "required_tools_or_access": (
        re.compile(
            r"\b(?:tool|tools|access|permission|role|admin console|dashboard|spreadsheet|"
            r"ticket queue|crm|sql console|break[- ]glass|credential|runbook link)\b",
            re.I,
        ),
    ),
    "reconciliation_step": (
        re.compile(
            r"\b(?:reconcile|reconciliation|true[- ]up|backfill|audit trail|compare records|"
            r"post[- ]recovery|after recovery|replay|verify totals)\b",
            re.I,
        ),
    ),
    "customer_communication": (
        re.compile(
            r"\b(?:customer communication|customer comms|notify customers|customer notice|"
            r"status page|support macro|email customers|banner|user message|incident update)\b",
            re.I,
        ),
    ),
}
_FIELD_LABELS = {
    "fallback_trigger": "Fallback trigger",
    "manual_procedure": "Manual procedure",
    "responsible_owner": "Responsible owner",
    "required_tools_or_access": "Required tools or access",
    "reconciliation_step": "Reconciliation step",
    "customer_communication": "Customer communication",
}
_RECOMMENDATIONS = {
    "responsible_owner": "Assign a responsible owner or DRI for executing the manual fallback.",
    "manual_procedure": "Document the manual fallback procedure as ordered operator steps or a runbook.",
    "required_tools_or_access": "List the tools, queues, credentials, roles, or break-glass access needed.",
    "reconciliation_step": "Define how manually processed work is reconciled after automation recovers.",
}
_OWNER_KEYS = (
    "owner",
    "owners",
    "assignee",
    "assignees",
    "dri",
    "responsible_owner",
    "owner_hint",
    "owner_hints",
    "oncall",
    "on_call",
)


@dataclass(frozen=True, slots=True)
class PlanManualFallbackOperationsRow:
    """Manual fallback operations guidance for one execution task."""

    task_id: str
    title: str
    fallback_signals: tuple[FallbackSignal, ...] = field(default_factory=tuple)
    fallback_trigger: str | None = None
    manual_procedure: str | None = None
    responsible_owner: str | None = None
    required_tools_or_access: str | None = None
    reconciliation_step: str | None = None
    customer_communication: str | None = None
    missing_fields: tuple[str, ...] = field(default_factory=tuple)
    recommendations: tuple[str, ...] = field(default_factory=tuple)
    readiness_level: FallbackReadinessLevel = "incomplete"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "fallback_signals": list(self.fallback_signals),
            "fallback_trigger": self.fallback_trigger,
            "manual_procedure": self.manual_procedure,
            "responsible_owner": self.responsible_owner,
            "required_tools_or_access": self.required_tools_or_access,
            "reconciliation_step": self.reconciliation_step,
            "customer_communication": self.customer_communication,
            "missing_fields": list(self.missing_fields),
            "recommendations": list(self.recommendations),
            "readiness_level": self.readiness_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanManualFallbackOperationsMatrix:
    """Plan-level manual fallback operations matrix."""

    plan_id: str | None = None
    rows: tuple[PlanManualFallbackOperationsRow, ...] = field(default_factory=tuple)
    fallback_task_ids: tuple[str, ...] = field(default_factory=tuple)
    non_fallback_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "fallback_task_ids": list(self.fallback_task_ids),
            "non_fallback_task_ids": list(self.non_fallback_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return matrix rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the manual fallback operations matrix as deterministic Markdown."""
        title = "# Plan Manual Fallback Operations Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Fallback task count: {self.summary.get('fallback_task_count', 0)}",
            f"- Missing field count: {self.summary.get('missing_field_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            f"- Fallback task ids: {_markdown_cell(', '.join(self.fallback_task_ids) or 'none')}",
            f"- Non-fallback task ids: {_markdown_cell(', '.join(self.non_fallback_task_ids) or 'none')}",
        ]
        if not self.rows:
            lines.extend(["", "No manual fallback operations tasks were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Trigger | Procedure | Owner | Tools or Access | Reconciliation | Customer Communication | Missing Fields | Recommendations | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.title)} | "
                f"{row.readiness_level} | "
                f"{_markdown_cell(row.fallback_trigger or 'missing')} | "
                f"{_markdown_cell(row.manual_procedure or 'missing')} | "
                f"{_markdown_cell(row.responsible_owner or 'missing')} | "
                f"{_markdown_cell(row.required_tools_or_access or 'missing')} | "
                f"{_markdown_cell(row.reconciliation_step or 'missing')} | "
                f"{_markdown_cell(row.customer_communication or 'missing')} | "
                f"{_markdown_cell(', '.join(row.missing_fields) or 'none')} | "
                f"{_markdown_cell('; '.join(row.recommendations) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_manual_fallback_operations_matrix(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> PlanManualFallbackOperationsMatrix:
    """Derive manual fallback operations rows from an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    candidates = [_row(task, index) for index, task in enumerate(tasks, start=1)]
    rows = tuple(
        sorted(
            (row for row in candidates if row is not None),
            key=lambda row: (
                _READINESS_ORDER[row.readiness_level],
                len(row.missing_fields),
                row.task_id,
                row.title.casefold(),
            ),
        )
    )
    fallback_task_ids = tuple(row.task_id for row in rows)
    fallback_set = set(fallback_task_ids)
    non_fallback_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in fallback_set
    )
    return PlanManualFallbackOperationsMatrix(
        plan_id=_optional_text(plan.get("id")),
        rows=rows,
        fallback_task_ids=fallback_task_ids,
        non_fallback_task_ids=non_fallback_task_ids,
        summary=_summary(rows, task_count=len(tasks), non_fallback_task_ids=non_fallback_task_ids),
    )


def summarize_plan_manual_fallback_operations_matrix(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> PlanManualFallbackOperationsMatrix:
    """Compatibility alias for building manual fallback operations matrices."""
    return build_plan_manual_fallback_operations_matrix(source)


def analyze_plan_manual_fallback_operations_matrix(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> PlanManualFallbackOperationsMatrix:
    """Compatibility alias for building manual fallback operations matrices."""
    return build_plan_manual_fallback_operations_matrix(source)


def plan_manual_fallback_operations_matrix_to_dict(
    matrix: PlanManualFallbackOperationsMatrix,
) -> dict[str, Any]:
    """Serialize a manual fallback operations matrix to a plain dictionary."""
    return matrix.to_dict()


plan_manual_fallback_operations_matrix_to_dict.__test__ = False


def plan_manual_fallback_operations_matrix_to_markdown(
    matrix: PlanManualFallbackOperationsMatrix,
) -> str:
    """Render a manual fallback operations matrix as Markdown."""
    return matrix.to_markdown()


plan_manual_fallback_operations_matrix_to_markdown.__test__ = False


def _row(task: Mapping[str, Any], index: int) -> PlanManualFallbackOperationsRow | None:
    signals, signal_evidence = _fallback_signals(task)
    extracted, field_evidence = _extracted_fields(task)
    if not signals:
        return None

    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    missing_fields = tuple(field for field in _FIELD_LABELS if not extracted.get(field))
    recommendations = tuple(_RECOMMENDATIONS[field] for field in missing_fields if field in _RECOMMENDATIONS)
    return PlanManualFallbackOperationsRow(
        task_id=task_id,
        title=title,
        fallback_signals=signals,
        fallback_trigger=extracted.get("fallback_trigger"),
        manual_procedure=extracted.get("manual_procedure"),
        responsible_owner=extracted.get("responsible_owner"),
        required_tools_or_access=extracted.get("required_tools_or_access"),
        reconciliation_step=extracted.get("reconciliation_step"),
        customer_communication=extracted.get("customer_communication"),
        missing_fields=missing_fields,
        recommendations=recommendations,
        readiness_level=_readiness_level(missing_fields),
        evidence=tuple(_dedupe([*signal_evidence, *field_evidence])),
    )


def _fallback_signals(task: Mapping[str, Any]) -> tuple[tuple[FallbackSignal, ...], tuple[str, ...]]:
    hits: set[FallbackSignal] = set()
    evidence: list[str] = []

    for field_name in ("files_or_modules", "files", "expected_files", "expected_file_paths"):
        for path in _strings(task.get(field_name)):
            normalized = _normalized_path(path)
            searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
            matched = False
            for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
                if pattern.search(normalized) or pattern.search(searchable):
                    hits.add(signal)
                    matched = True
            if matched:
                evidence.append(f"{field_name}: {path}")

    for source_field, text in _candidate_texts(task):
        matched = False
        for signal, patterns in _SIGNAL_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                hits.add(signal)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return (
        tuple(signal for signal in _SIGNAL_ORDER if signal in hits),
        tuple(_dedupe(evidence)),
    )


def _extracted_fields(task: Mapping[str, Any]) -> tuple[dict[str, str], tuple[str, ...]]:
    extracted: dict[str, str] = {}
    evidence: list[str] = []
    for field in (
        "fallback_trigger",
        "manual_procedure",
        "responsible_owner",
        "required_tools_or_access",
        "reconciliation_step",
        "customer_communication",
    ):
        if value := _metadata_field(task.get("metadata"), field):
            extracted[field] = value
            evidence.append(f"metadata.{field}: {value}")

    owner = _explicit_owner(task)
    if owner and "responsible_owner" not in extracted:
        extracted["responsible_owner"] = owner
        evidence.append(f"owner: {owner}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        for field, patterns in _FIELD_PATTERNS.items():
            if field in extracted:
                continue
            if any(pattern.search(text) for pattern in patterns):
                extracted[field] = _field_value(text)
                evidence.append(snippet)

    return extracted, tuple(_dedupe(evidence))


def _metadata_field(metadata: Any, field: str) -> str | None:
    if not isinstance(metadata, Mapping):
        return None
    candidate_keys = {
        field,
        field.replace("_", "-"),
        field.replace("_", " "),
    }
    aliases = {
        "fallback_trigger": ("trigger", "manual_fallback_trigger", "fallback_condition"),
        "manual_procedure": ("procedure", "runbook", "manual_runbook", "manual_steps"),
        "responsible_owner": ("owner", "dri", "assignee", "fallback_owner"),
        "required_tools_or_access": ("tools", "access", "required_access", "break_glass_access"),
        "reconciliation_step": ("reconciliation", "reconcile", "post_recovery_reconciliation"),
        "customer_communication": ("customer_comms", "communications", "customer_notice"),
    }
    candidate_keys.update(aliases.get(field, ()))
    normalized_candidates = {_normalize_key(key) for key in candidate_keys}
    for key in sorted(metadata, key=lambda item: str(item)):
        if _normalize_key(key) in normalized_candidates:
            return "; ".join(_strings(metadata[key])) or None
    return None


def _readiness_level(missing_fields: tuple[str, ...]) -> FallbackReadinessLevel:
    if not missing_fields:
        return "ready"
    if len(missing_fields) <= 2:
        return "partial"
    return "incomplete"


def _summary(
    rows: tuple[PlanManualFallbackOperationsRow, ...],
    *,
    task_count: int,
    non_fallback_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "fallback_task_count": len(rows),
        "fallback_task_ids": [row.task_id for row in rows],
        "non_fallback_task_ids": list(non_fallback_task_ids),
        "missing_field_count": sum(len(row.missing_fields) for row in rows),
        "readiness_counts": {
            level: sum(1 for row in rows if row.readiness_level == level)
            for level in _READINESS_ORDER
        },
        "missing_field_counts": {
            field: sum(1 for row in rows if field in row.missing_fields)
            for field in _FIELD_LABELS
        },
        "signal_counts": {
            signal: sum(1 for row in rows if signal in row.fallback_signals)
            for signal in _SIGNAL_ORDER
        },
    }


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "depends_on",
        "dependencies",
        "risks",
        "risk",
        "validation_commands",
        "validation_plan",
        "validation_plans",
        "tags",
        "labels",
        "notes",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for field_name in ("files_or_modules", "files", "expected_files", "expected_file_paths"):
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
            key_text = str(key).replace("_", " ").replace("-", " ")
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
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


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
        elif task := _object_payload(item):
            tasks.append(task)
    return tasks


def _object_payload(value: object) -> dict[str, Any]:
    if isinstance(value, (str, bytes, bytearray)):
        return {}
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "expected_files",
        "expected_file_paths",
        "acceptance_criteria",
        "criteria",
        "risk",
        "risks",
        "risk_level",
        "test_command",
        "validation_commands",
        "validation_plan",
        "validation_plans",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _explicit_owner(task: Mapping[str, Any]) -> str | None:
    for key in _OWNER_KEYS:
        if values := _strings(task.get(key)):
            return ", ".join(values)
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in _OWNER_KEYS:
            if values := _strings(metadata.get(key)):
                return ", ".join(values)
    return None


def _field_value(text: str) -> str:
    return _text(text)


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


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _normalize_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).casefold()).strip("_")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


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
    "FallbackReadinessLevel",
    "FallbackSignal",
    "PlanManualFallbackOperationsMatrix",
    "PlanManualFallbackOperationsRow",
    "analyze_plan_manual_fallback_operations_matrix",
    "build_plan_manual_fallback_operations_matrix",
    "plan_manual_fallback_operations_matrix_to_dict",
    "plan_manual_fallback_operations_matrix_to_markdown",
    "summarize_plan_manual_fallback_operations_matrix",
]
