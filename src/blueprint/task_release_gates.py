"""Derive task-level release gate checklist items from implementation tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


TaskReleaseGateType = Literal[
    "schema_change",
    "migration",
    "user_visible_behavior",
    "permission_change",
    "integration_change",
    "data_export",
    "rollout_flag",
    "monitoring",
    "rollback",
    "documentation_handoff",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SCHEMA_RE = re.compile(
    r"\b(?:schema|schemas|contract|contracts|json schema|protobuf|proto|avro|"
    r"openapi|swagger|payload shape|response shape|field|fields)\b",
    re.IGNORECASE,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrations|migrate|alembic|backfill|data fix|data migration|"
    r"database change|ddl|table|column|index|existing rows)\b",
    re.IGNORECASE,
)
_USER_VISIBLE_RE = re.compile(
    r"\b(?:user[- ]visible|customer[- ]visible|ui|ux|frontend|dashboard|screen|"
    r"page|copy|message|empty state|behavior|workflow|api response|endpoint)\b",
    re.IGNORECASE,
)
_PERMISSION_RE = re.compile(
    r"\b(?:permission|permissions|authorization|authorisation|authz|rbac|role|"
    r"roles|scope|scopes|policy|policies|access control|admin only|tenant access)\b",
    re.IGNORECASE,
)
_INTEGRATION_RE = re.compile(
    r"\b(?:integration|integrations|webhook|webhooks|external api|third[- ]party|"
    r"oauth|sso|slack|github|stripe|salesforce|connector|client callback|sync job)\b",
    re.IGNORECASE,
)
_EXPORT_RE = re.compile(
    r"\b(?:export|exports|exporter|csv|xlsx|report download|download format|"
    r"data extract|feed output|serialize|serialization)\b",
    re.IGNORECASE,
)
_FLAG_RE = re.compile(
    r"\b(?:feature flag|feature flags|flag|flags|flagged rollout|rollout flag|"
    r"kill switch|launchdarkly|toggle|dark launch|gradual rollout|canary)\b",
    re.IGNORECASE,
)
_MONITORING_RE = re.compile(
    r"\b(?:monitoring|monitor|metrics|metric|alert|alerts|dashboard|logs|logging|"
    r"observability|telemetry|slo|error rate|latency)\b",
    re.IGNORECASE,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|revert|backout|restore|kill switch|undo|"
    r"previous version|recovery)\b",
    re.IGNORECASE,
)
_DOCS_RE = re.compile(
    r"\b(?:documentation|docs|readme|runbook|handoff|operator guide|support guide|"
    r"release notes|changelog|migration guide)\b",
    re.IGNORECASE,
)
_GATE_ORDER: dict[TaskReleaseGateType, int] = {
    "schema_change": 0,
    "migration": 1,
    "user_visible_behavior": 2,
    "permission_change": 3,
    "integration_change": 4,
    "data_export": 5,
    "rollout_flag": 6,
    "monitoring": 7,
    "rollback": 8,
    "documentation_handoff": 9,
}


@dataclass(frozen=True, slots=True)
class TaskReleaseGateItem:
    """A task-scoped release gate with verifier-owned evidence requirements."""

    task_id: str
    title: str
    gate: TaskReleaseGateType
    reason: str
    required_evidence: tuple[str, ...] = field(default_factory=tuple)
    verifier_role: str = "release owner"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "gate": self.gate,
            "reason": self.reason,
            "required_evidence": list(self.required_evidence),
            "verifier_role": self.verifier_role,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskReleaseGatePlan:
    """Release gate checklist items for a plan or task collection."""

    plan_id: str | None = None
    gates: tuple[TaskReleaseGateItem, ...] = field(default_factory=tuple)
    release_gated_task_ids: tuple[str, ...] = field(default_factory=tuple)
    low_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "gates": [gate.to_dict() for gate in self.gates],
            "release_gated_task_ids": list(self.release_gated_task_ids),
            "low_impact_task_ids": list(self.low_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return gate records as plain dictionaries."""
        return [gate.to_dict() for gate in self.gates]

    def to_markdown(self) -> str:
        """Render task release gates as deterministic Markdown."""
        title = "# Task Release Gates"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.gates:
            lines.extend(["", "No task-level release gates were derived."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Gate | Reason | Required Evidence | Verifier Role |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for gate in self.gates:
            lines.append(
                "| "
                f"`{_markdown_cell(gate.task_id)}` | "
                f"{gate.gate} | "
                f"{_markdown_cell(gate.reason)} | "
                f"{_markdown_cell('; '.join(gate.required_evidence))} | "
                f"{_markdown_cell(gate.verifier_role)} |"
            )
        return "\n".join(lines)


def build_task_release_gate_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskReleaseGatePlan:
    """Build task-level release gates for release-impacting implementation tasks."""
    plan_id, tasks = _source_payload(source)
    gates = [
        gate
        for index, task in enumerate(tasks, start=1)
        for gate in _gates(task, index)
    ]
    gates.sort(key=lambda item: (_GATE_ORDER[item.gate], item.task_id, item.title.casefold()))
    result = tuple(gates)
    gated_task_ids = tuple(_dedupe(item.task_id for item in result))
    all_task_ids = tuple(
        _optional_text(task.get("id")) or f"task-{index}"
        for index, task in enumerate(tasks, start=1)
    )
    gate_counts = {
        gate: sum(1 for item in result if item.gate == gate) for gate in _GATE_ORDER
    }

    return TaskReleaseGatePlan(
        plan_id=plan_id,
        gates=result,
        release_gated_task_ids=gated_task_ids,
        low_impact_task_ids=tuple(
            task_id for task_id in all_task_ids if task_id not in gated_task_ids
        ),
        summary={
            "task_count": len(tasks),
            "release_gated_task_count": len(gated_task_ids),
            "gate_count": len(result),
            "gate_counts": gate_counts,
        },
    )


def task_release_gate_plan_to_dict(result: TaskReleaseGatePlan) -> dict[str, Any]:
    """Serialize a task release gate plan to a plain dictionary."""
    return result.to_dict()


task_release_gate_plan_to_dict.__test__ = False


def task_release_gate_plan_to_markdown(result: TaskReleaseGatePlan) -> str:
    """Render a task release gate plan as Markdown."""
    return result.to_markdown()


task_release_gate_plan_to_markdown.__test__ = False


def recommend_task_release_gates(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskReleaseGatePlan:
    """Compatibility alias for building task release gates."""
    return build_task_release_gate_plan(source)


def _gates(task: Mapping[str, Any], index: int) -> tuple[TaskReleaseGateItem, ...]:
    signals = _signals(task)
    if not signals:
        return ()

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    return tuple(
        TaskReleaseGateItem(
            task_id=task_id,
            title=title,
            gate=gate,
            reason=_reason(gate),
            required_evidence=_required_evidence(gate),
            verifier_role=_verifier_role(gate),
            evidence=tuple(_dedupe(signals[gate])),
        )
        for gate in sorted(signals, key=lambda item: _GATE_ORDER[item])
    )


def _signals(task: Mapping[str, Any]) -> dict[TaskReleaseGateType, tuple[str, ...]]:
    signals: dict[TaskReleaseGateType, list[str]] = {}
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _add_path_signals(signals, path)
    for source_field, text in _task_texts(task):
        _add_text_signals(signals, source_field, text)
    for source_field, text in _metadata_texts(task.get("metadata")):
        _add_text_signals(signals, source_field, text)
    return {gate: tuple(_dedupe(evidence)) for gate, evidence in signals.items() if evidence}


def _add_path_signals(
    signals: dict[TaskReleaseGateType, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original)
    folded = normalized.casefold()
    if not folded:
        return
    path = PurePosixPath(folded)
    parts = set(path.parts)
    name = path.name
    suffix = path.suffix
    evidence = f"files_or_modules: {original}"

    if bool({"schema", "schemas", "contracts", "proto", "protobuf"} & parts) or suffix in {
        ".proto",
        ".avsc",
    } or name in {"openapi.yaml", "openapi.yml", "swagger.yaml", "swagger.yml"}:
        _append(signals, "schema_change", evidence)
    if suffix == ".sql" or bool({"alembic", "migrations", "migration", "backfills"} & parts):
        _append(signals, "migration", evidence)
    if bool({"ui", "frontend", "components", "templates", "pages", "views"} & parts):
        _append(signals, "user_visible_behavior", evidence)
    if bool({"auth", "authorization", "permissions", "rbac", "policies"} & parts):
        _append(signals, "permission_change", evidence)
    if bool({"integrations", "integration", "webhooks", "connectors", "clients"} & parts):
        _append(signals, "integration_change", evidence)
    if bool({"exporters", "exports", "reports"} & parts) or (
        "export" in name and suffix in {".py", ".ts", ".js"}
    ):
        _append(signals, "data_export", evidence)
    if "flag" in name or bool({"flags", "feature_flags", "rollout"} & parts):
        _append(signals, "rollout_flag", evidence)
    if bool({"monitoring", "metrics", "observability", "dashboards", "alerts"} & parts):
        _append(signals, "monitoring", evidence)
    if "rollback" in name or "revert" in name:
        _append(signals, "rollback", evidence)
    if bool({"docs", "documentation", "runbooks"} & parts) or name in {
        "readme.md",
        "changelog.md",
    }:
        _append(signals, "documentation_handoff", evidence)


def _add_text_signals(
    signals: dict[TaskReleaseGateType, list[str]],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    if _SCHEMA_RE.search(text):
        _append(signals, "schema_change", evidence)
    if _MIGRATION_RE.search(text):
        _append(signals, "migration", evidence)
    if _USER_VISIBLE_RE.search(text):
        _append(signals, "user_visible_behavior", evidence)
    if _PERMISSION_RE.search(text):
        _append(signals, "permission_change", evidence)
    if _INTEGRATION_RE.search(text):
        _append(signals, "integration_change", evidence)
    if _EXPORT_RE.search(text):
        _append(signals, "data_export", evidence)
    if _FLAG_RE.search(text):
        _append(signals, "rollout_flag", evidence)
    if _MONITORING_RE.search(text):
        _append(signals, "monitoring", evidence)
    if _ROLLBACK_RE.search(text):
        _append(signals, "rollback", evidence)
    if _DOCS_RE.search(text):
        _append(signals, "documentation_handoff", evidence)


def _reason(gate: TaskReleaseGateType) -> str:
    return {
        "schema_change": (
            "Schema or contract changes can break producers, consumers, or persisted "
            "payloads unless contract evidence exists."
        ),
        "migration": (
            "Migration or backfill work can change production data and needs proof "
            "that apply, verify, and recovery paths are known."
        ),
        "user_visible_behavior": (
            "User-visible behavior changes need product-facing validation before the "
            "task is shipped."
        ),
        "permission_change": (
            "Permission or access-control changes can grant, revoke, or leak access "
            "without explicit security review evidence."
        ),
        "integration_change": (
            "Integration changes can break external systems or callbacks unless "
            "partner-facing behavior is verified."
        ),
        "data_export": (
            "Data export changes can alter downstream files, reports, or serialized "
            "fields that consumers depend on."
        ),
        "rollout_flag": (
            "Flagged rollout work needs evidence that the flag, default state, and "
            "owner-controlled release path are ready."
        ),
        "monitoring": (
            "Monitoring-sensitive work needs observable health evidence before and "
            "after release."
        ),
        "rollback": (
            "Rollback-sensitive work must show that the task can be stopped or "
            "reversed if release validation fails."
        ),
        "documentation_handoff": (
            "Documentation or handoff work needs proof that operators, support, or "
            "downstream owners received the release context."
        ),
    }[gate]


def _required_evidence(gate: TaskReleaseGateType) -> tuple[str, ...]:
    return {
        "schema_change": (
            "Contract fixture or schema diff reviewed for old and new payload shapes.",
            "Consumer-impact note covering compatibility or versioning.",
        ),
        "migration": (
            "Migration apply output from an isolated or staging environment.",
            "Post-migration verification query or data-quality check.",
            "Rollback or remediation command documented.",
        ),
        "user_visible_behavior": (
            "Acceptance evidence for changed user workflow or screen.",
            "Product or QA approval of the visible behavior.",
        ),
        "permission_change": (
            "Permission matrix or policy diff reviewed.",
            "Positive and negative authorization tests.",
        ),
        "integration_change": (
            "Integration contract, sandbox, or webhook verification result.",
            "External-owner notification or compatibility note.",
        ),
        "data_export": (
            "Golden export fixture or sample output reviewed.",
            "Downstream consumer-impact note for changed columns or fields.",
        ),
        "rollout_flag": (
            "Flag name, default state, owner, and targeted environments recorded.",
            "Enable and disable procedure verified.",
        ),
        "monitoring": (
            "Metric, log, alert, or dashboard link that will prove release health.",
            "Post-release observation window and owner recorded.",
        ),
        "rollback": (
            "Rollback trigger condition recorded.",
            "Step-by-step rollback or disable procedure verified.",
        ),
        "documentation_handoff": (
            "Runbook, release note, or handoff document updated.",
            "Receiving owner or support channel acknowledged the handoff.",
        ),
    }[gate]


def _verifier_role(gate: TaskReleaseGateType) -> str:
    return {
        "schema_change": "contract owner",
        "migration": "data owner",
        "user_visible_behavior": "QA owner",
        "permission_change": "security owner",
        "integration_change": "integration owner",
        "data_export": "data consumer owner",
        "rollout_flag": "release owner",
        "monitoring": "on-call owner",
        "rollback": "release owner",
        "documentation_handoff": "support owner",
    }[gate]


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
        return dict(plan) if isinstance(plan, Mapping) else {}


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


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "test_command",
        "risk_level",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    for index, text in enumerate(_strings(task.get("tags"))):
        texts.append((f"tags[{index}]", text))
    for index, text in enumerate(_strings(task.get("labels"))):
        texts.append((f"labels[{index}]", text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
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


def _append(
    signals: dict[TaskReleaseGateType, list[str]],
    gate: TaskReleaseGateType,
    evidence: str,
) -> None:
    signals.setdefault(gate, []).append(evidence)


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
    "TaskReleaseGateItem",
    "TaskReleaseGatePlan",
    "TaskReleaseGateType",
    "build_task_release_gate_plan",
    "recommend_task_release_gates",
    "task_release_gate_plan_to_dict",
    "task_release_gate_plan_to_markdown",
]
