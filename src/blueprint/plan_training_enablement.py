"""Build training and enablement checklists for briefs and execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import (
    ExecutionPlan,
    ExecutionTask,
    ImplementationBrief,
    SourceBrief,
)


EnablementAudience = Literal[
    "support",
    "sales",
    "customer_success",
    "operations",
    "admins",
    "end_users",
]
ReadinessStatus = Literal["ready", "needs_materials"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_AUDIENCE_ORDER: dict[EnablementAudience, int] = {
    "support": 0,
    "sales": 1,
    "customer_success": 2,
    "operations": 3,
    "admins": 4,
    "end_users": 5,
}
_SIGNAL_ORDER = (
    "support",
    "sales",
    "customer_success",
    "operations",
    "admins",
    "end_users",
    "onboarding",
    "workflows",
    "permissions",
    "billing",
    "reporting",
)
_MATERIAL_ORDER = (
    "support runbook",
    "support macros or FAQ",
    "sales talk track",
    "sales demo notes",
    "customer success rollout guide",
    "operations runbook",
    "admin guide",
    "permissions matrix",
    "billing FAQ",
    "reporting walkthrough",
    "workflow walkthrough",
    "onboarding guide",
    "end-user release note",
)
_MATERIAL_INDEX = {material: index for index, material in enumerate(_MATERIAL_ORDER)}

_SIGNAL_PATTERNS: dict[str, re.Pattern[str]] = {
    "support": re.compile(
        r"\b(?:support|helpdesk|customer care|ticket|tickets|escalation|"
        r"knowledge base|kb|faq|support macro|support macros|troubleshoot)\b",
        re.IGNORECASE,
    ),
    "sales": re.compile(
        r"\b(?:sales|account executive|ae|prospect|pitch|talk track|demo|"
        r"objection|pricing page|package|packaging|quote)\b",
        re.IGNORECASE,
    ),
    "customer_success": re.compile(
        r"\b(?:customer success|csm|account manager|renewal|adoption|"
        r"customer rollout|enablement|migration|implementation manager)\b",
        re.IGNORECASE,
    ),
    "operations": re.compile(
        r"\b(?:operations|ops|operator|on-call|oncall|"
        r"incident|monitoring|alert|rollback|deploy|deployment)\b",
        re.IGNORECASE,
    ),
    "admins": re.compile(
        r"\b(?:admin|admins|administrator|tenant owner|workspace owner|"
        r"permission|permissions|role|roles|rbac|settings|configuration)\b",
        re.IGNORECASE,
    ),
    "end_users": re.compile(
        r"\b(?:end user|end users|user-facing|customer-facing|users?|"
        r"notification|release note|help text)\b",
        re.IGNORECASE,
    ),
    "onboarding": re.compile(r"\b(?:onboarding|onboard|setup guide|getting started)\b", re.I),
    "workflows": re.compile(r"\b(?:workflow|workflows|journey|process|handoff)\b", re.I),
    "permissions": re.compile(
        r"\b(?:permission|permissions|role|roles|rbac|access control|entitlement)\b",
        re.I,
    ),
    "billing": re.compile(
        r"\b(?:billing|invoice|invoices|payment|payments|subscription|plan limit|"
        r"pricing|refund|chargeback|checkout)\b",
        re.I,
    ),
    "reporting": re.compile(
        r"\b(?:reporting|report|reports|dashboard|analytics|metric|metrics|export)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class PlanTrainingEnablementChecklistItem:
    """One enablement row for an audience affected by plan changes."""

    audience: EnablementAudience
    required_materials: tuple[str, ...]
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    readiness_status: ReadinessStatus = "needs_materials"
    owner_hints: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "audience": self.audience,
            "required_materials": list(self.required_materials),
            "affected_task_ids": list(self.affected_task_ids),
            "readiness_status": self.readiness_status,
            "owner_hints": list(self.owner_hints),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanTrainingEnablementChecklist:
    """Plan-level training and enablement checklist with rollup counts."""

    plan_id: str | None = None
    brief_id: str | None = None
    items: tuple[PlanTrainingEnablementChecklistItem, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "brief_id": self.brief_id,
            "items": [item.to_dict() for item in self.items],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return checklist items as plain dictionaries."""
        return [item.to_dict() for item in self.items]

    def to_markdown(self) -> str:
        """Render the checklist as deterministic Markdown."""
        title = "# Plan Training Enablement Checklist"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        elif self.brief_id:
            title = f"{title}: {self.brief_id}"

        lines = [title]
        if not self.items:
            lines.extend(["", "No training or enablement needs were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Audience | Materials | Affected Tasks | Readiness | Owner Hints | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for item in self.items:
            lines.append(
                "| "
                f"{item.audience} | "
                f"{_markdown_cell(', '.join(item.required_materials))} | "
                f"{_markdown_cell(', '.join(item.affected_task_ids) or '-')} | "
                f"{item.readiness_status} | "
                f"{_markdown_cell(', '.join(item.owner_hints) or '-')} | "
                f"{_markdown_cell('; '.join(item.evidence))} |"
            )
        return "\n".join(lines)


def build_plan_training_enablement_checklist(
    source: (
        Mapping[str, Any]
        | ImplementationBrief
        | SourceBrief
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
    execution_plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> PlanTrainingEnablementChecklist:
    """Build audience enablement rows from brief and execution-plan signals."""
    plan_id, brief_id, records = _source_records(source, execution_plan)
    builders: dict[EnablementAudience, _ItemBuilder] = {}

    for record in records:
        record_signals = _signals(record)
        for audience in _audiences(record_signals):
            builder = builders.setdefault(audience, _ItemBuilder(audience=audience))
            if task_id := _optional_text(record.get("task_id")):
                builder.task_ids.append(task_id)
            builder.materials.extend(_materials(audience, record_signals))
            builder.evidence.extend(
                evidence
                for signal in _SIGNAL_ORDER
                for evidence in record_signals.get(signal, ())
            )
            builder.owner_hints.extend(_owner_hints(record, audience, set(record_signals)))
            if _has_ready_signal(record):
                builder.ready_signals.append(True)

    items = tuple(_item(builders[audience]) for audience in _AUDIENCE_ORDER if audience in builders)
    audience_counts = {
        audience: sum(1 for item in items if item.audience == audience)
        for audience in _AUDIENCE_ORDER
    }
    return PlanTrainingEnablementChecklist(
        plan_id=plan_id,
        brief_id=brief_id,
        items=items,
        summary={
            "item_count": len(items),
            "audience_counts": audience_counts,
            "affected_task_count": len(
                {task_id for item in items for task_id in item.affected_task_ids}
            ),
            "ready_count": sum(1 for item in items if item.readiness_status == "ready"),
            "needs_materials_count": sum(
                1 for item in items if item.readiness_status == "needs_materials"
            ),
        },
    )


def plan_training_enablement_checklist_to_dict(
    result: PlanTrainingEnablementChecklist,
) -> dict[str, Any]:
    """Serialize a training enablement checklist to a plain dictionary."""
    return result.to_dict()


plan_training_enablement_checklist_to_dict.__test__ = False


def plan_training_enablement_checklist_to_markdown(
    result: PlanTrainingEnablementChecklist,
) -> str:
    """Render a training enablement checklist as Markdown."""
    return result.to_markdown()


plan_training_enablement_checklist_to_markdown.__test__ = False


def summarize_plan_training_enablement_checklist(
    source: (
        Mapping[str, Any]
        | ImplementationBrief
        | SourceBrief
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
    execution_plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> PlanTrainingEnablementChecklist:
    """Compatibility alias for building training enablement checklists."""
    return build_plan_training_enablement_checklist(source, execution_plan)


@dataclass(slots=True)
class _ItemBuilder:
    audience: EnablementAudience
    materials: list[str] = field(default_factory=list)
    task_ids: list[str] = field(default_factory=list)
    owner_hints: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    ready_signals: list[bool] = field(default_factory=list)


def _item(builder: _ItemBuilder) -> PlanTrainingEnablementChecklistItem:
    materials = tuple(
        sorted(_dedupe(builder.materials), key=lambda item: _MATERIAL_INDEX.get(item, 999))
    )
    return PlanTrainingEnablementChecklistItem(
        audience=builder.audience,
        required_materials=materials,
        affected_task_ids=tuple(_dedupe(builder.task_ids)),
        readiness_status="ready" if builder.ready_signals and materials else "needs_materials",
        owner_hints=tuple(_dedupe(builder.owner_hints)) or (_default_owner_hint(builder.audience),),
        evidence=tuple(_dedupe(builder.evidence)),
    )


def _signals(record: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    signals: dict[str, list[str]] = {}

    for path in _strings(record.get("files_or_modules") or record.get("files")):
        _add_path_signals(signals, path)

    for source_field, text in _text_fields(record):
        _add_text_signals(signals, source_field, text)

    for source_field, text in _metadata_texts(record.get("metadata")):
        if _is_owner_field(source_field):
            continue
        _add_text_signals(signals, source_field, text)

    return {signal: tuple(_dedupe(evidence)) for signal, evidence in signals.items() if evidence}


def _add_path_signals(signals: dict[str, list[str]], original: str) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    evidence = f"files_or_modules: {original}"

    if bool({"support", "helpdesk", "kb", "faq"} & parts):
        _append(signals, "support", evidence)
    if bool({"sales", "demos", "pricing"} & parts):
        _append(signals, "sales", evidence)
    if bool({"success", "customer-success", "onboarding"} & parts):
        _append(signals, "customer_success", evidence)
    if bool({"ops", "operations", "runbooks", "playbooks", "deploy"} & parts):
        _append(signals, "operations", evidence)
    if bool({"admin", "admins", "settings", "permissions", "rbac"} & parts):
        _append(signals, "admins", evidence)
    if bool({"users", "help", "release-notes"} & parts):
        _append(signals, "end_users", evidence)
    if "onboard" in name:
        _append(signals, "onboarding", evidence)
    if any(token in name for token in ("workflow", "journey")):
        _append(signals, "workflows", evidence)
    if any(token in name for token in ("permission", "rbac", "role")):
        _append(signals, "permissions", evidence)
    if any(token in name for token in ("billing", "invoice", "payment", "pricing")):
        _append(signals, "billing", evidence)
    if any(token in name for token in ("report", "dashboard", "analytics", "export")):
        _append(signals, "reporting", evidence)


def _add_text_signals(signals: dict[str, list[str]], source_field: str, text: str) -> None:
    evidence = f"{source_field}: {text}"
    for signal, pattern in _SIGNAL_PATTERNS.items():
        if pattern.search(text):
            _append(signals, signal, evidence)


def _audiences(signals: Mapping[str, tuple[str, ...]]) -> tuple[EnablementAudience, ...]:
    audiences: set[EnablementAudience] = {
        audience for audience in _AUDIENCE_ORDER if audience in signals
    }
    if "onboarding" in signals:
        audiences.update({"customer_success", "end_users"})
    if "workflows" in signals:
        audiences.update({"support", "customer_success", "operations", "end_users"})
    if "permissions" in signals:
        audiences.update({"support", "admins"})
    if "billing" in signals:
        audiences.update({"support", "sales", "customer_success", "admins"})
    if "reporting" in signals:
        audiences.update({"customer_success", "admins", "end_users"})
    return tuple(sorted(audiences, key=lambda audience: _AUDIENCE_ORDER[audience]))


def _materials(
    audience: EnablementAudience,
    signals: Mapping[str, tuple[str, ...]],
) -> tuple[str, ...]:
    materials: list[str] = []
    if audience == "support":
        materials.extend(["support runbook", "support macros or FAQ"])
    if audience == "sales":
        materials.append("sales talk track")
        if "billing" in signals:
            materials.append("billing FAQ")
        else:
            materials.append("sales demo notes")
    if audience == "customer_success":
        materials.append("customer success rollout guide")
        if "onboarding" in signals:
            materials.append("onboarding guide")
    if audience == "operations":
        materials.append("operations runbook")
    if audience == "admins":
        materials.append("admin guide")
        if "permissions" in signals:
            materials.append("permissions matrix")
        if "billing" in signals:
            materials.append("billing FAQ")
    if audience == "end_users":
        materials.append("end-user release note")
    if "reporting" in signals:
        materials.append("reporting walkthrough")
    if "workflows" in signals:
        materials.append("workflow walkthrough")
    if "onboarding" in signals and "onboarding guide" not in materials:
        materials.append("onboarding guide")
    return tuple(_dedupe(materials))


def _owner_hints(
    record: Mapping[str, Any],
    audience: EnablementAudience,
    matched_signals: set[str],
) -> tuple[str, ...]:
    hints: list[str] = []
    for field, value in _owner_fields(record):
        groups = _groups_for_owner_field(field, value, matched_signals)
        if audience in groups:
            hints.extend(_strings(value))
    if hints:
        return tuple(_dedupe(hints))
    return ()


def _default_owner_hint(audience: EnablementAudience) -> str:
    defaults: dict[EnablementAudience, str] = {
        "support": "support lead",
        "sales": "sales enablement",
        "customer_success": "customer success lead",
        "operations": "operations owner",
        "admins": "product owner",
        "end_users": "product marketing",
    }
    return defaults[audience]


def _owner_fields(value: Any, prefix: str = "") -> list[tuple[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    fields: list[tuple[str, Any]] = []
    for key in sorted(value, key=lambda item: str(item)):
        field = f"{prefix}.{key}" if prefix else str(key)
        child = value[key]
        if _is_owner_field(field):
            fields.append((field, child))
        if isinstance(child, Mapping):
            fields.extend(_owner_fields(child, field))
    return fields


def _groups_for_owner_field(
    field: str,
    value: Any,
    matched_signals: set[str],
) -> tuple[EnablementAudience, ...]:
    field_text = field.casefold()
    value_text = " ".join(_strings(value)).casefold()
    groups = [
        audience
        for audience in _AUDIENCE_ORDER
        if audience in field_text.replace(" ", "_")
        or re.search(rf"\b{re.escape(audience.replace('_', ' '))}\b", value_text)
    ]
    if "customer success" in field_text or "csm" in value_text:
        groups.append("customer_success")
    if "support" in field_text:
        groups.append("support")
    if "sales" in field_text:
        groups.append("sales")
    if "ops" in field_text or "operations" in field_text:
        groups.append("operations")
    if groups:
        return tuple(_dedupe(groups))
    matched_audiences = [audience for audience in _AUDIENCE_ORDER if audience in matched_signals]
    if matched_audiences:
        return tuple(matched_audiences)
    return tuple(_AUDIENCE_ORDER)


def _is_owner_field(field: str) -> bool:
    normalized = field.casefold()
    return any(token in normalized for token in ("owner", "assignee", "enablement_lead"))


def _has_ready_signal(record: Mapping[str, Any]) -> bool:
    ready_re = re.compile(
        r"\b(?:training ready|enablement ready|docs ready|runbook ready|"
        r"playbook ready|materials ready|published|documented)\b",
        re.IGNORECASE,
    )
    return any(ready_re.search(text) for _, text in _text_fields(record)) or any(
        ready_re.search(text) for _, text in _metadata_texts(record.get("metadata"))
    )


def _source_records(
    source: (
        Mapping[str, Any]
        | ImplementationBrief
        | SourceBrief
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
    execution_plan: Mapping[str, Any] | ExecutionPlan | None,
) -> tuple[str | None, str | None, list[dict[str, Any]]]:
    plan_id, plan_brief_id, plan_records = _plan_records(source)
    brief_id, brief_record = _brief_record(source)

    if execution_plan is not None:
        plan_id, plan_brief_id, plan_records = _plan_records(execution_plan)

    records = []
    if brief_record:
        records.append(brief_record)
    records.extend(plan_records)
    return plan_id, brief_id or plan_brief_id, records


def _plan_records(value: Any) -> tuple[str | None, str | None, list[dict[str, Any]]]:
    if isinstance(value, ExecutionTask):
        task = value.model_dump(mode="python")
        task["task_id"] = _optional_text(task.get("id"))
        return None, None, [task]
    if isinstance(value, ExecutionPlan):
        return (
            _optional_text(value.id),
            _optional_text(value.implementation_brief_id),
            [_task_record(task.model_dump(mode="python")) for task in value.tasks],
        )
    if isinstance(value, Mapping) and "tasks" in value:
        payload = _plan_payload(value)
        return (
            _optional_text(payload.get("id")),
            _optional_text(payload.get("implementation_brief_id")),
            [_task_record(task) for task in _task_payloads(payload.get("tasks"))],
        )
    if isinstance(value, Mapping) and not _is_task_like_mapping(value):
        return None, None, []
    if isinstance(value, Mapping):
        return None, None, [_task_record(dict(value))]

    try:
        iterator = iter(value)
    except TypeError:
        task = _task_like_payload(value)
        return None, None, [_task_record(task)] if task else []

    tasks = [_task_record(task) for item in iterator if (task := _task_like_payload(item))]
    return None, None, tasks


def _brief_record(value: Any) -> tuple[str | None, dict[str, Any] | None]:
    if isinstance(value, (ExecutionPlan, ExecutionTask)):
        return None, None
    if isinstance(value, Mapping) and "tasks" in value:
        return None, None
    if isinstance(value, Mapping) and _is_task_like_mapping(value):
        return None, None
    payload = _brief_payload(value)
    if not payload:
        return None, None
    brief_id = _optional_text(payload.get("id")) or _optional_text(payload.get("source_id"))
    payload["brief_id"] = brief_id
    payload["scope_name"] = (
        _optional_text(payload.get("title")) or brief_id or "implementation brief"
    )
    return brief_id, payload


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _brief_payload(brief: Any) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        value = brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    if isinstance(brief, Mapping):
        for model in (ImplementationBrief, SourceBrief):
            try:
                value = model.model_validate(brief).model_dump(mode="python")
                return dict(value) if isinstance(value, Mapping) else {}
            except (TypeError, ValueError, ValidationError):
                continue
        return dict(brief)
    return _object_payload(brief)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    return [task for item in items if (task := _task_like_payload(item))]


def _is_task_like_mapping(value: Mapping[str, Any]) -> bool:
    return any(
        key in value
        for key in (
            "acceptance_criteria",
            "depends_on",
            "files_or_modules",
            "files",
            "blocked_reason",
            "test_command",
            "suggested_engine",
            "description",
        )
    )


def _task_like_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if hasattr(value, "dict"):
        task = value.dict()
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    return _object_payload(value)


def _object_payload(value: Any) -> dict[str, Any]:
    fields = (
        "id",
        "implementation_brief_id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "target_user",
        "buyer",
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "product_surface",
        "scope",
        "non_goals",
        "assumptions",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "constraints",
        "validation_plan",
        "definition_of_done",
        "summary",
        "source_payload",
        "source_links",
        "milestone",
        "owner_type",
        "owner_role",
        "owner",
        "assignee",
        "suggested_engine",
        "risk_level",
        "test_command",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "tags",
        "labels",
        "metadata",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _task_record(task: Mapping[str, Any]) -> dict[str, Any]:
    record = dict(task)
    record["task_id"] = _optional_text(record.get("id"))
    record["scope_name"] = (
        _optional_text(record.get("title"))
        or _optional_text(record.get("id"))
        or "execution task"
    )
    return record


def _text_fields(record: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "domain",
        "target_user",
        "buyer",
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "product_surface",
        "architecture_notes",
        "data_requirements",
        "validation_plan",
        "summary",
        "milestone",
        "owner_type",
        "owner_role",
        "owner",
        "assignee",
        "suggested_engine",
        "risk_level",
        "test_command",
        "description",
        "blocked_reason",
    ):
        if text := _optional_text(record.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "risks",
        "constraints",
        "definition_of_done",
        "acceptance_criteria",
        "tags",
        "labels",
    ):
        for index, text in enumerate(_strings(record.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
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


def _append(signals: dict[str, list[str]], signal: str, evidence: str) -> None:
    signals.setdefault(signal, []).append(evidence)


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
    "EnablementAudience",
    "PlanTrainingEnablementChecklist",
    "PlanTrainingEnablementChecklistItem",
    "ReadinessStatus",
    "build_plan_training_enablement_checklist",
    "plan_training_enablement_checklist_to_dict",
    "plan_training_enablement_checklist_to_markdown",
    "summarize_plan_training_enablement_checklist",
]
