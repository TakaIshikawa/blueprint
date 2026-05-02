"""Identify stakeholder notification needs across execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


StakeholderNotificationAudience = Literal[
    "end_users",
    "admins",
    "billing_contacts",
    "customer_success",
    "support_agents",
    "operations",
    "data_governance",
    "security",
    "product",
    "engineering",
]
StakeholderNotificationTrigger = Literal[
    "customer_impact",
    "admin_change",
    "billing_change",
    "data_change",
    "downtime",
    "migration",
    "permission_change",
    "ui_behavior_change",
]
StakeholderNotificationInput = Literal[
    "audience_owner",
    "message_channel",
    "timing",
    "support_context",
    "rollback_message",
    "success_confirmation",
]
StakeholderNotificationRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[StakeholderNotificationRisk, int] = {"high": 0, "medium": 1, "low": 2}
_AUDIENCE_ORDER: tuple[StakeholderNotificationAudience, ...] = (
    "billing_contacts",
    "admins",
    "end_users",
    "customer_success",
    "support_agents",
    "operations",
    "data_governance",
    "security",
    "product",
    "engineering",
)
_TRIGGER_ORDER: tuple[StakeholderNotificationTrigger, ...] = (
    "downtime",
    "billing_change",
    "permission_change",
    "data_change",
    "migration",
    "admin_change",
    "customer_impact",
    "ui_behavior_change",
)
_INPUT_ORDER: tuple[StakeholderNotificationInput, ...] = (
    "audience_owner",
    "message_channel",
    "timing",
    "support_context",
    "rollback_message",
    "success_confirmation",
)

_CUSTOMER_IMPACT_RE = re.compile(
    r"\b(?:customer[- ]impact|customer[- ]visible|customer[- ]facing|user[- ]visible|"
    r"user[- ]facing|end users?|customers?|external users?|availability|degradation|"
    r"outage|service interruption|announcement|notify customers?|release notes?)\b",
    re.I,
)
_ADMIN_RE = re.compile(
    r"\b(?:admin|administrator|tenant owner|workspace owner|org owner|operator|settings|"
    r"configuration|management console|control plane|role|roles|rbac|access control)\b",
    re.I,
)
_BILLING_RE = re.compile(
    r"\b(?:billing|invoice|invoices|payment|payments|subscription|subscriptions|pricing|"
    r"plan change|paid plan|account plan|seat|seats|trial|renewal|refund|charge|checkout|"
    r"tax|credits?|entitlement|entitlements?)\b",
    re.I,
)
_DATA_RE = re.compile(
    r"\b(?:data change|data changes|delete|deletion|deleted|retention|export|import|"
    r"backfill|bulk update|reconcile|reconciliation|schema change|data model|customer data|"
    r"pii|personal data|data|record changes?|field changes?)\b",
    re.I,
)
_DOWNTIME_RE = re.compile(
    r"\b(?:downtime|outage|maintenance window|service interruption|unavailable|offline|"
    r"degradation|degraded|brownout|read[- ]only|freeze|pause writes?|cutover window)\b",
    re.I,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrate|migrating|cutover|backfill|existing accounts?|existing customers?|"
    r"upgrade path|legacy data|data migration|schema migration)\b",
    re.I,
)
_PERMISSION_RE = re.compile(
    r"\b(?:permission|permissions|role|roles|rbac|access control|scope|scopes|privilege|"
    r"privileges|authorization|admin access|entitlement|entitlements?)\b",
    re.I,
)
_UI_RE = re.compile(
    r"\b(?:ui|ux|interface|screen|page|form|button|modal|dialog|banner|navigation|"
    r"dashboard|workflow|checkout|onboarding|copy|message|email behavior|notification behavior)\b",
    re.I,
)
_PATH_RE = re.compile(
    r"(?:^|/)(?:app|web|ui|frontend|pages?|routes?|admin|billing|payments?|"
    r"migrations?|data|models?|permissions?|rbac|auth|notifications?|email)(?:/|$)",
    re.I,
)

_INPUT_PATTERNS: dict[StakeholderNotificationInput, re.Pattern[str]] = {
    "audience_owner": re.compile(
        r"\b(?:audience owner|stakeholder owner|notification owner|comms owner|dri|"
        r"responsible owner|customer owner)\b",
        re.I,
    ),
    "message_channel": re.compile(
        r"\b(?:message channel|communication channel|comms channel|slack|email|in[- ]app|"
        r"status page|help center|release notes?|changelog|support portal)\b",
        re.I,
    ),
    "timing": re.compile(
        r"\b(?:timing|send time|notification time|before rollout|during rollout|after rollout|"
        r"maintenance window|launch window|T[-+]\d+|advance notice|notice period)\b",
        re.I,
    ),
    "support_context": re.compile(
        r"\b(?:support context|support notes?|support brief|support macro|known issue|"
        r"helpdesk|ticket context|customer success context)\b",
        re.I,
    ),
    "rollback_message": re.compile(
        r"\b(?:rollback message|rollback comms?|rollback communication|roll back message|"
        r"fallback message|revert notice|incident update)\b",
        re.I,
    ),
    "success_confirmation": re.compile(
        r"\b(?:success confirmation|success message|all clear|completion notice|"
        r"resolved notice|post[- ]rollout confirmation|launch complete|validation confirmation)\b",
        re.I,
    ),
}
_MISSING_INPUT_TEXT: dict[StakeholderNotificationInput, str] = {
    "audience_owner": "Identify the audience owner responsible for stakeholder notification.",
    "message_channel": "Specify the message channel for the notification.",
    "timing": "Specify notification timing before, during, or after rollout.",
    "support_context": "Provide support context for stakeholder questions or tickets.",
    "rollback_message": "Prepare the rollback message if rollout needs to be reversed.",
    "success_confirmation": "Define how stakeholders receive success confirmation.",
}


@dataclass(frozen=True, slots=True)
class PlanStakeholderNotificationRecord:
    """Stakeholder notification coverage for one notification-worthy task."""

    task_id: str
    title: str
    notification_audiences: tuple[StakeholderNotificationAudience, ...] = field(
        default_factory=tuple
    )
    notification_triggers: tuple[StakeholderNotificationTrigger, ...] = field(default_factory=tuple)
    missing_inputs: tuple[str, ...] = field(default_factory=tuple)
    risk_level: StakeholderNotificationRisk = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "notification_audiences": list(self.notification_audiences),
            "notification_triggers": list(self.notification_triggers),
            "missing_inputs": list(self.missing_inputs),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanStakeholderNotificationMatrix:
    """Plan-level stakeholder notification matrix."""

    plan_id: str | None = None
    records: tuple[PlanStakeholderNotificationRecord, ...] = field(default_factory=tuple)
    notification_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "notification_task_ids": list(self.notification_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return notification records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the stakeholder notification matrix as deterministic Markdown."""
        title = "# Plan Stakeholder Notification Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Notification-worthy task count: {self.summary.get('notification_task_count', 0)}",
            f"- Missing input count: {self.summary.get('missing_input_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No stakeholder notification needs were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Audiences | Triggers | Missing Inputs | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell('; '.join(record.notification_audiences) or 'none')} | "
                f"{_markdown_cell('; '.join(record.notification_triggers) or 'none')} | "
                f"{_markdown_cell('; '.join(record.missing_inputs) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_stakeholder_notification_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanStakeholderNotificationMatrix:
    """Build a plan-level matrix of stakeholder notification needs."""
    plan_id, plan_context, tasks = _source_payload(source)
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        if (record := _record(task, index, plan_context)) is not None
    ]
    records.sort(
        key=lambda record: (
            _RISK_ORDER[record.risk_level],
            record.task_id,
            record.title.casefold(),
            record.notification_audiences,
            record.notification_triggers,
        )
    )
    result = tuple(records)
    return PlanStakeholderNotificationMatrix(
        plan_id=plan_id,
        records=result,
        notification_task_ids=tuple(record.task_id for record in result),
        summary={
            "task_count": len(tasks),
            "notification_task_count": len(result),
            "missing_input_count": sum(len(record.missing_inputs) for record in result),
            "risk_counts": {
                risk: sum(1 for record in result if record.risk_level == risk)
                for risk in _RISK_ORDER
            },
            "audience_counts": {
                audience: sum(1 for record in result if audience in record.notification_audiences)
                for audience in _AUDIENCE_ORDER
            },
            "trigger_counts": {
                trigger: sum(1 for record in result if trigger in record.notification_triggers)
                for trigger in _TRIGGER_ORDER
            },
        },
    )


def summarize_plan_stakeholder_notification_matrix(
    source_or_matrix: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanStakeholderNotificationMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanStakeholderNotificationMatrix:
    """Build the notification matrix, accepting an existing matrix unchanged."""
    if isinstance(source_or_matrix, PlanStakeholderNotificationMatrix):
        return source_or_matrix
    return build_plan_stakeholder_notification_matrix(source_or_matrix)


def plan_stakeholder_notification_matrix_to_dict(
    matrix: PlanStakeholderNotificationMatrix,
) -> dict[str, Any]:
    """Serialize a stakeholder notification matrix to a plain dictionary."""
    return matrix.to_dict()


plan_stakeholder_notification_matrix_to_dict.__test__ = False


def plan_stakeholder_notification_matrix_to_markdown(
    matrix: PlanStakeholderNotificationMatrix,
) -> str:
    """Render a stakeholder notification matrix as Markdown."""
    return matrix.to_markdown()


plan_stakeholder_notification_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    audiences: tuple[StakeholderNotificationAudience, ...] = field(default_factory=tuple)
    triggers: tuple[StakeholderNotificationTrigger, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)


def _record(
    task: Mapping[str, Any],
    index: int,
    plan_context: tuple[tuple[str, str], ...],
) -> PlanStakeholderNotificationRecord | None:
    signals = _signals(task)
    if not signals.triggers:
        return None

    input_context = _input_context(task, plan_context)
    present_inputs = {
        input_name
        for input_name, pattern in _INPUT_PATTERNS.items()
        if pattern.search(input_context)
    }
    missing_inputs = tuple(
        _MISSING_INPUT_TEXT[input_name]
        for input_name in _INPUT_ORDER
        if input_name not in present_inputs
    )
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    return PlanStakeholderNotificationRecord(
        task_id=task_id,
        title=title,
        notification_audiences=signals.audiences,
        notification_triggers=signals.triggers,
        missing_inputs=missing_inputs,
        risk_level=_risk_level(signals.triggers, missing_inputs),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    audiences: list[StakeholderNotificationAudience] = []
    triggers: list[StakeholderNotificationTrigger] = []
    evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        if _PATH_RE.search(normalized) or _any_signal(path_text):
            evidence.append(f"files_or_modules: {path}")
        _apply_text_signals(path_text, audiences, triggers)

    for source_field, text in _candidate_texts(task):
        before = len(triggers)
        _apply_text_signals(text, audiences, triggers)
        if len(triggers) > before or _any_signal(text):
            evidence.append(_evidence_snippet(source_field, text))

    ordered_triggers = _ordered_dedupe(triggers, _TRIGGER_ORDER)
    ordered_audiences = _ordered_dedupe(
        audiences or _audiences_for_triggers(ordered_triggers),
        _AUDIENCE_ORDER,
    )
    return _Signals(
        audiences=tuple(ordered_audiences),
        triggers=tuple(ordered_triggers),
        evidence=tuple(_dedupe(evidence)),
    )


def _apply_text_signals(
    text: str,
    audiences: list[StakeholderNotificationAudience],
    triggers: list[StakeholderNotificationTrigger],
) -> None:
    if _DOWNTIME_RE.search(text):
        audiences.extend(["end_users", "customer_success", "support_agents", "operations"])
        triggers.append("downtime")
    if _BILLING_RE.search(text):
        audiences.extend(["billing_contacts", "admins", "customer_success", "support_agents"])
        triggers.append("billing_change")
    if _PERMISSION_RE.search(text):
        audiences.extend(["admins", "support_agents", "security"])
        triggers.append("permission_change")
    if _DATA_RE.search(text):
        audiences.extend(
            ["admins", "customer_success", "support_agents", "operations", "data_governance"]
        )
        triggers.append("data_change")
    if _MIGRATION_RE.search(text):
        audiences.extend(
            ["end_users", "admins", "customer_success", "support_agents", "operations"]
        )
        triggers.append("migration")
    if _ADMIN_RE.search(text):
        audiences.extend(["admins", "support_agents", "operations"])
        triggers.append("admin_change")
    if _CUSTOMER_IMPACT_RE.search(text):
        audiences.extend(["end_users", "customer_success", "support_agents", "product"])
        triggers.append("customer_impact")
    if _UI_RE.search(text):
        audiences.extend(["end_users", "admins", "customer_success", "support_agents", "product"])
        triggers.append("ui_behavior_change")
    if re.search(r"\b(?:api|sdk|webhook|endpoint|integration|developer)\b", text, re.I):
        audiences.append("engineering")


def _any_signal(text: str) -> bool:
    return any(
        pattern.search(text)
        for pattern in (
            _CUSTOMER_IMPACT_RE,
            _ADMIN_RE,
            _BILLING_RE,
            _DATA_RE,
            _DOWNTIME_RE,
            _MIGRATION_RE,
            _PERMISSION_RE,
            _UI_RE,
        )
    )


def _risk_level(
    triggers: tuple[StakeholderNotificationTrigger, ...],
    missing_inputs: tuple[str, ...],
) -> StakeholderNotificationRisk:
    escalated = {"downtime", "billing_change", "permission_change", "data_change", "migration"}
    if any(trigger in escalated for trigger in triggers):
        return "high" if missing_inputs else "medium"
    if len(missing_inputs) >= 3 or len(triggers) >= 3:
        return "medium"
    return "low" if not missing_inputs else "medium"


def _audiences_for_triggers(
    triggers: tuple[StakeholderNotificationTrigger, ...],
) -> list[StakeholderNotificationAudience]:
    audiences: list[StakeholderNotificationAudience] = []
    for trigger in triggers:
        if trigger == "billing_change":
            audiences.extend(["billing_contacts", "admins", "customer_success", "support_agents"])
        elif trigger == "permission_change":
            audiences.extend(["admins", "support_agents", "security"])
        elif trigger == "data_change":
            audiences.extend(["admins", "support_agents", "operations", "data_governance"])
        elif trigger == "downtime":
            audiences.extend(["end_users", "customer_success", "support_agents", "operations"])
        elif trigger == "admin_change":
            audiences.extend(["admins", "support_agents"])
        else:
            audiences.extend(["end_users", "customer_success", "support_agents"])
    return audiences


def _input_context(task: Mapping[str, Any], plan_context: tuple[tuple[str, str], ...]) -> str:
    values: list[str] = []
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "risk",
        "validation_commands",
        "test_command",
        "notes",
    ):
        values.extend(_strings(task.get(field_name)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for source_field, text in _metadata_texts(metadata):
            normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
            if any(input_name in normalized for input_name in _INPUT_ORDER):
                values.append(f"{source_field}: {text}")
            elif any(
                keyword in normalized
                for keyword in (
                    "notification",
                    "comms",
                    "communication",
                    "support",
                    "rollback",
                    "success",
                    "channel",
                    "timing",
                )
            ):
                values.append(f"{source_field}: {text}")
    values.extend(
        text for source_field, text in plan_context if _context_field_is_input(source_field)
    )
    return " ".join(values)


def _context_field_is_input(source_field: str) -> bool:
    normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
    return any(input_name in normalized for input_name in _INPUT_ORDER) or any(
        keyword in normalized
        for keyword in (
            "notification",
            "comms",
            "communication",
            "support",
            "rollback",
            "success",
            "channel",
            "timing",
        )
    )


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, tuple[tuple[str, str], ...], list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return (
            _optional_text(payload.get("id")),
            tuple(_plan_context(payload)),
            [task.model_dump(mode="python") for task in source.tasks],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return (
                _optional_text(payload.get("id")),
                tuple(_plan_context(payload)),
                _task_payloads(payload.get("tasks")),
            )
        return None, (), [dict(source)]
    if _looks_like_task(source):
        return None, (), [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return (
            _optional_text(payload.get("id")),
            tuple(_plan_context(payload)),
            _task_payloads(payload.get("tasks")),
        )

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, (), []

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
    return None, (), tasks


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


def _plan_context(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("test_strategy", "handoff_prompt", "generation_prompt"):
        if text := _optional_text(plan.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("milestones", "metadata", "implementation_brief", "brief"):
        for source_field, text in _metadata_texts(plan.get(field_name), prefix=field_name):
            texts.append((source_field, text))
    return texts


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
        "criteria",
        "definition_of_done",
        "risks",
        "risk",
        "risk_level",
        "test_command",
        "validation_commands",
        "status",
        "metadata",
        "tags",
        "labels",
        "notes",
        "blocked_reason",
        "tasks",
        "milestones",
        "implementation_brief",
        "brief",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _candidate_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
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
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "depends_on",
        "tags",
        "labels",
        "notes",
        "validation_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return tuple(texts)


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _any_signal(key_text):
                texts.append((field, str(key)))
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
    text = _clean_text(str(value))
    return [text] if text else []


def _ordered_dedupe(items: Iterable[_T], order: tuple[_T, ...]) -> list[_T]:
    seen = set(items)
    return [item for item in order if item in seen]


def _dedupe(items: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return tuple(result)


def _evidence_snippet(source_field: str, text: str) -> str:
    return f"{source_field}: {_snippet(text)}"


def _snippet(text: str, limit: int = 180) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "..."


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/").strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip()


def _markdown_cell(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ").strip()


__all__ = [
    "PlanStakeholderNotificationMatrix",
    "PlanStakeholderNotificationRecord",
    "StakeholderNotificationAudience",
    "StakeholderNotificationInput",
    "StakeholderNotificationRisk",
    "StakeholderNotificationTrigger",
    "build_plan_stakeholder_notification_matrix",
    "plan_stakeholder_notification_matrix_to_dict",
    "plan_stakeholder_notification_matrix_to_markdown",
    "summarize_plan_stakeholder_notification_matrix",
]
