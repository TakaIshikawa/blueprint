"""Plan safeguards for destructive execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DestructiveAction = Literal[
    "delete",
    "purge",
    "overwrite",
    "revoke",
    "truncate",
    "archive",
    "disable",
    "bulk_update",
]
DestructiveActionSafeguard = Literal[
    "confirmation_gate",
    "dry_run",
    "audit_log",
    "backup_restore",
    "scoped_rollout",
    "operator_permissions",
]
DestructiveActionRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[DestructiveActionRisk, int] = {"high": 0, "medium": 1, "low": 2}
_ACTION_ORDER: dict[DestructiveAction, int] = {
    "delete": 0,
    "purge": 1,
    "overwrite": 2,
    "revoke": 3,
    "truncate": 4,
    "archive": 5,
    "disable": 6,
    "bulk_update": 7,
}
_SAFEGUARD_ORDER: tuple[DestructiveActionSafeguard, ...] = (
    "confirmation_gate",
    "dry_run",
    "audit_log",
    "backup_restore",
    "scoped_rollout",
    "operator_permissions",
)
_ACTION_PATTERNS: dict[DestructiveAction, re.Pattern[str]] = {
    "delete": re.compile(r"\b(?:delete|deletes|deleted|deleting|remove|removes|removed|removing)\b", re.I),
    "purge": re.compile(r"\b(?:purge|purges|purged|purging|hard[- ]delete|erase|wipe)\b", re.I),
    "overwrite": re.compile(r"\b(?:overwrite|overwrites|overwritten|replace existing|rewrite existing|clobber)\b", re.I),
    "revoke": re.compile(r"\b(?:revoke|revokes|revoked|revoking|remove access|strip permissions?)\b", re.I),
    "truncate": re.compile(r"\b(?:truncate|truncates|truncated|truncating|drop table|drop partition)\b", re.I),
    "archive": re.compile(r"\b(?:archive|archives|archived|archiving|deactivate old|retire records?)\b", re.I),
    "disable": re.compile(r"\b(?:disable|disables|disabled|disabling|deactivate|deactivates|suspend accounts?)\b", re.I),
    "bulk_update": re.compile(
        r"\b(?:bulk update|bulk[- ]edit|mass update|batch update|update all|update existing|"
        r"backfill|data correction|migrate existing)\b",
        re.I,
    ),
}
_PATH_ACTION_PATTERNS: dict[DestructiveAction, re.Pattern[str]] = {
    "delete": re.compile(r"(?:delete|remove|destroy)", re.I),
    "purge": re.compile(r"(?:purge|hard[-_]?delete|wipe|erase)", re.I),
    "overwrite": re.compile(r"(?:overwrite|replace[-_]?existing|rewrite[-_]?existing)", re.I),
    "revoke": re.compile(r"(?:revoke|permissions?|access)", re.I),
    "truncate": re.compile(r"(?:truncate|drop[-_]?table)", re.I),
    "archive": re.compile(r"(?:archive|deactivate|retire)", re.I),
    "disable": re.compile(r"(?:disable|deactivate|suspend)", re.I),
    "bulk_update": re.compile(r"(?:bulk|batch|backfill|data[-_]?correction|migration)", re.I),
}
_DATA_SCOPE_RE = re.compile(
    r"\b(?:user|users|customer|customers|account|accounts|tenant|tenants|production|prod|live data|"
    r"records?|rows?|table|database|db|data|dataset|permissions?|roles?|access|sessions?|tokens?|"
    r"billing|payment|orders?|profiles?|assets?|files?|documents?)\b",
    re.I,
)
_BULK_SCOPE_RE = re.compile(
    r"\b(?:bulk|batch|mass|all users|all customers|all accounts|all tenants|every account|"
    r"many records|production records|large dataset|millions?|backfill|migration|update all)\b",
    re.I,
)
_PRODUCTION_RE = re.compile(
    r"\b(?:production|prod|live data|customer data|user data|all users|all customers|all accounts|"
    r"all tenants|production records|payment|billing|ledger|permissions?|access)\b",
    re.I,
)
_LOW_SIGNAL_RE = re.compile(
    r"\b(?:read[- ]only|select query|report|dashboard|analytics view|documentation|docs?|readme|"
    r"copy|typo|formatting|style-only|comment-only|cosmetic|css|storybook|mock data|fixture)\b",
    re.I,
)
_LOW_PATH_RE = re.compile(r"(?:^|/)(?:ui|views?|components?|reports?)(?:/|$)|(?:report|copy|style|css)", re.I)
_DESTRUCTIVE_TAG_RE = re.compile(r"\b(?:destructive|dangerous|irreversible|data[- ]?destruction)\b", re.I)
_REAL_ACTION_RE = re.compile(
    r"\b(?:implement|run|execute|job|migration|script|worker|change|mutate|write|delete|purge|"
    r"overwrite|revoke|truncate|archive|disable|bulk update|backfill)\b",
    re.I,
)
_SAFEGUARD_PATTERNS: dict[DestructiveActionSafeguard, re.Pattern[str]] = {
    "confirmation_gate": re.compile(
        r"\b(?:confirmation gate|confirm(?:ation)? prompt|manual approval|operator approval|"
        r"two[- ]person|sign[- ]?off|approval gate)\b",
        re.I,
    ),
    "dry_run": re.compile(r"\b(?:dry[- ]?run|preview|simulation|no[- ]op|shadow mode|plan output)\b", re.I),
    "audit_log": re.compile(r"\b(?:audit log|audit trail|auditable|change log|operator log|who did what)\b", re.I),
    "backup_restore": re.compile(
        r"\b(?:backup|restore|snapshot|point[- ]in[- ]time|pitr|rollback|roll back|recovery|revert)\b",
        re.I,
    ),
    "scoped_rollout": re.compile(
        r"\b(?:scoped rollout|canary|pilot|limited rollout|small cohort|bounded scope|tenant allowlist|"
        r"batch(?:ed|ing)?|chunk(?:ed|ing)?|rate limit|throttl)\b",
        re.I,
    ),
    "operator_permissions": re.compile(
        r"\b(?:operator permissions?|admin-only|admin only|privileged operator|least privilege|"
        r"rbac|permission check|authorized operator)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class TaskDestructiveActionSafeguardRecord:
    """Destructive-action safeguards for one execution task."""

    task_id: str
    title: str
    risk_level: DestructiveActionRisk
    destructive_actions: tuple[DestructiveAction, ...] = field(default_factory=tuple)
    safeguards: tuple[DestructiveActionSafeguard, ...] = field(default_factory=tuple)
    missing_acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_level": self.risk_level,
            "destructive_actions": list(self.destructive_actions),
            "safeguards": list(self.safeguards),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDestructiveActionSafeguardPlan:
    """Task-level safeguard plan for destructive data actions."""

    plan_id: str | None = None
    records: tuple[TaskDestructiveActionSafeguardRecord, ...] = field(default_factory=tuple)
    destructive_action_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "destructive_action_task_ids": list(self.destructive_action_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return destructive-action records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the destructive-action safeguard plan as deterministic Markdown."""
        title = "# Task Destructive Action Safeguard Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        action_counts = self.summary.get("action_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Destructive action task count: {self.summary.get('destructive_action_task_count', 0)}",
            f"- Missing acceptance criteria count: {self.summary.get('missing_acceptance_criteria_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
            "- Action counts: "
            + ", ".join(f"{action} {action_counts.get(action, 0)}" for action in _ACTION_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No destructive data-action tasks were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Actions | Safeguards | Missing Acceptance Criteria | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell('; '.join(record.destructive_actions))} | "
                f"{_markdown_cell('; '.join(record.safeguards))} | "
                f"{_markdown_cell('; '.join(record.missing_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_destructive_action_safeguard_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDestructiveActionSafeguardPlan:
    """Build safeguard recommendations for destructive execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        if (record := _record(task, index)) is not None
    ]
    records.sort(
        key=lambda record: (
            _RISK_ORDER[record.risk_level],
            record.task_id,
            record.title.casefold(),
        )
    )
    result = tuple(records)
    risk_counts = {risk: sum(1 for record in result if record.risk_level == risk) for risk in _RISK_ORDER}
    action_counts = {
        action: sum(1 for record in result if action in record.destructive_actions)
        for action in _ACTION_ORDER
    }
    return TaskDestructiveActionSafeguardPlan(
        plan_id=plan_id,
        records=result,
        destructive_action_task_ids=tuple(record.task_id for record in result),
        summary={
            "task_count": len(tasks),
            "destructive_action_task_count": len(result),
            "missing_acceptance_criteria_count": sum(len(record.missing_acceptance_criteria) for record in result),
            "risk_counts": risk_counts,
            "action_counts": action_counts,
        },
    )


def summarize_task_destructive_action_safeguards(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDestructiveActionSafeguardPlan:
    """Compatibility alias for building destructive-action safeguard plans."""
    return build_task_destructive_action_safeguard_plan(source)


def task_destructive_action_safeguard_plan_to_dict(
    result: TaskDestructiveActionSafeguardPlan,
) -> dict[str, Any]:
    """Serialize a destructive-action safeguard plan to a plain dictionary."""
    return result.to_dict()


task_destructive_action_safeguard_plan_to_dict.__test__ = False


def task_destructive_action_safeguard_plan_to_markdown(
    result: TaskDestructiveActionSafeguardPlan,
) -> str:
    """Render a destructive-action safeguard plan as Markdown."""
    return result.to_markdown()


task_destructive_action_safeguard_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    actions: tuple[DestructiveAction, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    high_risk_evidence: tuple[str, ...] = field(default_factory=tuple)
    bulk_production: bool = False


def _record(task: Mapping[str, Any], index: int) -> TaskDestructiveActionSafeguardRecord | None:
    signals = _signals(task)
    if not signals.actions:
        return None

    acceptance_context = _acceptance_context(task)
    safeguards = tuple(_SAFEGUARD_ORDER)
    missing_safeguards = tuple(
        safeguard for safeguard in safeguards if not _SAFEGUARD_PATTERNS[safeguard].search(acceptance_context)
    )
    return TaskDestructiveActionSafeguardRecord(
        task_id=_optional_text(task.get("id")) or f"task-{index}",
        title=_optional_text(task.get("title")) or f"Task {index}",
        risk_level=_risk_level(signals, missing_safeguards),
        destructive_actions=signals.actions,
        safeguards=safeguards,
        missing_acceptance_criteria=_missing_acceptance_criteria(missing_safeguards),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    actions: set[DestructiveAction] = set()
    evidence: list[str] = []
    high_risk_evidence: list[str] = []
    bulk_production = False

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized or _LOW_PATH_RE.search(normalized):
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_actions = _actions_for_path(path_text)
        if path_actions and _path_has_scope(path_text):
            actions.update(path_actions)
            evidence.append(f"files_or_modules: {path}")
            if _PRODUCTION_RE.search(path_text):
                high_risk_evidence.append(f"files_or_modules: {path}")
            if _BULK_SCOPE_RE.search(path_text) and _PRODUCTION_RE.search(path_text):
                bulk_production = True

    combined_text = " ".join(text for _, text in _candidate_texts(task))
    combined_has_scope = bool(_DATA_SCOPE_RE.search(combined_text) or _BULK_SCOPE_RE.search(combined_text))
    for source_field, text in _candidate_texts(task):
        if _is_low_signal_only(text):
            continue
        text_actions = _actions_for_text(text)
        key_actions = _actions_for_text(source_field.replace(".", " ").replace("_", " ").replace("-", " "))
        matched_actions = text_actions | key_actions
        if matched_actions and (_has_data_scope(text, source_field) or _is_tag_field(source_field) and combined_has_scope):
            actions.update(matched_actions)
            evidence.append(_evidence_snippet(source_field, text))
            if _PRODUCTION_RE.search(text):
                high_risk_evidence.append(_evidence_snippet(source_field, text))
            if ("bulk_update" in matched_actions or _BULK_SCOPE_RE.search(text)) and _PRODUCTION_RE.search(text):
                bulk_production = True
        elif _is_tag_field(source_field) and _DESTRUCTIVE_TAG_RE.search(text) and combined_has_scope:
            evidence.append(_evidence_snippet(source_field, text))

    ordered_actions = tuple(action for action in _ACTION_ORDER if action in actions)
    return _Signals(
        actions=ordered_actions,
        evidence=tuple(_dedupe(evidence)),
        high_risk_evidence=tuple(_dedupe(high_risk_evidence)),
        bulk_production=bulk_production,
    )


def _actions_for_text(text: str) -> set[DestructiveAction]:
    actions: set[DestructiveAction] = set()
    for action, pattern in _ACTION_PATTERNS.items():
        if pattern.search(text):
            actions.add(action)
    return actions


def _actions_for_path(text: str) -> set[DestructiveAction]:
    actions = _actions_for_text(text)
    for action, pattern in _PATH_ACTION_PATTERNS.items():
        if pattern.search(text):
            actions.add(action)
    return actions


def _is_tag_field(source_field: str) -> bool:
    return source_field.startswith(("tags[", "labels["))


def _has_data_scope(text: str, source_field: str) -> bool:
    field = source_field.casefold()
    return bool(
        _DATA_SCOPE_RE.search(text)
        or _BULK_SCOPE_RE.search(text)
        or any(token in field for token in ("data", "permission", "access", "production", "table", "record"))
    )


def _path_has_scope(text: str) -> bool:
    return bool(
        _DATA_SCOPE_RE.search(text)
        or _BULK_SCOPE_RE.search(text)
        or re.search(r"\b(?:db|database|data|models?|repositories?|jobs?|workers?|migrations?)\b", text, re.I)
    )


def _is_low_signal_only(text: str) -> bool:
    return bool(_LOW_SIGNAL_RE.search(text)) and not bool(_REAL_ACTION_RE.search(text))


def _risk_level(
    signals: _Signals,
    missing_safeguards: tuple[DestructiveActionSafeguard, ...],
) -> DestructiveActionRisk:
    if signals.bulk_production:
        return "high"
    if any(action in signals.actions for action in ("purge", "truncate")):
        return "high"
    if signals.high_risk_evidence and missing_safeguards:
        return "high"
    if "bulk_update" in signals.actions and missing_safeguards:
        return "high"
    if len(missing_safeguards) >= 4:
        return "high"
    if missing_safeguards:
        return "medium"
    return "low"


def _missing_acceptance_criteria(
    missing_safeguards: tuple[DestructiveActionSafeguard, ...],
) -> tuple[str, ...]:
    templates = {
        "confirmation_gate": "Acceptance criteria require an explicit confirmation gate before destructive execution.",
        "dry_run": "Acceptance criteria require a dry-run or preview that reports affected records without mutation.",
        "audit_log": "Acceptance criteria require audit logs capturing operator, scope, timestamp, and outcome.",
        "backup_restore": "Acceptance criteria require verified backup, restore, rollback, or recovery coverage.",
        "scoped_rollout": "Acceptance criteria require scoped rollout controls such as canary, batch, or tenant limits.",
        "operator_permissions": "Acceptance criteria require privileged operator permissions or approval checks.",
    }
    return tuple(templates[safeguard] for safeguard in missing_safeguards)


def _acceptance_context(task: Mapping[str, Any]) -> str:
    values: list[str] = []
    for field_name in ("acceptance_criteria", "criteria", "risks", "risk"):
        values.extend(_strings(task.get(field_name)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for source_field, text in _metadata_texts(metadata):
            normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
            if any(
                keyword in normalized
                for keyword in (
                    "acceptance",
                    "criteria",
                    "safeguard",
                    "approval",
                    "confirmation",
                    "dry",
                    "audit",
                    "backup",
                    "restore",
                    "rollback",
                    "rollout",
                    "permission",
                )
            ):
                values.append(text)
    return " ".join(values)


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
        "criteria",
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
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
        "acceptance_criteria",
        "criteria",
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
    return texts


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
                if _actions_for_text(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _actions_for_text(key_text):
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
    text = _optional_text(value)
    return [text] if text else []


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


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
    "DestructiveAction",
    "DestructiveActionRisk",
    "DestructiveActionSafeguard",
    "TaskDestructiveActionSafeguardPlan",
    "TaskDestructiveActionSafeguardRecord",
    "build_task_destructive_action_safeguard_plan",
    "summarize_task_destructive_action_safeguards",
    "task_destructive_action_safeguard_plan_to_dict",
    "task_destructive_action_safeguard_plan_to_markdown",
]
