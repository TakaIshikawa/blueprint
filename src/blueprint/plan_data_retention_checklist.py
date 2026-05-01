"""Build data retention checklists for implementation briefs and execution plans."""

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


RetentionSignal = Literal[
    "logs",
    "analytics",
    "exports",
    "uploads",
    "pii",
    "audit_records",
    "backups",
    "databases",
    "caches",
    "generated_files",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_LOGS_RE = re.compile(
    r"\b(?:log|logs|logging|trace|traces|telemetry|observability|error report|"
    r"crash report)\b",
    re.IGNORECASE,
)
_ANALYTICS_RE = re.compile(
    r"\b(?:analytics|tracking|track event|tracked event|funnel|attribution|"
    r"usage metrics|product metrics|behavioral data)\b",
    re.IGNORECASE,
)
_EXPORTS_RE = re.compile(
    r"\b(?:export|exports|csv|spreadsheet|download|report artifact|portable data|"
    r"data portability)\b",
    re.IGNORECASE,
)
_UPLOADS_RE = re.compile(
    r"\b(?:upload|uploads|uploaded file|attachment|attachments|import file|"
    r"file ingest|media ingest)\b",
    re.IGNORECASE,
)
_PII_RE = re.compile(
    r"\b(?:pii|personal data|personally identifiable|email addresses?|phone numbers?|"
    r"mailing addresses?|ssn|social security|date of birth|birthdate|full name|"
    r"customer data|customer files?|customer records?|customer receipts?|"
    r"user data|profile data)\b",
    re.IGNORECASE,
)
_AUDIT_RE = re.compile(
    r"\b(?:audit record|audit records|audit log|audit trail|audit event|"
    r"security event|compliance event)\b",
    re.IGNORECASE,
)
_BACKUPS_RE = re.compile(
    r"\b(?:backup|backups|snapshot|snapshots|restore point|restore points|"
    r"archive copy|disaster recovery)\b",
    re.IGNORECASE,
)
_DATABASES_RE = re.compile(
    r"\b(?:database|databases|db|table|tables|schema|migration|persistent store|"
    r"postgres|mysql|sqlite|dynamodb|warehouse)\b",
    re.IGNORECASE,
)
_CACHES_RE = re.compile(
    r"\b(?:cache|caches|cached|redis|memcached|cdn cache|ttl|time to live|"
    r"expiration|expire)\b",
    re.IGNORECASE,
)
_GENERATED_FILES_RE = re.compile(
    r"\b(?:generated file|generated files|artifact|artifacts|report file|"
    r"temporary file|temp file|fixture|fixtures)\b",
    re.IGNORECASE,
)

_SIGNAL_ORDER: dict[RetentionSignal, int] = {
    "pii": 0,
    "logs": 1,
    "audit_records": 2,
    "analytics": 3,
    "exports": 4,
    "uploads": 5,
    "backups": 6,
    "databases": 7,
    "caches": 8,
    "generated_files": 9,
}


@dataclass(frozen=True, slots=True)
class PlanDataRetentionChecklistItem:
    """One retention checklist item inferred from a brief or execution task."""

    scope: str
    data_categories: tuple[RetentionSignal, ...]
    retention_question: str
    deletion_verification: str
    owner_role: str
    access_control: str
    auditability: str
    backup_handling: str
    documentation_update: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    task_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "scope": self.scope,
            "task_id": self.task_id,
            "data_categories": list(self.data_categories),
            "retention_question": self.retention_question,
            "deletion_verification": self.deletion_verification,
            "owner_role": self.owner_role,
            "access_control": self.access_control,
            "auditability": self.auditability,
            "backup_handling": self.backup_handling,
            "documentation_update": self.documentation_update,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanDataRetentionChecklist:
    """Plan-level data retention checklist and rollup counts."""

    plan_id: str | None = None
    brief_id: str | None = None
    items: tuple[PlanDataRetentionChecklistItem, ...] = field(default_factory=tuple)
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
        title = "# Plan Data Retention Checklist"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        elif self.brief_id:
            title = f"{title}: {self.brief_id}"

        lines = [title]
        if not self.items:
            lines.extend(["", "No data retention obligations were found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Scope | Categories | Retention Question | Deletion Verification | Owner Role | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for item in self.items:
            lines.append(
                "| "
                f"{_markdown_cell(item.scope)} | "
                f"{_markdown_cell(', '.join(item.data_categories))} | "
                f"{_markdown_cell(item.retention_question)} | "
                f"{_markdown_cell(item.deletion_verification)} | "
                f"{_markdown_cell(item.owner_role)} | "
                f"{_markdown_cell('; '.join(item.evidence))} |"
            )
        return "\n".join(lines)


def build_plan_data_retention_checklist(
    source: (
        Mapping[str, Any]
        | ImplementationBrief
        | SourceBrief
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
    execution_plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> PlanDataRetentionChecklist:
    """Build retention checklist items from brief and plan lifecycle signals."""
    plan_id, brief_id, records = _source_records(source, execution_plan)
    items = tuple(
        sorted(
            (
                item
                for index, record in enumerate(records, start=1)
                if (item := _checklist_item(record, index)) is not None
            ),
            key=lambda item: (
                item.task_id or "",
                min(_SIGNAL_ORDER[category] for category in item.data_categories),
                item.scope,
            ),
        )
    )
    category_counts = {
        category: sum(1 for item in items if category in item.data_categories)
        for category in _SIGNAL_ORDER
    }
    owner_role_counts: dict[str, int] = {}
    for item in items:
        owner_role_counts[item.owner_role] = owner_role_counts.get(item.owner_role, 0) + 1

    return PlanDataRetentionChecklist(
        plan_id=plan_id,
        brief_id=brief_id,
        items=items,
        summary={
            "item_count": len(items),
            "scope_count": len({item.scope for item in items}),
            "task_count": len({item.task_id for item in items if item.task_id}),
            "category_counts": category_counts,
            "owner_role_counts": dict(sorted(owner_role_counts.items())),
        },
    )


def plan_data_retention_checklist_to_dict(
    result: PlanDataRetentionChecklist,
) -> dict[str, Any]:
    """Serialize a data retention checklist to a plain dictionary."""
    return result.to_dict()


plan_data_retention_checklist_to_dict.__test__ = False


def plan_data_retention_checklist_to_markdown(
    result: PlanDataRetentionChecklist,
) -> str:
    """Render a data retention checklist as Markdown."""
    return result.to_markdown()


plan_data_retention_checklist_to_markdown.__test__ = False


def summarize_plan_data_retention_checklist(
    source: (
        Mapping[str, Any]
        | ImplementationBrief
        | SourceBrief
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
    execution_plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> PlanDataRetentionChecklist:
    """Compatibility alias for building plan data retention checklists."""
    return build_plan_data_retention_checklist(source, execution_plan)


def _checklist_item(
    record: Mapping[str, Any],
    index: int,
) -> PlanDataRetentionChecklistItem | None:
    task_id = _optional_text(record.get("task_id"))
    scope = _scope(record, index)
    signals = _signals(record)
    if not signals:
        return None

    categories = tuple(sorted(signals, key=lambda category: _SIGNAL_ORDER[category]))
    evidence = tuple(_dedupe(item for category in categories for item in signals[category]))
    owner_role = _owner_role(record, categories)

    return PlanDataRetentionChecklistItem(
        scope=scope,
        task_id=task_id,
        data_categories=categories,
        retention_question=_retention_question(scope, categories),
        deletion_verification=_deletion_verification(categories),
        owner_role=owner_role,
        access_control=_access_control(categories),
        auditability=_auditability(categories),
        backup_handling=_backup_handling(categories),
        documentation_update=_documentation_update(categories),
        evidence=evidence,
    )


def _signals(record: Mapping[str, Any]) -> dict[RetentionSignal, tuple[str, ...]]:
    signals: dict[RetentionSignal, list[str]] = {}

    for path in _strings(record.get("files_or_modules") or record.get("files")):
        _add_path_signals(signals, path)

    for source_field, text in _text_fields(record):
        _add_text_signals(signals, source_field, text)

    for source_field, text in _metadata_texts(record.get("metadata")):
        _add_text_signals(signals, source_field, text)

    return {
        category: tuple(_dedupe(evidence))
        for category, evidence in signals.items()
        if evidence
    }


def _add_path_signals(
    signals: dict[RetentionSignal, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    evidence = f"files_or_modules: {original}"

    if bool({"logs", "logging", "telemetry", "traces"} & parts) or "log" in name:
        _append(signals, "logs", evidence)
    if bool({"analytics", "metrics", "tracking", "events"} & parts):
        _append(signals, "analytics", evidence)
    if bool({"exports", "exporters", "downloads", "reports"} & parts) or "export" in name:
        _append(signals, "exports", evidence)
    if bool({"uploads", "attachments", "imports", "media"} & parts) or "upload" in name:
        _append(signals, "uploads", evidence)
    if bool({"users", "customers", "contacts", "profiles", "identity", "pii"} & parts):
        _append(signals, "pii", evidence)
    if bool({"audit", "audits", "compliance"} & parts) or "audit" in name:
        _append(signals, "audit_records", evidence)
    if bool({"backups", "backup", "snapshots", "archives"} & parts) or "backup" in name:
        _append(signals, "backups", evidence)
    if bool({"db", "database", "databases", "migrations", "models", "schema"} & parts):
        _append(signals, "databases", evidence)
    if bool({"cache", "caches", "redis", "memcached"} & parts) or "cache" in name:
        _append(signals, "caches", evidence)
    if bool({"generated", "artifacts", "fixtures", "tmp", "temp"} & parts) or any(
        token in name for token in ("generated", "artifact", "fixture", "tmp")
    ):
        _append(signals, "generated_files", evidence)


def _add_text_signals(
    signals: dict[RetentionSignal, list[str]],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    if _LOGS_RE.search(text):
        _append(signals, "logs", evidence)
    if _ANALYTICS_RE.search(text):
        _append(signals, "analytics", evidence)
    if _EXPORTS_RE.search(text):
        _append(signals, "exports", evidence)
    if _UPLOADS_RE.search(text):
        _append(signals, "uploads", evidence)
    if _PII_RE.search(text):
        _append(signals, "pii", evidence)
    if _AUDIT_RE.search(text):
        _append(signals, "audit_records", evidence)
    if _BACKUPS_RE.search(text):
        _append(signals, "backups", evidence)
    if _DATABASES_RE.search(text):
        _append(signals, "databases", evidence)
    if _CACHES_RE.search(text):
        _append(signals, "caches", evidence)
    if _GENERATED_FILES_RE.search(text):
        _append(signals, "generated_files", evidence)


def _retention_question(scope: str, categories: tuple[RetentionSignal, ...]) -> str:
    rendered = _category_label(categories)
    return f"What retention period and purge trigger apply to {rendered} in {scope}?"


def _deletion_verification(categories: tuple[RetentionSignal, ...]) -> str:
    if "backups" in categories:
        return "Verify deletion behavior across primary stores and backup restore windows."
    if "caches" in categories:
        return "Verify cache invalidation or TTL expiry removes retained data after deletion."
    if "exports" in categories or "generated_files" in categories:
        return "Verify generated artifacts are removed or expired after the retention period."
    if "logs" in categories or "audit_records" in categories:
        return "Verify deletion policy preserves required audit history while removing disallowed fields."
    return "Verify deleted records cannot be restored by jobs, syncs, caches, or derived artifacts."


def _owner_role(
    record: Mapping[str, Any],
    categories: tuple[RetentionSignal, ...],
) -> str:
    metadata = record.get("metadata")
    explicit = (
        _optional_text(record.get("owner_role"))
        or _optional_text(record.get("owner_type"))
        or _optional_text(record.get("owner"))
    )
    if not explicit and isinstance(metadata, Mapping):
        explicit = (
            _optional_text(metadata.get("retention_owner"))
            or _optional_text(metadata.get("owner_role"))
            or _optional_text(metadata.get("owner_type"))
            or _optional_text(metadata.get("owner"))
            or _optional_text(metadata.get("assignee"))
        )
    if explicit:
        return explicit
    if "pii" in categories:
        return "privacy owner"
    if "backups" in categories or "databases" in categories:
        return "data platform owner"
    if "logs" in categories or "audit_records" in categories:
        return "security owner"
    return "feature owner"


def _access_control(categories: tuple[RetentionSignal, ...]) -> str:
    if "pii" in categories:
        return "Limit access to roles approved for personal data handling."
    if "audit_records" in categories or "logs" in categories:
        return "Limit retained operational records to support, security, and compliance roles."
    if "exports" in categories or "uploads" in categories:
        return "Gate file access with authorization checks and expiring access where practical."
    return "Confirm only operationally required roles can access retained data."


def _auditability(categories: tuple[RetentionSignal, ...]) -> str:
    if "audit_records" in categories:
        return "Document immutable audit events, allowed redactions, and review access."
    if "exports" in categories or "uploads" in categories:
        return "Record who created, downloaded, deleted, or restored each file artifact."
    return "Record retention, deletion, and restore actions with enough evidence for review."


def _backup_handling(categories: tuple[RetentionSignal, ...]) -> str:
    if "backups" in categories:
        return "Define backup retention, restore exclusion, and eventual purge expectations."
    if "databases" in categories or "pii" in categories:
        return "Confirm backup restore procedures do not reintroduce deleted or expired data."
    return "Confirm whether retained data appears in backups and how purge is verified."


def _documentation_update(categories: tuple[RetentionSignal, ...]) -> str:
    if "pii" in categories:
        return "Update data inventory, privacy notes, and retention documentation."
    if "exports" in categories or "generated_files" in categories:
        return "Document artifact location, owner, retention period, and deletion process."
    return "Document retention period, deletion path, owner, and verification evidence."


def _category_label(categories: tuple[RetentionSignal, ...]) -> str:
    if len(categories) == 1:
        return categories[0].replace("_", " ")
    return ", ".join(category.replace("_", " ") for category in categories)


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
    if hasattr(value, "tasks"):
        payload = _object_payload(value)
        return (
            _optional_text(payload.get("id")),
            _optional_text(payload.get("implementation_brief_id")),
            [_task_record(task) for task in _task_payloads(payload.get("tasks"))],
        )

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
    payload["scope_name"] = _optional_text(payload.get("title")) or brief_id or "implementation brief"
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
        "validation_plan",
        "definition_of_done",
        "summary",
        "source_payload",
        "source_links",
        "milestone",
        "owner_type",
        "owner_role",
        "owner",
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
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _task_record(task: Mapping[str, Any]) -> dict[str, Any]:
    record = dict(task)
    record["task_id"] = _optional_text(record.get("id"))
    record["scope_name"] = (
        _optional_text(record.get("title"))
        or _optional_text(record.get("id"))
        or "execution task"
    )
    return record


def _scope(record: Mapping[str, Any], index: int) -> str:
    if task_id := _optional_text(record.get("task_id")):
        title = _optional_text(record.get("scope_name")) or task_id
        return f"{task_id}: {title}"
    return _optional_text(record.get("scope_name")) or f"scope-{index}"


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


def _append(
    signals: dict[RetentionSignal, list[str]],
    category: RetentionSignal,
    evidence: str,
) -> None:
    signals.setdefault(category, []).append(evidence)


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
    "PlanDataRetentionChecklist",
    "PlanDataRetentionChecklistItem",
    "RetentionSignal",
    "build_plan_data_retention_checklist",
    "plan_data_retention_checklist_to_dict",
    "plan_data_retention_checklist_to_markdown",
    "summarize_plan_data_retention_checklist",
]
