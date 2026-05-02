"""Assess task-level data deletion safety for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DeletionVector = Literal[
    "hard_delete",
    "soft_delete",
    "cascade_delete",
    "retention_purge",
    "user_request_erasure",
    "anonymization",
    "tombstone_cleanup",
]
DeletionSafeguard = Literal[
    "authorization_check",
    "dry_run_or_preview",
    "backup_or_restore_path",
    "audit_event",
    "dependency_cascade_review",
    "retention_policy_check",
    "customer_confirmation",
]
DeletionReadinessLevel = Literal["ready", "needs_safeguards", "blocked"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_VECTOR_ORDER: tuple[DeletionVector, ...] = (
    "hard_delete",
    "soft_delete",
    "cascade_delete",
    "retention_purge",
    "user_request_erasure",
    "anonymization",
    "tombstone_cleanup",
)
_SAFEGUARD_ORDER: tuple[DeletionSafeguard, ...] = (
    "authorization_check",
    "dry_run_or_preview",
    "backup_or_restore_path",
    "audit_event",
    "dependency_cascade_review",
    "retention_policy_check",
    "customer_confirmation",
)
_DELETION_RE = re.compile(
    r"\b(?:delete|deletes|deleted|deleting|deletion|destroy|destroyed|remove|removed|"
    r"removal|purge|purged|purging|erase|erased|erasure|expire|expires|expired|expiring|"
    r"anonymi[sz]e|anonymi[sz]ed|anonymi[sz]ation|redact|redacted|tombstone|tombstoned|"
    r"cascade[- ]?remove|cascade[- ]?delete|retention cleanup|cleanup expired|data cleanup)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,100}"
    r"\b(?:delete|deletion|purge|erasure|anonymi[sz]ation|tombstone|retention cleanup)\b|"
    r"\b(?:delete|deletion|purge|erasure|anonymi[sz]ation|tombstone|retention cleanup)\b"
    r".{0,100}\b(?:out of scope|not required|not needed|no change|unsupported)\b",
    re.I,
)
_VECTOR_PATTERNS: dict[DeletionVector, tuple[re.Pattern[str], ...]] = {
    "hard_delete": (
        re.compile(
            r"\b(?:hard delete|permanent(?:ly)? delete|physical(?:ly)? delete|"
            r"destroy records?|drop rows?|delete from|delete database rows?|remove database rows?)\b",
            re.I,
        ),
    ),
    "soft_delete": (
        re.compile(
            r"\b(?:soft delete|soft-delete|mark(?:ed)? as deleted|deleted_at|is_deleted|archive instead of delete)\b",
            re.I,
        ),
    ),
    "cascade_delete": (
        re.compile(
            r"\b(?:cascade delete|cascading delete|cascade removal|cascade-remove|"
            r"cascade removes?|delete dependent records?|remove child records?)\b",
            re.I,
        ),
    ),
    "retention_purge": (
        re.compile(
            r"\b(?:retention purge|retention cleanup|purge expired|expire old|expired data|ttl|time[- ]?to[- ]?live|scheduled purge|cleanup old)\b",
            re.I,
        ),
    ),
    "user_request_erasure": (
        re.compile(
            r"\b(?:user request erasure|customer request erasure|right to erasure|gdpr erasure|delete my data|account deletion request|privacy deletion request)\b",
            re.I,
        ),
    ),
    "anonymization": (
        re.compile(
            r"\b(?:anonymi[sz]e|anonymi[sz]ation|de[- ]?identify|deidentif(?:y|ication)|redact personal data|scrub pii|pseudonymi[sz]e)\b",
            re.I,
        ),
    ),
    "tombstone_cleanup": (
        re.compile(
            r"\b(?:tombstone cleanup|cleanup tombstones?|remove tombstones?|tombstoned records?|delete tombstones?)\b",
            re.I,
        ),
    ),
}
_SAFEGUARD_PATTERNS: dict[DeletionSafeguard, tuple[re.Pattern[str], ...]] = {
    "authorization_check": (
        re.compile(
            r"\b(?:authorization|authorisation|permission check|rbac|admin only|role check|access control|policy check)\b",
            re.I,
        ),
    ),
    "dry_run_or_preview": (
        re.compile(
            r"\b(?:dry[- ]?run|preview|simulate|simulation|impact report|count affected|show affected|review before execute)\b",
            re.I,
        ),
    ),
    "backup_or_restore_path": (
        re.compile(
            r"\b(?:backup|backups|restore|rollback|recovery path|point[- ]?in[- ]?time recovery|"
            r"snapshot|undelete|recoverable)\b",
            re.I,
        ),
    ),
    "audit_event": (
        re.compile(
            r"\b(?:audit event|audit log|audit trail|deletion log|log deletion|security event|compliance event)\b",
            re.I,
        ),
    ),
    "dependency_cascade_review": (
        re.compile(
            r"\b(?:cascade review|dependency review|referential integrity|foreign key review|"
            r"downstream impact|orphaned records?|review dependent records?|review child records?)\b",
            re.I,
        ),
    ),
    "retention_policy_check": (
        re.compile(
            r"\b(?:retention policy|retention rule|legal hold|hold check|policy check|ttl policy|expiration policy)\b",
            re.I,
        ),
    ),
    "customer_confirmation": (
        re.compile(
            r"\b(?:customer confirmation|user confirmation|confirm deletion|explicit confirmation|double confirm|typed confirmation|customer approval)\b",
            re.I,
        ),
    ),
}
_REQUIRED_BY_VECTOR: dict[DeletionVector, tuple[DeletionSafeguard, ...]] = {
    "hard_delete": (
        "authorization_check",
        "dry_run_or_preview",
        "backup_or_restore_path",
        "audit_event",
        "dependency_cascade_review",
    ),
    "soft_delete": ("authorization_check", "audit_event", "backup_or_restore_path"),
    "cascade_delete": (
        "authorization_check",
        "dry_run_or_preview",
        "audit_event",
        "dependency_cascade_review",
    ),
    "retention_purge": (
        "dry_run_or_preview",
        "audit_event",
        "retention_policy_check",
        "backup_or_restore_path",
    ),
    "user_request_erasure": (
        "authorization_check",
        "customer_confirmation",
        "audit_event",
        "retention_policy_check",
    ),
    "anonymization": ("authorization_check", "dry_run_or_preview", "audit_event"),
    "tombstone_cleanup": (
        "dry_run_or_preview",
        "audit_event",
        "dependency_cascade_review",
        "retention_policy_check",
    ),
}


@dataclass(frozen=True, slots=True)
class TaskDataDeletionSafetyRecord:
    """Deletion-safety assessment for one execution task."""

    task_id: str
    title: str
    deletion_vectors: tuple[DeletionVector, ...] = field(default_factory=tuple)
    required_safeguards: tuple[DeletionSafeguard, ...] = field(default_factory=tuple)
    present_safeguards: tuple[DeletionSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[DeletionSafeguard, ...] = field(default_factory=tuple)
    readiness_level: DeletionReadinessLevel = "needs_safeguards"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_actions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "deletion_vectors": list(self.deletion_vectors),
            "required_safeguards": list(self.required_safeguards),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness_level": self.readiness_level,
            "evidence": list(self.evidence),
            "recommended_actions": list(self.recommended_actions),
        }


@dataclass(frozen=True, slots=True)
class TaskDataDeletionSafetyPlan:
    """Plan-level task data deletion safety assessment."""

    plan_id: str | None = None
    records: tuple[TaskDataDeletionSafetyRecord, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "ignored_task_ids": list(self.ignored_task_ids),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return deletion safety records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the assessment as deterministic Markdown."""
        title = "# Task Data Deletion Safety Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Total tasks: {self.summary.get('total_task_count', 0)}",
            f"- Deletion-related tasks: {self.summary.get('deletion_task_count', 0)}",
            f"- Ignored tasks: {self.summary.get('ignored_task_count', 0)}",
            f"- Ready tasks: {self.summary.get('ready_task_count', 0)}",
            f"- Tasks needing safeguards: {self.summary.get('needs_safeguards_task_count', 0)}",
            f"- Blocked tasks: {self.summary.get('blocked_task_count', 0)}",
            f"- Missing safeguards: {self.summary.get('missing_safeguard_count', 0)}",
        ]
        if not self.records:
            lines.extend(["", "No data deletion safety records were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Records",
                "",
                (
                    "| Task | Title | Vectors | Required Safeguards | Present Safeguards | "
                    "Missing Safeguards | Readiness | Recommended Actions | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{_markdown_cell(record.title)} | "
                f"{_markdown_cell(', '.join(record.deletion_vectors))} | "
                f"{_markdown_cell(', '.join(record.required_safeguards))} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell(record.readiness_level)} | "
                f"{_markdown_cell('; '.join(record.recommended_actions) or 'Ready to implement with stated safeguards.')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_data_deletion_safety(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskDataDeletionSafetyPlan:
    """Assess deletion-safety readiness for deletion-related execution tasks."""
    plan_id, tasks = _source_payload(source)
    records: list[TaskDataDeletionSafetyRecord] = []
    ignored_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        record = _record_for_task(task, index)
        if record is None:
            ignored_task_ids.append(task_id)
        else:
            records.append(record)
    ordered_records = tuple(
        sorted(
            records,
            key=lambda record: (
                _readiness_rank(record.readiness_level),
                len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    ignored = tuple(sorted(_dedupe(ignored_task_ids), key=lambda value: value.casefold()))
    return TaskDataDeletionSafetyPlan(
        plan_id=plan_id,
        records=ordered_records,
        summary=_summary(
            ordered_records, total_task_count=len(tasks), ignored_task_count=len(ignored)
        ),
        ignored_task_ids=ignored,
    )


def generate_task_data_deletion_safety(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[TaskDataDeletionSafetyRecord, ...]:
    """Return deletion-safety records for relevant execution tasks."""
    return build_task_data_deletion_safety(source).records


def derive_task_data_deletion_safety(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskDataDeletionSafetyPlan
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskDataDeletionSafetyPlan:
    """Return an existing deletion-safety plan or build one from a plan-shaped source."""
    if isinstance(source, TaskDataDeletionSafetyPlan):
        return source
    return build_task_data_deletion_safety(source)


def summarize_task_data_deletion_safety(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskDataDeletionSafetyPlan
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskDataDeletionSafetyPlan:
    """Compatibility alias for task data deletion safety summaries."""
    return derive_task_data_deletion_safety(source)


def task_data_deletion_safety_to_dict(plan: TaskDataDeletionSafetyPlan) -> dict[str, Any]:
    """Serialize a task data deletion safety plan to a plain dictionary."""
    return plan.to_dict()


task_data_deletion_safety_to_dict.__test__ = False


def task_data_deletion_safety_to_dicts(
    records: (
        TaskDataDeletionSafetyPlan
        | tuple[TaskDataDeletionSafetyRecord, ...]
        | list[TaskDataDeletionSafetyRecord]
    ),
) -> list[dict[str, Any]]:
    """Serialize task data deletion safety records to dictionaries."""
    if isinstance(records, TaskDataDeletionSafetyPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


task_data_deletion_safety_to_dicts.__test__ = False


def task_data_deletion_safety_to_markdown(plan: TaskDataDeletionSafetyPlan) -> str:
    """Render a task data deletion safety plan as Markdown."""
    return plan.to_markdown()


task_data_deletion_safety_to_markdown.__test__ = False


def _record_for_task(task: Mapping[str, Any], index: int) -> TaskDataDeletionSafetyRecord | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    vectors: dict[DeletionVector, list[str]] = {}
    safeguards: dict[DeletionSafeguard, list[str]] = {}
    evidence: list[str] = []

    for source_field, text in _candidate_texts(task):
        if _NEGATED_SCOPE_RE.search(text) and not _explicit_structured_deletion_field(source_field):
            continue
        if _DELETION_RE.search(text) or _path_deletion_signal(source_field, text):
            evidence.append(_evidence_snippet(source_field, text))
        for vector, patterns in _VECTOR_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns) or _path_vector_match(
                vector, source_field, text
            ):
                snippet = _evidence_snippet(source_field, text)
                vectors.setdefault(vector, []).append(snippet)
                evidence.append(snippet)
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                snippet = _evidence_snippet(source_field, text)
                safeguards.setdefault(safeguard, []).append(snippet)
                evidence.append(snippet)

    if not vectors:
        return None

    deletion_vectors = tuple(vector for vector in _VECTOR_ORDER if vector in vectors)
    required = _required_safeguards(deletion_vectors)
    present = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguards)
    missing = tuple(safeguard for safeguard in required if safeguard not in present)
    readiness = _readiness_level(deletion_vectors, missing)
    return TaskDataDeletionSafetyRecord(
        task_id=task_id,
        title=title,
        deletion_vectors=deletion_vectors,
        required_safeguards=required,
        present_safeguards=present,
        missing_safeguards=missing,
        readiness_level=readiness,
        evidence=tuple(_dedupe(evidence)),
        recommended_actions=_recommended_actions(missing, readiness),
    )


def _required_safeguards(vectors: tuple[DeletionVector, ...]) -> tuple[DeletionSafeguard, ...]:
    required: list[DeletionSafeguard] = []
    for vector in vectors:
        required.extend(_REQUIRED_BY_VECTOR[vector])
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in set(required))


def _readiness_level(
    vectors: tuple[DeletionVector, ...],
    missing: tuple[DeletionSafeguard, ...],
) -> DeletionReadinessLevel:
    if not missing:
        return "ready"
    if "hard_delete" in vectors and (
        "authorization_check" in missing
        or "backup_or_restore_path" in missing
        or "audit_event" in missing
    ):
        return "blocked"
    if "user_request_erasure" in vectors and (
        "authorization_check" in missing or "customer_confirmation" in missing
    ):
        return "blocked"
    if len(missing) >= 4:
        return "blocked"
    return "needs_safeguards"


def _recommended_actions(
    missing: tuple[DeletionSafeguard, ...],
    readiness: DeletionReadinessLevel,
) -> tuple[str, ...]:
    if not missing:
        return ("Ready to implement after preserving the documented deletion safeguards.",)
    actions = {
        "authorization_check": "Add an explicit authorization or policy check before deletion runs.",
        "dry_run_or_preview": "Add a dry run, preview, or affected-record count before execution.",
        "backup_or_restore_path": "Document the backup, restore, rollback, or recovery path.",
        "audit_event": "Emit an audit event that records actor, scope, target, and outcome.",
        "dependency_cascade_review": "Review dependent records, cascades, and downstream effects.",
        "retention_policy_check": "Validate the deletion against retention policy and legal hold rules.",
        "customer_confirmation": "Require explicit customer or user confirmation for requested erasure.",
    }
    prefix = "Block implementation until" if readiness == "blocked" else "Before implementation"
    return tuple(f"{prefix}: {actions[safeguard]}" for safeguard in missing)


def _summary(
    records: tuple[TaskDataDeletionSafetyRecord, ...],
    *,
    total_task_count: int,
    ignored_task_count: int,
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "deletion_task_count": len(records),
        "ignored_task_count": ignored_task_count,
        "ready_task_count": sum(1 for record in records if record.readiness_level == "ready"),
        "needs_safeguards_task_count": sum(
            1 for record in records if record.readiness_level == "needs_safeguards"
        ),
        "blocked_task_count": sum(1 for record in records if record.readiness_level == "blocked"),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "vector_counts": {
            vector: sum(1 for record in records if vector in record.deletion_vectors)
            for vector in _VECTOR_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


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
        if hasattr(item, "model_dump"):
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
    return tasks


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
        "definition_of_done",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "modules",
        "tags",
        "labels",
        "notes",
        "risks",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        texts.extend(_metadata_texts(metadata))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            key_text = str(key).replace("_", " ")
            if _DELETION_RE.search(key_text) or any(
                pattern.search(key_text)
                for patterns in (*_VECTOR_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
                for pattern in patterns
            ):
                texts.append((field, key_text))
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


def _explicit_structured_deletion_field(source_field: str) -> bool:
    normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
    return any(
        token in normalized
        for token in ("deletion", "delete", "purge", "erasure", "anonymization", "tombstone")
    )


def _path_deletion_signal(source_field: str, text: str) -> bool:
    if not source_field.startswith(("files_or_modules", "files", "modules")):
        return False
    path = text.casefold().replace("-", "_")
    return any(
        token in path
        for token in (
            "delete",
            "deletion",
            "destroy",
            "purge",
            "erasure",
            "anonymize",
            "anonymise",
            "retention",
            "tombstone",
            "cascade",
        )
    )


def _path_vector_match(vector: DeletionVector, source_field: str, text: str) -> bool:
    if not _path_deletion_signal(source_field, text):
        return False
    path = text.casefold().replace("-", "_")
    tokens = {
        "hard_delete": ("hard_delete", "delete", "destroy", "drop"),
        "soft_delete": ("soft_delete", "deleted_at", "is_deleted"),
        "cascade_delete": ("cascade", "dependent", "foreign_key"),
        "retention_purge": ("retention", "purge", "ttl", "expiry", "expiration"),
        "user_request_erasure": ("erasure", "privacy_delete", "gdpr", "delete_request"),
        "anonymization": ("anonymize", "anonymise", "deidentify", "redact"),
        "tombstone_cleanup": ("tombstone",),
    }
    return any(token in path for token in tokens[vector])


def _readiness_rank(readiness: DeletionReadinessLevel) -> int:
    return {"blocked": 0, "needs_safeguards": 1, "ready": 2}[readiness]


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


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


__all__ = [
    "DeletionReadinessLevel",
    "DeletionSafeguard",
    "DeletionVector",
    "TaskDataDeletionSafetyPlan",
    "TaskDataDeletionSafetyRecord",
    "build_task_data_deletion_safety",
    "derive_task_data_deletion_safety",
    "generate_task_data_deletion_safety",
    "summarize_task_data_deletion_safety",
    "task_data_deletion_safety_to_dict",
    "task_data_deletion_safety_to_dicts",
    "task_data_deletion_safety_to_markdown",
]
