"""Plan permission and role migration readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


PermissionMigrationVector = Literal[
    "role_model_change",
    "permission_backfill",
    "scope_change",
    "entitlement_sync",
    "policy_rewrite",
    "group_mapping",
    "admin_override",
]
PermissionMigrationSafeguard = Literal[
    "compatibility_mapping",
    "migration_backfill",
    "audit_events",
    "rollback_plan",
    "access_review",
    "test_fixtures",
    "customer_communication",
]
PermissionMigrationReadinessLevel = Literal["missing", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_VECTOR_ORDER: tuple[PermissionMigrationVector, ...] = (
    "role_model_change",
    "permission_backfill",
    "scope_change",
    "entitlement_sync",
    "policy_rewrite",
    "group_mapping",
    "admin_override",
)
_SAFEGUARD_ORDER: tuple[PermissionMigrationSafeguard, ...] = (
    "compatibility_mapping",
    "migration_backfill",
    "audit_events",
    "rollback_plan",
    "access_review",
    "test_fixtures",
    "customer_communication",
)
_READINESS_ORDER: dict[PermissionMigrationReadinessLevel, int] = {
    "missing": 0,
    "partial": 1,
    "strong": 2,
}
_PATH_VECTOR_PATTERNS: dict[PermissionMigrationVector, re.Pattern[str]] = {
    "role_model_change": re.compile(r"(?:^|/)(?:roles?|rbac|acl|authorization|authz)(?:/|\.|_|-|$)", re.I),
    "permission_backfill": re.compile(r"(?:^|/)(?:backfills?|migrations?|permission[-_]?backfill)(?:/|\.|_|-|$)", re.I),
    "scope_change": re.compile(r"(?:^|/)(?:scopes?|oauth|permissions?)(?:/|\.|_|-|$)", re.I),
    "entitlement_sync": re.compile(r"(?:^|/)(?:entitlements?|plans?|billing|license|licenses)(?:/|\.|_|-|$)", re.I),
    "policy_rewrite": re.compile(r"(?:^|/)(?:polic(?:y|ies)|opa|casbin|rules?)(?:/|\.|_|-|$)", re.I),
    "group_mapping": re.compile(r"(?:^|/)(?:groups?|teams?|directory|scim|saml)(?:/|\.|_|-|$)", re.I),
    "admin_override": re.compile(r"(?:^|/)(?:admin|superuser|override|impersonat)(?:/|\.|_|-|$)", re.I),
}
_VECTOR_PATTERNS: dict[PermissionMigrationVector, re.Pattern[str]] = {
    "role_model_change": re.compile(
        r"\b(?:role model|role migration|role hierarchy|rbac|acl|authorization model|"
        r"permission model|roles? and permissions?|change roles?|new roles?|legacy roles?|roles?)\b",
        re.I,
    ),
    "permission_backfill": re.compile(
        r"\b(?:permission backfill|backfill permissions?|migrate permissions?|existing permissions?|"
        r"grant existing|revoke existing|populate permissions?|default grants?|seed permissions?)\b",
        re.I,
    ),
    "scope_change": re.compile(
        r"\b(?:scope change|oauth scopes?|api scopes?|permission scopes?|scoped access|"
        r"read scope|write scope|narrow scopes?|expand scopes?|least privilege)\b",
        re.I,
    ),
    "entitlement_sync": re.compile(
        r"\b(?:entitlement sync|sync entitlements?|plan entitlements?|feature entitlements?|"
        r"license entitlements?|billing entitlements?|seats? entitlements?|subscription access)\b",
        re.I,
    ),
    "policy_rewrite": re.compile(
        r"\b(?:policy rewrite|rewrite policies?|access policies?|authorization policies?|"
        r"policy engine|opa|casbin|rego|rules engine|policy rules?)\b",
        re.I,
    ),
    "group_mapping": re.compile(
        r"\b(?:group mapping|map groups?|directory groups?|saml groups?|scim groups?|"
        r"team mapping|identity provider groups?|idp groups?|group claims?)\b",
        re.I,
    ),
    "admin_override": re.compile(
        r"\b(?:admin override|super admin|superuser|break glass|emergency access|"
        r"manual grant|manual revoke|impersonation|support override)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[PermissionMigrationSafeguard, re.Pattern[str]] = {
    "compatibility_mapping": re.compile(
        r"\b(?:compatibility mapping|compatibility matrix|old[- ]to[- ]new|legacy[- ]to[- ]new|"
        r"role mapping|permission mapping|scope mapping|mapping table|migration matrix)\b",
        re.I,
    ),
    "migration_backfill": re.compile(
        r"\b(?:migration backfill|backfill job|backfill permissions?|backfill entitlements?|"
        r"batch backfill|idempotent backfill|dry[- ]run backfill|reconcile existing access)\b",
        re.I,
    ),
    "audit_events": re.compile(
        r"\b(?:audit events?|audit logs?|audit trail|access change events?|permission change logs?|"
        r"role change events?|who changed access|security event)\b",
        re.I,
    ),
    "rollback_plan": re.compile(
        r"\b(?:rollback plan|rollback strategy|roll back|revert plan|restore previous access|"
        r"undo grants?|undo revokes?|fallback policy|feature flag rollback)\b",
        re.I,
    ),
    "access_review": re.compile(
        r"\b(?:access review|security review|permission review|role review|least privilege review|"
        r"privilege review|manual review|approval review)\b",
        re.I,
    ),
    "test_fixtures": re.compile(
        r"\b(?:test fixtures?|fixture coverage|permission tests?|role tests?|authorization tests?|"
        r"policy tests?|access matrix tests?|regression tests?|integration tests?)\b",
        re.I,
    ),
    "customer_communication": re.compile(
        r"\b(?:customer communication|customer notice|notify customers?|user communication|release notes?|"
        r"admin notice|support runbook|customer support|migration email|changelog)\b",
        re.I,
    ),
}
_NEGATED_PERMISSION_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,120}\b(?:role|permission|scope|group|policy|entitlement|access)\b"
    r".{0,120}\b(?:change|migration|impact|work|updates?|required|needed|in scope)\b|"
    r"\b(?:role|permission|scope|group|policy|entitlement|access)\b.{0,120}\b(?:out of scope|"
    r"not required|not needed|unchanged|no changes?|no impact|non[- ]?goal)\b",
    re.I,
)
_NEGATED_SAFEGUARD_RE = re.compile(r"\b(?:missing|still missing|not defined|without|lacks?|need(?:s)?|todo)\b", re.I)
_RECOMMENDED_ACTIONS: dict[PermissionMigrationSafeguard, str] = {
    "compatibility_mapping": "Document old-to-new role, permission, scope, group, policy, or entitlement mappings before implementation.",
    "migration_backfill": "Define an idempotent backfill or reconciliation path for existing accounts and historical access records.",
    "audit_events": "Emit audit events for migrated grants, revocations, overrides, and policy decisions.",
    "rollback_plan": "Add rollback or fallback steps that restore previous access semantics without orphaning grants.",
    "access_review": "Schedule security or owner review of privileged roles, least-privilege changes, and sampled migrated accounts.",
    "test_fixtures": "Add fixtures and regression tests that cover old and new roles, scopes, policies, groups, and entitlements.",
    "customer_communication": "Prepare customer, admin, support, or release-note communication for visible access behavior changes.",
}


@dataclass(frozen=True, slots=True)
class TaskPermissionMigrationReadinessRecord:
    """Readiness guidance for one task that changes permissions or access models."""

    task_id: str
    title: str
    migration_vectors: tuple[PermissionMigrationVector, ...]
    present_safeguards: tuple[PermissionMigrationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[PermissionMigrationSafeguard, ...] = field(default_factory=tuple)
    readiness_level: PermissionMigrationReadinessLevel = "missing"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_followups: tuple[str, ...] = field(default_factory=tuple)

    @property
    def detected_vectors(self) -> tuple[PermissionMigrationVector, ...]:
        """Compatibility alias for vector-oriented consumers."""
        return self.migration_vectors

    @property
    def recommended_actions(self) -> tuple[str, ...]:
        """Compatibility alias for action-oriented consumers."""
        return self.recommended_followups

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "migration_vectors": list(self.migration_vectors),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness_level": self.readiness_level,
            "evidence": list(self.evidence),
            "recommended_followups": list(self.recommended_followups),
        }


@dataclass(frozen=True, slots=True)
class TaskPermissionMigrationReadinessPlan:
    """Plan-level permission migration readiness review."""

    plan_id: str | None = None
    records: tuple[TaskPermissionMigrationReadinessRecord, ...] = field(default_factory=tuple)
    permission_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def readiness_records(self) -> tuple[TaskPermissionMigrationReadinessRecord, ...]:
        """Compatibility view matching planners that expose rows as readiness_records."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskPermissionMigrationReadinessRecord, ...]:
        """Compatibility view for recommendation-oriented consumers."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "readiness_records": [record.to_dict() for record in self.readiness_records],
            "permission_task_ids": list(self.permission_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render permission migration readiness as deterministic Markdown."""
        title = "# Task Permission Migration Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Permission migration task count: {self.summary.get('permission_task_count', 0)}",
            f"- Ignored task count: {self.summary.get('ignored_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No permission migration readiness records were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)
        lines.extend(
            [
                "",
                "## Records",
                "",
                (
                    "| Task | Title | Readiness | Migration Vectors | Present Safeguards | "
                    "Missing Safeguards | Recommended Follow-ups | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell(', '.join(record.migration_vectors) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_followups) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_permission_migration_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPermissionMigrationReadinessPlan:
    """Build permission migration readiness records for task-shaped input."""
    plan_id, plan_context, tasks = _source_payload(source)
    candidates = [_record(task, index, plan_context) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_READINESS_ORDER[record.readiness_level], record.task_id, record.title.casefold()),
        )
    )
    ignored_task_ids = tuple(
        sorted(
            _dedupe(
                _task_id(task, index)
                for index, task in enumerate(tasks, start=1)
                if candidates[index - 1] is None
            ),
            key=lambda value: value.casefold(),
        )
    )
    return TaskPermissionMigrationReadinessPlan(
        plan_id=plan_id,
        records=records,
        permission_task_ids=tuple(record.task_id for record in records),
        ignored_task_ids=ignored_task_ids,
        summary=_summary(records, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def build_task_permission_migration_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPermissionMigrationReadinessPlan:
    """Compatibility alias for building permission migration readiness plans."""
    return build_task_permission_migration_readiness_plan(source)


def generate_task_permission_migration_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskPermissionMigrationReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPermissionMigrationReadinessPlan:
    """Compatibility alias for generating permission migration readiness plans."""
    if isinstance(source, TaskPermissionMigrationReadinessPlan):
        return source
    return build_task_permission_migration_readiness_plan(source)


def derive_task_permission_migration_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskPermissionMigrationReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPermissionMigrationReadinessPlan:
    """Compatibility alias for deriving permission migration readiness plans."""
    return generate_task_permission_migration_readiness_plan(source)


def extract_task_permission_migration_readiness_records(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskPermissionMigrationReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[TaskPermissionMigrationReadinessRecord, ...]:
    """Return permission migration readiness records from task-shaped input."""
    return derive_task_permission_migration_readiness_plan(source).records


def summarize_task_permission_migration_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskPermissionMigrationReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic summary counts for permission migration readiness."""
    return derive_task_permission_migration_readiness_plan(source).summary


def task_permission_migration_readiness_to_dict(
    plan: TaskPermissionMigrationReadinessPlan,
) -> dict[str, Any]:
    """Serialize a permission migration readiness plan to a plain dictionary."""
    return plan.to_dict()


task_permission_migration_readiness_to_dict.__test__ = False


def task_permission_migration_readiness_to_dicts(
    records: (
        tuple[TaskPermissionMigrationReadinessRecord, ...]
        | list[TaskPermissionMigrationReadinessRecord]
        | TaskPermissionMigrationReadinessPlan
    ),
) -> list[dict[str, Any]]:
    """Serialize permission migration readiness records to dictionaries."""
    if isinstance(records, TaskPermissionMigrationReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


task_permission_migration_readiness_to_dicts.__test__ = False


def task_permission_migration_readiness_to_markdown(
    plan: TaskPermissionMigrationReadinessPlan,
) -> str:
    """Render a permission migration readiness plan as Markdown."""
    return plan.to_markdown()


task_permission_migration_readiness_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    vectors: tuple[PermissionMigrationVector, ...] = field(default_factory=tuple)
    vector_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[PermissionMigrationSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _record(
    task: Mapping[str, Any],
    index: int,
    plan_context: tuple[tuple[str, str], ...],
) -> TaskPermissionMigrationReadinessRecord | None:
    task_signals = _signals(task, ())
    if not task_signals.vectors or _is_negated(task):
        return None
    signals = _signals(task, plan_context)
    required = _required_safeguards(signals.vectors)
    missing = tuple(safeguard for safeguard in required if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskPermissionMigrationReadinessRecord(
        task_id=task_id,
        title=title,
        migration_vectors=signals.vectors,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        readiness_level=_readiness_level(signals.present_safeguards, missing),
        evidence=tuple(_dedupe([*signals.vector_evidence, *signals.safeguard_evidence])),
        recommended_followups=tuple(_RECOMMENDED_ACTIONS[safeguard] for safeguard in missing),
    )


def _signals(
    task: Mapping[str, Any],
    plan_context: tuple[tuple[str, str], ...],
) -> _Signals:
    vector_hits: set[PermissionMigrationVector] = set()
    safeguard_hits: set[PermissionMigrationSafeguard] = set()
    vector_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for vector in _VECTOR_ORDER:
            if _PATH_VECTOR_PATTERNS[vector].search(normalized) or _VECTOR_PATTERNS[vector].search(path_text):
                vector_hits.add(vector)
                matched = True
        if matched:
            vector_evidence.append(f"files_or_modules: {path}")

    for source_field, text in (*_task_texts(task), *plan_context):
        matched_vectors = tuple(vector for vector in _VECTOR_ORDER if _VECTOR_PATTERNS[vector].search(text))
        if matched_vectors:
            vector_hits.update(matched_vectors)
            vector_evidence.append(_evidence_snippet(source_field, text))
        matched_safeguards = tuple(
            safeguard for safeguard in _SAFEGUARD_ORDER if _SAFEGUARD_PATTERNS[safeguard].search(text)
        )
        if matched_safeguards and not _NEGATED_SAFEGUARD_RE.search(text):
            safeguard_hits.update(matched_safeguards)
            safeguard_evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        vectors=tuple(vector for vector in _VECTOR_ORDER if vector in vector_hits),
        vector_evidence=tuple(_dedupe(vector_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _required_safeguards(
    vectors: tuple[PermissionMigrationVector, ...],
) -> tuple[PermissionMigrationSafeguard, ...]:
    required: set[PermissionMigrationSafeguard] = {
        "compatibility_mapping",
        "audit_events",
        "rollback_plan",
        "access_review",
        "test_fixtures",
    }
    if set(vectors) & {
        "role_model_change",
        "permission_backfill",
        "scope_change",
        "entitlement_sync",
        "policy_rewrite",
        "group_mapping",
    }:
        required.add("migration_backfill")
    if set(vectors) & {"role_model_change", "scope_change", "entitlement_sync", "policy_rewrite", "group_mapping"}:
        required.add("customer_communication")
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required)


def _readiness_level(
    present: tuple[PermissionMigrationSafeguard, ...],
    missing: tuple[PermissionMigrationSafeguard, ...],
) -> PermissionMigrationReadinessLevel:
    if not missing:
        return "strong"
    if present:
        return "partial"
    return "missing"


def _summary(
    records: tuple[TaskPermissionMigrationReadinessRecord, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    readiness_counts = {
        level: sum(1 for record in records if record.readiness_level == level)
        for level in _READINESS_ORDER
    }
    vector_counts = {
        vector: sum(1 for record in records if vector in record.migration_vectors)
        for vector in _VECTOR_ORDER
    }
    safeguard_counts = {
        safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
        for safeguard in _SAFEGUARD_ORDER
    }
    missing_safeguard_counts = {
        safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
        for safeguard in _SAFEGUARD_ORDER
    }
    missing_count = sum(len(record.missing_safeguards) for record in records)
    return {
        "task_count": task_count,
        "permission_task_count": len(records),
        "permission_task_ids": [record.task_id for record in records],
        "ignored_task_count": len(ignored_task_ids),
        "ignored_task_ids": list(ignored_task_ids),
        "missing_safeguard_count": missing_count,
        "readiness_counts": readiness_counts,
        "vector_counts": vector_counts,
        "present_safeguard_counts": safeguard_counts,
        "missing_safeguard_counts": missing_safeguard_counts,
        "status": (
            "no_permission_migration_signals"
            if not records
            else "strong"
            if missing_count == 0
            else "missing_permission_migration_safeguards"
        ),
    }


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
        return _optional_text(payload.get("id")), tuple(_plan_context(payload)), [
            task.model_dump(mode="python") for task in source.tasks
        ]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), tuple(_plan_context(payload)), _task_payloads(payload.get("tasks"))
        return None, (), [dict(source)]
    if _looks_like_task(source):
        return None, (), [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), tuple(_plan_context(payload)), _task_payloads(payload.get("tasks"))
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
    for field_name in ("target_engine", "target_repo", "project_type", "test_strategy", "handoff_prompt", "risk"):
        if text := _optional_text(plan.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("milestones", "risks", "acceptance_criteria", "metadata", "implementation_brief", "brief"):
        texts.extend(_metadata_texts(plan.get(field_name), prefix=field_name))
    return texts


def _task_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "owner_role",
        "suggested_engine",
        "risk",
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
        "dependencies",
        "tags",
        "labels",
        "notes",
        "validation_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    texts.extend(_metadata_texts(task.get("metadata")))
    return tuple(texts)


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            patterns = (*_VECTOR_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
            if isinstance(child, (Mapping, list, tuple, set)):
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, f"{key_text}: {text}"))
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


def _is_negated(task: Mapping[str, Any]) -> bool:
    return any(_NEGATED_PERMISSION_RE.search(text) for _, text in _task_texts(task))


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


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
        "owner_role",
        "suggested_engine",
        "depends_on",
        "dependencies",
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
        "blocked_reason",
        "tags",
        "labels",
        "notes",
        "tasks",
        "milestones",
        "implementation_brief",
        "brief",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
    return str(
        PurePosixPath(
            value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
        )
    )


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
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ").strip()


__all__ = [
    "PermissionMigrationReadinessLevel",
    "PermissionMigrationSafeguard",
    "PermissionMigrationVector",
    "TaskPermissionMigrationReadinessPlan",
    "TaskPermissionMigrationReadinessRecord",
    "build_task_permission_migration_readiness",
    "build_task_permission_migration_readiness_plan",
    "derive_task_permission_migration_readiness_plan",
    "extract_task_permission_migration_readiness_records",
    "generate_task_permission_migration_readiness_plan",
    "summarize_task_permission_migration_readiness",
    "task_permission_migration_readiness_to_dict",
    "task_permission_migration_readiness_to_dicts",
    "task_permission_migration_readiness_to_markdown",
]
