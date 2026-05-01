"""Build tenant isolation matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


TenantIsolationConcern = Literal[
    "tenant",
    "organization",
    "workspace",
    "account",
    "role",
    "membership",
    "invite",
    "shared_resource",
    "cross_tenant",
    "admin_impersonation",
]
IsolationRiskLevel = Literal["critical", "high", "medium"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CONCERN_ORDER: dict[TenantIsolationConcern, int] = {
    "cross_tenant": 0,
    "admin_impersonation": 1,
    "tenant": 2,
    "organization": 3,
    "workspace": 4,
    "account": 5,
    "membership": 6,
    "role": 7,
    "invite": 8,
    "shared_resource": 9,
}
_CONCERN_PATTERNS: dict[TenantIsolationConcern, re.Pattern[str]] = {
    "tenant": re.compile(
        r"(?<!cross-)(?<!cross )\b(?:tenant|tenants|tenant[- ]?id|tenant scoped|multi[- ]?tenant)\b",
        re.I,
    ),
    "organization": re.compile(
        r"\b(?:organization|organisations?|org[-_ ]?id|org scoped|org boundary)\b", re.I
    ),
    "workspace": re.compile(
        r"\b(?:workspace|workspaces|workspace[-_ ]?id|project space|team space)\b", re.I
    ),
    "account": re.compile(
        r"\b(?:account|accounts|account[-_ ]?id|customer account|account boundary)\b", re.I
    ),
    "role": re.compile(
        r"\b(?:role|roles|rbac|permission|permissions|privilege|privileges|admin role)\b", re.I
    ),
    "membership": re.compile(
        r"\b(?:membership|memberships|member list|member access|team member|org member)\b", re.I
    ),
    "invite": re.compile(
        r"\b(?:invite|invites|invitation|invitations|invite token|join link)\b", re.I
    ),
    "shared_resource": re.compile(
        r"\b(?:shared resource|shared resources|shared file|shared folder|shared dashboard|"
        r"shared view|global resource|public link|share link)\b",
        re.I,
    ),
    "cross_tenant": re.compile(
        r"\b(?:cross[- ]?tenant|between tenants|across tenants|tenant boundary|tenant boundaries|"
        r"other tenant|foreign tenant|tenant switch|switch tenants)\b",
        re.I,
    ),
    "admin_impersonation": re.compile(
        r"\b(?:impersonat\w+|admin as user|act as user|sudo user|support access|break glass|"
        r"break[- ]glass|delegated admin)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[str, re.Pattern[str]] = {
    "tenant_scoping": re.compile(
        r"\b(?:tenant scoped|scope by tenant|tenant filter|tenant[-_ ]?id filter|"
        r"tenant isolation|row[- ]level security|rls)\b",
        re.I,
    ),
    "authorization": re.compile(
        r"\b(?:authorization|authorisation|authorize|authorise|permission check|rbac|"
        r"access control|acl|policy check|role check)\b",
        re.I,
    ),
    "membership_validation": re.compile(
        r"\b(?:membership check|member validation|validate membership|belongs to workspace|"
        r"belongs to organization|org member|workspace member)\b",
        re.I,
    ),
    "invite_controls": re.compile(
        r"\b(?:invite token|token expiry|expiring token|single use|one[- ]time invite|"
        r"domain allowlist|invitation audit)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit log|audit trail|audit event|access log|security log|logged)\b",
        re.I,
    ),
    "impersonation_controls": re.compile(
        r"\b(?:impersonation banner|reason code|approval|session recording|break[- ]glass audit|"
        r"support access audit)\b",
        re.I,
    ),
    "negative_tests": re.compile(
        r"\b(?:negative\b.{0,40}\btests?|isolation tests?|cross[- ]tenant tests?|authorization tests?|permission tests?|"
        r"tenant fixture|workspace fixture)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class PlanTenantIsolationMatrixRow:
    """One tenant isolation row grouped by concern type."""

    concern_type: TenantIsolationConcern
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    risk_level: IsolationRiskLevel = "medium"
    required_safeguards: tuple[str, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[str, ...] = field(default_factory=tuple)
    recommended_validation: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "concern_type": self.concern_type,
            "task_ids": list(self.task_ids),
            "risk_level": self.risk_level,
            "required_safeguards": list(self.required_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "recommended_validation": list(self.recommended_validation),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanTenantIsolationMatrix:
    """Plan-level tenant isolation matrix and rollup counts."""

    plan_id: str | None = None
    rows: tuple[PlanTenantIsolationMatrixRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return tenant isolation rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the tenant isolation matrix as deterministic Markdown."""
        title = "# Plan Tenant Isolation Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title, "", "## Summary", ""]
        lines.extend(
            [
                f"- Concern count: {self.summary.get('concern_count', 0)}",
                f"- Impacted task count: {self.summary.get('impacted_task_count', 0)}",
                f"- Critical risk count: {self.summary.get('risk_counts', {}).get('critical', 0)}",
                f"- High risk count: {self.summary.get('risk_counts', {}).get('high', 0)}",
                f"- Medium risk count: {self.summary.get('risk_counts', {}).get('medium', 0)}",
                f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            ]
        )
        if not self.rows:
            lines.extend(["", "No tenant isolation impact was detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Concern | Risk | Tasks | Required Safeguards | Missing Safeguards | "
                    "Recommended Validation | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.concern_type} | "
                f"{row.risk_level} | "
                f"{_markdown_cell(', '.join(row.task_ids) or 'plan metadata')} | "
                f"{_markdown_cell('; '.join(row.required_safeguards))} | "
                f"{_markdown_cell('; '.join(row.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(row.recommended_validation))} | "
                f"{_markdown_cell('; '.join(row.evidence))} |"
            )
        return "\n".join(lines)


def build_plan_tenant_isolation_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | None
    ),
) -> PlanTenantIsolationMatrix:
    """Build a tenant isolation matrix from a plan, task, or task iterable."""
    plan_id, records = _source_records(source)
    builders: dict[TenantIsolationConcern, _RowBuilder] = {
        concern: _RowBuilder(concern) for concern in _CONCERN_ORDER
    }

    for record in records:
        task_id = _optional_text(record.get("task_id"))
        record_concerns: set[TenantIsolationConcern] = set()
        record_safeguards: set[str] = set()
        for source_field, text in _record_texts(record):
            snippet = _evidence_snippet(source_field, text)
            for concern, pattern in _CONCERN_PATTERNS.items():
                if pattern.search(text):
                    record_concerns.add(concern)
                    builder = builders[concern]
                    if task_id:
                        builder.task_ids.append(task_id)
                    builder.evidence.append(snippet)
            for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
                if pattern.search(text):
                    record_safeguards.add(safeguard)
        for concern in record_concerns:
            builders[concern].safeguards.update(record_safeguards)

    rows = tuple(
        _row(builder)
        for builder in sorted(builders.values(), key=lambda item: _CONCERN_ORDER[item.concern])
        if builder.evidence
    )
    risk_counts = {
        risk: sum(1 for row in rows if row.risk_level == risk)
        for risk in ("critical", "high", "medium")
    }
    return PlanTenantIsolationMatrix(
        plan_id=plan_id,
        rows=rows,
        summary={
            "concern_count": len(rows),
            "impacted_task_count": len({task_id for row in rows for task_id in row.task_ids}),
            "risk_counts": risk_counts,
            "missing_safeguard_count": sum(len(row.missing_safeguards) for row in rows),
        },
    )


def summarize_plan_tenant_isolation(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | None
    ),
) -> PlanTenantIsolationMatrix:
    """Compatibility alias for building tenant isolation matrices."""
    return build_plan_tenant_isolation_matrix(source)


def plan_tenant_isolation_matrix_to_dict(
    matrix: PlanTenantIsolationMatrix,
) -> dict[str, Any]:
    """Serialize a tenant isolation matrix to a plain dictionary."""
    return matrix.to_dict()


plan_tenant_isolation_matrix_to_dict.__test__ = False


def plan_tenant_isolation_matrix_to_markdown(matrix: PlanTenantIsolationMatrix) -> str:
    """Render a tenant isolation matrix as Markdown."""
    return matrix.to_markdown()


plan_tenant_isolation_matrix_to_markdown.__test__ = False


@dataclass(slots=True)
class _RowBuilder:
    concern: TenantIsolationConcern
    task_ids: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    safeguards: set[str] = field(default_factory=set)


def _row(builder: _RowBuilder) -> PlanTenantIsolationMatrixRow:
    required = _required_safeguards(builder.concern)
    validation = _recommended_validation(builder.concern)
    missing = tuple(item for item in required if _safeguard_key(item) not in builder.safeguards)
    return PlanTenantIsolationMatrixRow(
        concern_type=builder.concern,
        task_ids=tuple(sorted(_dedupe(builder.task_ids))),
        risk_level=_risk_level(builder.concern),
        required_safeguards=required,
        missing_safeguards=missing,
        recommended_validation=validation,
        evidence=tuple(_dedupe(builder.evidence)),
    )


def _risk_level(concern: TenantIsolationConcern) -> IsolationRiskLevel:
    if concern in {"cross_tenant", "admin_impersonation"}:
        return "critical"
    if concern in {"tenant", "organization", "workspace", "account", "role", "membership"}:
        return "high"
    return "medium"


def _required_safeguards(concern: TenantIsolationConcern) -> tuple[str, ...]:
    common = (
        "tenant_scoping: Scope every read, write, and background job to the active tenant boundary.",
        "authorization: Enforce server-side authorization before returning or mutating tenant-scoped data.",
        "negative_tests: Cover denied access with cross-tenant or cross-workspace fixtures.",
    )
    specific: dict[TenantIsolationConcern, tuple[str, ...]] = {
        "cross_tenant": (
            "audit_logging: Audit every intentional cross-tenant operation with actor, source tenant, target tenant, and reason.",
        ),
        "admin_impersonation": (
            "impersonation_controls: Require explicit approval or reason capture, visible impersonation state, and scoped session expiry.",
            "audit_logging: Audit impersonation start, action, and end events.",
        ),
        "membership": (
            "membership_validation: Validate current membership before granting workspace or organization access.",
        ),
        "role": (
            "authorization: Check role permissions on the server for each privileged action.",
        ),
        "invite": (
            "invite_controls: Use expiring, single-use invite tokens bound to the intended tenant or workspace.",
            "audit_logging: Record invite creation, acceptance, revocation, and failure events.",
        ),
        "shared_resource": (
            "authorization: Re-check viewer permissions when shared resources are opened, listed, or exported.",
            "audit_logging: Audit sharing changes and external access.",
        ),
    }
    return tuple(_dedupe([*common, *specific.get(concern, ())]))


def _recommended_validation(concern: TenantIsolationConcern) -> tuple[str, ...]:
    validation: dict[TenantIsolationConcern, tuple[str, ...]] = {
        "cross_tenant": (
            "Add fixtures for two tenants and assert source-tenant users cannot read or mutate target-tenant data unless an explicit cross-tenant path is used.",
            "Exercise reporting, search, export, and background jobs for tenant filter leaks.",
        ),
        "admin_impersonation": (
            "Test impersonation requires a privileged actor, captures a reason, expires, and leaves an audit trail.",
            "Assert impersonated sessions cannot elevate permissions beyond the target user and visible support state.",
        ),
        "invite": (
            "Test invite tokens cannot be reused, used after expiry, or accepted into a different tenant or workspace.",
        ),
        "membership": (
            "Test removed or inactive members lose access immediately across API, UI, and background workflows.",
        ),
        "role": (
            "Run role matrix tests for each privileged action and verify deny-by-default behavior.",
        ),
        "shared_resource": (
            "Test shared links, exports, and listings with owners, members, non-members, and revoked access.",
        ),
    }
    return validation.get(
        concern,
        (
            "Run positive and negative isolation tests with at least two tenants, organizations, or workspaces.",
            "Verify list, detail, update, delete, search, export, and job paths all apply the same boundary.",
        ),
    )


def _safeguard_key(value: str) -> str:
    return value.split(":", 1)[0]


def _source_records(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | None
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if source is None:
        return None, []
    if isinstance(source, ExecutionTask):
        return None, [_task_record(source.model_dump(mode="python"), 1)]
    if isinstance(source, ExecutionPlan):
        return (
            _optional_text(source.id),
            [
                _task_record(task.model_dump(mode="python"), index)
                for index, task in enumerate(source.tasks, start=1)
            ],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            records = [_plan_metadata_record(plan)]
            records.extend(
                _task_record(task, index)
                for index, task in enumerate(_task_payloads(plan.get("tasks")), start=1)
            )
            return _optional_text(plan.get("id")), records
        return None, [_task_record(dict(source), 1)]

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    records: list[dict[str, Any]] = []
    for index, item in enumerate(iterator, start=1):
        if isinstance(item, ExecutionTask):
            records.append(_task_record(item.model_dump(mode="python"), index))
        elif hasattr(item, "model_dump"):
            value = item.model_dump(mode="python")
            if isinstance(value, Mapping):
                records.append(_task_record(dict(value), index))
        elif isinstance(item, Mapping):
            records.append(_task_record(dict(item), index))
    return None, records


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | None) -> dict[str, Any]:
    if plan is None:
        return {}
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
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _plan_metadata_record(plan: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "title": _optional_text(plan.get("title")),
        "metadata": {
            key: value
            for key, value in plan.items()
            if key not in {"id", "implementation_brief_id", "tasks", "milestones"}
        },
    }


def _task_record(task: Mapping[str, Any], index: int) -> dict[str, Any]:
    value = dict(task)
    value["task_id"] = _optional_text(value.get("id")) or f"task-{index}"
    return value


def _record_texts(record: Mapping[str, Any]) -> list[tuple[str, str]]:
    task_id = _optional_text(record.get("task_id"))
    prefix = f"task[{task_id}]" if task_id else "plan"
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
        "implementation_notes",
        "notes",
        "risks",
        "assumptions",
        "source_references",
    ):
        for index, text in enumerate(_strings(record.get(field_name))):
            suffix = "" if index == 0 else f"[{index}]"
            texts.append((f"{prefix}.{field_name}{suffix}", text))
    for field_name in ("acceptance_criteria", "depends_on", "dependencies", "tags", "labels"):
        for index, text in enumerate(_strings(record.get(field_name))):
            texts.append((f"{prefix}.{field_name}[{index}]", text))
    for index, path in enumerate(_strings(record.get("files_or_modules") or record.get("files"))):
        texts.append((f"{prefix}.files_or_modules[{index}]", _normalized_path(path)))
    for field_path, text in _metadata_texts(record.get("metadata"), f"{prefix}.metadata"):
        texts.append((field_path, text))
    return texts


def _metadata_texts(value: Any, prefix: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field_path = f"{prefix}.{key}"
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.append((field_path, key_text))
                texts.extend(_metadata_texts(child, field_path))
            elif text := _optional_text(child):
                texts.append((field_path, text))
                texts.append((field_path, f"{key_text}: {text}"))
            else:
                texts.append((field_path, key_text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field_path = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field_path))
            elif text := _optional_text(item):
                texts.append((field_path, text))
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
        strings = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _SPACE_RE.sub(" ", str(value)).strip()
    return text or None


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/")


def _evidence_snippet(source_field: str, text: str, *, limit: int = 180) -> str:
    clean = _SPACE_RE.sub(" ", text).strip()
    if len(clean) > limit:
        clean = f"{clean[: limit - 3].rstrip()}..."
    return f"{source_field}: {clean}"


def _dedupe(values: Iterable[_T]) -> list[_T]:
    seen: set[Any] = set()
    result: list[_T] = []
    for value in values:
        key = value.casefold() if isinstance(value, str) else value
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
