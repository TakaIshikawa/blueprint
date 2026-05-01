"""Build plan-level data retention lifecycle impact matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


RetentionImpactSeverity = Literal["high", "medium", "low"]
RetentionDomain = Literal[
    "deletion",
    "purge",
    "archive",
    "backup",
    "audit_log",
    "analytics_history",
    "legal_hold",
    "retention_period",
    "user_data_lifecycle",
]
ImpactedDataClass = Literal[
    "user_data",
    "personal_data",
    "database_records",
    "files_and_exports",
    "operational_logs",
    "audit_records",
    "analytics_events",
    "backups",
    "archives",
    "legal_hold_records",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SEVERITY_ORDER: dict[RetentionImpactSeverity, int] = {"high": 0, "medium": 1, "low": 2}
_DOMAIN_ORDER: dict[RetentionDomain, int] = {
    "deletion": 0,
    "purge": 1,
    "retention_period": 2,
    "archive": 3,
    "backup": 4,
    "audit_log": 5,
    "analytics_history": 6,
    "legal_hold": 7,
    "user_data_lifecycle": 8,
}
_DATA_CLASS_ORDER: dict[ImpactedDataClass, int] = {
    "personal_data": 0,
    "user_data": 1,
    "database_records": 2,
    "files_and_exports": 3,
    "operational_logs": 4,
    "audit_records": 5,
    "analytics_events": 6,
    "backups": 7,
    "archives": 8,
    "legal_hold_records": 9,
}
_DOMAIN_PATTERNS: dict[RetentionDomain, re.Pattern[str]] = {
    "deletion": re.compile(
        r"\b(?:delete|deletes|deleted|deletion|remove user data|erase|erasure|forget me|right to be forgotten|tombstone)\b",
        re.I,
    ),
    "purge": re.compile(
        r"\b(?:purge|purges|purged|hard delete|vacuum|sweeper|cleanup job|janitor job|ttl cleanup)\b",
        re.I,
    ),
    "archive": re.compile(
        r"\b(?:archive|archives|archival|cold storage|long[- ]term storage|retire records?)\b", re.I
    ),
    "backup": re.compile(
        r"\b(?:backup|backups|snapshot|restore|restore point|disaster recovery|point[- ]in[- ]time recovery|pitr)\b",
        re.I,
    ),
    "audit_log": re.compile(
        r"\b(?:audit log|audit logs|audit trail|audit event|immutable log|compliance log|security event)\b",
        re.I,
    ),
    "analytics_history": re.compile(
        r"\b(?:analytics|tracking history|event history|usage history|behavioral history|metrics history|funnel|attribution)\b",
        re.I,
    ),
    "legal_hold": re.compile(
        r"\b(?:legal hold|litigation hold|preservation hold|hold notice|e[- ]?discovery|ediscovery)\b",
        re.I,
    ),
    "retention_period": re.compile(
        r"\b(?:retention period|retention policy|retain for|retained for|retention window|ttl|time to live|expiry|expiration)\b",
        re.I,
    ),
    "user_data_lifecycle": re.compile(
        r"\b(?:user data lifecycle|account deletion|account closure|offboarding|deactivation|reactivation|data lifecycle|data subject request|dsr)\b",
        re.I,
    ),
}
_DATA_CLASS_PATTERNS: dict[ImpactedDataClass, re.Pattern[str]] = {
    "personal_data": re.compile(
        r"\b(?:pii|personal data|personally identifiable|email addresses?|phone numbers?|address|date of birth|birthdate|ssn)\b",
        re.I,
    ),
    "user_data": re.compile(
        r"\b(?:user data|customer data|profile|account|accounts|users?|customers?|tenant data)\b",
        re.I,
    ),
    "database_records": re.compile(
        r"\b(?:database|db|table|record|records|row|rows|schema|migration|postgres|mysql|warehouse)\b",
        re.I,
    ),
    "files_and_exports": re.compile(
        r"\b(?:file|files|export|exports|csv|download|attachment|upload|artifact|report)\b", re.I
    ),
    "operational_logs": re.compile(
        r"\b(?:log|logs|logging|trace|traces|telemetry|observability|error report)\b", re.I
    ),
    "audit_records": re.compile(
        r"\b(?:audit record|audit records|audit log|audit trail|audit event|compliance event)\b",
        re.I,
    ),
    "analytics_events": re.compile(
        r"\b(?:analytics|event|events|tracking|metric|metrics|funnel|attribution)\b", re.I
    ),
    "backups": re.compile(
        r"\b(?:backup|backups|snapshot|restore point|restore|disaster recovery)\b", re.I
    ),
    "archives": re.compile(
        r"\b(?:archive|archives|archival|cold storage|long[- ]term storage)\b", re.I
    ),
    "legal_hold_records": re.compile(
        r"\b(?:legal hold|litigation hold|preservation hold|e[- ]?discovery|ediscovery)\b", re.I
    ),
}


@dataclass(frozen=True, slots=True)
class PlanDataRetentionImpactMatrixRow:
    """One lifecycle and retention impact row for an execution task."""

    task_id: str
    title: str
    impacted_data_class: ImpactedDataClass
    retention_domain: RetentionDomain
    required_decisions: tuple[str, ...] = field(default_factory=tuple)
    validation_recommendations: tuple[str, ...] = field(default_factory=tuple)
    severity: RetentionImpactSeverity = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "impacted_data_class": self.impacted_data_class,
            "retention_domain": self.retention_domain,
            "required_decisions": list(self.required_decisions),
            "validation_recommendations": list(self.validation_recommendations),
            "severity": self.severity,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanDataRetentionImpactMatrix:
    """Plan-level retention lifecycle impact matrix and rollup counts."""

    plan_id: str | None = None
    rows: tuple[PlanDataRetentionImpactMatrixRow, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return matrix rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the impact matrix as deterministic Markdown."""
        title = "# Plan Data Retention Impact Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('impact_count', 0)} impacts across "
                f"{self.summary.get('impacted_task_count', 0)} tasks "
                f"(high: {severity_counts.get('high', 0)}, "
                f"medium: {severity_counts.get('medium', 0)}, "
                f"low: {severity_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No data retention lifecycle impacts were detected."])
            if self.no_impact_task_ids:
                lines.extend(
                    ["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Severity | Data Class | Retention Domain | Required Decisions | Validation Recommendations | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` {_markdown_cell(row.title)} | "
                f"{row.severity} | "
                f"{row.impacted_data_class} | "
                f"{row.retention_domain} | "
                f"{_markdown_cell('; '.join(row.required_decisions) or 'none')} | "
                f"{_markdown_cell('; '.join(row.validation_recommendations) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(
                ["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"]
            )
        return "\n".join(lines)


def build_plan_data_retention_impact_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanDataRetentionImpactMatrix:
    """Build a retention lifecycle impact matrix for an execution plan."""
    plan_id, tasks = _source_payload(source)
    rows_by_task: list[tuple[str, tuple[PlanDataRetentionImpactMatrixRow, ...]]] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        rows_by_task.append((task_id, _task_rows(task, index)))

    rows = tuple(
        sorted(
            (row for _, task_rows in rows_by_task for row in task_rows),
            key=lambda row: (
                _SEVERITY_ORDER[row.severity],
                _DOMAIN_ORDER[row.retention_domain],
                _DATA_CLASS_ORDER[row.impacted_data_class],
                row.task_id,
                row.title.casefold(),
            ),
        )
    )
    impacted_task_ids = tuple(_dedupe(row.task_id for row in rows))
    no_impact_task_ids = tuple(task_id for task_id, task_rows in rows_by_task if not task_rows)
    severity_counts = {
        severity: sum(1 for row in rows if row.severity == severity) for severity in _SEVERITY_ORDER
    }
    retention_domain_counts = {
        domain: sum(1 for row in rows if row.retention_domain == domain) for domain in _DOMAIN_ORDER
    }
    data_class_counts = {
        data_class: sum(1 for row in rows if row.impacted_data_class == data_class)
        for data_class in _DATA_CLASS_ORDER
    }
    return PlanDataRetentionImpactMatrix(
        plan_id=plan_id,
        rows=rows,
        impacted_task_ids=impacted_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary={
            "task_count": len(tasks),
            "impact_count": len(rows),
            "impacted_task_count": len(impacted_task_ids),
            "no_impact_task_count": len(no_impact_task_ids),
            "severity_counts": severity_counts,
            "retention_domain_counts": retention_domain_counts,
            "data_class_counts": data_class_counts,
        },
    )


def summarize_plan_data_retention_impact_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanDataRetentionImpactMatrix:
    """Compatibility alias for building plan data retention impact matrices."""
    return build_plan_data_retention_impact_matrix(source)


def plan_data_retention_impact_matrix_to_dict(
    matrix: PlanDataRetentionImpactMatrix,
) -> dict[str, Any]:
    """Serialize a data retention impact matrix to a plain dictionary."""
    return matrix.to_dict()


plan_data_retention_impact_matrix_to_dict.__test__ = False


def plan_data_retention_impact_matrix_to_markdown(
    matrix: PlanDataRetentionImpactMatrix,
) -> str:
    """Render a data retention impact matrix as Markdown."""
    return matrix.to_markdown()


plan_data_retention_impact_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskImpactSignals:
    domains: tuple[RetentionDomain, ...]
    data_classes: tuple[ImpactedDataClass, ...]
    evidence: dict[str, tuple[str, ...]]


def _task_rows(task: Mapping[str, Any], index: int) -> tuple[PlanDataRetentionImpactMatrixRow, ...]:
    signals = _task_signals(task)
    if not signals.domains:
        return ()

    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    data_classes = signals.data_classes or _fallback_data_classes(signals.domains)
    rows: list[PlanDataRetentionImpactMatrixRow] = []
    for domain in signals.domains:
        data_class = _data_class_for_domain(domain, data_classes)
        severity = _severity(domain, data_class)
        rows.append(
            PlanDataRetentionImpactMatrixRow(
                task_id=task_id,
                title=title,
                impacted_data_class=data_class,
                retention_domain=domain,
                required_decisions=_required_decisions(domain, data_class),
                validation_recommendations=_validation_recommendations(domain, data_class),
                severity=severity,
                evidence=tuple(
                    _dedupe(signals.evidence.get(domain, ()) + signals.evidence.get(data_class, ()))
                ),
            )
        )
    return tuple(rows)


def _task_signals(task: Mapping[str, Any]) -> _TaskImpactSignals:
    domains: set[RetentionDomain] = set()
    data_classes: set[ImpactedDataClass] = set()
    evidence: dict[str, list[str]] = {}

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        snippet = f"files_or_modules: {path}"
        _match_domain_and_data_class(text, snippet, domains, data_classes, evidence)
        _match_path(normalized, snippet, domains, data_classes, evidence)

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        _match_domain_and_data_class(text, snippet, domains, data_classes, evidence)

    ordered_domains = tuple(domain for domain in _DOMAIN_ORDER if domain in domains)
    ordered_data_classes = tuple(
        data_class for data_class in _DATA_CLASS_ORDER if data_class in data_classes
    )
    return _TaskImpactSignals(
        domains=ordered_domains,
        data_classes=ordered_data_classes,
        evidence={key: tuple(_dedupe(values)) for key, values in evidence.items()},
    )


def _match_domain_and_data_class(
    text: str,
    snippet: str,
    domains: set[RetentionDomain],
    data_classes: set[ImpactedDataClass],
    evidence: dict[str, list[str]],
) -> None:
    for domain, pattern in _DOMAIN_PATTERNS.items():
        if pattern.search(text):
            domains.add(domain)
            evidence.setdefault(domain, []).append(snippet)
    for data_class, pattern in _DATA_CLASS_PATTERNS.items():
        if pattern.search(text):
            data_classes.add(data_class)
            evidence.setdefault(data_class, []).append(snippet)


def _match_path(
    normalized: str,
    snippet: str,
    domains: set[RetentionDomain],
    data_classes: set[ImpactedDataClass],
    evidence: dict[str, list[str]],
) -> None:
    path = PurePosixPath(normalized.casefold())
    parts = set(path.parts)
    name = path.name
    path_domains: set[RetentionDomain] = set()
    path_classes: set[ImpactedDataClass] = set()
    if {"retention", "ttl", "expiration", "expiry"} & parts:
        path_domains.add("retention_period")
    if {"delete", "deletion", "deletions", "erasure"} & parts or "delete" in name:
        path_domains.add("deletion")
    if {"purge", "purges", "cleanup", "sweeper", "janitor"} & parts or "purge" in name:
        path_domains.add("purge")
    if {"archive", "archives", "archival"} & parts or "archive" in name:
        path_domains.add("archive")
    if {"backup", "backups", "snapshots", "restore"} & parts or "backup" in name:
        path_domains.add("backup")
    if {"audit", "audits", "compliance"} & parts or "audit" in name:
        path_domains.add("audit_log")
        path_classes.add("audit_records")
    if {"analytics", "tracking", "events", "metrics"} & parts:
        path_domains.add("analytics_history")
        path_classes.add("analytics_events")
    if {"legal", "holds", "hold", "ediscovery"} & parts or "legal_hold" in name:
        path_domains.add("legal_hold")
        path_classes.add("legal_hold_records")
    if {"users", "accounts", "customers", "profiles", "identity"} & parts:
        path_domains.add("user_data_lifecycle")
        path_classes.add("user_data")
    if {"db", "database", "databases", "migrations", "schema", "models"} & parts:
        path_classes.add("database_records")
    if {"logs", "logging", "telemetry", "traces"} & parts or "log" in name:
        path_classes.add("operational_logs")
    if {"exports", "downloads", "files", "uploads", "attachments"} & parts:
        path_classes.add("files_and_exports")
    if path_domains or path_classes:
        domains.update(path_domains)
        data_classes.update(path_classes)
        for item in (*path_domains, *path_classes):
            evidence.setdefault(item, []).append(snippet)


def _fallback_data_classes(domains: tuple[RetentionDomain, ...]) -> tuple[ImpactedDataClass, ...]:
    mapped: list[ImpactedDataClass] = []
    for domain in domains:
        if domain == "backup":
            mapped.append("backups")
        elif domain == "archive":
            mapped.append("archives")
        elif domain == "audit_log":
            mapped.append("audit_records")
        elif domain == "analytics_history":
            mapped.append("analytics_events")
        elif domain == "legal_hold":
            mapped.append("legal_hold_records")
        else:
            mapped.append("database_records")
    return tuple(_dedupe(mapped))


def _data_class_for_domain(
    domain: RetentionDomain,
    data_classes: tuple[ImpactedDataClass, ...],
) -> ImpactedDataClass:
    preferred: dict[RetentionDomain, tuple[ImpactedDataClass, ...]] = {
        "backup": ("backups", "database_records", "user_data"),
        "archive": ("archives", "database_records", "files_and_exports"),
        "audit_log": ("audit_records", "operational_logs", "user_data"),
        "analytics_history": ("analytics_events", "user_data"),
        "legal_hold": ("legal_hold_records", "user_data", "personal_data"),
        "user_data_lifecycle": ("user_data", "personal_data", "database_records"),
        "deletion": ("personal_data", "user_data", "database_records"),
        "purge": ("personal_data", "user_data", "database_records"),
        "retention_period": ("personal_data", "user_data", "database_records"),
    }
    for data_class in preferred[domain]:
        if data_class in data_classes:
            return data_class
    return data_classes[0]


def _severity(domain: RetentionDomain, data_class: ImpactedDataClass) -> RetentionImpactSeverity:
    if domain in {"deletion", "purge", "legal_hold"}:
        return "high"
    if domain == "backup" and data_class in {
        "personal_data",
        "user_data",
        "database_records",
        "backups",
    }:
        return "high"
    if domain == "retention_period" and data_class in {"personal_data", "user_data"}:
        return "high"
    if domain in {
        "archive",
        "audit_log",
        "analytics_history",
        "retention_period",
        "user_data_lifecycle",
    }:
        return "medium"
    return "low"


def _required_decisions(domain: RetentionDomain, data_class: ImpactedDataClass) -> tuple[str, ...]:
    decisions = {
        "deletion": (
            "Define soft-delete, hard-delete, restore, and downstream propagation behavior.",
            "Identify who can approve deletion exceptions and recovery requests.",
        ),
        "purge": (
            "Set purge trigger, grace period, batch safety limits, and retry behavior.",
            "Decide how purge failures are escalated and reconciled.",
        ),
        "archive": (
            "Choose archive eligibility, storage location, access role, and restore path.",
            "Define when archived data becomes purge-eligible.",
        ),
        "backup": (
            "Define backup retention, restore windows, and deleted-data reintroduction controls.",
            "Document whether backups honor legal holds or deletion requests differently from primary stores.",
        ),
        "audit_log": (
            "Decide immutable fields, redaction boundaries, and retention period for audit evidence.",
            "Identify reviewers allowed to access retained audit records.",
        ),
        "analytics_history": (
            "Set aggregation, anonymization, and historical event retention rules.",
            "Decide whether user deletion removes, anonymizes, or detaches analytics history.",
        ),
        "legal_hold": (
            "Define legal-hold precedence over deletion, purge, archive, and backup expiration.",
            "Identify hold release authority and required audit evidence.",
        ),
        "retention_period": (
            "Set retention period, expiration trigger, and policy owner for the impacted data class.",
            "Decide customer, compliance, or operational exceptions to the default period.",
        ),
        "user_data_lifecycle": (
            "Define lifecycle states, deletion eligibility, restoration limits, and downstream synchronization.",
            "Decide user-visible status and support operations during lifecycle transitions.",
        ),
    }
    return (*decisions[domain], f"Confirm policy coverage for {data_class.replace('_', ' ')}.")


def _validation_recommendations(
    domain: RetentionDomain, data_class: ImpactedDataClass
) -> tuple[str, ...]:
    recommendations = {
        "deletion": (
            "Test deletion across primary records, derived records, search indexes, caches, exports, and background jobs.",
            "Verify deleted records cannot be restored outside the approved recovery window.",
        ),
        "purge": (
            "Run purge job dry-runs and fixture-based tests for expired, protected, failed, and already-purged records.",
            "Verify purge metrics, audit entries, and retry alerts.",
        ),
        "archive": (
            "Validate archive creation, access controls, restore flow, and archive-to-purge transition.",
        ),
        "backup": (
            "Run restore drills proving expired or deleted data is not silently reintroduced.",
            "Verify backup expiration and retention evidence are observable.",
        ),
        "audit_log": (
            "Verify audit records capture actor, timestamp, retention action, target data class, and policy version.",
            "Test redaction rules without breaking required audit history.",
        ),
        "analytics_history": (
            "Validate anonymization or detachment for deleted users and retention expiry for historical events.",
            "Check aggregates remain correct after event deletion or expiration.",
        ),
        "legal_hold": (
            "Test that held records are excluded from purge and backup expiration until hold release.",
            "Verify hold and release events are auditable.",
        ),
        "retention_period": (
            "Add boundary tests for records before, at, and after the retention deadline.",
            "Verify policy configuration changes are versioned and reviewed.",
        ),
        "user_data_lifecycle": (
            "Exercise account closure, deactivation, deletion, restoration, and downstream synchronization scenarios.",
            "Verify lifecycle state changes emit audit evidence and user/support-visible outcomes.",
        ),
    }
    return (
        *recommendations[domain],
        f"Attach validation evidence for {data_class.replace('_', ' ')}.",
    )


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
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "test_commands",
        "validation_command",
        "validation_commands",
        "status",
        "tags",
        "labels",
        "notes",
        "metadata",
        "blocked_reason",
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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes"):
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            elif key_text:
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
    "ImpactedDataClass",
    "PlanDataRetentionImpactMatrix",
    "PlanDataRetentionImpactMatrixRow",
    "RetentionDomain",
    "RetentionImpactSeverity",
    "build_plan_data_retention_impact_matrix",
    "plan_data_retention_impact_matrix_to_dict",
    "plan_data_retention_impact_matrix_to_markdown",
    "summarize_plan_data_retention_impact_matrix",
]
