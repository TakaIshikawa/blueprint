"""Build plan-level customer migration-window matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


MigrationSurface = Literal[
    "account_transition",
    "billing_or_subscription",
    "tenant_or_workspace",
    "identity_or_access",
    "api_or_integration",
    "data_or_content",
    "customer_experience",
]
MigrationWindowType = Literal[
    "maintenance_window",
    "customer_visible_migration_window",
    "beta_cohort",
    "staged_account_transition",
]
ReadinessGap = Literal[
    "customer_communication_plan",
    "scheduled_window_or_cohort",
    "rollback_checkpoint",
    "success_metrics_or_monitoring",
    "support_escalation_owner",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: dict[MigrationSurface, int] = {
    "account_transition": 0,
    "billing_or_subscription": 1,
    "tenant_or_workspace": 2,
    "identity_or_access": 3,
    "api_or_integration": 4,
    "data_or_content": 5,
    "customer_experience": 6,
}
_WINDOW_ORDER: dict[MigrationWindowType, int] = {
    "maintenance_window": 0,
    "customer_visible_migration_window": 1,
    "beta_cohort": 2,
    "staged_account_transition": 3,
}
_GAP_ORDER: tuple[ReadinessGap, ...] = (
    "customer_communication_plan",
    "scheduled_window_or_cohort",
    "rollback_checkpoint",
    "success_metrics_or_monitoring",
    "support_escalation_owner",
)
_PATH_SURFACE_PATTERNS: tuple[tuple[MigrationSurface, re.Pattern[str]], ...] = (
    ("account_transition", re.compile(r"(?:^|/)(?:accounts?|customers?|users?)(?:/|$)|account[-_]?migration", re.I)),
    ("billing_or_subscription", re.compile(r"(?:^|/)(?:billing|subscriptions?|plans?|invoices?)(?:/|$)", re.I)),
    ("tenant_or_workspace", re.compile(r"(?:^|/)(?:tenants?|workspaces?|orgs?|organizations?)(?:/|$)", re.I)),
    ("identity_or_access", re.compile(r"(?:^|/)(?:auth|identity|sso|login|permissions?|roles?)(?:/|$)", re.I)),
    ("api_or_integration", re.compile(r"(?:^|/)(?:api|integrations?|webhooks?|clients?|partners?)(?:/|$)", re.I)),
    ("data_or_content", re.compile(r"(?:^|/)(?:imports?|exports?|content|documents?|records?|migrations?)(?:/|$)", re.I)),
    ("customer_experience", re.compile(r"(?:^|/)(?:ui|frontend|onboarding|settings|notifications?)(?:/|$)", re.I)),
)
_TEXT_SURFACE_PATTERNS: tuple[tuple[MigrationSurface, re.Pattern[str]], ...] = (
    (
        "account_transition",
        re.compile(r"\b(?:account migration|customer migration|migrate customers?|account transition|customer transition)\b", re.I),
    ),
    (
        "billing_or_subscription",
        re.compile(r"\b(?:billing|subscription|pricing plan|plan migration|invoice|entitlement|seat migration)\b", re.I),
    ),
    (
        "tenant_or_workspace",
        re.compile(r"\b(?:tenant|workspace|organization|org migration|multi[- ]tenant|workspace move)\b", re.I),
    ),
    (
        "identity_or_access",
        re.compile(r"\b(?:sso|identity|authentication|authorization|login|permission|role migration|access migration)\b", re.I),
    ),
    (
        "api_or_integration",
        re.compile(r"\b(?:api migration|integration migration|partner migration|webhook migration|client migration)\b", re.I),
    ),
    (
        "data_or_content",
        re.compile(r"\b(?:data migration|content migration|record migration|import existing|export existing|move customer data)\b", re.I),
    ),
    (
        "customer_experience",
        re.compile(r"\b(?:customer[- ]visible|user[- ]visible|onboarding|customer experience|notification|settings migration)\b", re.I),
    ),
)
_MIGRATION_SIGNAL_RE = re.compile(
    r"\b(?:customer[- ]visible migration|customer migration|account migration|migration window|"
    r"maintenance window|beta cohort|cohort rollout|staged account transition|staged transition|"
    r"customer cutover|account cutover|tenant cutover|scheduled cutover|transition customers?|"
    r"migrate customers?|migrate accounts?|customer downtime|service downtime)\b",
    re.I,
)
_COMMUNICATION_RE = re.compile(
    r"\b(?:communicat|customer notice|notify customers?|email customers?|in[- ]app notice|status page|"
    r"release note|support script|customer success|csm|partner notice|announcement|invite)\b",
    re.I,
)
_SCHEDULE_RE = re.compile(
    r"\b(?:maintenance window|migration window|scheduled window|off[- ]hours|weekend|timezone|"
    r"date and time|start time|end time|cohort|beta|staged|wave|phase|pilot)\b",
    re.I,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|restore|revert|checkpoint|snapshot|backup|undo|abort criteria|"
    r"backout|fallback|return customers?)\b",
    re.I,
)
_MONITORING_RE = re.compile(
    r"\b(?:monitor|metric|success criteria|health check|smoke test|verification|audit|dashboard|alert|slo|sla)\b",
    re.I,
)
_SUPPORT_RE = re.compile(
    r"\b(?:support|escalation|runbook|on[- ]call|incident|help desk|customer success|csm|owner)\b",
    re.I,
)
_MAINTENANCE_RE = re.compile(r"\b(?:maintenance window|downtime|offline|service interruption|read[- ]only|freeze)\b", re.I)
_BETA_RE = re.compile(r"\b(?:beta cohort|beta customers?|pilot cohort|early access|limited cohort|cohort rollout)\b", re.I)
_STAGED_RE = re.compile(r"\b(?:staged|waves?|phased|batch(?:ed)?|per[- ]account|account[- ]by[- ]account|tenant[- ]by[- ]tenant)\b", re.I)


@dataclass(frozen=True, slots=True)
class PlanCustomerMigrationWindowRow:
    """Customer migration-window guidance for one execution-plan task."""

    task_id: str
    title: str
    migration_surface: MigrationSurface
    recommended_window_type: MigrationWindowType
    communication_needs: tuple[str, ...] = field(default_factory=tuple)
    rollback_checkpoint: str = "Define rollback checkpoint before implementation starts."
    readiness_gaps: tuple[ReadinessGap, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "migration_surface": self.migration_surface,
            "recommended_window_type": self.recommended_window_type,
            "communication_needs": list(self.communication_needs),
            "rollback_checkpoint": self.rollback_checkpoint,
            "readiness_gaps": list(self.readiness_gaps),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanCustomerMigrationWindowMatrix:
    """Plan-level customer migration-window matrix and rollup counts."""

    plan_id: str | None = None
    rows: tuple[PlanCustomerMigrationWindowRow, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "impacted_task_ids": list(self.impacted_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return migration-window rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the customer migration-window matrix as deterministic Markdown."""
        title = "# Plan Customer Migration Window Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        window_counts = self.summary.get("window_type_counts", {})
        surface_counts = self.summary.get("surface_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Impacted task count: {self.summary.get('impacted_task_count', 0)}",
            "- Window type counts: "
            + ", ".join(f"{window} {window_counts.get(window, 0)}" for window in _WINDOW_ORDER),
            "- Surface counts: "
            + ", ".join(f"{surface} {surface_counts.get(surface, 0)}" for surface in _SURFACE_ORDER),
        ]
        if not self.rows:
            lines.extend(["", "No customer migration-window signals were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Migration Surface | Recommended Window Type | Communication Needs | Rollback Checkpoint | Readiness Gaps | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.title)} | "
                f"{row.migration_surface} | "
                f"{row.recommended_window_type} | "
                f"{_markdown_cell('; '.join(row.communication_needs) or 'none')} | "
                f"{_markdown_cell(row.rollback_checkpoint)} | "
                f"{_markdown_cell('; '.join(row.readiness_gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_customer_migration_window_matrix(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanCustomerMigrationWindowMatrix:
    """Derive customer migration-window guidance from an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    milestones = _mapping_payloads(plan.get("milestones"))
    plan_risks = _mapping_payloads(plan.get("risks") or plan.get("risk_register"))
    rows = [
        row
        for index, task in enumerate(tasks, start=1)
        if (row := _row(task, index, milestones, plan_risks)) is not None
    ]
    rows.sort(
        key=lambda row: (
            _SURFACE_ORDER[row.migration_surface],
            _WINDOW_ORDER[row.recommended_window_type],
            row.task_id,
            row.title.casefold(),
        )
    )
    result = tuple(rows)
    window_counts = {
        window: sum(1 for row in result if row.recommended_window_type == window)
        for window in _WINDOW_ORDER
    }
    surface_counts = {
        surface: sum(1 for row in result if row.migration_surface == surface)
        for surface in _SURFACE_ORDER
    }

    return PlanCustomerMigrationWindowMatrix(
        plan_id=_optional_text(plan.get("id")),
        rows=result,
        impacted_task_ids=tuple(row.task_id for row in result),
        summary={
            "task_count": len(tasks),
            "impacted_task_count": len(result),
            "window_type_counts": window_counts,
            "surface_counts": surface_counts,
        },
    )


def summarize_plan_customer_migration_window(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanCustomerMigrationWindowMatrix:
    """Compatibility alias for building customer migration-window matrices."""
    return build_plan_customer_migration_window_matrix(source)


def plan_customer_migration_window_matrix_to_dict(
    matrix: PlanCustomerMigrationWindowMatrix,
) -> dict[str, Any]:
    """Serialize a customer migration-window matrix to a plain dictionary."""
    return matrix.to_dict()


plan_customer_migration_window_matrix_to_dict.__test__ = False


def plan_customer_migration_window_matrix_to_markdown(
    matrix: PlanCustomerMigrationWindowMatrix,
) -> str:
    """Render a customer migration-window matrix as Markdown."""
    return matrix.to_markdown()


plan_customer_migration_window_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[MigrationSurface, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    context: str = ""


def _row(
    task: Mapping[str, Any],
    index: int,
    milestones: list[dict[str, Any]],
    plan_risks: list[dict[str, Any]],
) -> PlanCustomerMigrationWindowRow | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task, task_id, title, milestones, plan_risks)
    if not signals.evidence:
        return None
    surface = signals.surfaces[0] if signals.surfaces else "account_transition"
    window_type = _recommended_window_type(surface, signals.context)
    return PlanCustomerMigrationWindowRow(
        task_id=task_id,
        title=title,
        migration_surface=surface,
        recommended_window_type=window_type,
        communication_needs=_communication_needs(surface, window_type, signals.context),
        rollback_checkpoint=_rollback_checkpoint(window_type, signals.context),
        readiness_gaps=_readiness_gaps(signals.context),
        evidence=signals.evidence,
    )


def _signals(
    task: Mapping[str, Any],
    task_id: str,
    title: str,
    milestones: list[dict[str, Any]],
    plan_risks: list[dict[str, Any]],
) -> _Signals:
    surfaces: set[MigrationSurface] = set()
    evidence: list[str] = []
    context_parts: list[str] = []
    paths = _strings(task.get("files_or_modules") or task.get("files"))
    for path in paths:
        normalized = _normalized_path(path)
        if not normalized:
            continue
        context_parts.append(path)
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for surface, pattern in _PATH_SURFACE_PATTERNS:
            if pattern.search(normalized) or pattern.search(path_text):
                surfaces.add(surface)
                matched = True
        if matched and _MIGRATION_SIGNAL_RE.search(path_text):
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        context_parts.append(text)
        for surface, pattern in _TEXT_SURFACE_PATTERNS:
            if pattern.search(text):
                surfaces.add(surface)
        if _MIGRATION_SIGNAL_RE.search(text):
            evidence.append(_evidence_snippet(source_field, text))

    task_milestone = _optional_text(task.get("milestone") or task.get("milestone_id"))
    for source_field, text in _associated_texts(milestones, task_id, task_milestone, "milestones"):
        context_parts.append(text)
        if _MIGRATION_SIGNAL_RE.search(text):
            evidence.append(_evidence_snippet(source_field, text))
        for surface, pattern in _TEXT_SURFACE_PATTERNS:
            if pattern.search(text):
                surfaces.add(surface)

    for source_field, text in _associated_texts(plan_risks, task_id, task_milestone, "risks"):
        context_parts.append(text)
        if _MIGRATION_SIGNAL_RE.search(text):
            evidence.append(_evidence_snippet(source_field, text))
        for surface, pattern in _TEXT_SURFACE_PATTERNS:
            if pattern.search(text):
                surfaces.add(surface)

    context = " ".join(context_parts)
    if surfaces and _MIGRATION_SIGNAL_RE.search(context) and not evidence:
        evidence.append("plan: customer migration-window surface detected")
    return _Signals(
        surfaces=tuple(surface for surface in _SURFACE_ORDER if surface in surfaces),
        evidence=tuple(_dedupe(evidence)),
        context=context,
    )


def _recommended_window_type(surface: MigrationSurface, context: str) -> MigrationWindowType:
    if _MAINTENANCE_RE.search(context):
        return "maintenance_window"
    if _BETA_RE.search(context):
        return "beta_cohort"
    if _STAGED_RE.search(context) or surface in {"account_transition", "tenant_or_workspace", "billing_or_subscription"}:
        return "staged_account_transition"
    return "customer_visible_migration_window"


def _communication_needs(
    surface: MigrationSurface,
    window_type: MigrationWindowType,
    context: str,
) -> tuple[str, ...]:
    needs = ["Customer notice with timing, impact, and expected completion criteria."]
    if window_type == "maintenance_window":
        needs.append("Status page or maintenance announcement before and during the window.")
    if window_type == "beta_cohort":
        needs.append("Beta cohort opt-in or invite copy with feedback and exit path.")
    if window_type == "staged_account_transition":
        needs.append("Account-by-account transition schedule shared with customer-facing teams.")
    if surface == "api_or_integration":
        needs.append("Partner or developer communication for integration timing and compatibility.")
    if surface == "billing_or_subscription":
        needs.append("Billing support brief covering invoice, entitlement, and plan changes.")
    if not _COMMUNICATION_RE.search(context):
        needs.append("Add an explicit communication owner and channel.")
    return tuple(_dedupe(needs))


def _rollback_checkpoint(window_type: MigrationWindowType, context: str) -> str:
    if _ROLLBACK_RE.search(context):
        if window_type == "beta_cohort":
            return "Rollback checkpoint documented for removing the cohort from the migrated experience."
        if window_type == "staged_account_transition":
            return "Rollback checkpoint documented before each account or cohort advances."
        return "Rollback checkpoint documented before the customer-visible window opens."
    if window_type == "maintenance_window":
        return "Define restore or abort checkpoint before the maintenance window opens."
    if window_type == "beta_cohort":
        return "Define exit criteria for returning beta customers to the stable path."
    if window_type == "staged_account_transition":
        return "Define per-account rollback checkpoint before advancing each stage."
    return "Define rollback checkpoint before customer-visible migration begins."


def _readiness_gaps(context: str) -> tuple[ReadinessGap, ...]:
    checks: dict[ReadinessGap, re.Pattern[str]] = {
        "customer_communication_plan": _COMMUNICATION_RE,
        "scheduled_window_or_cohort": _SCHEDULE_RE,
        "rollback_checkpoint": _ROLLBACK_RE,
        "success_metrics_or_monitoring": _MONITORING_RE,
        "support_escalation_owner": _SUPPORT_RE,
    }
    return tuple(gap for gap in _GAP_ORDER if not checks[gap].search(context))


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
    for field_name in ("acceptance_criteria", "criteria", "risks", "tags", "labels", "notes", "depends_on", "dependencies"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _associated_texts(
    records: list[dict[str, Any]],
    task_id: str,
    task_milestone: str | None,
    prefix: str,
) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for index, record in enumerate(records):
        flattened = " ".join(_strings(record))
        normalized = flattened.casefold()
        task_refs = set(_strings(record.get("tasks") or record.get("task_ids") or record.get("task_id")))
        record_key = _optional_text(record.get("id") or record.get("title") or record.get("name"))
        if (
            task_id in task_refs
            or task_id.casefold() in normalized
            or (task_milestone is not None and task_milestone == record_key)
        ):
            texts.append((f"{prefix}[{index}]", flattened))
    return texts


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if isinstance(plan, ExecutionPlan):
        return dict(plan.model_dump(mode="python"))
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
            tasks.append(dict(item.model_dump(mode="python")))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _mapping_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    return [dict(item) for item in items if isinstance(item, Mapping)]


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
                if _MIGRATION_SIGNAL_RE.search(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _MIGRATION_SIGNAL_RE.search(key_text):
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
    "MigrationSurface",
    "MigrationWindowType",
    "PlanCustomerMigrationWindowMatrix",
    "PlanCustomerMigrationWindowRow",
    "ReadinessGap",
    "build_plan_customer_migration_window_matrix",
    "plan_customer_migration_window_matrix_to_dict",
    "plan_customer_migration_window_matrix_to_markdown",
    "summarize_plan_customer_migration_window",
]
