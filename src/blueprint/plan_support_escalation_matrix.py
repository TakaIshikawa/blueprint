"""Summarize support escalation routing needs across execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SupportEscalationAudience = Literal[
    "end_users",
    "admins",
    "billing_contacts",
    "customer_success",
    "support_agents",
    "operations",
    "developers",
]
SupportEscalationTrigger = Literal[
    "user_visible_behavior_change",
    "admin_workflow_confusion",
    "billing_or_account_ticket",
    "migration_or_data_discrepancy",
    "support_tooling_change",
    "incident_or_rollback_escalation",
    "release_rollout_question",
]
SupportEscalationArtifact = Literal[
    "support_docs",
    "support_macros",
    "escalation_path",
    "operational_runbook",
    "customer_communication",
    "billing_faq",
    "admin_guide",
    "migration_guide",
]
SupportEscalationRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[SupportEscalationRisk, int] = {"high": 0, "medium": 1, "low": 2}
_AUDIENCE_ORDER: tuple[SupportEscalationAudience, ...] = (
    "billing_contacts",
    "admins",
    "end_users",
    "customer_success",
    "support_agents",
    "operations",
    "developers",
)
_TRIGGER_ORDER: tuple[SupportEscalationTrigger, ...] = (
    "billing_or_account_ticket",
    "migration_or_data_discrepancy",
    "incident_or_rollback_escalation",
    "admin_workflow_confusion",
    "user_visible_behavior_change",
    "support_tooling_change",
    "release_rollout_question",
)
_ARTIFACT_ORDER: tuple[SupportEscalationArtifact, ...] = (
    "support_docs",
    "support_macros",
    "escalation_path",
    "operational_runbook",
    "customer_communication",
    "billing_faq",
    "admin_guide",
    "migration_guide",
)

_USER_VISIBLE_RE = re.compile(
    r"\b(?:user[- ]facing|customer[- ]facing|customer[- ]visible|user[- ]visible|"
    r"end users?|customers?|dashboard|ui|ux|screen|page|form|workflow|checkout|"
    r"onboarding|notification|email|copy|message|self[- ]serve|release note)\b",
    re.I,
)
_ADMIN_RE = re.compile(
    r"\b(?:admin|administrator|tenant owner|workspace owner|operator|settings|"
    r"configuration|permission|permissions|role|roles|rbac|access control)\b",
    re.I,
)
_BILLING_RE = re.compile(
    r"\b(?:billing|invoice|invoices|payment|payments|subscription|subscriptions|"
    r"plan change|pricing|account plan|account changes?|seat|seats|trial|renewal|"
    r"refund|charge|checkout|tax|credits?|entitlement|entitlements?)\b",
    re.I,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrate|backfill|data migration|schema migration|existing customers?|"
    r"existing accounts?|cutover|import|export|reconcile|reconciliation|bulk update)\b",
    re.I,
)
_SUPPORT_TOOLING_RE = re.compile(
    r"\b(?:support tooling|support tool|support console|agent console|helpdesk|ticket|"
    r"tickets|zendesk|intercom|support macro|macro|canned response|knowledge base|kb)\b",
    re.I,
)
_INCIDENT_RE = re.compile(
    r"\b(?:incident|outage|degradation|rollback|roll back|kill switch|on-call|oncall|"
    r"pager|alert|sev[ -]?[0123]|launch watch|watch window|production risk|hotfix)\b",
    re.I,
)
_ROLLOUT_RE = re.compile(
    r"\b(?:rollout|roll out|launch|release|deploy|deployment|canary|gradual|"
    r"feature flag|flagged|beta|early access|customer communication|announce|announcement)\b",
    re.I,
)
_DEVELOPER_RE = re.compile(r"\b(?:api|sdk|webhook|endpoint|developer|integration)\b", re.I)
_PATH_RE = re.compile(
    r"(?:^|/)(?:app|web|ui|frontend|pages?|routes?|admin|billing|payments?|"
    r"support|helpdesk|ops|runbooks?|migrations?|api|sdk)(?:/|$)",
    re.I,
)

_ARTIFACT_PATTERNS: dict[SupportEscalationArtifact, re.Pattern[str]] = {
    "support_docs": re.compile(
        r"\b(?:support docs?|support guide|support documentation|help article|help center|"
        r"knowledge base|kb article|troubleshooting docs?|faq)\b",
        re.I,
    ),
    "support_macros": re.compile(
        r"\b(?:support macro|support macros|macro|macros|canned response|agent script|"
        r"support snippet|zendesk macro|intercom macro)\b",
        re.I,
    ),
    "escalation_path": re.compile(
        r"\b(?:escalation path|escalation route|escalate to|tier 2|tier two|tier 3|"
        r"tier three|support owner|engineering owner|triage owner|on-call|oncall)\b",
        re.I,
    ),
    "operational_runbook": re.compile(
        r"\b(?:runbook|operational runbook|ops runbook|rollback steps?|launch watch|"
        r"incident response|operator guide)\b",
        re.I,
    ),
    "customer_communication": re.compile(
        r"\b(?:customer communication|customer comms|announcement|announce|notify customers?|"
        r"customer email|release notes?|changelog|known issue)\b",
        re.I,
    ),
    "billing_faq": re.compile(r"\b(?:billing faq|invoice faq|pricing faq|refund faq)\b", re.I),
    "admin_guide": re.compile(r"\b(?:admin guide|administrator guide|permission matrix|rbac guide)\b", re.I),
    "migration_guide": re.compile(r"\b(?:migration guide|upgrade guide|cutover guide|backfill guide)\b", re.I),
}
_OWNER_KEY_RE = re.compile(r"\b(?:owner|dri|responsible|team|lead|oncall|on-call)\b", re.I)

_MISSING_ARTIFACT_TEXT: dict[SupportEscalationArtifact, str] = {
    "support_docs": "Support-facing documentation or troubleshooting notes are linked in acceptance criteria.",
    "support_macros": "Support macros, canned responses, or ticket snippets are prepared for likely launch questions.",
    "escalation_path": "Support escalation paths identify the owning team or on-call route for rollout issues.",
    "operational_runbook": "An operational runbook covers launch watch, rollback, and incident-prone support steps.",
    "customer_communication": "Customer-facing communication or release-note guidance is ready before rollout.",
    "billing_faq": "Billing/account FAQ content covers expected invoice, subscription, refund, or entitlement questions.",
    "admin_guide": "Admin workflow guidance covers configuration, permission, and role-management questions.",
    "migration_guide": "Migration or data reconciliation guidance covers existing-customer support questions.",
}


@dataclass(frozen=True, slots=True)
class PlanSupportEscalationRecord:
    """Support escalation routing for one support-relevant execution task."""

    task_id: str
    title: str
    affected_audience: tuple[SupportEscalationAudience, ...] = field(default_factory=tuple)
    likely_support_triggers: tuple[SupportEscalationTrigger, ...] = field(default_factory=tuple)
    required_enablement_artifacts: tuple[SupportEscalationArtifact, ...] = field(default_factory=tuple)
    escalation_owner_hints: tuple[str, ...] = field(default_factory=tuple)
    missing_acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    risk_level: SupportEscalationRisk = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "affected_audience": list(self.affected_audience),
            "likely_support_triggers": list(self.likely_support_triggers),
            "required_enablement_artifacts": list(self.required_enablement_artifacts),
            "escalation_owner_hints": list(self.escalation_owner_hints),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "evidence": list(self.evidence),
            "risk_level": self.risk_level,
        }


@dataclass(frozen=True, slots=True)
class PlanSupportEscalationMatrix:
    """Plan-level customer support escalation matrix."""

    plan_id: str | None = None
    records: tuple[PlanSupportEscalationRecord, ...] = field(default_factory=tuple)
    support_relevant_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "support_relevant_task_ids": list(self.support_relevant_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return escalation records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the escalation matrix as deterministic Markdown."""
        title = "# Plan Support Escalation Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Support-relevant task count: {self.summary.get('support_relevant_task_count', 0)}",
            f"- Missing acceptance criterion count: {self.summary.get('missing_acceptance_criterion_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No support escalation needs were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Audience | Triggers | Required Artifacts | Owner Hints | Missing Acceptance Criteria | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell('; '.join(record.affected_audience) or 'none')} | "
                f"{_markdown_cell('; '.join(record.likely_support_triggers) or 'none')} | "
                f"{_markdown_cell('; '.join(record.required_enablement_artifacts) or 'none')} | "
                f"{_markdown_cell('; '.join(record.escalation_owner_hints) or 'none')} | "
                f"{_markdown_cell('; '.join(record.missing_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_support_escalation_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanSupportEscalationMatrix:
    """Build a plan-level matrix of support escalation routing needs."""
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
            record.affected_audience,
            record.likely_support_triggers,
        )
    )
    result = tuple(records)
    risk_counts = {
        risk: sum(1 for record in result if record.risk_level == risk) for risk in _RISK_ORDER
    }
    audience_counts = {
        audience: sum(1 for record in result if audience in record.affected_audience)
        for audience in _AUDIENCE_ORDER
    }
    artifact_counts = {
        artifact: sum(1 for record in result if artifact in record.required_enablement_artifacts)
        for artifact in _ARTIFACT_ORDER
    }
    return PlanSupportEscalationMatrix(
        plan_id=plan_id,
        records=result,
        support_relevant_task_ids=tuple(record.task_id for record in result),
        summary={
            "task_count": len(tasks),
            "support_relevant_task_count": len(result),
            "missing_acceptance_criterion_count": sum(
                len(record.missing_acceptance_criteria) for record in result
            ),
            "risk_counts": risk_counts,
            "audience_counts": audience_counts,
            "artifact_counts": artifact_counts,
        },
    )


def summarize_plan_support_escalation_matrix(
    source_or_matrix: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanSupportEscalationMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanSupportEscalationMatrix:
    """Build the escalation matrix, accepting an existing matrix unchanged."""
    if isinstance(source_or_matrix, PlanSupportEscalationMatrix):
        return source_or_matrix
    return build_plan_support_escalation_matrix(source_or_matrix)


def plan_support_escalation_matrix_to_dict(
    matrix: PlanSupportEscalationMatrix,
) -> dict[str, Any]:
    """Serialize a support escalation matrix to a plain dictionary."""
    return matrix.to_dict()


plan_support_escalation_matrix_to_dict.__test__ = False


def plan_support_escalation_matrix_to_markdown(matrix: PlanSupportEscalationMatrix) -> str:
    """Render a support escalation matrix as Markdown."""
    return matrix.to_markdown()


plan_support_escalation_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    audiences: tuple[SupportEscalationAudience, ...] = field(default_factory=tuple)
    triggers: tuple[SupportEscalationTrigger, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    owner_hints: tuple[str, ...] = field(default_factory=tuple)


def _record(
    task: Mapping[str, Any],
    index: int,
    plan_context: tuple[tuple[str, str], ...],
) -> PlanSupportEscalationRecord | None:
    signals = _signals(task, plan_context)
    if not signals.triggers:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    artifacts = _required_artifacts(signals.triggers, signals.audiences)
    acceptance_context = _acceptance_context(task, plan_context)
    missing_artifacts = tuple(
        artifact for artifact in artifacts if not _ARTIFACT_PATTERNS[artifact].search(acceptance_context)
    )
    return PlanSupportEscalationRecord(
        task_id=task_id,
        title=title,
        affected_audience=signals.audiences,
        likely_support_triggers=signals.triggers,
        required_enablement_artifacts=artifacts,
        escalation_owner_hints=signals.owner_hints or _default_owner_hints(signals.triggers),
        missing_acceptance_criteria=tuple(_MISSING_ARTIFACT_TEXT[item] for item in missing_artifacts),
        evidence=signals.evidence,
        risk_level=_risk_level(signals.triggers, missing_artifacts),
    )


def _signals(task: Mapping[str, Any], plan_context: tuple[tuple[str, str], ...]) -> _Signals:
    audiences: list[SupportEscalationAudience] = []
    triggers: list[SupportEscalationTrigger] = []
    evidence: list[str] = []
    owner_hints: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        if _PATH_RE.search(normalized) or _any_signal(path_text):
            evidence.append(f"files_or_modules: {path}")
        _apply_text_signals(path_text, audiences, triggers)

    for source_field, text in (*_candidate_texts(task), *plan_context):
        owner_field = _OWNER_KEY_RE.search(source_field.replace("_", " "))
        if owner_field:
            owner_hints.extend(_owner_hints(source_field, text))
            continue
        if _coverage_only_field(source_field):
            continue
        before = len(triggers)
        _apply_text_signals(text, audiences, triggers)
        if len(triggers) > before or _any_signal(text):
            evidence.append(_evidence_snippet(source_field, text))
        owner_hints.extend(_owner_hints(source_field, text))

    triggers = _ordered_dedupe(triggers, _TRIGGER_ORDER)
    audiences = _ordered_dedupe(audiences or _audiences_for_triggers(triggers), _AUDIENCE_ORDER)
    return _Signals(
        audiences=tuple(audiences),
        triggers=tuple(triggers),
        evidence=tuple(_dedupe(evidence)),
        owner_hints=tuple(_dedupe(owner_hints)),
    )


def _apply_text_signals(
    text: str,
    audiences: list[SupportEscalationAudience],
    triggers: list[SupportEscalationTrigger],
) -> None:
    if _BILLING_RE.search(text):
        audiences.extend(["billing_contacts", "admins", "customer_success", "support_agents"])
        triggers.append("billing_or_account_ticket")
    if _MIGRATION_RE.search(text):
        audiences.extend(["end_users", "admins", "customer_success", "support_agents", "operations"])
        triggers.append("migration_or_data_discrepancy")
    if _INCIDENT_RE.search(text):
        audiences.extend(["support_agents", "operations"])
        triggers.append("incident_or_rollback_escalation")
    if _ADMIN_RE.search(text):
        audiences.extend(["admins", "support_agents"])
        triggers.append("admin_workflow_confusion")
    if _USER_VISIBLE_RE.search(text):
        audiences.extend(["end_users", "support_agents"])
        triggers.append("user_visible_behavior_change")
    if _SUPPORT_TOOLING_RE.search(text):
        audiences.extend(["support_agents", "operations"])
        triggers.append("support_tooling_change")
    if _ROLLOUT_RE.search(text):
        audiences.extend(["end_users", "customer_success", "support_agents", "operations"])
        triggers.append("release_rollout_question")
    if _DEVELOPER_RE.search(text):
        audiences.append("developers")


def _any_signal(text: str) -> bool:
    return any(
        pattern.search(text)
        for pattern in (
            _USER_VISIBLE_RE,
            _ADMIN_RE,
            _BILLING_RE,
            _MIGRATION_RE,
            _SUPPORT_TOOLING_RE,
            _INCIDENT_RE,
            _ROLLOUT_RE,
            _DEVELOPER_RE,
        )
    )


def _required_artifacts(
    triggers: tuple[SupportEscalationTrigger, ...],
    audiences: tuple[SupportEscalationAudience, ...],
) -> tuple[SupportEscalationArtifact, ...]:
    artifacts: list[SupportEscalationArtifact] = ["support_docs", "escalation_path"]
    if "user_visible_behavior_change" in triggers or "release_rollout_question" in triggers:
        artifacts.extend(["support_macros", "customer_communication"])
    if "billing_or_account_ticket" in triggers:
        artifacts.extend(["support_macros", "billing_faq"])
    if "admin_workflow_confusion" in triggers or "admins" in audiences:
        artifacts.append("admin_guide")
    if "migration_or_data_discrepancy" in triggers:
        artifacts.extend(["operational_runbook", "migration_guide"])
    if "incident_or_rollback_escalation" in triggers:
        artifacts.append("operational_runbook")
    if "support_tooling_change" in triggers:
        artifacts.append("support_macros")
    return tuple(_ordered_dedupe(artifacts, _ARTIFACT_ORDER))


def _risk_level(
    triggers: tuple[SupportEscalationTrigger, ...],
    missing_artifacts: tuple[SupportEscalationArtifact, ...],
) -> SupportEscalationRisk:
    if (
        "billing_or_account_ticket" in triggers
        or "migration_or_data_discrepancy" in triggers
        or "incident_or_rollback_escalation" in triggers
    ):
        return "high" if missing_artifacts else "medium"
    if len(triggers) >= 3 or len(missing_artifacts) >= 3:
        return "medium"
    return "low" if not missing_artifacts else "medium"


def _default_owner_hints(triggers: tuple[SupportEscalationTrigger, ...]) -> tuple[str, ...]:
    hints: list[str] = ["Support lead"]
    if "billing_or_account_ticket" in triggers:
        hints.extend(["Billing owner", "Customer Success"])
    if "migration_or_data_discrepancy" in triggers:
        hints.extend(["Data migration owner", "Operations"])
    if "incident_or_rollback_escalation" in triggers:
        hints.extend(["On-call engineer", "Incident commander"])
    if "admin_workflow_confusion" in triggers:
        hints.append("Admin workflow owner")
    if "user_visible_behavior_change" in triggers:
        hints.append("Product owner")
    if "support_tooling_change" in triggers:
        hints.append("Support operations")
    return tuple(_dedupe(hints))


def _audiences_for_triggers(
    triggers: tuple[SupportEscalationTrigger, ...],
) -> list[SupportEscalationAudience]:
    audiences: list[SupportEscalationAudience] = []
    for trigger in triggers:
        if trigger == "billing_or_account_ticket":
            audiences.extend(["billing_contacts", "admins", "customer_success", "support_agents"])
        elif trigger == "admin_workflow_confusion":
            audiences.extend(["admins", "support_agents"])
        elif trigger == "incident_or_rollback_escalation":
            audiences.extend(["support_agents", "operations"])
        elif trigger == "support_tooling_change":
            audiences.append("support_agents")
        else:
            audiences.extend(["end_users", "support_agents"])
    return audiences


def _acceptance_context(task: Mapping[str, Any], plan_context: tuple[tuple[str, str], ...]) -> str:
    values: list[str] = []
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "risk",
        "validation_commands",
        "test_command",
    ):
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
                    "support",
                    "macro",
                    "escalation",
                    "runbook",
                    "docs",
                    "faq",
                    "guide",
                    "communication",
                )
            ):
                values.append(text)
    values.extend(text for source_field, text in plan_context if _context_field_is_artifact(source_field))
    return " ".join(values)


def _context_field_is_artifact(source_field: str) -> bool:
    normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
    return any(
        keyword in normalized
        for keyword in (
            "acceptance",
            "definition",
            "support",
            "macro",
            "escalation",
            "runbook",
            "docs",
            "faq",
            "guide",
            "communication",
            "release",
        )
    )


def _coverage_only_field(source_field: str) -> bool:
    normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
    return normalized.startswith(
        (
            "acceptance_criteria",
            "criteria",
            "definition_of_done",
            "metadata.acceptance",
            "metadata.criteria",
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
    fields = (
        "target_engine",
        "target_repo",
        "project_type",
        "test_strategy",
        "handoff_prompt",
        "generation_prompt",
    )
    texts: list[tuple[str, str]] = []
    for field_name in fields:
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
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
                if _any_signal(key_text) and not _OWNER_KEY_RE.search(key_text):
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


def _owner_hints(source_field: str, text: str) -> list[str]:
    key = source_field.rsplit(".", maxsplit=1)[-1]
    key_text = key.replace("_", " ").replace("-", " ")
    if not (_OWNER_KEY_RE.search(source_field) or _OWNER_KEY_RE.search(key_text)):
        return []
    return [_clean_text(text)[:120].rstrip()]


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
    "PlanSupportEscalationArtifact",
    "PlanSupportEscalationMatrix",
    "PlanSupportEscalationRecord",
    "SupportEscalationAudience",
    "SupportEscalationRisk",
    "SupportEscalationTrigger",
    "build_plan_support_escalation_matrix",
    "plan_support_escalation_matrix_to_dict",
    "plan_support_escalation_matrix_to_markdown",
    "summarize_plan_support_escalation_matrix",
]
