"""Generate operational runbook requirements for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


OperationalSignal = Literal[
    "deploy",
    "migration",
    "incident_response",
    "alert",
    "on_call",
    "feature_flag",
    "backfill",
    "queue",
    "cron_job",
    "external_service",
    "rollback",
]
RunbookRequirementStatus = Literal["runbook_required", "no_runbook_needed"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_DEPLOY_RE = re.compile(
    r"\b(?:deploy|deployment|release|rollout|ship to production|prod release|"
    r"production rollout|canary|blue-green|blue green)\b",
    re.IGNORECASE,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrate|schema change|database change|ddl|alter table|"
    r"data migration|expand and contract)\b",
    re.IGNORECASE,
)
_INCIDENT_RE = re.compile(
    r"\b(?:incident|sev[ -]?[0-9]|postmortem|outage|degradation|hotfix|"
    r"emergency fix|customer impact)\b",
    re.IGNORECASE,
)
_ALERT_RE = re.compile(
    r"\b(?:alert|alerts|pager|page|paging|monitor|monitoring|slo|sla|"
    r"error budget|threshold|alarm)\b",
    re.IGNORECASE,
)
_ON_CALL_RE = re.compile(
    r"\b(?:on-call|on call|incident commander|primary responder|support rotation|"
    r"handoff|escalation policy)\b",
    re.IGNORECASE,
)
_FEATURE_FLAG_RE = re.compile(
    r"\b(?:feature flag|feature-flag|flagged rollout|kill switch|toggle|"
    r"launchdarkly|split.io|gradual rollout)\b",
    re.IGNORECASE,
)
_BACKFILL_RE = re.compile(
    r"\b(?:backfill|back-fill|reprocess|replay|bulk update|batch update|"
    r"data repair|historical data)\b",
    re.IGNORECASE,
)
_QUEUE_RE = re.compile(
    r"\b(?:queue|queues|worker|workers|job processor|background job|sidekiq|"
    r"celery|rq|kafka|sqs|pubsub|dead letter|dlq)\b",
    re.IGNORECASE,
)
_CRON_RE = re.compile(
    r"\b(?:cron|crontab|scheduled job|scheduler|periodic task|nightly job|"
    r"daily job)\b",
    re.IGNORECASE,
)
_EXTERNAL_SERVICE_RE = re.compile(
    r"\b(?:external service|external api|third party|third-party|vendor|partner|"
    r"webhook|integration|stripe|salesforce|slack api)\b",
    re.IGNORECASE,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|revert|restore|downgrade|disable flag|abort rollout|"
    r"undo migration)\b",
    re.IGNORECASE,
)
_SIGNAL_ORDER: dict[OperationalSignal, int] = {
    "deploy": 0,
    "migration": 1,
    "incident_response": 2,
    "alert": 3,
    "on_call": 4,
    "feature_flag": 5,
    "backfill": 6,
    "queue": 7,
    "cron_job": 8,
    "external_service": 9,
    "rollback": 10,
}


@dataclass(frozen=True, slots=True)
class TaskOperationalRunbookSections:
    """Actionable runbook checklist sections for one execution task."""

    pre_checks: tuple[str, ...] = field(default_factory=tuple)
    execution_steps: tuple[str, ...] = field(default_factory=tuple)
    monitoring: tuple[str, ...] = field(default_factory=tuple)
    rollback: tuple[str, ...] = field(default_factory=tuple)
    escalation: tuple[str, ...] = field(default_factory=tuple)
    post_checks: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "pre_checks": list(self.pre_checks),
            "execution_steps": list(self.execution_steps),
            "monitoring": list(self.monitoring),
            "rollback": list(self.rollback),
            "escalation": list(self.escalation),
            "post_checks": list(self.post_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskOperationalRunbook:
    """Operational runbook requirement for one execution task."""

    task_id: str
    title: str
    requirement_status: RunbookRequirementStatus
    operational_signals: tuple[OperationalSignal, ...] = field(default_factory=tuple)
    sections: TaskOperationalRunbookSections = field(default_factory=TaskOperationalRunbookSections)
    rationale: str = "No production operations runbook needed."
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def runbook_required(self) -> bool:
        """Return whether this task needs operational runbook guidance."""
        return self.requirement_status == "runbook_required"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "requirement_status": self.requirement_status,
            "runbook_required": self.runbook_required,
            "operational_signals": list(self.operational_signals),
            "sections": self.sections.to_dict(),
            "rationale": self.rationale,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskOperationalRunbookPlan:
    """Operational runbook requirements for a plan or task collection."""

    plan_id: str | None = None
    runbooks: tuple[TaskOperationalRunbook, ...] = field(default_factory=tuple)
    runbook_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_runbook_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "runbooks": [runbook.to_dict() for runbook in self.runbooks],
            "runbook_task_ids": list(self.runbook_task_ids),
            "no_runbook_task_ids": list(self.no_runbook_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return task runbook records as plain dictionaries."""
        return [runbook.to_dict() for runbook in self.runbooks]

    def to_markdown(self) -> str:
        """Render operational runbook requirements as deterministic Markdown."""
        title = "# Task Operational Runbook Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.runbooks:
            lines.extend(["", "No tasks were available for operational runbook planning."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Status | Signals | Pre-checks | Execution | Monitoring | Rollback | Escalation | Post-checks |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for runbook in self.runbooks:
            sections = runbook.sections
            lines.append(
                "| "
                f"`{_markdown_cell(runbook.task_id)}` | "
                f"{runbook.requirement_status} | "
                f"{_markdown_cell(', '.join(runbook.operational_signals) or 'none')} | "
                f"{_markdown_cell('; '.join(sections.pre_checks) or runbook.rationale)} | "
                f"{_markdown_cell('; '.join(sections.execution_steps) or 'none')} | "
                f"{_markdown_cell('; '.join(sections.monitoring) or 'none')} | "
                f"{_markdown_cell('; '.join(sections.rollback) or 'none')} | "
                f"{_markdown_cell('; '.join(sections.escalation) or 'none')} | "
                f"{_markdown_cell('; '.join(sections.post_checks) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_operational_runbook_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | None
    ),
) -> TaskOperationalRunbookPlan:
    """Build operational runbook requirements for production-affecting tasks."""
    plan_id, tasks = _source_payload(source)
    runbooks = tuple(_runbook(task, index) for index, task in enumerate(tasks, start=1))
    runbook_task_ids = tuple(
        runbook.task_id for runbook in runbooks if runbook.runbook_required
    )
    no_runbook_task_ids = tuple(
        runbook.task_id for runbook in runbooks if not runbook.runbook_required
    )
    signal_counts = {
        signal: sum(1 for runbook in runbooks if signal in runbook.operational_signals)
        for signal in _SIGNAL_ORDER
    }

    return TaskOperationalRunbookPlan(
        plan_id=plan_id,
        runbooks=runbooks,
        runbook_task_ids=runbook_task_ids,
        no_runbook_task_ids=no_runbook_task_ids,
        summary={
            "task_count": len(tasks),
            "runbook_task_count": len(runbook_task_ids),
            "no_runbook_task_count": len(no_runbook_task_ids),
            "signal_counts": signal_counts,
        },
    )


def task_operational_runbook_plan_to_dict(
    result: TaskOperationalRunbookPlan,
) -> dict[str, Any]:
    """Serialize an operational runbook plan to a plain dictionary."""
    return result.to_dict()


task_operational_runbook_plan_to_dict.__test__ = False


def task_operational_runbook_plan_to_markdown(
    result: TaskOperationalRunbookPlan,
) -> str:
    """Render an operational runbook plan as Markdown."""
    return result.to_markdown()


task_operational_runbook_plan_to_markdown.__test__ = False


def recommend_task_operational_runbooks(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
) -> TaskOperationalRunbookPlan:
    """Compatibility alias for building task operational runbook requirements."""
    return build_task_operational_runbook_plan(source)


def _runbook(task: Mapping[str, Any], index: int) -> TaskOperationalRunbook:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals:
        return TaskOperationalRunbook(
            task_id=task_id,
            title=title,
            requirement_status="no_runbook_needed",
        )

    operational_signals = tuple(sorted(signals, key=lambda signal: _SIGNAL_ORDER[signal]))
    evidence = tuple(_dedupe(item for signal in operational_signals for item in signals[signal]))
    sections = _sections(operational_signals)

    return TaskOperationalRunbook(
        task_id=task_id,
        title=title,
        requirement_status="runbook_required",
        operational_signals=operational_signals,
        sections=sections,
        rationale=f"Task affects production operations: {', '.join(operational_signals)}.",
        evidence=evidence,
    )


def _signals(task: Mapping[str, Any]) -> dict[OperationalSignal, tuple[str, ...]]:
    signals: dict[OperationalSignal, list[str]] = {}

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _add_path_signals(signals, path)
    for source_field, text in _task_texts(task):
        _add_text_signals(signals, source_field, text)
    for source_field, text in _metadata_texts(task.get("metadata")):
        _add_text_signals(signals, source_field, text)

    return {
        signal: tuple(_dedupe(evidence))
        for signal, evidence in signals.items()
        if evidence
    }


def _add_path_signals(
    signals: dict[OperationalSignal, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original)
    folded = normalized.casefold()
    if not folded:
        return
    path = PurePosixPath(folded)
    parts = set(path.parts)
    name = path.name
    evidence = f"files_or_modules: {original}"

    if bool({"deploy", "deploys", "deployment", "deployments", "release", "releases"} & parts):
        _append(signals, "deploy", evidence)
    if bool({"migrations", "migration", "schema"} & parts) or "migration" in name:
        _append(signals, "migration", evidence)
    if bool({"incidents", "incident", "hotfix", "runbooks"} & parts):
        _append(signals, "incident_response", evidence)
    if bool({"alerts", "alerting", "monitoring", "observability", "slo"} & parts):
        _append(signals, "alert", evidence)
    if "oncall" in parts or "on-call" in parts:
        _append(signals, "on_call", evidence)
    if bool({"flags", "feature_flags", "feature-flags", "toggles"} & parts) or "flag" in name:
        _append(signals, "feature_flag", evidence)
    if bool({"backfills", "backfill", "data_repairs"} & parts) or "backfill" in name:
        _append(signals, "backfill", evidence)
    if bool({"queues", "queue", "workers", "worker", "jobs"} & parts):
        _append(signals, "queue", evidence)
    if bool({"cron", "crons", "schedules", "scheduler", "scheduled"} & parts) or "cron" in name:
        _append(signals, "cron_job", evidence)
    if bool({"integrations", "vendors", "partners", "webhooks", "external"} & parts):
        _append(signals, "external_service", evidence)
    if bool({"rollback", "rollbacks", "revert"} & parts) or "rollback" in name:
        _append(signals, "rollback", evidence)


def _add_text_signals(
    signals: dict[OperationalSignal, list[str]],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    if _DEPLOY_RE.search(text):
        _append(signals, "deploy", evidence)
    if _MIGRATION_RE.search(text):
        _append(signals, "migration", evidence)
    if _INCIDENT_RE.search(text):
        _append(signals, "incident_response", evidence)
    if _ALERT_RE.search(text):
        _append(signals, "alert", evidence)
    if _ON_CALL_RE.search(text):
        _append(signals, "on_call", evidence)
    if _FEATURE_FLAG_RE.search(text):
        _append(signals, "feature_flag", evidence)
    if _BACKFILL_RE.search(text):
        _append(signals, "backfill", evidence)
    if _QUEUE_RE.search(text):
        _append(signals, "queue", evidence)
    if _CRON_RE.search(text):
        _append(signals, "cron_job", evidence)
    if _EXTERNAL_SERVICE_RE.search(text):
        _append(signals, "external_service", evidence)
    if _ROLLBACK_RE.search(text):
        _append(signals, "rollback", evidence)


def _sections(signals: tuple[OperationalSignal, ...]) -> TaskOperationalRunbookSections:
    pre_checks: list[str] = [
        "Confirm the production change owner, execution window, and go/no-go decision point.",
    ]
    execution_steps: list[str] = [
        "Execute the change in small, observable steps and record timestamps for each step.",
    ]
    monitoring: list[str] = [
        "Watch service health, error rate, latency, and business-critical success metrics during execution.",
    ]
    rollback: list[str] = [
        "Define the rollback trigger, responsible owner, and exact command or change used to restore service.",
    ]
    escalation: list[str] = [
        "Identify the on-call contact, escalation channel, and decision maker for pausing or reverting.",
    ]
    post_checks: list[str] = [
        "Verify user-facing behavior, logs, and dashboards after the change is complete.",
    ]

    for signal in signals:
        additions = _section_items_for_signal(signal)
        pre_checks.extend(additions["pre_checks"])
        execution_steps.extend(additions["execution_steps"])
        monitoring.extend(additions["monitoring"])
        rollback.extend(additions["rollback"])
        escalation.extend(additions["escalation"])
        post_checks.extend(additions["post_checks"])

    return TaskOperationalRunbookSections(
        pre_checks=tuple(_dedupe(pre_checks)),
        execution_steps=tuple(_dedupe(execution_steps)),
        monitoring=tuple(_dedupe(monitoring)),
        rollback=tuple(_dedupe(rollback)),
        escalation=tuple(_dedupe(escalation)),
        post_checks=tuple(_dedupe(post_checks)),
    )


def _section_items_for_signal(signal: OperationalSignal) -> dict[str, tuple[str, ...]]:
    return {
        "deploy": {
            "pre_checks": ("Confirm the target environment, build artifact, deployment method, and freeze window.",),
            "execution_steps": ("Deploy to the first production slice or canary before widening exposure.",),
            "monitoring": ("Compare canary and baseline metrics before continuing rollout.",),
            "rollback": ("Keep the previous artifact or release version available for immediate redeploy.",),
            "escalation": ("Notify the release owner before expanding beyond the initial rollout slice.",),
            "post_checks": ("Confirm the deployed version is active on all intended production targets.",),
        },
        "migration": {
            "pre_checks": ("Verify backups, migration lock behavior, expected runtime, and schema compatibility.",),
            "execution_steps": ("Run migrations in the approved order and capture row counts or schema versions.",),
            "monitoring": ("Monitor database load, lock waits, error rates, and application compatibility.",),
            "rollback": ("Document whether the migration is reversible and the restore or forward-fix path.",),
            "escalation": ("Have the database owner available until migration post-checks pass.",),
            "post_checks": ("Verify schema version, application reads and writes, and affected data counts.",),
        },
        "incident_response": {
            "pre_checks": ("Confirm incident severity, current mitigations, affected users, and communication owner.",),
            "execution_steps": ("Apply the mitigation with an incident timeline entry for each action.",),
            "monitoring": ("Track recovery metrics and customer-impact indicators until they return to baseline.",),
            "rollback": ("Define when to stop the mitigation and restore the pre-incident behavior.",),
            "escalation": ("Keep incident command and stakeholder channels updated during execution.",),
            "post_checks": ("Record residual risk, follow-up owners, and whether incident status can be downgraded.",),
        },
        "alert": {
            "pre_checks": ("Confirm alert thresholds, routing, mute windows, and dashboard links before rollout.",),
            "execution_steps": ("Apply alert changes with routing enabled only after threshold review.",),
            "monitoring": ("Watch for alert floods, missing pages, and signal-to-noise regressions.",),
            "rollback": ("Keep the previous alert rule or routing policy ready to restore.",),
            "escalation": ("Confirm the receiving on-call rotation knows the new alert behavior.",),
            "post_checks": ("Trigger or simulate the alert path and verify the expected notification target.",),
        },
        "on_call": {
            "pre_checks": ("Confirm primary, secondary, and escalation contacts for the execution window.",),
            "execution_steps": ("Announce start and completion in the operational coordination channel.",),
            "monitoring": ("Watch the on-call queue for pages or support escalations caused by the change.",),
            "rollback": ("Give the on-call owner authority to pause or revert when impact exceeds thresholds.",),
            "escalation": ("Document the escalation chain and response-time expectations.",),
            "post_checks": ("Hand off current state, known risks, and open follow-ups to the next responder.",),
        },
        "feature_flag": {
            "pre_checks": ("Confirm flag default, targeting rules, owners, and kill-switch behavior.",),
            "execution_steps": ("Increase flag exposure gradually and pause between increments for health checks.",),
            "monitoring": ("Compare flagged and unflagged cohorts for errors, latency, and conversion changes.",),
            "rollback": ("Document the exact flag setting that disables the change immediately.",),
            "escalation": ("Ensure product and operations owners agree on exposure expansion criteria.",),
            "post_checks": ("Confirm final flag state, cleanup owner, and any stale flag removal task.",),
        },
        "backfill": {
            "pre_checks": ("Estimate affected records, batching, idempotency, and load limits before starting.",),
            "execution_steps": ("Run the backfill in bounded batches with checkpoints and resumable state.",),
            "monitoring": ("Monitor throughput, failed records, queue depth, database load, and user-visible errors.",),
            "rollback": ("Define how to stop the backfill and repair or revert partially processed records.",),
            "escalation": ("Have data and service owners available while batches are running.",),
            "post_checks": ("Reconcile processed, skipped, failed, and expected record counts.",),
        },
        "queue": {
            "pre_checks": ("Confirm queue names, worker capacity, retry policy, and dead-letter handling.",),
            "execution_steps": ("Drain, pause, or scale workers according to the planned queue change sequence.",),
            "monitoring": ("Watch queue depth, processing latency, retries, dead letters, and worker errors.",),
            "rollback": ("Keep the prior worker version and queue routing available for restore.",),
            "escalation": ("Identify the owner who can pause producers or consumers when backlog grows.",),
            "post_checks": ("Verify backlog recovery and absence of stuck or duplicate jobs.",),
        },
        "cron_job": {
            "pre_checks": ("Confirm schedule, timezone, concurrency, and missed-run behavior.",),
            "execution_steps": ("Apply schedule changes and trigger a controlled run when appropriate.",),
            "monitoring": ("Watch the next scheduled execution, duration, exit status, and downstream side effects.",),
            "rollback": ("Keep the previous schedule or disabled state documented for quick restore.",),
            "escalation": ("Identify who can disable the job if it loops, overlaps, or overloads dependencies.",),
            "post_checks": ("Verify the scheduler registered the expected next run time.",),
        },
        "external_service": {
            "pre_checks": ("Confirm vendor status, credentials, rate limits, timeouts, and fallback behavior.",),
            "execution_steps": ("Roll out external-service changes behind guarded retries and timeouts.",),
            "monitoring": ("Monitor vendor error rates, timeout rates, retry volume, and integration-specific alerts.",),
            "rollback": ("Document how to disable the integration or restore the previous endpoint or credentials.",),
            "escalation": ("Record vendor support contact and internal integration owner.",),
            "post_checks": ("Verify successful requests, failure handling, and absence of unexpected vendor-side errors.",),
        },
        "rollback": {
            "pre_checks": ("Confirm rollback has been tested or rehearsed for the affected component.",),
            "execution_steps": ("Prepare rollback materials before making the forward change.",),
            "monitoring": ("Monitor rollback-readiness indicators alongside forward-change health metrics.",),
            "rollback": ("List the exact rollback steps, expected duration, and validation checks.",),
            "escalation": ("Define who can authorize rollback when thresholds are breached.",),
            "post_checks": ("If rollback is used, verify service recovery and document any data reconciliation needed.",),
        },
    }[signal]


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | None
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
    if hasattr(source, "tasks"):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)
    except TypeError:
        task = _task_like_payload(source)
        return (None, [task]) if task else (None, [])

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_like_payload(item):
            tasks.append(task)
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
        if task := _task_like_payload(item):
            tasks.append(task)
    return tasks


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
        "title",
        "description",
        "milestone",
        "owner_type",
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


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "test_command",
        "risk_level",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    for index, text in enumerate(_strings(task.get("tags"))):
        texts.append((f"tags[{index}]", text))
    for index, text in enumerate(_strings(task.get("labels"))):
        texts.append((f"labels[{index}]", text))
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
    signals: dict[OperationalSignal, list[str]],
    signal: OperationalSignal,
    evidence: str,
) -> None:
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
    "OperationalSignal",
    "RunbookRequirementStatus",
    "TaskOperationalRunbook",
    "TaskOperationalRunbookPlan",
    "TaskOperationalRunbookSections",
    "build_task_operational_runbook_plan",
    "recommend_task_operational_runbooks",
    "task_operational_runbook_plan_to_dict",
    "task_operational_runbook_plan_to_markdown",
]
