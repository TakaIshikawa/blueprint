"""Plan observability readiness work for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


ObservabilitySignal = Literal[
    "observability",
    "structured_logging",
    "metrics",
    "tracing",
    "dashboard",
    "alerts",
    "runbook",
    "verification",
    "api_context",
    "background_job_context",
]
ObservabilityReadinessCategory = Literal[
    "structured_logging",
    "metrics",
    "distributed_tracing",
    "dashboard",
    "alerting",
    "runbook",
    "verification",
]
ObservabilityReadinessLevel = Literal["needs_planning", "partial", "ready"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[ObservabilityReadinessLevel, int] = {
    "needs_planning": 0,
    "partial": 1,
    "ready": 2,
}
_SIGNAL_ORDER: tuple[ObservabilitySignal, ...] = (
    "observability",
    "structured_logging",
    "metrics",
    "tracing",
    "dashboard",
    "alerts",
    "runbook",
    "verification",
    "api_context",
    "background_job_context",
)
_CATEGORY_ORDER: tuple[ObservabilityReadinessCategory, ...] = (
    "structured_logging",
    "metrics",
    "distributed_tracing",
    "dashboard",
    "alerting",
    "runbook",
    "verification",
)
_SIGNAL_PATTERNS: dict[ObservabilitySignal, re.Pattern[str]] = {
    "observability": re.compile(r"\b(?:observability|monitoring|telemetry|instrument(?:ation|ed)?)\b", re.I),
    "structured_logging": re.compile(
        r"\b(?:structured logs?|json logs?|logs?|log fields?|logging|correlation id|request id|trace id|job id|audit log)\b",
        re.I,
    ),
    "metrics": re.compile(
        r"\b(?:metrics?|counter|histogram|gauge|latency|duration|throughput|slo|success rate|queue depth)\b",
        re.I,
    ),
    "tracing": re.compile(
        r"\b(?:traces?|tracing|distributed tracing|span|opentelemetry|otel|trace propagation|parent span)\b",
        re.I,
    ),
    "dashboard": re.compile(r"\b(?:dashboards?|grafana|datadog|new relic|chart|panel|service overview)\b", re.I),
    "alerts": re.compile(
        r"\b(?:alerts?|alerting|pagerduty|page|thresholds?|alarm|notify|notification|error budget|on-call)\b",
        re.I,
    ),
    "runbook": re.compile(r"\b(?:runbooks?|playbooks?|response steps?|triage guide|escalation|owner)\b", re.I),
    "verification": re.compile(
        r"\b(?:verify|verification|validate|validation|test|smoke test|post[- ]deploy|launch watch|synthetic)\b",
        re.I,
    ),
    "api_context": re.compile(
        r"\b(?:api|endpoint|http|rest|graphql|request|response|status code|handler|controller)\b",
        re.I,
    ),
    "background_job_context": re.compile(
        r"\b(?:background job|worker|queue|consumer|cron|scheduled job|batch job|async job|retry|dead letter|dlq)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[ObservabilitySignal, re.Pattern[str]] = {
    "observability": re.compile(r"(?:observability|monitoring|telemetry|instrumentation)", re.I),
    "structured_logging": re.compile(r"(?:log|logging|logger|audit)", re.I),
    "metrics": re.compile(r"(?:metric|prometheus|statsd|slo)", re.I),
    "tracing": re.compile(r"(?:trac|otel|opentelemetry)", re.I),
    "dashboard": re.compile(r"(?:dashboard|grafana|datadog|new[_-]?relic)", re.I),
    "alerts": re.compile(r"(?:alert|pager|alarm)", re.I),
    "runbook": re.compile(r"(?:runbook|playbook)", re.I),
    "verification": re.compile(r"(?:smoke|validation|synthetic|launch[_-]?watch)", re.I),
    "api_context": re.compile(r"(?:api|endpoint|routes?|handlers?|controllers?|graphql)", re.I),
    "background_job_context": re.compile(r"(?:workers?|jobs?|queues?|consumers?|cron|batch|dlq)", re.I),
}
_NO_OBSERVABILITY_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:observability|monitoring|telemetry|logs?|metrics?|traces?|alerts?|dashboards?)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|requirements?)\b",
    re.I,
)
_CATEGORY_SIGNAL_MAP: dict[ObservabilityReadinessCategory, ObservabilitySignal] = {
    "structured_logging": "structured_logging",
    "metrics": "metrics",
    "distributed_tracing": "tracing",
    "dashboard": "dashboard",
    "alerting": "alerts",
    "runbook": "runbook",
    "verification": "verification",
}


@dataclass(frozen=True, slots=True)
class ObservabilityReadinessTask:
    """One generated implementation task for observability readiness."""

    category: ObservabilityReadinessCategory
    title: str
    description: str
    acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    verification_steps: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "title": self.title,
            "description": self.description,
            "acceptance_criteria": list(self.acceptance_criteria),
            "verification_steps": list(self.verification_steps),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskObservabilityReadinessRecord:
    """Observability readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[ObservabilitySignal, ...]
    context: Literal["api", "background_job", "generic"] = "generic"
    generated_tasks: tuple[ObservabilityReadinessTask, ...] = field(default_factory=tuple)
    readiness: ObservabilityReadinessLevel = "needs_planning"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[ObservabilitySignal, ...]:
        """Compatibility view for planners that name detected signals matched signals."""
        return self.detected_signals

    @property
    def recommended_tasks(self) -> tuple[ObservabilityReadinessTask, ...]:
        """Compatibility view for generated readiness tasks."""
        return self.generated_tasks

    @property
    def acceptance_criteria(self) -> tuple[str, ...]:
        """Flatten generated task acceptance criteria for simple consumers."""
        return tuple(criteria for task in self.generated_tasks for criteria in task.acceptance_criteria)

    @property
    def verification_steps(self) -> tuple[str, ...]:
        """Flatten generated task verification steps for simple consumers."""
        return tuple(step for task in self.generated_tasks for step in task.verification_steps)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "context": self.context,
            "generated_tasks": [task.to_dict() for task in self.generated_tasks],
            "readiness": self.readiness,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskObservabilityReadinessPlan:
    """Plan-level observability readiness tasks."""

    plan_id: str | None = None
    records: tuple[TaskObservabilityReadinessRecord, ...] = field(default_factory=tuple)
    observability_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskObservabilityReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskObservabilityReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    @property
    def impacted_task_ids(self) -> tuple[str, ...]:
        """Compatibility view matching planners that expose impacted task ids."""
        return self.observability_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "observability_task_ids": list(self.observability_task_ids),
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return observability readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render observability readiness guidance as deterministic Markdown."""
        title = "# Task Observability Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        category_counts = self.summary.get("generated_task_category_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Observability task count: {self.summary.get('observability_task_count', 0)}",
            f"- Generated readiness task count: {self.summary.get('generated_task_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
            "- Generated task counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task observability readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Context | Signals | Generated Tasks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            generated = "; ".join(f"{task.category}: {task.title}" for task in record.generated_tasks)
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{record.context} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(generated or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_observability_readiness_plan(source: Any) -> TaskObservabilityReadinessPlan:
    """Build observability readiness records for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                -len(record.generated_tasks),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    observability_task_ids = tuple(record.task_id for record in records)
    impacted = set(observability_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted
    )
    return TaskObservabilityReadinessPlan(
        plan_id=plan_id,
        records=records,
        observability_task_ids=observability_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_observability_readiness(source: Any) -> TaskObservabilityReadinessPlan:
    """Compatibility alias for building observability readiness plans."""
    return build_task_observability_readiness_plan(source)


def recommend_task_observability_readiness(source: Any) -> TaskObservabilityReadinessPlan:
    """Compatibility alias for recommending observability readiness tasks."""
    return build_task_observability_readiness_plan(source)


def summarize_task_observability_readiness(source: Any) -> TaskObservabilityReadinessPlan:
    """Build an observability readiness plan, accepting an existing plan unchanged."""
    if isinstance(source, TaskObservabilityReadinessPlan):
        return source
    return build_task_observability_readiness_plan(source)


def generate_task_observability_readiness(source: Any) -> TaskObservabilityReadinessPlan:
    """Compatibility alias for generating observability readiness plans."""
    return build_task_observability_readiness_plan(source)


def extract_task_observability_readiness(source: Any) -> TaskObservabilityReadinessPlan:
    """Compatibility alias for extracting observability readiness plans."""
    return build_task_observability_readiness_plan(source)


def derive_task_observability_readiness(source: Any) -> TaskObservabilityReadinessPlan:
    """Compatibility alias for deriving observability readiness plans."""
    return build_task_observability_readiness_plan(source)


def task_observability_readiness_plan_to_dict(result: TaskObservabilityReadinessPlan) -> dict[str, Any]:
    """Serialize an observability readiness plan to a plain dictionary."""
    return result.to_dict()


task_observability_readiness_plan_to_dict.__test__ = False


def task_observability_readiness_plan_to_dicts(
    result: TaskObservabilityReadinessPlan | Iterable[TaskObservabilityReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize observability readiness records to plain dictionaries."""
    if isinstance(result, TaskObservabilityReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_observability_readiness_plan_to_dicts.__test__ = False
task_observability_readiness_to_dicts = task_observability_readiness_plan_to_dicts
task_observability_readiness_to_dicts.__test__ = False


def task_observability_readiness_plan_to_markdown(result: TaskObservabilityReadinessPlan) -> str:
    """Render an observability readiness plan as Markdown."""
    return result.to_markdown()


task_observability_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[ObservabilitySignal, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _task_record(task: Mapping[str, Any], index: int) -> TaskObservabilityReadinessRecord | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not _has_observability_requirement(signals.signals):
        return None

    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    context = _context(signals.signals)
    generated_tasks = _generated_tasks(title, signals, context)
    return TaskObservabilityReadinessRecord(
        task_id=task_id,
        title=title,
        detected_signals=signals.signals,
        context=context,
        generated_tasks=generated_tasks,
        readiness=_readiness(signals.signals, generated_tasks),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[ObservabilitySignal] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(
        task.get("files_or_modules")
        or task.get("files")
        or task.get("expected_file_paths")
        or task.get("expected_files")
        or task.get("paths")
    ):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or _SIGNAL_PATTERNS[signal].search(searchable):
                signal_hits.add(signal)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        if _NO_OBSERVABILITY_RE.search(text):
            explicitly_no_impact = True
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    if signal_hits & {
        "structured_logging",
        "metrics",
        "tracing",
        "dashboard",
        "alerts",
        "runbook",
        "verification",
    }:
        signal_hits.add("observability")

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _generated_tasks(
    source_title: str,
    signals: _Signals,
    context: Literal["api", "background_job", "generic"],
) -> tuple[ObservabilityReadinessTask, ...]:
    categories = _categories_for_signals(signals.signals)
    evidence = tuple(sorted(signals.evidence, key=_evidence_priority))[:3]
    rationale = "; ".join(evidence) if evidence else "Observability-related task context was detected."
    tasks: list[ObservabilityReadinessTask] = []
    for category in _CATEGORY_ORDER:
        if category not in categories:
            continue
        title = f"{_category_title(category)} for {source_title}"
        description, acceptance, verification = _category_guidance(category, context)
        tasks.append(
            ObservabilityReadinessTask(
                category=category,
                title=title,
                description=f"{description} Rationale: {rationale}",
                acceptance_criteria=acceptance,
                verification_steps=verification,
                evidence=evidence,
            )
        )
    return tuple(tasks)


def _categories_for_signals(signals: tuple[ObservabilitySignal, ...]) -> set[ObservabilityReadinessCategory]:
    if "observability" in signals and not any(_CATEGORY_SIGNAL_MAP[category] in signals for category in _CATEGORY_ORDER):
        return set(_CATEGORY_ORDER)
    categories = {category for category, signal in _CATEGORY_SIGNAL_MAP.items() if signal in signals}
    if "alerts" in signals:
        categories.update({"alerting", "runbook", "verification"})
    if categories & {"structured_logging", "metrics", "distributed_tracing", "dashboard", "alerting"}:
        categories.add("verification")
    return categories


def _category_guidance(
    category: ObservabilityReadinessCategory,
    context: Literal["api", "background_job", "generic"],
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    subject = {
        "api": "API endpoint",
        "background_job": "background job",
        "generic": "implementation path",
    }[context]
    if category == "structured_logging":
        return (
            f"Add structured logging to the {subject} with stable fields for correlation and failure triage.",
            (
                f"Logs include a stable operation name for the {subject}.",
                "Logs include correlation identifiers such as request_id, trace_id, tenant_id, or job_id where available.",
                "Error logs include sanitized failure reason, status/outcome, and retryability without leaking sensitive payloads.",
            ),
            (
                "Exercise a success path and a failure path and inspect emitted log fields.",
                "Confirm log records can be filtered by the correlation identifier named in the task.",
            ),
        )
    if category == "metrics":
        return (
            f"Instrument metrics for the {subject} so volume, latency, errors, and saturation can be tracked.",
            (
                f"Metrics include count, success/failure count, and duration for the {subject}.",
                "Metric names, labels, and units are documented and avoid high-cardinality payload values.",
                "Metrics distinguish expected validation failures from unexpected system failures where relevant.",
            ),
            (
                "Run the relevant unit, integration, or smoke path and confirm metrics are emitted.",
                "Verify metric labels stay bounded for representative inputs.",
            ),
        )
    if category == "distributed_tracing":
        return (
            f"Propagate distributed tracing through the {subject} and its downstream calls.",
            (
                f"A span is created or continued for the {subject} with useful operation attributes.",
                "Trace context is propagated to downstream HTTP, queue, database, or service calls where applicable.",
                "Errors and retries are annotated on spans with sanitized metadata.",
            ),
            (
                "Generate a representative request or job run and confirm the trace contains the expected span chain.",
                "Confirm failure traces link logs and metrics through trace_id or equivalent correlation fields.",
            ),
        )
    if category == "dashboard":
        return (
            f"Create or update a dashboard for the {subject}'s operational health.",
            (
                "Dashboard panels show traffic or execution volume, latency/duration, error rate, and saturation/backlog where relevant.",
                "Dashboard links to the owning service, deployment environment, and related logs/traces.",
                "Dashboard is discoverable from the service or team observability index.",
            ),
            (
                "Open the dashboard in a non-production or production-safe environment and confirm panels populate.",
                "Validate dashboard filters select the task's service, endpoint, queue, or job name.",
            ),
        )
    if category == "alerting":
        return (
            f"Define alert thresholds for the {subject} before rollout.",
            (
                "Alerts cover elevated error rate, latency/duration breach, and lack of successful executions where applicable.",
                "Thresholds include evaluation window, severity, owner, and paging versus ticketing behavior.",
                "Alert notifications include dashboard and runbook links.",
            ),
            (
                "Validate alert rules or monitors with the observability provider's test command or preview mode.",
                "Confirm notification routing reaches the intended owner without paging production unnecessarily.",
            ),
        )
    if category == "runbook":
        return (
            f"Attach runbook guidance for operating the {subject}.",
            (
                "Runbook names likely symptoms, first triage queries, dashboards, and rollback or mitigation steps.",
                "Runbook identifies the owner, escalation path, and customer-impact assessment steps.",
                "Alert descriptions and dashboard annotations link back to the runbook.",
            ),
            (
                "Follow the runbook against a staged or synthetic incident and confirm each referenced link works.",
                "Confirm the on-call or owning team can access the runbook and dashboards.",
            ),
        )
    return (
        f"Add verification steps that prove observability works for the {subject}.",
        (
            "Verification covers logs, metrics, traces, dashboards, and alerts that were added or changed.",
            "Verification commands or manual checks are recorded in the execution task before handoff.",
            "Post-deploy validation confirms telemetry appears in the target environment.",
        ),
        (
            "Run the task's validation command or smoke test and capture evidence of emitted telemetry.",
            "Confirm failures are visible through at least one log query, metric, trace, dashboard panel, or alert preview.",
        ),
    )


def _has_observability_requirement(signals: tuple[ObservabilitySignal, ...]) -> bool:
    return any(signal in signals for signal in _SIGNAL_ORDER if not signal.endswith("_context"))


def _context(signals: tuple[ObservabilitySignal, ...]) -> Literal["api", "background_job", "generic"]:
    if "background_job_context" in signals and "api_context" not in signals:
        return "background_job"
    if "api_context" in signals:
        return "api"
    return "generic"


def _readiness(
    signals: tuple[ObservabilitySignal, ...],
    generated_tasks: tuple[ObservabilityReadinessTask, ...],
) -> ObservabilityReadinessLevel:
    categories = {task.category for task in generated_tasks}
    if set(_CATEGORY_ORDER) <= categories and {"runbook", "verification"} <= set(signals):
        return "ready"
    if {"dashboard", "alerts", "runbook", "verification"} & set(signals):
        return "partial"
    return "needs_planning"


def _summary(
    records: tuple[TaskObservabilityReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    generated_tasks = [task for record in records for task in record.generated_tasks]
    return {
        "task_count": task_count,
        "observability_task_count": len(records),
        "observability_task_ids": [record.task_id for record in records],
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
        "generated_task_count": len(generated_tasks),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "generated_task_category_counts": {
            category: sum(1 for task in generated_tasks if task.category == category)
            for category in _CATEGORY_ORDER
        },
        "status": "no_observability_signals" if not records else "observability_readiness_tasks_generated",
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, TaskObservabilityReadinessPlan):
        return source.plan_id, [record.to_dict() for record in source.records]
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
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
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
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
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
        "expected_file_paths",
        "expected_files",
        "paths",
        "acceptance_criteria",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
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
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
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
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
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


def _metadata_key_is_signal(value: str) -> bool:
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values()])


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value) or _strings(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value) or _strings(metadata_value))
            else:
                commands.extend(_strings(metadata_value))
    return tuple(("validation_commands", command) for command in _dedupe(commands))


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
    path = value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
    return str(PurePosixPath(path)) if path else ""


def _category_title(category: ObservabilityReadinessCategory) -> str:
    return category.replace("_", " ").title()


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _evidence_priority(value: str) -> tuple[int, str]:
    if value.startswith("description:"):
        return (0, value)
    if value.startswith("metadata."):
        return (1, value)
    if value.startswith("acceptance_criteria"):
        return (2, value)
    return (3, value)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


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


__all__ = [
    "ObservabilityReadinessCategory",
    "ObservabilityReadinessLevel",
    "ObservabilityReadinessTask",
    "ObservabilitySignal",
    "TaskObservabilityReadinessPlan",
    "TaskObservabilityReadinessRecord",
    "analyze_task_observability_readiness",
    "build_task_observability_readiness_plan",
    "derive_task_observability_readiness",
    "extract_task_observability_readiness",
    "generate_task_observability_readiness",
    "recommend_task_observability_readiness",
    "summarize_task_observability_readiness",
    "task_observability_readiness_plan_to_dict",
    "task_observability_readiness_plan_to_dicts",
    "task_observability_readiness_plan_to_markdown",
    "task_observability_readiness_to_dicts",
]
