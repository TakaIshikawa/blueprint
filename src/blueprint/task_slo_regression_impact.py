"""Identify task-level SLO regression and error-budget risks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SloDimension = Literal["availability", "latency", "error_rate", "throughput", "freshness"]
SloRiskLevel = Literal["high", "medium", "low"]
SloImpactCheck = Literal[
    "baseline_metric_capture",
    "latency_error_rate_guardrails",
    "capacity_validation",
    "alert_threshold_review",
    "rollback_criteria",
    "post_deploy_monitoring",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_DIMENSION_ORDER: tuple[SloDimension, ...] = (
    "availability",
    "latency",
    "error_rate",
    "throughput",
    "freshness",
)
_CHECK_ORDER: tuple[SloImpactCheck, ...] = (
    "baseline_metric_capture",
    "latency_error_rate_guardrails",
    "capacity_validation",
    "alert_threshold_review",
    "rollback_criteria",
    "post_deploy_monitoring",
)
_RISK_ORDER: dict[SloRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}

_API_RE = re.compile(
    r"\b(?:api|endpoint|route|controller|handler|graphql|rest|http|webhook|rpc|grpc|"
    r"integration|third[- ]party|dependency|vendor|provider|client)\b",
    re.I,
)
_LATENCY_RE = re.compile(
    r"\b(?:latency|response time|slow|timeout|p50|p75|p90|p95|p99|percentile|"
    r"performance|hot path|critical path|request duration)\b",
    re.I,
)
_AVAILABILITY_RE = re.compile(
    r"\b(?:availability|uptime|outage|degradation|downtime|failover|circuit breaker|"
    r"health check|readiness|liveness|critical path|production traffic)\b",
    re.I,
)
_ERROR_RATE_RE = re.compile(
    r"\b(?:error rate|errors?|failure|failures|5xx|4xx|exception|exceptions|crash|"
    r"retry|retries|retry storm|dead letter|dlq|poison message|error budget)\b",
    re.I,
)
_THROUGHPUT_RE = re.compile(
    r"\b(?:throughput|capacity|load|rps|qps|requests per second|rate limit|quota|"
    r"burst|concurrency|parallelism|worker|consumer|producer|backpressure|scale|autoscal)\b",
    re.I,
)
_FRESHNESS_RE = re.compile(
    r"\b(?:freshness|staleness|stale|lag|delay|replication lag|sync delay|backfill|"
    r"etl|cron|scheduled|nightly|hourly|cache invalidation|ttl|eventual consistency)\b",
    re.I,
)
_QUEUE_RE = re.compile(
    r"\b(?:queue|queues|queued|background job|job|worker|consumer|producer|kafka|sqs|"
    r"pubsub|rabbitmq|celery|sidekiq|resque|backlog|queue lag|dlq|dead letter)\b",
    re.I,
)
_DATABASE_RE = re.compile(
    r"\b(?:database|db|sql|query|queries|index|indexes|migration|transaction|"
    r"read replica|replication|lock|n\+1|orm|cache|redis|memcached)\b",
    re.I,
)
_PATH_RE = re.compile(
    r"(^|/)(?:api|routes?|controllers?|handlers?|graphql|resolvers?|integrations?|"
    r"clients?|services?|jobs?|workers?|queues?|consumers?|producers?|cron|tasks|"
    r"migrations?|models?|queries|repositories|dao|cache|redis|infra|deploy|k8s|"
    r"kubernetes)(/|$)|\.(?:sql|ddl)$",
    re.I,
)
_NEGATIVE_RE = re.compile(
    r"\b(?:docs?|documentation|readme|copy|typo|formatting|style-only|comment-only|"
    r"test fixture|mock data|storybook)\b",
    re.I,
)

_CHECK_TEXT: dict[SloImpactCheck, str] = {
    "baseline_metric_capture": "Capture current baseline SLO metrics before implementation.",
    "latency_error_rate_guardrails": "Add latency and error-rate guardrails for the affected paths.",
    "capacity_validation": "Validate capacity, throughput, and saturation under expected peak load.",
    "alert_threshold_review": "Review alert thresholds, paging routes, and error-budget burn alerts.",
    "rollback_criteria": "Define rollback criteria tied to SLO or error-budget regression.",
    "post_deploy_monitoring": "Schedule post-deploy monitoring for the affected SLO metrics.",
}
_CHECK_PATTERNS: dict[SloImpactCheck, re.Pattern[str]] = {
    "baseline_metric_capture": re.compile(
        r"\b(?:baseline|before/after|before and after|current metrics?|existing metrics?|"
        r"capture metrics?|benchmark|load test|trace sample)\b",
        re.I,
    ),
    "latency_error_rate_guardrails": re.compile(
        r"\b(?:latency|p95|p99|error rate|5xx|4xx|slo assertion|guardrail|threshold|"
        r"performance test|synthetic check)\b",
        re.I,
    ),
    "capacity_validation": re.compile(
        r"\b(?:capacity|throughput|load test|stress test|peak load|concurrency|"
        r"saturation|autoscaling|queue depth|backlog)\b",
        re.I,
    ),
    "alert_threshold_review": re.compile(
        r"\b(?:alert|alerts|paging|pager|on-call|oncall|error-budget burn|burn rate|"
        r"dashboard|slo monitor|monitor threshold)\b",
        re.I,
    ),
    "rollback_criteria": re.compile(
        r"\b(?:rollback|roll back|revert|abort|kill switch|feature flag|disable|"
        r"go/no-go|rollback criteria)\b",
        re.I,
    ),
    "post_deploy_monitoring": re.compile(
        r"\b(?:post[- ]deploy|post deployment|launch watch|watch window|monitor after|"
        r"deployment monitoring|canary monitor|release watch)\b",
        re.I,
    ),
}
_METRICS_BY_DIMENSION: dict[SloDimension, tuple[str, ...]] = {
    "availability": ("uptime percentage", "successful request ratio", "error-budget burn rate"),
    "latency": ("p50 latency", "p95 latency", "p99 latency"),
    "error_rate": ("5xx rate", "exception rate", "retry rate"),
    "throughput": ("request rate", "queue depth", "consumer throughput", "saturation"),
    "freshness": ("data freshness lag", "replication lag", "cache hit ratio", "stale read rate"),
}


@dataclass(frozen=True, slots=True)
class TaskSloRegressionImpactRecord:
    """SLO regression guidance for one execution task."""

    task_id: str
    title: str
    impacted_slo_dimensions: tuple[SloDimension, ...] = field(default_factory=tuple)
    missing_checks: tuple[str, ...] = field(default_factory=tuple)
    suggested_metrics: tuple[str, ...] = field(default_factory=tuple)
    risk_level: SloRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "impacted_slo_dimensions": list(self.impacted_slo_dimensions),
            "missing_checks": list(self.missing_checks),
            "suggested_metrics": list(self.suggested_metrics),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskSloRegressionImpactPlan:
    """Plan-level SLO regression impact review."""

    plan_id: str | None = None
    records: tuple[TaskSloRegressionImpactRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return SLO regression impact records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    @property
    def findings(self) -> tuple[TaskSloRegressionImpactRecord, ...]:
        """Compatibility view matching planners that name task records findings."""
        return self.records

    def to_markdown(self) -> str:
        """Render the SLO regression impact plan as deterministic Markdown."""
        title = "# Task SLO Regression Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        dimension_counts = self.summary.get("dimension_counts", {})
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('impacted_task_count', 0)} impacted tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(no impact: {self.summary.get('no_impact_task_count', 0)})."
            ),
            "Dimensions: "
            + ", ".join(f"{dimension} {dimension_counts.get(dimension, 0)}" for dimension in _DIMENSION_ORDER)
            + ".",
            "Risk: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER) + ".",
        ]
        if not self.records:
            lines.extend(["", "No SLO regression impact records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Risk | Dimensions | Missing Checks | Suggested Metrics | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell('; '.join(record.impacted_slo_dimensions))} | "
                f"{_markdown_cell('; '.join(record.missing_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.suggested_metrics) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_slo_regression_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSloRegressionImpactPlan:
    """Detect execution tasks that could regress service-level objectives."""
    plan_id, plan_context, tasks = _source_payload(source)
    records: list[TaskSloRegressionImpactRecord] = []
    no_impact_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        record = _record(task, index, plan_context)
        if record:
            records.append(record)
        else:
            no_impact_task_ids.append(task_id)

    records.sort(
        key=lambda record: (
            _RISK_ORDER[record.risk_level],
            record.task_id,
            record.title.casefold(),
            record.impacted_slo_dimensions,
        )
    )
    result = tuple(records)
    impacted_task_ids = tuple(record.task_id for record in result)
    risk_counts = {risk: sum(1 for record in result if record.risk_level == risk) for risk in _RISK_ORDER}
    dimension_counts = {
        dimension: sum(1 for record in result if dimension in record.impacted_slo_dimensions)
        for dimension in _DIMENSION_ORDER
    }
    return TaskSloRegressionImpactPlan(
        plan_id=plan_id,
        records=result,
        impacted_task_ids=impacted_task_ids,
        no_impact_task_ids=tuple(no_impact_task_ids),
        summary={
            "task_count": len(tasks),
            "record_count": len(result),
            "impacted_task_count": len(impacted_task_ids),
            "no_impact_task_count": len(no_impact_task_ids),
            "missing_check_count": sum(len(record.missing_checks) for record in result),
            "dimension_counts": dimension_counts,
            "risk_counts": risk_counts,
            "impacted_task_ids": list(impacted_task_ids),
            "no_impact_task_ids": list(no_impact_task_ids),
        },
    )


def derive_task_slo_regression_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSloRegressionImpactPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSloRegressionImpactPlan:
    """Compatibility alias for building SLO regression impact plans."""
    if isinstance(source, TaskSloRegressionImpactPlan):
        return source
    return build_task_slo_regression_impact_plan(source)


def summarize_task_slo_regression_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSloRegressionImpactPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSloRegressionImpactPlan:
    """Summarize task-level SLO regression impacts."""
    return derive_task_slo_regression_impact_plan(source)


def summarize_task_slo_regression_impacts(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSloRegressionImpactPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSloRegressionImpactPlan:
    """Plural compatibility alias for task-level SLO regression impacts."""
    return derive_task_slo_regression_impact_plan(source)


def task_slo_regression_impact_plan_to_dict(matrix: TaskSloRegressionImpactPlan) -> dict[str, Any]:
    """Serialize an SLO regression impact plan to a plain dictionary."""
    return matrix.to_dict()


task_slo_regression_impact_plan_to_dict.__test__ = False


def task_slo_regression_impact_plan_to_markdown(matrix: TaskSloRegressionImpactPlan) -> str:
    """Render an SLO regression impact plan as Markdown."""
    return matrix.to_markdown()


task_slo_regression_impact_plan_to_markdown.__test__ = False


def _record(
    task: Mapping[str, Any],
    index: int,
    plan_context: tuple[tuple[str, str], ...],
) -> TaskSloRegressionImpactRecord | None:
    dimensions, evidence = _signals(task, plan_context)
    if not dimensions:
        return None
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    acceptance_context = _acceptance_context(task, plan_context)
    required_checks = _required_checks(dimensions)
    missing_check_keys = tuple(check for check in required_checks if not _CHECK_PATTERNS[check].search(acceptance_context))
    return TaskSloRegressionImpactRecord(
        task_id=task_id,
        title=title,
        impacted_slo_dimensions=dimensions,
        missing_checks=tuple(_CHECK_TEXT[check] for check in missing_check_keys),
        suggested_metrics=_suggested_metrics(dimensions),
        risk_level=_risk_level(dimensions, missing_check_keys),
        evidence=evidence,
    )


def _signals(
    task: Mapping[str, Any],
    plan_context: tuple[tuple[str, str], ...],
) -> tuple[tuple[SloDimension, ...], tuple[str, ...]]:
    dimensions: list[SloDimension] = []
    evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        before = len(dimensions)
        _apply_text_signals(path_text, dimensions)
        if _PATH_RE.search(normalized) or len(dimensions) > before:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in (*_candidate_texts(task), *plan_context):
        if _coverage_only_field(source_field):
            continue
        before = len(dimensions)
        _apply_text_signals(text, dimensions)
        if len(dimensions) > before or _any_signal(text):
            evidence.append(_evidence_snippet(source_field, text))

    dimensions = _ordered_dedupe(dimensions, _DIMENSION_ORDER)
    if not dimensions and _suppressed(task, plan_context):
        return (), ()
    return tuple(dimensions), tuple(_dedupe(evidence))


def _apply_text_signals(text: str, dimensions: list[SloDimension]) -> None:
    if _AVAILABILITY_RE.search(text):
        dimensions.append("availability")
    if _LATENCY_RE.search(text) or _API_RE.search(text):
        dimensions.append("latency")
    if _ERROR_RATE_RE.search(text) or _API_RE.search(text) or _DATABASE_RE.search(text):
        dimensions.append("error_rate")
    if _THROUGHPUT_RE.search(text) or _QUEUE_RE.search(text):
        dimensions.append("throughput")
    if _FRESHNESS_RE.search(text) or _QUEUE_RE.search(text) or _DATABASE_RE.search(text):
        dimensions.append("freshness")


def _any_signal(text: str) -> bool:
    return any(
        pattern.search(text)
        for pattern in (
            _API_RE,
            _LATENCY_RE,
            _AVAILABILITY_RE,
            _ERROR_RATE_RE,
            _THROUGHPUT_RE,
            _FRESHNESS_RE,
            _QUEUE_RE,
            _DATABASE_RE,
        )
    )


def _suppressed(task: Mapping[str, Any], plan_context: tuple[tuple[str, str], ...]) -> bool:
    text = " ".join(value for _, value in (*_candidate_texts(task), *plan_context))
    return bool(_NEGATIVE_RE.search(text))


def _required_checks(dimensions: tuple[SloDimension, ...]) -> tuple[SloImpactCheck, ...]:
    checks: list[SloImpactCheck] = [
        "baseline_metric_capture",
        "latency_error_rate_guardrails",
        "alert_threshold_review",
        "rollback_criteria",
        "post_deploy_monitoring",
    ]
    if "throughput" in dimensions or "freshness" in dimensions:
        checks.append("capacity_validation")
    return tuple(_ordered_dedupe(checks, _CHECK_ORDER))


def _risk_level(
    dimensions: tuple[SloDimension, ...],
    missing_checks: tuple[SloImpactCheck, ...],
) -> SloRiskLevel:
    if not missing_checks:
        return "low"
    if "availability" in dimensions and ("alert_threshold_review" in missing_checks or "rollback_criteria" in missing_checks):
        return "high"
    if len(dimensions) >= 4 or len(missing_checks) >= 5:
        return "high"
    return "medium"


def _suggested_metrics(dimensions: tuple[SloDimension, ...]) -> tuple[str, ...]:
    metrics: list[str] = []
    for dimension in dimensions:
        metrics.extend(_METRICS_BY_DIMENSION[dimension])
    return tuple(_dedupe(metrics))


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
                    "monitor",
                    "metric",
                    "slo",
                    "alert",
                    "rollback",
                    "guardrail",
                    "validation",
                    "test",
                )
            ):
                values.append(text)
    values.extend(text for source_field, text in plan_context if _context_field_is_acceptance(source_field))
    return " ".join(values)


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "blocked_reason", "notes"):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("risks", "acceptance_criteria", "criteria", "tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


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
            _plan_context(payload),
            _task_payloads(payload.get("tasks")),
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return _optional_text(plan.get("id")), _plan_context(plan), _task_payloads(plan.get("tasks"))
        return None, (), [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return _optional_text(plan.get("id")), _plan_context(plan), _task_payloads(plan.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, (), []
    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, (), tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _plan_context(plan: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for field_name in ("test_strategy", "handoff_prompt", "generation_prompt"):
        if text := _optional_text(plan.get(field_name)):
            texts.append((f"plan.{field_name}", text))
    for source_field, text in _metadata_texts(plan.get("metadata"), "plan.metadata"):
        texts.append((source_field, text))
    return tuple(texts)


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
    return _object_payload(value)


def _object_payload(value: Any) -> dict[str, Any]:
    if value is None or isinstance(value, (str, bytes)):
        return {}
    data: dict[str, Any] = {}
    for name in dir(value):
        if name.startswith("_"):
            continue
        try:
            item = getattr(value, name)
        except Exception:
            continue
        if not callable(item):
            data[name] = item
    return data


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                key_text = str(key).replace("_", " ").replace("-", " ")
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


def _context_field_is_acceptance(source_field: str) -> bool:
    normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
    return any(
        keyword in normalized
        for keyword in (
            "acceptance",
            "definition",
            "monitor",
            "metric",
            "slo",
            "alert",
            "rollback",
            "guardrail",
            "validation",
            "test",
        )
    )


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/").casefold()


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


def _ordered_dedupe(values: Iterable[_T], preferred_order: Iterable[_T]) -> list[_T]:
    present = set(values)
    ordered = [item for item in preferred_order if item in present]
    ordered.extend(value for value in values if value not in ordered)
    return ordered


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
    "TaskSloRegressionImpactPlan",
    "TaskSloRegressionImpactRecord",
    "build_task_slo_regression_impact_plan",
    "derive_task_slo_regression_impact_plan",
    "summarize_task_slo_regression_impact",
    "summarize_task_slo_regression_impacts",
    "task_slo_regression_impact_plan_to_dict",
    "task_slo_regression_impact_plan_to_markdown",
]
