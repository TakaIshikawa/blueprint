"""Derive task-level performance budget guidance for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


PerformanceBudgetCategory = Literal[
    "frontend_bundle",
    "page_load",
    "api_latency",
    "database_query",
    "batch_job",
    "queue_throughput",
    "resource_utilization",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[PerformanceBudgetCategory, ...] = (
    "frontend_bundle",
    "page_load",
    "api_latency",
    "database_query",
    "batch_job",
    "queue_throughput",
    "resource_utilization",
)
_CATEGORY_RANK = {category: index for index, category in enumerate(_CATEGORY_ORDER)}

_TEXT_SIGNAL_PATTERNS: dict[PerformanceBudgetCategory, tuple[re.Pattern[str], ...]] = {
    "frontend_bundle": (
        re.compile(
            r"\b(?:bundle|bundles?|webpack|vite|rollup|parcel|chunk|chunks?|"
            r"tree[- ]?shak|code split|lazy load|javascript payload|css payload|"
            r"asset size|compressed size)\b",
            re.IGNORECASE,
        ),
    ),
    "page_load": (
        re.compile(
            r"\b(?:page load|load time|lcp|largest contentful paint|inp|fid|cls|"
            r"core web vitals|web vitals|first contentful paint|fcp|time to interactive|tti|"
            r"render performance|hydration)\b",
            re.IGNORECASE,
        ),
    ),
    "api_latency": (
        re.compile(
            r"\b(?:api latency|endpoint latency|response time|request latency|p95|p99|"
            r"slo|sla|timeout|rate limit|graphql resolver|rest endpoint|http endpoint)\b",
            re.IGNORECASE,
        ),
    ),
    "database_query": (
        re.compile(
            r"\b(?:database|db|sql|query|queries|index|indexes|explain plan|"
            r"full table scan|n\+1|orm|join|transaction latency|read replica)\b",
            re.IGNORECASE,
        ),
    ),
    "batch_job": (
        re.compile(
            r"\b(?:batch job|batch|cron|scheduled job|backfill|etl|import job|"
            r"export job|nightly job|bulk processing|long[- ]running job)\b",
            re.IGNORECASE,
        ),
    ),
    "queue_throughput": (
        re.compile(
            r"\b(?:queue|queues|throughput|consumer|producer|worker|workers|"
            r"message rate|queue lag|backlog|dead letter|dlq|retry storm|kafka|sqs|"
            r"pubsub|rabbitmq|celery)\b",
            re.IGNORECASE,
        ),
    ),
    "resource_utilization": (
        re.compile(
            r"\b(?:memory|cpu|resource utilization|resource usage|heap|rss|"
            r"garbage collection|gc pressure|leak|container limit|pod limit|"
            r"autoscaling|hpa|replica|capacity|load test|stress test)\b",
            re.IGNORECASE,
        ),
    ),
}
_FILE_SIGNAL_PATTERNS: tuple[tuple[PerformanceBudgetCategory, re.Pattern[str]], ...] = (
    ("frontend_bundle", re.compile(r"(^|/)(?:webpack|vite|rollup|parcel)\.config\.", re.I)),
    ("api_latency", re.compile(r"(^|/)(?:api|routes|controllers|handlers|graphql|resolvers)(/|$)", re.I)),
    ("database_query", re.compile(r"(^|/)(?:models|queries|repositories|dao|migrations|alembic)(/|$)|\.(?:sql|ddl)$", re.I)),
    ("batch_job", re.compile(r"(^|/)(?:jobs|tasks|cron|batch|backfills|etl)(/|$)", re.I)),
    ("queue_throughput", re.compile(r"(^|/)(?:workers|consumers|producers|queues?|messaging)(/|$)", re.I)),
    ("resource_utilization", re.compile(r"(^|/)(?:infra|terraform|helm|k8s|kubernetes|deploy)(/|$)", re.I)),
    ("resource_utilization", re.compile(r"(^|/)(?:dockerfile|docker-compose\.ya?ml)$", re.I)),
)

_MEASUREMENT_TARGETS: dict[PerformanceBudgetCategory, str] = {
    "frontend_bundle": "Keep added compressed JavaScript/CSS under 25 KB or document an approved bundle budget exception.",
    "page_load": "Keep p95 LCP under 2.5s and p95 INP under 200 ms on the target page.",
    "api_latency": "Keep p95 API latency under 300 ms and p99 under 1 s at expected peak traffic.",
    "database_query": "Keep p95 query latency under 100 ms and explain plans free of full scans on hot paths.",
    "batch_job": "Keep job runtime within the current operational window plus 10%.",
    "queue_throughput": "Keep queue lag under 60 s and sustained consumer throughput at or above expected producer rate.",
    "resource_utilization": "Keep peak CPU under 70% and memory growth under 10% versus baseline during load.",
}
_BENCHMARK_EVIDENCE: dict[PerformanceBudgetCategory, str] = {
    "frontend_bundle": "Attach before/after bundle analyzer output with compressed and parsed size deltas.",
    "page_load": "Capture Lighthouse, WebPageTest, or RUM baseline and after-change p75/p95 Web Vitals.",
    "api_latency": "Record before/after load-test or trace samples for p50, p95, p99, error rate, and request volume.",
    "database_query": "Attach EXPLAIN output, query timing, row counts, and before/after index or plan comparison.",
    "batch_job": "Record representative input size, runtime, retry count, and resource usage before and after the change.",
    "queue_throughput": "Capture producer rate, consumer rate, queue depth, lag, retry rate, and DLQ count under expected load.",
    "resource_utilization": "Capture CPU, memory, heap, container limit, and saturation metrics during representative load.",
}
_REGRESSION_GUARDS: dict[PerformanceBudgetCategory, str] = {
    "frontend_bundle": "Add a bundle-size check or CI artifact diff that fails when the approved budget is exceeded.",
    "page_load": "Add synthetic or RUM alert thresholds for Web Vitals regressions on the affected route.",
    "api_latency": "Add latency SLO assertions, trace sampling, or performance tests for the changed endpoint.",
    "database_query": "Add query-count, query-plan, or slow-query regression checks for the hot path.",
    "batch_job": "Add runtime and retry-count alerts for the job plus a representative benchmark fixture.",
    "queue_throughput": "Add queue lag, backlog, DLQ, and consumer throughput alerts tied to the expected producer rate.",
    "resource_utilization": "Add load-test or autoscaling guardrails for CPU, memory, and saturation metrics.",
}
_FOLLOW_UP_ACTIONS: dict[PerformanceBudgetCategory, tuple[str, ...]] = {
    "frontend_bundle": (
        "Confirm the affected bundles and routes before implementation.",
        "Review dependency additions for lazy loading, splitting, or removal.",
    ),
    "page_load": (
        "Identify target devices, network profile, and route-level baseline.",
        "Confirm images, hydration, and render-blocking assets stay within budget.",
    ),
    "api_latency": (
        "Define expected request volume and percentile SLO before merging.",
        "Confirm tracing covers the endpoint and downstream dependencies.",
    ),
    "database_query": (
        "Review query plans on staging-like data before release.",
        "Confirm indexes and migrations are reversible or otherwise operationally approved.",
    ),
    "batch_job": (
        "Benchmark against representative input sizes before scheduling the rollout.",
        "Define pause, resume, retry, and rollback behavior for long-running work.",
    ),
    "queue_throughput": (
        "Size consumer concurrency against expected producer rate.",
        "Confirm backpressure, retry, and DLQ behavior under load.",
    ),
    "resource_utilization": (
        "Confirm container limits, autoscaling thresholds, and capacity headroom.",
        "Run a focused load or stress test before production rollout.",
    ),
}
_SUPPORTING_EVIDENCE_RE = re.compile(
    r"\b(?:benchmark|load[- ]?test|stress[- ]?test|lighthouse|webpagetest|"
    r"bundle analyzer|explain plan|k6|locust|jmeter|profile|profiling|rum)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class TaskPerformanceBudgetRecord:
    """Performance budget guidance for one impacted execution task."""

    task_id: str
    title: str
    budget_category: PerformanceBudgetCategory
    suggested_measurement_target: str
    benchmark_evidence: str
    regression_guard: str
    evidence_requirements: tuple[str, ...] = field(default_factory=tuple)
    follow_up_actions: tuple[str, ...] = field(default_factory=tuple)
    detected_signals: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "budget_category": self.budget_category,
            "suggested_measurement_target": self.suggested_measurement_target,
            "benchmark_evidence": self.benchmark_evidence,
            "regression_guard": self.regression_guard,
            "evidence_requirements": list(self.evidence_requirements),
            "follow_up_actions": list(self.follow_up_actions),
            "detected_signals": list(self.detected_signals),
        }


@dataclass(frozen=True, slots=True)
class TaskPerformanceBudgetPlan:
    """Task-level performance budget plan."""

    plan_id: str | None = None
    records: tuple[TaskPerformanceBudgetRecord, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
        }

    def to_markdown(self) -> str:
        """Render performance budget guidance as deterministic Markdown."""
        title = "# Task Performance Budget Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No task performance budget signals detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Category | Target | Benchmark Evidence | Regression Guard | Follow-up Actions |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{record.budget_category} | "
                f"{_markdown_cell(record.suggested_measurement_target)} | "
                f"{_markdown_cell(record.benchmark_evidence)} | "
                f"{_markdown_cell(record.regression_guard)} | "
                f"{_markdown_cell('; '.join(record.follow_up_actions))} |"
            )
        return "\n".join(lines)


def build_task_performance_budget_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskPerformanceBudgetPlan:
    """Detect execution tasks that need task-level performance budgets."""
    plan_id, plan_evidence, tasks = _source_payload(source)
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        for record in _task_records(task, index, plan_evidence)
    ]
    records.sort(key=lambda record: (record.task_id, _CATEGORY_RANK[record.budget_category]))
    return TaskPerformanceBudgetPlan(plan_id=plan_id, records=tuple(records))


def derive_task_performance_budget_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskPerformanceBudgetPlan:
    """Compatibility alias for building a task performance budget plan."""
    return build_task_performance_budget_plan(source)


def build_task_performance_budget(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskPerformanceBudgetPlan:
    """Compatibility alias for building a task performance budget plan."""
    return build_task_performance_budget_plan(source)


def derive_task_performance_budget(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskPerformanceBudgetPlan:
    """Compatibility alias for building a task performance budget plan."""
    return build_task_performance_budget_plan(source)


def task_performance_budget_plan_to_dict(
    plan: TaskPerformanceBudgetPlan,
) -> dict[str, Any]:
    """Serialize a task performance budget plan to a plain dictionary."""
    return plan.to_dict()


task_performance_budget_plan_to_dict.__test__ = False


def task_performance_budget_plan_to_markdown(plan: TaskPerformanceBudgetPlan) -> str:
    """Render a task performance budget plan as Markdown."""
    return plan.to_markdown()


task_performance_budget_plan_to_markdown.__test__ = False

task_performance_budget_to_dict = task_performance_budget_plan_to_dict
task_performance_budget_to_dict.__test__ = False
task_performance_budget_to_markdown = task_performance_budget_plan_to_markdown
task_performance_budget_to_markdown.__test__ = False


def _task_records(
    task: Mapping[str, Any],
    index: int,
    plan_evidence: tuple[str, ...],
) -> list[TaskPerformanceBudgetRecord]:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    evidence_by_category = _detected_evidence(task)

    return [
        TaskPerformanceBudgetRecord(
            task_id=task_id,
            title=title,
            budget_category=category,
            suggested_measurement_target=_MEASUREMENT_TARGETS[category],
            benchmark_evidence=_BENCHMARK_EVIDENCE[category],
            regression_guard=_REGRESSION_GUARDS[category],
            evidence_requirements=tuple(
                _dedupe(
                    [
                        *evidence,
                        *_supporting_evidence(task),
                        *plan_evidence,
                        _BENCHMARK_EVIDENCE[category],
                    ]
                )
            ),
            follow_up_actions=_FOLLOW_UP_ACTIONS[category],
            detected_signals=evidence,
        )
        for category, evidence in evidence_by_category.items()
    ]


def _detected_evidence(
    task: Mapping[str, Any],
) -> dict[PerformanceBudgetCategory, tuple[str, ...]]:
    evidence_by_category: dict[PerformanceBudgetCategory, list[str]] = {}
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        for category, pattern in _FILE_SIGNAL_PATTERNS:
            if pattern.search(normalized):
                _append_evidence(evidence_by_category, category, f"files_or_modules: {path}")

    for field_path, text in _task_texts(task):
        for category, patterns in _TEXT_SIGNAL_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                _append_evidence(evidence_by_category, category, f"{field_path}: {text}")

    for field_path, text in _metadata_texts(task.get("metadata")):
        hinted_category = _metadata_hint_category(field_path)
        if hinted_category:
            _append_evidence(evidence_by_category, hinted_category, f"{field_path}: {text}")
        for category, patterns in _TEXT_SIGNAL_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                _append_evidence(evidence_by_category, category, f"{field_path}: {text}")

    return {
        category: tuple(_dedupe(evidence_by_category.get(category, ())))
        for category in _CATEGORY_ORDER
        if evidence_by_category.get(category)
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, tuple[str, ...], list[dict[str, Any]]]:
    if source is None:
        return None, (), []
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        plan = source.model_dump(mode="python")
        return (
            _optional_text(plan.get("id")),
            tuple(_plan_evidence(plan)),
            [task.model_dump(mode="python") for task in source.tasks],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return (
                _optional_text(plan.get("id")),
                tuple(_plan_evidence(plan)),
                _task_payloads(plan.get("tasks")),
            )
        return None, (), [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return (
            _optional_text(plan.get("id")),
            tuple(_plan_evidence(plan)),
            _task_payloads(plan.get("tasks")),
        )

    tasks: list[dict[str, Any]] = []
    for item in source:
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
    fields = (
        "id",
        "tasks",
        "title",
        "description",
        "milestone",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "test_strategy",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "validation_plan",
        "metadata",
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
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
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


def _plan_evidence(plan: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    if text := _optional_text(plan.get("test_strategy")):
        evidence.append(f"test_strategy: {text}")
    evidence.extend(_validation_values(plan))
    return _dedupe(evidence)


def _validation_values(item: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command", "validation_plan"):
        if text := _optional_text(item.get(key)):
            values.append(f"{key}: {text}")
    metadata = item.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "performance_budget",
            "benchmark",
            "benchmarks",
            "validation_plan",
            "validation_plans",
            "validation_gates",
            "validation_gate",
            "validation_commands",
            "validation_command",
            "test_commands",
            "test_command",
        ):
            value = metadata.get(key)
            if isinstance(value, Mapping):
                values.extend(f"metadata.{key}: {command}" for command in flatten_validation_commands(value))
            else:
                values.extend(f"metadata.{key}: {text}" for text in _strings(value))
    return _dedupe(values)


def _supporting_evidence(task: Mapping[str, Any]) -> list[str]:
    return [
        f"{field_path}: {text}"
        for field_path, text in _task_texts(task)
        if _SUPPORTING_EVIDENCE_RE.search(text)
    ]


def _metadata_hint_category(source_field: str) -> PerformanceBudgetCategory | None:
    field = source_field.casefold()
    if any(token in field for token in ("bundle", "chunk", "asset_size")):
        return "frontend_bundle"
    if any(token in field for token in ("web_vital", "page_load", "lcp", "inp")):
        return "page_load"
    if any(token in field for token in ("api", "endpoint", "latency", "slo")):
        return "api_latency"
    if any(token in field for token in ("database", "query", "sql", "index")):
        return "database_query"
    if any(token in field for token in ("batch", "cron", "backfill", "etl")):
        return "batch_job"
    if any(token in field for token in ("queue", "throughput", "worker", "consumer")):
        return "queue_throughput"
    if any(token in field for token in ("memory", "cpu", "resource", "capacity", "autoscaling")):
        return "resource_utilization"
    return None


def _append_evidence(
    evidence_by_category: dict[PerformanceBudgetCategory, list[str]],
    category: PerformanceBudgetCategory,
    evidence: str,
) -> None:
    evidence_by_category.setdefault(category, []).append(evidence)


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
    normalized = value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.strip("/")


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _dedupe(values: Iterable[_T | None]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "PerformanceBudgetCategory",
    "TaskPerformanceBudgetPlan",
    "TaskPerformanceBudgetRecord",
    "build_task_performance_budget",
    "build_task_performance_budget_plan",
    "derive_task_performance_budget",
    "derive_task_performance_budget_plan",
    "task_performance_budget_to_dict",
    "task_performance_budget_to_markdown",
    "task_performance_budget_plan_to_dict",
    "task_performance_budget_plan_to_markdown",
]
