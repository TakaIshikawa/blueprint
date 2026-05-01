"""Assess execution-plan tasks for service-level objective impact."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SloImpactSignal = Literal[
    "latency",
    "availability",
    "error_rate",
    "throughput",
    "data_freshness",
    "customer_reliability",
]
SloImpactSeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_LATENCY_RE = re.compile(
    r"\b(?:latency|p95|p99|response time|slow|slowness|timeout|timeouts?|"
    r"time to first byte|ttfb|performance budget)\b",
    re.IGNORECASE,
)
_AVAILABILITY_RE = re.compile(
    r"\b(?:availability|uptime|downtime|outage|resilien(?:ce|t)|failover|"
    r"health ?check|readiness|liveness|degradation|degraded|incident)\b",
    re.IGNORECASE,
)
_ERROR_RATE_RE = re.compile(
    r"\b(?:error rate|errors?|5xx|500s?|exceptions?|crashes?|failure rate|"
    r"failed requests?|retry storm|retry budget)\b",
    re.IGNORECASE,
)
_THROUGHPUT_RE = re.compile(
    r"\b(?:throughput|requests per second|rps|qps|rate limit|rate-limit|"
    r"queue depth|backlog|batch size|concurrency|parallelism|worker capacity|"
    r"jobs per|messages per|events per)\b",
    re.IGNORECASE,
)
_FRESHNESS_RE = re.compile(
    r"\b(?:freshness|staleness|stale|lag|replication lag|sync lag|data age|"
    r"last updated|last successful|watermark|etl|ingestion delay|delayed data)\b",
    re.IGNORECASE,
)
_CUSTOMER_RELIABILITY_RE = re.compile(
    r"\b(?:customer[- ]facing|user[- ]facing|end users?|production traffic|"
    r"runtime-critical|critical path|checkout|login|signup|payment|billing|"
    r"api endpoint|public api|service reliability|slo|sla|error budget)\b",
    re.IGNORECASE,
)
_SERVICE_RE = re.compile(
    r"\b(?:api|backend|service|endpoint|controller|route|handler|rpc|grpc|"
    r"rest|graphql|webhook|worker|queue|consumer|job|scheduler|cron|etl|sync)\b",
    re.IGNORECASE,
)
_HIGH_RISK_RE = re.compile(r"\b(?:high|critical|severe|runtime-critical)\b", re.I)

_SIGNAL_ORDER: dict[SloImpactSignal, int] = {
    "latency": 0,
    "availability": 1,
    "error_rate": 2,
    "throughput": 3,
    "data_freshness": 4,
    "customer_reliability": 5,
}
_SEVERITY_ORDER: dict[SloImpactSeverity, int] = {"high": 0, "medium": 1, "low": 2}


@dataclass(frozen=True, slots=True)
class TaskSloImpactFinding:
    """SLO impact guidance for one execution task."""

    task_id: str
    title: str
    severity: SloImpactSeverity
    signals: tuple[SloImpactSignal, ...] = field(default_factory=tuple)
    recommended_slo_checks: tuple[str, ...] = field(default_factory=tuple)
    rationale: str = "No SLO-impacting runtime, reliability, or freshness signals detected."
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "severity": self.severity,
            "signals": list(self.signals),
            "recommended_slo_checks": list(self.recommended_slo_checks),
            "rationale": self.rationale,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskSloImpactPlan:
    """SLO impact findings for a plan or task collection."""

    plan_id: str | None = None
    task_impacts: tuple[TaskSloImpactFinding, ...] = field(default_factory=tuple)
    slo_impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    low_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "task_impacts": [impact.to_dict() for impact in self.task_impacts],
            "slo_impacted_task_ids": list(self.slo_impacted_task_ids),
            "low_impact_task_ids": list(self.low_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return task impact records as plain dictionaries."""
        return [impact.to_dict() for impact in self.task_impacts]

    def to_markdown(self) -> str:
        """Render SLO impact findings as deterministic Markdown."""
        title = "# Task SLO Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.task_impacts:
            lines.extend(["", "No tasks were available for SLO impact assessment."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Severity | Signals | Recommended SLO Checks | Evidence |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for impact in self.task_impacts:
            lines.append(
                "| "
                f"`{_markdown_cell(impact.task_id)}` | "
                f"{impact.severity} | "
                f"{_markdown_cell(', '.join(impact.signals) or 'none')} | "
                f"{_markdown_cell('; '.join(impact.recommended_slo_checks))} | "
                f"{_markdown_cell('; '.join(impact.evidence) or impact.rationale)} |"
            )
        return "\n".join(lines)


def build_task_slo_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
) -> TaskSloImpactPlan:
    """Assess tasks for latency, reliability, throughput, and freshness SLO impact."""
    plan_id, tasks = _source_payload(source)
    findings_with_order = [
        (_finding(task, index), index) for index, task in enumerate(tasks, start=1)
    ]
    findings = tuple(
        finding
        for finding, _ in sorted(
            findings_with_order,
            key=lambda item: (_SEVERITY_ORDER[item[0].severity], item[1]),
        )
    )
    slo_impacted_task_ids = tuple(
        finding.task_id for finding in findings if finding.severity != "low"
    )
    low_impact_task_ids = tuple(
        finding.task_id for finding in findings if finding.severity == "low"
    )
    severity_counts = {
        severity: sum(1 for finding in findings if finding.severity == severity)
        for severity in _SEVERITY_ORDER
    }
    signal_counts = {
        signal: sum(1 for finding in findings if signal in finding.signals)
        for signal in _SIGNAL_ORDER
    }

    return TaskSloImpactPlan(
        plan_id=plan_id,
        task_impacts=findings,
        slo_impacted_task_ids=slo_impacted_task_ids,
        low_impact_task_ids=low_impact_task_ids,
        summary={
            "task_count": len(tasks),
            "slo_impacted_task_count": len(slo_impacted_task_ids),
            "low_impact_task_count": len(low_impact_task_ids),
            "severity_counts": severity_counts,
            "signal_counts": signal_counts,
        },
    )


def task_slo_impact_plan_to_dict(result: TaskSloImpactPlan) -> dict[str, Any]:
    """Serialize an SLO impact plan to a plain dictionary."""
    return result.to_dict()


task_slo_impact_plan_to_dict.__test__ = False


def task_slo_impact_plan_to_markdown(result: TaskSloImpactPlan) -> str:
    """Render an SLO impact plan as Markdown."""
    return result.to_markdown()


task_slo_impact_plan_to_markdown.__test__ = False


def recommend_task_slo_impacts(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
) -> TaskSloImpactPlan:
    """Compatibility alias for building task SLO impact assessments."""
    return build_task_slo_impact_plan(source)


def _finding(task: Mapping[str, Any], index: int) -> TaskSloImpactFinding:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals:
        return TaskSloImpactFinding(
            task_id=task_id,
            title=title,
            severity="low",
            recommended_slo_checks=(
                "Confirm no SLO dashboards, alerts, or runtime-critical paths are affected.",
            ),
        )

    ordered_signals = tuple(sorted(signals, key=lambda signal: _SIGNAL_ORDER[signal]))
    severity = _severity(ordered_signals, task)
    evidence = tuple(_dedupe(item for signal in ordered_signals for item in signals[signal]))
    checks = tuple(_dedupe(check for signal in ordered_signals for check in _checks(signal)))

    return TaskSloImpactFinding(
        task_id=task_id,
        title=title,
        severity=severity,
        signals=ordered_signals,
        recommended_slo_checks=checks,
        rationale=_rationale(ordered_signals, severity),
        evidence=evidence,
    )


def _signals(task: Mapping[str, Any]) -> dict[SloImpactSignal, tuple[str, ...]]:
    signals: dict[SloImpactSignal, list[str]] = {}
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


def _add_path_signals(signals: dict[SloImpactSignal, list[str]], original: str) -> None:
    normalized = _normalized_path(original)
    folded = normalized.casefold()
    if not folded:
        return
    path = PurePosixPath(folded)
    parts = set(path.parts)
    name = path.name
    evidence = f"files_or_modules: {original}"

    if bool({"api", "apis", "routes", "controllers", "handlers", "endpoints"} & parts):
        _append(signals, "customer_reliability", evidence)
        _append(signals, "availability", evidence)
    if bool({"backend", "services", "service", "server", "webhooks"} & parts):
        _append(signals, "availability", evidence)
    if bool({"performance", "perf", "latency"} & parts) or any(
        token in name for token in ("latency", "performance", "timeout")
    ):
        _append(signals, "latency", evidence)
    if bool({"errors", "exceptions", "retries"} & parts) or any(
        token in name for token in ("error", "exception", "retry")
    ):
        _append(signals, "error_rate", evidence)
    if bool({"jobs", "workers", "queues", "consumers", "batch"} & parts) or any(
        token in name for token in ("worker", "queue", "job", "batch")
    ):
        _append(signals, "throughput", evidence)
    if bool({"sync", "etl", "ingest", "ingestion", "replication", "freshness"} & parts) or any(
        token in name for token in ("sync", "freshness", "ingest", "etl")
    ):
        _append(signals, "data_freshness", evidence)


def _add_text_signals(
    signals: dict[SloImpactSignal, list[str]],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    if _LATENCY_RE.search(text):
        _append(signals, "latency", evidence)
    if _AVAILABILITY_RE.search(text):
        _append(signals, "availability", evidence)
    if _ERROR_RATE_RE.search(text):
        _append(signals, "error_rate", evidence)
    if _THROUGHPUT_RE.search(text):
        _append(signals, "throughput", evidence)
    if _FRESHNESS_RE.search(text):
        _append(signals, "data_freshness", evidence)
    if _CUSTOMER_RELIABILITY_RE.search(text):
        _append(signals, "customer_reliability", evidence)

    if _SERVICE_RE.search(text) and (
        _LATENCY_RE.search(text)
        or _AVAILABILITY_RE.search(text)
        or _ERROR_RATE_RE.search(text)
    ):
        _append(signals, "customer_reliability", evidence)


def _severity(
    signals: tuple[SloImpactSignal, ...],
    task: Mapping[str, Any],
) -> SloImpactSeverity:
    signal_set = set(signals)
    context = _task_context(task)
    runtime_critical = "customer_reliability" in signal_set or bool(_SERVICE_RE.search(context))
    high_risk = bool(_HIGH_RISK_RE.search(_text(task.get("risk_level")))) or bool(
        _HIGH_RISK_RE.search(context)
    )

    if (
        bool({"availability", "error_rate"} & signal_set)
        and runtime_critical
        and (high_risk or "customer_reliability" in signal_set)
    ):
        return "high"
    if "latency" in signal_set and runtime_critical and high_risk:
        return "high"
    if signal_set:
        return "medium"
    return "low"


def _checks(signal: SloImpactSignal) -> tuple[str, ...]:
    return {
        "latency": (
            "Compare p95 and p99 latency before and after the task on affected endpoints or jobs.",
        ),
        "availability": (
            "Verify uptime, health-check, and successful-request SLOs for the affected service path.",
        ),
        "error_rate": (
            "Track 5xx, exception, and failed-operation rate against the current error budget.",
        ),
        "throughput": (
            "Measure throughput, queue depth, backlog age, and worker saturation under expected load.",
        ),
        "data_freshness": (
            "Check data age, replication lag, watermark delay, and last successful run freshness.",
        ),
        "customer_reliability": (
            "Run customer-journey or synthetic checks for the affected runtime-critical path.",
        ),
    }[signal]


def _rationale(
    signals: tuple[SloImpactSignal, ...],
    severity: SloImpactSeverity,
) -> str:
    rendered = ", ".join(signal.replace("_", " ") for signal in signals)
    if severity == "high":
        return f"Task touches runtime-critical SLO signals requiring explicit guardrails: {rendered}."
    return f"Task touches SLO-adjacent runtime or operational signals: {rendered}."


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
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
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _task_context(task: Mapping[str, Any]) -> str:
    values = [text for _, text in _task_texts(task)]
    values.extend(_strings(task.get("files_or_modules") or task.get("files")))
    values.extend(text for _, text in _metadata_texts(task.get("metadata")))
    return " ".join(values)


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
    signals: dict[SloImpactSignal, list[str]],
    signal: SloImpactSignal,
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
    "SloImpactSeverity",
    "SloImpactSignal",
    "TaskSloImpactFinding",
    "TaskSloImpactPlan",
    "build_task_slo_impact_plan",
    "recommend_task_slo_impacts",
    "task_slo_impact_plan_to_dict",
    "task_slo_impact_plan_to_markdown",
]
