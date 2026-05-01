"""Plan telemetry requirements for rollout-sensitive execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


TelemetryStatus = Literal[
    "telemetry_required",
    "telemetry_optional",
    "telemetry_not_needed",
]
RolloutTelemetrySignal = Literal[
    "rollout",
    "deployment",
    "migration",
    "feature_flag",
    "experiment",
    "performance",
    "queue",
    "user_workflow",
]
TelemetrySignalType = Literal[
    "metric",
    "log",
    "trace",
    "dashboard",
    "alert",
    "success_indicator",
    "failure_indicator",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_NON_OPERATIONAL_RE = re.compile(
    r"\b(?:docs?|documentation|readme|adr|guide|changelog|"
    r"unit tests?|integration tests?|e2e tests?|test[- ]?only|fixtures?|mocks?|"
    r"specs?|snapshot)\b",
    re.IGNORECASE,
)
_OPERATIONAL_RE = re.compile(
    r"\b(?:rollout|deploy|deployment|migration|migrate|feature flag|experiment|"
    r"performance|latency|queue|workflow|customer|user-facing|production)\b",
    re.IGNORECASE,
)
_SIGNAL_PATTERNS: dict[RolloutTelemetrySignal, re.Pattern[str]] = {
    "rollout": re.compile(
        r"\b(?:rollout|roll out|canary|progressive delivery|phased launch|"
        r"gradual release|ramp|rollback|backout|release gate)\b",
        re.IGNORECASE,
    ),
    "deployment": re.compile(
        r"\b(?:deploy|deployment|release|production|prod|blue[- ]?green|"
        r"zero[- ]?downtime|post[- ]?deploy|pre[- ]?deploy)\b",
        re.IGNORECASE,
    ),
    "migration": re.compile(
        r"\b(?:migration|migrate|backfill|data move|schema change|dual[- ]?write|"
        r"dual[- ]?read|cutover|legacy|compatibility|version bump)\b",
        re.IGNORECASE,
    ),
    "feature_flag": re.compile(
        r"\b(?:feature flag|feature toggle|flagged|flag gate|kill switch|"
        r"launchdarkly|split\.io|flipper|enable flag|disable flag)\b",
        re.IGNORECASE,
    ),
    "experiment": re.compile(
        r"\b(?:experiment|ab test|a/b test|variant|cohort|treatment|control group|"
        r"experiment guardrail)\b",
        re.IGNORECASE,
    ),
    "performance": re.compile(
        r"\b(?:performance|latency|throughput|response time|p95|p99|slo|sla|"
        r"timeout|memory|cpu|load|cache|rate limit)\b",
        re.IGNORECASE,
    ),
    "queue": re.compile(
        r"\b(?:queue|queues|worker|workers|job|jobs|background task|consumer|"
        r"producer|dead letter|dlq|retry|backlog|lag)\b",
        re.IGNORECASE,
    ),
    "user_workflow": re.compile(
        r"\b(?:user-facing|customer-facing|workflow|checkout|signup|sign up|login|"
        r"onboarding|payment|billing|notification|email|webhook|journey)\b",
        re.IGNORECASE,
    ),
}
_SIGNAL_ORDER: dict[RolloutTelemetrySignal, int] = {
    "rollout": 0,
    "deployment": 1,
    "migration": 2,
    "feature_flag": 3,
    "experiment": 4,
    "performance": 5,
    "queue": 6,
    "user_workflow": 7,
}
_SIGNAL_TYPE_ORDER: dict[TelemetrySignalType, int] = {
    "metric": 0,
    "log": 1,
    "trace": 2,
    "dashboard": 3,
    "alert": 4,
    "success_indicator": 5,
    "failure_indicator": 6,
}


@dataclass(frozen=True, slots=True)
class TaskRolloutTelemetryRecord:
    """Telemetry planning guidance for one execution task."""

    task_id: str
    task_title: str
    telemetry_status: TelemetryStatus
    detected_signals: tuple[RolloutTelemetrySignal, ...]
    signal_types: tuple[TelemetrySignalType, ...]
    metrics: tuple[str, ...]
    logs: tuple[str, ...]
    traces: tuple[str, ...]
    dashboards: tuple[str, ...]
    alerts: tuple[str, ...]
    success_indicators: tuple[str, ...]
    failure_indicators: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "telemetry_status": self.telemetry_status,
            "detected_signals": list(self.detected_signals),
            "signal_types": list(self.signal_types),
            "metrics": list(self.metrics),
            "logs": list(self.logs),
            "traces": list(self.traces),
            "dashboards": list(self.dashboards),
            "alerts": list(self.alerts),
            "success_indicators": list(self.success_indicators),
            "failure_indicators": list(self.failure_indicators),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskRolloutTelemetryPlan:
    """Plan-level telemetry guidance and rollup counts."""

    plan_id: str | None = None
    records: tuple[TaskRolloutTelemetryRecord, ...] = field(default_factory=tuple)
    telemetry_required_task_ids: tuple[str, ...] = field(default_factory=tuple)
    telemetry_optional_task_ids: tuple[str, ...] = field(default_factory=tuple)
    telemetry_not_needed_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "telemetry_required_task_ids": list(self.telemetry_required_task_ids),
            "telemetry_optional_task_ids": list(self.telemetry_optional_task_ids),
            "telemetry_not_needed_task_ids": list(self.telemetry_not_needed_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return telemetry records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render telemetry guidance as deterministic Markdown."""
        title = "# Task Rollout Telemetry Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No execution tasks were found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Status | Signals | Metrics | Logs | Traces | Dashboards | Alerts | Success Indicators | Failure Indicators |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{record.telemetry_status} | "
                f"{_markdown_cell(', '.join(record.detected_signals))} | "
                f"{_markdown_cell('; '.join(record.metrics))} | "
                f"{_markdown_cell('; '.join(record.logs))} | "
                f"{_markdown_cell('; '.join(record.traces))} | "
                f"{_markdown_cell('; '.join(record.dashboards))} | "
                f"{_markdown_cell('; '.join(record.alerts))} | "
                f"{_markdown_cell('; '.join(record.success_indicators))} | "
                f"{_markdown_cell('; '.join(record.failure_indicators))} |"
            )
        return "\n".join(lines)


def build_task_rollout_telemetry_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskRolloutTelemetryPlan:
    """Build telemetry requirements for rollout-sensitive execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (_task_record(task, index) for index, task in enumerate(tasks, start=1)),
            key=lambda record: (record.task_id, record.task_title),
        )
    )
    telemetry_required_task_ids = tuple(
        record.task_id for record in records if record.telemetry_status == "telemetry_required"
    )
    telemetry_optional_task_ids = tuple(
        record.task_id for record in records if record.telemetry_status == "telemetry_optional"
    )
    telemetry_not_needed_task_ids = tuple(
        record.task_id for record in records if record.telemetry_status == "telemetry_not_needed"
    )
    status_counts = {
        status: sum(1 for record in records if record.telemetry_status == status)
        for status in ("telemetry_required", "telemetry_optional", "telemetry_not_needed")
    }
    signal_type_counts = {
        signal_type: sum(1 for record in records if signal_type in record.signal_types)
        for signal_type in _SIGNAL_TYPE_ORDER
    }
    rollout_signal_counts = {
        signal: sum(1 for record in records if signal in record.detected_signals)
        for signal in _SIGNAL_ORDER
    }

    return TaskRolloutTelemetryPlan(
        plan_id=plan_id,
        records=records,
        telemetry_required_task_ids=telemetry_required_task_ids,
        telemetry_optional_task_ids=telemetry_optional_task_ids,
        telemetry_not_needed_task_ids=telemetry_not_needed_task_ids,
        summary={
            "record_count": len(records),
            "telemetry_required_count": len(telemetry_required_task_ids),
            "telemetry_optional_count": len(telemetry_optional_task_ids),
            "telemetry_not_needed_count": len(telemetry_not_needed_task_ids),
            "status_counts": status_counts,
            "signal_type_counts": signal_type_counts,
            "rollout_signal_counts": rollout_signal_counts,
        },
    )


def task_rollout_telemetry_plan_to_dict(
    result: TaskRolloutTelemetryPlan,
) -> dict[str, Any]:
    """Serialize a rollout telemetry plan to a plain dictionary."""
    return result.to_dict()


task_rollout_telemetry_plan_to_dict.__test__ = False


def task_rollout_telemetry_plan_to_markdown(
    result: TaskRolloutTelemetryPlan,
) -> str:
    """Render a rollout telemetry plan as Markdown."""
    return result.to_markdown()


task_rollout_telemetry_plan_to_markdown.__test__ = False


def summarize_task_rollout_telemetry(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskRolloutTelemetryPlan:
    """Compatibility alias for building rollout telemetry plans."""
    return build_task_rollout_telemetry_plan(source)


def _task_record(task: Mapping[str, Any], index: int) -> TaskRolloutTelemetryRecord:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    detected = tuple(sorted(signals, key=lambda signal: _SIGNAL_ORDER[signal]))
    evidence = tuple(_dedupe(evidence for signal in detected for evidence in signals.get(signal, ())))
    status = _telemetry_status(task, detected, evidence)
    needs = _telemetry_needs(detected, status)

    return TaskRolloutTelemetryRecord(
        task_id=task_id,
        task_title=title,
        telemetry_status=status,
        detected_signals=detected,
        signal_types=tuple(
            signal_type
            for signal_type in _SIGNAL_TYPE_ORDER
            if needs[_needs_key(signal_type)]
        ),
        metrics=tuple(needs["metrics"]),
        logs=tuple(needs["logs"]),
        traces=tuple(needs["traces"]),
        dashboards=tuple(needs["dashboards"]),
        alerts=tuple(needs["alerts"]),
        success_indicators=tuple(needs["success_indicators"]),
        failure_indicators=tuple(needs["failure_indicators"]),
        evidence=evidence,
    )


def _signals(task: Mapping[str, Any]) -> dict[RolloutTelemetrySignal, tuple[str, ...]]:
    signals: dict[RolloutTelemetrySignal, list[str]] = {}

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _add_path_signals(signals, path)

    for source_field, text in _candidate_texts(task):
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text):
                _append(signals, signal, _evidence_snippet(source_field, text))

    return {
        signal: tuple(_dedupe(evidence))
        for signal, evidence in signals.items()
        if evidence
    }


def _add_path_signals(
    signals: dict[RolloutTelemetrySignal, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    evidence = f"files_or_modules: {original}"

    if bool({"deploy", "deployment", "deployments", "releases", ".github", "workflows"} & parts):
        _append(signals, "deployment", evidence)
    if bool({"migrations", "migration", "db", "database"} & parts) or "migration" in name:
        _append(signals, "migration", evidence)
    if any(token in normalized for token in ("feature_flag", "feature-flag", "flags", "launchdarkly")):
        _append(signals, "feature_flag", evidence)
    if bool({"experiments", "experiment", "ab_tests", "ab-tests"} & parts):
        _append(signals, "experiment", evidence)
    if any(token in normalized for token in ("perf", "performance", "latency", "cache")):
        _append(signals, "performance", evidence)
    if bool({"queues", "queue", "workers", "worker", "jobs", "job"} & parts):
        _append(signals, "queue", evidence)
    if bool({"ui", "frontend", "pages", "routes", "checkout", "billing", "auth"} & parts):
        _append(signals, "user_workflow", evidence)


def _telemetry_status(
    task: Mapping[str, Any],
    detected_signals: tuple[RolloutTelemetrySignal, ...],
    evidence: tuple[str, ...],
) -> TelemetryStatus:
    if not detected_signals:
        return "telemetry_not_needed"

    combined = " ".join(
        list(evidence)
        + [
            _optional_text(task.get("title")) or "",
            _optional_text(task.get("description")) or "",
        ]
    )
    non_operational = _is_non_operational_task(task, combined)
    operational = bool(_OPERATIONAL_RE.search(combined))
    if non_operational and not operational:
        return "telemetry_not_needed"
    if non_operational:
        return "telemetry_optional"
    return "telemetry_required"


def _is_non_operational_task(task: Mapping[str, Any], combined_evidence: str) -> bool:
    path_text = " ".join(_strings(task.get("files_or_modules") or task.get("files")))
    normalized_paths = path_text.casefold()
    doc_or_test_path = bool(
        re.search(r"(?:^|/)(?:docs?|test|tests|spec|specs|fixtures?)(?:/|$)", normalized_paths)
        or re.search(r"\.(?:md|mdx|rst|txt)$", normalized_paths)
    )
    return doc_or_test_path or bool(_NON_OPERATIONAL_RE.search(combined_evidence))


def _telemetry_needs(
    detected_signals: tuple[RolloutTelemetrySignal, ...],
    status: TelemetryStatus,
) -> dict[str, list[str]]:
    if status == "telemetry_not_needed":
        return {
            "metrics": [],
            "logs": [],
            "traces": [],
            "dashboards": [],
            "alerts": [],
            "success_indicators": [],
            "failure_indicators": [],
        }

    needs = {
        "metrics": ["Track task-specific adoption, error rate, latency, and throughput before and after the change."],
        "logs": ["Emit structured logs with task identifier, rollout cohort, outcome, and rollback state."],
        "traces": ["Propagate trace spans through the changed path with attributes for rollout state and failure reason."],
        "dashboards": ["Create or update a dashboard that compares baseline and post-change health for this task."],
        "alerts": ["Define alerts for elevated errors, latency regression, stalled rollout, or rollback trigger conditions."],
        "success_indicators": ["Success: health metrics stay within baseline while intended adoption or completion increases."],
        "failure_indicators": ["Failure: errors, latency, retries, backlog, or user drop-off exceed the rollback threshold."],
    }

    if status == "telemetry_optional":
        needs["metrics"] = ["Note the existing metric or dashboard that would verify this non-operational task if needed."]
        needs["logs"] = ["Record whether existing logs are sufficient; add no new logging unless operational behavior changes."]
        needs["traces"] = []
        needs["dashboards"] = []
        needs["alerts"] = []
        needs["success_indicators"] = ["Success: documentation, tests, or validation notes identify existing observability coverage."]
        needs["failure_indicators"] = []
        return needs

    _add_signal_specific_needs(needs, detected_signals)
    return {key: _dedupe(value) for key, value in needs.items()}


def _add_signal_specific_needs(
    needs: dict[str, list[str]],
    detected_signals: tuple[RolloutTelemetrySignal, ...],
) -> None:
    if "rollout" in detected_signals or "feature_flag" in detected_signals:
        needs["metrics"].append("Track rollout percentage, enabled users, exposure count, and flag evaluation errors.")
        needs["alerts"].append("Alert when canary cohorts breach error, latency, or rollback guardrail thresholds.")
        needs["success_indicators"].append("Success: rollout reaches the planned cohort with no guardrail breach.")
        needs["failure_indicators"].append("Failure: canary or flag cohort breaches rollback guardrails.")
    if "deployment" in detected_signals:
        needs["metrics"].append("Track deploy health, version adoption, restart count, and post-deploy error budget burn.")
        needs["logs"].append("Log deployment version, environment, migration step, and rollback action for changed services.")
        needs["dashboards"].append("Show deploy version, service health, error budget burn, and rollback status together.")
    if "migration" in detected_signals:
        needs["metrics"].append("Track migration progress, remaining records, mismatch count, and dual-read or dual-write errors.")
        needs["logs"].append("Log migration batch id, record counts, skipped records, retries, and reconciliation outcome.")
        needs["traces"].append("Trace old and new code paths during migration to compare latency and failure modes.")
        needs["alerts"].append("Alert on migration stalls, reconciliation mismatches, duplicate writes, or rollback conditions.")
    if "experiment" in detected_signals:
        needs["metrics"].append("Track experiment exposure, conversion, guardrail metrics, and variant assignment errors.")
        needs["logs"].append("Log anonymized experiment key, variant, cohort, and conversion outcome.")
        needs["success_indicators"].append("Success: primary metric improves without guardrail degradation.")
    if "performance" in detected_signals:
        needs["metrics"].append("Track p50, p95, p99 latency, throughput, saturation, timeout rate, and resource use.")
        needs["alerts"].append("Alert when latency, saturation, timeout rate, or error budget burn exceeds baseline thresholds.")
    if "queue" in detected_signals:
        needs["metrics"].append("Track queue depth, age of oldest item, consumer lag, retry count, DLQ count, and processing time.")
        needs["logs"].append("Log job id, queue name, attempt count, processing result, and dead-letter reason.")
        needs["traces"].append("Trace enqueue, dequeue, processing, and downstream calls with queue and job attributes.")
        needs["alerts"].append("Alert on backlog growth, stale jobs, retry storms, or dead-letter spikes.")
    if "user_workflow" in detected_signals:
        needs["metrics"].append("Track workflow starts, completions, abandonment, user-visible errors, and support-impacting failures.")
        needs["traces"].append("Trace the full user workflow across frontend, API, worker, and third-party boundaries.")
        needs["dashboards"].append("Expose workflow conversion, user-visible errors, latency, and dependency health.")


def _needs_key(signal_type: TelemetrySignalType) -> str:
    return {
        "metric": "metrics",
        "log": "logs",
        "trace": "traces",
        "dashboard": "dashboards",
        "alert": "alerts",
        "success_indicator": "success_indicators",
        "failure_indicator": "failure_indicators",
    }[signal_type]


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
    for field_name in ("acceptance_criteria", "tags", "labels"):
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
        | Iterable[Mapping[str, Any] | ExecutionTask]
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

    try:
        iterator = iter(source)
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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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
    signals: dict[RolloutTelemetrySignal, list[str]],
    signal: RolloutTelemetrySignal,
    evidence: str,
) -> None:
    signals.setdefault(signal, []).append(evidence)


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
    "RolloutTelemetrySignal",
    "TaskRolloutTelemetryPlan",
    "TaskRolloutTelemetryRecord",
    "TelemetrySignalType",
    "TelemetryStatus",
    "build_task_rollout_telemetry_plan",
    "summarize_task_rollout_telemetry",
    "task_rollout_telemetry_plan_to_dict",
    "task_rollout_telemetry_plan_to_markdown",
]
