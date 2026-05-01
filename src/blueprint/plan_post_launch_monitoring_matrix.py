"""Build plan-level post-launch monitoring matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


PostLaunchMonitoringSignalType = Literal[
    "metrics",
    "logs",
    "alerts",
    "dashboards",
    "slo",
    "conversions",
    "background_jobs",
    "integrations",
    "migrations",
    "rollouts",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: dict[PostLaunchMonitoringSignalType, int] = {
    "metrics": 0,
    "logs": 1,
    "alerts": 2,
    "dashboards": 3,
    "slo": 4,
    "conversions": 5,
    "background_jobs": 6,
    "integrations": 7,
    "migrations": 8,
    "rollouts": 9,
}
_TEXT_SIGNAL_PATTERNS: tuple[tuple[PostLaunchMonitoringSignalType, re.Pattern[str]], ...] = (
    ("metrics", re.compile(r"\b(?:metric|metrics|kpi|telemetry|instrumentation|counter)\b", re.I)),
    ("logs", re.compile(r"\b(?:log|logs|logging|audit log|structured log|trace|traces|tracing)\b", re.I)),
    ("alerts", re.compile(r"\b(?:alert|alerts|alerting|alarm|pager|page|paging|opsgenie|pagerduty)\b", re.I)),
    ("dashboards", re.compile(r"\b(?:dashboard|dashboards|chart|report|looker|mode|metabase|grafana|datadog)\b", re.I)),
    ("slo", re.compile(r"\b(?:slo|sla|service level|error budget|burn rate|latency objective|availability)\b", re.I)),
    ("conversions", re.compile(r"\b(?:conversion|conversions|funnel|checkout|signup|activation|adoption|drop[- ]?off)\b", re.I)),
    (
        "background_jobs",
        re.compile(r"\b(?:background job|worker|queue|consumer|producer|cron|scheduled job|batch job|etl|sync job|dead letter|retry)\b", re.I),
    ),
    (
        "integrations",
        re.compile(r"\b(?:integration|external|third[- ]party|vendor|partner|webhook|oauth|api provider|dependency|stripe|salesforce|slack|twilio)\b", re.I),
    ),
    (
        "migrations",
        re.compile(r"\b(?:migration|migrate|schema|ddl|backfill|data migration|database migration|cutover|dual write)\b", re.I),
    ),
    (
        "rollouts",
        re.compile(r"\b(?:rollout|roll out|launch|release|go[- ]live|canary|wave|phase|ramp|feature flag|staged rollout|beta)\b", re.I),
    ),
)
_PATH_SIGNAL_PATTERNS: tuple[tuple[PostLaunchMonitoringSignalType, re.Pattern[str]], ...] = (
    ("metrics", re.compile(r"(?:^|/)(?:metrics?|telemetry|instrumentation)(?:/|$)|metrics?", re.I)),
    ("logs", re.compile(r"(?:^|/)(?:logs?|logging|traces?|tracing)(?:/|$)|logging?", re.I)),
    ("alerts", re.compile(r"(?:^|/)(?:alerts?|alerting|monitors?|pagerduty|opsgenie)(?:/|$)|alerts?", re.I)),
    ("dashboards", re.compile(r"(?:^|/)(?:dashboards?|reports?|grafana|looker)(?:/|$)|dashboards?", re.I)),
    ("slo", re.compile(r"(?:^|/)(?:slo|slos|sla|error-budget)(?:/|$)|slo", re.I)),
    ("conversions", re.compile(r"(?:^|/)(?:funnels?|conversion|checkout|signup|activation)(?:/|$)", re.I)),
    ("background_jobs", re.compile(r"(?:^|/)(?:jobs?|workers?|queues?|cron|consumers?|producers?)(?:/|$)", re.I)),
    ("integrations", re.compile(r"(?:^|/)(?:integrations?|webhooks?|clients?|providers?|oauth)(?:/|$)", re.I)),
    ("migrations", re.compile(r"(?:^|/)(?:migrations?|schema|backfills?)(?:/|$)|\.sql$", re.I)),
    ("rollouts", re.compile(r"(?:^|/)(?:rollouts?|releases?|flags?|experiments?)(?:/|$)", re.I)),
)
_NEGATIVE_RE = re.compile(
    r"\b(?:docs?|documentation|readme|copy|typo|formatting|comment-only|style-only|test fixture|mock data)\b",
    re.I,
)
_OWNER_KEYS = (
    "owner",
    "owners",
    "owner_hint",
    "owner_hints",
    "owner_type",
    "assignee",
    "assignees",
    "dri",
    "oncall",
    "on_call",
    "team",
)
_DASHBOARD_ALERT_KEYS = (
    "dashboard",
    "dashboards",
    "alert",
    "alerts",
    "monitor",
    "monitors",
    "monitoring",
    "required_dashboard",
    "required_dashboards",
    "required_alert",
    "required_alerts",
)
_TIMING_KEYS = ("first_check", "first_check_timing", "monitoring_window", "watch_window", "launch_watch")
_ROLLBACK_KEYS = (
    "rollback_trigger",
    "rollback_triggers",
    "rollback_trigger_notes",
    "rollback",
    "kill_switch",
    "revert_when",
)


@dataclass(frozen=True, slots=True)
class PlanPostLaunchMonitoringSignal:
    """Monitoring requirement for one post-launch signal type."""

    signal_type: PostLaunchMonitoringSignalType
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    required_dashboards_or_alerts: tuple[str, ...] = field(default_factory=tuple)
    first_check_timing: str = ""
    owner_hints: tuple[str, ...] = field(default_factory=tuple)
    rollback_trigger_notes: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "signal_type": self.signal_type,
            "affected_task_ids": list(self.affected_task_ids),
            "required_dashboards_or_alerts": list(self.required_dashboards_or_alerts),
            "first_check_timing": self.first_check_timing,
            "owner_hints": list(self.owner_hints),
            "rollback_trigger_notes": list(self.rollback_trigger_notes),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanPostLaunchMonitoringMatrix:
    """Plan-level post-launch monitoring matrix and rollup counts."""

    plan_id: str | None = None
    rows: tuple[PlanPostLaunchMonitoringSignal, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return monitoring rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    @property
    def records(self) -> tuple[PlanPostLaunchMonitoringSignal, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_markdown(self) -> str:
        """Render the post-launch monitoring matrix as deterministic Markdown."""
        title = "# Plan Post-Launch Monitoring Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        signal_counts = self.summary.get("signal_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Monitoring signal count: {self.summary.get('monitoring_signal_count', 0)}",
            f"- Covered task count: {self.summary.get('covered_task_count', 0)}",
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
        ]
        if not self.rows:
            lines.extend(["", "No post-launch monitoring signals were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Signal | Tasks | Dashboards/Alerts | First Check | Owners | Rollback Triggers | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.signal_type} | "
                f"{_markdown_cell(', '.join(row.affected_task_ids) or 'plan metadata')} | "
                f"{_markdown_cell('; '.join(row.required_dashboards_or_alerts) or 'none')} | "
                f"{_markdown_cell(row.first_check_timing or 'none')} | "
                f"{_markdown_cell('; '.join(row.owner_hints) or 'none')} | "
                f"{_markdown_cell('; '.join(row.rollback_trigger_notes) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def generate_plan_post_launch_monitoring_matrix(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> PlanPostLaunchMonitoringMatrix:
    """Derive post-launch monitoring requirements from an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    grouped: dict[PostLaunchMonitoringSignalType, dict[str, list[str]]] = {
        signal: {
            "task_ids": [],
            "dashboards_or_alerts": [],
            "timings": [],
            "owners": [],
            "rollback_triggers": [],
            "evidence": [],
            "texts": [],
        }
        for signal in _SIGNAL_ORDER
    }

    for source_field, text in _plan_candidate_texts(plan):
        matched_signals = _text_signals(text)
        for signal in matched_signals:
            grouped[signal]["evidence"].append(_evidence_snippet(f"plan.{source_field}", text))
            grouped[signal]["texts"].append(text)

    plan_metadata = plan.get("metadata")
    plan_defaults = _monitoring_metadata_values(plan_metadata if isinstance(plan_metadata, Mapping) else {})
    for signal in _metadata_signals(plan_metadata if isinstance(plan_metadata, Mapping) else {}):
        grouped[signal]["dashboards_or_alerts"].extend(plan_defaults["dashboards_or_alerts"])
        grouped[signal]["timings"].extend(plan_defaults["timings"])
        grouped[signal]["owners"].extend(plan_defaults["owners"])
        grouped[signal]["rollback_triggers"].extend(plan_defaults["rollback_triggers"])

    for index, task in enumerate(tasks, start=1):
        signals, values = _task_signals(task)
        if signals and _suppressed(task):
            continue
        task_id = _task_id(task, index)
        for signal in signals:
            grouped[signal]["task_ids"].append(task_id)
            grouped[signal]["dashboards_or_alerts"].extend(values["dashboards_or_alerts"])
            grouped[signal]["timings"].extend(values["timings"])
            grouped[signal]["owners"].extend(values["owners"])
            grouped[signal]["rollback_triggers"].extend(values["rollback_triggers"])
            grouped[signal]["evidence"].extend(values["evidence"])
            grouped[signal]["texts"].extend(values["texts"])

    rows = tuple(
        _row(signal, grouped[signal])
        for signal in _SIGNAL_ORDER
        if grouped[signal]["task_ids"] or grouped[signal]["evidence"] or grouped[signal]["dashboards_or_alerts"]
    )
    covered_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.affected_task_ids))
    signal_counts = {signal: sum(1 for row in rows if row.signal_type == signal) for signal in _SIGNAL_ORDER}
    return PlanPostLaunchMonitoringMatrix(
        plan_id=_optional_text(plan.get("id")),
        rows=rows,
        summary={
            "task_count": len(tasks),
            "monitoring_signal_count": len(rows),
            "covered_task_count": len(covered_task_ids),
            "rollback_trigger_count": sum(len(row.rollback_trigger_notes) for row in rows),
            "signal_counts": signal_counts,
            "covered_task_ids": list(covered_task_ids),
        },
    )


def build_plan_post_launch_monitoring_matrix(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> PlanPostLaunchMonitoringMatrix:
    """Compatibility alias for generating post-launch monitoring matrices."""
    return generate_plan_post_launch_monitoring_matrix(source)


def derive_plan_post_launch_monitoring_matrix(
    source: Mapping[str, Any] | ExecutionPlan | PlanPostLaunchMonitoringMatrix | object,
) -> PlanPostLaunchMonitoringMatrix:
    """Return an existing matrix or generate one from a plan-shaped source."""
    if isinstance(source, PlanPostLaunchMonitoringMatrix):
        return source
    return generate_plan_post_launch_monitoring_matrix(source)


def summarize_plan_post_launch_monitoring(
    source: Mapping[str, Any] | ExecutionPlan | PlanPostLaunchMonitoringMatrix | object,
) -> PlanPostLaunchMonitoringMatrix:
    """Compatibility alias for post-launch monitoring summaries."""
    return derive_plan_post_launch_monitoring_matrix(source)


def plan_post_launch_monitoring_matrix_to_dict(
    matrix: PlanPostLaunchMonitoringMatrix,
) -> dict[str, Any]:
    """Serialize a post-launch monitoring matrix to a plain dictionary."""
    return matrix.to_dict()


plan_post_launch_monitoring_matrix_to_dict.__test__ = False


def plan_post_launch_monitoring_matrix_to_markdown(
    matrix: PlanPostLaunchMonitoringMatrix,
) -> str:
    """Render a post-launch monitoring matrix as Markdown."""
    return matrix.to_markdown()


plan_post_launch_monitoring_matrix_to_markdown.__test__ = False


def _task_signals(
    task: Mapping[str, Any],
) -> tuple[tuple[PostLaunchMonitoringSignalType, ...], dict[str, list[str]]]:
    signals: set[PostLaunchMonitoringSignalType] = set()
    evidence: list[str] = []
    texts: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        path_text = _path_text(normalized)
        matched = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS:
            if pattern.search(normalized) or pattern.search(path_text):
                signals.add(signal)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")
            texts.append(path)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        signals.update(_metadata_signals(metadata))

    for source_field, text in _candidate_texts(task):
        matched_signals = _text_signals(text)
        if matched_signals:
            signals.update(matched_signals)
            evidence.append(_evidence_snippet(source_field, text))
            texts.append(text)

    metadata_values = _monitoring_metadata_values(metadata if isinstance(metadata, Mapping) else {})
    return (
        tuple(signal for signal in _SIGNAL_ORDER if signal in signals),
        {
            "dashboards_or_alerts": _dedupe(metadata_values["dashboards_or_alerts"]),
            "timings": _dedupe(metadata_values["timings"]),
            "owners": _dedupe([*metadata_values["owners"], *_owner_hints(task)]),
            "rollback_triggers": _dedupe(metadata_values["rollback_triggers"]),
            "evidence": _dedupe(evidence),
            "texts": _dedupe(texts),
        },
    )


def _row(
    signal: PostLaunchMonitoringSignalType,
    values: Mapping[str, list[str]],
) -> PlanPostLaunchMonitoringSignal:
    task_ids = tuple(sorted(_dedupe(values.get("task_ids", ()))))
    dashboards_or_alerts = tuple(_dedupe(values.get("dashboards_or_alerts", ())) or _default_dashboards_or_alerts(signal))
    timings = tuple(_dedupe(values.get("timings", ())))
    owners = tuple(_dedupe(values.get("owners", ())) or _default_owner_hints(signal))
    rollback_triggers = tuple(
        _dedupe(values.get("rollback_triggers", ())) or _default_rollback_triggers(signal)
    )
    return PlanPostLaunchMonitoringSignal(
        signal_type=signal,
        affected_task_ids=task_ids,
        required_dashboards_or_alerts=dashboards_or_alerts,
        first_check_timing=("; ".join(timings) if timings else _default_first_check_timing(signal)),
        owner_hints=owners,
        rollback_trigger_notes=rollback_triggers,
        evidence=tuple(_dedupe(values.get("evidence", ()))),
    )


def _text_signals(text: str) -> tuple[PostLaunchMonitoringSignalType, ...]:
    return tuple(signal for signal, pattern in _TEXT_SIGNAL_PATTERNS if pattern.search(text))


def _metadata_signals(metadata: Mapping[str, Any]) -> tuple[PostLaunchMonitoringSignalType, ...]:
    signals: list[PostLaunchMonitoringSignalType] = []
    for key, value in _walk_metadata(metadata):
        normalized_key = key.casefold().replace("-", "_").replace(" ", "_")
        if normalized_key in {"monitoring_signal", "monitoring_signals", "signal", "signals"}:
            for item in _strings(value):
                normalized = item.casefold().replace("-", "_").replace(" ", "_")
                if normalized in _SIGNAL_ORDER:
                    signals.append(normalized)  # type: ignore[arg-type]
                elif normalized in {"conversion", "funnel", "funnels"}:
                    signals.append("conversions")
                elif normalized in {"job", "jobs", "worker", "queue"}:
                    signals.append("background_jobs")
                elif normalized in {"migration", "schema_migration"}:
                    signals.append("migrations")
                elif normalized in {"rollout", "release"}:
                    signals.append("rollouts")
    return tuple(_dedupe(signals))


def _monitoring_metadata_values(metadata: Mapping[str, Any]) -> dict[str, list[str]]:
    return {
        "dashboards_or_alerts": _metadata_key_values(metadata, _DASHBOARD_ALERT_KEYS),
        "timings": _metadata_key_values(metadata, _TIMING_KEYS),
        "owners": _metadata_key_values(metadata, _OWNER_KEYS),
        "rollback_triggers": _metadata_key_values(metadata, _ROLLBACK_KEYS),
    }


def _metadata_key_values(metadata: Mapping[str, Any], keys: tuple[str, ...]) -> list[str]:
    values: list[str] = []
    wanted = {key.casefold() for key in keys}
    for key, value in _walk_metadata(metadata):
        normalized = key.casefold().replace("-", "_").replace(" ", "_")
        if normalized in wanted:
            values.extend(_strings(value))
    return values


def _walk_metadata(value: Mapping[str, Any], prefix: str = "") -> list[tuple[str, Any]]:
    pairs: list[tuple[str, Any]] = []
    for key in sorted(value, key=lambda item: str(item)):
        key_text = str(key)
        child = value[key]
        pairs.append((key_text, child))
        if isinstance(child, Mapping):
            pairs.extend(_walk_metadata(child, f"{prefix}.{key_text}" if prefix else key_text))
    return pairs


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "depends_on", "dependencies", "tags", "labels", "notes"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        texts.append(("files_or_modules", path))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key, value in _walk_metadata(metadata):
            for text in _strings(value):
                texts.append((f"metadata.{key}", text))
    return texts


def _plan_candidate_texts(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "target_engine",
        "target_repo",
        "project_type",
        "test_strategy",
        "handoff_prompt",
        "generation_prompt",
        "status",
    ):
        if text := _optional_text(plan.get(field_name)):
            texts.append((field_name, text))
    return texts


def _default_dashboards_or_alerts(signal: PostLaunchMonitoringSignalType) -> tuple[str, ...]:
    defaults = {
        "metrics": ("Launch metrics dashboard",),
        "logs": ("Structured log query for changed path",),
        "alerts": ("Production health alert",),
        "dashboards": ("Launch health dashboard",),
        "slo": ("SLO burn-rate dashboard and alert",),
        "conversions": ("Conversion funnel dashboard",),
        "background_jobs": ("Queue depth, retry, and worker error alert",),
        "integrations": ("Dependency health dashboard and provider error alert",),
        "migrations": ("Migration progress and database health dashboard",),
        "rollouts": ("Rollout cohort health dashboard",),
    }
    return defaults[signal]


def _default_first_check_timing(signal: PostLaunchMonitoringSignalType) -> str:
    defaults = {
        "metrics": "Within 30 minutes after launch, then hourly during the first day.",
        "logs": "Within 15 minutes after launch, then during the first support handoff.",
        "alerts": "Immediately after deployment and again after the first alert evaluation window.",
        "dashboards": "At launch plus every rollout wave checkpoint.",
        "slo": "After the first burn-rate window and again before widening rollout.",
        "conversions": "After the first meaningful traffic window, then daily for the first week.",
        "background_jobs": "After the first scheduled run or queue drain cycle.",
        "integrations": "After the first live dependency call and at vendor status handoff.",
        "migrations": "After dry-run/live migration start, midpoint, and completion verification.",
        "rollouts": "At each canary, wave, or ramp percentage checkpoint.",
    }
    return defaults[signal]


def _default_owner_hints(signal: PostLaunchMonitoringSignalType) -> tuple[str, ...]:
    defaults = {
        "metrics": ("analytics owner", "feature owner"),
        "logs": ("service owner", "on-call owner"),
        "alerts": ("on-call owner", "SRE owner"),
        "dashboards": ("release owner", "analytics owner"),
        "slo": ("SRE owner", "service owner"),
        "conversions": ("product owner", "analytics owner"),
        "background_jobs": ("operations owner", "service owner"),
        "integrations": ("integration owner", "vendor escalation owner"),
        "migrations": ("database owner", "release owner"),
        "rollouts": ("release owner", "feature owner"),
    }
    return defaults[signal]


def _default_rollback_triggers(signal: PostLaunchMonitoringSignalType) -> tuple[str, ...]:
    defaults = {
        "metrics": ("Rollback if launch metrics materially regress from baseline.",),
        "logs": ("Rollback if new error logs or traces indicate customer-impacting failures.",),
        "alerts": ("Rollback if production health alerts fire or page volume exceeds the launch threshold.",),
        "dashboards": ("Rollback if the launch dashboard shows sustained health regression.",),
        "slo": ("Rollback if SLO burn rate or error budget impact exceeds the agreed threshold.",),
        "conversions": ("Rollback if conversion or funnel completion drops beyond the launch guardrail.",),
        "background_jobs": ("Rollback or pause if queue depth, retries, dead letters, or worker errors spike.",),
        "integrations": ("Rollback or disable dependency if provider errors, latency, or webhook failures spike.",),
        "migrations": ("Rollback or halt migration if database errors, lock waits, or data validation failures spike.",),
        "rollouts": ("Pause or roll back if canary or wave health falls below the rollout guardrail.",),
    }
    return defaults[signal]


def _owner_hints(task: Mapping[str, Any]) -> list[str]:
    hints: list[str] = []
    for key in _OWNER_KEYS:
        hints.extend(_strings(task.get(key)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        hints.extend(_metadata_key_values(metadata, _OWNER_KEYS))
    return hints


def _suppressed(task: Mapping[str, Any]) -> bool:
    text = " ".join(value for _, value in _candidate_texts(task))
    return bool(_NEGATIVE_RE.search(text))


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
    return value.replace("\\", "/").casefold()


def _path_text(path: str) -> str:
    return " ".join(part.replace("_", " ").replace("-", " ") for part in PurePosixPath(path).parts)


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
    "PlanPostLaunchMonitoringMatrix",
    "PlanPostLaunchMonitoringSignal",
    "PostLaunchMonitoringSignalType",
    "build_plan_post_launch_monitoring_matrix",
    "derive_plan_post_launch_monitoring_matrix",
    "generate_plan_post_launch_monitoring_matrix",
    "plan_post_launch_monitoring_matrix_to_dict",
    "plan_post_launch_monitoring_matrix_to_markdown",
    "summarize_plan_post_launch_monitoring",
]
