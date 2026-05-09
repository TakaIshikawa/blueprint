"""Generate monitoring strategy matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from blueprint.domain.models import ExecutionPlan, ExecutionTask


MonitoringCoverageScore = Literal["comprehensive", "partial", "minimal"]

_SCORE_ORDER: dict[MonitoringCoverageScore, int] = {"minimal": 0, "partial": 1, "comprehensive": 2}

# Pattern matching for monitoring strategy signals
_METRICS_COLLECTION_RE = re.compile(
    r"\b(?:metrics?\s+collection|collect\s+metrics?|gather\s+metrics?|"
    r"(?:cpu|memory|disk|network|latency|throughput)\s+metrics?|"
    r"(?:custom|business|application)\s+metrics?|instrumentation|telemetry)\b",
    re.I,
)
_ALERTING_RULES_RE = re.compile(
    r"\b(?:alerting\s+rules?|alert\s+rules?|alerting\s+(?:configuration|setup)|"
    r"(?:configure|define|set\s+up)\s+alerts?|alert\s+thresholds?|"
    r"alert\s+(?:policy|policies)|pagerduty|opsgenie)\b",
    re.I,
)
_DASHBOARD_REQUIREMENTS_RE = re.compile(
    r"\b(?:dashboards?|visualization|(?:grafana|datadog|cloudwatch)\s+dashboards?|"
    r"monitoring\s+dashboards?|metrics?\s+dashboards?|"
    r"(?:create|build)\s+dashboards?|observability\s+dashboards?)\b",
    re.I,
)
_LOG_AGGREGATION_RE = re.compile(
    r"\b(?:log\s+aggregation|aggregate\s+logs?|centralized\s+logging|"
    r"(?:elk|splunk|cloudwatch|datadog)\s+logs?|log\s+(?:collection|shipping)|"
    r"structured\s+logging|log\s+(?:management|analysis))\b",
    re.I,
)
_TRACING_STRATEGY_RE = re.compile(
    r"\b(?:tracing\s+strateg(?:y|ies)|distributed\s+tracing|"
    r"(?:jaeger|zipkin|opentelemetry|otel|x-ray)\s+tracing|"
    r"trace\s+(?:context|propagation|sampling)|request\s+tracing)\b",
    re.I,
)
_BLIND_SPOTS_RE = re.compile(
    r"\b(?:blind\s+spots?|monitoring\s+(?:gaps?|coverage\s+gaps?)|"
    r"(?:missing|lacking)\s+(?:monitoring|observability|visibility)|"
    r"unmonitored|unobserved|visibility\s+gaps?)\b",
    re.I,
)
_NOISY_ALERTS_RE = re.compile(
    r"\b(?:noisy\s+alerts?|alert\s+noise|alert\s+fatigue|"
    r"(?:false|spurious)\s+alerts?|reduce\s+alert\s+noise|"
    r"alert\s+(?:tuning|optimization)|flapping\s+alerts?)\b",
    re.I,
)
_SLI_DEFINITION_RE = re.compile(
    r"\b(?:sli|service\s+level\s+indicators?|"
    r"define\s+sli|sli\s+(?:definition|metrics?)|"
    r"key\s+(?:performance\s+)?indicators?|kpi)\b",
    re.I,
)
_RETENTION_POLICY_RE = re.compile(
    r"\b(?:retention\s+(?:policy|policies)|data\s+retention|"
    r"metrics?\s+retention|logs?\s+retention|"
    r"retention\s+(?:period|duration|window)|storage\s+retention)\b",
    re.I,
)
_INCIDENT_DETECTION_RE = re.compile(
    r"\b(?:incident\s+detection|detect\s+incidents?|anomaly\s+detection|"
    r"(?:detect|identify)\s+(?:anomalies|issues|problems)|"
    r"outlier\s+detection|incident\s+(?:response|management))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class MonitoringStrategyMatrixRow:
    """Monitoring strategy signals for one execution task."""

    task_id: str
    title: str
    metrics_collection: str = "missing"
    alerting_rules: str = "missing"
    dashboards: str = "missing"
    log_aggregation: str = "missing"
    tracing_strategy: str = "missing"
    blind_spots_identified: str = "missing"
    noisy_alerts_addressed: str = "missing"
    sli_defined: str = "missing"
    retention_policy: str = "missing"
    incident_detection: str = "missing"
    coverage_score: MonitoringCoverageScore = "minimal"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "metrics_collection": self.metrics_collection,
            "alerting_rules": self.alerting_rules,
            "dashboards": self.dashboards,
            "log_aggregation": self.log_aggregation,
            "tracing_strategy": self.tracing_strategy,
            "blind_spots_identified": self.blind_spots_identified,
            "noisy_alerts_addressed": self.noisy_alerts_addressed,
            "sli_defined": self.sli_defined,
            "retention_policy": self.retention_policy,
            "incident_detection": self.incident_detection,
            "coverage_score": self.coverage_score,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class MonitoringStrategyMatrix:
    """Monitoring strategy matrix for an execution plan."""

    plan_id: str | None = None
    rows: tuple[MonitoringStrategyMatrixRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible representation."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_markdown(self) -> str:
        """Render the matrix as Markdown."""
        title = "# Monitoring Strategy Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"

        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Tasks analyzed: {self.summary.get('task_count', 0)}",
            f"- Comprehensive coverage: {self.summary.get('comprehensive_count', 0)}",
            f"- Partial coverage: {self.summary.get('partial_count', 0)}",
            f"- Minimal coverage: {self.summary.get('minimal_count', 0)}",
            f"- Overall coverage: {self.summary.get('overall_coverage', 0)}%",
        ]

        if not self.rows:
            lines.extend(["", "No monitoring strategy signals detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                "| Task | Metrics | Alerts | Dashboards | Logs | Tracing | SLI | Incident Detection | Score |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )

        for row in self.rows:
            lines.append(
                f"| {_markdown_cell(row.title)} | "
                f"{_markdown_cell(row.metrics_collection)} | "
                f"{_markdown_cell(row.alerting_rules)} | "
                f"{_markdown_cell(row.dashboards)} | "
                f"{_markdown_cell(row.log_aggregation)} | "
                f"{_markdown_cell(row.tracing_strategy)} | "
                f"{_markdown_cell(row.sli_defined)} | "
                f"{_markdown_cell(row.incident_detection)} | "
                f"{row.coverage_score} |"
            )

        return "\n".join(lines)


def generate_monitoring_strategy_matrix(
    plan: ExecutionPlan | Mapping[str, Any] | str,
) -> MonitoringStrategyMatrix:
    """Generate monitoring strategy matrix from execution plan."""
    plan_id, tasks = _extract_plan_data(plan)
    rows = tuple(_analyze_task(task) for task in tasks)
    summary = _calculate_summary(rows)

    return MonitoringStrategyMatrix(
        plan_id=plan_id,
        rows=rows,
        summary=summary,
    )


def _extract_plan_data(plan: ExecutionPlan | Mapping[str, Any] | str) -> tuple[str | None, list[Mapping[str, Any]]]:
    """Extract plan ID and tasks from various input formats."""
    if isinstance(plan, ExecutionPlan):
        return plan.id, [task.model_dump() for task in plan.tasks]
    if isinstance(plan, Mapping):
        plan_id = plan.get("id") or plan.get("plan_id")
        tasks = plan.get("tasks", [])
        return str(plan_id) if plan_id else None, tasks
    return None, []


def _analyze_task(task: Mapping[str, Any]) -> MonitoringStrategyMatrixRow:
    """Analyze monitoring strategy signals in a task."""
    task_id = str(task.get("id", "unknown"))
    title = str(task.get("title", "Untitled"))

    text = _extract_searchable_text(task)
    evidence_list: list[str] = []

    metrics_collection = _check_signal(_METRICS_COLLECTION_RE, text, evidence_list)
    alerting_rules = _check_signal(_ALERTING_RULES_RE, text, evidence_list)
    dashboards = _check_signal(_DASHBOARD_REQUIREMENTS_RE, text, evidence_list)
    log_aggregation = _check_signal(_LOG_AGGREGATION_RE, text, evidence_list)
    tracing_strategy = _check_signal(_TRACING_STRATEGY_RE, text, evidence_list)
    blind_spots = _check_signal(_BLIND_SPOTS_RE, text, evidence_list)
    noisy_alerts = _check_signal(_NOISY_ALERTS_RE, text, evidence_list)
    sli_defined = _check_signal(_SLI_DEFINITION_RE, text, evidence_list)
    retention_policy = _check_signal(_RETENTION_POLICY_RE, text, evidence_list)
    incident_detection = _check_signal(_INCIDENT_DETECTION_RE, text, evidence_list)

    present_count = sum(
        1
        for signal in [
            metrics_collection,
            alerting_rules,
            dashboards,
            log_aggregation,
            tracing_strategy,
            blind_spots,
            noisy_alerts,
            sli_defined,
            retention_policy,
            incident_detection,
        ]
        if signal == "present"
    )

    if present_count >= 7:
        coverage_score: MonitoringCoverageScore = "comprehensive"
    elif present_count >= 4:
        coverage_score = "partial"
    else:
        coverage_score = "minimal"

    return MonitoringStrategyMatrixRow(
        task_id=task_id,
        title=title,
        metrics_collection=metrics_collection,
        alerting_rules=alerting_rules,
        dashboards=dashboards,
        log_aggregation=log_aggregation,
        tracing_strategy=tracing_strategy,
        blind_spots_identified=blind_spots,
        noisy_alerts_addressed=noisy_alerts,
        sli_defined=sli_defined,
        retention_policy=retention_policy,
        incident_detection=incident_detection,
        coverage_score=coverage_score,
        evidence=tuple(evidence_list[:5]),
    )


def _extract_searchable_text(task: Mapping[str, Any]) -> str:
    """Extract all searchable text from a task."""
    parts: list[str] = []
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = task.get(field)
        if isinstance(value, str):
            parts.append(value)
    for field in ("acceptance_criteria", "requirements", "notes"):
        value = task.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)
    return " ".join(parts)


def _check_signal(pattern: re.Pattern[str], text: str, evidence_list: list[str]) -> str:
    """Check if pattern is present in text and collect evidence."""
    match = pattern.search(text)
    if match:
        evidence_list.append(match.group()[:50])
        return "present"
    return "missing"


def _calculate_summary(rows: tuple[MonitoringStrategyMatrixRow, ...]) -> dict[str, Any]:
    """Calculate summary statistics for the matrix."""
    if not rows:
        return {
            "task_count": 0,
            "comprehensive_count": 0,
            "partial_count": 0,
            "minimal_count": 0,
            "overall_coverage": 0,
        }

    score_counts = {"comprehensive": 0, "partial": 0, "minimal": 0}
    for row in rows:
        score_counts[row.coverage_score] += 1

    overall_coverage = int((score_counts["comprehensive"] + score_counts["partial"] * 0.5) / len(rows) * 100)

    return {
        "task_count": len(rows),
        "comprehensive_count": score_counts["comprehensive"],
        "partial_count": score_counts["partial"],
        "minimal_count": score_counts["minimal"],
        "overall_coverage": overall_coverage,
    }


def _markdown_cell(text: str) -> str:
    """Escape text for Markdown table cells."""
    return text.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "MonitoringStrategyMatrixRow",
    "MonitoringStrategyMatrix",
    "generate_monitoring_strategy_matrix",
]
