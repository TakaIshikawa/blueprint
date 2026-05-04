"""Build plan-level data quality monitoring matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_secrets_rotation_readiness_matrix import (
    _candidate_texts,
    _dedupe,
    _evidence_snippet,
    _markdown_cell,
    _optional_text,
    _source_payload,
    _task_id,
)


DataChangeSignal = Literal[
    "etl",
    "sync_job",
    "migration",
    "import",
    "export",
    "derived_table",
    "analytics_event",
    "backfill",
]
MonitoringSafeguard = Literal[
    "row_count_check",
    "freshness_check",
    "null_constraint_check",
    "duplicate_detection",
    "reconciliation",
    "alert_owner",
    "dashboard",
    "sampling",
    "rollback_path",
    "audit_evidence",
]

_SIGNAL_ORDER: tuple[DataChangeSignal, ...] = (
    "backfill",
    "etl",
    "sync_job",
    "migration",
    "import",
    "export",
    "derived_table",
    "analytics_event",
)
_SAFEGUARD_ORDER: tuple[MonitoringSafeguard, ...] = (
    "row_count_check",
    "freshness_check",
    "null_constraint_check",
    "duplicate_detection",
    "reconciliation",
    "alert_owner",
    "dashboard",
    "sampling",
    "rollback_path",
    "audit_evidence",
)

_SIGNAL_PATTERNS: dict[DataChangeSignal, re.Pattern[str]] = {
    "etl": re.compile(
        r"\b(?:etl|extract[- ]transform[- ]load|data pipeline|data ingestion|data processing)\b",
        re.I,
    ),
    "sync_job": re.compile(
        r"\b(?:sync|synchroniz(?:e|ation|ing)|replicat(?:e|ion|ing)|sync job)\b",
        re.I,
    ),
    "migration": re.compile(
        r"\b(?:migrat(?:e|ion|ing)|data migration|schema migration)\b",
        re.I,
    ),
    "import": re.compile(
        r"\b(?:import|data import|bulk import|ingest)\b",
        re.I,
    ),
    "export": re.compile(
        r"\b(?:export|data export|bulk export|extract)\b",
        re.I,
    ),
    "derived_table": re.compile(
        r"\b(?:derived table|materialized view|aggregate|aggregation|computed table|view)\b",
        re.I,
    ),
    "analytics_event": re.compile(
        r"\b(?:analytics events?|event tracking|telemetry|metrics events?|event data)\b",
        re.I,
    ),
    "backfill": re.compile(
        r"\b(?:backfill|back[- ]fill|historical data|historical load|data repair)\b",
        re.I,
    ),
}

_SAFEGUARD_PATTERNS: dict[MonitoringSafeguard, re.Pattern[str]] = {
    "row_count_check": re.compile(
        r"\b(?:row count|record count|count check|row validation|count assertion|total records)\b",
        re.I,
    ),
    "freshness_check": re.compile(
        r"\b(?:freshness|staleness|data freshness|freshness check|stale data|timestamp check|recency)\b",
        re.I,
    ),
    "null_constraint_check": re.compile(
        r"\b(?:null check|not null|no null|constraint check|data validation|schema validation|nullability|null values?)\b",
        re.I,
    ),
    "duplicate_detection": re.compile(
        r"\b(?:duplicate|dedup|deduplicat(?:e|ion)|unique constraint|uniqueness check|duplicate detection)\b",
        re.I,
    ),
    "reconciliation": re.compile(
        r"\b(?:reconcil(?:e|iation)|comparison|source target comparison|data diff|checksum|hash comparison)\b",
        re.I,
    ),
    "alert_owner": re.compile(
        r"\b(?:alert|notification|page|oncall|owner|dri|notify|escalat(?:e|ion)|pager)\b",
        re.I,
    ),
    "dashboard": re.compile(
        r"\b(?:dashboard|monitoring dashboard|metrics dashboard|grafana|datadog|observability)\b",
        re.I,
    ),
    "sampling": re.compile(
        r"\b(?:sampl(?:e|ing)|spot check|sample validation|random check|statistical check)\b",
        re.I,
    ),
    "rollback_path": re.compile(
        r"\b(?:rollback|revert|undo|correction|fix|remediat(?:e|ion)|recovery path)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit|evidence|log|audit trail|data lineage|compliance|change record)\b",
        re.I,
    ),
}

_DATA_CHANGE_RE = re.compile(
    r"\b(?:data|table|row|record|database|schema|etl|sync|import|export|migration|backfill|pipeline|event)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PlanDataQualityMonitoringRow:
    """Data quality monitoring coverage for one task."""

    task_id: str
    title: str
    data_change_signal: DataChangeSignal
    row_count_check: str = "missing"
    freshness_check: str = "missing"
    null_constraint_check: str = "missing"
    duplicate_detection: str = "missing"
    reconciliation: str = "missing"
    alert_owner: str = "missing"
    dashboard: str = "missing"
    sampling: str = "missing"
    rollback_path: str = "missing"
    audit_evidence: str = "missing"
    missing_safeguards: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "data_change_signal": self.data_change_signal,
            "row_count_check": self.row_count_check,
            "freshness_check": self.freshness_check,
            "null_constraint_check": self.null_constraint_check,
            "duplicate_detection": self.duplicate_detection,
            "reconciliation": self.reconciliation,
            "alert_owner": self.alert_owner,
            "dashboard": self.dashboard,
            "sampling": self.sampling,
            "rollback_path": self.rollback_path,
            "audit_evidence": self.audit_evidence,
            "missing_safeguards": list(self.missing_safeguards),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanDataQualityMonitoringMatrix:
    """Plan-level data quality monitoring matrix."""

    plan_id: str | None = None
    rows: tuple[PlanDataQualityMonitoringRow, ...] = field(default_factory=tuple)
    data_change_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_data_change_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanDataQualityMonitoringRow, ...]:
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "data_change_task_ids": list(self.data_change_task_ids),
            "no_data_change_task_ids": list(self.no_data_change_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        title = "# Plan Data Quality Monitoring Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        if not self.rows:
            return "\n".join([title, "", "No data quality monitoring rows were inferred."])
        lines = [
            title,
            "",
            "| Task | Signal | Row Count | Freshness | Null/Constraint | Duplicate | Reconciliation | Alert Owner | Dashboard | Sampling | Rollback | Audit |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | {row.data_change_signal} | "
                f"{_status_marker(row.row_count_check)} | {_status_marker(row.freshness_check)} | "
                f"{_status_marker(row.null_constraint_check)} | {_status_marker(row.duplicate_detection)} | "
                f"{_status_marker(row.reconciliation)} | {_status_marker(row.alert_owner)} | "
                f"{_status_marker(row.dashboard)} | {_status_marker(row.sampling)} | "
                f"{_status_marker(row.rollback_path)} | {_status_marker(row.audit_evidence)} |"
            )
        return "\n".join(lines)


def build_plan_data_quality_monitoring_matrix(source: Any) -> PlanDataQualityMonitoringMatrix:
    plan_id, tasks = _source_payload(source)
    rows: list[PlanDataQualityMonitoringRow] = []
    no_data_change_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index)
        if row:
            rows.append(row)
        else:
            no_data_change_task_ids.append(_task_id(task, index))
    result = tuple(rows)
    return PlanDataQualityMonitoringMatrix(
        plan_id=plan_id,
        rows=result,
        data_change_task_ids=tuple(row.task_id for row in result),
        no_data_change_task_ids=tuple(no_data_change_task_ids),
        summary=_summary(len(tasks), result),
    )


def generate_plan_data_quality_monitoring_matrix(source: Any) -> PlanDataQualityMonitoringMatrix:
    return build_plan_data_quality_monitoring_matrix(source)


def analyze_plan_data_quality_monitoring_matrix(source: Any) -> PlanDataQualityMonitoringMatrix:
    if isinstance(source, PlanDataQualityMonitoringMatrix):
        return source
    return build_plan_data_quality_monitoring_matrix(source)


def derive_plan_data_quality_monitoring_matrix(source: Any) -> PlanDataQualityMonitoringMatrix:
    return analyze_plan_data_quality_monitoring_matrix(source)


def extract_plan_data_quality_monitoring_matrix(source: Any) -> PlanDataQualityMonitoringMatrix:
    return derive_plan_data_quality_monitoring_matrix(source)


def summarize_plan_data_quality_monitoring_matrix(
    source: PlanDataQualityMonitoringMatrix | Iterable[PlanDataQualityMonitoringRow] | Any,
) -> dict[str, Any] | PlanDataQualityMonitoringMatrix:
    if isinstance(source, PlanDataQualityMonitoringMatrix):
        return dict(source.summary)
    if isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)) or hasattr(source, "tasks") or hasattr(source, "title"):
        return build_plan_data_quality_monitoring_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows)


def plan_data_quality_monitoring_matrix_to_dict(matrix: PlanDataQualityMonitoringMatrix) -> dict[str, Any]:
    return matrix.to_dict()


plan_data_quality_monitoring_matrix_to_dict.__test__ = False


def plan_data_quality_monitoring_matrix_to_dicts(
    matrix: PlanDataQualityMonitoringMatrix | Iterable[PlanDataQualityMonitoringRow],
) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanDataQualityMonitoringMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_data_quality_monitoring_matrix_to_dicts.__test__ = False


def plan_data_quality_monitoring_matrix_to_markdown(matrix: PlanDataQualityMonitoringMatrix) -> str:
    return matrix.to_markdown()


plan_data_quality_monitoring_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanDataQualityMonitoringRow | None:
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)

    # Must have data change signals
    if not _DATA_CHANGE_RE.search(context):
        return None

    # Detect data change signal
    data_change_signal = _detect_data_change_signal(texts)
    if not data_change_signal:
        return None

    # Check for each safeguard
    statuses = {
        "row_count_check": _status(_SAFEGUARD_PATTERNS["row_count_check"], texts),
        "freshness_check": _status(_SAFEGUARD_PATTERNS["freshness_check"], texts),
        "null_constraint_check": _status(_SAFEGUARD_PATTERNS["null_constraint_check"], texts),
        "duplicate_detection": _status(_SAFEGUARD_PATTERNS["duplicate_detection"], texts),
        "reconciliation": _status(_SAFEGUARD_PATTERNS["reconciliation"], texts),
        "alert_owner": _status(_SAFEGUARD_PATTERNS["alert_owner"], texts, skip_fields=("id",)),
        "dashboard": _status(_SAFEGUARD_PATTERNS["dashboard"], texts),
        "sampling": _status(_SAFEGUARD_PATTERNS["sampling"], texts),
        "rollback_path": _status(_SAFEGUARD_PATTERNS["rollback_path"], texts),
        "audit_evidence": _status(_SAFEGUARD_PATTERNS["audit_evidence"], texts),
    }

    missing_safeguards = tuple(
        safeguard
        for safeguard in _SAFEGUARD_ORDER
        if statuses.get(safeguard, "missing") == "missing"
    )

    return PlanDataQualityMonitoringRow(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        data_change_signal=data_change_signal,
        missing_safeguards=missing_safeguards,
        evidence=tuple(
            _dedupe(
                _evidence_snippet(field, text)
                for field, text in texts
                if _DATA_CHANGE_RE.search(text)
            )
        ),
        **statuses,
    )


def _detect_data_change_signal(texts: Iterable[tuple[str, str]]) -> DataChangeSignal | None:
    context = " ".join(text for _, text in texts)
    for signal in _SIGNAL_ORDER:
        if _SIGNAL_PATTERNS[signal].search(context):
            return signal
    return None


def _status(
    pattern: re.Pattern[str],
    texts: Iterable[tuple[str, str]],
    *,
    skip_fields: tuple[str, ...] = (),
) -> str:
    return "present" if any(field not in skip_fields and pattern.search(text) for field, text in texts) else "missing"


def _status_marker(status: str) -> str:
    """Return a checkmark for present, x for missing."""
    return "✓" if status == "present" else "✗"


def _summary(task_count: int, rows: Iterable[PlanDataQualityMonitoringRow]) -> dict[str, Any]:
    row_list = list(rows)

    signal_counts: dict[DataChangeSignal, int] = {signal: 0 for signal in _SIGNAL_ORDER}
    safeguard_counts: dict[str, int] = {}

    for row in row_list:
        signal_counts[row.data_change_signal] = signal_counts.get(row.data_change_signal, 0) + 1
        for safeguard in row.missing_safeguards:
            safeguard_counts[safeguard] = safeguard_counts.get(safeguard, 0) + 1

    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "data_change_task_count": len(row_list),
        "no_data_change_task_count": task_count - len(row_list),
        "signal_counts": signal_counts,
        "missing_safeguard_counts": safeguard_counts,
        "tasks_with_missing_safeguards": sum(1 for row in row_list if row.missing_safeguards),
        "tasks_with_complete_coverage": sum(1 for row in row_list if not row.missing_safeguards),
    }
