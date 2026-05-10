"""Detect and measure scope creep in blueprint execution plans.

Tracks scope changes over time by comparing current plan state against a
baseline snapshot. Calculates drift percentage, change velocity, and
categorizes scope changes to provide actionable scope management data.

Key capabilities
-----------------
* **Baseline snapshots** – capture initial task count, effort, and timeline
* **Change detection** – identify added, removed, and modified tasks
* **Drift percentage** – (current − baseline) / baseline × 100
* **Change velocity** – rate of scope additions over time
* **Categorization** – new requirements, rework, refinement
* **Threshold alerts** – configurable drift limits
* **Attribution** – track who introduced each change
* **Trend analysis** – scope stability across sprints
* **Reporting** – stakeholder-ready scope change reports
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ChangeCategory(str, Enum):
    """Classification of a scope change."""

    NEW_REQUIREMENT = "new_requirement"
    REWORK = "rework"
    REFINEMENT = "refinement"
    REMOVAL = "removal"


class ChangeType(str, Enum):
    """Atomic change type."""

    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"


class AlertSeverity(str, Enum):
    """Severity of a threshold alert."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TaskSnapshot:
    """Immutable snapshot of a single task's scope-relevant fields."""

    task_id: str
    title: str
    effort: float = 0.0
    status: str = "pending"
    assignee: str | None = None
    tags: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "effort": self.effort,
            "status": self.status,
            "assignee": self.assignee,
            "tags": list(self.tags),
        }


@dataclass(frozen=True, slots=True)
class ScopeBaseline:
    """Baseline snapshot of plan scope at a point in time."""

    captured_at: datetime
    task_count: int
    total_effort: float
    timeline_start: datetime | None
    timeline_end: datetime | None
    tasks: tuple[TaskSnapshot, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "captured_at": self.captured_at.isoformat(),
            "task_count": self.task_count,
            "total_effort": self.total_effort,
            "timeline_start": self.timeline_start.isoformat() if self.timeline_start else None,
            "timeline_end": self.timeline_end.isoformat() if self.timeline_end else None,
            "tasks": [t.to_dict() for t in self.tasks],
        }


@dataclass(frozen=True, slots=True)
class ScopeChange:
    """A single detected scope change."""

    change_type: ChangeType
    category: ChangeCategory
    task_id: str
    title: str
    description: str
    attributed_to: str | None = None
    detected_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "change_type": self.change_type.value,
            "category": self.category.value,
            "task_id": self.task_id,
            "title": self.title,
            "description": self.description,
            "attributed_to": self.attributed_to,
            "detected_at": self.detected_at.isoformat() if self.detected_at else None,
        }


@dataclass(frozen=True, slots=True)
class ThresholdAlert:
    """Alert raised when scope drift exceeds a configured threshold."""

    severity: AlertSeverity
    metric: str
    threshold: float
    actual: float
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity.value,
            "metric": self.metric,
            "threshold": self.threshold,
            "actual": self.actual,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class DriftResult:
    """Aggregated drift measurement result."""

    baseline: ScopeBaseline
    current_task_count: int
    current_total_effort: float
    task_count_drift_pct: float
    effort_drift_pct: float
    changes: tuple[ScopeChange, ...] = ()
    alerts: tuple[ThresholdAlert, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "baseline": self.baseline.to_dict(),
            "current_task_count": self.current_task_count,
            "current_total_effort": self.current_total_effort,
            "task_count_drift_pct": self.task_count_drift_pct,
            "effort_drift_pct": self.effort_drift_pct,
            "changes": [c.to_dict() for c in self.changes],
            "alerts": [a.to_dict() for a in self.alerts],
        }


@dataclass(frozen=True, slots=True)
class SprintScopeTrend:
    """Scope stability data for a single sprint period."""

    sprint_label: str
    start: datetime
    end: datetime
    additions: int
    removals: int
    modifications: int
    net_change: int
    drift_pct: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "sprint_label": self.sprint_label,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "additions": self.additions,
            "removals": self.removals,
            "modifications": self.modifications,
            "net_change": self.net_change,
            "drift_pct": self.drift_pct,
        }


@dataclass(frozen=True, slots=True)
class ChangeVelocity:
    """Rate of scope changes over a time window."""

    window_start: datetime
    window_end: datetime
    additions_per_day: float
    removals_per_day: float
    modifications_per_day: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "additions_per_day": self.additions_per_day,
            "removals_per_day": self.removals_per_day,
            "modifications_per_day": self.modifications_per_day,
        }


@dataclass(slots=True)
class ScopeCreepDetectorConfig:
    """Configuration for drift thresholds and detection behaviour."""

    task_count_warning_pct: float = 10.0
    task_count_critical_pct: float = 25.0
    effort_warning_pct: float = 15.0
    effort_critical_pct: float = 30.0


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------


def capture_baseline(
    tasks: list[dict[str, Any]],
    timeline_start: datetime | None = None,
    timeline_end: datetime | None = None,
    captured_at: datetime | None = None,
) -> ScopeBaseline:
    """Create a baseline scope snapshot from a list of task dicts.

    Args:
        tasks: List of task dictionaries with at least ``task_id`` and ``title``.
        timeline_start: Optional project start date.
        timeline_end: Optional project end date.
        captured_at: Timestamp for the snapshot (defaults to now).

    Returns:
        An immutable ``ScopeBaseline`` instance.
    """
    snapshots = tuple(_task_dict_to_snapshot(t) for t in tasks)
    total_effort = sum(s.effort for s in snapshots)
    return ScopeBaseline(
        captured_at=captured_at or datetime.now(),
        task_count=len(snapshots),
        total_effort=total_effort,
        timeline_start=timeline_start,
        timeline_end=timeline_end,
        tasks=snapshots,
    )


def detect_changes(
    baseline: ScopeBaseline,
    current_tasks: list[dict[str, Any]],
    detected_at: datetime | None = None,
) -> list[ScopeChange]:
    """Compare current tasks against a baseline and return scope changes.

    Args:
        baseline: The reference scope snapshot.
        current_tasks: Current list of task dictionaries.
        detected_at: Timestamp for the detection (defaults to now).

    Returns:
        List of ``ScopeChange`` instances.
    """
    now = detected_at or datetime.now()
    baseline_map: dict[str, TaskSnapshot] = {t.task_id: t for t in baseline.tasks}
    current_map: dict[str, TaskSnapshot] = {
        s.task_id: s for s in (_task_dict_to_snapshot(t) for t in current_tasks)
    }

    changes: list[ScopeChange] = []

    # Additions
    for tid, snap in current_map.items():
        if tid not in baseline_map:
            changes.append(
                ScopeChange(
                    change_type=ChangeType.ADDED,
                    category=ChangeCategory.NEW_REQUIREMENT,
                    task_id=tid,
                    title=snap.title,
                    description=f"Task '{snap.title}' added after baseline",
                    attributed_to=snap.assignee,
                    detected_at=now,
                )
            )

    # Removals
    for tid, snap in baseline_map.items():
        if tid not in current_map:
            changes.append(
                ScopeChange(
                    change_type=ChangeType.REMOVED,
                    category=ChangeCategory.REMOVAL,
                    task_id=tid,
                    title=snap.title,
                    description=f"Task '{snap.title}' removed from plan",
                    attributed_to=None,
                    detected_at=now,
                )
            )

    # Modifications
    for tid, current_snap in current_map.items():
        if tid in baseline_map:
            baseline_snap = baseline_map[tid]
            diffs = _diff_snapshots(baseline_snap, current_snap)
            if diffs:
                category = _categorize_modification(baseline_snap, current_snap)
                changes.append(
                    ScopeChange(
                        change_type=ChangeType.MODIFIED,
                        category=category,
                        task_id=tid,
                        title=current_snap.title,
                        description=f"Modified: {', '.join(diffs)}",
                        attributed_to=current_snap.assignee,
                        detected_at=now,
                    )
                )

    return changes


def calculate_drift(
    baseline: ScopeBaseline,
    current_tasks: list[dict[str, Any]],
    config: ScopeCreepDetectorConfig | None = None,
    detected_at: datetime | None = None,
) -> DriftResult:
    """Calculate scope drift between baseline and current state.

    Args:
        baseline: The reference scope snapshot.
        current_tasks: Current list of task dictionaries.
        config: Optional threshold configuration.
        detected_at: Timestamp for the detection.

    Returns:
        A ``DriftResult`` with drift percentages, changes, and alerts.
    """
    cfg = config or ScopeCreepDetectorConfig()
    changes = detect_changes(baseline, current_tasks, detected_at=detected_at)
    current_snapshots = [_task_dict_to_snapshot(t) for t in current_tasks]
    current_count = len(current_snapshots)
    current_effort = sum(s.effort for s in current_snapshots)

    task_drift = _pct_change(baseline.task_count, current_count)
    effort_drift = _pct_change(baseline.total_effort, current_effort)

    alerts = _evaluate_thresholds(task_drift, effort_drift, cfg)

    return DriftResult(
        baseline=baseline,
        current_task_count=current_count,
        current_total_effort=current_effort,
        task_count_drift_pct=task_drift,
        effort_drift_pct=effort_drift,
        changes=tuple(changes),
        alerts=tuple(alerts),
    )


def calculate_change_velocity(
    changes: list[ScopeChange] | tuple[ScopeChange, ...],
) -> ChangeVelocity | None:
    """Compute the rate of scope changes per day.

    Args:
        changes: Collection of scope changes with ``detected_at`` timestamps.

    Returns:
        A ``ChangeVelocity`` or ``None`` if there are no timestamped changes.
    """
    timestamped = [c for c in changes if c.detected_at is not None]
    if not timestamped:
        return None

    dates = [c.detected_at for c in timestamped]
    min_dt = min(dates)  # type: ignore[type-var]
    max_dt = max(dates)  # type: ignore[type-var]
    span_days = max((max_dt - min_dt).total_seconds() / 86400, 1.0)  # type: ignore[operator]

    additions = sum(1 for c in timestamped if c.change_type == ChangeType.ADDED)
    removals = sum(1 for c in timestamped if c.change_type == ChangeType.REMOVED)
    modifications = sum(1 for c in timestamped if c.change_type == ChangeType.MODIFIED)

    return ChangeVelocity(
        window_start=min_dt,  # type: ignore[arg-type]
        window_end=max_dt,  # type: ignore[arg-type]
        additions_per_day=additions / span_days,
        removals_per_day=removals / span_days,
        modifications_per_day=modifications / span_days,
    )


def analyze_trends(
    sprint_snapshots: list[dict[str, Any]],
    baseline: ScopeBaseline,
) -> list[SprintScopeTrend]:
    """Analyze scope stability across sprints.

    Args:
        sprint_snapshots: List of dicts with ``sprint_label``, ``start``, ``end``,
            ``additions``, ``removals``, ``modifications``.
        baseline: The reference baseline for drift percentage calculation.

    Returns:
        List of ``SprintScopeTrend`` records.
    """
    trends: list[SprintScopeTrend] = []
    cumulative_net = 0
    for snap in sprint_snapshots:
        adds = int(snap.get("additions", 0))
        rems = int(snap.get("removals", 0))
        mods = int(snap.get("modifications", 0))
        net = adds - rems
        cumulative_net += net
        drift = _pct_change(baseline.task_count, baseline.task_count + cumulative_net)
        trends.append(
            SprintScopeTrend(
                sprint_label=snap["sprint_label"],
                start=snap["start"],
                end=snap["end"],
                additions=adds,
                removals=rems,
                modifications=mods,
                net_change=net,
                drift_pct=drift,
            )
        )
    return trends


def generate_scope_change_report(drift_result: DriftResult) -> dict[str, Any]:
    """Produce a stakeholder-ready scope change report.

    Args:
        drift_result: The computed drift result.

    Returns:
        Dictionary suitable for JSON serialization or rendering.
    """
    by_category: dict[str, int] = {}
    by_type: dict[str, int] = {}
    attributions: dict[str, int] = {}
    for change in drift_result.changes:
        by_category[change.category.value] = by_category.get(change.category.value, 0) + 1
        by_type[change.change_type.value] = by_type.get(change.change_type.value, 0) + 1
        if change.attributed_to:
            attributions[change.attributed_to] = attributions.get(change.attributed_to, 0) + 1

    return {
        "summary": {
            "baseline_task_count": drift_result.baseline.task_count,
            "current_task_count": drift_result.current_task_count,
            "task_count_drift_pct": drift_result.task_count_drift_pct,
            "baseline_effort": drift_result.baseline.total_effort,
            "current_effort": drift_result.current_total_effort,
            "effort_drift_pct": drift_result.effort_drift_pct,
            "total_changes": len(drift_result.changes),
        },
        "changes_by_category": by_category,
        "changes_by_type": by_type,
        "attributions": attributions,
        "alerts": [a.to_dict() for a in drift_result.alerts],
        "changes": [c.to_dict() for c in drift_result.changes],
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _task_dict_to_snapshot(task: dict[str, Any]) -> TaskSnapshot:
    tags = task.get("tags") or ()
    if isinstance(tags, list):
        tags = tuple(tags)
    return TaskSnapshot(
        task_id=task.get("task_id", task.get("id", "")),
        title=task.get("title", ""),
        effort=float(task.get("effort", 0.0)),
        status=task.get("status", "pending"),
        assignee=task.get("assignee"),
        tags=tags,
    )


def _diff_snapshots(old: TaskSnapshot, new: TaskSnapshot) -> list[str]:
    diffs: list[str] = []
    if old.title != new.title:
        diffs.append(f"title changed from '{old.title}' to '{new.title}'")
    if old.effort != new.effort:
        diffs.append(f"effort changed from {old.effort} to {new.effort}")
    if old.status != new.status:
        diffs.append(f"status changed from '{old.status}' to '{new.status}'")
    if old.tags != new.tags:
        diffs.append("tags changed")
    return diffs


def _categorize_modification(old: TaskSnapshot, new: TaskSnapshot) -> ChangeCategory:
    if old.effort != new.effort and new.effort > old.effort:
        return ChangeCategory.REWORK
    if old.title != new.title:
        return ChangeCategory.REFINEMENT
    return ChangeCategory.REFINEMENT


def _pct_change(baseline_value: float, current_value: float) -> float:
    if baseline_value == 0:
        return 100.0 if current_value > 0 else 0.0
    return ((current_value - baseline_value) / baseline_value) * 100.0


def _evaluate_thresholds(
    task_drift: float,
    effort_drift: float,
    config: ScopeCreepDetectorConfig,
) -> list[ThresholdAlert]:
    alerts: list[ThresholdAlert] = []
    abs_task = abs(task_drift)
    abs_effort = abs(effort_drift)

    if abs_task >= config.task_count_critical_pct:
        alerts.append(
            ThresholdAlert(
                severity=AlertSeverity.CRITICAL,
                metric="task_count_drift_pct",
                threshold=config.task_count_critical_pct,
                actual=task_drift,
                message=f"Task count drift {task_drift:.1f}% exceeds critical threshold {config.task_count_critical_pct}%",
            )
        )
    elif abs_task >= config.task_count_warning_pct:
        alerts.append(
            ThresholdAlert(
                severity=AlertSeverity.WARNING,
                metric="task_count_drift_pct",
                threshold=config.task_count_warning_pct,
                actual=task_drift,
                message=f"Task count drift {task_drift:.1f}% exceeds warning threshold {config.task_count_warning_pct}%",
            )
        )

    if abs_effort >= config.effort_critical_pct:
        alerts.append(
            ThresholdAlert(
                severity=AlertSeverity.CRITICAL,
                metric="effort_drift_pct",
                threshold=config.effort_critical_pct,
                actual=effort_drift,
                message=f"Effort drift {effort_drift:.1f}% exceeds critical threshold {config.effort_critical_pct}%",
            )
        )
    elif abs_effort >= config.effort_warning_pct:
        alerts.append(
            ThresholdAlert(
                severity=AlertSeverity.WARNING,
                metric="effort_drift_pct",
                threshold=config.effort_warning_pct,
                actual=effort_drift,
                message=f"Effort drift {effort_drift:.1f}% exceeds warning threshold {config.effort_warning_pct}%",
            )
        )

    return alerts


__all__ = [
    "AlertSeverity",
    "ChangeCategory",
    "ChangeType",
    "ChangeVelocity",
    "DriftResult",
    "ScopeBaseline",
    "ScopeChange",
    "ScopeCreepDetectorConfig",
    "SprintScopeTrend",
    "TaskSnapshot",
    "ThresholdAlert",
    "analyze_trends",
    "calculate_change_velocity",
    "calculate_drift",
    "capture_baseline",
    "detect_changes",
    "generate_scope_change_report",
]
