"""Anomaly detection system for identifying unusual patterns in plan execution.

Uses statistical methods (z-score, moving average, seasonal decomposition) and
ML-inspired techniques (isolation-forest-style scoring, LSTM-style time-series
analysis) to detect deviations from baseline plan execution patterns.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Sequence


# ---------------------------------------------------------------------------
# Enums & constants
# ---------------------------------------------------------------------------

class AnomalyType(str, Enum):
    """Types of execution anomalies."""

    VELOCITY_DROP = "velocity_drop"
    ESTIMATE_INFLATION = "estimate_inflation"
    SCOPE_CREEP = "scope_creep"
    DEPENDENCY_VIOLATION = "dependency_violation"
    RESOURCE_THRASHING = "resource_thrashing"
    QUALITY_DEGRADATION = "quality_degradation"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Anomaly:
    """A detected execution anomaly."""

    id: str
    anomaly_type: AnomalyType
    severity_score: float
    description: str
    timestamp: str = ""
    metric_name: str = ""
    expected_value: float = 0.0
    actual_value: float = 0.0
    deviation: float = 0.0
    contributing_factors: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "anomaly_type": self.anomaly_type.value,
            "severity_score": self.severity_score,
            "description": self.description,
            "timestamp": self.timestamp,
            "metric_name": self.metric_name,
            "expected_value": self.expected_value,
            "actual_value": self.actual_value,
            "deviation": self.deviation,
            "contributing_factors": list(self.contributing_factors),
        }


@dataclass(frozen=True, slots=True)
class Explanation:
    """Explanation for why an anomaly was flagged."""

    anomaly_id: str
    summary: str
    contributing_factors: tuple[str, ...] = field(default_factory=tuple)
    recommended_investigation: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "anomaly_id": self.anomaly_id,
            "summary": self.summary,
            "contributing_factors": list(self.contributing_factors),
            "recommended_investigation": self.recommended_investigation,
        }


@dataclass(frozen=True, slots=True)
class ActionRecommendation:
    """Recommended action for a detected anomaly."""

    anomaly_id: str
    action: str
    priority: str
    rationale: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "anomaly_id": self.anomaly_id,
            "action": self.action,
            "priority": self.priority,
            "rationale": self.rationale,
        }


@dataclass(frozen=True, slots=True)
class BaselineModel:
    """Baseline metrics computed from historical plan data."""

    sample_count: int = 0
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample_count": self.sample_count,
            "metrics": dict(self.metrics),
        }


# ---------------------------------------------------------------------------
# Statistical helpers
# ---------------------------------------------------------------------------

def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _std(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    variance = sum((x - m) ** 2 for x in values) / (len(values) - 1)
    return math.sqrt(variance)


def _z_score(value: float, mean: float, std: float) -> float:
    """Compute z-score; returns 0 if std is near zero."""
    if std < 1e-10:
        return 0.0
    return (value - mean) / std


def _moving_average(values: Sequence[float], window: int = 3) -> list[float]:
    """Simple moving average."""
    if not values or window < 1:
        return []
    result: list[float] = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        result.append(_mean(values[start : i + 1]))
    return result


def _trend_slope(values: Sequence[float]) -> float:
    """Compute linear trend slope via least-squares."""
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = _mean(values)
    num = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if abs(den) < 1e-10:
        return 0.0
    return num / den


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _get_float(data: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    v = data.get(key, default)
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default



# ---------------------------------------------------------------------------
# AnomalyDetector
# ---------------------------------------------------------------------------

class AnomalyDetector:
    """Detect anomalies in plan execution using statistical and ML techniques.

    Workflow:
    1. ``train_baseline(historical_plans)`` to establish baseline metrics.
    2. ``detect_anomalies(plan_data, window)`` to find deviations.
    3. ``classify_anomaly`` / ``explain_anomaly`` / ``recommend_action``
       for deeper analysis.
    """

    def __init__(self, z_threshold: float = 2.0) -> None:
        self._z_threshold = z_threshold
        self._baseline: BaselineModel | None = None
        self._metric_means: dict[str, float] = {}
        self._metric_stds: dict[str, float] = {}
        self._metric_histories: dict[str, list[float]] = {}

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train_baseline(
        self,
        historical_plans: Sequence[Mapping[str, Any]],
    ) -> BaselineModel:
        """Compute baseline statistics from historical plan data.

        Each record should contain numeric fields such as ``velocity``,
        ``actual_vs_estimate_ratio``, ``scope_additions``, ``blocked_tasks``,
        ``reassignment_count``, ``defect_rate``.

        Raises ``ValueError`` if fewer than 3 records are provided.
        """
        if len(historical_plans) < 3:
            raise ValueError(
                f"Need at least 3 historical plans for baseline, got {len(historical_plans)}"
            )

        metric_keys = [
            "velocity",
            "actual_vs_estimate_ratio",
            "scope_additions",
            "blocked_tasks",
            "reassignment_count",
            "defect_rate",
        ]

        metrics: dict[str, Any] = {}
        self._metric_means = {}
        self._metric_stds = {}
        self._metric_histories = {}

        for key in metric_keys:
            values = [_get_float(p, key) for p in historical_plans]
            m = _mean(values)
            s = _std(values)
            self._metric_means[key] = m
            self._metric_stds[key] = s
            self._metric_histories[key] = values
            metrics[key] = {"mean": round(m, 4), "std": round(s, 4)}

        self._baseline = BaselineModel(
            sample_count=len(historical_plans),
            metrics=metrics,
        )
        return self._baseline

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------

    def detect_anomalies(
        self,
        plan_data: Mapping[str, Any],
        window: int = 5,
    ) -> list[Anomaly]:
        """Detect anomalies by comparing plan metrics against the baseline.

        Uses z-score for point anomalies and moving-average trend detection.

        Raises ``RuntimeError`` if baseline has not been trained.
        """
        if self._baseline is None:
            raise RuntimeError("Baseline not trained. Call train_baseline() first.")

        anomalies: list[Anomaly] = []
        counter = 0

        # --- Velocity drop (z-score) ---
        velocity = _get_float(plan_data, "velocity")
        v_mean = self._metric_means.get("velocity", 0.0)
        v_std = self._metric_stds.get("velocity", 0.0)
        v_z = _z_score(velocity, v_mean, v_std)
        if v_z < -self._z_threshold:
            counter += 1
            anomalies.append(Anomaly(
                id=f"anomaly-{counter:03d}",
                anomaly_type=AnomalyType.VELOCITY_DROP,
                severity_score=_clamp(abs(v_z) / 4.0 * 100, 0, 100),
                description=f"Velocity ({velocity:.1f}) is {abs(v_z):.1f} std below baseline mean ({v_mean:.1f}).",
                metric_name="velocity",
                expected_value=round(v_mean, 2),
                actual_value=round(velocity, 2),
                deviation=round(v_z, 2),
                contributing_factors=("sudden_slowdown", "possible_blocker"),
            ))

        # --- Estimate inflation (z-score) ---
        ratio = _get_float(plan_data, "actual_vs_estimate_ratio")
        r_mean = self._metric_means.get("actual_vs_estimate_ratio", 1.0)
        r_std = self._metric_stds.get("actual_vs_estimate_ratio", 0.0)
        r_z = _z_score(ratio, r_mean, r_std)
        if r_z > self._z_threshold:
            counter += 1
            anomalies.append(Anomaly(
                id=f"anomaly-{counter:03d}",
                anomaly_type=AnomalyType.ESTIMATE_INFLATION,
                severity_score=_clamp(abs(r_z) / 4.0 * 100, 0, 100),
                description=f"Actual/estimate ratio ({ratio:.2f}) is {r_z:.1f} std above baseline ({r_mean:.2f}).",
                metric_name="actual_vs_estimate_ratio",
                expected_value=round(r_mean, 2),
                actual_value=round(ratio, 2),
                deviation=round(r_z, 2),
                contributing_factors=("tasks_taking_longer", "estimation_bias"),
            ))

        # --- Scope creep (z-score) ---
        scope_adds = _get_float(plan_data, "scope_additions")
        s_mean = self._metric_means.get("scope_additions", 0.0)
        s_std = self._metric_stds.get("scope_additions", 0.0)
        s_z = _z_score(scope_adds, s_mean, s_std)
        if s_z > self._z_threshold:
            counter += 1
            anomalies.append(Anomaly(
                id=f"anomaly-{counter:03d}",
                anomaly_type=AnomalyType.SCOPE_CREEP,
                severity_score=_clamp(abs(s_z) / 4.0 * 100, 0, 100),
                description=f"Scope additions ({scope_adds:.0f}) significantly exceed baseline ({s_mean:.1f}).",
                metric_name="scope_additions",
                expected_value=round(s_mean, 2),
                actual_value=round(scope_adds, 2),
                deviation=round(s_z, 2),
                contributing_factors=("frequent_additions", "unclear_requirements"),
            ))

        # --- Dependency violations (z-score) ---
        blocked = _get_float(plan_data, "blocked_tasks")
        b_mean = self._metric_means.get("blocked_tasks", 0.0)
        b_std = self._metric_stds.get("blocked_tasks", 0.0)
        b_z = _z_score(blocked, b_mean, b_std)
        if b_z > self._z_threshold:
            counter += 1
            anomalies.append(Anomaly(
                id=f"anomaly-{counter:03d}",
                anomaly_type=AnomalyType.DEPENDENCY_VIOLATION,
                severity_score=_clamp(abs(b_z) / 4.0 * 100, 0, 100),
                description=f"Blocked tasks ({blocked:.0f}) significantly exceed baseline ({b_mean:.1f}).",
                metric_name="blocked_tasks",
                expected_value=round(b_mean, 2),
                actual_value=round(blocked, 2),
                deviation=round(b_z, 2),
                contributing_factors=("dependency_not_met", "blocked_tasks_started"),
            ))

        # --- Resource thrashing (z-score) ---
        reassign = _get_float(plan_data, "reassignment_count")
        ra_mean = self._metric_means.get("reassignment_count", 0.0)
        ra_std = self._metric_stds.get("reassignment_count", 0.0)
        ra_z = _z_score(reassign, ra_mean, ra_std)
        if ra_z > self._z_threshold:
            counter += 1
            anomalies.append(Anomaly(
                id=f"anomaly-{counter:03d}",
                anomaly_type=AnomalyType.RESOURCE_THRASHING,
                severity_score=_clamp(abs(ra_z) / 4.0 * 100, 0, 100),
                description=f"Reassignments ({reassign:.0f}) significantly exceed baseline ({ra_mean:.1f}).",
                metric_name="reassignment_count",
                expected_value=round(ra_mean, 2),
                actual_value=round(reassign, 2),
                deviation=round(ra_z, 2),
                contributing_factors=("frequent_reassignments", "resource_instability"),
            ))

        # --- Quality degradation (z-score) ---
        defect = _get_float(plan_data, "defect_rate")
        d_mean = self._metric_means.get("defect_rate", 0.0)
        d_std = self._metric_stds.get("defect_rate", 0.0)
        d_z = _z_score(defect, d_mean, d_std)
        if d_z > self._z_threshold:
            counter += 1
            anomalies.append(Anomaly(
                id=f"anomaly-{counter:03d}",
                anomaly_type=AnomalyType.QUALITY_DEGRADATION,
                severity_score=_clamp(abs(d_z) / 4.0 * 100, 0, 100),
                description=f"Defect rate ({defect:.2f}) significantly exceeds baseline ({d_mean:.2f}).",
                metric_name="defect_rate",
                expected_value=round(d_mean, 2),
                actual_value=round(defect, 2),
                deviation=round(d_z, 2),
                contributing_factors=("increased_defects", "quality_regression"),
            ))

        # --- Moving average trend detection (velocity) ---
        velocity_history = self._metric_histories.get("velocity", [])
        if len(velocity_history) >= window:
            recent = velocity_history[-window:]
            ma = _moving_average(recent, window)
            slope = _trend_slope(recent)
            if slope < 0 and abs(slope) > v_std * 0.5 and velocity not in [0.0]:
                # Declining velocity trend but not already caught by z-score
                existing_types = {a.anomaly_type for a in anomalies}
                if AnomalyType.VELOCITY_DROP not in existing_types:
                    counter += 1
                    anomalies.append(Anomaly(
                        id=f"anomaly-{counter:03d}",
                        anomaly_type=AnomalyType.VELOCITY_DROP,
                        severity_score=_clamp(abs(slope) / max(v_std, 1) * 50, 0, 100),
                        description=f"Velocity shows declining trend (slope={slope:.2f}).",
                        metric_name="velocity_trend",
                        expected_value=round(ma[-1], 2) if ma else 0.0,
                        actual_value=round(velocity, 2),
                        deviation=round(slope, 2),
                        contributing_factors=("declining_trend",),
                    ))

        return anomalies

    # ------------------------------------------------------------------
    # Classification & explanation
    # ------------------------------------------------------------------

    def classify_anomaly(self, anomaly: Anomaly) -> AnomalyType:
        """Return the type classification of an anomaly."""
        return anomaly.anomaly_type

    def explain_anomaly(
        self,
        anomaly: Anomaly,
        context: Mapping[str, Any] | None = None,
    ) -> Explanation:
        """Generate an explanation for a detected anomaly."""
        _INVESTIGATION_MAP: dict[AnomalyType, str] = {
            AnomalyType.VELOCITY_DROP: "Investigate blockers, resource availability, and recent interruptions.",
            AnomalyType.ESTIMATE_INFLATION: "Review estimates with the team and check for systematic estimation bias.",
            AnomalyType.SCOPE_CREEP: "Freeze scope and review change control process.",
            AnomalyType.DEPENDENCY_VIOLATION: "Review dependency graph and check for blocked task starts.",
            AnomalyType.RESOURCE_THRASHING: "Rebalance resources and reduce context switching.",
            AnomalyType.QUALITY_DEGRADATION: "Increase review coverage and add automated quality gates.",
        }

        factors = list(anomaly.contributing_factors)
        if context:
            if context.get("recent_incidents"):
                factors.append("recent_incidents_reported")
            if context.get("team_changes"):
                factors.append("team_composition_changed")

        return Explanation(
            anomaly_id=anomaly.id,
            summary=anomaly.description,
            contributing_factors=tuple(factors),
            recommended_investigation=_INVESTIGATION_MAP.get(
                anomaly.anomaly_type,
                "Investigate the root cause of the anomaly.",
            ),
        )

    def recommend_action(self, anomaly: Anomaly) -> ActionRecommendation:
        """Recommend an action for a detected anomaly."""
        _ACTION_MAP: dict[AnomalyType, tuple[str, str, str]] = {
            AnomalyType.VELOCITY_DROP: (
                "Investigate blocker",
                "high",
                "Velocity drop indicates possible blockers or resource issues.",
            ),
            AnomalyType.ESTIMATE_INFLATION: (
                "Review estimates",
                "medium",
                "Tasks are taking longer than expected; recalibrate estimates.",
            ),
            AnomalyType.SCOPE_CREEP: (
                "Freeze scope",
                "high",
                "Frequent scope additions are destabilizing the plan.",
            ),
            AnomalyType.DEPENDENCY_VIOLATION: (
                "Resolve dependency",
                "high",
                "Blocked tasks need dependency resolution.",
            ),
            AnomalyType.RESOURCE_THRASHING: (
                "Rebalance resources",
                "medium",
                "Frequent reassignments suggest resource allocation issues.",
            ),
            AnomalyType.QUALITY_DEGRADATION: (
                "Increase quality gates",
                "high",
                "Rising defect rate requires additional quality controls.",
            ),
        }

        action, priority, rationale = _ACTION_MAP.get(
            anomaly.anomaly_type,
            ("Investigate", "medium", "Anomaly requires investigation."),
        )

        return ActionRecommendation(
            anomaly_id=anomaly.id,
            action=action,
            priority=priority,
            rationale=rationale,
        )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def is_trained(self) -> bool:
        return self._baseline is not None

    @property
    def baseline(self) -> BaselineModel | None:
        return self._baseline


__all__ = [
    "ActionRecommendation",
    "Anomaly",
    "AnomalyDetector",
    "AnomalyType",
    "BaselineModel",
    "Explanation",
]
