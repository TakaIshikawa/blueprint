"""Risk modeling system for quantifying and tracking plan execution risks.

Analyzes plan data to identify risks across five categories (schedule, resource,
technical, dependency, scope), calculates probability-weighted risk scores, and
generates mitigation recommendations.  Uses historical data to calibrate
probability models when available.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Sequence


# ---------------------------------------------------------------------------
# Enums & constants
# ---------------------------------------------------------------------------

class RiskCategory(str, Enum):
    """Categories of plan execution risk."""

    SCHEDULE = "schedule"
    RESOURCE = "resource"
    TECHNICAL = "technical"
    DEPENDENCY = "dependency"
    SCOPE = "scope"


class ImpactLevel(str, Enum):
    """Qualitative impact levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


_IMPACT_NUMERIC: dict[ImpactLevel, float] = {
    ImpactLevel.LOW: 0.25,
    ImpactLevel.MEDIUM: 0.50,
    ImpactLevel.HIGH: 0.75,
    ImpactLevel.CRITICAL: 1.0,
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Risk:
    """A single identified risk."""

    id: str
    category: RiskCategory
    title: str
    description: str
    probability: float = 0.5
    impact: ImpactLevel = ImpactLevel.MEDIUM
    indicators: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "category": self.category.value,
            "title": self.title,
            "description": self.description,
            "probability": self.probability,
            "impact": self.impact.value,
            "indicators": list(self.indicators),
        }


@dataclass(frozen=True, slots=True)
class RiskScore:
    """Quantified risk score combining probability and impact."""

    risk_id: str
    probability: float
    impact_level: ImpactLevel
    impact_numeric: float
    composite_score: float
    severity: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "risk_id": self.risk_id,
            "probability": self.probability,
            "impact_level": self.impact_level.value,
            "impact_numeric": self.impact_numeric,
            "composite_score": self.composite_score,
            "severity": self.severity,
        }


@dataclass(frozen=True, slots=True)
class Mitigation:
    """A recommended risk mitigation action."""

    risk_id: str
    strategy: str
    description: str
    priority: str
    effort: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "risk_id": self.risk_id,
            "strategy": self.strategy,
            "description": self.description,
            "priority": self.priority,
            "effort": self.effort,
        }


@dataclass(frozen=True, slots=True)
class ImpactAnalysis:
    """Result of simulating a risk's impact on a plan."""

    risk_id: str
    schedule_delay_days: float
    cost_increase_pct: float
    quality_impact: str
    affected_tasks: tuple[str, ...] = field(default_factory=tuple)
    cascading_effects: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "risk_id": self.risk_id,
            "schedule_delay_days": self.schedule_delay_days,
            "cost_increase_pct": self.cost_increase_pct,
            "quality_impact": self.quality_impact,
            "affected_tasks": list(self.affected_tasks),
            "cascading_effects": list(self.cascading_effects),
        }


@dataclass(frozen=True, slots=True)
class HeatMapCell:
    """A cell in the probability-vs-impact heat map."""

    probability_range: str
    impact_level: str
    risk_ids: tuple[str, ...] = field(default_factory=tuple)
    count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "probability_range": self.probability_range,
            "impact_level": self.impact_level,
            "risk_ids": list(self.risk_ids),
            "count": self.count,
        }


@dataclass(frozen=True, slots=True)
class RiskAssessment:
    """Complete risk assessment for a plan."""

    plan_id: str
    risks: tuple[Risk, ...] = field(default_factory=tuple)
    scores: tuple[RiskScore, ...] = field(default_factory=tuple)
    overall_risk_score: float = 0.0
    risk_level: str = "low"
    heat_map: tuple[HeatMapCell, ...] = field(default_factory=tuple)
    mitigations: tuple[Mitigation, ...] = field(default_factory=tuple)
    category_summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "risks": [r.to_dict() for r in self.risks],
            "scores": [s.to_dict() for s in self.scores],
            "overall_risk_score": self.overall_risk_score,
            "risk_level": self.risk_level,
            "heat_map": [c.to_dict() for c in self.heat_map],
            "mitigations": [m.to_dict() for m in self.mitigations],
            "category_summary": dict(self.category_summary),
        }


@dataclass(frozen=True, slots=True)
class RiskTrend:
    """Risk trend data point for tracking risk over time."""

    timestamp: str
    overall_score: float
    category_scores: dict[str, float] = field(default_factory=dict)
    alert: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "overall_score": self.overall_score,
            "category_scores": dict(self.category_scores),
            "alert": self.alert,
        }


# ---------------------------------------------------------------------------
# Indicator detection helpers
# ---------------------------------------------------------------------------

_PROBABILITY_RANGES = [
    ("very_low", 0.0, 0.2),
    ("low", 0.2, 0.4),
    ("medium", 0.4, 0.6),
    ("high", 0.6, 0.8),
    ("very_high", 0.8, 1.0),
]


def _classify_severity(composite: float) -> str:
    """Classify composite score into a severity label."""
    if composite >= 0.75:
        return "critical"
    if composite >= 0.50:
        return "high"
    if composite >= 0.25:
        return "medium"
    return "low"


def _overall_risk_level(score: float) -> str:
    """Map an overall risk score to a textual level."""
    if score >= 0.7:
        return "critical"
    if score >= 0.5:
        return "high"
    if score >= 0.3:
        return "medium"
    return "low"


def _clamp_probability(p: float) -> float:
    return max(0.0, min(1.0, p))


def _get_float(data: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    v = data.get(key, default)
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _get_int(data: Mapping[str, Any], key: str, default: int = 0) -> int:
    v = data.get(key, default)
    try:
        return int(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _get_str(data: Mapping[str, Any], key: str, default: str = "") -> str:
    v = data.get(key, default)
    return str(v) if v is not None else default


# ---------------------------------------------------------------------------
# Risk identification
# ---------------------------------------------------------------------------

def _identify_schedule_risks(plan: Mapping[str, Any]) -> list[Risk]:
    """Identify schedule-related risks."""
    risks: list[Risk] = []
    estimated_days = _get_float(plan, "estimated_days")
    num_tasks = _get_int(plan, "num_tasks")
    buffer_days = _get_float(plan, "buffer_days")
    deadline_set = bool(plan.get("deadline"))

    indicators: list[str] = []
    prob = 0.3  # base probability

    if estimated_days > 0 and buffer_days / max(estimated_days, 1) < 0.1:
        indicators.append("tight_deadline")
        prob += 0.2

    if deadline_set and estimated_days > 30:
        indicators.append("long_duration_with_deadline")
        prob += 0.1

    if num_tasks > 20:
        indicators.append("many_tasks")
        prob += 0.1

    if indicators:
        impact = ImpactLevel.HIGH if "tight_deadline" in indicators else ImpactLevel.MEDIUM
        risks.append(Risk(
            id="schedule-001",
            category=RiskCategory.SCHEDULE,
            title="Schedule overrun risk",
            description="Plan timeline may not accommodate all required work.",
            probability=_clamp_probability(prob),
            impact=impact,
            indicators=tuple(indicators),
        ))

    return risks


def _identify_resource_risks(plan: Mapping[str, Any]) -> list[Risk]:
    """Identify resource-related risks."""
    risks: list[Risk] = []
    team_size = _get_int(plan, "team_size")
    num_tasks = _get_int(plan, "num_tasks")

    indicators: list[str] = []
    prob = 0.2

    if team_size > 8:
        indicators.append("large_team_size")
        prob += 0.15

    if team_size > 0 and num_tasks / team_size > 5:
        indicators.append("high_task_per_person_ratio")
        prob += 0.15

    if team_size == 1 and num_tasks > 5:
        indicators.append("single_point_of_failure")
        prob += 0.2

    if indicators:
        impact = ImpactLevel.HIGH if "single_point_of_failure" in indicators else ImpactLevel.MEDIUM
        risks.append(Risk(
            id="resource-001",
            category=RiskCategory.RESOURCE,
            title="Resource availability risk",
            description="Team capacity may be insufficient for the planned work.",
            probability=_clamp_probability(prob),
            impact=impact,
            indicators=tuple(indicators),
        ))

    return risks


def _identify_technical_risks(plan: Mapping[str, Any]) -> list[Risk]:
    """Identify technical risks."""
    risks: list[Risk] = []
    complexity = _get_float(plan, "avg_task_complexity")
    has_novel_tech = bool(plan.get("novel_technology") or plan.get("new_technology"))
    num_unknowns = _get_int(plan, "num_unknowns")

    indicators: list[str] = []
    prob = 0.2

    if complexity > 7:
        indicators.append("high_complexity")
        prob += 0.2
    elif complexity > 5:
        indicators.append("moderate_complexity")
        prob += 0.1

    if has_novel_tech:
        indicators.append("novel_technology")
        prob += 0.2

    if num_unknowns > 3:
        indicators.append("many_unknowns")
        prob += 0.15

    if indicators:
        impact = ImpactLevel.HIGH if "novel_technology" in indicators else ImpactLevel.MEDIUM
        risks.append(Risk(
            id="technical-001",
            category=RiskCategory.TECHNICAL,
            title="Technical complexity risk",
            description="Technical challenges may impede progress.",
            probability=_clamp_probability(prob),
            impact=impact,
            indicators=tuple(indicators),
        ))

    return risks


def _identify_dependency_risks(plan: Mapping[str, Any]) -> list[Risk]:
    """Identify dependency-related risks."""
    risks: list[Risk] = []
    num_deps = _get_int(plan, "num_dependencies")
    external_deps = _get_int(plan, "external_dependencies")
    blocker_count = _get_int(plan, "blocker_count")

    indicators: list[str] = []
    prob = 0.2

    if external_deps > 2:
        indicators.append("external_dependencies")
        prob += 0.2

    if blocker_count > 0:
        indicators.append("active_blockers")
        prob += 0.25

    if num_deps > 15:
        indicators.append("many_internal_dependencies")
        prob += 0.1

    if indicators:
        impact = ImpactLevel.CRITICAL if "active_blockers" in indicators else ImpactLevel.HIGH
        risks.append(Risk(
            id="dependency-001",
            category=RiskCategory.DEPENDENCY,
            title="Dependency blocker risk",
            description="External or internal dependencies may delay execution.",
            probability=_clamp_probability(prob),
            impact=impact,
            indicators=tuple(indicators),
        ))

    return risks


def _identify_scope_risks(plan: Mapping[str, Any]) -> list[Risk]:
    """Identify scope-related risks."""
    risks: list[Risk] = []
    scope_change_count = _get_int(plan, "scope_change_count")
    requirements_stability = _get_str(plan, "requirements_stability")
    num_unknowns = _get_int(plan, "num_unknowns")

    indicators: list[str] = []
    prob = 0.2

    if scope_change_count > 3:
        indicators.append("frequent_scope_changes")
        prob += 0.25

    if requirements_stability in ("low", "volatile"):
        indicators.append("scope_ambiguity")
        prob += 0.2

    if num_unknowns > 5:
        indicators.append("many_unknowns_scope")
        prob += 0.1

    if indicators:
        impact = ImpactLevel.HIGH if "frequent_scope_changes" in indicators else ImpactLevel.MEDIUM
        risks.append(Risk(
            id="scope-001",
            category=RiskCategory.SCOPE,
            title="Scope creep risk",
            description="Requirements may change, expanding the scope of work.",
            probability=_clamp_probability(prob),
            impact=impact,
            indicators=tuple(indicators),
        ))

    return risks


# ---------------------------------------------------------------------------
# RiskModeler
# ---------------------------------------------------------------------------

class RiskModeler:
    """Analyze risk factors and calculate risk scores for execution plans.

    Supports historical data calibration for improved probability estimates.
    """

    def __init__(self) -> None:
        self._historical_data: list[Mapping[str, Any]] = []
        self._calibration_factors: dict[str, float] = {}
        self._risk_history: list[RiskTrend] = []

    # ------------------------------------------------------------------
    # Historical data
    # ------------------------------------------------------------------

    def load_historical_data(
        self,
        data: Sequence[Mapping[str, Any]],
    ) -> None:
        """Load historical plan data for probability calibration.

        Each record should have outcome fields such as ``schedule_overrun``,
        ``resource_shortage``, ``technical_failure``, ``dependency_blocked``,
        ``scope_changed`` (bool), along with the standard numeric plan fields.
        """
        self._historical_data = list(data)
        self._calibrate()

    def _calibrate(self) -> None:
        """Calibrate probability adjustments from historical outcomes."""
        if not self._historical_data:
            self._calibration_factors = {}
            return

        n = len(self._historical_data)
        category_outcome_keys = {
            RiskCategory.SCHEDULE: "schedule_overrun",
            RiskCategory.RESOURCE: "resource_shortage",
            RiskCategory.TECHNICAL: "technical_failure",
            RiskCategory.DEPENDENCY: "dependency_blocked",
            RiskCategory.SCOPE: "scope_changed",
        }

        for category, key in category_outcome_keys.items():
            occurred = sum(1 for r in self._historical_data if r.get(key))
            rate = occurred / n
            # Store a calibration multiplier centered on 1.0
            # If historical rate = 0.5, multiplier stays 1.0
            # Higher observed rate → higher multiplier
            self._calibration_factors[category.value] = rate / 0.5 if rate > 0 else 0.5

    # ------------------------------------------------------------------
    # Core methods
    # ------------------------------------------------------------------

    def identify_risks(self, plan: Any) -> list[Risk]:
        """Identify risks across all categories from plan data.

        Returns a list of ``Risk`` objects, one per detected category.
        """
        if not isinstance(plan, Mapping):
            return []

        risks: list[Risk] = []
        risks.extend(_identify_schedule_risks(plan))
        risks.extend(_identify_resource_risks(plan))
        risks.extend(_identify_technical_risks(plan))
        risks.extend(_identify_dependency_risks(plan))
        risks.extend(_identify_scope_risks(plan))

        # Apply historical calibration
        if self._calibration_factors:
            calibrated: list[Risk] = []
            for risk in risks:
                factor = self._calibration_factors.get(risk.category.value, 1.0)
                new_prob = _clamp_probability(risk.probability * factor)
                calibrated.append(Risk(
                    id=risk.id,
                    category=risk.category,
                    title=risk.title,
                    description=risk.description,
                    probability=new_prob,
                    impact=risk.impact,
                    indicators=risk.indicators,
                ))
            risks = calibrated

        return risks

    def calculate_risk_score(self, risk: Risk) -> RiskScore:
        """Calculate a composite risk score for a single risk.

        Composite = probability * impact_numeric.
        """
        impact_num = _IMPACT_NUMERIC[risk.impact]
        composite = risk.probability * impact_num
        severity = _classify_severity(composite)

        return RiskScore(
            risk_id=risk.id,
            probability=risk.probability,
            impact_level=risk.impact,
            impact_numeric=impact_num,
            composite_score=round(composite, 4),
            severity=severity,
        )

    def assess_plan_risk(self, plan_id: str, plan: Mapping[str, Any]) -> RiskAssessment:
        """Run a full risk assessment for a plan.

        Identifies risks, scores them, builds a heat map, and generates
        mitigation recommendations.
        """
        risks = self.identify_risks(plan)
        scores = [self.calculate_risk_score(r) for r in risks]
        heat_map = self._build_heat_map(risks)
        mitigations = self.generate_mitigation_recommendations(risks)
        category_summary = self._category_summary(risks, scores)

        overall = (
            sum(s.composite_score for s in scores) / len(scores)
            if scores
            else 0.0
        )
        risk_level = _overall_risk_level(overall)

        return RiskAssessment(
            plan_id=plan_id,
            risks=tuple(risks),
            scores=tuple(scores),
            overall_risk_score=round(overall, 4),
            risk_level=risk_level,
            heat_map=tuple(heat_map),
            mitigations=tuple(mitigations),
            category_summary=category_summary,
        )

    def simulate_risk_impact(
        self,
        risk: Risk,
        plan: Mapping[str, Any],
    ) -> ImpactAnalysis:
        """Simulate the impact of a risk materializing on the plan."""
        estimated_days = _get_float(plan, "estimated_days", 10.0)
        num_tasks = _get_int(plan, "num_tasks", 1)

        impact_num = _IMPACT_NUMERIC[risk.impact]

        # Schedule delay scales with probability, impact, and plan duration
        delay = estimated_days * risk.probability * impact_num * 0.5
        cost_pct = risk.probability * impact_num * 30.0  # up to 30% cost increase

        # Estimate affected tasks proportionally
        affected_fraction = impact_num * risk.probability
        affected_count = max(1, int(math.ceil(num_tasks * affected_fraction)))
        affected_tasks = tuple(f"task-{i+1}" for i in range(affected_count))

        # Quality impact
        if impact_num >= 0.75:
            quality = "significant degradation"
        elif impact_num >= 0.50:
            quality = "moderate degradation"
        else:
            quality = "minor degradation"

        cascading: list[str] = []
        if risk.category == RiskCategory.DEPENDENCY:
            cascading.append("Blocked tasks may cascade delays to downstream work.")
        if risk.category == RiskCategory.SCHEDULE:
            cascading.append("Late delivery may trigger scope cuts or quality trade-offs.")
        if risk.category == RiskCategory.RESOURCE:
            cascading.append("Resource gaps may force task reassignment or hiring.")
        if risk.category == RiskCategory.TECHNICAL:
            cascading.append("Technical debt may accumulate if shortcuts are taken.")
        if risk.category == RiskCategory.SCOPE:
            cascading.append("Scope expansion may invalidate existing estimates.")

        return ImpactAnalysis(
            risk_id=risk.id,
            schedule_delay_days=round(delay, 2),
            cost_increase_pct=round(cost_pct, 2),
            quality_impact=quality,
            affected_tasks=affected_tasks,
            cascading_effects=tuple(cascading),
        )

    def generate_mitigation_recommendations(
        self,
        risks: Sequence[Risk],
    ) -> list[Mitigation]:
        """Generate mitigation recommendations for identified risks."""
        mitigations: list[Mitigation] = []

        _STRATEGY_MAP: dict[RiskCategory, list[tuple[str, str, str, str]]] = {
            RiskCategory.SCHEDULE: [
                (
                    "Add buffer time",
                    "Add 20-30% buffer to the estimated timeline to absorb delays.",
                    "high",
                    "low",
                ),
                (
                    "Reduce scope",
                    "Identify and defer non-critical features to meet the deadline.",
                    "medium",
                    "medium",
                ),
            ],
            RiskCategory.RESOURCE: [
                (
                    "Assign backup resources",
                    "Identify backup personnel who can step in if primary resources become unavailable.",
                    "high",
                    "medium",
                ),
                (
                    "Cross-train team members",
                    "Ensure multiple team members can handle critical tasks.",
                    "medium",
                    "medium",
                ),
            ],
            RiskCategory.TECHNICAL: [
                (
                    "Prototype unknowns",
                    "Build proof-of-concept implementations for technically uncertain areas.",
                    "high",
                    "medium",
                ),
                (
                    "Technical spike",
                    "Allocate dedicated time for investigating technical unknowns before committing to estimates.",
                    "high",
                    "low",
                ),
            ],
            RiskCategory.DEPENDENCY: [
                (
                    "Establish dependency SLAs",
                    "Agree on delivery timelines with external dependency owners.",
                    "high",
                    "low",
                ),
                (
                    "Build abstraction layers",
                    "Create interfaces that allow swapping dependencies if blocked.",
                    "medium",
                    "high",
                ),
            ],
            RiskCategory.SCOPE: [
                (
                    "Freeze requirements",
                    "Establish a requirements freeze date and change control process.",
                    "high",
                    "low",
                ),
                (
                    "Define MVP clearly",
                    "Document the minimum viable scope with explicit acceptance criteria.",
                    "high",
                    "low",
                ),
            ],
        }

        for risk in risks:
            strategies = _STRATEGY_MAP.get(risk.category, [])
            for strategy, desc, priority, effort in strategies:
                mitigations.append(Mitigation(
                    risk_id=risk.id,
                    strategy=strategy,
                    description=desc,
                    priority=priority,
                    effort=effort,
                ))

        return mitigations

    # ------------------------------------------------------------------
    # Heat map
    # ------------------------------------------------------------------

    def _build_heat_map(self, risks: Sequence[Risk]) -> list[HeatMapCell]:
        """Build a probability-vs-impact heat map from identified risks."""
        cells: list[HeatMapCell] = []

        for prob_label, prob_low, prob_high in _PROBABILITY_RANGES:
            for impact_level in ImpactLevel:
                matching = [
                    r.id
                    for r in risks
                    if prob_low <= r.probability < prob_high
                    and r.impact == impact_level
                ]
                # Include boundary: if prob == 1.0 it falls in very_high
                if prob_high == 1.0:
                    matching.extend(
                        r.id
                        for r in risks
                        if r.probability == 1.0
                        and r.impact == impact_level
                        and r.id not in matching
                    )
                cells.append(HeatMapCell(
                    probability_range=f"{prob_label} ({prob_low:.1f}-{prob_high:.1f})",
                    impact_level=impact_level.value,
                    risk_ids=tuple(matching),
                    count=len(matching),
                ))

        return cells

    # ------------------------------------------------------------------
    # Category summary
    # ------------------------------------------------------------------

    def _category_summary(
        self,
        risks: Sequence[Risk],
        scores: Sequence[RiskScore],
    ) -> dict[str, Any]:
        """Produce a per-category summary."""
        score_map = {s.risk_id: s for s in scores}
        summary: dict[str, Any] = {}

        for category in RiskCategory:
            cat_risks = [r for r in risks if r.category == category]
            cat_scores = [score_map[r.id] for r in cat_risks if r.id in score_map]
            avg_score = (
                sum(s.composite_score for s in cat_scores) / len(cat_scores)
                if cat_scores
                else 0.0
            )
            summary[category.value] = {
                "risk_count": len(cat_risks),
                "avg_composite_score": round(avg_score, 4),
                "max_severity": (
                    max((s.severity for s in cat_scores), key=lambda sv: {
                        "low": 0, "medium": 1, "high": 2, "critical": 3,
                    }.get(sv, 0))
                    if cat_scores
                    else "none"
                ),
            }

        return summary

    # ------------------------------------------------------------------
    # Risk tracking
    # ------------------------------------------------------------------

    def track_risk(
        self,
        timestamp: str,
        assessment: RiskAssessment,
    ) -> RiskTrend:
        """Record a risk assessment data point for trend tracking.

        Returns a ``RiskTrend`` with an alert if risk is increasing.
        """
        category_scores = {
            cat: info.get("avg_composite_score", 0.0)
            for cat, info in assessment.category_summary.items()
        }

        alert: str | None = None
        if len(self._risk_history) >= 2:
            prev = self._risk_history[-1]
            prev_prev = self._risk_history[-2]
            if (
                assessment.overall_risk_score > prev.overall_score
                and prev.overall_score > prev_prev.overall_score
            ):
                alert = "Risk score increasing over consecutive assessments."

        trend = RiskTrend(
            timestamp=timestamp,
            overall_score=assessment.overall_risk_score,
            category_scores=category_scores,
            alert=alert,
        )
        self._risk_history.append(trend)
        return trend

    @property
    def risk_history(self) -> list[RiskTrend]:
        """Return recorded risk trend data points."""
        return list(self._risk_history)


__all__ = [
    "HeatMapCell",
    "ImpactAnalysis",
    "ImpactLevel",
    "Mitigation",
    "Risk",
    "RiskAssessment",
    "RiskCategory",
    "RiskModeler",
    "RiskScore",
    "RiskTrend",
]
