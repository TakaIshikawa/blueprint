"""Plan simulation with Monte Carlo and what-if scenario analysis.

Supports scenario creation, Monte Carlo simulation (1000+ iterations),
sensitivity analysis, and scenario comparison for plan execution predictions.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Scenario:
    """A hypothetical plan modification scenario."""

    id: str
    plan_id: str
    description: str
    changes: dict[str, Any] = field(default_factory=dict)
    base_tasks: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "plan_id": self.plan_id,
            "description": self.description,
            "changes": dict(self.changes),
            "base_tasks": [dict(t) for t in self.base_tasks],
        }


@dataclass(frozen=True, slots=True)
class OutcomeDistribution:
    """Probability distribution of outcomes from simulation."""

    mean: float = 0.0
    median: float = 0.0
    std: float = 0.0
    p10: float = 0.0
    p25: float = 0.0
    p75: float = 0.0
    p90: float = 0.0
    min_value: float = 0.0
    max_value: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "mean": round(self.mean, 2),
            "median": round(self.median, 2),
            "std": round(self.std, 2),
            "p10": round(self.p10, 2),
            "p25": round(self.p25, 2),
            "p75": round(self.p75, 2),
            "p90": round(self.p90, 2),
            "min_value": round(self.min_value, 2),
            "max_value": round(self.max_value, 2),
        }


@dataclass(frozen=True, slots=True)
class SimulationResult:
    """Result of a Monte Carlo simulation run."""

    scenario_id: str
    iterations: int
    completion_distribution: OutcomeDistribution
    cost_distribution: OutcomeDistribution
    risk_exposure: float = 0.0
    on_time_probability: float = 0.0
    raw_durations: tuple[float, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "iterations": self.iterations,
            "completion_distribution": self.completion_distribution.to_dict(),
            "cost_distribution": self.cost_distribution.to_dict(),
            "risk_exposure": round(self.risk_exposure, 4),
            "on_time_probability": round(self.on_time_probability, 4),
        }


@dataclass(frozen=True, slots=True)
class SensitivityVariable:
    """Result of sensitivity analysis for one variable."""

    variable: str
    impact_score: float
    direction: str
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "variable": self.variable,
            "impact_score": round(self.impact_score, 4),
            "direction": self.direction,
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class SensitivityReport:
    """Sensitivity analysis report for a plan."""

    plan_id: str
    variables: tuple[SensitivityVariable, ...] = field(default_factory=tuple)
    most_sensitive: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "variables": [v.to_dict() for v in self.variables],
            "most_sensitive": self.most_sensitive,
        }


@dataclass(frozen=True, slots=True)
class ComparisonReport:
    """Comparison report for multiple scenarios."""

    scenarios: tuple[str, ...] = field(default_factory=tuple)
    timeline_deltas: dict[str, float] = field(default_factory=dict)
    cost_deltas: dict[str, float] = field(default_factory=dict)
    risk_deltas: dict[str, float] = field(default_factory=dict)
    recommendation: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenarios": list(self.scenarios),
            "timeline_deltas": dict(self.timeline_deltas),
            "cost_deltas": dict(self.cost_deltas),
            "risk_deltas": dict(self.risk_deltas),
            "recommendation": self.recommendation,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_float(data: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    v = data.get(key, default)
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Compute percentile from a sorted list."""
    if not sorted_values:
        return 0.0
    idx = pct / 100.0 * (len(sorted_values) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return sorted_values[lo]
    frac = idx - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def _build_distribution(values: list[float]) -> OutcomeDistribution:
    """Build an OutcomeDistribution from raw simulation values."""
    if not values:
        return OutcomeDistribution()
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mean = sum(sorted_vals) / n
    variance = sum((x - mean) ** 2 for x in sorted_vals) / max(n - 1, 1)
    std = math.sqrt(variance)
    median = _percentile(sorted_vals, 50)
    return OutcomeDistribution(
        mean=mean,
        median=median,
        std=std,
        p10=_percentile(sorted_vals, 10),
        p25=_percentile(sorted_vals, 25),
        p75=_percentile(sorted_vals, 75),
        p90=_percentile(sorted_vals, 90),
        min_value=sorted_vals[0],
        max_value=sorted_vals[-1],
    )


# ---------------------------------------------------------------------------
# PlanSimulator
# ---------------------------------------------------------------------------

class PlanSimulator:
    """Simulate plan execution with Monte Carlo and what-if analysis.

    Workflow:
    1. ``create_scenario(plan_id, changes)`` to define hypothetical changes.
    2. ``simulate_execution(scenario, iterations)`` to run Monte Carlo.
    3. ``run_sensitivity_analysis(plan, variables)`` for variable impact.
    4. ``compare_scenarios(scenarios)`` to compare alternatives.
    """

    def __init__(self, seed: int | None = None) -> None:
        self._rng = random.Random(seed)

    # ------------------------------------------------------------------
    # Scenario creation
    # ------------------------------------------------------------------

    def create_scenario(
        self,
        plan_id: str,
        changes: Mapping[str, Any],
        description: str = "",
        base_tasks: Sequence[Mapping[str, Any]] | None = None,
    ) -> Scenario:
        """Create a hypothetical scenario for simulation.

        Supported changes:
        - ``duration_factor``: multiply task durations
        - ``team_size``: override team size
        - ``add_tasks``: number of tasks to add
        - ``remove_tasks``: number of tasks to remove
        - ``resource_availability``: float multiplier
        - ``estimate_confidence``: confidence level (0-1) for randomization
        """
        tasks = tuple(dict(t) for t in base_tasks) if base_tasks else ()
        return Scenario(
            id=f"scenario-{plan_id}-{self._rng.randint(1000, 9999)}",
            plan_id=plan_id,
            description=description or f"Scenario for plan {plan_id}",
            changes=dict(changes),
            base_tasks=tasks,
        )

    # ------------------------------------------------------------------
    # Monte Carlo simulation
    # ------------------------------------------------------------------

    def simulate_execution(
        self,
        scenario: Scenario,
        iterations: int = 1000,
    ) -> SimulationResult:
        """Run Monte Carlo simulation on a scenario.

        Randomizes task durations based on estimate confidence and calculates
        outcome distributions.

        Args:
            scenario: The scenario to simulate.
            iterations: Number of Monte Carlo iterations (default 1000).
        """
        confidence = _get_float(scenario.changes, "estimate_confidence", 0.8)
        duration_factor = _get_float(scenario.changes, "duration_factor", 1.0)
        team_size = _get_float(scenario.changes, "team_size", 4.0)
        base_duration = _get_float(scenario.changes, "base_duration", 30.0)
        deadline = _get_float(scenario.changes, "deadline_days", 0.0)

        # Uncertainty range: higher confidence → narrower range
        uncertainty = max(0.05, 1.0 - confidence)

        durations: list[float] = []
        costs: list[float] = []
        cost_per_day = max(team_size, 1) * 500  # $500/person/day

        for _ in range(iterations):
            # Randomize task duration with triangular distribution
            base = base_duration * duration_factor
            low = base * (1 - uncertainty)
            high = base * (1 + uncertainty * 2)
            mode = base
            sim_duration = self._rng.triangular(low, high, mode)
            durations.append(sim_duration)
            costs.append(sim_duration * cost_per_day)

        completion_dist = _build_distribution(durations)
        cost_dist = _build_distribution(costs)

        # On-time probability
        if deadline > 0:
            on_time = sum(1 for d in durations if d <= deadline) / iterations
        else:
            on_time = 0.0

        # Risk exposure = probability of exceeding baseline by >20%
        risk_threshold = base_duration * duration_factor * 1.2
        risk_exposure = sum(1 for d in durations if d > risk_threshold) / iterations

        return SimulationResult(
            scenario_id=scenario.id,
            iterations=iterations,
            completion_distribution=completion_dist,
            cost_distribution=cost_dist,
            risk_exposure=risk_exposure,
            on_time_probability=on_time,
            raw_durations=tuple(durations),
        )

    # ------------------------------------------------------------------
    # Outcome distribution
    # ------------------------------------------------------------------

    def predict_outcome_distribution(
        self,
        scenario: Scenario,
        iterations: int = 1000,
    ) -> OutcomeDistribution:
        """Predict completion date probability distribution."""
        result = self.simulate_execution(scenario, iterations)
        return result.completion_distribution

    # ------------------------------------------------------------------
    # Sensitivity analysis
    # ------------------------------------------------------------------

    def run_sensitivity_analysis(
        self,
        plan: Mapping[str, Any],
        variables: Sequence[str] | None = None,
    ) -> SensitivityReport:
        """Analyze which variables have the most impact on plan outcomes.

        Tests each variable by adjusting it ±20% and measuring outcome change.
        """
        plan_id = str(plan.get("id", "plan"))
        if variables is None:
            variables = ["team_size", "base_duration", "estimate_confidence"]

        base_changes = dict(plan)
        base_scenario = self.create_scenario(plan_id, base_changes, "baseline")
        base_result = self.simulate_execution(base_scenario, iterations=200)
        base_mean = base_result.completion_distribution.mean

        sensitivity_vars: list[SensitivityVariable] = []

        for var in variables:
            base_val = _get_float(plan, var, 1.0)
            if base_val == 0:
                base_val = 1.0

            # Test +20%
            high_changes = dict(base_changes)
            high_changes[var] = base_val * 1.2
            high_scenario = self.create_scenario(plan_id, high_changes, f"{var}+20%")
            high_result = self.simulate_execution(high_scenario, iterations=200)

            # Test -20%
            low_changes = dict(base_changes)
            low_changes[var] = base_val * 0.8
            low_scenario = self.create_scenario(plan_id, low_changes, f"{var}-20%")
            low_result = self.simulate_execution(low_scenario, iterations=200)

            high_mean = high_result.completion_distribution.mean
            low_mean = low_result.completion_distribution.mean
            spread = abs(high_mean - low_mean)
            impact = spread / max(base_mean, 1.0)

            direction = "higher increases duration" if high_mean > low_mean else "higher decreases duration"

            sensitivity_vars.append(SensitivityVariable(
                variable=var,
                impact_score=impact,
                direction=direction,
                description=f"Changing {var} by ±20% shifts completion by ~{spread:.1f} days.",
            ))

        # Sort by impact
        sensitivity_vars.sort(key=lambda v: v.impact_score, reverse=True)
        most_sensitive = sensitivity_vars[0].variable if sensitivity_vars else ""

        return SensitivityReport(
            plan_id=plan_id,
            variables=tuple(sensitivity_vars),
            most_sensitive=most_sensitive,
        )

    # ------------------------------------------------------------------
    # Scenario comparison
    # ------------------------------------------------------------------

    def compare_scenarios(
        self,
        scenarios: Sequence[tuple[Scenario, SimulationResult]],
    ) -> ComparisonReport:
        """Compare multiple scenario simulation results.

        Args:
            scenarios: Sequence of (Scenario, SimulationResult) tuples.
        """
        if len(scenarios) < 2:
            return ComparisonReport(
                scenarios=tuple(s.id for s, _ in scenarios),
                recommendation="Need at least 2 scenarios to compare.",
            )

        # Use first scenario as baseline
        base_scenario, base_result = scenarios[0]
        base_duration = base_result.completion_distribution.mean
        base_cost = base_result.cost_distribution.mean
        base_risk = base_result.risk_exposure

        scenario_ids: list[str] = []
        timeline_deltas: dict[str, float] = {}
        cost_deltas: dict[str, float] = {}
        risk_deltas: dict[str, float] = {}

        for scenario, result in scenarios:
            scenario_ids.append(scenario.id)
            timeline_deltas[scenario.id] = round(
                result.completion_distribution.mean - base_duration, 2
            )
            cost_deltas[scenario.id] = round(
                result.cost_distribution.mean - base_cost, 2
            )
            risk_deltas[scenario.id] = round(
                result.risk_exposure - base_risk, 4
            )

        # Find best scenario (lowest duration)
        best_id = min(
            scenario_ids,
            key=lambda sid: timeline_deltas.get(sid, 0.0),
        )
        recommendation = f"Scenario {best_id} has the shortest expected duration."

        return ComparisonReport(
            scenarios=tuple(scenario_ids),
            timeline_deltas=timeline_deltas,
            cost_deltas=cost_deltas,
            risk_deltas=risk_deltas,
            recommendation=recommendation,
        )


__all__ = [
    "ComparisonReport",
    "OutcomeDistribution",
    "PlanSimulator",
    "Scenario",
    "SensitivityReport",
    "SensitivityVariable",
    "SimulationResult",
]
