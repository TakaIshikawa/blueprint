"""Tests for plan simulation with what-if scenario analysis."""

import pytest

from blueprint.simulation.plan_simulator import (
    ComparisonReport,
    OutcomeDistribution,
    PlanSimulator,
    Scenario,
    SensitivityReport,
    SensitivityVariable,
    SimulationResult,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simulator() -> PlanSimulator:
    return PlanSimulator(seed=42)


# ---------------------------------------------------------------------------
# Scenario creation
# ---------------------------------------------------------------------------

class TestCreateScenario:
    def test_creates_scenario_with_changes(self, simulator: PlanSimulator) -> None:
        changes = {"duration_factor": 1.5, "team_size": 6}
        scenario = simulator.create_scenario("plan-1", changes, "Test scenario")
        assert isinstance(scenario, Scenario)
        assert scenario.plan_id == "plan-1"
        assert scenario.changes["duration_factor"] == 1.5
        assert scenario.description == "Test scenario"

    def test_scenario_with_base_tasks(self, simulator: PlanSimulator) -> None:
        tasks = [
            {"id": "t1", "duration": 5, "confidence": 0.9},
            {"id": "t2", "duration": 3, "confidence": 0.7},
        ]
        scenario = simulator.create_scenario(
            "plan-2", {}, base_tasks=tasks,
        )
        assert len(scenario.base_tasks) == 2
        assert scenario.base_tasks[0]["id"] == "t1"

    def test_scenario_to_dict(self, simulator: PlanSimulator) -> None:
        scenario = simulator.create_scenario("plan-3", {"team_size": 4})
        d = scenario.to_dict()
        assert d["plan_id"] == "plan-3"
        assert "changes" in d

    def test_scenario_default_description(self, simulator: PlanSimulator) -> None:
        scenario = simulator.create_scenario("plan-4", {})
        assert "plan-4" in scenario.description


# ---------------------------------------------------------------------------
# Monte Carlo simulation
# ---------------------------------------------------------------------------

class TestSimulateExecution:
    def test_runs_1000_iterations(self, simulator: PlanSimulator) -> None:
        scenario = simulator.create_scenario(
            "mc-1",
            {"base_duration": 30, "estimate_confidence": 0.8},
        )
        result = simulator.simulate_execution(scenario, iterations=1000)
        assert isinstance(result, SimulationResult)
        assert result.iterations == 1000
        assert len(result.raw_durations) == 1000

    def test_runs_more_than_1000_iterations(self, simulator: PlanSimulator) -> None:
        scenario = simulator.create_scenario(
            "mc-2",
            {"base_duration": 20, "estimate_confidence": 0.9},
        )
        result = simulator.simulate_execution(scenario, iterations=2000)
        assert result.iterations == 2000
        assert len(result.raw_durations) == 2000

    def test_randomizes_based_on_confidence(self, simulator: PlanSimulator) -> None:
        # Low confidence → wider distribution
        low_conf = simulator.create_scenario(
            "conf-1", {"base_duration": 30, "estimate_confidence": 0.3},
        )
        high_conf = simulator.create_scenario(
            "conf-2", {"base_duration": 30, "estimate_confidence": 0.95},
        )

        low_result = simulator.simulate_execution(low_conf, iterations=1000)
        high_result = simulator.simulate_execution(high_conf, iterations=1000)

        # Low confidence should have wider std
        assert low_result.completion_distribution.std > high_result.completion_distribution.std

    def test_completion_distribution_has_percentiles(
        self, simulator: PlanSimulator
    ) -> None:
        scenario = simulator.create_scenario(
            "dist-1", {"base_duration": 25, "estimate_confidence": 0.8},
        )
        result = simulator.simulate_execution(scenario, iterations=1000)
        dist = result.completion_distribution
        assert dist.p10 <= dist.p25 <= dist.median <= dist.p75 <= dist.p90

    def test_cost_distribution_calculated(self, simulator: PlanSimulator) -> None:
        scenario = simulator.create_scenario(
            "cost-1", {"base_duration": 20, "team_size": 5},
        )
        result = simulator.simulate_execution(scenario, iterations=500)
        assert result.cost_distribution.mean > 0

    def test_on_time_probability_with_deadline(
        self, simulator: PlanSimulator
    ) -> None:
        scenario = simulator.create_scenario(
            "deadline-1",
            {"base_duration": 20, "estimate_confidence": 0.9, "deadline_days": 25},
        )
        result = simulator.simulate_execution(scenario, iterations=1000)
        # Most simulations should finish before generous deadline
        assert result.on_time_probability > 0.5

    def test_on_time_probability_zero_without_deadline(
        self, simulator: PlanSimulator
    ) -> None:
        scenario = simulator.create_scenario(
            "no-deadline", {"base_duration": 20},
        )
        result = simulator.simulate_execution(scenario, iterations=100)
        assert result.on_time_probability == 0.0

    def test_risk_exposure_calculated(self, simulator: PlanSimulator) -> None:
        scenario = simulator.create_scenario(
            "risk-1", {"base_duration": 30, "estimate_confidence": 0.5},
        )
        result = simulator.simulate_execution(scenario, iterations=1000)
        assert 0.0 <= result.risk_exposure <= 1.0

    def test_simulation_result_to_dict(self, simulator: PlanSimulator) -> None:
        scenario = simulator.create_scenario(
            "dict-1", {"base_duration": 15},
        )
        result = simulator.simulate_execution(scenario, iterations=100)
        d = result.to_dict()
        assert d["iterations"] == 100
        assert "completion_distribution" in d
        assert "cost_distribution" in d

    def test_duration_factor_scales_results(self, simulator: PlanSimulator) -> None:
        base = simulator.create_scenario(
            "factor-1", {"base_duration": 20, "duration_factor": 1.0},
        )
        scaled = simulator.create_scenario(
            "factor-2", {"base_duration": 20, "duration_factor": 2.0},
        )
        base_result = simulator.simulate_execution(base, iterations=500)
        scaled_result = simulator.simulate_execution(scaled, iterations=500)
        # Doubled duration factor should roughly double mean
        ratio = scaled_result.completion_distribution.mean / max(
            base_result.completion_distribution.mean, 1
        )
        assert 1.5 < ratio < 2.5


# ---------------------------------------------------------------------------
# Outcome distribution
# ---------------------------------------------------------------------------

class TestOutcomeDistribution:
    def test_predict_returns_distribution(self, simulator: PlanSimulator) -> None:
        scenario = simulator.create_scenario(
            "pred-1", {"base_duration": 25, "estimate_confidence": 0.85},
        )
        dist = simulator.predict_outcome_distribution(scenario, iterations=500)
        assert isinstance(dist, OutcomeDistribution)
        assert dist.mean > 0
        assert dist.std >= 0

    def test_distribution_to_dict(self) -> None:
        dist = OutcomeDistribution(
            mean=25.5, median=24.0, std=3.2,
            p10=20.0, p25=22.0, p75=28.0, p90=30.0,
            min_value=18.0, max_value=35.0,
        )
        d = dist.to_dict()
        assert d["mean"] == 25.5
        assert d["median"] == 24.0


# ---------------------------------------------------------------------------
# Sensitivity analysis
# ---------------------------------------------------------------------------

class TestSensitivityAnalysis:
    def test_sensitivity_report_structure(self, simulator: PlanSimulator) -> None:
        plan = {
            "id": "sens-1",
            "base_duration": 30,
            "team_size": 5,
            "estimate_confidence": 0.8,
        }
        report = simulator.run_sensitivity_analysis(plan)
        assert isinstance(report, SensitivityReport)
        assert report.plan_id == "sens-1"
        assert len(report.variables) > 0
        assert report.most_sensitive

    def test_sensitivity_custom_variables(self, simulator: PlanSimulator) -> None:
        plan = {
            "id": "sens-2",
            "base_duration": 20,
            "team_size": 3,
            "estimate_confidence": 0.7,
        }
        report = simulator.run_sensitivity_analysis(
            plan, variables=["base_duration", "team_size"],
        )
        var_names = [v.variable for v in report.variables]
        assert "base_duration" in var_names
        assert "team_size" in var_names

    def test_sensitivity_impact_scores_positive(
        self, simulator: PlanSimulator
    ) -> None:
        plan = {
            "base_duration": 30,
            "team_size": 4,
            "estimate_confidence": 0.8,
        }
        report = simulator.run_sensitivity_analysis(plan)
        for v in report.variables:
            assert v.impact_score >= 0

    def test_sensitivity_variable_to_dict(self) -> None:
        sv = SensitivityVariable(
            variable="team_size",
            impact_score=0.15,
            direction="higher decreases duration",
            description="test",
        )
        d = sv.to_dict()
        assert d["variable"] == "team_size"
        assert d["impact_score"] == 0.15

    def test_sensitivity_report_to_dict(self, simulator: PlanSimulator) -> None:
        plan = {"base_duration": 20, "team_size": 3}
        report = simulator.run_sensitivity_analysis(plan)
        d = report.to_dict()
        assert "variables" in d
        assert "most_sensitive" in d


# ---------------------------------------------------------------------------
# Scenario comparison
# ---------------------------------------------------------------------------

class TestCompareScenarios:
    def test_compare_two_scenarios(self, simulator: PlanSimulator) -> None:
        s1 = simulator.create_scenario("cmp-1", {"base_duration": 20})
        s2 = simulator.create_scenario("cmp-2", {"base_duration": 30})
        r1 = simulator.simulate_execution(s1, iterations=200)
        r2 = simulator.simulate_execution(s2, iterations=200)

        report = simulator.compare_scenarios([(s1, r1), (s2, r2)])
        assert isinstance(report, ComparisonReport)
        assert len(report.scenarios) == 2
        assert len(report.timeline_deltas) == 2
        assert report.recommendation

    def test_compare_shows_deltas(self, simulator: PlanSimulator) -> None:
        s1 = simulator.create_scenario("delta-1", {"base_duration": 20})
        s2 = simulator.create_scenario("delta-2", {"base_duration": 40})
        r1 = simulator.simulate_execution(s1, iterations=200)
        r2 = simulator.simulate_execution(s2, iterations=200)

        report = simulator.compare_scenarios([(s1, r1), (s2, r2)])
        # First scenario is baseline (delta = 0)
        assert report.timeline_deltas[s1.id] == 0.0
        # Second should be positive (longer)
        assert report.timeline_deltas[s2.id] > 0

    def test_compare_single_scenario(self, simulator: PlanSimulator) -> None:
        s1 = simulator.create_scenario("single", {"base_duration": 20})
        r1 = simulator.simulate_execution(s1, iterations=100)
        report = simulator.compare_scenarios([(s1, r1)])
        assert "at least 2" in report.recommendation.lower()

    def test_comparison_report_to_dict(self, simulator: PlanSimulator) -> None:
        s1 = simulator.create_scenario("dict-cmp-1", {"base_duration": 15})
        s2 = simulator.create_scenario("dict-cmp-2", {"base_duration": 25})
        r1 = simulator.simulate_execution(s1, iterations=100)
        r2 = simulator.simulate_execution(s2, iterations=100)
        report = simulator.compare_scenarios([(s1, r1), (s2, r2)])
        d = report.to_dict()
        assert "scenarios" in d
        assert "timeline_deltas" in d
        assert "recommendation" in d

    def test_compare_identifies_best_scenario(
        self, simulator: PlanSimulator
    ) -> None:
        s1 = simulator.create_scenario("best-1", {"base_duration": 30})
        s2 = simulator.create_scenario("best-2", {"base_duration": 10})
        r1 = simulator.simulate_execution(s1, iterations=200)
        r2 = simulator.simulate_execution(s2, iterations=200)
        report = simulator.compare_scenarios([(s1, r1), (s2, r2)])
        # Scenario 2 should have negative delta (shorter) so it's best
        assert s2.id in report.recommendation


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_zero_duration(self, simulator: PlanSimulator) -> None:
        scenario = simulator.create_scenario(
            "zero", {"base_duration": 0, "estimate_confidence": 0.9},
        )
        result = simulator.simulate_execution(scenario, iterations=100)
        assert isinstance(result, SimulationResult)

    def test_small_iteration_count(self, simulator: PlanSimulator) -> None:
        scenario = simulator.create_scenario(
            "small", {"base_duration": 10},
        )
        result = simulator.simulate_execution(scenario, iterations=1)
        assert result.iterations == 1
        assert len(result.raw_durations) == 1

    def test_deterministic_with_seed(self) -> None:
        sim1 = PlanSimulator(seed=123)
        sim2 = PlanSimulator(seed=123)
        s1 = sim1.create_scenario("det-1", {"base_duration": 20})
        s2 = sim2.create_scenario("det-2", {"base_duration": 20})
        r1 = sim1.simulate_execution(s1, iterations=100)
        r2 = sim2.simulate_execution(s2, iterations=100)
        assert r1.completion_distribution.mean == r2.completion_distribution.mean
