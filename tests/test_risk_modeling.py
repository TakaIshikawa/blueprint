"""Tests for risk modeling system."""

import pytest

from blueprint.analytics.risk_modeling import (
    HeatMapCell,
    ImpactAnalysis,
    ImpactLevel,
    Mitigation,
    Risk,
    RiskAssessment,
    RiskCategory,
    RiskModeler,
    RiskScore,
    RiskTrend,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def modeler() -> RiskModeler:
    return RiskModeler()


@pytest.fixture
def high_risk_plan() -> dict:
    return {
        "estimated_days": 60,
        "buffer_days": 2,
        "deadline": "2025-06-01",
        "num_tasks": 30,
        "team_size": 10,
        "avg_task_complexity": 8,
        "novel_technology": True,
        "num_unknowns": 5,
        "num_dependencies": 20,
        "external_dependencies": 4,
        "blocker_count": 2,
        "scope_change_count": 5,
        "requirements_stability": "low",
    }


@pytest.fixture
def low_risk_plan() -> dict:
    return {
        "estimated_days": 10,
        "buffer_days": 3,
        "num_tasks": 3,
        "team_size": 2,
        "avg_task_complexity": 2,
        "num_dependencies": 1,
        "external_dependencies": 0,
        "blocker_count": 0,
        "scope_change_count": 0,
    }


@pytest.fixture
def historical_data() -> list[dict]:
    return [
        {
            "num_tasks": 20,
            "estimated_days": 30,
            "team_size": 5,
            "schedule_overrun": True,
            "resource_shortage": False,
            "technical_failure": True,
            "dependency_blocked": False,
            "scope_changed": True,
        },
        {
            "num_tasks": 10,
            "estimated_days": 15,
            "team_size": 3,
            "schedule_overrun": False,
            "resource_shortage": True,
            "technical_failure": False,
            "dependency_blocked": False,
            "scope_changed": False,
        },
        {
            "num_tasks": 25,
            "estimated_days": 40,
            "team_size": 6,
            "schedule_overrun": True,
            "resource_shortage": True,
            "technical_failure": True,
            "dependency_blocked": True,
            "scope_changed": True,
        },
        {
            "num_tasks": 5,
            "estimated_days": 7,
            "team_size": 2,
            "schedule_overrun": False,
            "resource_shortage": False,
            "technical_failure": False,
            "dependency_blocked": False,
            "scope_changed": False,
        },
        {
            "num_tasks": 15,
            "estimated_days": 20,
            "team_size": 4,
            "schedule_overrun": True,
            "resource_shortage": False,
            "technical_failure": False,
            "dependency_blocked": True,
            "scope_changed": True,
        },
    ]


# ---------------------------------------------------------------------------
# Risk identification
# ---------------------------------------------------------------------------

class TestIdentifyRisks:
    def test_identifies_schedule_risk(self, modeler: RiskModeler) -> None:
        plan = {"estimated_days": 30, "buffer_days": 1, "num_tasks": 25}
        risks = modeler.identify_risks(plan)
        categories = [r.category for r in risks]
        assert RiskCategory.SCHEDULE in categories

    def test_identifies_resource_risk(self, modeler: RiskModeler) -> None:
        plan = {"team_size": 12, "num_tasks": 10}
        risks = modeler.identify_risks(plan)
        categories = [r.category for r in risks]
        assert RiskCategory.RESOURCE in categories

    def test_identifies_technical_risk(self, modeler: RiskModeler) -> None:
        plan = {"avg_task_complexity": 9, "novel_technology": True, "num_unknowns": 4}
        risks = modeler.identify_risks(plan)
        categories = [r.category for r in risks]
        assert RiskCategory.TECHNICAL in categories

    def test_identifies_dependency_risk(self, modeler: RiskModeler) -> None:
        plan = {"external_dependencies": 5, "blocker_count": 3, "num_dependencies": 20}
        risks = modeler.identify_risks(plan)
        categories = [r.category for r in risks]
        assert RiskCategory.DEPENDENCY in categories

    def test_identifies_scope_risk(self, modeler: RiskModeler) -> None:
        plan = {"scope_change_count": 6, "requirements_stability": "low"}
        risks = modeler.identify_risks(plan)
        categories = [r.category for r in risks]
        assert RiskCategory.SCOPE in categories

    def test_high_risk_plan_identifies_all_categories(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        risks = modeler.identify_risks(high_risk_plan)
        categories = {r.category for r in risks}
        assert categories == {
            RiskCategory.SCHEDULE,
            RiskCategory.RESOURCE,
            RiskCategory.TECHNICAL,
            RiskCategory.DEPENDENCY,
            RiskCategory.SCOPE,
        }

    def test_low_risk_plan_may_identify_fewer_risks(
        self, modeler: RiskModeler, low_risk_plan: dict
    ) -> None:
        risks = modeler.identify_risks(low_risk_plan)
        assert len(risks) < 5

    def test_empty_plan_returns_no_risks(self, modeler: RiskModeler) -> None:
        risks = modeler.identify_risks({})
        assert risks == []

    def test_non_mapping_input_returns_empty(self, modeler: RiskModeler) -> None:
        risks = modeler.identify_risks("not a dict")  # type: ignore[arg-type]
        assert risks == []

    def test_risk_has_required_fields(self, modeler: RiskModeler) -> None:
        plan = {"estimated_days": 30, "buffer_days": 1}
        risks = modeler.identify_risks(plan)
        for risk in risks:
            assert risk.id
            assert risk.category in RiskCategory
            assert risk.title
            assert risk.description
            assert 0.0 <= risk.probability <= 1.0
            assert risk.impact in ImpactLevel

    def test_risk_indicators_populated(self, modeler: RiskModeler) -> None:
        plan = {"estimated_days": 30, "buffer_days": 1, "num_tasks": 25}
        risks = modeler.identify_risks(plan)
        schedule_risks = [r for r in risks if r.category == RiskCategory.SCHEDULE]
        assert schedule_risks
        assert len(schedule_risks[0].indicators) > 0


# ---------------------------------------------------------------------------
# Risk probability (0-1)
# ---------------------------------------------------------------------------

class TestRiskProbability:
    def test_probability_within_bounds(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        risks = modeler.identify_risks(high_risk_plan)
        for risk in risks:
            assert 0.0 <= risk.probability <= 1.0

    def test_higher_risk_factors_increase_probability(self, modeler: RiskModeler) -> None:
        low = {"estimated_days": 10, "buffer_days": 3}
        high = {"estimated_days": 60, "buffer_days": 1, "num_tasks": 30, "deadline": "2025-01-01"}
        low_risks = modeler.identify_risks(low)
        high_risks = modeler.identify_risks(high)

        low_schedule = [r for r in low_risks if r.category == RiskCategory.SCHEDULE]
        high_schedule = [r for r in high_risks if r.category == RiskCategory.SCHEDULE]

        if low_schedule and high_schedule:
            assert high_schedule[0].probability > low_schedule[0].probability

    def test_historical_calibration_adjusts_probability(
        self, modeler: RiskModeler, historical_data: list[dict]
    ) -> None:
        plan = {"scope_change_count": 5, "requirements_stability": "low"}
        risks_before = modeler.identify_risks(plan)

        modeler.load_historical_data(historical_data)
        risks_after = modeler.identify_risks(plan)

        # With 3/5 scope_changed=True, rate=0.6, factor=1.2
        scope_before = [r for r in risks_before if r.category == RiskCategory.SCOPE]
        scope_after = [r for r in risks_after if r.category == RiskCategory.SCOPE]

        assert scope_before and scope_after
        # Probability should differ after calibration
        assert scope_before[0].probability != scope_after[0].probability


# ---------------------------------------------------------------------------
# Impact levels
# ---------------------------------------------------------------------------

class TestImpactLevels:
    def test_impact_level_is_valid_enum(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        risks = modeler.identify_risks(high_risk_plan)
        for risk in risks:
            assert risk.impact in (
                ImpactLevel.LOW,
                ImpactLevel.MEDIUM,
                ImpactLevel.HIGH,
                ImpactLevel.CRITICAL,
            )

    def test_blockers_produce_critical_impact(self, modeler: RiskModeler) -> None:
        plan = {"blocker_count": 5, "external_dependencies": 3}
        risks = modeler.identify_risks(plan)
        dep_risks = [r for r in risks if r.category == RiskCategory.DEPENDENCY]
        assert dep_risks
        assert dep_risks[0].impact == ImpactLevel.CRITICAL


# ---------------------------------------------------------------------------
# Composite risk scoring
# ---------------------------------------------------------------------------

class TestRiskScoring:
    def test_calculate_risk_score_structure(self, modeler: RiskModeler) -> None:
        risk = Risk(
            id="test-001",
            category=RiskCategory.SCHEDULE,
            title="Test risk",
            description="Test",
            probability=0.7,
            impact=ImpactLevel.HIGH,
        )
        score = modeler.calculate_risk_score(risk)
        assert isinstance(score, RiskScore)
        assert score.risk_id == "test-001"
        assert score.probability == 0.7
        assert score.impact_level == ImpactLevel.HIGH
        assert score.impact_numeric == 0.75
        # composite = 0.7 * 0.75 = 0.525
        assert abs(score.composite_score - 0.525) < 0.001

    def test_composite_score_is_probability_times_impact(
        self, modeler: RiskModeler
    ) -> None:
        risk = Risk(
            id="test-002",
            category=RiskCategory.TECHNICAL,
            title="Test",
            description="Test",
            probability=0.5,
            impact=ImpactLevel.MEDIUM,
        )
        score = modeler.calculate_risk_score(risk)
        expected = 0.5 * 0.50  # probability * impact_numeric
        assert abs(score.composite_score - expected) < 0.001

    def test_severity_classification(self, modeler: RiskModeler) -> None:
        # Low: composite < 0.25
        low_risk = Risk(
            id="low", category=RiskCategory.SCOPE, title="t", description="d",
            probability=0.2, impact=ImpactLevel.LOW,
        )
        assert modeler.calculate_risk_score(low_risk).severity == "low"

        # High: composite >= 0.50
        high_risk = Risk(
            id="high", category=RiskCategory.SCHEDULE, title="t", description="d",
            probability=0.8, impact=ImpactLevel.HIGH,
        )
        assert modeler.calculate_risk_score(high_risk).severity == "high"

    def test_score_to_dict(self, modeler: RiskModeler) -> None:
        risk = Risk(
            id="dict-test",
            category=RiskCategory.RESOURCE,
            title="Test",
            description="Test",
            probability=0.6,
            impact=ImpactLevel.MEDIUM,
        )
        score = modeler.calculate_risk_score(risk)
        d = score.to_dict()
        assert d["risk_id"] == "dict-test"
        assert d["impact_level"] == "medium"
        assert "composite_score" in d


# ---------------------------------------------------------------------------
# Risk assessment
# ---------------------------------------------------------------------------

class TestRiskAssessment:
    def test_assess_plan_risk_structure(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        assessment = modeler.assess_plan_risk("plan-1", high_risk_plan)
        assert isinstance(assessment, RiskAssessment)
        assert assessment.plan_id == "plan-1"
        assert len(assessment.risks) > 0
        assert len(assessment.scores) == len(assessment.risks)
        assert 0.0 <= assessment.overall_risk_score <= 1.0
        assert assessment.risk_level in ("low", "medium", "high", "critical")

    def test_assessment_includes_heat_map(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        assessment = modeler.assess_plan_risk("plan-2", high_risk_plan)
        assert len(assessment.heat_map) > 0
        for cell in assessment.heat_map:
            assert isinstance(cell, HeatMapCell)
            assert cell.probability_range
            assert cell.impact_level

    def test_assessment_includes_mitigations(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        assessment = modeler.assess_plan_risk("plan-3", high_risk_plan)
        assert len(assessment.mitigations) > 0
        for m in assessment.mitigations:
            assert isinstance(m, Mitigation)

    def test_assessment_includes_category_summary(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        assessment = modeler.assess_plan_risk("plan-4", high_risk_plan)
        summary = assessment.category_summary
        for cat in RiskCategory:
            assert cat.value in summary
            assert "risk_count" in summary[cat.value]
            assert "avg_composite_score" in summary[cat.value]

    def test_assessment_to_dict(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        assessment = modeler.assess_plan_risk("plan-5", high_risk_plan)
        d = assessment.to_dict()
        assert d["plan_id"] == "plan-5"
        assert isinstance(d["risks"], list)
        assert isinstance(d["scores"], list)
        assert isinstance(d["heat_map"], list)
        assert isinstance(d["mitigations"], list)

    def test_empty_plan_assessment(self, modeler: RiskModeler) -> None:
        assessment = modeler.assess_plan_risk("empty-plan", {})
        assert assessment.overall_risk_score == 0.0
        assert assessment.risk_level == "low"
        assert len(assessment.risks) == 0


# ---------------------------------------------------------------------------
# Heat map
# ---------------------------------------------------------------------------

class TestHeatMap:
    def test_heat_map_has_all_cells(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        assessment = modeler.assess_plan_risk("hm-1", high_risk_plan)
        # 5 probability ranges * 4 impact levels = 20 cells
        assert len(assessment.heat_map) == 20

    def test_heat_map_risk_ids_match(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        assessment = modeler.assess_plan_risk("hm-2", high_risk_plan)
        all_risk_ids = {r.id for r in assessment.risks}
        hm_risk_ids = set()
        for cell in assessment.heat_map:
            hm_risk_ids.update(cell.risk_ids)
        # Every risk should appear in exactly one cell
        assert hm_risk_ids == all_risk_ids

    def test_heat_map_cell_to_dict(self) -> None:
        cell = HeatMapCell(
            probability_range="high (0.6-0.8)",
            impact_level="critical",
            risk_ids=("r1", "r2"),
            count=2,
        )
        d = cell.to_dict()
        assert d["count"] == 2
        assert d["risk_ids"] == ["r1", "r2"]


# ---------------------------------------------------------------------------
# Impact simulation
# ---------------------------------------------------------------------------

class TestImpactSimulation:
    def test_simulate_risk_impact_structure(self, modeler: RiskModeler) -> None:
        risk = Risk(
            id="sim-001",
            category=RiskCategory.SCHEDULE,
            title="Delay risk",
            description="Test",
            probability=0.8,
            impact=ImpactLevel.HIGH,
        )
        plan = {"estimated_days": 30, "num_tasks": 10}
        analysis = modeler.simulate_risk_impact(risk, plan)

        assert isinstance(analysis, ImpactAnalysis)
        assert analysis.risk_id == "sim-001"
        assert analysis.schedule_delay_days > 0
        assert analysis.cost_increase_pct > 0
        assert analysis.quality_impact
        assert len(analysis.affected_tasks) > 0

    def test_higher_impact_produces_larger_delay(self, modeler: RiskModeler) -> None:
        plan = {"estimated_days": 30, "num_tasks": 10}

        low_risk = Risk(
            id="low-sim", category=RiskCategory.SCHEDULE, title="t", description="d",
            probability=0.3, impact=ImpactLevel.LOW,
        )
        high_risk = Risk(
            id="high-sim", category=RiskCategory.SCHEDULE, title="t", description="d",
            probability=0.9, impact=ImpactLevel.CRITICAL,
        )

        low_analysis = modeler.simulate_risk_impact(low_risk, plan)
        high_analysis = modeler.simulate_risk_impact(high_risk, plan)

        assert high_analysis.schedule_delay_days > low_analysis.schedule_delay_days

    def test_cascading_effects_by_category(self, modeler: RiskModeler) -> None:
        plan = {"estimated_days": 10, "num_tasks": 5}

        for category in RiskCategory:
            risk = Risk(
                id=f"{category.value}-cascade",
                category=category,
                title="t",
                description="d",
                probability=0.5,
                impact=ImpactLevel.MEDIUM,
            )
            analysis = modeler.simulate_risk_impact(risk, plan)
            assert len(analysis.cascading_effects) > 0

    def test_impact_analysis_to_dict(self, modeler: RiskModeler) -> None:
        risk = Risk(
            id="dict-sim", category=RiskCategory.TECHNICAL, title="t", description="d",
            probability=0.5, impact=ImpactLevel.MEDIUM,
        )
        analysis = modeler.simulate_risk_impact(risk, {"estimated_days": 20, "num_tasks": 5})
        d = analysis.to_dict()
        assert "schedule_delay_days" in d
        assert "cost_increase_pct" in d
        assert isinstance(d["affected_tasks"], list)


# ---------------------------------------------------------------------------
# Mitigation recommendations
# ---------------------------------------------------------------------------

class TestMitigations:
    def test_generates_mitigations_for_each_category(self, modeler: RiskModeler) -> None:
        risks = [
            Risk(id="s1", category=RiskCategory.SCHEDULE, title="t", description="d"),
            Risk(id="r1", category=RiskCategory.RESOURCE, title="t", description="d"),
            Risk(id="t1", category=RiskCategory.TECHNICAL, title="t", description="d"),
            Risk(id="d1", category=RiskCategory.DEPENDENCY, title="t", description="d"),
            Risk(id="sc1", category=RiskCategory.SCOPE, title="t", description="d"),
        ]
        mitigations = modeler.generate_mitigation_recommendations(risks)
        assert len(mitigations) >= 5  # at least one per category (actually 2 each)

        risk_ids_in_mitigations = {m.risk_id for m in mitigations}
        for risk in risks:
            assert risk.id in risk_ids_in_mitigations

    def test_mitigation_structure(self, modeler: RiskModeler) -> None:
        risks = [Risk(id="m1", category=RiskCategory.SCHEDULE, title="t", description="d")]
        mitigations = modeler.generate_mitigation_recommendations(risks)
        assert mitigations
        m = mitigations[0]
        assert m.risk_id == "m1"
        assert m.strategy
        assert m.description
        assert m.priority in ("low", "medium", "high")
        assert m.effort in ("low", "medium", "high")

    def test_mitigation_to_dict(self, modeler: RiskModeler) -> None:
        risks = [Risk(id="md1", category=RiskCategory.TECHNICAL, title="t", description="d")]
        mitigations = modeler.generate_mitigation_recommendations(risks)
        d = mitigations[0].to_dict()
        assert "strategy" in d
        assert "priority" in d

    def test_schedule_mitigations_include_buffer(self, modeler: RiskModeler) -> None:
        risks = [Risk(id="buf", category=RiskCategory.SCHEDULE, title="t", description="d")]
        mitigations = modeler.generate_mitigation_recommendations(risks)
        strategies = [m.strategy for m in mitigations]
        assert "Add buffer time" in strategies

    def test_technical_mitigations_include_prototype(self, modeler: RiskModeler) -> None:
        risks = [Risk(id="proto", category=RiskCategory.TECHNICAL, title="t", description="d")]
        mitigations = modeler.generate_mitigation_recommendations(risks)
        strategies = [m.strategy for m in mitigations]
        assert "Prototype unknowns" in strategies

    def test_empty_risks_produce_no_mitigations(self, modeler: RiskModeler) -> None:
        mitigations = modeler.generate_mitigation_recommendations([])
        assert mitigations == []


# ---------------------------------------------------------------------------
# Historical data calibration
# ---------------------------------------------------------------------------

class TestHistoricalCalibration:
    def test_load_historical_data(
        self, modeler: RiskModeler, historical_data: list[dict]
    ) -> None:
        modeler.load_historical_data(historical_data)
        assert len(modeler._historical_data) == 5

    def test_calibration_affects_risk_probability(
        self, modeler: RiskModeler, historical_data: list[dict]
    ) -> None:
        plan = {
            "estimated_days": 30,
            "buffer_days": 1,
            "num_tasks": 25,
            "scope_change_count": 5,
            "requirements_stability": "low",
        }

        risks_uncalibrated = modeler.identify_risks(plan)
        modeler.load_historical_data(historical_data)
        risks_calibrated = modeler.identify_risks(plan)

        # probabilities should differ
        uncal_probs = {r.category: r.probability for r in risks_uncalibrated}
        cal_probs = {r.category: r.probability for r in risks_calibrated}

        common_cats = set(uncal_probs.keys()) & set(cal_probs.keys())
        assert common_cats
        # At least one category should have a different probability
        assert any(uncal_probs[c] != cal_probs[c] for c in common_cats)

    def test_calibration_probabilities_remain_bounded(
        self, modeler: RiskModeler, historical_data: list[dict], high_risk_plan: dict
    ) -> None:
        modeler.load_historical_data(historical_data)
        risks = modeler.identify_risks(high_risk_plan)
        for risk in risks:
            assert 0.0 <= risk.probability <= 1.0


# ---------------------------------------------------------------------------
# Risk trend tracking
# ---------------------------------------------------------------------------

class TestRiskTrending:
    def test_track_risk_records_history(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        assessment = modeler.assess_plan_risk("trend-1", high_risk_plan)
        trend = modeler.track_risk("2025-01-01", assessment)
        assert isinstance(trend, RiskTrend)
        assert trend.timestamp == "2025-01-01"
        assert len(modeler.risk_history) == 1

    def test_increasing_trend_triggers_alert(self, modeler: RiskModeler) -> None:
        # Create 3 assessments with increasing risk
        trend = None
        for i, score in enumerate([0.3, 0.5, 0.7]):
            assessment = RiskAssessment(
                plan_id="trend-plan",
                overall_risk_score=score,
                risk_level="high",
                category_summary={},
            )
            trend = modeler.track_risk(f"2025-01-0{i+1}", assessment)

        # The third data point should trigger an alert
        assert trend is not None
        assert trend.alert is not None
        assert "increasing" in trend.alert.lower()

    def test_stable_trend_no_alert(self, modeler: RiskModeler) -> None:
        trend = None
        for i, score in enumerate([0.5, 0.5, 0.5]):
            assessment = RiskAssessment(
                plan_id="stable-plan",
                overall_risk_score=score,
                risk_level="medium",
                category_summary={},
            )
            trend = modeler.track_risk(f"2025-01-0{i+1}", assessment)

        assert trend is not None
        assert trend.alert is None

    def test_trend_to_dict(self, modeler: RiskModeler) -> None:
        assessment = RiskAssessment(
            plan_id="dict-trend",
            overall_risk_score=0.4,
            risk_level="medium",
            category_summary={"schedule": {"avg_composite_score": 0.4}},
        )
        trend = modeler.track_risk("2025-03-01", assessment)
        d = trend.to_dict()
        assert d["timestamp"] == "2025-03-01"
        assert "overall_score" in d
        assert "category_scores" in d


# ---------------------------------------------------------------------------
# Data class serialization
# ---------------------------------------------------------------------------

class TestSerialization:
    def test_risk_to_dict(self) -> None:
        risk = Risk(
            id="ser-1",
            category=RiskCategory.SCHEDULE,
            title="Test",
            description="Desc",
            probability=0.6,
            impact=ImpactLevel.HIGH,
            indicators=("tight_deadline", "many_tasks"),
        )
        d = risk.to_dict()
        assert d["id"] == "ser-1"
        assert d["category"] == "schedule"
        assert d["probability"] == 0.6
        assert d["impact"] == "high"
        assert d["indicators"] == ["tight_deadline", "many_tasks"]

    def test_risk_assessment_round_trip(
        self, modeler: RiskModeler, high_risk_plan: dict
    ) -> None:
        assessment = modeler.assess_plan_risk("rt-1", high_risk_plan)
        d = assessment.to_dict()
        assert isinstance(d, dict)
        assert d["plan_id"] == "rt-1"
        assert len(d["risks"]) == len(assessment.risks)
        assert len(d["scores"]) == len(assessment.scores)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_single_person_team_high_task_count(self, modeler: RiskModeler) -> None:
        plan = {"team_size": 1, "num_tasks": 10}
        risks = modeler.identify_risks(plan)
        resource_risks = [r for r in risks if r.category == RiskCategory.RESOURCE]
        assert resource_risks
        assert "single_point_of_failure" in resource_risks[0].indicators

    def test_zero_estimated_days(self, modeler: RiskModeler) -> None:
        plan = {"estimated_days": 0, "buffer_days": 0}
        risks = modeler.identify_risks(plan)
        # Should not crash; may or may not produce schedule risk
        for r in risks:
            assert 0.0 <= r.probability <= 1.0

    def test_negative_values_handled(self, modeler: RiskModeler) -> None:
        plan = {"estimated_days": -5, "num_tasks": -1, "team_size": 0}
        # Should not crash
        risks = modeler.identify_risks(plan)
        assert isinstance(risks, list)

    def test_very_high_values(self, modeler: RiskModeler) -> None:
        plan = {
            "estimated_days": 1000,
            "buffer_days": 10,
            "num_tasks": 500,
            "team_size": 100,
            "avg_task_complexity": 10,
            "external_dependencies": 50,
            "blocker_count": 20,
            "scope_change_count": 100,
        }
        risks = modeler.identify_risks(plan)
        for r in risks:
            # Probability must still be clamped to [0, 1]
            assert 0.0 <= r.probability <= 1.0
