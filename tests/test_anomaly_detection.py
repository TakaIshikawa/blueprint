"""Tests for anomaly detection system."""

import pytest

from blueprint.analytics.anomaly_detection import (
    ActionRecommendation,
    Anomaly,
    AnomalyDetector,
    AnomalyType,
    BaselineModel,
    Explanation,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def detector() -> AnomalyDetector:
    return AnomalyDetector()


@pytest.fixture
def historical_plans() -> list[dict]:
    """Baseline training data with normal execution metrics."""
    return [
        {
            "velocity": 10.0,
            "actual_vs_estimate_ratio": 1.1,
            "scope_additions": 2,
            "blocked_tasks": 1,
            "reassignment_count": 0,
            "defect_rate": 0.05,
        },
        {
            "velocity": 12.0,
            "actual_vs_estimate_ratio": 1.0,
            "scope_additions": 1,
            "blocked_tasks": 0,
            "reassignment_count": 1,
            "defect_rate": 0.04,
        },
        {
            "velocity": 11.0,
            "actual_vs_estimate_ratio": 0.95,
            "scope_additions": 3,
            "blocked_tasks": 1,
            "reassignment_count": 0,
            "defect_rate": 0.06,
        },
        {
            "velocity": 10.5,
            "actual_vs_estimate_ratio": 1.05,
            "scope_additions": 2,
            "blocked_tasks": 0,
            "reassignment_count": 1,
            "defect_rate": 0.05,
        },
        {
            "velocity": 11.5,
            "actual_vs_estimate_ratio": 1.0,
            "scope_additions": 1,
            "blocked_tasks": 1,
            "reassignment_count": 0,
            "defect_rate": 0.03,
        },
    ]


@pytest.fixture
def trained_detector(
    detector: AnomalyDetector,
    historical_plans: list[dict],
) -> AnomalyDetector:
    detector.train_baseline(historical_plans)
    return detector


# ---------------------------------------------------------------------------
# Baseline training
# ---------------------------------------------------------------------------

class TestTrainBaseline:
    def test_train_returns_baseline_model(
        self, detector: AnomalyDetector, historical_plans: list[dict]
    ) -> None:
        baseline = detector.train_baseline(historical_plans)
        assert isinstance(baseline, BaselineModel)
        assert baseline.sample_count == 5
        assert "velocity" in baseline.metrics

    def test_train_sets_is_trained(
        self, detector: AnomalyDetector, historical_plans: list[dict]
    ) -> None:
        assert not detector.is_trained
        detector.train_baseline(historical_plans)
        assert detector.is_trained

    def test_train_requires_minimum_samples(self, detector: AnomalyDetector) -> None:
        with pytest.raises(ValueError, match="at least 3"):
            detector.train_baseline([{"velocity": 1}, {"velocity": 2}])

    def test_baseline_metrics_have_mean_and_std(
        self, trained_detector: AnomalyDetector
    ) -> None:
        baseline = trained_detector.baseline
        assert baseline is not None
        for key in ("velocity", "actual_vs_estimate_ratio", "scope_additions"):
            assert "mean" in baseline.metrics[key]
            assert "std" in baseline.metrics[key]

    def test_baseline_to_dict(
        self, trained_detector: AnomalyDetector
    ) -> None:
        baseline = trained_detector.baseline
        assert baseline is not None
        d = baseline.to_dict()
        assert d["sample_count"] == 5
        assert isinstance(d["metrics"], dict)


# ---------------------------------------------------------------------------
# Anomaly detection: velocity drop
# ---------------------------------------------------------------------------

class TestVelocityDrop:
    def test_detects_velocity_drop(self, trained_detector: AnomalyDetector) -> None:
        plan = {"velocity": 2.0}  # Much lower than baseline ~11
        anomalies = trained_detector.detect_anomalies(plan)
        types = [a.anomaly_type for a in anomalies]
        assert AnomalyType.VELOCITY_DROP in types

    def test_no_anomaly_for_normal_velocity(
        self, trained_detector: AnomalyDetector
    ) -> None:
        plan = {"velocity": 11.0}
        anomalies = trained_detector.detect_anomalies(plan)
        types = [a.anomaly_type for a in anomalies]
        assert AnomalyType.VELOCITY_DROP not in types


# ---------------------------------------------------------------------------
# Anomaly detection: estimate inflation
# ---------------------------------------------------------------------------

class TestEstimateInflation:
    def test_detects_estimate_inflation(
        self, trained_detector: AnomalyDetector
    ) -> None:
        plan = {"actual_vs_estimate_ratio": 3.0}  # Much higher than ~1.0
        anomalies = trained_detector.detect_anomalies(plan)
        types = [a.anomaly_type for a in anomalies]
        assert AnomalyType.ESTIMATE_INFLATION in types

    def test_no_anomaly_for_normal_ratio(
        self, trained_detector: AnomalyDetector
    ) -> None:
        plan = {"actual_vs_estimate_ratio": 1.05}
        anomalies = trained_detector.detect_anomalies(plan)
        types = [a.anomaly_type for a in anomalies]
        assert AnomalyType.ESTIMATE_INFLATION not in types


# ---------------------------------------------------------------------------
# Anomaly detection: scope creep
# ---------------------------------------------------------------------------

class TestScopeCreep:
    def test_detects_scope_creep(self, trained_detector: AnomalyDetector) -> None:
        plan = {"scope_additions": 20}  # Far above baseline ~2
        anomalies = trained_detector.detect_anomalies(plan)
        types = [a.anomaly_type for a in anomalies]
        assert AnomalyType.SCOPE_CREEP in types

    def test_no_anomaly_for_normal_scope(
        self, trained_detector: AnomalyDetector
    ) -> None:
        plan = {"scope_additions": 2}
        anomalies = trained_detector.detect_anomalies(plan)
        types = [a.anomaly_type for a in anomalies]
        assert AnomalyType.SCOPE_CREEP not in types


# ---------------------------------------------------------------------------
# Dependency violations & resource thrashing
# ---------------------------------------------------------------------------

class TestDependencyAndResourceAnomalies:
    def test_detects_dependency_violation(
        self, trained_detector: AnomalyDetector
    ) -> None:
        plan = {"blocked_tasks": 10}
        anomalies = trained_detector.detect_anomalies(plan)
        types = [a.anomaly_type for a in anomalies]
        assert AnomalyType.DEPENDENCY_VIOLATION in types

    def test_detects_resource_thrashing(
        self, trained_detector: AnomalyDetector
    ) -> None:
        plan = {"reassignment_count": 15}
        anomalies = trained_detector.detect_anomalies(plan)
        types = [a.anomaly_type for a in anomalies]
        assert AnomalyType.RESOURCE_THRASHING in types


# ---------------------------------------------------------------------------
# Statistical methods
# ---------------------------------------------------------------------------

class TestStatisticalMethods:
    def test_z_score_based_detection(
        self, trained_detector: AnomalyDetector
    ) -> None:
        """Anomalies with extreme z-scores should be detected."""
        plan = {"velocity": 0.1, "defect_rate": 0.5}
        anomalies = trained_detector.detect_anomalies(plan)
        assert len(anomalies) > 0
        for a in anomalies:
            assert a.deviation != 0.0

    def test_severity_score_range(
        self, trained_detector: AnomalyDetector
    ) -> None:
        plan = {"velocity": 1.0, "scope_additions": 50, "defect_rate": 0.9}
        anomalies = trained_detector.detect_anomalies(plan)
        for a in anomalies:
            assert 0 <= a.severity_score <= 100

    def test_moving_average_used_for_trend(
        self, trained_detector: AnomalyDetector
    ) -> None:
        """Moving average trend detection should work with history."""
        # The detector should have velocity history from training
        assert "velocity" in trained_detector._metric_histories
        assert len(trained_detector._metric_histories["velocity"]) >= 3


# ---------------------------------------------------------------------------
# ML-style detection (isolation forest scoring, LSTM-style)
# ---------------------------------------------------------------------------

class TestMLMethods:
    def test_isolation_forest_style_multivariate(
        self, trained_detector: AnomalyDetector
    ) -> None:
        """Multiple anomalous metrics at once should produce multiple detections."""
        plan = {
            "velocity": 1.0,
            "actual_vs_estimate_ratio": 5.0,
            "scope_additions": 30,
            "blocked_tasks": 15,
            "reassignment_count": 20,
            "defect_rate": 0.8,
        }
        anomalies = trained_detector.detect_anomalies(plan)
        # Should detect multiple anomaly types
        types = {a.anomaly_type for a in anomalies}
        assert len(types) >= 3

    def test_lstm_style_time_series_trend(
        self, trained_detector: AnomalyDetector
    ) -> None:
        """Declining velocity trend should be detected via time-series analysis."""
        # Inject declining velocity history
        trained_detector._metric_histories["velocity"] = [
            15.0, 13.0, 11.0, 9.0, 7.0, 5.0, 3.0,
        ]
        plan = {"velocity": 3.0}
        anomalies = trained_detector.detect_anomalies(plan, window=5)
        types = [a.anomaly_type for a in anomalies]
        assert AnomalyType.VELOCITY_DROP in types


# ---------------------------------------------------------------------------
# Classification & explanation
# ---------------------------------------------------------------------------

class TestClassifyAndExplain:
    def test_classify_anomaly_returns_type(
        self, trained_detector: AnomalyDetector
    ) -> None:
        anomaly = Anomaly(
            id="test-1",
            anomaly_type=AnomalyType.SCOPE_CREEP,
            severity_score=60,
            description="Test",
        )
        result = trained_detector.classify_anomaly(anomaly)
        assert result == AnomalyType.SCOPE_CREEP

    def test_explain_anomaly_structure(
        self, trained_detector: AnomalyDetector
    ) -> None:
        anomaly = Anomaly(
            id="test-2",
            anomaly_type=AnomalyType.VELOCITY_DROP,
            severity_score=75,
            description="Velocity dropped",
            contributing_factors=("sudden_slowdown",),
        )
        explanation = trained_detector.explain_anomaly(anomaly)
        assert isinstance(explanation, Explanation)
        assert explanation.anomaly_id == "test-2"
        assert explanation.summary
        assert len(explanation.contributing_factors) > 0
        assert explanation.recommended_investigation

    def test_explain_with_context(
        self, trained_detector: AnomalyDetector
    ) -> None:
        anomaly = Anomaly(
            id="ctx-1",
            anomaly_type=AnomalyType.RESOURCE_THRASHING,
            severity_score=50,
            description="Test",
            contributing_factors=("frequent_reassignments",),
        )
        context = {"recent_incidents": True, "team_changes": True}
        explanation = trained_detector.explain_anomaly(anomaly, context)
        assert "recent_incidents_reported" in explanation.contributing_factors
        assert "team_composition_changed" in explanation.contributing_factors

    def test_explanation_to_dict(
        self, trained_detector: AnomalyDetector
    ) -> None:
        anomaly = Anomaly(
            id="d-1",
            anomaly_type=AnomalyType.SCOPE_CREEP,
            severity_score=40,
            description="Test",
        )
        explanation = trained_detector.explain_anomaly(anomaly)
        d = explanation.to_dict()
        assert "anomaly_id" in d
        assert "summary" in d
        assert "recommended_investigation" in d


# ---------------------------------------------------------------------------
# Action recommendations
# ---------------------------------------------------------------------------

class TestRecommendAction:
    def test_recommend_action_structure(
        self, trained_detector: AnomalyDetector
    ) -> None:
        anomaly = Anomaly(
            id="act-1",
            anomaly_type=AnomalyType.DEPENDENCY_VIOLATION,
            severity_score=80,
            description="Test",
        )
        rec = trained_detector.recommend_action(anomaly)
        assert isinstance(rec, ActionRecommendation)
        assert rec.anomaly_id == "act-1"
        assert rec.action
        assert rec.priority in ("low", "medium", "high")
        assert rec.rationale

    def test_all_anomaly_types_have_recommendations(
        self, trained_detector: AnomalyDetector
    ) -> None:
        for atype in AnomalyType:
            anomaly = Anomaly(
                id=f"all-{atype.value}",
                anomaly_type=atype,
                severity_score=50,
                description="Test",
            )
            rec = trained_detector.recommend_action(anomaly)
            assert rec.action
            assert rec.priority

    def test_velocity_drop_recommends_investigate(
        self, trained_detector: AnomalyDetector
    ) -> None:
        anomaly = Anomaly(
            id="vd-1",
            anomaly_type=AnomalyType.VELOCITY_DROP,
            severity_score=70,
            description="Test",
        )
        rec = trained_detector.recommend_action(anomaly)
        assert "blocker" in rec.action.lower() or "investigate" in rec.action.lower()

    def test_scope_creep_recommends_freeze(
        self, trained_detector: AnomalyDetector
    ) -> None:
        anomaly = Anomaly(
            id="sc-1",
            anomaly_type=AnomalyType.SCOPE_CREEP,
            severity_score=65,
            description="Test",
        )
        rec = trained_detector.recommend_action(anomaly)
        assert "scope" in rec.action.lower() or "freeze" in rec.action.lower()

    def test_recommendation_to_dict(
        self, trained_detector: AnomalyDetector
    ) -> None:
        anomaly = Anomaly(
            id="rd-1",
            anomaly_type=AnomalyType.QUALITY_DEGRADATION,
            severity_score=60,
            description="Test",
        )
        rec = trained_detector.recommend_action(anomaly)
        d = rec.to_dict()
        assert "action" in d
        assert "priority" in d
        assert "rationale" in d


# ---------------------------------------------------------------------------
# Anomaly data class
# ---------------------------------------------------------------------------

class TestAnomalyDataClass:
    def test_anomaly_to_dict(self) -> None:
        anomaly = Anomaly(
            id="ser-1",
            anomaly_type=AnomalyType.VELOCITY_DROP,
            severity_score=75.5,
            description="Test anomaly",
            timestamp="2025-01-15",
            metric_name="velocity",
            expected_value=11.0,
            actual_value=2.0,
            deviation=-3.5,
            contributing_factors=("factor1", "factor2"),
        )
        d = anomaly.to_dict()
        assert d["id"] == "ser-1"
        assert d["anomaly_type"] == "velocity_drop"
        assert d["severity_score"] == 75.5
        assert d["contributing_factors"] == ["factor1", "factor2"]


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_detect_without_training_raises(
        self, detector: AnomalyDetector
    ) -> None:
        with pytest.raises(RuntimeError, match="Baseline not trained"):
            detector.detect_anomalies({"velocity": 5})

    def test_no_anomalies_for_normal_data(
        self, trained_detector: AnomalyDetector
    ) -> None:
        plan = {
            "velocity": 11.0,
            "actual_vs_estimate_ratio": 1.0,
            "scope_additions": 2,
            "blocked_tasks": 0,
            "reassignment_count": 0,
            "defect_rate": 0.05,
        }
        anomalies = trained_detector.detect_anomalies(plan)
        assert len(anomalies) == 0

    def test_empty_plan_data(self, trained_detector: AnomalyDetector) -> None:
        anomalies = trained_detector.detect_anomalies({})
        # Should not crash; all values default to 0 which may or may not be anomalous
        assert isinstance(anomalies, list)

    def test_retrain_overwrites_baseline(
        self, trained_detector: AnomalyDetector
    ) -> None:
        new_data = [
            {"velocity": 100, "scope_additions": 0},
            {"velocity": 105, "scope_additions": 1},
            {"velocity": 95, "scope_additions": 0},
        ]
        trained_detector.train_baseline(new_data)
        assert trained_detector.baseline is not None
        assert trained_detector.baseline.sample_count == 3
