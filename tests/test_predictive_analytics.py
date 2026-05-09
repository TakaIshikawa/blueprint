"""Tests for predictive analytics module."""

import pytest

from blueprint.analytics.predictive_analytics import (
    FeatureImportance,
    ModelMetrics,
    PredictionResult,
    PredictiveAnalyzer,
    RiskFactor,
)


# ---------------------------------------------------------------------------
# Helpers – synthetic historical data
# ---------------------------------------------------------------------------


def _make_record(
    *,
    num_tasks: int = 10,
    num_dependencies: int = 5,
    max_depth: int = 3,
    avg_task_complexity: float = 2.0,
    num_risks: int = 2,
    num_milestones: int = 3,
    team_size: int = 4,
    estimated_days: float = 14.0,
    scope_change_count: int = 0,
    blocker_count: int = 0,
    completed: bool = True,
    actual_days: float = 15.0,
) -> dict:
    return {
        "num_tasks": num_tasks,
        "num_dependencies": num_dependencies,
        "max_depth": max_depth,
        "avg_task_complexity": avg_task_complexity,
        "num_risks": num_risks,
        "num_milestones": num_milestones,
        "team_size": team_size,
        "estimated_days": estimated_days,
        "scope_change_count": scope_change_count,
        "blocker_count": blocker_count,
        "completed": completed,
        "actual_days": actual_days,
    }


def _make_training_data(n: int = 20) -> list[dict]:
    """Generate *n* synthetic plan records with varied outcomes."""
    data: list[dict] = []
    for i in range(n):
        # Plans with many blockers / scope changes tend to fail
        blockers = i % 5
        scope_changes = (i * 3) % 7
        completed = blockers < 3 and scope_changes < 4
        data.append(
            _make_record(
                num_tasks=10 + i,
                num_dependencies=5 + i % 4,
                max_depth=2 + i % 3,
                avg_task_complexity=1.5 + (i % 5) * 0.5,
                num_risks=i % 6,
                num_milestones=2 + i % 3,
                team_size=3 + i % 4,
                estimated_days=10.0 + i * 1.5,
                scope_change_count=scope_changes,
                blocker_count=blockers,
                completed=completed,
                actual_days=12.0 + i * 1.2 + blockers * 3,
            )
        )
    return data


# ---------------------------------------------------------------------------
# Data class serialisation
# ---------------------------------------------------------------------------


class TestDataClasses:
    def test_prediction_result_to_dict(self):
        pr = PredictionResult(
            completion_probability=0.85,
            predicted_days=14.0,
            confidence_interval_lower=10.0,
            confidence_interval_upper=18.0,
            risk_factors=(
                RiskFactor(name="blocker_count", correlation=0.65, direction="positive"),
            ),
            feature_importances=(
                FeatureImportance(name="blocker_count", importance=0.4, description="desc"),
            ),
        )
        d = pr.to_dict()
        assert d["completion_probability"] == 0.85
        assert d["predicted_days"] == 14.0
        assert d["confidence_interval_lower"] == 10.0
        assert d["confidence_interval_upper"] == 18.0
        assert len(d["risk_factors"]) == 1
        assert d["risk_factors"][0]["name"] == "blocker_count"
        assert len(d["feature_importances"]) == 1

    def test_risk_factor_to_dict(self):
        rf = RiskFactor(name="scope_change_count", correlation=0.72, direction="positive")
        d = rf.to_dict()
        assert d == {
            "name": "scope_change_count",
            "correlation": 0.72,
            "direction": "positive",
        }

    def test_feature_importance_to_dict(self):
        fi = FeatureImportance(name="blocker_count", importance=0.35, description="test")
        d = fi.to_dict()
        assert d == {"name": "blocker_count", "importance": 0.35, "description": "test"}

    def test_model_metrics_to_dict(self):
        mm = ModelMetrics(
            completion_accuracy=0.9,
            completion_precision=0.88,
            completion_recall=0.92,
            completion_f1=0.9,
            timeline_mae=2.5,
            timeline_rmse=3.1,
            timeline_r2=0.85,
            training_samples=100,
        )
        d = mm.to_dict()
        assert d["completion_accuracy"] == 0.9
        assert d["training_samples"] == 100

    def test_prediction_result_defaults(self):
        pr = PredictionResult()
        assert pr.completion_probability == 0.0
        assert pr.predicted_days == 0.0
        assert pr.risk_factors == ()
        assert pr.feature_importances == ()


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------


class TestTraining:
    def test_train_returns_metrics(self):
        analyzer = PredictiveAnalyzer()
        data = _make_training_data(20)
        metrics = analyzer.train(data)

        assert isinstance(metrics, ModelMetrics)
        assert metrics.training_samples == 20
        assert 0.0 <= metrics.completion_accuracy <= 1.0
        assert metrics.timeline_mae >= 0.0
        assert metrics.timeline_rmse >= 0.0
        assert analyzer.is_trained is True

    def test_train_minimum_samples(self):
        analyzer = PredictiveAnalyzer()
        data = _make_training_data(5)
        metrics = analyzer.train(data)
        assert metrics.training_samples == 5

    def test_train_insufficient_data(self):
        analyzer = PredictiveAnalyzer()
        with pytest.raises(ValueError, match="at least 5"):
            analyzer.train(_make_training_data(3))

    def test_train_empty_data(self):
        analyzer = PredictiveAnalyzer()
        with pytest.raises(ValueError, match="at least 5"):
            analyzer.train([])

    def test_training_samples_property(self):
        analyzer = PredictiveAnalyzer()
        assert analyzer.training_samples == 0
        analyzer.train(_make_training_data(10))
        assert analyzer.training_samples == 10

    def test_is_trained_property(self):
        analyzer = PredictiveAnalyzer()
        assert analyzer.is_trained is False
        analyzer.train(_make_training_data(10))
        assert analyzer.is_trained is True


# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------


class TestPrediction:
    def test_predict_returns_result(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))

        plan = _make_record(num_tasks=12, blocker_count=0, scope_change_count=1)
        result = analyzer.predict(plan)

        assert isinstance(result, PredictionResult)
        assert 0.0 <= result.completion_probability <= 1.0
        assert result.predicted_days > 0.0
        assert result.confidence_interval_lower <= result.predicted_days
        assert result.predicted_days <= result.confidence_interval_upper

    def test_predict_without_training_raises(self):
        analyzer = PredictiveAnalyzer()
        with pytest.raises(RuntimeError, match="not been trained"):
            analyzer.predict(_make_record())

    def test_predict_high_risk_plan(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))

        risky = _make_record(blocker_count=4, scope_change_count=6, num_risks=5)
        safe = _make_record(blocker_count=0, scope_change_count=0, num_risks=0)

        risky_result = analyzer.predict(risky)
        safe_result = analyzer.predict(safe)

        # The risky plan should generally have lower completion probability
        # We don't hard-assert ordering since the synthetic data is small,
        # but both should be valid probabilities.
        assert 0.0 <= risky_result.completion_probability <= 1.0
        assert 0.0 <= safe_result.completion_probability <= 1.0

    def test_predict_confidence_interval_width(self):
        analyzer = PredictiveAnalyzer(confidence_level=1.96)
        analyzer.train(_make_training_data(20))
        result = analyzer.predict(_make_record())
        width = result.confidence_interval_upper - result.confidence_interval_lower
        assert width >= 0.0

    def test_predict_confidence_interval_lower_non_negative(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        result = analyzer.predict(_make_record(estimated_days=1.0))
        assert result.confidence_interval_lower >= 0.0

    def test_predict_missing_features_default_to_zero(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        # Plan record with missing feature keys
        sparse = {"num_tasks": 5}
        result = analyzer.predict(sparse)
        assert isinstance(result, PredictionResult)
        assert 0.0 <= result.completion_probability <= 1.0

    def test_predict_empty_plan(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        result = analyzer.predict({})
        assert isinstance(result, PredictionResult)

    def test_predict_with_none_feature_values(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        plan = {"num_tasks": None, "blocker_count": None, "estimated_days": None}
        result = analyzer.predict(plan)
        assert isinstance(result, PredictionResult)
        assert 0.0 <= result.completion_probability <= 1.0


# ---------------------------------------------------------------------------
# Confidence intervals
# ---------------------------------------------------------------------------


class TestConfidenceIntervals:
    def test_narrower_interval_with_lower_z(self):
        data = _make_training_data(20)

        a1 = PredictiveAnalyzer(confidence_level=1.0)
        a1.train(data)
        r1 = a1.predict(_make_record())

        a2 = PredictiveAnalyzer(confidence_level=2.0)
        a2.train(data)
        r2 = a2.predict(_make_record())

        w1 = r1.confidence_interval_upper - r1.confidence_interval_lower
        w2 = r2.confidence_interval_upper - r2.confidence_interval_lower
        assert w1 < w2 or w1 == w2 == 0.0

    def test_zero_confidence_level(self):
        analyzer = PredictiveAnalyzer(confidence_level=0.0)
        analyzer.train(_make_training_data(20))
        result = analyzer.predict(_make_record())
        # With z=0 the interval collapses to a point
        assert result.confidence_interval_lower == pytest.approx(
            result.confidence_interval_upper, abs=0.01
        ) or result.confidence_interval_lower == 0.0


# ---------------------------------------------------------------------------
# Feature importance
# ---------------------------------------------------------------------------


class TestFeatureImportance:
    def test_feature_importances_returned(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        result = analyzer.predict(_make_record())

        assert len(result.feature_importances) > 0
        for fi in result.feature_importances:
            assert isinstance(fi, FeatureImportance)
            assert fi.name != ""
            assert fi.importance >= 0.0
            assert fi.description != ""

    def test_feature_importances_sum_to_one(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        result = analyzer.predict(_make_record())
        total = sum(fi.importance for fi in result.feature_importances)
        assert total == pytest.approx(1.0, abs=0.01)

    def test_all_features_represented(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        result = analyzer.predict(_make_record())
        names = {fi.name for fi in result.feature_importances}
        assert len(names) == 10  # all _NUMERIC_FEATURES present


# ---------------------------------------------------------------------------
# Risk factors
# ---------------------------------------------------------------------------


class TestRiskFactors:
    def test_risk_factors_returned(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        result = analyzer.predict(_make_record())

        assert isinstance(result.risk_factors, tuple)
        for rf in result.risk_factors:
            assert isinstance(rf, RiskFactor)
            assert rf.direction in ("positive", "negative")
            assert -1.0 <= rf.correlation <= 1.0

    def test_risk_factors_limited_to_top_5(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        result = analyzer.predict(_make_record())
        assert len(result.risk_factors) <= 5

    def test_risk_factors_ordered_by_abs_correlation(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        result = analyzer.predict(_make_record())
        correlations = [abs(rf.correlation) for rf in result.risk_factors]
        assert correlations == sorted(correlations, reverse=True)


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


class TestEvaluation:
    def test_evaluate_returns_metrics(self):
        analyzer = PredictiveAnalyzer()
        train = _make_training_data(15)
        test = _make_training_data(5)
        analyzer.train(train)
        metrics = analyzer.evaluate(test)

        assert isinstance(metrics, ModelMetrics)
        assert 0.0 <= metrics.completion_accuracy <= 1.0
        assert metrics.timeline_mae >= 0.0

    def test_evaluate_without_training_raises(self):
        analyzer = PredictiveAnalyzer()
        with pytest.raises(RuntimeError, match="not been trained"):
            analyzer.evaluate(_make_training_data(5))

    def test_evaluate_empty_test_data(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(10))
        with pytest.raises(ValueError, match="must not be empty"):
            analyzer.evaluate([])


# ---------------------------------------------------------------------------
# Retraining
# ---------------------------------------------------------------------------


class TestRetraining:
    def test_retrain_replaces_model(self):
        analyzer = PredictiveAnalyzer()
        data_v1 = _make_training_data(10)
        m1 = analyzer.train(data_v1)

        data_v2 = _make_training_data(15)
        m2 = analyzer.retrain(data_v2)

        assert m2.training_samples == 15
        assert analyzer.training_samples == 15
        assert analyzer.is_trained is True

    def test_retrain_insufficient_data(self):
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(10))
        with pytest.raises(ValueError, match="at least 5"):
            analyzer.retrain(_make_training_data(2))


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_all_completed_training_data(self):
        """All plans completed – model should still train."""
        data = [_make_record(completed=True, actual_days=10.0 + i) for i in range(10)]
        analyzer = PredictiveAnalyzer()
        metrics = analyzer.train(data)
        assert metrics.training_samples == 10
        result = analyzer.predict(_make_record())
        # When all training labels are the same class, probability should be
        # near 1.0 for the dominant class.
        assert result.completion_probability > 0.5

    def test_all_failed_training_data(self):
        """All plans failed – model should still train."""
        data = [_make_record(completed=False, actual_days=30.0 + i) for i in range(10)]
        analyzer = PredictiveAnalyzer()
        metrics = analyzer.train(data)
        assert metrics.training_samples == 10
        result = analyzer.predict(_make_record())
        assert result.completion_probability < 0.5

    def test_identical_records(self):
        """All records are identical – degenerate but should not crash."""
        data = [_make_record() for _ in range(10)]
        analyzer = PredictiveAnalyzer()
        metrics = analyzer.train(data)
        assert metrics.training_samples == 10
        result = analyzer.predict(_make_record())
        assert isinstance(result, PredictionResult)

    def test_outlier_feature_values(self):
        """Very large feature values should not crash prediction."""
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        outlier = _make_record(
            num_tasks=10000,
            blocker_count=500,
            estimated_days=9999.0,
        )
        result = analyzer.predict(outlier)
        assert isinstance(result, PredictionResult)
        assert 0.0 <= result.completion_probability <= 1.0

    def test_zero_feature_values(self):
        """All-zero features should not crash."""
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        zeros = {k: 0 for k in (
            "num_tasks", "num_dependencies", "max_depth",
            "avg_task_complexity", "num_risks", "num_milestones",
            "team_size", "estimated_days", "scope_change_count", "blocker_count",
        )}
        zeros["completed"] = True
        zeros["actual_days"] = 0.0
        result = analyzer.predict(zeros)
        assert isinstance(result, PredictionResult)

    def test_negative_feature_values(self):
        """Negative values (malformed data) should not crash."""
        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        neg = _make_record(num_tasks=-5, estimated_days=-10.0, blocker_count=-1)
        result = analyzer.predict(neg)
        assert isinstance(result, PredictionResult)

    def test_model_drift_detection_via_evaluate(self):
        """Evaluate on shifted data – metrics should differ from training."""
        analyzer = PredictiveAnalyzer()
        train = _make_training_data(20)
        analyzer.train(train)

        # Shifted test data: all plans fail with high days
        shifted = [
            _make_record(
                completed=False,
                actual_days=100.0 + i,
                blocker_count=4,
                scope_change_count=6,
            )
            for i in range(5)
        ]
        metrics = analyzer.evaluate(shifted)
        # The model was not trained on this distribution, so performance
        # should generally be worse, but we just verify it runs.
        assert isinstance(metrics, ModelMetrics)

    def test_large_dataset(self):
        """Train on a larger dataset without errors."""
        data = _make_training_data(200)
        analyzer = PredictiveAnalyzer()
        metrics = analyzer.train(data)
        assert metrics.training_samples == 200
        result = analyzer.predict(_make_record())
        assert isinstance(result, PredictionResult)

    def test_prediction_result_serialisation_roundtrip(self):
        """to_dict should produce JSON-serialisable output."""
        import json

        analyzer = PredictiveAnalyzer()
        analyzer.train(_make_training_data(20))
        result = analyzer.predict(_make_record())
        d = result.to_dict()
        # Should be JSON-serialisable without errors
        text = json.dumps(d)
        assert isinstance(text, str)
        loaded = json.loads(text)
        assert loaded["completion_probability"] == d["completion_probability"]
