"""Predict plan outcomes and risks using machine learning on historical data.

Trains models on historical plan data (completion time, success rate, risk
realization) and produces predictions with confidence intervals and feature
importance explanations.

Methodology
-----------
* **Completion probability** – a logistic-regression classifier trained on
  binary success/failure labels.  Outputs calibrated probabilities.
* **Timeline prediction** – a ridge-regression model trained on actual
  completion times (days).  Confidence intervals are derived from residual
  standard deviation on the training set.
* **Risk factor correlation** – Pearson correlation between each feature
  and the binary issue indicator, surfaced as signed importance scores.

Limitations
-----------
* Predictions are only as good as the historical data supplied.  Small or
  biased training sets will produce unreliable estimates.
* The models are intentionally simple (linear) so they remain interpretable
  and fast.  Non-linear effects are not captured.
* Confidence intervals assume roughly normal residuals.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import numpy as np
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
)
from sklearn.preprocessing import StandardScaler


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

_NUMERIC_FEATURES: tuple[str, ...] = (
    "num_tasks",
    "num_dependencies",
    "max_depth",
    "avg_task_complexity",
    "num_risks",
    "num_milestones",
    "team_size",
    "estimated_days",
    "scope_change_count",
    "blocker_count",
)


def _extract_features(record: Mapping[str, Any]) -> list[float]:
    """Extract a fixed-length numeric feature vector from a plan record."""
    return [float(record.get(f, 0) or 0) for f in _NUMERIC_FEATURES]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PredictionResult:
    """Outcome prediction for a single plan."""

    completion_probability: float = 0.0
    predicted_days: float = 0.0
    confidence_interval_lower: float = 0.0
    confidence_interval_upper: float = 0.0
    risk_factors: tuple[RiskFactor, ...] = field(default_factory=tuple)
    feature_importances: tuple[FeatureImportance, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "completion_probability": self.completion_probability,
            "predicted_days": self.predicted_days,
            "confidence_interval_lower": self.confidence_interval_lower,
            "confidence_interval_upper": self.confidence_interval_upper,
            "risk_factors": [rf.to_dict() for rf in self.risk_factors],
            "feature_importances": [fi.to_dict() for fi in self.feature_importances],
        }


@dataclass(frozen=True, slots=True)
class RiskFactor:
    """A feature correlated with plan issues."""

    name: str
    correlation: float = 0.0
    direction: str = "positive"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "correlation": self.correlation,
            "direction": self.direction,
        }


@dataclass(frozen=True, slots=True)
class FeatureImportance:
    """Importance of a feature in the prediction model."""

    name: str
    importance: float = 0.0
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "importance": self.importance,
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class ModelMetrics:
    """Evaluation metrics for the trained models."""

    completion_accuracy: float = 0.0
    completion_precision: float = 0.0
    completion_recall: float = 0.0
    completion_f1: float = 0.0
    timeline_mae: float = 0.0
    timeline_rmse: float = 0.0
    timeline_r2: float = 0.0
    training_samples: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "completion_accuracy": self.completion_accuracy,
            "completion_precision": self.completion_precision,
            "completion_recall": self.completion_recall,
            "completion_f1": self.completion_f1,
            "timeline_mae": self.timeline_mae,
            "timeline_rmse": self.timeline_rmse,
            "timeline_r2": self.timeline_r2,
            "training_samples": self.training_samples,
        }


# ---------------------------------------------------------------------------
# Analyzer
# ---------------------------------------------------------------------------

_MIN_TRAINING_SAMPLES = 5


class PredictiveAnalyzer:
    """Build prediction models from historical plan data.

    Parameters
    ----------
    confidence_level : float
        The Z-multiplier for confidence intervals (default 1.96 ≈ 95 %).
    """

    def __init__(self, confidence_level: float = 1.96) -> None:
        self._confidence_z = confidence_level
        self._scaler: StandardScaler | None = None
        self._completion_model: LogisticRegression | None = None
        self._timeline_model: Ridge | None = None
        self._residual_std: float = 0.0
        self._trained = False
        self._training_samples = 0
        self._feature_names: tuple[str, ...] = _NUMERIC_FEATURES
        # Correlation of each feature with the issue indicator
        self._risk_correlations: np.ndarray | None = None
        # When training data has only one class, we skip the classifier
        self._single_class: float | None = None

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train(
        self,
        historical_data: Sequence[Mapping[str, Any]],
    ) -> ModelMetrics:
        """Train prediction models on historical plan records.

        Each record should be a mapping with:
        * Numeric feature keys (see ``_NUMERIC_FEATURES``).
        * ``"completed"`` – bool indicating whether the plan succeeded.
        * ``"actual_days"`` – float with actual completion time in days.

        Returns ``ModelMetrics`` summarising training-set performance.
        Raises ``ValueError`` if fewer than ``_MIN_TRAINING_SAMPLES``
        records are provided.
        """
        if len(historical_data) < _MIN_TRAINING_SAMPLES:
            raise ValueError(
                f"Need at least {_MIN_TRAINING_SAMPLES} training samples, "
                f"got {len(historical_data)}"
            )

        X_raw = np.array([_extract_features(r) for r in historical_data], dtype=np.float64)
        y_completed = np.array(
            [1.0 if r.get("completed", False) else 0.0 for r in historical_data],
            dtype=np.float64,
        )
        y_days = np.array(
            [float(r.get("actual_days", 0) or 0) for r in historical_data],
            dtype=np.float64,
        )

        # Scale features
        self._scaler = StandardScaler()
        X = self._scaler.fit_transform(X_raw)

        # --- Completion classifier ---
        unique_classes = np.unique(y_completed)
        if len(unique_classes) < 2:
            # Single-class: skip logistic regression, record the constant
            self._single_class = float(unique_classes[0])
            self._completion_model = None
            y_pred_cls = y_completed.copy()
        else:
            self._single_class = None
            self._completion_model = LogisticRegression(max_iter=1000, solver="lbfgs")
            self._completion_model.fit(X, y_completed)
            y_pred_cls = self._completion_model.predict(X)

        # --- Timeline regressor ---
        self._timeline_model = Ridge(alpha=1.0)
        self._timeline_model.fit(X, y_days)
        y_pred_days = self._timeline_model.predict(X)
        residuals = y_days - y_pred_days
        self._residual_std = float(np.std(residuals, ddof=1)) if len(residuals) > 1 else 0.0

        # --- Risk correlations (feature ↔ issue indicator) ---
        # "issue" = NOT completed → 1 - y_completed
        issue_indicator = 1.0 - y_completed
        correlations = np.zeros(X.shape[1])
        for i in range(X.shape[1]):
            feat = X[:, i]
            std_feat = np.std(feat)
            std_issue = np.std(issue_indicator)
            if std_feat > 0 and std_issue > 0:
                correlations[i] = float(np.corrcoef(feat, issue_indicator)[0, 1])
        self._risk_correlations = correlations

        self._trained = True
        self._training_samples = len(historical_data)

        return ModelMetrics(
            completion_accuracy=float(accuracy_score(y_completed, y_pred_cls)),
            completion_precision=float(
                precision_score(y_completed, y_pred_cls, zero_division=0.0)
            ),
            completion_recall=float(
                recall_score(y_completed, y_pred_cls, zero_division=0.0)
            ),
            completion_f1=float(f1_score(y_completed, y_pred_cls, zero_division=0.0)),
            timeline_mae=float(mean_absolute_error(y_days, y_pred_days)),
            timeline_rmse=float(math.sqrt(mean_squared_error(y_days, y_pred_days))),
            timeline_r2=float(r2_score(y_days, y_pred_days)) if len(y_days) > 1 else 0.0,
            training_samples=self._training_samples,
        )

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict(self, plan_data: Mapping[str, Any]) -> PredictionResult:
        """Predict outcome for a single plan.

        Raises ``RuntimeError`` if the model has not been trained.
        """
        if not self._trained:
            raise RuntimeError("Model has not been trained. Call train() first.")

        assert self._scaler is not None
        assert self._timeline_model is not None
        assert self._risk_correlations is not None

        x_raw = np.array([_extract_features(plan_data)], dtype=np.float64)
        x_scaled = self._scaler.transform(x_raw)

        # Completion probability
        if self._single_class is not None:
            prob = self._single_class  # constant when only one class was seen
        else:
            assert self._completion_model is not None
            prob = float(self._completion_model.predict_proba(x_scaled)[0, 1])

        # Timeline
        predicted_days = float(self._timeline_model.predict(x_scaled)[0])
        ci_lower = max(0.0, predicted_days - self._confidence_z * self._residual_std)
        ci_upper = predicted_days + self._confidence_z * self._residual_std

        # Risk factors (top correlated features)
        risk_factors = self._top_risk_factors()

        # Feature importances from the completion model coefficients
        feature_importances = self._feature_importances()

        return PredictionResult(
            completion_probability=prob,
            predicted_days=predicted_days,
            confidence_interval_lower=ci_lower,
            confidence_interval_upper=ci_upper,
            risk_factors=risk_factors,
            feature_importances=feature_importances,
        )

    # ------------------------------------------------------------------
    # Retraining
    # ------------------------------------------------------------------

    def retrain(
        self,
        historical_data: Sequence[Mapping[str, Any]],
    ) -> ModelMetrics:
        """Retrain the models with new/updated historical data.

        This fully replaces the existing models.
        """
        return self.train(historical_data)

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def evaluate(
        self,
        test_data: Sequence[Mapping[str, Any]],
    ) -> ModelMetrics:
        """Evaluate the trained models against a held-out test set.

        Raises ``RuntimeError`` if the model has not been trained.
        """
        if not self._trained:
            raise RuntimeError("Model has not been trained. Call train() first.")

        assert self._scaler is not None
        assert self._timeline_model is not None

        if not test_data:
            raise ValueError("test_data must not be empty")

        X_raw = np.array([_extract_features(r) for r in test_data], dtype=np.float64)
        y_completed = np.array(
            [1.0 if r.get("completed", False) else 0.0 for r in test_data],
            dtype=np.float64,
        )
        y_days = np.array(
            [float(r.get("actual_days", 0) or 0) for r in test_data],
            dtype=np.float64,
        )

        X = self._scaler.transform(X_raw)
        if self._single_class is not None:
            y_pred_cls = np.full_like(y_completed, self._single_class)
        else:
            assert self._completion_model is not None
            y_pred_cls = self._completion_model.predict(X)
        y_pred_days = self._timeline_model.predict(X)

        return ModelMetrics(
            completion_accuracy=float(accuracy_score(y_completed, y_pred_cls)),
            completion_precision=float(
                precision_score(y_completed, y_pred_cls, zero_division=0.0)
            ),
            completion_recall=float(
                recall_score(y_completed, y_pred_cls, zero_division=0.0)
            ),
            completion_f1=float(f1_score(y_completed, y_pred_cls, zero_division=0.0)),
            timeline_mae=float(mean_absolute_error(y_days, y_pred_days)),
            timeline_rmse=float(math.sqrt(mean_squared_error(y_days, y_pred_days))),
            timeline_r2=float(r2_score(y_days, y_pred_days)) if len(y_days) > 1 else 0.0,
            training_samples=len(test_data),
        )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def is_trained(self) -> bool:
        return self._trained

    @property
    def training_samples(self) -> int:
        return self._training_samples

    def _top_risk_factors(self, top_n: int = 5) -> tuple[RiskFactor, ...]:
        """Return the top-N features most correlated with plan issues."""
        if self._risk_correlations is None:
            return ()
        indices = np.argsort(np.abs(self._risk_correlations))[::-1][:top_n]
        factors: list[RiskFactor] = []
        for idx in indices:
            corr = float(self._risk_correlations[idx])
            if corr == 0.0:
                continue
            factors.append(
                RiskFactor(
                    name=self._feature_names[idx],
                    correlation=round(corr, 4),
                    direction="positive" if corr > 0 else "negative",
                )
            )
        return tuple(factors)

    def _feature_importances(self) -> tuple[FeatureImportance, ...]:
        """Derive feature importances from the completion model coefficients."""
        if self._completion_model is None:
            return ()
        coefs = self._completion_model.coef_[0]
        abs_coefs = np.abs(coefs)
        total = float(abs_coefs.sum()) or 1.0
        importances: list[FeatureImportance] = []
        for idx in np.argsort(abs_coefs)[::-1]:
            importances.append(
                FeatureImportance(
                    name=self._feature_names[idx],
                    importance=round(float(abs_coefs[idx]) / total, 4),
                    description=(
                        f"{'Higher' if coefs[idx] > 0 else 'Lower'} "
                        f"{self._feature_names[idx]} increases completion probability"
                        if coefs[idx] > 0
                        else f"Higher {self._feature_names[idx]} decreases completion probability"
                    ),
                )
            )
        return tuple(importances)


__all__ = [
    "FeatureImportance",
    "ModelMetrics",
    "PredictionResult",
    "PredictiveAnalyzer",
    "RiskFactor",
]
