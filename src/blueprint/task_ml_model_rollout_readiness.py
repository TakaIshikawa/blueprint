"""Assess readiness for ML model rollout and inference deployment tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskMLModelRolloutReadinessFinding = SimpleReadinessRecord
TaskMLModelRolloutReadinessPlan = SimpleReadinessPlan

_SIGNALS = {
    "model_rollout": re.compile(r"\b(?:model rollout|roll out model|ml rollout|model launch|release model)\b", re.I),
    "model_serving": re.compile(r"\b(?:model serving|serving model|inference service|prediction service|model endpoint)\b", re.I),
    "inference_deployment": re.compile(r"\b(?:inference deployment|deploy inference|deploy model|model deployment|inference rollout)\b", re.I),
    "champion_challenger": re.compile(r"\b(?:champion.?challenger|challenger model|champion model|A/B model|ab model)\b", re.I),
    "model_version": re.compile(r"\b(?:model version|versioned model|model artifact|model registry|registered model)\b", re.I),
    "model_monitoring": re.compile(r"\b(?:model monitoring|drift monitoring|prediction monitoring|inference monitoring|model metrics)\b", re.I),
    "model_rollback": re.compile(r"\b(?:model rollback|rollback model|revert model|previous model|disable model)\b", re.I),
}
_PATH_SIGNALS = {
    "model_rollout": re.compile(r"model[-_]?rollout|rollouts?", re.I),
    "model_serving": re.compile(r"serving|inference|predictions?|endpoints?", re.I),
    "inference_deployment": re.compile(r"deploy|deployment|inference", re.I),
    "champion_challenger": re.compile(r"champion|challenger|ab[-_]?model", re.I),
    "model_version": re.compile(r"model[-_]?registry|versions?|artifacts?", re.I),
    "model_monitoring": re.compile(r"monitoring|drift|metrics", re.I),
    "model_rollback": re.compile(r"rollback|revert", re.I),
}
_CRITERIA = {
    "model_versioning": re.compile(r"\b(?:model version|versioned model|model registry|artifact version|registered model|model lineage)\b", re.I),
    "evaluation_gates": re.compile(r"\b(?:evaluation gate|eval gate|offline evaluation|quality gate|acceptance metric|holdout|precision|recall|auc|accuracy threshold)\b", re.I),
    "shadow_canary_plan": re.compile(r"\b(?:shadow|canary|champion.?challenger|traffic split|gradual rollout|percentage rollout|A/B test|ab test)\b", re.I),
    "monitoring": re.compile(r"\b(?:monitoring|monitor|drift|metrics|alerts?|dashboard|latency|error rate|prediction quality)\b", re.I),
    "rollback_owner": re.compile(r"\b(?:rollback owner|owner for rollback|rollback approver|on[- ]call owner|model owner|release owner|revert owner)\b", re.I),
}
_GUIDANCE = {
    "model_versioning": "Identify the model version, registry artifact, and lineage for rollout.",
    "evaluation_gates": "Define evaluation gates and acceptance metrics before deployment.",
    "shadow_canary_plan": "Plan shadow, canary, champion/challenger, or staged traffic rollout.",
    "monitoring": "Add monitoring for drift, prediction quality, latency, errors, and service health.",
    "rollback_owner": "Name the rollback owner and decision path for reverting the model.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:model rollout|model deployment|inference deployment|model serving)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_ml_model_rollout_readiness_plan(source: Any) -> TaskMLModelRolloutReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task ML Model Rollout Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_ml_model_rollout_readiness(source: Any) -> TaskMLModelRolloutReadinessPlan:
    return build_task_ml_model_rollout_readiness_plan(source)


def extract_task_ml_model_rollout_readiness(source: Any) -> TaskMLModelRolloutReadinessPlan:
    return build_task_ml_model_rollout_readiness_plan(source)


def generate_task_ml_model_rollout_readiness(source: Any) -> TaskMLModelRolloutReadinessPlan:
    return build_task_ml_model_rollout_readiness_plan(source)


def derive_task_ml_model_rollout_readiness(source: Any) -> TaskMLModelRolloutReadinessPlan:
    return build_task_ml_model_rollout_readiness_plan(source)


def summarize_task_ml_model_rollout_readiness(source: Any) -> TaskMLModelRolloutReadinessPlan:
    return build_task_ml_model_rollout_readiness_plan(source)


def recommend_task_ml_model_rollout_readiness(source: Any) -> TaskMLModelRolloutReadinessPlan:
    return build_task_ml_model_rollout_readiness_plan(source)


def task_ml_model_rollout_readiness_plan_to_dict(report: TaskMLModelRolloutReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_ml_model_rollout_readiness_plan_to_dict.__test__ = False


def task_ml_model_rollout_readiness_plan_to_dicts(report: TaskMLModelRolloutReadinessPlan) -> list[dict[str, Any]]:
    return report.to_dicts()


task_ml_model_rollout_readiness_plan_to_dicts.__test__ = False


def task_ml_model_rollout_readiness_plan_to_markdown(report: TaskMLModelRolloutReadinessPlan) -> str:
    return report.to_markdown()


task_ml_model_rollout_readiness_plan_to_markdown.__test__ = False
