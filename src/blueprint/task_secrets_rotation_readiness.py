"""Assess readiness for tasks that rotate secrets or credentials."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskSecretsRotationReadinessPlan = SimpleReadinessPlan
TaskSecretsRotationReadinessRecord = SimpleReadinessRecord
TaskSecretsRotationReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "secret_rotation": re.compile(r"\b(?:secret|credential|password|api key|client secret|token|certificate|cert)\b.{0,80}\b(?:rotate|rotation|roll|renew|replace)\b|\b(?:rotate|rotation|renew|replace)\b.{0,80}\b(?:secret|credential|password|api key|client secret|token|certificate|cert)\b", re.I),
    "credential_migration": re.compile(r"\b(?:credential migration|key rollover|token rollover|secret rollover|vault rotation)\b", re.I),
}
_PATH_SIGNALS = {
    "secret_rotation": re.compile(r"(?:secret|credential|api key|client secret|token|cert|vault|rotation)", re.I),
}
_CRITERIA = {
    "inventory": re.compile(r"\b(?:inventory|catalog|list|enumerate|map)\b.{0,80}\b(?:secret|credential|key|token|consumer|dependency)\b", re.I),
    "staged_rollout": re.compile(r"\b(?:dual[- ]?(?:read|write)|overlap window|old and new|staged rollout|canary|phased|parallel)\b", re.I),
    "rollback": re.compile(r"\b(?:rollback|roll back|restore previous|revert|keep previous|fallback)\b", re.I),
    "owner_coordination": re.compile(r"\b(?:owner|on[- ]call|coordinate|coordination|notify|stakeholder|consumer team|service owner)\b", re.I),
    "validation": re.compile(r"\b(?:validate|validation|smoke test|post[- ]rotation|verify|verification|integration test)\b", re.I),
    "monitoring": re.compile(r"\b(?:monitor|alert|dashboard|telemetry|metrics|log)\b", re.I),
}
_GUIDANCE = {
    "inventory": "Add an inventory of affected secrets, consumers, owners, and dependencies.",
    "staged_rollout": "Add a staged rollout or dual-read/write overlap window for old and new credentials.",
    "rollback": "Add rollback or previous-secret retention instructions.",
    "owner_coordination": "Name owners and coordination steps for affected teams or vendors.",
    "validation": "Add post-rotation validation checks.",
    "monitoring": "Add monitoring or alerting for rotation failures.",
}
_NO_IMPACT = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:secret|credential|token|api key)\b.{0,80}\b(?:rotation|changes?|impact|required|needed)\b", re.I)


def build_task_secrets_rotation_readiness_plan(source: Any) -> TaskSecretsRotationReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task Secrets Rotation Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_secrets_rotation_readiness(source: Any) -> TaskSecretsRotationReadinessPlan:
    return build_task_secrets_rotation_readiness_plan(source)


def extract_task_secrets_rotation_readiness(source: Any) -> TaskSecretsRotationReadinessPlan:
    return build_task_secrets_rotation_readiness_plan(source)


def generate_task_secrets_rotation_readiness(source: Any) -> TaskSecretsRotationReadinessPlan:
    return build_task_secrets_rotation_readiness_plan(source)


def derive_task_secrets_rotation_readiness(source: Any) -> TaskSecretsRotationReadinessPlan:
    return build_task_secrets_rotation_readiness_plan(source)


def summarize_task_secrets_rotation_readiness(source: Any) -> TaskSecretsRotationReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_task_secrets_rotation_readiness_plan(source)


def recommend_task_secrets_rotation_readiness(source: Any) -> tuple[TaskSecretsRotationReadinessRecord, ...]:
    return build_task_secrets_rotation_readiness_plan(source).records


def task_secrets_rotation_readiness_plan_to_dict(result: TaskSecretsRotationReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_secrets_rotation_readiness_plan_to_dict.__test__ = False


def task_secrets_rotation_readiness_plan_to_dicts(
    result: TaskSecretsRotationReadinessPlan | Iterable[TaskSecretsRotationReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_secrets_rotation_readiness_plan_to_dicts.__test__ = False
task_secrets_rotation_readiness_to_dicts = task_secrets_rotation_readiness_plan_to_dicts
task_secrets_rotation_readiness_to_dicts.__test__ = False


def task_secrets_rotation_readiness_plan_to_markdown(result: TaskSecretsRotationReadinessPlan) -> str:
    return result.to_markdown()


task_secrets_rotation_readiness_plan_to_markdown.__test__ = False

