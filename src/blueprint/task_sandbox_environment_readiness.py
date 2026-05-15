"""Assess readiness for sandbox environment provisioning and maintenance tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import (
    SimpleReadinessPlan,
    SimpleReadinessRecord,
    build_simple_readiness_plan,
)


TaskSandboxEnvironmentReadinessPlan = SimpleReadinessPlan
TaskSandboxEnvironmentReadinessRecord = SimpleReadinessRecord
TaskSandboxEnvironmentReadinessFinding = SimpleReadinessRecord
TaskSandboxEnvironmentReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "sandbox_environment": re.compile(
        r"\b(?:sandbox environment|sandbox provisioning|sandbox maintenance|developer sandbox|test sandbox|"
        r"customer sandbox|tenant sandbox|non[- ]production sandbox)\b",
        re.I,
    ),
    "sandbox_creation": re.compile(
        r"\b(?:create|creation|provision|provisioning|stand up|bootstrap|instantiate|new)\b.{0,60}\b(?:sandbox|test environment|tenant environment)\b|"
        r"\b(?:sandbox|test environment|tenant environment)\b.{0,60}\b(?:create|creation|provision|provisioning|bootstrap|instantiate|new)\b",
        re.I,
    ),
    "sandbox_reset": re.compile(
        r"\b(?:sandbox reset|reset sandbox|sandbox cleanup|sandbox clean up|cleanup sandbox|rebuild sandbox|refresh sandbox|"
        r"destroy sandbox|recreate sandbox)\b",
        re.I,
    ),
    "sandbox_seed": re.compile(
        r"\b(?:sandbox seed|seed sandbox|seed data|fixtures?|fixture data|test data|baseline data|sample data|"
        r"data seeding)\b",
        re.I,
    ),
    "sandbox_isolation": re.compile(
        r"\b(?:sandbox isolation|tenant isolation|data isolation|network isolation|environment isolation|isolated sandbox|"
        r"boundary|boundaries|dedicated tenant)\b",
        re.I,
    ),
    "sandbox_credentials": re.compile(
        r"\b(?:sandbox credentials?|test credentials?|non[- ]production credentials?|sandbox secrets?|api keys?|"
        r"credential handling|secret handling|env vars?|environment variables?)\b",
        re.I,
    ),
    "webhook_test_endpoints": re.compile(
        r"\b(?:webhook test endpoints?|test endpoints?|callback endpoints?|sandbox webhooks?|webhook sandbox|"
        r"test callback|ngrok|endpoint routing)\b",
        re.I,
    ),
    "tenant_sandbox": re.compile(
        r"\b(?:tenant sandbox|customer sandbox|per[- ]tenant sandbox|tenant-specific sandbox|tenant test environment)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "sandbox_environment": re.compile(r"(?:sandbox|sandboxes|test[_\s-]?environment|non[_\s-]?prod)", re.I),
    "sandbox_creation": re.compile(r"(?:sandbox|environment).*(?:provision|create|bootstrap|template)", re.I),
    "sandbox_reset": re.compile(r"(?:sandbox|environment).*(?:reset|cleanup|clean[_\s-]?up|rebuild|refresh)", re.I),
    "sandbox_seed": re.compile(r"(?:sandbox|environment|test).*(?:seed|fixture|sample[_\s-]?data|test[_\s-]?data)", re.I),
    "sandbox_isolation": re.compile(r"(?:sandbox|tenant|environment).*(?:isolation|boundary|network|policy)", re.I),
    "sandbox_credentials": re.compile(r"(?:sandbox|test|environment).*(?:credentials?|secrets?|api[_\s-]?keys?|env[_\s-]?vars?)", re.I),
    "webhook_test_endpoints": re.compile(r"(?:webhook|callback|endpoint).*(?:sandbox|test|non[_\s-]?prod)", re.I),
    "tenant_sandbox": re.compile(r"(?:tenant|customer).*(?:sandbox|test[_\s-]?environment)", re.I),
}
_CRITERIA = {
    "environment_boundaries": re.compile(
        r"\b(?:environment boundaries|sandbox boundaries|boundary|boundaries|network boundary|allowed services|"
        r"production separation|prod separation|non[- ]production only|egress policy|ingress policy)\b",
        re.I,
    ),
    "data_isolation": re.compile(
        r"\b(?:data isolation|tenant isolation|isolated data|separate database|dedicated schema|tenant boundary|"
        r"no production data|masked data|synthetic data|scrubbed data|pii removed)\b",
        re.I,
    ),
    "seed_fixture_strategy": re.compile(
        r"\b(?:seed strategy|fixture strategy|seed data|fixtures?|fixture data|test data|baseline data|sample data|"
        r"data seeding|known dataset)\b",
        re.I,
    ),
    "credential_handling": re.compile(
        r"\b(?:credential handling|credentials?|sandbox secrets?|test secrets?|secret handling|api keys?|env vars?|"
        r"environment variables?|vault|secrets manager|rotated secrets?)\b",
        re.I,
    ),
    "reset_cleanup_behavior": re.compile(
        r"\b(?:reset behavior|cleanup behavior|reset|cleanup|clean up|teardown|rebuild|refresh|destroy|recreate|"
        r"ttl|expiration|scheduled reset)\b",
        re.I,
    ),
    "owner": re.compile(
        r"\b(?:owner|owned by|dri|responsible team|maintainer|platform team|developer experience team|"
        r"environment owner|approver|accountable)\b",
        re.I,
    ),
    "validation_checks": re.compile(
        r"\b(?:validation checks?|validation|validate|verify|smoke tests?|health checks?|provisioning tests?|"
        r"integration tests?|contract tests?|pytest|post[- ]provision checks?|post[- ]reset checks?)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "environment_boundaries": "Define environment boundaries, including production separation, allowed services, network boundaries, ingress, egress, or non-production constraints.",
    "data_isolation": "Document data isolation with tenant isolation, separate databases or schemas, masked data, synthetic data, scrubbed data, or no production data.",
    "seed_fixture_strategy": "Add a seed or fixture strategy using seed data, fixtures, baseline data, sample data, known datasets, or test data.",
    "credential_handling": "Describe credential handling for sandbox secrets, test credentials, API keys, env vars, a vault, secrets manager, or rotated secrets.",
    "reset_cleanup_behavior": "Define reset or cleanup behavior, including teardown, rebuild, refresh, destroy, recreate, TTL, expiration, or scheduled resets.",
    "owner": "Name the owner, DRI, responsible team, maintainer, platform team, developer experience team, environment owner, approver, or accountable party.",
    "validation_checks": "Add validation checks such as smoke tests, health checks, provisioning tests, integration tests, contract tests, pytest, or post-reset checks.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:sandbox|test environment|tenant sandbox|fixture|seed data)\b"
    r".{0,80}\b(?:required|needed|planned|scope|impact|changes?)\b",
    re.I,
)


def build_task_sandbox_environment_readiness_plan(source: Any) -> TaskSandboxEnvironmentReadinessPlan:
    """Build sandbox environment readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Sandbox Environment Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_sandbox_environment_readiness = build_task_sandbox_environment_readiness_plan
extract_task_sandbox_environment_readiness = build_task_sandbox_environment_readiness_plan
generate_task_sandbox_environment_readiness = build_task_sandbox_environment_readiness_plan
derive_task_sandbox_environment_readiness = build_task_sandbox_environment_readiness_plan
summarize_task_sandbox_environment_readiness = build_task_sandbox_environment_readiness_plan
summarize_task_sandbox_environment_readiness_plan = build_task_sandbox_environment_readiness_plan


def recommend_task_sandbox_environment_readiness(source: Any) -> tuple[TaskSandboxEnvironmentReadinessRecord, ...]:
    return build_task_sandbox_environment_readiness_plan(source).records


def task_sandbox_environment_readiness_plan_to_dict(result: TaskSandboxEnvironmentReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_sandbox_environment_readiness_plan_to_dict.__test__ = False


def task_sandbox_environment_readiness_plan_to_dicts(
    result: TaskSandboxEnvironmentReadinessPlan | Iterable[TaskSandboxEnvironmentReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_sandbox_environment_readiness_plan_to_dicts.__test__ = False
task_sandbox_environment_readiness_to_dicts = task_sandbox_environment_readiness_plan_to_dicts
task_sandbox_environment_readiness_to_dicts.__test__ = False


def task_sandbox_environment_readiness_plan_to_markdown(result: TaskSandboxEnvironmentReadinessPlan) -> str:
    return result.to_markdown()


task_sandbox_environment_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskSandboxEnvironmentReadinessFinding",
    "TaskSandboxEnvironmentReadinessPlan",
    "TaskSandboxEnvironmentReadinessRecord",
    "TaskSandboxEnvironmentReadinessRecommendation",
    "analyze_task_sandbox_environment_readiness",
    "build_task_sandbox_environment_readiness_plan",
    "derive_task_sandbox_environment_readiness",
    "extract_task_sandbox_environment_readiness",
    "generate_task_sandbox_environment_readiness",
    "recommend_task_sandbox_environment_readiness",
    "summarize_task_sandbox_environment_readiness",
    "summarize_task_sandbox_environment_readiness_plan",
    "task_sandbox_environment_readiness_plan_to_dict",
    "task_sandbox_environment_readiness_plan_to_dicts",
    "task_sandbox_environment_readiness_plan_to_markdown",
    "task_sandbox_environment_readiness_to_dicts",
]
