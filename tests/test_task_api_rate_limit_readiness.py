import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_rate_limit_readiness import (
    TaskApiRateLimitReadinessFinding,
    TaskApiRateLimitReadinessPlan,
    analyze_task_api_rate_limit_readiness,
    build_task_api_rate_limit_readiness_plan,
    extract_task_api_rate_limit_readiness,
    generate_task_api_rate_limit_readiness,
    recommend_task_api_rate_limit_readiness,
    summarize_task_api_rate_limit_readiness,
    task_api_rate_limit_readiness_plan_to_dict,
    task_api_rate_limit_readiness_plan_to_dicts,
)
from blueprint.task_email_deliverability_readiness import build_task_email_deliverability_readiness_plan


def test_ready_api_rate_limit_task_has_no_actionable_gaps():
    result = analyze_task_api_rate_limit_readiness(
        _plan(
            [
                _task(
                    "task-api-limits",
                    title="Add API rate limits for public endpoints",
                    description=(
                        "Implement API rate limits, quotas, throttling, burst limits, usage caps, and "
                        "client backoff behavior. Define limit dimensions per endpoint, method, plan tier, "
                        "per minute and daily quota windows. Store quota counters in Redis with atomic "
                        "increment, TTL counters, sliding window, and reset behavior. Enforce at API gateway "
                        "middleware before handlers. Return 429 Too Many Requests with RateLimit headers, "
                        "Retry-After, quota exceeded errors, remaining and reset headers. Document client "
                        "retry guidance with exponential backoff, jitter, and bounded retries. Scope limits "
                        "per tenant, per user, API key, workspace, and organization. Emit metrics, logs, "
                        "traces, alerts, dashboards, and audit logs. Support admin override, allowlist, "
                        "temporary increase, and support workflows. Add tests for windows, quota exhaustion, "
                        "429 headers, scoping, concurrency, overrides, and backoff."
                    ),
                    files_or_modules=["src/api/rate_limit/public_api_rate_limits.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskApiRateLimitReadinessPlan)
    assert result.plan_id == "plan-rate-limit"
    assert result.rate_limit_task_ids == ("task-api-limits",)
    finding = result.findings[0]
    assert isinstance(finding, TaskApiRateLimitReadinessFinding)
    assert finding.detected_signals == (
        "api_rate_limit",
        "quota",
        "throttle",
        "burst_limit",
        "usage_cap",
        "client_backoff",
    )
    assert finding.present_requirements == (
        "limit_dimensions",
        "quota_storage",
        "enforcement_point",
        "response_headers_errors",
        "retry_after_backoff_guidance",
        "tenant_user_scoping",
        "observability",
        "override_admin_handling",
        "test_coverage",
    )
    assert finding.missing_requirements == ()
    assert finding.actionable_gaps == ()
    assert finding.risk_level == "low"
    assert "files_or_modules: src/api/rate_limit/public_api_rate_limits.py" in finding.evidence
    assert result.summary["rate_limit_task_count"] == 1
    assert result.summary["missing_requirement_count"] == 0
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_partial_rate_limit_task_reports_specific_actionable_gaps():
    result = build_task_api_rate_limit_readiness_plan(
        _plan(
            [
                _task(
                    "task-quota",
                    title="Throttle API usage caps",
                    description=(
                        "Add API rate limiting for usage caps and burst limits. Enforce at middleware and "
                        "return 429 Too Many Requests with Retry-After headers."
                    ),
                    files_or_modules=["src/api/quota/usage_cap_throttle.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-quota"
    assert finding.detected_signals == (
        "api_rate_limit",
        "quota",
        "throttle",
        "burst_limit",
        "usage_cap",
        "client_backoff",
    )
    assert finding.present_requirements == ("enforcement_point", "response_headers_errors", "retry_after_backoff_guidance")
    assert finding.missing_requirements == (
        "limit_dimensions",
        "quota_storage",
        "tenant_user_scoping",
        "observability",
        "override_admin_handling",
        "test_coverage",
    )
    assert finding.risk_level == "high"
    assert finding.actionable_gaps == (
        "Define limit dimensions such as endpoint, method, plan tier, burst window, and per-second/minute/day quotas.",
        "Specify durable or distributed quota counter storage, atomic updates, expiration windows, and reset behavior.",
        "Clarify tenant, user, account, API key, workspace, or organization scoping and isolation rules.",
        "Add metrics, logs, traces, alerts, dashboards, and audit events for limit checks and denials.",
        "Plan admin overrides, allowlists, support workflows, temporary increases, and bypass auditability.",
        "Add tests for limit windows, quota exhaustion, headers/errors, scoping, concurrency, overrides, and backoff.",
    )
    assert result.summary["missing_requirement_counts"]["quota_storage"] == 1
    assert result.summary["present_requirement_counts"]["enforcement_point"] == 1


def test_path_hints_contribute_to_detection():
    result = build_task_api_rate_limit_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Wire API guard",
                    description="Add middleware metrics and tests.",
                    files_or_modules=[
                        "src/api/rate_limit/routes.py",
                        "src/billing/quota_store.py",
                        "src/traffic/throttle_rules.py",
                        "src/limits/usage_cap_policy.py",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert {"api_rate_limit", "quota", "throttle", "usage_cap"} <= set(finding.detected_signals)
    assert "files_or_modules: src/api/rate_limit/routes.py" in finding.evidence
    assert "files_or_modules: src/billing/quota_store.py" in finding.evidence
    assert "test_coverage" in finding.present_requirements


def test_unrelated_and_explicit_no_impact_tasks_are_not_applicable():
    result = build_task_api_rate_limit_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update API docs copy",
                    description="Adjust helper text for endpoint descriptions.",
                    files_or_modules=["src/api/docs.py"],
                ),
                _task(
                    "task-no-limits",
                    title="Refactor billing model",
                    description="No rate limiting, quota, throttling, usage cap, or backoff changes are required.",
                ),
            ]
        )
    )

    assert result.findings == ()
    assert result.records == ()
    assert result.rate_limit_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy", "task-no-limits")
    assert result.to_dicts() == []
    assert result.summary == {
        "total_task_count": 2,
        "rate_limit_task_count": 0,
        "not_applicable_task_ids": ["task-copy", "task-no-limits"],
        "missing_requirement_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "api_rate_limit": 0,
            "quota": 0,
            "throttle": 0,
            "burst_limit": 0,
            "usage_cap": 0,
            "client_backoff": 0,
        },
        "present_requirement_counts": {
            "limit_dimensions": 0,
            "quota_storage": 0,
            "enforcement_point": 0,
            "response_headers_errors": 0,
            "retry_after_backoff_guidance": 0,
            "tenant_user_scoping": 0,
            "observability": 0,
            "override_admin_handling": 0,
            "test_coverage": 0,
        },
        "missing_requirement_counts": {
            "limit_dimensions": 0,
            "quota_storage": 0,
            "enforcement_point": 0,
            "response_headers_errors": 0,
            "retry_after_backoff_guidance": 0,
            "tenant_user_scoping": 0,
            "observability": 0,
            "override_admin_handling": 0,
            "test_coverage": 0,
        },
    }


def test_model_object_aliases_serialization_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Rate limit telemetry",
                description="Add API rate limit metrics, logs, alerts, dashboards, and tests.",
            ),
            _task(
                "task-a",
                title="Quota storage enforcement",
                description=(
                    "API quota uses Redis token bucket quota counters with middleware enforcement, per user "
                    "and per tenant scoping, 429 headers, Retry-After, admin override, and integration tests."
                ),
                metadata={"limit_dimensions": "Per endpoint, method, plan tier, and daily quota windows."},
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_task_api_rate_limit_readiness(model)
    payload = task_api_rate_limit_readiness_plan_to_dict(result)
    task_result = build_task_api_rate_limit_readiness_plan(ExecutionTask.model_validate(plan["tasks"][1]))
    object_result = build_task_api_rate_limit_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="Throttle usage cap",
            description="Throttle API usage cap requests with middleware and 429 Retry-After headers.",
            files_or_modules=["src/api/throttle.py"],
        )
    )

    assert plan == original
    assert result.rate_limit_task_ids == ("task-z", "task-a")
    assert result.records == result.findings
    assert task_result.findings[0].task_id == "task-a"
    assert object_result.findings[0].task_id == "task-object"
    assert extract_task_api_rate_limit_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_api_rate_limit_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_api_rate_limit_readiness(plan).to_dict() == result.to_dict()
    assert task_api_rate_limit_readiness_plan_to_dicts(result) == payload["findings"]
    assert task_api_rate_limit_readiness_plan_to_dicts(result.findings) == payload["findings"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "findings",
        "rate_limit_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["findings"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_requirements",
        "missing_requirements",
        "risk_level",
        "evidence",
        "actionable_gaps",
    ]


def test_email_deliverability_analyzer_remains_independent():
    plan = _plan(
        [
            _task(
                "task-api-limit-only",
                title="API rate limit middleware",
                description="Add API rate limits with quota storage, 429 Retry-After headers, observability, and tests.",
                files_or_modules=["src/api/rate_limit.py"],
            )
        ]
    )

    rate_limits = build_task_api_rate_limit_readiness_plan(plan)
    email = build_task_email_deliverability_readiness_plan(plan)

    assert rate_limits.rate_limit_task_ids == ("task-api-limit-only",)
    assert email.email_task_ids == ()
    assert email.records == ()


def _plan(tasks, plan_id="plan-rate-limit"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-rate-limit",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
