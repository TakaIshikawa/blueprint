import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_api_security_headers_readiness import (
    TaskAPISecurityHeadersReadinessPlan,
    analyze_task_api_security_headers_readiness,
    build_task_api_security_headers_readiness_plan,
    recommend_task_api_security_headers_readiness,
    summarize_task_api_security_headers_readiness,
    summarize_task_api_security_headers_readiness_plan,
    task_api_security_headers_readiness_plan_to_dict,
    task_api_security_headers_readiness_plan_to_dicts,
    task_api_security_headers_readiness_plan_to_markdown,
)


def test_complete_api_security_headers_task_is_ready():
    result = build_task_api_security_headers_readiness_plan(
        _plan(
            [
                _task(
                    "headers-ready",
                    title="Add API security headers",
                    description="Implement API security headers for every response.",
                    acceptance_criteria=[
                        "Explicit header set includes HSTS, CSP, X-Frame-Options, X-Content-Type-Options, Referrer-Policy, and Permissions-Policy.",
                        "Environment rollout starts in staging, then canary, then production behind a feature flag.",
                        "Compatibility testing covers browsers, API consumers, legacy clients, and mobile clients.",
                        "Regression tests include pytest integration checks and header assertions.",
                        "Security platform owner is the DRI and approver.",
                        "Monitoring alerts track CSP violation reports with rollback and runbook guidance.",
                    ],
                    files_or_modules=["src/api/security_headers.py"],
                ),
                _task("copy", title="Update copy", description="Refresh onboarding text."),
            ]
        )
    )

    assert isinstance(result, TaskAPISecurityHeadersReadinessPlan)
    assert result.impacted_task_ids == ("headers-ready",)
    assert result.ignored_task_ids == ("copy",)
    record = result.records[0]
    assert record.detected_signals == ("security_headers", "hsts_csp", "browser_hardening_headers")
    assert record.present_criteria == (
        "explicit_header_set",
        "environment_rollout",
        "compatibility_testing",
        "regression_coverage",
        "owner",
        "monitoring_rollback_guidance",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_partial_api_security_headers_task_reports_deterministic_actionable_gaps():
    result = analyze_task_api_security_headers_readiness(
        [_task("headers-partial", title="Harden response headers", description="Add HSTS and CSP to API responses.")]
    )

    record = result.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("explicit_header_set",)
    assert record.missing_criteria == (
        "environment_rollout",
        "compatibility_testing",
        "regression_coverage",
        "owner",
        "monitoring_rollback_guidance",
    )
    assert record.recommended_follow_up_actions == (
        "Document rollout by environment with staging, canary, feature flag, or production rollout controls.",
        "Add compatibility testing for browsers, API consumers, legacy clients, mobile clients, or integrations.",
        "Add regression coverage with unit, integration, contract, pytest, or header assertion tests.",
        "Name the owner, DRI, responsible team, maintainer, security team, platform team, or approver.",
        "Add monitoring, alerting, observability, rollback, disable, revert, or runbook guidance.",
    )


def test_path_hints_and_nested_metadata_contribute_evidence_without_mutation():
    source = _plan(
        [
            _task(
                "headers-paths",
                title="Response middleware",
                description="Add secure response headers.",
                files_or_modules=["middleware/csp.py", "api/cors_response_hardening.py", "config/referrer_policy.yml"],
                metadata={
                    "rollout": {
                        "owner": "Security team owner approves rollout.",
                        "plan": "Stage in sandbox and production canary with compatibility testing.",
                    }
                },
            )
        ]
    )
    original = copy.deepcopy(source)

    result = build_task_api_security_headers_readiness_plan(ExecutionPlan.model_validate(source))

    assert source == original
    record = result.records[0]
    assert record.detected_signals == ("security_headers", "hsts_csp", "browser_hardening_headers", "cors_response_hardening")
    assert record.present_criteria == ("explicit_header_set", "environment_rollout", "compatibility_testing", "owner")
    assert record.missing_criteria == ("regression_coverage", "monitoring_rollback_guidance")
    assert any("metadata.rollout.owner" in item for item in record.evidence)
    assert any("files_or_modules: middleware/csp.py" in item for item in record.evidence)
    assert any("files_or_modules: api/cors_response_hardening.py" in item for item in record.evidence)
    assert any("files_or_modules[2]: config/referrer_policy.yml" in item for item in record.evidence)


def test_no_impact_and_conversion_helpers_are_stable():
    result = summarize_task_api_security_headers_readiness(
        _plan(
            [
                _task(
                    "headers-noop",
                    title="Docs refresh",
                    description="No security headers or HSTS changes are required for this documentation update.",
                ),
                _task("headers-partial", title="Add Permissions-Policy", description="Set Permissions-Policy header."),
            ],
            plan_id="plan-security-headers-sort",
        )
    )

    payload = task_api_security_headers_readiness_plan_to_dict(result)
    markdown = task_api_security_headers_readiness_plan_to_markdown(result)

    assert [record.task_id for record in result.records] == ["headers-partial"]
    assert result.ignored_task_ids == ("headers-noop",)
    assert analyze_task_api_security_headers_readiness(result) is result
    assert summarize_task_api_security_headers_readiness_plan(result) is result
    assert recommend_task_api_security_headers_readiness(result) == result.records
    assert task_api_security_headers_readiness_plan_to_dicts(result) == payload["records"]
    assert task_api_security_headers_readiness_plan_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-security-headers-sort"
    assert markdown.startswith("# Task API Security Headers Readiness: plan-security-headers-sort")
    assert "| Task | Title | Readiness |" in markdown


def test_invalid_inputs_return_empty_simple_plan():
    assert build_task_api_security_headers_readiness_plan(42).records == ()
    assert build_task_api_security_headers_readiness_plan({"id": "bad", "tasks": "not a list"}).records == ()
    assert build_task_api_security_headers_readiness_plan({"tasks": []}).summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-security-headers"):
    return {"id": plan_id, "implementation_brief_id": "brief-security-headers", "milestones": [], "tasks": tasks}


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
