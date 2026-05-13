import json

from blueprint.task_blue_green_deployment_readiness import (
    TaskBlueGreenDeploymentReadinessPlan,
    TaskBlueGreenDeploymentReadinessRecord,
    analyze_task_blue_green_deployment_readiness,
    build_task_blue_green_deployment_readiness_plan,
    extract_task_blue_green_deployment_readiness,
    generate_task_blue_green_deployment_readiness,
    plan_task_blue_green_deployment_readiness,
    recommend_task_blue_green_deployment_readiness,
    task_blue_green_deployment_readiness_plan_to_dict,
    task_blue_green_deployment_readiness_plan_to_dicts,
    task_blue_green_deployment_readiness_plan_to_markdown,
    task_blue_green_deployment_readiness_to_dict,
)


def test_ready_blue_green_deployment_plan_has_all_criteria():
    result = build_task_blue_green_deployment_readiness_plan(
        _plan(
            [
                _task(
                    "bg-ready",
                    "Prepare blue-green deployment cutover",
                    (
                        "Provision equivalent blue and green environments for the API stack. "
                        "Use weighted load balancer routing to switch 10 percent then 100 percent traffic. "
                        "Readiness and liveness health checks gate the cutover. "
                        "Database schema compatibility uses backward-compatible migrations and dual-write checks. "
                        "Rollback trigger is error rate above 2 percent and steps switch traffic back. "
                        "Deployment observability includes latency dashboard, metrics, logs, traces, and alerts. "
                        "Release manager owner and SRE on-call approve the runbook handoff. "
                        "Smoke validation evidence and QA sign-off must be attached before completion."
                    ),
                    files_or_modules=["deploy/blue_green_cutover.yaml"],
                )
            ]
        )
    )

    record = result.records[0]

    assert isinstance(result, TaskBlueGreenDeploymentReadinessPlan)
    assert isinstance(record, TaskBlueGreenDeploymentReadinessRecord)
    assert record.readiness == "ready"
    assert record.present_criteria == (
        "parallel_environment_provisioning",
        "traffic_switch_strategy",
        "health_checks",
        "data_compatibility",
        "rollback_trigger",
        "observability",
        "ownership",
        "validation_evidence",
    )
    assert record.missing_criteria == ()
    assert record.recommended_follow_up_actions == ()
    assert result.impacted_task_ids == ("bg-ready",)
    assert result.summary["readiness_counts"]["ready"] == 1


def test_partial_blue_green_deployment_plan_reports_deterministic_gaps():
    result = analyze_task_blue_green_deployment_readiness(
        _plan(
            [
                _task(
                    "bg-partial",
                    "Add blue-green traffic switch",
                    (
                        "Use service mesh weighted routing to switch traffic between blue and green. "
                        "Readiness health checks must pass before cutover. "
                        "Deployment dashboard tracks latency and error rate."
                    ),
                )
            ]
        )
    )

    record = result.records[0]

    assert record.readiness == "partial"
    assert record.present_criteria == (
        "traffic_switch_strategy",
        "health_checks",
        "observability",
    )
    assert record.missing_criteria == (
        "parallel_environment_provisioning",
        "data_compatibility",
        "rollback_trigger",
        "ownership",
        "validation_evidence",
    )
    assert record.recommended_follow_up_actions == (
        "Describe how the parallel blue and green environments, stacks, or clusters will be provisioned and kept equivalent.",
        "Document data and schema compatibility, including backward-compatible migrations, dual writes, or version constraints.",
        "Define rollback triggers, thresholds, ownership, and exact steps to switch traffic back.",
        "Name the deployment owner, approver, on-call role, or operating team responsible for the cutover.",
        "Capture validation evidence such as smoke test results, QA sign-off, synthetic checks, or release artifacts.",
    )
    assert any("service mesh weighted routing" in item for item in record.evidence)


def test_absent_blue_green_deployment_plan_is_ignored_and_serializes():
    plan = _plan(
        [
            _task("bg-no-impact", "Update copy", "No blue-green deployment changes are in scope."),
            _task("docs", "Edit docs", "Adjust onboarding text."),
        ]
    )

    result = recommend_task_blue_green_deployment_readiness(plan)
    payload = task_blue_green_deployment_readiness_plan_to_dict(result)
    markdown = task_blue_green_deployment_readiness_plan_to_markdown(result)

    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.ignored_task_ids == ("bg-no-impact", "docs")
    assert result.summary["impacted_task_count"] == 0
    assert json.loads(json.dumps(payload)) == payload
    assert task_blue_green_deployment_readiness_plan_to_dicts(result) == []
    assert task_blue_green_deployment_readiness_to_dict(result) == payload
    assert plan_task_blue_green_deployment_readiness(plan).to_dict() == result.to_dict()
    assert extract_task_blue_green_deployment_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_blue_green_deployment_readiness(plan).to_dict() == result.to_dict()
    assert markdown.startswith("# Task Blue Green Deployment Readiness")
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "impacted_task_ids",
        "ignored_task_ids",
        "summary",
    ]


def _plan(tasks):
    return {"id": "plan-blue-green", "tasks": tasks}


def _task(task_id, title, description, **extra):
    return {"id": task_id, "title": title, "description": description, **extra}
