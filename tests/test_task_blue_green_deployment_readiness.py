import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_blue_green_deployment_readiness import (
    TaskBlueGreenDeploymentReadinessFinding,
    TaskBlueGreenDeploymentReadinessPlan,
    plan_task_blue_green_deployment_readiness,
    task_blue_green_deployment_readiness_to_dict,
)


def test_strong_blue_green_deployment_task_has_all_safeguards():
    result = plan_task_blue_green_deployment_readiness(
        _plan(
            [
                _task(
                    "task-bg-deploy",
                    title="Implement blue/green deployment for API service",
                    prompt=(
                        "Configure load balancer for traffic switching with percentage-based rollout. "
                        "Implement health check endpoints with readiness and liveness probes. "
                        "Ensure database schema compatibility with backward-compatible migrations and dual-write support. "
                        "Document rollback procedure with automation steps and trigger conditions. "
                        "Set up deployment monitoring with error rate metrics, latency tracking, and alert dashboards. "
                        "Execute smoke tests covering critical paths and integration points before traffic switch. "
                        "Create operator runbook with manual approval gates and on-call responsibilities. "
                        "Add comprehensive validation for all deployment stages."
                    ),
                    outputs=["config/loadbalancer.yaml", "ops/runbook.md", "tests/smoke_tests.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskBlueGreenDeploymentReadinessPlan)
    assert result.plan_id == "plan-bg-deploy"
    assert result.blue_green_deployment_task_ids == ("task-bg-deploy",)
    finding = result.findings[0]
    assert isinstance(finding, TaskBlueGreenDeploymentReadinessFinding)
    assert finding.detected_signals == (
        "traffic_switching",
        "health_checks",
        "database_compatibility",
        "rollback_trigger",
        "monitoring",
        "smoke_validation",
        "operator_ownership",
    )
    assert finding.present_safeguards == (
        "traffic_switch_mechanism",
        "health_check_validation",
        "database_schema_compatibility",
        "rollback_procedure",
        "deployment_monitoring",
        "smoke_test_execution",
        "operator_handoff",
    )
    assert finding.missing_safeguards == ()
    assert finding.actionable_remediations == ()
    assert finding.readiness == "strong"
    assert any("Configure load balancer for traffic switching" in ev for ev in finding.evidence)
    assert result.summary["blue_green_deployment_task_count"] == 1
    assert result.summary["overall_readiness"] == "strong"


def test_partial_blue_green_deployment_task_reports_missing_safeguards():
    result = plan_task_blue_green_deployment_readiness(
        _plan(
            [
                _task(
                    "task-basic-deploy",
                    title="Add traffic switching for blue/green deployment",
                    prompt=(
                        "Configure load balancer for traffic routing between blue and green environments. "
                        "Add health check validation before switching traffic. "
                        "Set up deployment monitoring with basic metrics."
                    ),
                    outputs=["config/lb_config.yaml"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-basic-deploy"
    assert finding.detected_signals == ("traffic_switching", "health_checks", "monitoring")
    assert "traffic_switch_mechanism" in finding.present_safeguards
    assert "health_check_validation" in finding.present_safeguards
    assert "deployment_monitoring" in finding.present_safeguards
    assert "database_schema_compatibility" in finding.missing_safeguards
    assert "rollback_procedure" in finding.missing_safeguards
    assert "smoke_test_execution" in finding.missing_safeguards
    assert "operator_handoff" in finding.missing_safeguards
    assert finding.readiness == "weak"
    assert len(finding.actionable_remediations) == 3
    assert any("database" in remediation.lower() for remediation in finding.actionable_remediations)


def test_path_hints_contribute_to_blue_green_deployment_detection():
    result = plan_task_blue_green_deployment_readiness(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Configure blue/green deployment infrastructure",
                    prompt="Set up traffic routing with rollback procedure and monitoring dashboards.",
                    outputs=[
                        "config/traffic_router.yaml",
                        "ops/rollback_script.sh",
                        "monitoring/deployment_dashboard.json",
                        "tests/smoke_validation.py",
                        "database/migration_schema.sql",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert {"traffic_switching", "rollback_trigger", "monitoring", "smoke_validation", "database_compatibility"} <= set(finding.detected_signals)
    assert "traffic_switch_mechanism" in finding.present_safeguards
    assert "rollback_procedure" in finding.present_safeguards
    assert "deployment_monitoring" in finding.present_safeguards


def test_unrelated_and_explicit_no_impact_tasks_are_not_applicable():
    result = plan_task_blue_green_deployment_readiness(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update API docs",
                    prompt="Improve API endpoint documentation.",
                    outputs=["docs/api.md"],
                ),
                _task(
                    "task-no-deploy",
                    title="Add feature flag",
                    prompt="This task has no deployment requirements and no blue/green rollout is involved.",
                    outputs=["src/features/flag.py"],
                ),
            ]
        )
    )

    assert result.blue_green_deployment_task_ids == ()
    assert result.not_applicable_task_ids == ("task-docs", "task-no-deploy")
    assert result.findings == ()
    assert result.summary["blue_green_deployment_task_count"] == 0
    assert result.summary["not_applicable_task_count"] == 2


def test_rollback_and_smoke_validation_signals():
    result = plan_task_blue_green_deployment_readiness(
        _plan(
            [
                _task(
                    "task-rollback-smoke",
                    title="Implement rollback procedure and smoke tests",
                    prompt=(
                        "Document rollback plan with trigger conditions and automation steps for reverting to blue environment. "
                        "Execute smoke tests on green environment covering critical paths before traffic switch."
                    ),
                    outputs=["ops/rollback_guide.md", "tests/smoke_suite.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "rollback_trigger" in finding.detected_signals
    assert "smoke_validation" in finding.detected_signals
    assert "rollback_procedure" in finding.present_safeguards
    assert "smoke_test_execution" in finding.present_safeguards


def test_health_checks_and_monitoring_signals():
    result = plan_task_blue_green_deployment_readiness(
        _plan(
            [
                _task(
                    "task-health-monitor",
                    title="Add health checks and deployment monitoring",
                    prompt=(
                        "Implement health check endpoints with readiness and liveness probes for green environment. "
                        "Set up deployment monitoring with error rate, latency metrics, and alert dashboards for real-time tracking."
                    ),
                    outputs=["src/health/probes.py", "monitoring/alerts.yaml"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "health_checks" in finding.detected_signals
    assert "monitoring" in finding.detected_signals
    assert "health_check_validation" in finding.present_safeguards
    assert "deployment_monitoring" in finding.present_safeguards


def test_database_compatibility_and_operator_ownership_signals():
    result = plan_task_blue_green_deployment_readiness(
        _plan(
            [
                _task(
                    "task-db-ops",
                    title="Ensure database compatibility and operator handoff",
                    prompt=(
                        "Ensure database schema compatibility with backward-compatible migrations and dual-write strategy. "
                        "Create operator runbook with manual approval gates and on-call responsibilities for deployment."
                    ),
                    outputs=["database/migrations/", "ops/deployment_runbook.md"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "database_compatibility" in finding.detected_signals
    assert "operator_ownership" in finding.detected_signals
    assert "database_schema_compatibility" in finding.present_safeguards
    assert "operator_handoff" in finding.present_safeguards


def test_serialization_and_compatibility_views():
    plan = _plan(
        [
            _task(
                "task-deploy-001",
                title="Implement blue/green deployment",
                prompt="Configure traffic switching with health check validation and rollback procedure.",
                outputs=["config/deploy.yaml"],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = plan_task_blue_green_deployment_readiness(plan)
    serialized = task_blue_green_deployment_readiness_to_dict(result)

    assert plan == original
    assert result.records == result.findings
    assert serialized["blue_green_deployment_task_ids"] == ["task-deploy-001"]
    assert "findings" in serialized
    assert "summary" in serialized
    assert list(serialized) == ["plan_id", "findings", "blue_green_deployment_task_ids", "not_applicable_task_ids", "summary", "records"]
    assert json.loads(json.dumps(serialized)) == serialized
    finding = result.findings[0]
    assert finding.actionable_gaps == finding.actionable_remediations


def test_multiple_tasks_with_different_readiness_levels():
    result = plan_task_blue_green_deployment_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Comprehensive blue/green deployment",
                    prompt=(
                        "Configure traffic switching, health check validation, database schema compatibility, "
                        "rollback procedure, deployment monitoring, smoke test execution, and operator handoff with runbook."
                    ),
                    outputs=["config/full_deploy.yaml"],
                ),
                _task(
                    "task-partial",
                    title="Basic deployment configuration",
                    prompt="Add traffic switching with health check validation.",
                    outputs=["config/basic_deploy.yaml"],
                ),
                _task(
                    "task-weak",
                    title="Traffic routing",
                    prompt="Configure load balancer for traffic routing.",
                    outputs=["config/lb.yaml"],
                ),
            ]
        )
    )

    assert len(result.findings) == 3
    assert result.blue_green_deployment_task_ids == ("task-strong", "task-partial", "task-weak")
    assert result.summary["readiness_distribution"]["strong"] == 1
    assert result.summary["readiness_distribution"]["weak"] == 2
    assert result.summary["overall_readiness"] in {"weak", "partial"}


def test_object_and_dict_inputs_are_handled():
    plan_dict = {
        "id": "plan-dict",
        "tasks": [
            {
                "task_id": "task-001",
                "title": "Blue/green deployment",
                "prompt": "Configure traffic switching with health check validation and rollback procedure.",
            }
        ],
    }
    plan_obj = SimpleNamespace(
        id="plan-obj",
        tasks=[
            SimpleNamespace(
                task_id="task-002",
                title="Deployment setup",
                prompt="Add traffic routing with monitoring and smoke tests.",
            )
        ],
    )

    result_dict = plan_task_blue_green_deployment_readiness(plan_dict)
    result_obj = plan_task_blue_green_deployment_readiness(plan_obj)

    assert result_dict.plan_id == "plan-dict"
    assert result_dict.blue_green_deployment_task_ids == ("task-001",)
    assert result_obj.plan_id == "plan-obj"
    assert result_obj.blue_green_deployment_task_ids == ("task-002",)


def test_empty_plan_produces_empty_findings():
    result = plan_task_blue_green_deployment_readiness(_plan([]))

    assert result.blue_green_deployment_task_ids == ()
    assert result.not_applicable_task_ids == ()
    assert result.findings == ()
    assert result.summary["blue_green_deployment_task_count"] == 0
    assert result.summary["overall_readiness"] == "weak"


def _plan(tasks):
    return {
        "id": "plan-bg-deploy",
        "tasks": tasks,
    }


def _task(task_id, *, title="", prompt="", outputs=None, scope=None):
    return {
        "task_id": task_id,
        "title": title,
        "prompt": prompt,
        "outputs": outputs or [],
        "scope": scope or [],
    }
