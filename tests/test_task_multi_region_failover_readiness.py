import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_multi_region_failover_readiness import (
    TaskMultiRegionFailoverReadinessPlan,
    TaskMultiRegionFailoverReadinessRecommendation,
    analyze_task_multi_region_failover_readiness,
    build_task_multi_region_failover_readiness_plan,
    extract_task_multi_region_failover_readiness,
    generate_task_multi_region_failover_readiness,
    recommend_task_multi_region_failover_readiness,
    summarize_task_multi_region_failover_readiness,
    task_multi_region_failover_readiness_plan_to_dict,
    task_multi_region_failover_readiness_plan_to_dicts,
    task_multi_region_failover_readiness_plan_to_markdown,
)


def test_detects_multi_region_failover_surfaces_and_missing_controls():
    result = build_task_multi_region_failover_readiness_plan(
        _plan(
            [
                _task(
                    "task-failover",
                    title="Add multi-region failover for public API",
                    description=(
                        "Deploy the public API to multiple regions with active-active routing, "
                        "DNS failover, traffic steering, and regional replicas."
                    ),
                    files_or_modules=[
                        "infra/multi_region/api_failover.tf",
                        "src/replication/regional_replicas.py",
                    ],
                    acceptance_criteria=["Customer-facing production traffic fails over between regions."],
                )
            ]
        )
    )

    assert isinstance(result, TaskMultiRegionFailoverReadinessPlan)
    assert result.failover_task_ids == ("task-failover",)
    record = result.records[0]
    assert isinstance(record, TaskMultiRegionFailoverReadinessRecommendation)
    assert {
        "multi_region_deployment",
        "failover",
        "active_active_routing",
        "regional_replica",
        "dns_failover",
        "traffic_steering",
        "customer_facing",
    } <= set(record.failover_surfaces)
    assert record.required_controls == (
        "failover_trigger",
        "health_check_signal",
        "data_replication_validation",
        "traffic_steering_plan",
        "regional_rollback_path",
        "runbook_owner",
        "recovery_metric",
    )
    assert "health_check_signal" not in record.present_controls
    assert "health_check_signal" in record.missing_controls
    assert record.risk_level == "high"
    assert any("description:" in item and "active-active routing" in item for item in record.evidence)
    assert "files_or_modules: infra/multi_region/api_failover.tf" in record.evidence
    assert result.summary["failover_task_count"] == 1
    assert result.summary["surface_counts"]["dns_failover"] == 1


def test_metadata_acceptance_criteria_and_paths_detect_controls_and_surfaces():
    result = analyze_task_multi_region_failover_readiness(
        _plan(
            [
                _task(
                    "task-dr",
                    title="Prepare active-passive disaster recovery",
                    description="Use a passive region for regional outage recovery.",
                    files_or_modules=["ops/dr/route53_failover.yml"],
                    metadata={
                        "failover_trigger": "Manual trigger when regional health checks fail.",
                        "recovery": {
                            "traffic_steering_plan": "Route traffic by DNS weights after draining primary.",
                            "owner": "Runbook owner is the incident commander.",
                        },
                    },
                    acceptance_criteria=[
                        "RTO and RPO recovery metrics are recorded.",
                        "Regional rollback path restores traffic to the primary region.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert {"failover", "active_passive_routing", "disaster_recovery", "dns_failover"} <= set(
        record.failover_surfaces
    )
    assert record.present_controls == (
        "failover_trigger",
        "health_check_signal",
        "traffic_steering_plan",
        "regional_rollback_path",
        "runbook_owner",
        "recovery_metric",
    )
    assert record.missing_controls == ("data_replication_validation",)
    assert record.risk_level == "medium"
    assert any("metadata.failover_trigger" in item for item in record.evidence)
    assert any("metadata.recovery.traffic_steering_plan" in item for item in record.evidence)


def test_customer_facing_data_replication_is_not_high_risk_when_controls_are_complete():
    result = build_task_multi_region_failover_readiness_plan(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Customer-facing regional data failover",
                    description=(
                        "Customer-facing checkout failover uses data replication across regions "
                        "and traffic steering through the global load balancer."
                    ),
                    acceptance_criteria=[
                        "Failover trigger is a regional health go/no-go decision point.",
                        "Health check signal includes synthetic checks and regional monitoring signal.",
                        "Data replication validation covers replica lag, checksums, and data reconciliation.",
                        "Traffic steering plan shifts traffic weights through DNS routing.",
                        "Regional rollback path restores traffic to the primary region.",
                        "Runbook owner is the on-call incident commander.",
                        "Recovery metric records RTO, RPO, latency, and availability targets.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert {"customer_facing", "data_replication", "failover", "traffic_steering"} <= set(record.failover_surfaces)
    assert record.missing_controls == ()
    assert record.risk_level == "medium"
    assert result.summary["risk_counts"] == {"high": 0, "medium": 1, "low": 0}
    assert result.summary["missing_control_count"] == 0


def test_serialization_markdown_aliases_order_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Multi-region routing | checkout",
                description="Traffic steering sends checkout production traffic across regions.",
            ),
            _task(
                "task-a",
                title="DNS failover",
                description="DNS failover includes health checks and Route53 weighted routing.",
            ),
            _task("task-copy", title="Update status copy", description="Change empty state wording."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_multi_region_failover_readiness(plan)
    payload = task_multi_region_failover_readiness_plan_to_dict(result)
    markdown = task_multi_region_failover_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["recommendations"]
    assert task_multi_region_failover_readiness_plan_to_dicts(result) == payload["recommendations"]
    assert task_multi_region_failover_readiness_plan_to_dicts(result.records) == payload["recommendations"]
    assert extract_task_multi_region_failover_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_multi_region_failover_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_multi_region_failover_readiness(plan).to_dict() == result.to_dict()
    assert result.records == result.recommendations
    assert result.failover_task_ids == ("task-z", "task-a")
    assert result.ignored_task_ids == ("task-copy",)
    assert list(payload) == [
        "plan_id",
        "recommendations",
        "records",
        "failover_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "failover_surfaces",
        "required_controls",
        "present_controls",
        "missing_controls",
        "risk_level",
        "evidence",
        "recommended_follow_up_actions",
    ]
    assert [record.risk_level for record in result.records] == ["high", "medium"]
    assert markdown.startswith("# Task Multi-Region Failover Readiness: plan-failover")
    assert "Multi-region routing \\| checkout" in markdown
    assert "| Task | Title | Risk | Failover Surfaces | Present Controls | Missing Controls | Evidence |" in markdown


def test_execution_plan_execution_task_iterable_and_no_op_behavior():
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Regional replica validation",
            description="Validate cross-region data replication and RPO before failover.",
        )
    )
    iterable_result = build_task_multi_region_failover_readiness_plan([model_task])
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-dr",
                    title="DR failover runbook",
                    description="Disaster recovery runbook covers regional outage failover.",
                ),
                _task("task-copy", title="Update footer copy", description="Adjust static text."),
            ]
        )
    )

    result = build_task_multi_region_failover_readiness_plan(plan)
    noop = build_task_multi_region_failover_readiness_plan(
        _plan([_task("task-copy", title="Update copy", description="Adjust static wording.")])
    )

    assert iterable_result.plan_id is None
    assert iterable_result.failover_task_ids == ("task-model",)
    assert result.plan_id == "plan-failover"
    assert result.failover_task_ids == ("task-dr",)
    assert result.ignored_task_ids == ("task-copy",)
    assert noop.records == ()
    assert noop.failover_task_ids == ()
    assert noop.ignored_task_ids == ("task-copy",)
    assert noop.to_dicts() == []
    assert noop.summary == {
        "task_count": 1,
        "failover_task_count": 0,
        "ignored_task_ids": ["task-copy"],
        "missing_control_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "surface_counts": {},
        "present_control_counts": {
            "failover_trigger": 0,
            "health_check_signal": 0,
            "data_replication_validation": 0,
            "traffic_steering_plan": 0,
            "regional_rollback_path": 0,
            "runbook_owner": 0,
            "recovery_metric": 0,
        },
        "failover_task_ids": [],
    }
    assert "No multi-region failover readiness recommendations" in noop.to_markdown()
    assert "Ignored tasks: task-copy" in noop.to_markdown()


def _plan(tasks, plan_id="plan-failover"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-failover",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-failover",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "medium",
        "estimated_hours": 2.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
