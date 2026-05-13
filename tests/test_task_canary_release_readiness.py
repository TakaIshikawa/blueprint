from blueprint.domain.models import ExecutionPlan
from blueprint.task_canary_release_readiness import (
    build_task_canary_release_readiness_plan,
    recommend_task_canary_release_readiness,
    task_canary_release_readiness_plan_to_dict,
    task_canary_release_readiness_plan_to_markdown,
    task_canary_release_readiness_to_dicts,
)


def test_complete_canary_task_is_low_risk_without_gaps():
    plan = build_task_canary_release_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    "Canary release checkout",
                    "Canary release starts with 5% traffic slice for internal users and beta cohort. Success metrics track error rate and latency. Rollback trigger uses kill switch at SLO breach. Monitoring dashboard and alerts are ready. Ramp schedule increases to 25% then 50%.",
                )
            ]
        )
    )

    finding = plan.records[0]
    assert finding.readiness == "ready"
    assert finding.risk_level == "low"
    assert finding.missing_criteria == ()
    assert finding.actionable_gaps == ()


def test_partial_canary_task_reports_all_required_gaps_in_order():
    plan = build_task_canary_release_readiness_plan([_task("task-partial", "Canary deploy profile", "Canary rollout for profile card.")])
    finding = plan.records[0]

    assert finding.readiness == "partial"
    assert finding.risk_level == "high"
    assert finding.missing_criteria == (
        "traffic_slice",
        "audience_criteria",
        "success_metrics",
        "rollback_trigger",
        "monitoring",
        "ramp_schedule",
    )
    assert finding.actionable_gaps[0].startswith("Define the initial traffic")


def test_non_canary_and_negated_tasks_are_ignored():
    plan = build_task_canary_release_readiness_plan(
        _plan(
            [
                _task("task-docs", "Update docs", "Refresh documentation only."),
                _task("task-no-canary", "Release flag", "No canary release work is required."),
            ]
        )
    )

    assert plan.records == ()
    assert plan.ignored_task_ids == ("task-docs", "task-no-canary")


def test_execution_plan_and_serialization_are_stable():
    model = ExecutionPlan.model_validate(
        _plan([_task("task-model", "Canary release API", "Canary release has 10% traffic, allowlist audience, dashboard monitoring, rollback trigger, success metrics, and daily ramp schedule.")])
    )
    plan = build_task_canary_release_readiness_plan(model)
    payload = task_canary_release_readiness_plan_to_dict(plan)

    assert recommend_task_canary_release_readiness(model) == plan.records
    assert task_canary_release_readiness_to_dicts(plan) == payload["findings"]
    assert task_canary_release_readiness_plan_to_markdown(plan) == plan.to_markdown()
    assert payload["summary"]["canary_task_count"] == 1


def _plan(tasks):
    return {"id": "plan-canary", "implementation_brief_id": "brief-canary", "milestones": [], "tasks": tasks}


def _task(task_id, title, description):
    return {"id": task_id, "title": title, "description": description, "acceptance_criteria": []}
