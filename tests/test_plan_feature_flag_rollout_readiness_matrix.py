import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_feature_flag_rollout_readiness_matrix import (
    PlanFeatureFlagRolloutReadinessMatrix,
    PlanFeatureFlagRolloutReadinessRow,
    analyze_plan_feature_flag_rollout_readiness_matrix,
    build_plan_feature_flag_rollout_readiness_matrix,
    derive_plan_feature_flag_rollout_readiness_matrix,
    extract_plan_feature_flag_rollout_readiness_matrix,
    generate_plan_feature_flag_rollout_readiness_matrix,
    plan_feature_flag_rollout_readiness_matrix_to_dict,
    plan_feature_flag_rollout_readiness_matrix_to_dicts,
    plan_feature_flag_rollout_readiness_matrix_to_markdown,
    summarize_plan_feature_flag_rollout_readiness_matrix,
)


def test_multiple_rollout_tasks_group_by_surface_with_evidence_and_stable_ordering():
    result = build_plan_feature_flag_rollout_readiness_matrix(
        _plan(
            [
                _task(
                    "task-checkout-ramp",
                    title="Rollout checkout_v2 feature flag",
                    description="Gradual rollout checkout_v2 to 10 percent of beta customers.",
                    acceptance_criteria=[
                        "Owner: Payments PM.",
                        "Targeting uses tenant allowlist, cohort, and percentage ramp criteria.",
                        "Monitoring dashboard tracks conversion, latency, error rate, and alerts.",
                    ],
                    metadata={"flag": "checkout_v2"},
                ),
                _task(
                    "task-search-canary",
                    title="Canary search_ranker experiment",
                    description="Run canary experiment search_ranker for internal users.",
                    acceptance_criteria=[
                        "Owner: Search team.",
                        "Targeting is beta users in the internal cohort.",
                        "Monitoring metrics and alerts are attached to the experiment dashboard.",
                        "Rollback disables the variant and the kill-switch is global off.",
                        "Cleanup removes the experiment flag after graduation.",
                    ],
                ),
                _task(
                    "task-checkout-safety",
                    title="Add checkout_v2 kill-switch and cleanup",
                    description="Feature flag checkout_v2 gets a kill-switch.",
                    acceptance_criteria=[
                        "Rollback turns off checkout_v2 immediately.",
                        "Kill-switch is emergency off for all traffic.",
                        "Cleanup deletes the flag after the ramp completes.",
                    ],
                ),
                _task("task-copy", title="Update help copy", description="Refresh settings labels."),
            ]
        )
    )

    assert isinstance(result, PlanFeatureFlagRolloutReadinessMatrix)
    assert all(isinstance(row, PlanFeatureFlagRolloutReadinessRow) for row in result.rows)
    assert result.plan_id == "plan-rollout"
    assert result.rollout_task_ids == ("task-checkout-ramp", "task-checkout-safety", "task-search-canary")
    assert result.no_rollout_task_ids == ("task-copy",)
    assert [row.rollout_surface for row in result.rows] == ["checkout_v2", "search_ranker"]

    checkout = _row(result, "checkout_v2")
    assert checkout.task_ids == ("task-checkout-ramp", "task-checkout-safety")
    assert checkout.readiness == "ready"
    assert checkout.severity == "low"
    assert checkout.gaps == ()
    assert any("checkout_v2" in item for item in checkout.evidence)

    search = _row(result, "search_ranker")
    assert search.readiness == "ready"
    assert search.kill_switch == "present"


def test_missing_rollback_monitoring_owner_or_cleanup_downgrades_with_actionable_gaps():
    result = build_plan_feature_flag_rollout_readiness_matrix(
        _plan(
            [
                _task(
                    "task-missing-safety",
                    title="Beta flag profile_card",
                    description="Launch profile_card as a beta feature flag for 5 percent of users.",
                    acceptance_criteria=[
                        "Targeting uses a percentage ramp and allowlist.",
                        "Kill-switch can disable the feature globally.",
                    ],
                ),
                _task(
                    "task-partial",
                    title="Experiment checkout_banner",
                    description="Owner Growth runs experiment checkout_banner with targeting cohorts.",
                    acceptance_criteria=[
                        "Monitoring dashboard, rollback trigger, and kill-switch are documented.",
                    ],
                ),
            ]
        )
    )

    blocked = _row(result, "profile_card")
    assert blocked.readiness == "blocked"
    assert blocked.severity == "high"
    assert "Missing rollout owner." in blocked.gaps
    assert "Missing monitoring or alert criteria." in blocked.gaps
    assert "Missing rollback criteria." in blocked.gaps
    assert "Missing flag cleanup criteria." in blocked.gaps

    partial = _row(result, "checkout_banner")
    assert partial.readiness == "partial"
    assert partial.severity == "medium"
    assert partial.gaps == ("Missing flag cleanup criteria.",)
    assert result.summary["readiness_counts"] == {"blocked": 1, "partial": 1, "ready": 0}
    assert result.summary["severity_counts"] == {"high": 1, "medium": 1, "low": 0}


def test_no_rollout_signals_return_empty_rows_and_stable_summary_counts():
    result = build_plan_feature_flag_rollout_readiness_matrix(
        _plan(
            [
                _task("task-api", title="Build API endpoint", description="Implement normal CRUD behavior."),
                _task("task-docs", title="Document endpoint", description="Update docs."),
            ]
        )
    )

    assert result.rows == ()
    assert result.rollout_task_ids == ()
    assert result.no_rollout_task_ids == ("task-api", "task-docs")
    assert result.summary == {
        "task_count": 2,
        "row_count": 0,
        "rollout_task_count": 0,
        "no_rollout_task_count": 2,
        "readiness_counts": {"blocked": 0, "partial": 0, "ready": 0},
        "severity_counts": {"high": 0, "medium": 0, "low": 0},
        "gap_counts": {},
        "surface_counts": {},
    }
    assert "No feature flag rollout readiness rows were inferred." in result.to_markdown()
    assert "No rollout signals: task-api, task-docs" in result.to_markdown()


def test_serialization_aliases_markdown_model_object_input_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-flag | ramp",
                title="Rollout billing | portal feature flag",
                description="Rollout billing_portal with owner, targeting, monitoring, rollback, kill-switch, and cleanup criteria.",
                acceptance_criteria=["Beta cohort uses percentage ramp and the cleanup removes the flag."],
            )
        ]
    )
    original = copy.deepcopy(plan)
    model_plan = ExecutionPlan.model_validate(plan)

    result = build_plan_feature_flag_rollout_readiness_matrix(model_plan)
    payload = plan_feature_flag_rollout_readiness_matrix_to_dict(result)
    markdown = plan_feature_flag_rollout_readiness_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_feature_flag_rollout_readiness_matrix(plan).to_dict() == result.to_dict()
    assert analyze_plan_feature_flag_rollout_readiness_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_feature_flag_rollout_readiness_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_feature_flag_rollout_readiness_matrix(plan).to_dict() == result.to_dict()
    assert summarize_plan_feature_flag_rollout_readiness_matrix(result) == result.summary
    assert plan_feature_flag_rollout_readiness_matrix_to_dicts(result) == payload["rows"]
    assert plan_feature_flag_rollout_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "rollout_task_ids",
        "no_rollout_task_ids",
        "summary",
    ]
    assert "billing \\| portal" in markdown
    assert "task-flag \\| ramp" in markdown

    object_result = build_plan_feature_flag_rollout_readiness_matrix(
        SimpleNamespace(
            id="object-flag",
            title="Canary object_flag",
            description="Owner runs object_flag canary with targeting, monitoring, rollback, kill-switch, and cleanup.",
            acceptance_criteria=["Ready"],
        )
    )
    invalid = build_plan_feature_flag_rollout_readiness_matrix(23)

    assert object_result.rows[0].task_ids == ("object-flag",)
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0


def _row(result, surface):
    return next(row for row in result.rows if row.rollout_surface == surface)


def _plan(tasks, *, plan_id="plan-rollout"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-rollout",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    depends_on=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "depends_on": [] if depends_on is None else depends_on,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
