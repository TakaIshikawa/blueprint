import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_mobile_app_release_readiness_matrix import (
    PlanMobileAppReleaseReadinessMatrix,
    PlanMobileAppReleaseReadinessRow,
    analyze_plan_mobile_app_release_readiness_matrix,
    build_plan_mobile_app_release_readiness_matrix,
    derive_plan_mobile_app_release_readiness_matrix,
    extract_plan_mobile_app_release_readiness_matrix,
    generate_plan_mobile_app_release_readiness_matrix,
    plan_mobile_app_release_readiness_matrix_to_dict,
    plan_mobile_app_release_readiness_matrix_to_dicts,
    plan_mobile_app_release_readiness_matrix_to_markdown,
    summarize_plan_mobile_app_release_readiness_matrix,
)


def test_matrix_groups_mobile_release_tasks_into_ready_row():
    result = build_plan_mobile_app_release_readiness_matrix(
        _plan(
            [
                _task(
                    "task-store-review",
                    title="Prepare iOS and Android app store review",
                    description="Submit mobile app release metadata through App Store Connect and Google Play review.",
                    acceptance_criteria=[
                        "Reviewer access, store listing, and review notes are complete.",
                        "Version compatibility covers iOS 17+, Android 12+, build number, and version code.",
                    ],
                ),
                _task(
                    "task-rollout-monitoring",
                    title="Run phased mobile rollout with monitoring",
                    description=(
                        "Use phased rollout at 5%, 25%, and 100% with Crashlytics and analytics dashboards."
                    ),
                    acceptance_criteria=[
                        "Pause rollout if crash-free threshold drops or ANR alerts fire.",
                        "Feature flag gating and remote config can disable the new flow.",
                        "Hotfix path uses expedited review and rollback to the previous build.",
                    ],
                ),
                _task("task-copy", title="Refresh web copy", description="Update onboarding labels."),
            ]
        )
    )

    assert isinstance(result, PlanMobileAppReleaseReadinessMatrix)
    assert all(isinstance(row, PlanMobileAppReleaseReadinessRow) for row in result.rows)
    assert result.plan_id == "plan-mobile-release"
    assert result.mobile_release_task_ids == ("task-rollout-monitoring", "task-store-review")
    assert result.non_mobile_task_ids == ("task-copy",)
    assert len(result.rows) == 1

    row = result.rows[0]
    assert row.platform == "multi_platform"
    assert row.release_area == "mobile_app_release"
    assert row.task_ids == ("task-rollout-monitoring", "task-store-review")
    assert row.readiness == "ready"
    assert row.severity == "low"
    assert row.gaps == ()
    assert row.signals == (
        "app_store_review",
        "phased_rollout",
        "version_compatibility",
        "crash_analytics_monitoring",
        "rollback_hotfix_path",
        "feature_flag_gating",
    )
    assert any("App Store Connect" in item for item in row.evidence)
    assert any("Crashlytics" in item for item in row.evidence)


def test_missing_rollout_monitoring_or_rollback_marks_blocked_or_partial():
    result = build_plan_mobile_app_release_readiness_matrix(
        _plan(
            [
                _task(
                    "task-no-rollback",
                    title="Submit Android app release for review",
                    description="Google Play review and staged rollout are planned for the Android app.",
                    acceptance_criteria=[
                        "Crashlytics monitoring and analytics alerts gate widening.",
                        "Gap: rollback or hotfix path is TBD.",
                    ],
                ),
                _task(
                    "task-no-rollout",
                    title="Prepare iOS mobile app release",
                    description="App Store review package includes version compatibility for iOS 17.",
                    acceptance_criteria=["Crash-free monitoring is defined, but rollout is not documented."],
                ),
                _task(
                    "task-partial",
                    title="Coordinate mobile release rollout",
                    description="Mobile app phased rollout uses store review notes and hotfix expedited review.",
                    acceptance_criteria=["Rollback owner is named, but monitoring is not defined."],
                ),
            ]
        )
    )

    blocked_rollback = _row(result, "task-no-rollback")
    assert blocked_rollback.platform == "android"
    assert blocked_rollback.readiness == "blocked"
    assert blocked_rollback.severity == "high"
    assert "Missing rollback or hotfix path." in blocked_rollback.gaps

    blocked_rollout = _row(result, "task-no-rollout")
    assert blocked_rollout.platform == "ios"
    assert blocked_rollout.readiness == "blocked"
    assert "Missing phased rollout or staged rollout plan." in blocked_rollout.gaps

    partial = _row(result, "task-partial")
    assert partial.platform == "mobile"
    assert partial.readiness == "partial"
    assert partial.severity == "medium"
    assert "Missing crash, analytics, or release monitoring evidence." in partial.gaps

    assert result.summary["readiness_counts"] == {"blocked": 2, "partial": 1, "ready": 0}
    assert result.summary["severity_counts"] == {"high": 2, "medium": 1, "low": 0}


def test_model_mapping_object_raw_inputs_and_stable_ordering():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-ios",
                    title="Ship iOS release",
                    description="App Store review, phased rollout, Crashlytics monitoring, and rollback hotfix path.",
                ),
                _task(
                    "task-android",
                    title="Ship Android release",
                    description="Play Store review, staged rollout, analytics alerts, and expedited review hotfix.",
                ),
                _task("task-admin", title="Admin export", description="Add CSV export."),
            ]
        )
    )

    result = build_plan_mobile_app_release_readiness_matrix(plan)

    assert [row.platform for row in result.rows] == ["android", "ios"]
    assert [row.task_ids[0] for row in result.rows] == ["task-android", "task-ios"]
    assert result.non_mobile_task_ids == ("task-admin",)

    raw_result = build_plan_mobile_app_release_readiness_matrix(
        [
            _task(
                "task-raw",
                title="Mobile app launch",
                description="Review submission, phased rollout, monitoring dashboard, and rollback path.",
            )
        ]
    )
    object_result = build_plan_mobile_app_release_readiness_matrix(
        SimpleNamespace(
            id="task-object",
            title="iOS release",
            description="App Store review, staged rollout, crash analytics monitoring, and hotfix rollback.",
        )
    )

    assert raw_result.plan_id is None
    assert raw_result.rows[0].readiness == "ready"
    assert object_result.rows[0].platform == "ios"
    assert object_result.rows[0].readiness == "ready"


def test_serialization_aliases_markdown_empty_invalid_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-mobile | 1",
                title="Mobile | release",
                description="App review, phased rollout, analytics monitoring, and rollback path.",
                acceptance_criteria=["Push notification impact and feature flag gating are documented."],
            ),
            _task("task-docs", title="Docs", description="Update web docs."),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_mobile_app_release_readiness_matrix(plan)
    payload = plan_mobile_app_release_readiness_matrix_to_dict(result)
    markdown = plan_mobile_app_release_readiness_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_mobile_app_release_readiness_matrix(plan).to_dict() == result.to_dict()
    assert analyze_plan_mobile_app_release_readiness_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_mobile_app_release_readiness_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_mobile_app_release_readiness_matrix(plan).to_dict() == result.to_dict()
    assert summarize_plan_mobile_app_release_readiness_matrix(result) == result.summary
    assert plan_mobile_app_release_readiness_matrix_to_dicts(result) == payload["rows"]
    assert plan_mobile_app_release_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "mobile_release_task_ids",
        "non_mobile_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "platform",
        "release_area",
        "task_ids",
        "titles",
        "signals",
        "gaps",
        "readiness",
        "severity",
        "evidence",
    ]
    assert markdown.startswith("# Plan Mobile App Release Readiness Matrix: plan-mobile-release")
    assert "Mobile \\| release" in markdown
    assert "task-mobile \\| 1" in markdown
    assert "Non-mobile tasks: task-docs" in markdown

    empty = build_plan_mobile_app_release_readiness_matrix({"id": "empty-mobile", "tasks": []})
    invalid = build_plan_mobile_app_release_readiness_matrix(23)

    assert empty.to_dict() == {
        "plan_id": "empty-mobile",
        "rows": [],
        "records": [],
        "mobile_release_task_ids": [],
        "non_mobile_task_ids": [],
        "summary": {
            "task_count": 0,
            "row_count": 0,
            "mobile_release_task_count": 0,
            "non_mobile_task_count": 0,
            "readiness_counts": {"blocked": 0, "partial": 0, "ready": 0},
            "severity_counts": {"high": 0, "medium": 0, "low": 0},
            "platform_counts": {},
            "signal_counts": {
                "app_store_review": 0,
                "phased_rollout": 0,
                "version_compatibility": 0,
                "push_notification_impact": 0,
                "crash_analytics_monitoring": 0,
                "rollback_hotfix_path": 0,
                "feature_flag_gating": 0,
            },
        },
    }
    assert "No mobile app release readiness rows were inferred." in empty.to_markdown()
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0


def _row(result, task_id):
    return next(row for row in result.rows if task_id in row.task_ids)


def _plan(tasks, *, plan_id="plan-mobile-release"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-mobile-release",
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
