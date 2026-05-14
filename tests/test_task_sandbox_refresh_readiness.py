import copy
import json

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord
from blueprint.domain.models import ExecutionPlan
from blueprint.task_sandbox_refresh_readiness import (
    TaskSandboxRefreshReadinessPlan,
    analyze_task_sandbox_refresh_readiness,
    build_task_sandbox_refresh_readiness_plan,
    recommend_task_sandbox_refresh_readiness,
    summarize_task_sandbox_refresh_readiness,
    summarize_task_sandbox_refresh_readiness_plan,
    task_sandbox_refresh_readiness_plan_to_dict,
    task_sandbox_refresh_readiness_plan_to_dicts,
    task_sandbox_refresh_readiness_plan_to_markdown,
)


def test_complete_sandbox_refresh_task_is_ready_with_no_actionable_gaps():
    result = build_task_sandbox_refresh_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Refresh sandbox from production snapshot",
                    description="Sandbox refresh rebuilds tenant data from a source snapshot.",
                    acceptance_criteria=[
                        "Refresh source is the approved production snapshot and backup source.",
                        "Data masking redacts PII and sensitive data before sandbox access.",
                        "Downtime window is scheduled during the maintenance window.",
                        "Service coordination covers dependent services, paused jobs, and queue drain.",
                        "Validation smoke tests run health checks, login tests, and row count verification.",
                        "Rollback restore point uses a pre-refresh backup and documented restore plan.",
                        "Stakeholder notification sends Slack notice, email notice, and announcement.",
                    ],
                    files_or_modules=["ops/sandbox/refresh_runbook.md"],
                ),
                _task("task-copy", title="Update onboarding copy", description="Change help text."),
            ]
        )
    )

    assert isinstance(result, TaskSandboxRefreshReadinessPlan)
    assert isinstance(result, SimpleReadinessPlan)
    assert result.impacted_task_ids == ("task-ready",)
    assert result.ignored_task_ids == ("task-copy",)
    record = result.records[0]
    assert isinstance(record, SimpleReadinessRecord)
    assert record.detected_signals == ("sandbox_refresh", "environment_refresh")
    assert record.present_criteria == (
        "refresh_source",
        "data_masking",
        "downtime_window",
        "service_coordination",
        "validation_smoke_tests",
        "rollback_restore_point",
        "stakeholder_notification",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_partial_sandbox_refresh_task_reports_required_missing_requirements():
    result = analyze_task_sandbox_refresh_readiness(
        [
            _task(
                "task-partial",
                title="Refresh staging environment",
                description="Run a staging refresh for QA.",
            )
        ]
    )

    record = result.records[0]
    assert record.readiness == "needs_planning"
    assert record.present_criteria == ()
    assert record.missing_criteria == (
        "refresh_source",
        "data_masking",
        "downtime_window",
        "service_coordination",
        "validation_smoke_tests",
        "rollback_restore_point",
        "stakeholder_notification",
    )
    assert record.recommended_follow_up_actions == (
        "Identify the refresh source such as production snapshot, backup, dump, fixture, or baseline environment.",
        "Confirm sensitive data is masked, redacted, anonymized, scrubbed, synthetic, or explicitly absent.",
        "Schedule the downtime, maintenance, refresh, freeze, or service interruption window.",
        "Coordinate dependent services, integration owners, queues, jobs, webhooks, caches, or upstream/downstream systems.",
        "Add validation smoke tests such as health checks, login checks, row counts, or post-refresh verification.",
        "Document rollback, restore point, pre-refresh backup, snapshot, revert, or recovery steps.",
        "Notify stakeholders through announcements, Slack, email, status page, release notes, or team notices.",
    )


def test_environment_sandbox_staging_fixture_and_restore_paths_contribute_evidence_without_mutation():
    source = _plan(
        [
            _task(
                "task-paths",
                title="Fixture refresh for lower environment",
                description="Environment refresh restores QA fixtures.",
                files_or_modules=[
                    "environments/sandbox/refresh.yaml",
                    "ops/staging/restore-point.md",
                    "fixtures/environment_refresh.sql",
                ],
                metadata={
                    "runbook": {
                        "source": "Refresh source is a baseline source dump.",
                        "validation": "Post-refresh check validates health check and row count.",
                    },
                    "notification": "Team notice is posted before the refresh window.",
                },
            )
        ]
    )
    original = copy.deepcopy(source)

    result = build_task_sandbox_refresh_readiness_plan(ExecutionPlan.model_validate(source))

    assert source == original
    record = result.records[0]
    assert record.detected_signals == ("sandbox_refresh", "staging_refresh", "environment_refresh")
    assert record.present_criteria == (
        "refresh_source",
        "downtime_window",
        "validation_smoke_tests",
        "rollback_restore_point",
        "stakeholder_notification",
    )
    assert record.missing_criteria == (
        "data_masking",
        "service_coordination",
    )
    assert any("metadata.runbook.source" in item for item in record.evidence)
    assert any("files_or_modules: environments/sandbox/refresh.yaml" in item for item in record.evidence)
    assert any("files_or_modules: ops/staging/restore-point.md" in item for item in record.evidence)
    assert any("files_or_modules: fixtures/environment_refresh.sql" in item for item in record.evidence)


def test_no_impact_tasks_are_excluded_and_aliases_are_stable():
    result = summarize_task_sandbox_refresh_readiness(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Document sandbox login",
                    description="No sandbox refresh or environment restore changes are planned.",
                ),
                _task(
                    "task-partial",
                    title="Sandbox data refresh",
                    description="Sandbox refresh includes data masking and stakeholder notification.",
                ),
            ],
            plan_id="plan-sandbox-refresh-sort",
        )
    )

    payload = task_sandbox_refresh_readiness_plan_to_dict(result)
    markdown = task_sandbox_refresh_readiness_plan_to_markdown(result)

    assert [record.task_id for record in result.records] == ["task-partial"]
    assert result.ignored_task_ids == ("task-docs",)
    assert analyze_task_sandbox_refresh_readiness(result) is result
    assert summarize_task_sandbox_refresh_readiness_plan(result) is result
    assert recommend_task_sandbox_refresh_readiness(result) == result.records
    assert result.to_dicts() == payload["records"]
    assert task_sandbox_refresh_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-sandbox-refresh-sort"
    assert markdown.startswith("# Task Sandbox Refresh Readiness: plan-sandbox-refresh-sort")
    assert "| Task | Title | Readiness |" in markdown


def test_invalid_inputs_return_empty_simple_plan():
    assert build_task_sandbox_refresh_readiness_plan(42).records == ()
    assert build_task_sandbox_refresh_readiness_plan({"id": "bad", "tasks": "not a list"}).records == ()
    assert build_task_sandbox_refresh_readiness_plan({"tasks": []}).summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-sandbox-refresh"):
    return {"id": plan_id, "implementation_brief_id": "brief-sandbox-refresh", "milestones": [], "tasks": tasks}


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
