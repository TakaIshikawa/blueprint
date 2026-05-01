import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_rollout_telemetry import (
    TaskRolloutTelemetryPlan,
    TaskRolloutTelemetryRecord,
    build_task_rollout_telemetry_plan,
    summarize_task_rollout_telemetry,
    task_rollout_telemetry_plan_to_dict,
    task_rollout_telemetry_plan_to_markdown,
)


def test_rollout_migration_queue_and_workflow_tasks_require_concrete_telemetry():
    result = build_task_rollout_telemetry_plan(
        _plan(
            [
                _task(
                    "task-rollout",
                    title="Roll out checkout feature flag",
                    description=(
                        "Canary the customer-facing checkout workflow behind a feature flag "
                        "with rollback guardrails."
                    ),
                    files_or_modules=[
                        "src/frontend/checkout/CheckoutPage.tsx",
                        "src/flags/checkout_feature_flag.py",
                    ],
                    acceptance_criteria=[
                        "Dashboard tracks rollout percentage, conversion, p95 latency, and errors.",
                        "Alert fires when canary guardrails breach rollback thresholds.",
                    ],
                ),
                _task(
                    "task-migration",
                    title="Migrate invoice queue workers",
                    description=(
                        "Deploy a database migration and backfill invoice jobs with dual-write "
                        "compatibility for background workers."
                    ),
                    files_or_modules=[
                        "db/migrations/20260502_invoice_jobs.sql",
                        "src/workers/invoice_queue.py",
                    ],
                    metadata={"runbook": {"rollback": "Pause queue consumers and roll back deployment."}},
                ),
            ]
        )
    )

    rollout = _record(result, "task-rollout")
    assert rollout.telemetry_status == "telemetry_required"
    assert rollout.detected_signals == (
        "rollout",
        "feature_flag",
        "performance",
        "user_workflow",
    )
    assert {"metric", "log", "trace", "dashboard", "alert"}.issubset(
        set(rollout.signal_types)
    )
    assert any("rollout percentage" in item for item in rollout.metrics)
    assert any("structured logs" in item for item in rollout.logs)
    assert any("trace spans" in item for item in rollout.traces)
    assert any("canary cohorts" in item for item in rollout.alerts)
    assert any("Success:" in item for item in rollout.success_indicators)
    assert any("Failure:" in item for item in rollout.failure_indicators)

    migration = _record(result, "task-migration")
    assert migration.telemetry_status == "telemetry_required"
    assert migration.detected_signals == ("deployment", "migration", "queue")
    assert any("migration progress" in item for item in migration.metrics)
    assert any("migration batch id" in item for item in migration.logs)
    assert any("old and new code paths" in item for item in migration.traces)
    assert any("migration stalls" in item for item in migration.alerts)
    assert "metadata.runbook.rollback: Pause queue consumers and roll back deployment." in migration.evidence

    assert result.telemetry_required_task_ids == ("task-migration", "task-rollout")
    assert result.telemetry_optional_task_ids == ()
    assert result.telemetry_not_needed_task_ids == ()
    assert result.summary["telemetry_required_count"] == 2
    assert result.summary["signal_type_counts"]["metric"] == 2
    assert result.summary["signal_type_counts"]["alert"] == 2
    assert result.summary["rollout_signal_counts"]["migration"] == 1
    assert result.summary["rollout_signal_counts"]["queue"] == 1


def test_documentation_test_only_and_benign_tasks_are_not_required():
    result = build_task_rollout_telemetry_plan(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Document deployment rollback runbook",
                    description="Update docs with rollout and alert expectations for operators.",
                    files_or_modules=["docs/runbooks/deployment_rollback.md"],
                    acceptance_criteria=["Release notes describe the runbook change."],
                ),
                _task(
                    "task-tests",
                    title="Add queue worker retry tests",
                    description="Test-only coverage for background job retry behavior.",
                    files_or_modules=["tests/test_invoice_queue_worker.py"],
                ),
                _task(
                    "task-refactor",
                    title="Refactor formatting helpers",
                    description="Rename helper variables and keep unit tests passing.",
                    files_or_modules=["src/blueprint/formatting.py"],
                ),
            ]
        )
    )

    docs = _record(result, "task-docs")
    tests = _record(result, "task-tests")
    refactor = _record(result, "task-refactor")

    assert docs.telemetry_status == "telemetry_optional"
    assert docs.signal_types == ("metric", "log", "success_indicator")
    assert any("existing metric or dashboard" in item for item in docs.metrics)
    assert tests.telemetry_status == "telemetry_optional"
    assert tests.detected_signals == ("queue",)
    assert refactor.telemetry_status == "telemetry_not_needed"
    assert refactor.signal_types == ()
    assert result.telemetry_required_task_ids == ()
    assert result.telemetry_optional_task_ids == ("task-docs", "task-tests")
    assert result.telemetry_not_needed_task_ids == ("task-refactor",)
    assert result.summary["status_counts"] == {
        "telemetry_required": 0,
        "telemetry_optional": 2,
        "telemetry_not_needed": 1,
    }


def test_model_input_serializes_stably_and_renders_markdown():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-deploy",
                    title="Deploy notifications experiment",
                    description="Release an A/B test for the user-facing notification workflow.",
                    files_or_modules=["src/experiments/notifications.py"],
                    acceptance_criteria=["Alerting covers variant assignment errors."],
                )
            ]
        )
    )

    result = summarize_task_rollout_telemetry(plan)
    payload = task_rollout_telemetry_plan_to_dict(result)

    assert isinstance(result, TaskRolloutTelemetryPlan)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert list(payload) == [
        "plan_id",
        "records",
        "telemetry_required_task_ids",
        "telemetry_optional_task_ids",
        "telemetry_not_needed_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "task_title",
        "telemetry_status",
        "detected_signals",
        "signal_types",
        "metrics",
        "logs",
        "traces",
        "dashboards",
        "alerts",
        "success_indicators",
        "failure_indicators",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert TaskRolloutTelemetryRecord(
        task_id="task-deploy",
        task_title="Deploy notifications experiment",
        telemetry_status="telemetry_required",
        detected_signals=("deployment", "experiment", "user_workflow"),
        signal_types=(
            "metric",
            "log",
            "trace",
            "dashboard",
            "alert",
            "success_indicator",
            "failure_indicator",
        ),
        metrics=(
            "Track task-specific adoption, error rate, latency, and throughput before and after the change.",
            "Track deploy health, version adoption, restart count, and post-deploy error budget burn.",
            "Track experiment exposure, conversion, guardrail metrics, and variant assignment errors.",
            "Track workflow starts, completions, abandonment, user-visible errors, and support-impacting failures.",
        ),
        logs=(
            "Emit structured logs with task identifier, rollout cohort, outcome, and rollback state.",
            "Log deployment version, environment, migration step, and rollback action for changed services.",
            "Log anonymized experiment key, variant, cohort, and conversion outcome.",
        ),
        traces=(
            "Propagate trace spans through the changed path with attributes for rollout state and failure reason.",
            "Trace the full user workflow across frontend, API, worker, and third-party boundaries.",
        ),
        dashboards=(
            "Create or update a dashboard that compares baseline and post-change health for this task.",
            "Show deploy version, service health, error budget burn, and rollback status together.",
            "Expose workflow conversion, user-visible errors, latency, and dependency health.",
        ),
        alerts=(
            "Define alerts for elevated errors, latency regression, stalled rollout, or rollback trigger conditions.",
        ),
        success_indicators=(
            "Success: health metrics stay within baseline while intended adoption or completion increases.",
            "Success: primary metric improves without guardrail degradation.",
        ),
        failure_indicators=(
            "Failure: errors, latency, retries, backlog, or user drop-off exceed the rollback threshold.",
        ),
        evidence=(
            "title: Deploy notifications experiment",
            "description: Release an A/B test for the user-facing notification workflow.",
            "files_or_modules: src/experiments/notifications.py",
            "acceptance_criteria[0]: Alerting covers variant assignment errors.",
        ),
    ) in result.records
    assert task_rollout_telemetry_plan_to_markdown(result) == "\n".join(
        [
            "# Task Rollout Telemetry Plan: plan-rollout-telemetry",
            "",
            "| Task | Status | Signals | Metrics | Logs | Traces | Dashboards | Alerts | Success Indicators | Failure Indicators |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            "| task-deploy | telemetry_required | deployment, experiment, user_workflow | "
            "Track task-specific adoption, error rate, latency, and throughput before and after the change.; "
            "Track deploy health, version adoption, restart count, and post-deploy error budget burn.; "
            "Track experiment exposure, conversion, guardrail metrics, and variant assignment errors.; "
            "Track workflow starts, completions, abandonment, user-visible errors, and support-impacting failures. | "
            "Emit structured logs with task identifier, rollout cohort, outcome, and rollback state.; "
            "Log deployment version, environment, migration step, and rollback action for changed services.; "
            "Log anonymized experiment key, variant, cohort, and conversion outcome. | "
            "Propagate trace spans through the changed path with attributes for rollout state and failure reason.; "
            "Trace the full user workflow across frontend, API, worker, and third-party boundaries. | "
            "Create or update a dashboard that compares baseline and post-change health for this task.; "
            "Show deploy version, service health, error budget burn, and rollback status together.; "
            "Expose workflow conversion, user-visible errors, latency, and dependency health. | "
            "Define alerts for elevated errors, latency regression, stalled rollout, or rollback trigger conditions. | "
            "Success: health metrics stay within baseline while intended adoption or completion increases.; "
            "Success: primary metric improves without guardrail degradation. | "
            "Failure: errors, latency, retries, backlog, or user drop-off exceed the rollback threshold. |",
        ]
    )
    assert task_rollout_telemetry_plan_to_markdown(result) == result.to_markdown()


def test_plain_task_iterable_and_single_task_mapping_are_supported():
    iterable_result = build_task_rollout_telemetry_plan(
        [
            _task(
                "task-flag",
                title="Enable feature flag",
                description="Roll out the new flag to a canary cohort.",
            )
        ]
    )
    mapping_result = build_task_rollout_telemetry_plan(
        _task(
            "task-performance",
            title="Improve p95 latency",
            description="Tune cache performance for the customer workflow.",
        )
    )

    assert iterable_result.plan_id is None
    assert iterable_result.records[0].telemetry_status == "telemetry_required"
    assert mapping_result.records[0].detected_signals == ("performance", "user_workflow")


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks):
    return {
        "id": "plan-rollout-telemetry",
        "implementation_brief_id": "brief-rollout-telemetry",
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
    tags=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": (
            acceptance_criteria if acceptance_criteria is not None else ["Done"]
        ),
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        task["tags"] = tags
    return task
