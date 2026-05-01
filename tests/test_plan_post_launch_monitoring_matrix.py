import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_post_launch_monitoring_matrix import (
    PlanPostLaunchMonitoringMatrix,
    PlanPostLaunchMonitoringSignal,
    build_plan_post_launch_monitoring_matrix,
    derive_plan_post_launch_monitoring_matrix,
    generate_plan_post_launch_monitoring_matrix,
    plan_post_launch_monitoring_matrix_to_dict,
    plan_post_launch_monitoring_matrix_to_markdown,
    summarize_plan_post_launch_monitoring,
)


def test_monitoring_rows_group_tasks_by_signal_type_in_stable_order():
    result = generate_plan_post_launch_monitoring_matrix(
        _plan(
            [
                _task(
                    "task-rollout",
                    title="Roll out checkout conversion dashboard",
                    description="Launch feature flag wave 1 and track conversion metrics.",
                    acceptance_criteria=["Dashboard monitors checkout conversion and launch metrics."],
                    metadata={
                        "dashboard": "Checkout launch dashboard",
                        "first_check_timing": "15 minutes after wave 1",
                        "owner": "growth DRI",
                        "rollback_trigger": "Rollback if checkout conversion drops 5%.",
                    },
                ),
                _task(
                    "task-alerts",
                    title="Add SLO alerting",
                    description="Create burn rate alert and latency SLO dashboard.",
                    files_or_modules=["ops/alerts/checkout_slo.yml"],
                    metadata={"owner": "SRE on-call"},
                ),
            ]
        )
    )

    assert isinstance(result, PlanPostLaunchMonitoringMatrix)
    assert all(isinstance(row, PlanPostLaunchMonitoringSignal) for row in result.rows)
    assert [row.signal_type for row in result.rows] == [
        "metrics",
        "alerts",
        "dashboards",
        "slo",
        "conversions",
        "rollouts",
    ]
    by_signal = {row.signal_type: row for row in result.rows}
    assert by_signal["metrics"].affected_task_ids == ("task-rollout",)
    assert by_signal["alerts"].affected_task_ids == ("task-alerts",)
    assert by_signal["dashboards"].affected_task_ids == ("task-alerts", "task-rollout")
    assert "Checkout launch dashboard" in by_signal["rollouts"].required_dashboards_or_alerts
    assert by_signal["rollouts"].first_check_timing == "15 minutes after wave 1"
    assert "growth DRI" in by_signal["conversions"].owner_hints
    assert "Rollback if checkout conversion drops 5%." in by_signal[
        "conversions"
    ].rollback_trigger_notes


def test_jobs_integrations_and_migrations_receive_default_monitoring_guidance():
    result = generate_plan_post_launch_monitoring_matrix(
        _plan(
            [
                _task(
                    "task-worker",
                    title="Start invoice worker",
                    description="Run background job queue consumer with retry monitoring.",
                    files_or_modules=["src/workers/invoice_worker.py"],
                ),
                _task(
                    "task-webhook",
                    title="Enable vendor webhook integration",
                    description="Release external provider webhook and watch provider error rate.",
                    files_or_modules=["src/integrations/vendor_webhook.py"],
                ),
                _task(
                    "task-migration",
                    title="Run account schema migration",
                    description="Backfill account records during cutover.",
                    files_or_modules=["db/migrations/20260502_accounts.sql"],
                ),
            ]
        )
    )

    by_signal = {row.signal_type: row for row in result.rows}
    assert by_signal["background_jobs"].affected_task_ids == ("task-worker",)
    assert by_signal["integrations"].affected_task_ids == ("task-webhook",)
    assert by_signal["migrations"].affected_task_ids == ("task-migration",)
    assert by_signal["background_jobs"].required_dashboards_or_alerts == (
        "Queue depth, retry, and worker error alert",
    )
    assert by_signal["integrations"].owner_hints == (
        "integration owner",
        "vendor escalation owner",
    )
    assert by_signal["migrations"].first_check_timing == (
        "After dry-run/live migration start, midpoint, and completion verification."
    )
    assert "database errors" in by_signal["migrations"].rollback_trigger_notes[0]


def test_plan_metadata_can_create_rows_without_task_specific_signal():
    result = generate_plan_post_launch_monitoring_matrix(
        _plan(
            [_task("task-ui", title="Adjust account settings page", description="Update layout.")],
            metadata={
                "monitoring_signals": ["logs", "alerts"],
                "required_alerts": ["Launch watch alert"],
                "watch_window": "First two hours after release",
                "owner": "release captain",
                "rollback_triggers": ["Rollback if support escalations exceed threshold."],
            },
        )
    )

    assert [row.signal_type for row in result.rows] == ["logs", "alerts"]
    assert result.rows[0].affected_task_ids == ()
    assert result.rows[0].required_dashboards_or_alerts == ("Launch watch alert",)
    assert result.rows[0].first_check_timing == "First two hours after release"
    assert result.rows[0].owner_hints == ("release captain",)
    assert result.rows[0].rollback_trigger_notes == (
        "Rollback if support escalations exceed threshold.",
    )


def test_serialization_markdown_aliases_model_input_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Launch payment provider | phase 1",
                description="Release integration with logs, alerts, dashboard, and rollout monitoring.",
                acceptance_criteria=["Rollback trigger is provider errors above 2%."],
                metadata={"required_dashboard": "Payment provider health"},
            )
        ],
        plan_id="plan-monitoring-model",
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = generate_plan_post_launch_monitoring_matrix(model)
    derived = derive_plan_post_launch_monitoring_matrix(result)
    summarized = summarize_plan_post_launch_monitoring(plan)
    payload = plan_post_launch_monitoring_matrix_to_dict(result)
    markdown = plan_post_launch_monitoring_matrix_to_markdown(result)

    assert plan == original
    assert derived is result
    assert summarized.to_dicts() == generate_plan_post_launch_monitoring_matrix(plan).to_dicts()
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["rows"]
    assert result.records == result.rows
    assert list(payload) == ["plan_id", "rows", "summary"]
    assert list(payload["rows"][0]) == [
        "signal_type",
        "affected_task_ids",
        "required_dashboards_or_alerts",
        "first_check_timing",
        "owner_hints",
        "rollback_trigger_notes",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert "Launch payment provider \\| phase 1" in markdown
    assert build_plan_post_launch_monitoring_matrix(plan).summary == result.summary


def test_empty_or_unrelated_plan_returns_noop_matrix():
    result = generate_plan_post_launch_monitoring_matrix(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update README copy",
                    description="Adjust documentation wording only.",
                    files_or_modules=["docs/readme.md"],
                )
            ],
            plan_id="plan-empty",
        )
    )

    assert result.rows == ()
    assert result.summary == {
        "task_count": 1,
        "monitoring_signal_count": 0,
        "covered_task_count": 0,
        "rollback_trigger_count": 0,
        "signal_counts": {
            "metrics": 0,
            "logs": 0,
            "alerts": 0,
            "dashboards": 0,
            "slo": 0,
            "conversions": 0,
            "background_jobs": 0,
            "integrations": 0,
            "migrations": 0,
            "rollouts": 0,
        },
        "covered_task_ids": [],
    }
    assert result.to_markdown() == (
        "# Plan Post-Launch Monitoring Matrix: plan-empty\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Monitoring signal count: 0\n"
        "- Covered task count: 0\n"
        "- Signal counts: metrics 0, logs 0, alerts 0, dashboards 0, slo 0, conversions 0, "
        "background_jobs 0, integrations 0, migrations 0, rollouts 0\n"
        "\n"
        "No post-launch monitoring signals were detected."
    )


def _plan(tasks, *, plan_id="plan-post-launch-monitoring", metadata=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-post-launch-monitoring",
        "milestones": [{"name": "Launch"}],
        "tasks": tasks,
        "metadata": {} if metadata is None else metadata,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    risk_level=None,
    tags=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
        "risk_level": risk_level,
        "metadata": {} if metadata is None else metadata,
        **({} if tags is None else {"tags": tags}),
    }
