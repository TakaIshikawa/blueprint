import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_kill_switch_readiness import (
    PlanKillSwitchReadinessMatrix,
    PlanKillSwitchReadinessRecord,
    build_plan_kill_switch_readiness_matrix,
    derive_plan_kill_switch_readiness_matrix,
    plan_kill_switch_readiness_matrix_to_dict,
    plan_kill_switch_readiness_matrix_to_markdown,
    summarize_plan_kill_switch_readiness,
)


def test_launch_integration_and_migration_tasks_are_detected_with_expected_surfaces():
    result = build_plan_kill_switch_readiness_matrix(
        _plan(
            [
                _task(
                    "task-launch",
                    title="Launch public checkout feature",
                    description="Roll out a customer-facing checkout release to all users.",
                    files_or_modules=["src/payments/checkout.py"],
                    acceptance_criteria=["Checkout succeeds for eligible customers."],
                ),
                _task(
                    "task-integration",
                    title="Enable vendor webhook integration",
                    description="Connect external provider webhook for account updates.",
                    files_or_modules=["src/integrations/vendor_webhook.py"],
                    metadata={"owner": "integrations DRI"},
                ),
                _task(
                    "task-migration",
                    title="Run account schema migration",
                    description="Backfill account records using a write migration.",
                    files_or_modules=["db/migrations/20260501_accounts.sql"],
                    metadata={"owner": "database DRI"},
                ),
            ]
        )
    )

    assert isinstance(result, PlanKillSwitchReadinessMatrix)
    by_id = {record.task_id: record for record in result.records}
    assert by_id["task-launch"].kill_switch_surface == "feature_launch"
    assert by_id["task-integration"].kill_switch_surface == "external_integration"
    assert by_id["task-migration"].kill_switch_surface == "data_migration"
    assert all(isinstance(record, PlanKillSwitchReadinessRecord) for record in result.records)
    assert "Provide a dependency disable, bypass, or fallback path." in by_id[
        "task-integration"
    ].missing_acceptance_criteria
    assert "Provide a data-write pause or read-only mode for affected writes." in by_id[
        "task-migration"
    ].missing_acceptance_criteria
    assert "files_or_modules: db/migrations/20260501_accounts.sql" in by_id[
        "task-migration"
    ].evidence


def test_acceptance_criteria_recognize_all_containment_safeguards_and_lower_risk():
    record = build_plan_kill_switch_readiness_matrix(
        _task(
            "task-covered",
            title="Roll out billing integration",
            description="Launch a third-party billing integration for production traffic.",
            acceptance_criteria=[
                "Feature flag and config toggle can disable the integration immediately.",
                "Canary traffic split supports ramp-down to zero percent.",
                "Rollback command is documented and dry-run before launch.",
                "Pause writes by switching billing updates to read-only mode.",
                "Circuit breaker fallback bypasses the provider when disabled.",
                "Smoke test, health check, dashboard, and customer impact verification run after rollback.",
            ],
            metadata={"approver": "release manager"},
        )
    ).records[0]

    assert record.risk_level == "low"
    assert record.missing_acceptance_criteria == ()
    assert record.containment_options == (
        "Feature flag or config toggle",
        "Traffic/ramp-down control",
        "Rollback command or revert procedure",
        "Pause writes or switch to read-only mode",
        "Disable, bypass, or fall back from the dependency",
        "Post-disable verification",
    )
    assert record.owner_approval_hints == ("release manager",)


def test_high_risk_broad_user_facing_task_escalates_without_containment_path():
    record = build_plan_kill_switch_readiness_matrix(
        _task(
            "task-global",
            title="Global account permissions launch",
            description="Enable public user-facing permissions changes for all customers.",
            risk_level="high",
            acceptance_criteria=["Permissions render in the account page."],
        )
    ).records[0]

    assert record.kill_switch_surface == "feature_launch"
    assert record.risk_level == "high"
    assert record.containment_options == ()
    assert "Add a feature flag or config toggle that can disable the change." in record.missing_acceptance_criteria
    assert "Block autonomous execution" in record.verification_steps[-1]


def test_deterministic_ordering_summary_markdown_and_json_serialization():
    plan = _plan(
        [
            _task(
                "task-z-worker",
                title="Start invoice worker queue",
                description="Launch background job worker for queued invoices.",
                acceptance_criteria=["Pause queue and stop worker commands are ready."],
            ),
            _task(
                "task-a-integration",
                title="External payment provider rollout | phase 1",
                description="Release payment integration to production traffic.",
                acceptance_criteria=["Fallback bypass and disable provider runbook is approved."],
            ),
        ],
        plan_id="plan-kill-switch-order",
    )
    result = summarize_plan_kill_switch_readiness(plan)
    payload = plan_kill_switch_readiness_matrix_to_dict(result)
    markdown = plan_kill_switch_readiness_matrix_to_markdown(result)

    assert [record.task_id for record in result.records] == [
        "task-a-integration",
        "task-z-worker",
    ]
    assert result.summary == {
        "task_count": 2,
        "record_count": 2,
        "ready_task_count": 0,
        "at_risk_task_count": 2,
        "no_signal_task_count": 0,
        "missing_acceptance_criteria_count": 10,
        "risk_counts": {"high": 2, "medium": 0, "low": 0},
        "surface_counts": {
            "feature_launch": 0,
            "data_migration": 0,
            "external_integration": 1,
            "background_job": 1,
            "payment_or_account_flow": 0,
            "broad_user_facing_change": 0,
            "high_risk_change": 0,
        },
        "ready_task_ids": [],
        "at_risk_task_ids": ["task-a-integration", "task-z-worker"],
        "no_signal_task_ids": [],
    }
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "records",
        "ready_task_ids",
        "at_risk_task_ids",
        "no_signal_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "kill_switch_surface",
        "containment_options",
        "missing_acceptance_criteria",
        "owner_approval_hints",
        "verification_steps",
        "risk_level",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert "External payment provider rollout \\| phase 1" in markdown


def test_pydantic_model_input_aliases_and_no_mutation():
    task = _task(
        "task-model",
        title="Launch account webhook",
        description="Release account integration for production traffic.",
        acceptance_criteria=["Feature flag, rollback, fallback, and health check are documented."],
    )
    original = copy.deepcopy(task)
    plan = ExecutionPlan.model_validate(_plan([task], plan_id="plan-model"))
    task_model = ExecutionTask.model_validate(task)

    result = build_plan_kill_switch_readiness_matrix(plan)
    direct = summarize_plan_kill_switch_readiness(task_model)
    derived = derive_plan_kill_switch_readiness_matrix(result)

    assert task == original
    assert derived is result
    assert direct.records[0].task_id == "task-model"
    assert result.to_dicts() == result.to_dict()["records"]
    assert result.findings == result.records


def test_low_risk_documentation_task_returns_noop_output():
    result = build_plan_kill_switch_readiness_matrix(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update rollout documentation copy",
                    description="Adjust README wording and labels only.",
                    files_or_modules=["docs/rollout.md"],
                )
            ],
            plan_id="plan-empty",
        )
    )

    assert result.records == ()
    assert result.ready_task_ids == ()
    assert result.at_risk_task_ids == ()
    assert result.no_signal_task_ids == ("task-docs",)
    assert result.summary == {
        "task_count": 1,
        "record_count": 0,
        "ready_task_count": 0,
        "at_risk_task_count": 0,
        "no_signal_task_count": 1,
        "missing_acceptance_criteria_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "surface_counts": {
            "feature_launch": 0,
            "data_migration": 0,
            "background_job": 0,
            "external_integration": 0,
            "payment_or_account_flow": 0,
            "broad_user_facing_change": 0,
            "high_risk_change": 0,
        },
        "ready_task_ids": [],
        "at_risk_task_ids": [],
        "no_signal_task_ids": ["task-docs"],
    }
    assert result.to_markdown() == (
        "# Plan Kill Switch Readiness Matrix: plan-empty\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Readiness record count: 0\n"
        "- At-risk task count: 0\n"
        "- Missing acceptance criteria count: 0\n"
        "- Risk counts: high 0, medium 0, low 0\n"
        "\n"
        "No kill-switch readiness signals were detected."
    )


def _plan(tasks, *, plan_id="plan-kill-switch", metadata=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-kill-switch",
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
