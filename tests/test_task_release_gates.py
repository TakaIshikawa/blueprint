import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_release_gates import (
    TaskReleaseGatePlan,
    build_task_release_gate_plan,
    recommend_task_release_gates,
    task_release_gate_plan_to_dict,
    task_release_gate_plan_to_markdown,
)


def test_schema_migration_permission_integration_export_and_rollout_gates():
    result = build_task_release_gate_plan(
        _plan(
            [
                _task(
                    "task-release-impact",
                    title="Add schema migration for permissioned integration export",
                    description=(
                        "Change OpenAPI response schema, apply an account migration, update "
                        "RBAC permissions, call a Slack integration, and add a CSV export "
                        "behind a feature flag."
                    ),
                    files_or_modules=[
                        "src/schemas/account.avsc",
                        "migrations/20260501_account.sql",
                        "src/auth/permissions.py",
                        "src/integrations/slack.py",
                        "src/exporters/account_exporter.py",
                        "src/flags/account_export.py",
                    ],
                    acceptance_criteria=[
                        "Schema diff, migration output, permission tests, sandbox integration, "
                        "golden export fixture, and flag disable procedure are attached.",
                    ],
                )
            ]
        )
    )

    gates = {item.gate: item for item in result.gates}

    assert list(gates) == [
        "schema_change",
        "migration",
        "permission_change",
        "integration_change",
        "data_export",
        "rollout_flag",
    ]
    assert gates["schema_change"].verifier_role == "contract owner"
    assert "Contract fixture" in gates["schema_change"].required_evidence[0]
    assert gates["migration"].verifier_role == "data owner"
    assert "Migration apply output" in gates["migration"].required_evidence[0]
    assert gates["permission_change"].verifier_role == "security owner"
    assert "authorization tests" in gates["permission_change"].required_evidence[1]
    assert gates["integration_change"].verifier_role == "integration owner"
    assert "sandbox" in gates["integration_change"].required_evidence[0]
    assert gates["data_export"].verifier_role == "data consumer owner"
    assert "Golden export fixture" in gates["data_export"].required_evidence[0]
    assert gates["rollout_flag"].verifier_role == "release owner"
    assert "default state" in gates["rollout_flag"].required_evidence[0]
    assert result.release_gated_task_ids == ("task-release-impact",)
    assert result.low_impact_task_ids == ()


def test_monitoring_rollback_documentation_and_user_visible_gates_from_metadata():
    result = build_task_release_gate_plan(
        _plan(
            [
                _task(
                    "task-ops-ui",
                    title="Update dashboard behavior with release handoff",
                    description="Change customer-visible dashboard workflow.",
                    files_or_modules=[
                        "src/ui/dashboard.py",
                        "docs/runbooks/dashboard_release.md",
                    ],
                    metadata={
                        "release_gate_notes": [
                            "Monitoring dashboard and alerts cover latency.",
                            "Rollback trigger is elevated error rate; disable the flag.",
                            "Support handoff acknowledges the runbook.",
                        ]
                    },
                )
            ]
        )
    )

    gates = {item.gate: item for item in result.gates}

    assert list(gates) == [
        "user_visible_behavior",
        "rollout_flag",
        "monitoring",
        "rollback",
        "documentation_handoff",
    ]
    assert gates["user_visible_behavior"].verifier_role == "QA owner"
    assert "Acceptance evidence" in gates["user_visible_behavior"].required_evidence[0]
    assert gates["monitoring"].verifier_role == "on-call owner"
    assert "Post-release observation window" in gates["monitoring"].required_evidence[1]
    assert gates["rollback"].verifier_role == "release owner"
    assert "Rollback trigger condition" in gates["rollback"].required_evidence[0]
    assert gates["documentation_handoff"].verifier_role == "support owner"
    assert "handoff document updated" in gates["documentation_handoff"].required_evidence[0]
    assert any("metadata.release_gate_notes" in item for item in gates["rollback"].evidence)


def test_low_impact_tasks_return_empty_gate_set_and_summary_counts():
    result = build_task_release_gate_plan(
        _plan(
            [
                _task(
                    "task-internal",
                    title="Refactor internal helper",
                    description="Simplify pure helper naming for maintainability.",
                    files_or_modules=["src/blueprint/internal_helpers.py"],
                    acceptance_criteria=["Unit tests still pass."],
                )
            ]
        )
    )

    assert result.gates == ()
    assert result.release_gated_task_ids == ()
    assert result.low_impact_task_ids == ("task-internal",)
    assert result.summary == {
        "task_count": 1,
        "release_gated_task_count": 0,
        "gate_count": 0,
        "gate_counts": {
            "schema_change": 0,
            "migration": 0,
            "user_visible_behavior": 0,
            "permission_change": 0,
            "integration_change": 0,
            "data_export": 0,
            "rollout_flag": 0,
            "monitoring": 0,
            "rollback": 0,
            "documentation_handoff": 0,
        },
    }


def test_execution_plan_input_serializes_without_mutation_and_schema_is_stable():
    plan = _plan(
        [
            _task(
                "task-schema",
                title="Change public schema",
                description="Update JSON schema field and response shape.",
                files_or_modules=["src/contracts/public.schema.json"],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_release_gate_plan(ExecutionPlan.model_validate(plan))
    payload = task_release_gate_plan_to_dict(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["gates"]
    assert list(payload) == [
        "plan_id",
        "gates",
        "release_gated_task_ids",
        "low_impact_task_ids",
        "summary",
    ]
    assert list(payload["gates"][0]) == [
        "task_id",
        "title",
        "gate",
        "reason",
        "required_evidence",
        "verifier_role",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_iterable_task_input_single_task_alias_and_invalid_plan_handling():
    iterable = build_task_release_gate_plan(
        [
            _task(
                "task-report",
                title="Change report export",
                description="Update CSV data export columns.",
                files_or_modules=["src/reports/export.py"],
            ),
            _task("task-copyedit", title="Rename internal variable"),
        ]
    )
    task = ExecutionTask.model_validate(
        _task(
            "task-permission",
            title="Tighten admin permission",
            description="Change access control policy for admin roles.",
            files_or_modules=["src/policies/admin.py"],
        )
    )
    single = recommend_task_release_gates(task)
    invalid = build_task_release_gate_plan({"id": "plan-invalid", "tasks": "nope"})

    assert iterable.plan_id is None
    assert iterable.release_gated_task_ids == ("task-report",)
    assert iterable.low_impact_task_ids == ("task-copyedit",)
    assert isinstance(single, TaskReleaseGatePlan)
    assert single.plan_id is None
    assert single.gates[0].gate == "permission_change"
    assert invalid.gates == ()
    assert invalid.summary["task_count"] == 0


def test_markdown_formatting_and_empty_markdown():
    result = build_task_release_gate_plan(
        _plan(
            [
                _task(
                    "task-migration",
                    title="Add account migration",
                    description="Run database migration and document rollback.",
                    files_or_modules=["migrations/20260501_account.sql"],
                )
            ]
        )
    )
    markdown = task_release_gate_plan_to_markdown(result)
    empty = build_task_release_gate_plan({"id": "plan-empty", "tasks": []})

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Release Gates: plan-release")
    assert (
        "| Task | Gate | Reason | Required Evidence | Verifier Role |\n"
        "| --- | --- | --- | --- | --- |"
    ) in markdown
    assert "| `task-migration` | migration | Migration or backfill work" in markdown
    assert "Migration apply output" in markdown
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Release Gates: plan-empty",
            "",
            "No task-level release gates were derived.",
        ]
    )


def _plan(tasks):
    return {
        "id": "plan-release",
        "implementation_brief_id": "brief-release",
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
    metadata=None,
    tags=None,
    test_command=None,
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
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    if test_command is not None:
        task["test_command"] = test_command
    return task
