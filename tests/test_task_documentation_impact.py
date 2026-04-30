import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_documentation_impact import (
    build_task_documentation_impact_plan,
    task_documentation_impact_plan_to_dict,
    task_documentation_impact_plan_to_markdown,
)


def test_infers_documentation_targets_for_core_task_surfaces():
    result = build_task_documentation_impact_plan(
        _plan(
            [
                _task(
                    "task-api",
                    title="Add profile API endpoint",
                    description="Create a REST route with a new response field.",
                    files_or_modules=["src/api/profile.py"],
                    acceptance_criteria=["Endpoint returns active profile fields."],
                ),
                _task(
                    "task-config",
                    title="Add export feature flag",
                    description="Read a new environment variable from configuration.",
                    files_or_modules=["src/config/export.yaml"],
                    acceptance_criteria=["Flag defaults are tested."],
                ),
                _task(
                    "task-migration",
                    title="Backfill account status",
                    description="Run a database migration with rollback verification.",
                    files_or_modules=["migrations/versions/add_account_status.sql"],
                    acceptance_criteria=["Existing rows receive the default status."],
                ),
                _task(
                    "task-cli",
                    title="Add import CLI command",
                    description="Expose a command line dry-run option.",
                    files_or_modules=["src/blueprint/cli/imports.py"],
                    acceptance_criteria=["CLI exits zero for dry-run imports."],
                ),
                _task(
                    "task-ui",
                    title="Update checkout screen",
                    description="Change the browser form and confirmation copy.",
                    files_or_modules=["frontend/components/CheckoutFlow.tsx"],
                    acceptance_criteria=["Users can complete checkout from the updated screen."],
                ),
                _task(
                    "task-ops",
                    title="Deploy webhook retry worker",
                    description="Add a worker queue and alert for provider timeouts.",
                    files_or_modules=["ops/runbooks/webhook-retry.md", "src/workers/webhooks.py"],
                    acceptance_criteria=["Timeout retries are visible after deployment."],
                    risk_level="high",
                ),
            ]
        )
    )

    assert _targets(result, "task-api") == {
        "README": "optional",
        "api_docs": "required",
        "changelog_release_notes": "required",
        "troubleshooting_docs": "optional",
    }
    assert _targets(result, "task-config") == {
        "README": "optional",
        "changelog_release_notes": "required",
        "configuration_docs": "required",
        "troubleshooting_docs": "optional",
    }
    assert _targets(result, "task-migration") == {
        "README": "optional",
        "migration_notes": "required",
        "runbooks": "required",
        "changelog_release_notes": "required",
        "troubleshooting_docs": "required",
    }
    assert _targets(result, "task-cli") == {
        "README": "optional",
        "user_facing_docs": "required",
        "changelog_release_notes": "required",
        "troubleshooting_docs": "optional",
    }
    assert _targets(result, "task-ui") == {
        "user_facing_docs": "required",
        "changelog_release_notes": "required",
        "troubleshooting_docs": "optional",
    }
    assert _targets(result, "task-ops") == {
        "runbooks": "required",
        "changelog_release_notes": "required",
        "troubleshooting_docs": "required",
    }
    assert result.required_task_ids == (
        "task-api",
        "task-config",
        "task-migration",
        "task-cli",
        "task-ui",
        "task-ops",
    )


def test_required_optional_recommendations_and_evidence_are_deterministic():
    result = build_task_documentation_impact_plan(
        [
            _task(
                "task-mixed",
                title="Add search API and UI",
                description="New endpoint powers a browser page with troubleshooting for failures.",
                files_or_modules=["src/api/search.py", "src/pages/Search.tsx"],
                acceptance_criteria=[
                    "API documentation is updated for the search response contract.",
                    "User-facing documentation is updated for the new search page.",
                ],
            )
        ]
    )
    impact = result.task_impacts[0]

    assert [target.doc_target for target in impact.doc_targets] == [
        "README",
        "api_docs",
        "user_facing_docs",
        "changelog_release_notes",
        "troubleshooting_docs",
    ]
    assert _targets(result, "task-mixed") == {
        "README": "optional",
        "api_docs": "required",
        "user_facing_docs": "required",
        "changelog_release_notes": "required",
        "troubleshooting_docs": "optional",
    }
    assert _target(impact, "api_docs").evidence == (
        "files_or_modules: src/api/search.py",
        "title: Add search API and UI",
        "description: New endpoint powers a browser page with troubleshooting for failures.",
        "acceptance_criteria[0]: API documentation is updated for the search response contract.",
    )
    assert _target(impact, "user_facing_docs").evidence == (
        "files_or_modules: src/pages/Search.tsx",
        "title: Add search API and UI",
        "description: New endpoint powers a browser page with troubleshooting for failures.",
        "acceptance_criteria[1]: User-facing documentation is updated for the new search page.",
    )
    assert impact.suggested_acceptance_criteria == (
        "Changelog or release notes summarize the user-visible or operator-visible change.",
    )


def test_suggested_acceptance_criteria_are_generated_for_missing_required_docs_only():
    result = build_task_documentation_impact_plan(
        [
            _task(
                "task-config",
                title="Add billing env var",
                description="Configuration adds BILLING_API_URL and updates release notes.",
                files_or_modules=["config/billing.yaml"],
                acceptance_criteria=[
                    "Changelog entry describes the billing configuration change.",
                ],
            )
        ]
    )

    assert _impact(result, "task-config").suggested_acceptance_criteria == (
        "Configuration documentation lists new or changed settings, defaults, and required environment variables.",
    )


def test_model_and_dict_inputs_are_supported_without_mutation_and_serialize_stably():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Refresh admin report",
                description="Render the admin UI report from persisted data.",
                files_or_modules=["src/pages/AdminReport.tsx"],
                metadata={"documentation_note": "Update user docs for report filters."},
            )
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_task_documentation_impact_plan(model)
    payload = task_documentation_impact_plan_to_dict(result)
    markdown = task_documentation_impact_plan_to_markdown(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["task_impacts"]
    assert list(payload) == ["plan_id", "task_impacts", "required_task_ids"]
    assert list(payload["task_impacts"][0]) == [
        "task_id",
        "title",
        "doc_targets",
        "suggested_acceptance_criteria",
    ]
    assert list(payload["task_impacts"][0]["doc_targets"][0]) == [
        "doc_target",
        "status",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert markdown.startswith("# Task Documentation Impact Plan: plan-docs")
    assert "| `task-model` | user_facing_docs | required |" in markdown


def test_single_task_model_and_empty_plan_are_supported():
    task = ExecutionTask.model_validate(
        _task(
            "task-single",
            title="Update release copy",
            description="Update non-functional copy only.",
            acceptance_criteria=["Done"],
        )
    )

    single = build_task_documentation_impact_plan(task)
    empty = build_task_documentation_impact_plan(_plan([]))

    assert single.plan_id is None
    assert single.task_impacts[0].task_id == "task-single"
    assert single.task_impacts[0].doc_targets == ()
    assert single.required_task_ids == ()
    assert empty.to_markdown() == (
        "# Task Documentation Impact Plan: plan-docs\n\nNo documentation impacts were derived."
    )


def _impact(result, task_id):
    return next(impact for impact in result.task_impacts if impact.task_id == task_id)


def _targets(result, task_id):
    return {
        target.doc_target: target.status
        for target in _impact(result, task_id).doc_targets
    }


def _target(impact, doc_target):
    return next(target for target in impact.doc_targets if target.doc_target == doc_target)


def _plan(tasks):
    return {
        "id": "plan-docs",
        "implementation_brief_id": "brief-docs",
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
    test_command=None,
    risk_level=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if test_command is not None:
        task["test_command"] = test_command
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
