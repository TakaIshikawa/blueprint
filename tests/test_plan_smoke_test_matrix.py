import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_smoke_test_matrix import (
    PlanSmokeTestMatrixRow,
    build_plan_smoke_test_matrix,
    plan_smoke_test_matrix_to_dict,
)


def test_mixed_plan_builds_deterministic_compact_smoke_matrix():
    result = build_plan_smoke_test_matrix(
        _plan(
            [
                _task(
                    "task-ui",
                    title="Update checkout flow",
                    description="Change the browser form and confirmation screen.",
                    files_or_modules=["frontend/components/CheckoutFlow.tsx"],
                    acceptance_criteria=["Users can complete checkout from the updated screen."],
                    test_command="pnpm test -- CheckoutFlow.test.tsx",
                ),
                _task(
                    "task-api",
                    title="Add profile API endpoint",
                    description="Create a backend route that returns profile responses.",
                    files_or_modules=["src/api/profile.py"],
                    acceptance_criteria=["Endpoint returns active profile fields."],
                    test_command="poetry run pytest tests/test_profile_api.py",
                ),
                _task(
                    "task-data",
                    title="Backfill account status",
                    description="Run a database migration and verify row counts.",
                    files_or_modules=["migrations/versions/add_account_status.sql"],
                    acceptance_criteria=["Existing rows receive the default status."],
                    test_command="poetry run pytest tests/test_account_status_migration.py",
                    risk_level="high",
                ),
                _task(
                    "task-integration",
                    title="Sync CRM provider records",
                    description="Call an external Salesforce API client and handle retries.",
                    files_or_modules=["src/integrations/salesforce/client.py"],
                    acceptance_criteria=["Provider timeout retries are visible."],
                    test_command="poetry run pytest tests/test_salesforce_sync.py",
                ),
                _task(
                    "task-cli",
                    title="Add import CLI command",
                    description="Expose a command line flag for dry-run imports.",
                    files_or_modules=["src/blueprint/cli/imports.py"],
                    acceptance_criteria=["CLI exits zero for dry-run imports."],
                    test_command="poetry run pytest tests/test_import_cli.py",
                ),
            ],
            test_strategy="Run poetry run pytest",
        )
    )

    assert [row.area for row in result.rows] == [
        "user_flow",
        "api_backend",
        "data",
        "integration",
        "cli",
        "regression",
    ]
    assert _row(result, "user_flow") == PlanSmokeTestMatrixRow(
        area="user_flow",
        name="User Flow",
        covered_task_ids=("task-ui",),
        priority="low",
        rationale=(
            "Smoke the primary user-visible path across 1 task. "
            "Signal: title: Update checkout flow."
        ),
        suggested_command="pnpm test -- CheckoutFlow.test.tsx",
    )
    assert _row(result, "api_backend").covered_task_ids == ("task-api",)
    assert _row(result, "api_backend").suggested_command == (
        "poetry run pytest tests/test_profile_api.py"
    )
    assert _row(result, "data").priority == "high"
    assert _row(result, "integration").covered_task_ids == ("task-integration",)
    assert _row(result, "cli").suggested_command == "poetry run pytest tests/test_import_cli.py"
    assert _row(result, "regression").covered_task_ids == (
        "task-ui",
        "task-api",
        "task-data",
        "task-integration",
        "task-cli",
    )
    assert _row(result, "regression").priority == "high"
    assert _row(result, "regression").suggested_command == "poetry run pytest"


def test_plan_level_commands_are_not_duplicated_as_task_level_commands():
    result = build_plan_smoke_test_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    title="Add search API",
                    description="Backend route for search.",
                    files_or_modules=["src/api/search.py"],
                    test_command="poetry run pytest",
                ),
                _task(
                    "task-ui",
                    title="Add search screen",
                    description="Frontend page for search.",
                    files_or_modules=["src/pages/Search.tsx"],
                    test_command="pnpm test -- Search.test.tsx",
                ),
            ],
            test_strategy="poetry run pytest",
            metadata={
                "validation_commands": {
                    "test": ["poetry run pytest"],
                    "lint": ["poetry run ruff check"],
                }
            },
        )
    )

    assert _row(result, "api_backend").suggested_command is None
    assert _row(result, "user_flow").suggested_command == "pnpm test -- Search.test.tsx"
    assert _row(result, "regression").suggested_command == "poetry run pytest"
    assert [row.suggested_command for row in result.rows].count("poetry run pytest") == 1


def test_model_and_dict_inputs_are_supported_without_mutation_and_serialize_stably():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Refresh admin report",
                description="Render the admin UI report from persisted data.",
                files_or_modules=["src/pages/AdminReport.tsx", "src/store/reports.py"],
                metadata={"test_commands": ["pnpm test -- AdminReport.test.tsx"]},
            )
        ],
        metadata={"validation_command": "poetry run pytest"},
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_plan_smoke_test_matrix(model)
    payload = plan_smoke_test_matrix_to_dict(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert list(payload) == ["plan_id", "rows"]
    assert list(payload["rows"][0]) == [
        "area",
        "name",
        "covered_task_ids",
        "priority",
        "rationale",
        "suggested_command",
    ]
    assert json.loads(json.dumps(payload)) == payload
    markdown = result.to_markdown()
    assert markdown.startswith("# Plan Smoke Test Matrix: plan-smoke")
    assert "| User Flow | low | task-model | `pnpm test -- AdminReport.test.tsx` |" in markdown
    assert "| Regression | medium | task-model | `poetry run pytest` |" in markdown


def test_empty_plan_returns_empty_serializable_matrix():
    result = build_plan_smoke_test_matrix(_plan([]))

    assert result.plan_id == "plan-smoke"
    assert result.rows == ()
    assert result.to_dicts() == []
    assert result.to_markdown() == (
        "# Plan Smoke Test Matrix: plan-smoke\n\nNo smoke tests were derived."
    )


def _row(result, area):
    return next(row for row in result.rows if row.area == area)


def _plan(tasks, *, test_strategy=None, metadata=None):
    plan = {
        "id": "plan-smoke",
        "implementation_brief_id": "brief-smoke",
        "milestones": [],
        "tasks": tasks,
    }
    if test_strategy is not None:
        plan["test_strategy"] = test_strategy
    if metadata is not None:
        plan["metadata"] = metadata
    return plan


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
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if test_command is not None:
        task["test_command"] = test_command
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
