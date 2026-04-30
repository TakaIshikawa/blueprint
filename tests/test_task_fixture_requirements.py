import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_fixture_requirements import (
    FixtureRequirement,
    TaskFixturePlan,
    build_task_fixture_requirements,
    task_fixture_requirements_to_dict,
)


def test_database_auth_external_and_cleanup_requirements_are_inferred():
    result = build_task_fixture_requirements(
        _plan(
            [
                _task(
                    "task-audit",
                    title="Add admin audit export",
                    description=(
                        "Persist audit records for admin users and call the external webhook "
                        "when exports complete."
                    ),
                    files_or_modules=[
                        "src/blueprint/store/audit_repository.py",
                        "src/blueprint/auth/admin.py",
                        "src/blueprint/integrations/webhooks.py",
                    ],
                    acceptance_criteria=[
                        "Seed two admin users and database rows before running validation.",
                        "Reset webhook queues after each test run.",
                    ],
                    test_command="poetry run pytest tests/test_audit_export.py",
                )
            ]
        )
    )

    plan = result.plans[0]

    assert isinstance(plan, TaskFixturePlan)
    assert plan.task_id == "task-audit"
    assert _categories(plan) == (
        "auth_users",
        "database_seed_data",
        "external_service_mocks",
        "cleanup_reset",
    )
    assert _requirement(plan, "auth_users") == FixtureRequirement(
        category="auth_users",
        requirement=(
            "Prepare users, roles, permissions, sessions, or tokens needed to validate access paths."
        ),
        evidence=(
            "files_or_modules: src/blueprint/auth/admin.py",
            "title: Add admin audit export",
            "description: Persist audit records for admin users and call the external webhook when exports complete.",
            "acceptance_criteria[0]: Seed two admin users and database rows before running validation.",
        ),
        setup_hints=(
            "Name required personas and privilege levels.",
            "Keep credentials or tokens test-scoped.",
        ),
    )
    assert _requirement(plan, "database_seed_data").evidence == (
        "files_or_modules: src/blueprint/store/audit_repository.py",
        "description: Persist audit records for admin users and call the external webhook when exports complete.",
        "acceptance_criteria[0]: Seed two admin users and database rows before running validation.",
    )
    assert _requirement(plan, "external_service_mocks").evidence == (
        "files_or_modules: src/blueprint/integrations/webhooks.py",
        "description: Persist audit records for admin users and call the external webhook when exports complete.",
        "acceptance_criteria[1]: Reset webhook queues after each test run.",
    )
    assert _requirement(plan, "cleanup_reset").evidence == (
        "acceptance_criteria[1]: Reset webhook queues after each test run.",
    )
    assert result.requirement_counts_by_category == {
        "auth_users": 1,
        "database_seed_data": 1,
        "external_service_mocks": 1,
        "cleanup_reset": 1,
    }


def test_file_fixtures_and_migration_snapshots_from_paths_text_and_metadata():
    result = build_task_fixture_requirements(
        _plan(
            [
                _task(
                    "task-import",
                    title="Import region catalog",
                    description="Read a CSV fixture and apply an Alembic migration snapshot.",
                    files_or_modules=[
                        "tests/fixtures/regions.csv",
                        "migrations/versions/20260501_regions.py",
                    ],
                    acceptance_criteria=[
                        "Rollback uses before and after schema snapshots.",
                    ],
                    metadata={
                        "fixture_files": ["tests/fixtures/regions.csv"],
                        "migration_snapshot": "artifacts/schema-before.json",
                    },
                )
            ]
        )
    )

    plan = result.plans[0]

    assert _categories(plan) == (
        "database_seed_data",
        "file_fixtures",
        "migration_snapshots",
    )
    assert _requirement(plan, "file_fixtures").evidence == (
        "files_or_modules: tests/fixtures/regions.csv",
        "description: Read a CSV fixture and apply an Alembic migration snapshot.",
        "metadata.fixture_files[0]: tests/fixtures/regions.csv",
        "metadata.migration_snapshot: artifacts/schema-before.json",
    )
    assert _requirement(plan, "migration_snapshots").evidence == (
        "files_or_modules: migrations/versions/20260501_regions.py",
        "description: Read a CSV fixture and apply an Alembic migration snapshot.",
        "acceptance_criteria[0]: Rollback uses before and after schema snapshots.",
        "metadata.migration_snapshot: artifacts/schema-before.json",
    )


def test_execution_plan_and_task_models_are_accepted_and_serialize_stably():
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Mock billing API for account roles",
            description="Stub the external billing API for account role checks.",
            files_or_modules=["src/blueprint/services/billing_client.py"],
            acceptance_criteria=["Admin account validation uses mocked API responses."],
            metadata={"mock_responses": ["billing-success.json"]},
        )
    )
    plan_model = ExecutionPlan.model_validate(
        {
            "id": "plan-fixtures-model",
            "implementation_brief_id": "brief-fixtures",
            "milestones": [],
            "tasks": [task_model.model_dump(mode="python")],
        }
    )

    result = build_task_fixture_requirements(plan_model)
    payload = task_fixture_requirements_to_dict(result)

    assert payload == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "task_count",
        "plans",
        "requirement_counts_by_category",
    ]
    assert list(payload["plans"][0]) == ["task_id", "title", "requirements"]
    assert list(payload["plans"][0]["requirements"][0]) == [
        "category",
        "requirement",
        "evidence",
        "setup_hints",
    ]
    assert result.plan_id == "plan-fixtures-model"
    assert result.task_count == 1
    assert _categories(result.plans[0]) == (
        "auth_users",
        "file_fixtures",
        "external_service_mocks",
    )
    assert json.loads(json.dumps(payload)) == payload


def test_markdown_representation_is_stable_for_empty_and_populated_tasks():
    result = build_task_fixture_requirements(
        [
            _task(
                "task-empty",
                title="Tighten button spacing",
                files_or_modules=["src/components/Button.tsx"],
                acceptance_criteria=["Spacing matches the design."],
            ),
            _task(
                "task-file",
                title="Validate uploaded PDF fixture",
                description="Use a sample PDF file fixture.",
                files_or_modules=["tests/fixtures/sample.pdf"],
                acceptance_criteria=["Temporary upload file is removed during cleanup."],
            ),
        ]
    )

    assert result.to_markdown() == (
        "# Task Fixture Requirements\n\n"
        "## task-empty - Tighten button spacing\n"
        "- No fixture requirements inferred.\n\n"
        "## task-file - Validate uploaded PDF fixture\n"
        "- **File fixtures**: Prepare stable files, uploads, exports, samples, or fixture directories.\n"
        "  - Setup: Store fixture files under a deterministic test path.; Document required file names and formats.\n"
        "  - Evidence: files_or_modules: tests/fixtures/sample.pdf; title: Validate uploaded PDF fixture; "
        "description: Use a sample PDF file fixture.; acceptance_criteria[0]: Temporary upload file is removed during cleanup.\n"
        "- **Cleanup/reset**: Define cleanup, reset, teardown, or isolation steps for generated validation state.\n"
        "  - Setup: Reset persisted state between validation runs.; Remove temporary files, queues, or cached records.\n"
        "  - Evidence: acceptance_criteria[0]: Temporary upload file is removed during cleanup."
    )


def test_inputs_are_not_mutated():
    source = _plan(
        [
            _task(
                "task-mutable",
                title="Seed users",
                acceptance_criteria=["Seed admin users."],
                metadata={"fixture_files": ["tests/fixtures/users.json"]},
            )
        ]
    )
    original = copy.deepcopy(source)

    build_task_fixture_requirements(source)

    assert source == original


def _requirement(plan, category):
    return next(
        requirement for requirement in plan.requirements if requirement.category == category
    )


def _categories(plan):
    return tuple(requirement.category for requirement in plan.requirements)


def _plan(tasks):
    return {
        "id": "plan-fixtures",
        "implementation_brief_id": "brief-fixtures",
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
    if metadata is not None:
        task["metadata"] = metadata
    return task
