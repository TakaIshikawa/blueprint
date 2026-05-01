from copy import deepcopy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_dependency_fixtures import (
    TaskDependencyFixtureRequirement,
    infer_task_dependency_fixtures,
    task_dependency_fixtures_to_dict,
)


def test_api_contract_fixture_is_inferred_from_acceptance_criteria_and_paths():
    advice = infer_task_dependency_fixtures(
        _plan(
            [
                _task(
                    "task-api",
                    acceptance_criteria=[
                        "Validate request schema and response schema against the OpenAPI contract."
                    ],
                    files=["src/api/routes.py", "openapi/billing.yaml"],
                )
            ]
        )
    )

    assert len(advice.requirements) == 1
    requirement = advice.requirements[0]
    assert requirement.fixture_type == "api_contract"
    assert requirement.likely_location == "tests/fixtures/api/task-api.json"
    assert requirement.consuming_task_ids == ("task-api",)
    assert requirement.confidence >= 0.8
    assert "API contracts or schemas" in requirement.rationale


def test_database_seed_fixture_is_inferred_from_metadata():
    advice = infer_task_dependency_fixtures(
        _plan(
            [
                _task(
                    "task-billing",
                    metadata={"seed_records": ["customers", "invoices"]},
                    files=["src/billing/models.py"],
                )
            ]
        )
    )

    assert advice.requirements[0].to_dict() == {
        "fixture_type": "database_seed",
        "likely_location": "tests/fixtures/db/customers-invoices.json",
        "consuming_task_ids": ["task-billing"],
        "confidence": 0.93,
        "rationale": (
            "Task task-billing likely needs database seed fixtures because "
            "metadata seed_records: customers, invoices; expected files include "
            "database-related paths."
        ),
    }


def test_filesystem_sample_fixture_is_inferred_from_validation_command():
    advice = infer_task_dependency_fixtures(
        _plan(
            [
                _task(
                    "task-import",
                    test_command="pytest tests/test_import.py --sample-file fixtures/sample.csv",
                    acceptance_criteria=["Import CSV upload handles malformed rows."],
                    files=["src/importer.py"],
                )
            ]
        )
    )

    requirement = advice.requirements[0]
    assert requirement.fixture_type == "filesystem_sample"
    assert requirement.likely_location == "tests/fixtures/files/task-import"
    assert requirement.consuming_task_ids == ("task-import",)
    assert "file-system sample data" in requirement.rationale


def test_configuration_fixture_is_inferred_from_config_paths():
    advice = infer_task_dependency_fixtures(
        _plan(
            [
                _task(
                    "task-config",
                    acceptance_criteria=["Feature flag behavior is covered for disabled mode."],
                    files=["src/settings.py", ".env.example"],
                )
            ]
        )
    )

    requirement = advice.requirements[0]
    assert requirement.fixture_type == "configuration"
    assert requirement.likely_location == "tests/fixtures/config/task-config.env"
    assert requirement.confidence >= 0.8
    assert "configuration inputs" in requirement.rationale


def test_mock_service_fixture_is_inferred_from_external_integration_metadata():
    advice = infer_task_dependency_fixtures(
        _plan(
            [
                _task(
                    "task-webhook",
                    metadata={"mock_services": ["stripe"]},
                    acceptance_criteria=["Webhook retry behavior handles the Stripe sandbox."],
                    files=["src/integrations/stripe.py"],
                )
            ]
        )
    )

    requirement = advice.requirements[0]
    assert requirement.fixture_type == "mock_service"
    assert requirement.likely_location == "tests/fixtures/mocks/stripe.json"
    assert requirement.confidence == 0.98
    assert "mocked external service behavior" in requirement.rationale


def test_mixed_fixture_aggregation_deduplicates_consuming_tasks_and_serializes():
    plan = _plan(
        [
            _task(
                "task-api",
                metadata={"api_contract_fixture_path": "tests/fixtures/api/shared-billing.json"},
                acceptance_criteria=["Validate billing API contract responses."],
            ),
            _task(
                "task-worker",
                metadata={"fixtures": {"api_contract": "tests/fixtures/api/shared-billing.json"}},
                acceptance_criteria=["Worker emits billing API response payloads."],
            ),
            _task(
                "task-db",
                metadata={"tables": ["accounts"]},
                acceptance_criteria=["Database has seeded account records."],
            ),
            _task(
                "task-config",
                metadata={"config_fixtures": ["tenant-a"]},
                acceptance_criteria=["Configuration selects tenant-specific settings."],
            ),
        ]
    )
    original = deepcopy(plan)

    advice = infer_task_dependency_fixtures(ExecutionPlan.model_validate(plan))
    payload = task_dependency_fixtures_to_dict(advice)

    assert plan == original
    assert isinstance(advice.requirements[0], TaskDependencyFixtureRequirement)
    assert advice.fixture_types == ("api_contract", "database_seed", "configuration")
    assert [requirement.fixture_type for requirement in advice.requirements] == [
        "api_contract",
        "database_seed",
        "configuration",
    ]
    assert advice.requirements[0].likely_location == "tests/fixtures/api/shared-billing.json"
    assert advice.requirements[0].consuming_task_ids == ("task-api", "task-worker")
    assert "Task task-api" in advice.requirements[0].rationale
    assert "Task task-worker" in advice.requirements[0].rationale
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks, *, plan_id="plan-test"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Build", "description": "Build the feature"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    files=None,
    acceptance_criteria=None,
    test_command=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "description": f"Implement {task_id}",
        "milestone": "Build",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files or ["src/app.py"],
        "acceptance_criteria": acceptance_criteria or [f"{task_id} works"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
