import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_input_validation_readiness import (
    TaskInputValidationReadinessPlan,
    TaskInputValidationReadinessRecord,
    analyze_task_input_validation_readiness,
    build_task_input_validation_readiness_plan,
    extract_task_input_validation_readiness,
    generate_task_input_validation_readiness,
    recommend_task_input_validation_readiness,
    summarize_task_input_validation_readiness,
    task_input_validation_readiness_plan_to_dict,
    task_input_validation_readiness_plan_to_dicts,
    task_input_validation_readiness_plan_to_markdown,
)


def test_flags_api_payload_form_import_cli_config_and_user_input_surfaces():
    result = build_task_input_validation_readiness_plan(
        _plan(
            [
                _task(
                    "task-api",
                    title="Validate API payloads",
                    description="Add request body validation for JSON payloads on API endpoints.",
                    files_or_modules=["src/api/payloads/create_user.py"],
                    acceptance_criteria=["Schema validation rejects invalid JSON payloads."],
                ),
                _task(
                    "task-form",
                    title="Validate profile form",
                    description="The settings form accepts user-provided input fields.",
                ),
                _task(
                    "task-import",
                    title="Harden CSV import parser",
                    description="CSV import parser handles spreadsheet import rows from external feeds.",
                ),
                _task(
                    "task-cli",
                    title="Validate CLI arguments",
                    description="Command-line arguments and flags configure the maintenance command.",
                    files_or_modules=["src/cli/commands.py"],
                ),
                _task(
                    "task-config",
                    title="Validate YAML config files",
                    description="Configuration files and environment variables drive feature config.",
                    files_or_modules=["src/config/settings_loader.py"],
                ),
                _task(
                    "task-ugc",
                    title="Validate comments",
                    description="Comments are user-generated content with free-text customer input.",
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert isinstance(result, TaskInputValidationReadinessPlan)
    assert all(isinstance(record, TaskInputValidationReadinessRecord) for record in result.records)
    assert result.validation_task_ids == (
        "task-api",
        "task-config",
        "task-form",
        "task-import",
        "task-ugc",
        "task-cli",
    )
    assert by_id["task-api"].validation_surfaces == ("api_payload",)
    assert by_id["task-form"].validation_surfaces == ("form", "user_generated_input")
    assert by_id["task-import"].validation_surfaces == ("import_parser",)
    assert by_id["task-cli"].validation_surfaces == ("cli_argument",)
    assert by_id["task-config"].validation_surfaces == ("config_file",)
    assert by_id["task-ugc"].validation_surfaces == ("user_generated_input",)
    assert by_id["task-api"].risk_level == "high"
    assert by_id["task-cli"].risk_level == "medium"
    assert "boundary_values" in by_id["task-api"].missing_acceptance_criteria
    assert "required_fields" in by_id["task-api"].missing_acceptance_criteria
    assert any("files_or_modules: src/api/payloads/create_user.py" in item for item in by_id["task-api"].evidence)
    assert result.summary["surface_counts"]["api_payload"] == 1
    assert result.summary["risk_counts"] == {"high": 5, "medium": 1, "low": 0}


def test_detects_present_validation_acceptance_criteria_and_missing_checks():
    result = analyze_task_input_validation_readiness(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Validate checkout form",
                    description="Checkout form accepts user supplied input.",
                    acceptance_criteria=[
                        "Schema validation uses Zod for every form field.",
                        "Boundary values cover empty string, maximum length, zero, and negative values.",
                        "Malformed payloads and invalid input return deterministic bad request errors.",
                        "Required fields show missing field validation.",
                        "User-visible error messages are actionable inline errors.",
                        "Backward-compatible rollout keeps legacy clients on warn-only validation first.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.present_acceptance_criteria == (
        "schema_validation",
        "boundary_values",
        "malformed_payloads",
        "required_fields",
        "user_visible_errors",
        "backward_compatible_rollout",
    )
    assert record.missing_acceptance_criteria == ()
    assert record.suggested_validation_checks == ()
    assert record.risk_level == "medium"
    assert result.summary["missing_acceptance_criteria_count"] == 0


def test_non_validation_copy_tasks_return_empty_deterministic_result():
    result = build_task_input_validation_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update onboarding copy",
                    description="Adjust empty state text and loading labels.",
                    files_or_modules=["src/ui/onboarding_empty_state.tsx"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.validation_task_ids == ()
    assert result.ignored_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "validation_task_count": 0,
        "ignored_task_ids": ["task-copy"],
        "missing_acceptance_criteria_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_acceptance_criteria_counts": {
            "schema_validation": 0,
            "boundary_values": 0,
            "malformed_payloads": 0,
            "required_fields": 0,
            "user_visible_errors": 0,
            "backward_compatible_rollout": 0,
        },
        "surface_counts": {},
        "validation_task_ids": [],
    }
    assert "No input validation readiness records" in result.to_markdown()
    assert "Ignored tasks: task-copy" in result.to_markdown()


def test_serialization_markdown_aliases_order_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Validate profile form | settings",
                description="Profile form needs required fields and user-visible errors.",
            ),
            _task(
                "task-a",
                title="Validate API request payload",
                description="API payload validation must cover malformed payloads.",
            ),
            _task("task-copy", title="Update copy", description="Change button text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_input_validation_readiness(plan)
    payload = task_input_validation_readiness_plan_to_dict(result)
    markdown = task_input_validation_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_input_validation_readiness_plan_to_dicts(result) == payload["records"]
    assert task_input_validation_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_input_validation_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_input_validation_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_input_validation_readiness(plan).to_dict() == result.to_dict()
    assert result.recommendations == result.records
    assert result.validation_task_ids == ("task-a", "task-z")
    assert result.ignored_task_ids == ("task-copy",)
    assert list(payload) == [
        "plan_id",
        "records",
        "validation_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "validation_surfaces",
        "risk_level",
        "present_acceptance_criteria",
        "missing_acceptance_criteria",
        "suggested_validation_checks",
        "evidence",
    ]
    assert [record.risk_level for record in result.records] == ["high", "medium"]
    assert markdown.startswith("# Task Input Validation Readiness: plan-input-validation")
    assert "Validate profile form \\| settings" in markdown
    assert "| Task | Title | Risk | Validation Surfaces | Missing Acceptance Criteria | Suggested Validation Checks | Evidence |" in markdown


def test_execution_plan_execution_task_mapping_and_iterable_inputs_are_supported():
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Validate API payload model",
            description="API payload has schema validation and required fields.",
            acceptance_criteria=[
                "Schema validation covers request bodies.",
                "Required fields fail with 422 response errors.",
            ],
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-plan",
                    title="Validate config file parser",
                    description="Config file parser validates YAML config with boundary values.",
                )
            ],
            plan_id="plan-model",
        )
    )
    iterable_result = build_task_input_validation_readiness_plan(
        [
            _task(
                "task-iter",
                title="Validate CLI arguments",
                description="CLI arguments reject malformed input.",
            )
        ]
    )

    task_result = build_task_input_validation_readiness_plan(task_model)
    plan_result = build_task_input_validation_readiness_plan(plan_model)

    assert task_result.plan_id is None
    assert task_result.records[0].task_id == "task-model"
    assert task_result.records[0].validation_surfaces == ("api_payload",)
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-plan"
    assert plan_result.records[0].validation_surfaces == ("config_file",)
    assert iterable_result.validation_task_ids == ("task-iter",)


def _plan(tasks, plan_id="plan-input-validation"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-input-validation",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-input-validation",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
