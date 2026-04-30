import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_validation_artifacts import (
    TaskValidationArtifact,
    TaskValidationArtifactPlan,
    build_task_validation_artifact_plan,
    task_validation_artifact_plan_to_dict,
)


def test_tasks_with_test_command_include_test_output_artifact():
    artifact_plan = build_task_validation_artifact_plan(
        {
            "id": "plan-validation",
            "tasks": [
                _task(
                    "task-tests",
                    "Add validation tests",
                    files=["src/blueprint/task_validation_artifacts.py"],
                    test_command="poetry run pytest tests/test_task_validation_artifacts.py",
                    risk_level="low",
                )
            ],
        }
    )

    task = artifact_plan.tasks[0]

    assert isinstance(artifact_plan, TaskValidationArtifactPlan)
    assert isinstance(task.artifacts[0], TaskValidationArtifact)
    assert task.to_dict()["artifacts"] == [
        {
            "type": "test_output",
            "label": "Test output",
            "reason": "Task includes a test_command; capture the command output.",
            "command": "poetry run pytest tests/test_task_validation_artifacts.py",
            "paths": [],
        }
    ]
    assert artifact_plan.summary == {
        "task_count": 1,
        "artifact_counts": {"test_output": 1},
        "risk_counts": {"low": 1},
    }


def test_ui_files_and_screenshot_acceptance_require_screenshot_artifact():
    artifact_plan = build_task_validation_artifact_plan(
        {
            "id": "plan-validation",
            "tasks": [
                _task(
                    "task-ui-file",
                    "Update settings panel",
                    files=["src/components/SettingsPanel.tsx"],
                    acceptance=["Settings panel renders the updated empty state"],
                ),
                _task(
                    "task-visual-proof",
                    "Capture visual proof",
                    files=["src/blueprint/exporters/report.py"],
                    acceptance=["Acceptance handoff includes a screenshot of the review report"],
                ),
            ],
        }
    )

    assert [_artifact_types(task) for task in artifact_plan.tasks] == [
        ["screenshot"],
        ["screenshot"],
    ]
    assert artifact_plan.tasks[0].artifacts[0].paths == (
        "src/components/SettingsPanel.tsx",
    )
    assert artifact_plan.summary["artifact_counts"] == {"screenshot": 2}


def test_migration_schema_and_config_files_produce_review_artifacts():
    artifact_plan = build_task_validation_artifact_plan(
        {
            "id": "plan-validation",
            "tasks": [
                _task(
                    "task-migration",
                    "Add audit migration",
                    files=["migrations/20260501_add_audit_events.sql"],
                    acceptance=["Migration creates the audit_events table"],
                ),
                _task(
                    "task-schema",
                    "Update event schema",
                    files=["schemas/event.schema.json"],
                    acceptance=["Schema accepts the new event kind"],
                ),
                _task(
                    "task-config",
                    "Enable validation config",
                    files=["pyproject.toml", "config/validation.yml"],
                    acceptance=["Config defaults are reviewable"],
                ),
            ],
        }
    )

    assert _artifact_types(artifact_plan.tasks[0]) == [
        "migration_note",
        "log_excerpt",
        "schema_review",
    ]
    assert _artifact_types(artifact_plan.tasks[1]) == ["schema_review"]
    assert _artifact_types(artifact_plan.tasks[2]) == ["config_review"]
    assert artifact_plan.summary == {
        "task_count": 3,
        "artifact_counts": {
            "config_review": 1,
            "log_excerpt": 1,
            "migration_note": 1,
            "schema_review": 2,
        },
        "risk_counts": {"unspecified": 3},
    }


def test_api_high_risk_and_metadata_artifacts_are_aggregated_deterministically():
    artifact_plan = build_task_validation_artifact_plan(
        {
            "id": "plan-validation",
            "tasks": [
                _task(
                    "task-api",
                    "Add task artifact API",
                    description="Expose a REST endpoint returning a sample response.",
                    files=["src/blueprint/api/task_artifacts.py"],
                    acceptance=["API returns a validation artifact sample"],
                    risk_level="high",
                    metadata={"validation_artifacts": ["log excerpt", "api-sample"]},
                ),
                _task(
                    "task-manual",
                    "Manual handoff",
                    acceptance=["Reviewer can manually verify the handoff note"],
                    risk_level="medium",
                    metadata={"requires_screenshot": "true"},
                ),
            ],
        }
    )

    assert _artifact_types(artifact_plan.tasks[0]) == [
        "api_sample",
        "manual_verification_note",
        "log_excerpt",
    ]
    assert _artifact_types(artifact_plan.tasks[1]) == [
        "screenshot",
        "manual_verification_note",
    ]
    assert artifact_plan.summary == {
        "task_count": 2,
        "artifact_counts": {
            "api_sample": 1,
            "log_excerpt": 1,
            "manual_verification_note": 2,
            "screenshot": 1,
        },
        "risk_counts": {"high": 1, "medium": 1},
    }


def test_accepts_execution_plan_models_and_serializes_stably():
    plan_model = ExecutionPlan.model_validate(
        {
            "id": "plan-model",
            "implementation_brief_id": "brief-model",
            "milestones": [],
            "tasks": [
                _task(
                    "task-model",
                    "Model task",
                    files=["src/pages/index.tsx"],
                    acceptance=["Page screenshot is captured"],
                    risk_level="LOW",
                )
            ],
        }
    )

    artifact_plan = build_task_validation_artifact_plan(plan_model)
    payload = task_validation_artifact_plan_to_dict(artifact_plan)

    assert payload == artifact_plan.to_dict()
    assert list(payload) == ["plan_id", "tasks", "summary"]
    assert payload["plan_id"] == "plan-model"
    assert payload["tasks"][0]["risk_level"] == "low"
    assert json.loads(json.dumps(payload)) == payload


def _artifact_types(task):
    return [artifact.type for artifact in task.artifacts]


def _task(
    task_id,
    title,
    *,
    description=None,
    files=None,
    acceptance=None,
    test_command=None,
    risk_level=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": description or f"Implement {title}.",
        "files_or_modules": files or [],
        "acceptance_criteria": acceptance or [f"{title} is complete"],
    }
    if test_command is not None:
        task["test_command"] = test_command
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
