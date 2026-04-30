from blueprint.task_rollback_plan import (
    TaskRollbackPlan,
    generate_task_rollback_plans,
)


def test_generate_task_rollback_plans_returns_records_in_task_order():
    plans = generate_task_rollback_plans(
        {
            "id": "plan-rollback",
            "tasks": [
                _task("task-api", "Backend API", files=["src/api.py"]),
                _task("task-docs", "Docs", files=["README.md"]),
            ],
        }
    )

    assert [plan.task_id for plan in plans] == ["task-api", "task-docs"]
    assert all(isinstance(plan, TaskRollbackPlan) for plan in plans)


def test_high_risk_data_change_requires_manual_review_and_strong_verification():
    plan = generate_task_rollback_plans(
        {
            "id": "plan-rollback",
            "tasks": [
                _task(
                    "task-migration",
                    "Add customer audit migration",
                    files=["migrations/20260501_add_customer_audit.sql"],
                    acceptance=["Migration creates customer audit records"],
                    risk_level="high",
                    test_command="poetry run pytest tests/test_db_migrations.py",
                )
            ],
        }
    )[0]

    assert plan.manual_review_required is True
    assert plan.checkpoint_files == ["migrations/20260501_add_customer_audit.sql"]
    assert "poetry run pytest tests/test_db_migrations.py" in plan.verification_commands
    assert "git diff --check" in plan.verification_commands
    assert any("migration rollback" in command for command in plan.verification_commands)
    assert "pre-change backup" in plan.rollback_strategy
    assert plan.notes == ["Manual review required for risk profile: high-risk, data/schema."]


def test_config_change_requires_manual_review_and_startup_validation():
    plan = generate_task_rollback_plans(
        {
            "id": "plan-rollback",
            "tasks": [
                _task(
                    "task-config",
                    "Update feature flag config",
                    description="Change configuration for rollout defaults.",
                    files=["config/features.yaml"],
                    acceptance=["Feature flag defaults are restored on rollback"],
                )
            ],
        }
    )[0]

    assert plan.manual_review_required is True
    assert plan.rollback_strategy.startswith("Restore the previous configuration values")
    assert "git diff --check" in plan.verification_commands
    assert any("configuration validation" in command for command in plan.verification_commands)
    assert plan.notes == ["Manual review required for risk profile: config."]


def test_missing_files_or_modules_gets_explicit_checkpoint_note():
    plan = generate_task_rollback_plans(
        {
            "id": "plan-rollback",
            "tasks": [
                _task(
                    "task-sparse",
                    "Fix reported bug",
                    files=[],
                    acceptance=["Bug no longer reproduces"],
                )
            ],
        }
    )[0]

    assert plan.manual_review_required is False
    assert plan.checkpoint_files == []
    assert "Capture the actual files changed" in plan.rollback_strategy
    assert plan.verification_commands == [
        "Review git diff to confirm only intended task files changed."
    ]
    assert plan.notes == [
        "No files_or_modules were listed; identify changed files before rollback."
    ]


def test_existing_metadata_rollback_hints_are_incorporated():
    plan = generate_task_rollback_plans(
        {
            "id": "plan-rollback",
            "tasks": [
                _task(
                    "task-api",
                    "Add backend endpoint",
                    files=["src/api.py"],
                    acceptance=["Endpoint returns 200"],
                    metadata={
                        "rollback_hint": "Remove the route registration and handler.",
                        "test_commands": ["poetry run pytest tests/test_api.py"],
                    },
                )
            ],
        }
    )[0]

    assert plan.rollback_strategy == (
        "Use the task rollback hint first, then revert the task-scoped file changes."
    )
    assert plan.verification_commands == ["poetry run pytest tests/test_api.py"]
    assert plan.notes == ["Task rollback hint: Remove the route registration and handler."]


def test_low_risk_isolated_task_receives_lightweight_guidance_and_uses_test_command():
    plan = generate_task_rollback_plans(
        {
            "id": "plan-rollback",
            "tasks": [
                _task(
                    "task-exporter",
                    "Add isolated exporter docs",
                    files=[
                        "src/blueprint/exporters/task_rollback.md",
                        "docs/task-rollback.md",
                    ],
                    acceptance=["Docs describe rollback export fields"],
                    risk_level="low",
                    test_command="poetry run pytest tests/test_task_rollback_plan.py",
                )
            ],
        }
    )[0]

    assert plan.manual_review_required is False
    assert plan.rollback_strategy == (
        "Revert the documented or isolated file changes from version control."
    )
    assert plan.verification_commands == ["poetry run pytest tests/test_task_rollback_plan.py"]
    assert plan.notes == [
        "Lightweight scope; version-control rollback and a diff review should be enough."
    ]


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
