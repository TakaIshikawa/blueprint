import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_config_drift import (
    TaskConfigDriftPlan,
    TaskConfigDriftRecord,
    build_task_config_drift_plan,
    derive_task_config_drift_plan,
    task_config_drift_plan_to_dict,
    task_config_drift_plan_to_markdown,
)


def test_env_var_changes_generate_drift_guidance_with_required_fields():
    result = build_task_config_drift_plan(
        _plan(
            [
                _task(
                    "task-env",
                    title="Add PAYMENT_API_KEY secret",
                    description="Read PAYMENT_API_KEY from the environment and document .env setup.",
                    files_or_modules=[".env.example", "src/settings.py"],
                    metadata={"env_vars": ["PAYMENT_API_KEY required in CI and production"]},
                )
            ]
        )
    )

    record = _record(result, "task-env", "environment")

    assert record.affected_environments == ("local", "ci", "staging", "production")
    assert record.drift_source == "environment variable or secret change"
    assert record.severity == "medium"
    assert "Add an explicit validation plan before implementation or deployment." in (
        record.prevention_checklist
    )
    assert record.detection_evidence == (
        "files_or_modules: .env.example",
        "title: Add PAYMENT_API_KEY secret",
        "description: Read PAYMENT_API_KEY from the environment and document .env setup.",
        "metadata.env_vars[0]: PAYMENT_API_KEY required in CI and production",
    )
    assert record.rollback_notes


def test_iac_ci_feature_flag_and_config_file_changes_are_detected():
    result = build_task_config_drift_plan(
        _plan(
            [
                _task(
                    "task-iac",
                    title="Update Terraform and Helm settings",
                    files_or_modules=["infra/main.tf", "helm/api/values.yaml"],
                    validation_command="terraform plan && helm lint helm/api",
                ),
                _task(
                    "task-ci",
                    title="Update release workflow",
                    files_or_modules=[".github/workflows/release.yml"],
                    acceptance_criteria=["Dry-run the workflow before merge."],
                ),
                _task(
                    "task-flag",
                    title="Add feature flag for checkout",
                    description="Create LaunchDarkly feature flag with a kill switch.",
                    metadata={"feature_flag": "checkout_v2 defaults off in production"},
                    validation_command="poetry run pytest tests/test_flags.py",
                ),
                _task(
                    "task-config",
                    title="Change service YAML config",
                    files_or_modules=["config/service.yaml"],
                    metadata={"config_keys": ["request_timeout_ms"]},
                    validation_command="poetry run pytest tests/test_config.py",
                ),
            ]
        )
    )

    assert _record(result, "task-iac", "infrastructure").severity == "medium"
    assert _record(result, "task-iac", "infrastructure").affected_environments == (
        "staging",
        "production",
    )
    assert _record(result, "task-ci", "ci").affected_environments == ("ci",)
    assert _record(result, "task-ci", "ci").severity == "high"
    assert _record(result, "task-flag", "feature_flag").severity == "low"
    assert _record(result, "task-flag", "feature_flag").affected_environments == (
        "staging",
        "production",
    )
    assert _record(result, "task-config", "config_file").severity == "low"
    assert _record(result, "task-config", "config_file").affected_environments == (
        "local",
        "ci",
        "staging",
        "production",
    )


def test_validation_plan_lowers_severity_for_equivalent_tasks():
    result = build_task_config_drift_plan(
        _plan(
            [
                _task(
                    "task-unvalidated",
                    title="Update production Terraform",
                    files_or_modules=["terraform/prod/main.tf"],
                ),
                _task(
                    "task-validated",
                    title="Update production Terraform",
                    files_or_modules=["terraform/prod/main.tf"],
                    metadata={"validation_plan": "Run terraform plan for staging and production."},
                ),
            ],
            test_strategy=None,
        )
    )

    assert _record(result, "task-unvalidated", "infrastructure").severity == "high"
    validated = _record(result, "task-validated", "infrastructure")
    assert validated.severity == "medium"
    assert validated.validation_evidence == (
        "metadata.validation_plan: Run terraform plan for staging and production.",
    )


def test_migration_and_deployment_settings_include_rollback_notes_and_evidence():
    result = build_task_config_drift_plan(
        _plan(
            [
                _task(
                    "task-migration",
                    title="Run Alembic migration for audit table",
                    files_or_modules=["migrations/versions/20260502_audit.py"],
                    acceptance_criteria=[
                        "Validate alembic downgrade -1 rollback before production."
                    ],
                ),
                _task(
                    "task-deploy",
                    title="Change canary deployment settings",
                    files_or_modules=["deploy/api.yaml"],
                    metadata={"affected_environments": ["staging", "production"]},
                    validation_command="kubectl rollout status deployment/api",
                ),
            ]
        )
    )

    migration = _record(result, "task-migration", "migration")
    deploy = _record(result, "task-deploy", "deployment")

    assert migration.affected_environments == ("staging", "production")
    assert any("downgrade" in note for note in migration.rollback_notes)
    assert deploy.affected_environments == ("staging", "production")
    assert deploy.severity == "medium"
    assert any("files_or_modules: deploy/api.yaml" in item for item in deploy.detection_evidence)


def test_low_risk_non_config_task_returns_empty_result():
    result = build_task_config_drift_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Polish button copy",
                    description="Update UI label text.",
                    files_or_modules=["src/components/Button.tsx"],
                    acceptance_criteria=["Button copy matches product guidance."],
                )
            ]
        )
    )

    assert result.plan_id == "plan-config-drift"
    assert result.records == ()
    assert result.to_dict() == {"plan_id": "plan-config-drift", "records": []}


def test_serialization_aliases_model_inputs_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-config",
                title="Update appsettings",
                files_or_modules=["appsettings.production.json"],
                validation_command="poetry run pytest tests/test_settings.py",
            )
        ],
        plan_id="plan-model",
        test_strategy=None,
    )
    result = build_task_config_drift_plan(ExecutionPlan.model_validate(plan))
    alias_result = derive_task_config_drift_plan(plan)
    single = build_task_config_drift_plan(ExecutionTask.model_validate(plan["tasks"][0]))
    payload = task_config_drift_plan_to_dict(result)

    assert isinstance(result, TaskConfigDriftPlan)
    assert isinstance(TaskConfigDriftRecord, type)
    assert payload == result.to_dict()
    assert alias_result.to_dict() == result.to_dict()
    assert single.plan_id is None
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "severity",
        "drift_source",
        "category",
        "affected_environments",
        "prevention_checklist",
        "detection_evidence",
        "rollback_notes",
        "validation_evidence",
    ]
    assert task_config_drift_plan_to_markdown(result).startswith(
        "# Task Configuration Drift Plan: plan-model\n\n| Task | Category |"
    )


def _record(result, task_id, category):
    return next(
        record
        for record in result.records
        if record.task_id == task_id and record.category == category
    )


def _plan(tasks, *, plan_id="plan-config-drift", test_strategy="Run focused drift tests."):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-config-drift",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": test_strategy,
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    validation_command=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": None,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
    if validation_command is not None:
        task["test_command"] = validation_command
    return task
