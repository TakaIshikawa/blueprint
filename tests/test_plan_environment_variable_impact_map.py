import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_environment_variable_impact_map import (
    EnvironmentVariableImpactRecord,
    PlanEnvironmentVariableImpactMap,
    build_plan_environment_variable_impact_map,
    derive_plan_environment_variable_impact_map,
    plan_environment_variable_impact_map_to_dict,
    plan_environment_variable_impact_map_to_markdown,
    summarize_plan_environment_variable_impact_map,
)


def test_keys_are_extracted_from_text_metadata_acceptance_commands_and_paths():
    result = build_plan_environment_variable_impact_map(
        _plan(
            [
                _task(
                    "task-text",
                    title="Configure STRIPE_WEBHOOK_SECRET for production checkout",
                    description="Set config key billing.retry_limit before go-live.",
                ),
                _task(
                    "task-acceptance",
                    title="Gate checkout rollout",
                    acceptance_criteria=[
                        "Feature flag checkout_v2_enabled defaults off in staging.",
                        "Acceptance uses API_BASE_URL for preview smoke checks.",
                    ],
                ),
                _task(
                    "task-metadata",
                    title="Add email provider settings",
                    metadata={
                        "EMAIL_PROVIDER_TOKEN": "from secret store",
                        "feature_flags": {"email_digest_enabled": True},
                        "validation_commands": {
                            "test": ["CI=true SENDGRID_API_KEY=dummy poetry run pytest"]
                        },
                    },
                ),
                _task(
                    "task-path",
                    title="Add deploy config",
                    files_or_modules=[
                        "deploy/production/STRIPE_WEBHOOK_SECRET.yaml",
                        "config/staging/FEATURE_CHECKOUT_ENABLED.yml",
                    ],
                ),
            ]
        )
    )

    by_key = {record.key_name: record for record in result.records}

    assert by_key["STRIPE_WEBHOOK_SECRET"].task_ids == ("task-text", "task-path")
    assert by_key["STRIPE_WEBHOOK_SECRET"].likely_environment_scope == "production"
    assert by_key["STRIPE_WEBHOOK_SECRET"].sensitivity == "sensitive"
    assert by_key["SENDGRID_API_KEY"].likely_environment_scope == "ci"
    assert by_key["EMAIL_PROVIDER_TOKEN"].sensitivity == "sensitive"
    assert by_key["checkout_v2_enabled"].likely_environment_scope == "staging"
    assert by_key["FEATURE_CHECKOUT_ENABLED"].likely_environment_scope == "staging"
    assert by_key["billing.retry_limit"].sensitivity == "non_sensitive"
    assert by_key["API_BASE_URL"].likely_environment_scope == "preview"
    assert any(
        "acceptance_criteria: Feature flag checkout_v2_enabled defaults off in staging."
        in evidence
        for evidence in by_key["checkout_v2_enabled"].evidence
    )
    assert any(
        "validation_commands: CI=true SENDGRID_API_KEY=dummy poetry run pytest" in evidence
        for evidence in by_key["SENDGRID_API_KEY"].evidence
    )
    assert all(isinstance(record, EnvironmentVariableImpactRecord) for record in result.records)


def test_sensitive_and_non_sensitive_records_sort_deterministically_with_summary_counts():
    result = summarize_plan_environment_variable_impact_map(
        _plan(
            [
                _task(
                    "task-z",
                    title="Set PUBLIC_API_BASE_URL for preview.",
                    description="Feature toggle zeta_rollout_enabled controls runtime behavior.",
                ),
                _task(
                    "task-a",
                    title="Rotate APP_PRIVATE_KEY in production.",
                    description="Set deployment setting workers.max_replicas=4 in helm.",
                ),
            ],
            plan_id="plan-env-order",
        )
    )

    assert [record.key_name for record in result.records] == [
        "APP_PRIVATE_KEY",
        "PUBLIC_API_BASE_URL",
        "workers.max_replicas",
        "zeta_rollout_enabled",
    ]
    assert result.summary == {
        "task_count": 2,
        "key_count": 4,
        "scope_counts": {
            "local": 0,
            "ci": 0,
            "preview": 1,
            "staging": 0,
            "production": 1,
            "deployment": 1,
            "runtime": 1,
        },
        "sensitivity_counts": {"sensitive": 1, "non_sensitive": 3},
        "sensitive_key_count": 1,
        "non_sensitive_key_count": 3,
    }


def test_serialization_markdown_aliases_and_input_plan_are_deterministic_without_mutation():
    plan = _plan(
        [
            _task(
                "task-secret",
                title="Rotate PAYMENT_TOKEN | phase 1",
                description="Update production secret name PAYMENT_TOKEN.",
                acceptance_criteria=["Coordinate secret rotation with release manager."],
            )
        ],
        plan_id="plan-env",
    )
    original = copy.deepcopy(plan)

    result = build_plan_environment_variable_impact_map(plan)
    payload = plan_environment_variable_impact_map_to_dict(result)

    assert plan == original
    assert isinstance(result, PlanEnvironmentVariableImpactMap)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert result.findings == result.records
    assert list(payload) == ["plan_id", "records", "summary"]
    assert list(payload["records"][0]) == [
        "key_name",
        "task_ids",
        "likely_environment_scope",
        "sensitivity",
        "evidence",
        "recommended_coordination_notes",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert plan_environment_variable_impact_map_to_markdown(result) == result.to_markdown()
    assert "PAYMENT_TOKEN \\| phase" in result.to_markdown()
    assert "| PAYMENT_TOKEN | task-secret | production | sensitive |" in result.to_markdown()


def test_execution_plan_model_iterable_alias_and_empty_plan_are_supported():
    task = _task(
        "task-model",
        title="Set LOCAL_API_URL for local development",
        description="Runtime config account.login.enabled gates the login path.",
    )
    model = ExecutionPlan.model_validate(_plan([task], plan_id="plan-model"))
    task_model = ExecutionTask.model_validate(task)

    result = derive_plan_environment_variable_impact_map(model)
    direct = build_plan_environment_variable_impact_map(task_model)
    iterable = build_plan_environment_variable_impact_map([model.tasks[0]])
    derived = derive_plan_environment_variable_impact_map(result)
    empty = build_plan_environment_variable_impact_map({"id": "plan-empty", "tasks": []})

    assert result.plan_id == "plan-model"
    assert direct.records[0].task_ids == ("task-model",)
    assert iterable.plan_id is None
    assert derived is result
    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Environment Variable Impact Map: plan-empty",
            "",
            "## Summary",
            "",
            "- Task count: 0",
            "- Key count: 0",
            "- Scope counts: local 0, ci 0, preview 0, staging 0, production 0, deployment 0, runtime 0",
            "- Sensitivity counts: sensitive 0, non_sensitive 0",
            "",
            "No environment variable or configuration key impacts were detected.",
        ]
    )


def _plan(tasks, *, plan_id="plan-env-impact"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-env-impact",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    test_command=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
        "test_command": test_command,
        "status": "pending",
        "metadata": {} if metadata is None else metadata,
    }
