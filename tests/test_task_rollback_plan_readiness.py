import json

from blueprint.task_rollback_plan_readiness import (
    analyze_task_rollback_plan_readiness,
    build_task_rollback_plan_readiness_plan,
    recommend_task_rollback_plan_readiness,
    task_rollback_plan_readiness_plan_to_dict,
    task_rollback_plan_readiness_plan_to_dicts,
    task_rollback_plan_readiness_plan_to_markdown,
)


def test_complete_rollback_plan_task_is_ready():
    result = build_task_rollback_plan_readiness_plan(
        {
            "id": "plan-rollback",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Rollback plan for post-deploy rollback validation",
                    "description": "Emergency rollback includes a revert plan for deployment rollback.",
                    "acceptance_criteria": [
                        "Rollback trigger defines trigger condition, failure threshold, error threshold, and abort condition.",
                        "Rollback procedure lists rollback steps, revert procedure, backout steps, and runbook playbook.",
                        "Data schema compatibility covers backward compatible migration compatibility and schema rollback.",
                        "Owner approver names owner, approver, release manager, on-call, and sign-off.",
                        "Verification checks include health checks, smoke tests, post-rollback checks, and rollback validation.",
                        "Customer impact communication includes customer notice, status update, support message, and incident update.",
                        "Time limit and stop condition define rollback window, maximum duration, timeout, and abort after.",
                        "Tests include rollback tests, revert tests, migration tests, and smoke tests.",
                    ],
                    "files_or_modules": ["src/deploy/rollback/rollback_plan.py"],
                }
            ],
        }
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "rollback_trigger",
        "rollback_procedure",
        "data_schema_compatibility",
        "owner_approver",
        "verification_checks",
        "customer_impact_communication",
        "time_limit_stop_condition",
        "tests",
    )


def test_partial_rollback_plan_reports_gaps_and_ignores_no_impact():
    result = analyze_task_rollback_plan_readiness(
        [
            {
                "id": "task-partial",
                "title": "Add rollback plan",
                "description": "Rollback trigger has an owner approver and verification checks.",
                "metadata": {"deploy": {"procedure": "Rollback procedure uses a runbook."}},
                "validation_commands": ["python -m pytest tests/deploy/test_rollback_plan.py"],
            },
            {
                "id": "task-copy",
                "title": "Deploy docs cleanup",
                "description": "No rollback plan, revert plan, emergency rollback, or rollback validation changes are planned.",
            },
        ]
    )

    record = result.records[0]
    assert result.ignored_task_ids == ("task-copy",)
    assert record.readiness == "partial"
    assert record.present_criteria == ("rollback_trigger", "rollback_procedure", "owner_approver", "verification_checks", "tests")
    assert record.missing_criteria == ("data_schema_compatibility", "customer_impact_communication", "time_limit_stop_condition")
    assert any("metadata.deploy.procedure" in item for item in record.evidence)
    assert any("validation_commands[0]" in item for item in record.evidence)


def test_rollback_plan_path_hints_serialization_and_markdown_are_stable():
    result = build_task_rollback_plan_readiness_plan(
        {"id": "plan-path", "tasks": [{"id": "task-path", "title": "Refactor deploy safety", "files_or_modules": ["src/deploy/migration/rollback_validation.py"]}]}
    )
    payload = task_rollback_plan_readiness_plan_to_dict(result)

    assert result.records[0].detected_signals == ("rollback_plan", "rollback_validation", "deployment_rollback")
    assert recommend_task_rollback_plan_readiness(result) == result.records
    assert task_rollback_plan_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-path"
    assert task_rollback_plan_readiness_plan_to_markdown(result).startswith("# Task Rollback Plan Readiness: plan-path")
