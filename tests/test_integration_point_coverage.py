import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.integration_point_coverage import (
    IntegrationPointCoverage,
    analyze_integration_point_coverage,
    integration_point_coverage_to_dict,
)


def test_exact_matches_return_covered_records_in_brief_order():
    coverage = analyze_integration_point_coverage(
        _brief(["Stripe webhook", "GitHub API"]),
        _plan(
            [
                _task(
                    "task-stripe",
                    "Implement Stripe webhook handler",
                    description="Process incoming Stripe webhook events.",
                ),
                _task(
                    "task-github",
                    "Add repository sync",
                    acceptance=["GitHub API returns repository metadata"],
                ),
            ]
        ),
    )

    assert [record.integration_point for record in coverage] == [
        "Stripe webhook",
        "GitHub API",
    ]
    assert all(isinstance(record, IntegrationPointCoverage) for record in coverage)
    assert [record.covered for record in coverage] == [True, True]
    assert coverage[0].task_ids == ("task-stripe",)
    assert coverage[1].task_ids == ("task-github",)
    assert coverage[0].evidence == (
        "task-stripe title: Implement Stripe webhook handler",
        "task-stripe description: Process incoming Stripe webhook events.",
        "task-stripe acceptance_criteria: Implement Stripe webhook handler is complete",
    )


def test_partial_token_matches_are_case_insensitive_and_deterministic():
    coverage = analyze_integration_point_coverage(
        _brief(["Local repository metadata"]),
        _plan(
            [
                _task(
                    "task-repo",
                    "Read repository context",
                    description="Collect metadata before planning.",
                    files=["src/blueprint/repository.py"],
                ),
                _task(
                    "task-unrelated",
                    "Render dashboard",
                    description="Update chart filters.",
                ),
            ]
        ),
    )

    assert coverage[0].to_dict() == {
        "integration_point": "Local repository metadata",
        "covered": True,
        "task_ids": ["task-repo"],
        "evidence": [
            "task-repo title: Read repository context",
            "task-repo description: Collect metadata before planning.",
            "task-repo files_or_modules: src/blueprint/repository.py",
        ],
        "missing_reason": None,
        "suggested_task_title": None,
    }


def test_metadata_and_test_command_text_participate_in_matching():
    coverage = analyze_integration_point_coverage(
        _brief(["Warehouse API"]),
        _plan(
            [
                _task(
                    "task-warehouse",
                    "Add ingestion checks",
                    metadata={
                        "integration_points": ["Warehouse API"],
                        "notes": {"handoff": "Verify warehouse credentials"},
                    },
                    test_command="poetry run pytest tests/test_warehouse_api.py",
                )
            ]
        ),
    )

    assert coverage[0].covered is True
    assert coverage[0].task_ids == ("task-warehouse",)
    assert coverage[0].evidence == (
        "task-warehouse metadata.integration_points.1: Warehouse API",
        "task-warehouse metadata.notes.handoff: Verify warehouse credentials",
        "task-warehouse test_command: poetry run pytest tests/test_warehouse_api.py",
    )


def test_uncovered_integration_points_include_reviewable_gap_fields():
    coverage = analyze_integration_point_coverage(
        _brief(["Slack webhook"]),
        _plan([_task("task-report", "Build report export")]),
    )

    assert coverage[0].to_dict() == {
        "integration_point": "Slack webhook",
        "covered": False,
        "task_ids": [],
        "evidence": [],
        "missing_reason": "No execution task mentions this integration point.",
        "suggested_task_title": "Cover Slack webhook integration point",
    }


def test_accepts_domain_models_and_serializes_empty_integration_points_stably():
    brief_model = ImplementationBrief.model_validate(
        {
            "id": "brief-model",
            "source_brief_id": "source-model",
            "title": "Brief model",
            "problem_statement": "Need a stable plan.",
            "mvp_goal": "Ship coverage analysis.",
            "scope": [],
            "non_goals": [],
            "assumptions": [],
            "integration_points": [],
            "risks": [],
            "validation_plan": "Run tests.",
            "definition_of_done": [],
        }
    )
    plan_model = ExecutionPlan.model_validate(
        {
            "id": "plan-model",
            "implementation_brief_id": "brief-model",
            "milestones": [],
            "tasks": [_task("task-model", "Model task")],
        }
    )

    coverage = analyze_integration_point_coverage(brief_model, plan_model)
    payload = integration_point_coverage_to_dict(coverage)

    assert coverage == ()
    assert payload == []
    assert json.loads(json.dumps(payload)) == payload


def _brief(integration_points):
    return {
        "id": "brief-coverage",
        "integration_points": integration_points,
    }


def _plan(tasks):
    return {
        "id": "plan-coverage",
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    description=None,
    files=None,
    acceptance=None,
    metadata=None,
    test_command=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": description or f"Implement {title}.",
        "files_or_modules": files or [],
        "acceptance_criteria": acceptance or [f"{title} is complete"],
    }
    if metadata is not None:
        task["metadata"] = metadata
    if test_command is not None:
        task["test_command"] = test_command
    return task
