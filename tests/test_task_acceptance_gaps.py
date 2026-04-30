import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_acceptance_gaps import (
    TaskAcceptanceGapFinding,
    suggest_task_acceptance_gaps,
    task_acceptance_gap_findings_to_dict,
)


def test_tasks_with_no_acceptance_criteria_produce_high_severity_findings():
    findings = suggest_task_acceptance_gaps(
        _plan(
            [
                _task(
                    "task-empty",
                    "Build empty path",
                    acceptance_criteria=[],
                )
            ]
        )
    )

    assert findings == (
        TaskAcceptanceGapFinding(
            task_id="task-empty",
            title="Build empty path",
            severity="high",
            missing_dimensions=(
                "acceptance_criteria",
                "observable_behavior",
                "validation_evidence",
            ),
            suggested_criteria=(
                "Verify Build empty path produces the expected observable outcome.",
                "Add test or validation evidence proving Build empty path is complete.",
            ),
        ),
    )


def test_implementation_only_criteria_are_flagged_as_lacking_observable_behavior():
    findings = suggest_task_acceptance_gaps(
        _plan(
            [
                _task(
                    "task-impl",
                    "Build API",
                    acceptance_criteria=[
                        "Implement the API endpoint",
                        "Refactor the serializer module",
                    ],
                )
            ]
        )
    )

    assert findings[0].severity == "high"
    assert findings[0].missing_dimensions == (
        "observable_behavior",
        "validation_evidence",
    )
    assert findings[0].suggested_criteria == (
        "Verify the user-visible or API behavior for Build API.",
        "Add test or validation evidence proving Build API is complete.",
    )


def test_missing_validation_evidence_is_a_medium_gap():
    findings = suggest_task_acceptance_gaps(
        _plan(
            [
                _task(
                    "task-observable",
                    "Render dashboard",
                    acceptance_criteria=["Dashboard renders the filtered task list"],
                )
            ]
        )
    )

    assert findings == (
        TaskAcceptanceGapFinding(
            task_id="task-observable",
            title="Render dashboard",
            severity="medium",
            missing_dimensions=("validation_evidence",),
            suggested_criteria=(
                "Add test or validation evidence proving Render dashboard is complete.",
            ),
        ),
    )


def test_high_risk_tasks_without_risk_or_rollback_validation_get_specific_suggestion():
    findings = suggest_task_acceptance_gaps(
        _plan(
            [
                _task(
                    "task-risk",
                    "Migrate accounts",
                    acceptance_criteria=[
                        "Verify migrated accounts return the same billing status",
                        "Regression tests pass for account billing",
                    ],
                    risk_level="critical",
                )
            ]
        )
    )

    assert findings == (
        TaskAcceptanceGapFinding(
            task_id="task-risk",
            title="Migrate accounts",
            severity="high",
            missing_dimensions=("rollback_or_risk_check",),
            suggested_criteria=(
                "Validate rollback, fallback, or risk mitigation for Migrate accounts "
                "before release.",
            ),
        ),
    )


def test_complete_criteria_do_not_produce_findings():
    findings = suggest_task_acceptance_gaps(
        _plan(
            [
                _task(
                    "task-ready",
                    "Ship checkout",
                    acceptance_criteria=[
                        "Verify checkout returns a paid receipt for successful cards",
                        "Pytest regression tests pass for checkout payment handling",
                        "Rollback plan restores the previous payment adapter safely",
                    ],
                    risk_level="high",
                )
            ]
        )
    )

    assert findings == ()


def test_accepts_execution_plan_models_and_serializes_stably():
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    "Build model path",
                    acceptance_criteria=["Create the model helper"],
                )
            ]
        )
    )

    findings = suggest_task_acceptance_gaps(plan_model)
    payload = task_acceptance_gap_findings_to_dict(findings)

    assert payload == [finding.to_dict() for finding in findings]
    assert list(payload[0]) == [
        "task_id",
        "title",
        "severity",
        "missing_dimensions",
        "suggested_criteria",
    ]
    assert payload[0]["missing_dimensions"] == [
        "observable_behavior",
        "validation_evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks):
    return {
        "id": "plan-acceptance-gaps",
        "implementation_brief_id": "brief-acceptance-gaps",
        "target_repo": "example/repo",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    acceptance_criteria,
    risk_level="low",
):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": acceptance_criteria,
        "risk_level": risk_level,
        "status": "pending",
    }
