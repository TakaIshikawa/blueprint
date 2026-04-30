import json

from blueprint.domain.models import ExecutionTask
from blueprint.validation_evidence_readiness import (
    TaskValidationEvidenceReadiness,
    build_validation_evidence_readiness,
    validation_evidence_readiness_to_dict,
)


def test_strong_low_risk_evidence_is_ready():
    result = build_validation_evidence_readiness(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Add profile preference toggle",
                    acceptance_criteria=["Preference persists after refresh."],
                    test_command="poetry run pytest tests/test_profile.py",
                    metadata={
                        "validation_evidence": {
                            "pytest": "tests/test_profile.py passed",
                            "artifact": "artifacts/profile-preference.log",
                        }
                    },
                )
            ]
        )
    )

    readiness = result.tasks[0]

    assert isinstance(readiness, TaskValidationEvidenceReadiness)
    assert readiness.task_id == "task-ready"
    assert readiness.risk_level == "low"
    assert readiness.score == 85
    assert readiness.grade == "ready"
    assert readiness.missing_evidence == ()
    assert readiness.suggested_next_evidence == ()
    assert readiness.evidence == (
        "acceptance_criteria[0]: Preference persists after refresh.",
        "validation command: poetry run pytest tests/test_profile.py",
        "metadata.validation_evidence.artifact: artifacts/profile-preference.log",
        "metadata.validation_evidence.pytest: tests/test_profile.py passed",
    )


def test_missing_test_command_stays_partial_and_suggests_command():
    result = build_validation_evidence_readiness(
        _plan(
            [
                _task(
                    "task-no-command",
                    title="Render account audit summary",
                    acceptance_criteria=["Audit summary shows the latest actor."],
                    metadata={"validation_artifacts": ["artifacts/audit-summary.png"]},
                )
            ]
        )
    )

    readiness = result.tasks[0]

    assert readiness.score == 55
    assert readiness.grade == "partial"
    assert readiness.missing_evidence == ("test_command",)
    assert readiness.suggested_next_evidence == (
        "Name the focused validation command reviewers should trust for Render account audit summary.",
    )


def test_high_risk_task_requires_risk_review_artifact_for_ready_grade():
    result = build_validation_evidence_readiness(
        _plan(
            [
                _task(
                    "task-risky",
                    title="Replace billing webhook contract",
                    risk_level="high",
                    acceptance_criteria=["Webhook retries preserve idempotency."],
                    test_command="poetry run pytest tests/test_billing_webhook.py",
                    metadata={
                        "validation_evidence": ["pytest output: 18 passed"],
                    },
                ),
                _task(
                    "task-risky-reviewed",
                    title="Replace billing webhook contract with review",
                    risk_level="high",
                    acceptance_criteria=["Webhook retries preserve idempotency."],
                    test_command="poetry run pytest tests/test_billing_webhook.py",
                    metadata={
                        "validation_evidence": ["pytest output: 18 passed"],
                        "reviewer_notes": "Rollback uses the legacy webhook handler flag.",
                    },
                ),
            ]
        )
    )

    without_review, with_review = result.tasks

    assert without_review.score == 85
    assert without_review.grade == "partial"
    assert without_review.missing_evidence == ("risk_review",)
    assert without_review.suggested_next_evidence == (
        "Add reviewer, rollback, rollout, or risk mitigation evidence for high-risk Replace billing webhook contract.",
    )
    assert with_review.score == 100
    assert with_review.grade == "ready"
    assert with_review.missing_evidence == ()


def test_completed_task_without_validation_evidence_is_flagged():
    result = build_validation_evidence_readiness(
        _plan(
            [
                _task(
                    "task-complete-empty",
                    title="Tighten settings copy",
                    status="completed",
                    acceptance_criteria=[],
                )
            ]
        )
    )

    readiness = result.tasks[0]

    assert readiness.score == 0
    assert readiness.grade == "insufficient"
    assert readiness.missing_evidence == (
        "acceptance_criteria",
        "test_command",
        "validation_artifact",
        "completed_without_evidence",
    )
    assert readiness.suggested_next_evidence[-1] == (
        "Reopen or block review for Tighten settings copy until validation proof is recorded."
    )


def test_summary_counts_and_serialization_are_stable_for_model_inputs():
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Load region catalog",
            acceptance_criteria=["Regions are visible in admin search."],
            metadata={
                "validation_command": "poetry run pytest tests/test_regions.py",
                "validation_artifacts": ["artifacts/regions-pytest.log"],
            },
        )
    )
    result = build_validation_evidence_readiness(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Add export filter",
                    acceptance_criteria=["Export includes only filtered rows."],
                    test_command="poetry run pytest tests/test_exports.py",
                    metadata={"validation_evidence": "pytest passed"},
                ),
                _task(
                    "task-partial",
                    title="Update audit copy",
                    acceptance_criteria=["Audit copy appears in the details view."],
                    metadata={"validation_artifacts": ["artifacts/audit-copy.png"]},
                ),
                _task(
                    "task-empty",
                    title="Complete empty task",
                    status="done",
                    acceptance_criteria=[],
                ),
                task_model,
            ]
        )
    )
    payload = validation_evidence_readiness_to_dict(result)

    assert payload == result.to_dict()
    assert result.summary_counts == {"ready": 2, "partial": 1, "insufficient": 1}
    assert list(payload) == ["plan_id", "task_count", "summary_counts", "tasks"]
    assert list(payload["tasks"][0]) == [
        "task_id",
        "title",
        "risk_level",
        "status",
        "score",
        "grade",
        "missing_evidence",
        "suggested_next_evidence",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks):
    return {
        "id": "plan-validation-evidence",
        "implementation_brief_id": "brief-validation-evidence",
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
    risk_level=None,
    status="pending",
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": (
            acceptance_criteria if acceptance_criteria is not None else ["Done"]
        ),
        "status": status,
    }
    if test_command is not None:
        task["test_command"] = test_command
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
