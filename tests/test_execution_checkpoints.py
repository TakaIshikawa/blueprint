import json

from blueprint.domain.models import ExecutionPlan
from blueprint.execution_checkpoints import (
    ExecutionCheckpoint,
    execution_checkpoints_to_dict,
    recommend_execution_checkpoints,
)


def test_milestone_boundary_checkpoints_use_last_task_in_each_milestone():
    checkpoints = recommend_execution_checkpoints(
        _plan(
            [
                _task("task-setup", "Set up project", milestone="Foundation"),
                _task("task-api", "Build API", milestone="Foundation"),
                _task("task-ui", "Build UI", milestone="Delivery"),
            ]
        )
    )

    assert [checkpoint.to_dict() for checkpoint in checkpoints] == [
        {
            "checkpoint_id": "checkpoint-after-task-api",
            "reason": "milestone_boundary:Foundation",
            "trigger_task_ids": ["task-api"],
            "recommended_reviewer_role": "delivery_lead",
            "blocking": True,
            "suggested_acceptance_checks": [
                "Confirm milestone 'Foundation' acceptance criteria are complete.",
                "Review validation evidence before dispatching work after 'Foundation'.",
            ],
        },
        {
            "checkpoint_id": "checkpoint-after-task-ui",
            "reason": "milestone_boundary:Delivery",
            "trigger_task_ids": ["task-ui"],
            "recommended_reviewer_role": "delivery_lead",
            "blocking": True,
            "suggested_acceptance_checks": [
                "Confirm milestone 'Delivery' acceptance criteria are complete.",
                "Review validation evidence before dispatching work after 'Delivery'.",
            ],
        },
    ]
    assert all(isinstance(checkpoint, ExecutionCheckpoint) for checkpoint in checkpoints)


def test_high_risk_tasks_get_blocking_pre_dispatch_checkpoints():
    checkpoints = recommend_execution_checkpoints(
        _plan(
            [
                _task("task-low", "Update copy", risk_level="low"),
                _task("task-risk", "Replace ingestion pipeline", risk_level="HIGH"),
            ]
        )
    )

    assert len(checkpoints) == 1
    assert checkpoints[0].to_dict() == {
        "checkpoint_id": "checkpoint-before-task-risk",
        "reason": "high_risk_task:high",
        "trigger_task_ids": ["task-risk"],
        "recommended_reviewer_role": "technical_lead",
        "blocking": True,
        "suggested_acceptance_checks": [
            "Review the implementation approach for 'Replace ingestion pipeline'.",
            "Confirm rollback or mitigation notes exist for high-risk task task-risk.",
        ],
    }


def test_fan_out_dependency_checkpoints_follow_tasks_with_many_dependents():
    checkpoints = recommend_execution_checkpoints(
        _plan(
            [
                _task("task-contract", "Define shared contract"),
                _task("task-api", "Use API contract", depends_on=["task-contract"]),
                _task("task-ui", "Use UI contract", depends_on=["task-contract"]),
                _task("task-worker", "Use worker contract", depends_on=["task-contract"]),
            ]
        )
    )

    assert checkpoints[0].to_dict() == {
        "checkpoint_id": "checkpoint-after-task-contract",
        "reason": "fan_out_dependency:3 dependents",
        "trigger_task_ids": ["task-contract"],
        "recommended_reviewer_role": "technical_lead",
        "blocking": True,
        "suggested_acceptance_checks": [
            "Validate task-contract output before unblocking 3 dependent tasks.",
            "Confirm dependent tasks can consume the completed task-contract contract.",
        ],
    }


def test_sensitive_file_checkpoints_cover_inferred_and_configured_paths():
    checkpoints = recommend_execution_checkpoints(
        {
            "id": "plan-checkpoints",
            "metadata": {"sensitive_file_patterns": ["custom/secrets/*"]},
            "tasks": [
                _task(
                    "task-auth",
                    "Update session auth",
                    files=["src/auth/session.py"],
                ),
                _task(
                    "task-configured",
                    "Rotate custom key",
                    files=["custom/secrets/service-key.json"],
                ),
            ],
        }
    )

    assert [checkpoint.to_dict() for checkpoint in checkpoints] == [
        {
            "checkpoint_id": "checkpoint-before-task-auth",
            "reason": "sensitive_files:auth",
            "trigger_task_ids": ["task-auth"],
            "recommended_reviewer_role": "security_reviewer",
            "blocking": True,
            "suggested_acceptance_checks": [
                "Review sensitive change 'src/auth/session.py' before dispatch.",
                "Confirm tests or manual checks cover auth behavior.",
            ],
        },
        {
            "checkpoint_id": "checkpoint-before-task-configured",
            "reason": "sensitive_files:configured",
            "trigger_task_ids": ["task-configured"],
            "recommended_reviewer_role": "release_manager",
            "blocking": True,
            "suggested_acceptance_checks": [
                "Review sensitive change 'custom/secrets/service-key.json' before dispatch.",
                "Confirm tests or manual checks cover configured behavior.",
            ],
        },
    ]


def test_duplicate_pre_task_triggers_merge_into_one_checkpoint():
    checkpoints = recommend_execution_checkpoints(
        _plan(
            [
                _task(
                    "task-billing",
                    "Replace billing webhook",
                    files=["src/billing/webhook.py"],
                    risk_level="critical",
                )
            ]
        )
    )

    assert len(checkpoints) == 1
    assert checkpoints[0].to_dict() == {
        "checkpoint_id": "checkpoint-before-task-billing",
        "reason": "high_risk_task:critical; sensitive_files:billing",
        "trigger_task_ids": ["task-billing"],
        "recommended_reviewer_role": "security_reviewer",
        "blocking": True,
        "suggested_acceptance_checks": [
            "Review the implementation approach for 'Replace billing webhook'.",
            "Confirm rollback or mitigation notes exist for high-risk task task-billing.",
            "Review sensitive change 'src/billing/webhook.py' before dispatch.",
            "Confirm tests or manual checks cover billing behavior.",
        ],
    }


def test_minimal_plans_and_model_inputs_serialize_stably():
    plan_model = ExecutionPlan.model_validate(
        {
            "id": "plan-model",
            "implementation_brief_id": "brief-model",
            "milestones": [],
            "tasks": [_task("task-model", "Model task")],
        }
    )

    checkpoints = recommend_execution_checkpoints(plan_model)
    payload = execution_checkpoints_to_dict(checkpoints)

    assert checkpoints == ()
    assert payload == []
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks):
    return {
        "id": "plan-checkpoints",
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    milestone=None,
    depends_on=None,
    files=None,
    acceptance=None,
    risk_level=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "depends_on": depends_on or [],
        "files_or_modules": files or [],
        "acceptance_criteria": acceptance or [f"{title} is complete"],
    }
    if milestone is not None:
        task["milestone"] = milestone
    if risk_level is not None:
        task["risk_level"] = risk_level
    return task
