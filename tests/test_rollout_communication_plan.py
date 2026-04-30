import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.rollout_communication_plan import (
    RolloutCommunicationCheckpoint,
    build_rollout_communication_plan,
    rollout_communication_checkpoints_to_dicts,
)


def test_milestone_announcements_use_brief_context_and_metadata():
    checkpoints = build_rollout_communication_plan(_plan(), _brief())

    first = checkpoints[0]

    assert isinstance(first, RolloutCommunicationCheckpoint)
    assert first.to_dict() == {
        "checkpoint_id": "comm-milestone-foundation",
        "source_type": "milestone",
        "source_id": "foundation",
        "audience": [
            "release manager",
            "platform team",
            "api owners",
            "operators",
            "Engineering",
            "Payments API",
            "engineering",
            "support",
        ],
        "trigger": "Milestone 'Foundation' is ready for kickoff or completion review.",
        "summary": (
            "Announce milestone 'Foundation' for Rollout Communication Brief: "
            "Prepare contracts and validation."
        ),
        "dependencies": [
            "API contract review",
            "Integration: Payments API",
            "Repository: example/repo",
        ],
        "expected_evidence": [
            "Milestone task completion: task-contracts",
            "contract review approved",
            "Validation plan: Run regression and rollout smoke tests.",
            "Definition of done: Stakeholders receive release updates",
            "Run pytest",
        ],
        "channel_labels": ["release", "product", "engineering", "support"],
    }


def test_high_risk_task_callouts_include_dependencies_and_validation_evidence():
    checkpoints = build_rollout_communication_plan(_plan(), _brief())

    high_risk = checkpoints[2]

    assert high_risk.to_dict() == {
        "checkpoint_id": "comm-task-task-rollout",
        "source_type": "task",
        "source_id": "task-rollout",
        "audience": [
            "release manager",
            "release lead",
            "support lead",
            "agent",
            "operators",
            "Engineering",
            "Payments API",
            "support",
        ],
        "trigger": (
            "High-risk task 'task-rollout' is ready for implementation or release review."
        ),
        "summary": (
            "Escalate high-risk task task-rollout: Enable staged rollout "
            "(high risk): Customer-facing availability risk."
        ),
        "dependencies": [
            "Task dependency: task-contracts",
            "Milestone: Release",
            "Integration: Payments API",
        ],
        "expected_evidence": [
            "Smoke test passes",
            "rollback tested",
            "poetry run pytest tests/test_rollout.py",
            "Validation plan: Run regression and rollout smoke tests.",
            "Definition of done: Stakeholders receive release updates",
        ],
        "channel_labels": ["engineering", "release", "support"],
    }


def test_sparse_stakeholder_data_falls_back_to_engineering_defaults():
    checkpoints = build_rollout_communication_plan(
        {
            "id": "plan-sparse",
            "implementation_brief_id": "brief-sparse",
            "milestones": [{"name": "Only Step"}],
            "tasks": [
                {
                    "id": "task-risk",
                    "title": "Touch release path",
                    "description": "Update the release path.",
                    "risk_level": "critical",
                    "depends_on": [],
                    "acceptance_criteria": [],
                }
            ],
        }
    )

    assert [(item.source_type, item.audience, item.channel_labels) for item in checkpoints] == [
        ("milestone", ("engineering",), ("release", "product")),
        ("task", ("engineering",), ("engineering", "release")),
    ]


def test_checkpoint_ordering_and_serialization_are_stable():
    checkpoints = build_rollout_communication_plan(_plan(), _brief())
    payload = rollout_communication_checkpoints_to_dicts(checkpoints)

    assert [checkpoint.checkpoint_id for checkpoint in checkpoints] == [
        "comm-milestone-foundation",
        "comm-milestone-release",
        "comm-task-task-rollout",
    ]
    assert payload == [checkpoint.to_dict() for checkpoint in checkpoints]
    assert list(payload[0]) == [
        "checkpoint_id",
        "source_type",
        "source_id",
        "audience",
        "trigger",
        "summary",
        "dependencies",
        "expected_evidence",
        "channel_labels",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_accepts_execution_plan_and_implementation_brief_models():
    plan_model = ExecutionPlan.model_validate(_plan())
    brief_model = ImplementationBrief.model_validate(_brief())

    checkpoints = build_rollout_communication_plan(plan_model, brief_model)

    assert checkpoints[1].summary == (
        "Announce milestone 'Release' for Rollout Communication Brief: "
        "Coordinate customer rollout."
    )
    assert checkpoints[2].source_id == "task-rollout"
    assert checkpoints[2].channel_labels == ("engineering", "release", "support")


def _plan():
    return {
        "id": "plan-rollout-comms",
        "implementation_brief_id": "brief-rollout-comms",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [
            {
                "id": "Foundation",
                "description": "Prepare contracts and validation.",
                "dependencies": ["API contract review"],
                "metadata": {
                    "stakeholders": ["platform team"],
                    "audience": ["api owners"],
                    "tags": ["backend"],
                    "expected_evidence": ["contract review approved"],
                },
            },
            {
                "name": "Release",
                "description": "Coordinate customer rollout.",
                "metadata": {
                    "owners": ["release lead"],
                    "stakeholders": ["support lead"],
                    "tags": ["customer", "release"],
                },
            },
        ],
        "test_strategy": "Run pytest",
        "status": "draft",
        "metadata": {"stakeholders": ["release manager"]},
        "tasks": [
            {
                "id": "task-contracts",
                "title": "Finalize contracts",
                "description": "Document integration contracts.",
                "milestone": "Foundation",
                "owner_type": "agent",
                "depends_on": [],
                "files_or_modules": ["src/contracts.py"],
                "acceptance_criteria": ["Contract tests pass"],
                "risk_level": "medium",
                "status": "pending",
                "metadata": {"labels": ["api"]},
            },
            {
                "id": "task-rollout",
                "title": "Enable staged rollout",
                "description": "Enable customer rollout controls.",
                "milestone": "Release",
                "owner_type": "agent",
                "depends_on": ["task-contracts"],
                "files_or_modules": ["src/rollout.py"],
                "acceptance_criteria": ["Smoke test passes"],
                "risk_level": "high",
                "test_command": "poetry run pytest tests/test_rollout.py",
                "status": "pending",
                "metadata": {
                    "owners": ["release lead"],
                    "stakeholders": ["support lead"],
                    "tags": ["customer", "release"],
                    "expected_evidence": ["rollback tested"],
                    "risk": "Customer-facing availability risk",
                },
            },
        ],
    }


def _brief():
    return {
        "id": "brief-rollout-comms",
        "source_brief_id": "source-rollout-comms",
        "title": "Rollout Communication Brief",
        "target_user": "operators",
        "buyer": "Engineering",
        "problem_statement": "Execution needs coordinated rollout communication.",
        "mvp_goal": "Keep stakeholders aligned during release.",
        "scope": ["Build communication checkpoints"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": ["Payments API"],
        "risks": ["Customer-impacting rollout needs support coverage"],
        "validation_plan": "Run regression and rollout smoke tests.",
        "definition_of_done": ["Stakeholders receive release updates"],
        "status": "planned",
    }
