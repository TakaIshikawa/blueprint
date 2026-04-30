import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_review_packet import (
    TaskReviewPacket,
    build_task_review_packets,
    task_review_packets_to_dict,
)


def test_builds_review_packets_for_normal_dict_plan():
    packets = build_task_review_packets(
        _plan(
            [
                _task(
                    "task-api",
                    "Update API",
                    files_or_modules=["src/features/api.py"],
                    acceptance_criteria=["API returns the new status field"],
                    depends_on=["task-schema"],
                    test_command="pytest tests/test_api.py",
                )
            ]
        )
    )

    assert list(packets) == ["task-api"]
    assert packets["task-api"] == TaskReviewPacket(
        task_id="task-api",
        title="Update API",
        expected_files=("src/features/api.py",),
        acceptance_criteria=("API returns the new status field",),
        dependencies=("task-schema",),
        risk_indicators=(),
        validation_commands=("pytest tests/test_api.py",),
        reviewer_focus_areas=(
            "Confirm changed files match the task scope and validation command output.",
        ),
    )


def test_sparse_tasks_get_stable_defaults_and_missing_data_focus():
    packets = build_task_review_packets(
        {
            "id": "plan-sparse",
            "tasks": [
                {
                    "title": "Review sparse task",
                    "files_or_modules": [],
                    "acceptance_criteria": [],
                    "test_command": "",
                }
            ],
        }
    )

    packet = packets["task-1"]

    assert packet.task_id == "task-1"
    assert packet.title == "Review sparse task"
    assert packet.expected_files == ()
    assert packet.acceptance_criteria == ()
    assert packet.dependencies == ()
    assert packet.validation_commands == ()
    assert packet.risk_indicators == (
        "missing acceptance criteria",
        "missing validation command",
    )
    assert packet.reviewer_focus_areas == (
        "Require concrete validation evidence because no task-level test command is listed.",
        "Clarify expected behavior because acceptance criteria are missing.",
    )


def test_accepts_execution_plan_model_and_serializes_to_json_compatible_dicts():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-metadata",
                    "Use metadata",
                    files_or_modules=["src/features/widget.py"],
                    acceptance_criteria=["Widget renders the empty state"],
                    test_command=None,
                    metadata={
                        "expected_files": ["tests/test_widget.py"],
                        "validation_commands": {
                            "lint": ["poetry run ruff check"],
                            "test": ["pytest tests/test_widget.py"],
                        },
                    },
                )
            ]
        )
    )

    first = build_task_review_packets(model)
    second = build_task_review_packets(model)
    payload = task_review_packets_to_dict(first)

    assert payload == task_review_packets_to_dict(second)
    assert payload == {
        "task-metadata": {
            "task_id": "task-metadata",
            "title": "Use metadata",
            "expected_files": ["src/features/widget.py", "tests/test_widget.py"],
            "acceptance_criteria": ["Widget renders the empty state"],
            "dependencies": [],
            "risk_indicators": [],
            "validation_commands": [
                "pytest tests/test_widget.py",
                "poetry run ruff check",
            ],
            "reviewer_focus_areas": [
                "Confirm changed files match the task scope and validation command output.",
            ],
        }
    }
    assert list(payload["task-metadata"]) == [
        "task_id",
        "title",
        "expected_files",
        "acceptance_criteria",
        "dependencies",
        "risk_indicators",
        "validation_commands",
        "reviewer_focus_areas",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_high_risk_paths_dependencies_and_weak_acceptance_generate_focus_areas():
    packets = build_task_review_packets(
        _plan(
            [
                _task(
                    "task-review",
                    "Update risky surfaces",
                    files_or_modules=[
                        " config/app.yml ",
                        "src/schema/user.graphql",
                        "migrations/202605010101_add_user.sql",
                        "src/core/service.py",
                    ],
                    acceptance_criteria=["Works"],
                    depends_on=["task-api", "task-db"],
                    risk_level="high",
                    test_command=None,
                )
            ]
        )
    )

    packet = packets["task-review"]

    assert packet.expected_files == (
        "config/app.yml",
        "src/schema/user.graphql",
        "migrations/202605010101_add_user.sql",
        "src/core/service.py",
    )
    assert packet.risk_indicators == (
        "config path: config/app.yml",
        "schema path: src/schema/user.graphql",
        "database path: migrations/202605010101_add_user.sql",
        "shared path: src/core/service.py",
        "high risk task metadata",
        "depends on 2 task(s)",
        "weak acceptance criteria",
        "missing validation command",
    )
    assert packet.reviewer_focus_areas == (
        "Inspect risky file changes for configuration, data, schema, or shared-impact regressions.",
        "Verify dependency contracts and task ordering across upstream work.",
        "Require concrete validation evidence because no task-level test command is listed.",
        "Translate broad acceptance criteria into observable review checks.",
        "Give the implementation a deeper review because task risk is elevated.",
    )


def test_output_order_is_deterministic_and_task_keyed():
    plan = _plan(
        [
            _task(
                "task-b",
                "Second in list",
                files_or_modules={"src/b.py", "src/a.py"},
                acceptance_criteria=["B is observable"],
            ),
            _task(
                "task-a",
                "First dependency",
                files_or_modules=["src/a.py"],
                acceptance_criteria=["A is observable"],
            ),
        ]
    )

    first = task_review_packets_to_dict(build_task_review_packets(plan))
    second = task_review_packets_to_dict(build_task_review_packets(plan))

    assert list(first) == ["task-b", "task-a"]
    assert first == second
    assert first["task-b"]["expected_files"] == ["src/a.py", "src/b.py"]


def _plan(tasks):
    return {
        "id": "plan-review-packet",
        "implementation_brief_id": "brief-review-packet",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [{"name": "Implementation"}],
        "test_strategy": "Run focused validation",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "metadata": {},
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    files_or_modules,
    acceptance_criteria,
    depends_on=None,
    risk_level="low",
    test_command="pytest tests/test_task_review_packet.py",
    metadata=None,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-review-packet",
        "title": title,
        "description": f"Implement {title}",
        "milestone": "Implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria,
        "estimated_complexity": "medium",
        "risk_level": risk_level,
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
    }
