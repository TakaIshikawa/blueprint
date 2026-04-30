from blueprint.domain.models import ExecutionPlan
from blueprint.merge_queue import (
    MergeQueueEntry,
    merge_queue_to_dicts,
    recommend_merge_queue,
)


def test_tasks_with_unsatisfied_dependencies_are_held_behind_prerequisites():
    entries = recommend_merge_queue(
        _plan_with_tasks(
            [
                _task("task-schema", "Update Schema", status="pending"),
                _task(
                    "task-api",
                    "Update API",
                    depends_on=["task-schema"],
                    status="completed",
                ),
                _task(
                    "task-ui",
                    "Update UI",
                    depends_on=["missing-task"],
                    status="completed",
                ),
            ]
        )
    )

    assert [(entry.task_id, entry.recommended_action, entry.merge_after) for entry in entries] == [
        ("task-schema", "review", ()),
        ("task-api", "hold", ("task-schema",)),
        ("task-ui", "hold", ("missing-task",)),
    ]


def test_overlapping_file_paths_list_peer_task_conflicts():
    entries = recommend_merge_queue(
        _plan_with_tasks(
            [
                _task(
                    "task-api",
                    "Update API",
                    files_or_modules=["src/shared.py", "src/api.py"],
                    status="completed",
                ),
                _task(
                    "task-ui",
                    "Update UI",
                    files_or_modules=["src/shared.py", "src/ui.py"],
                    status="completed",
                ),
                _task(
                    "task-docs",
                    "Update Docs",
                    files_or_modules=["docs/guide.md"],
                    status="completed",
                ),
            ]
        )
    )

    conflicts = {entry.task_id: entry.conflicts_with for entry in entries}

    assert conflicts == {
        "task-api": ("task-ui",),
        "task-ui": ("task-api",),
        "task-docs": (),
    }


def test_high_risk_tasks_receive_manual_review_notes_before_merge():
    entries = recommend_merge_queue(
        _plan_with_tasks(
            [
                _task(
                    "task-migration",
                    "Run Migration",
                    risk_level="high",
                    test_command="poetry run pytest tests/test_migration.py::test_upgrade",
                    status="completed",
                )
            ]
        )
    )

    assert entries[0].recommended_action == "merge"
    assert entries[0].review_notes == (
        "Manual review required before merge: high risk",
        "Test command appears narrow; consider broader regression coverage",
    )


def test_ready_independent_tasks_retain_deterministic_plan_order():
    entries = recommend_merge_queue(
        ExecutionPlan.model_validate(
            _plan_with_tasks(
                [
                    _task("task-third", "Third", status="pending"),
                    _task("task-first", "First", status="completed"),
                    _task("task-second", "Second", status="pending"),
                ]
            )
        )
    )
    payload = merge_queue_to_dicts(entries)

    assert [entry.task_id for entry in entries] == [
        "task-third",
        "task-first",
        "task-second",
    ]
    assert [entry.recommended_action for entry in entries] == [
        "review",
        "merge",
        "review",
    ]
    assert isinstance(entries[0], MergeQueueEntry)
    assert payload == [entry.to_dict() for entry in entries]
    assert list(payload[0]) == [
        "task_id",
        "recommended_action",
        "merge_after",
        "conflicts_with",
        "review_notes",
    ]


def test_in_progress_and_blocked_tasks_are_not_merge_queue_candidates():
    entries = recommend_merge_queue(
        _plan_with_tasks(
            [
                _task("task-progress", "In Progress", status="in_progress"),
                _task("task-blocked", "Blocked", status="blocked"),
                _task("task-complete", "Complete", status="completed"),
            ]
        )
    )

    assert [entry.task_id for entry in entries] == ["task-complete"]


def _plan_with_tasks(tasks):
    return {
        "id": "plan-merge-queue",
        "implementation_brief_id": "ib-merge-queue",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Build", "description": "Build the implementation"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    depends_on=None,
    files_or_modules=None,
    risk_level="medium",
    test_command="poetry run pytest",
    status="pending",
):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [] if depends_on is None else depends_on,
        "files_or_modules": ["src/app.py"] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": "medium",
        "risk_level": risk_level,
        "test_command": test_command,
        "status": status,
    }
