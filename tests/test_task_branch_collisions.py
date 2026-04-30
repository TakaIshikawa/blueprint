from blueprint.task_branch_collisions import (
    TaskBranchCollision,
    detect_task_branch_collisions,
    task_branch_collisions_to_dicts,
)


def test_duplicate_generated_branch_names_are_grouped_with_all_tasks():
    collisions = detect_task_branch_collisions(
        _plan_with_tasks(
            [
                _task("task/a", "Build API!"),
                _task("task?a", "Build API?"),
                _task("task:a", "Build API."),
            ]
        )
    )

    assert len(collisions) == 1
    collision = collisions[0]
    assert isinstance(collision, TaskBranchCollision)
    assert collision.generated_branch == "task/task-a-build-api"
    assert collision.task_ids == ("task/a", "task?a", "task:a")
    assert collision.titles == ("Build API!", "Build API?", "Build API.")
    assert collision.suggested_unique_branch == {
        "task/a": "task/task-a-build-api",
        "task?a": "task/task-a-build-api-2",
        "task:a": "task/task-a-build-api-3",
    }


def test_metadata_branch_names_participate_in_collision_detection():
    collisions = detect_task_branch_collisions(
        _plan_with_tasks(
            [
                _task("task-api", "Build API", metadata={"branch_name": "task/shared"}),
                _task("task-docs", "Write docs", metadata={"git_branch": "task/shared"}),
                _task("task-ui", "Build UI"),
            ]
        )
    )

    assert len(collisions) == 1
    assert collisions[0].generated_branch == "task/shared"
    assert collisions[0].task_ids == ("task-api", "task-docs")
    assert collisions[0].suggested_unique_branch == {
        "task-api": "task/task-api-build-api",
        "task-docs": "task/task-docs-write-docs",
    }


def test_punctuation_only_branch_differences_are_flagged_as_confusing():
    collisions = detect_task_branch_collisions(
        _plan_with_tasks(
            [
                _task("task-api", "Build API", metadata={"branch_name": "task/build.api"}),
                _task("task-docs", "Write docs", metadata={"branch": "task/build-api"}),
            ]
        )
    )

    assert len(collisions) == 1
    assert collisions[0].task_ids == ("task-api", "task-docs")
    assert collisions[0].suggested_unique_branch == {
        "task-api": "task/task-api-build-api",
        "task-docs": "task/task-docs-write-docs",
    }


def test_duplicate_titles_are_flagged_even_when_task_ids_make_generated_names_unique():
    collisions = detect_task_branch_collisions(
        _plan_with_tasks(
            [
                _task("task-api", "Build API"),
                _task("task-service", "Build API"),
            ]
        )
    )

    assert len(collisions) == 1
    assert collisions[0].task_ids == ("task-api", "task-service")
    assert collisions[0].titles == ("Build API", "Build API")
    assert collisions[0].suggested_unique_branch == {
        "task-api": "task/task-api-build-api",
        "task-service": "task/task-service-build-api",
    }


def test_unique_task_sets_return_empty_collision_list():
    assert (
        detect_task_branch_collisions(
            _plan_with_tasks(
                [
                    _task("task-api", "Build API"),
                    _task("task-ui", "Build UI"),
                    _task("task-docs", "Write docs"),
                ]
            )
        )
        == []
    )


def test_task_branch_collisions_serialize_to_dicts():
    collisions = [
        TaskBranchCollision(
            generated_branch="task/shared",
            task_ids=("task-a", "task-b"),
            titles=("A", "B"),
            suggested_unique_branch={
                "task-a": "task/task-a-a",
                "task-b": "task/task-b-b",
            },
        )
    ]

    assert task_branch_collisions_to_dicts(collisions) == [
        {
            "generated_branch": "task/shared",
            "task_ids": ["task-a", "task-b"],
            "titles": ["A", "B"],
            "suggested_unique_branch": {
                "task-a": "task/task-a-a",
                "task-b": "task/task-b-b",
            },
        }
    ]


def _plan_with_tasks(tasks):
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
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


def _task(task_id, title, *, metadata=None):
    task = {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
