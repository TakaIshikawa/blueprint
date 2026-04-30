from blueprint.domain.models import ExecutionPlan
from blueprint.task_branch_plan import (
    TaskBranchCandidate,
    generate_task_branch_plan,
    task_branch_plan_to_dict,
)


def test_independent_tasks_are_assigned_to_separate_branch_candidates():
    candidates = generate_task_branch_plan(
        _plan_with_tasks(
            [
                _task("task-api", "Build API", files_or_modules=["src/api.py"]),
                _task("task-ui", "Build UI", files_or_modules=["src/ui.py"]),
            ]
        )
    )

    assert [candidate.branch_name for candidate in candidates] == [
        "task/task-api-build-api",
        "task/task-ui-build-ui",
    ]
    assert [candidate.task_ids for candidate in candidates] == [
        ("task-api",),
        ("task-ui",),
    ]
    assert all(candidate.suggested_base == "main" for candidate in candidates)
    assert all(candidate.conflict_warnings == () for candidate in candidates)
    assert isinstance(candidates[0], TaskBranchCandidate)


def test_dependency_chain_with_shared_files_is_grouped_on_one_branch():
    candidates = generate_task_branch_plan(
        _plan_with_tasks(
            [
                _task(
                    "task-model",
                    "Build Model",
                    files_or_modules=["src/model.py"],
                ),
                _task(
                    "task-api",
                    "Build API",
                    depends_on=["task-model"],
                    files_or_modules=["src/model.py", "src/api.py"],
                ),
                _task(
                    "task-tests",
                    "Add Tests",
                    depends_on=["task-api"],
                    files_or_modules=["src/api.py", "tests/test_api.py"],
                ),
            ]
        )
    )

    assert len(candidates) == 1
    assert candidates[0].branch_name == "task/task-model-build-model"
    assert candidates[0].task_ids == ("task-model", "task-api", "task-tests")
    assert candidates[0].suggested_base == "main"
    assert candidates[0].conflict_warnings == ()
    assert candidates[0].reason.startswith(
        "grouped dependent tasks:task-model -> task-api -> task-tests"
    )


def test_dependent_high_risk_tasks_are_ordered_with_dependency_branch_as_base():
    candidates = generate_task_branch_plan(
        _plan_with_tasks(
            [
                _task(
                    "task-schema",
                    "Update Schema",
                    files_or_modules=["src/schema.py"],
                    risk_level="high",
                    estimated_complexity="high",
                ),
                _task(
                    "task-ui",
                    "Update UI",
                    depends_on=["task-schema"],
                    files_or_modules=["src/ui.py"],
                    risk_level="medium",
                    estimated_complexity="medium",
                ),
            ]
        )
    )

    assert [candidate.task_ids for candidate in candidates] == [
        ("task-schema",),
        ("task-ui",),
    ]
    assert candidates[0].suggested_base == "main"
    assert candidates[1].suggested_base == "task/task-schema-update-schema"
    assert candidates[1].reason.startswith("ordered after dependencies:task-schema")


def test_shared_file_overlap_produces_conflict_warnings_without_grouping_independent_tasks():
    candidates = generate_task_branch_plan(
        _plan_with_tasks(
            [
                _task("task-api", "Build API", files_or_modules=["src/shared.py"]),
                _task("task-ui", "Build UI", files_or_modules=["src/shared.py"]),
            ]
        )
    )

    assert [candidate.task_ids for candidate in candidates] == [
        ("task-api",),
        ("task-ui",),
    ]
    assert candidates[0].conflict_warnings == (
        "shares files_or_modules with task-ui: src/shared.py",
    )
    assert candidates[1].conflict_warnings == (
        "shares files_or_modules with task-api: src/shared.py",
    )


def test_branch_names_are_stable_and_reuse_task_branch_name_conventions():
    candidates = generate_task_branch_plan(
        ExecutionPlan.model_validate(
            _plan_with_tasks(
                [
                    _task(
                        "task/bad@{id}.lock",
                        "Fix: unsafe ~ branch^ name?",
                        files_or_modules=["src/app.py"],
                    ),
                    _task(
                        "task-long",
                        "Implement a long title that must truncate deterministically",
                        files_or_modules=["src/other.py"],
                    ),
                ]
            )
        ),
        max_length=48,
    )
    payload = task_branch_plan_to_dict(candidates)

    assert candidates[0].branch_name == "task/task-bad-id-lock-fix-unsafe-branch-name"
    assert candidates[1].branch_name == "task/task-long-implement-a-long-title-that-must"
    assert payload == [candidate.to_dict() for candidate in candidates]
    assert list(payload[0]) == [
        "branch_name",
        "task_ids",
        "reason",
        "conflict_warnings",
        "suggested_base",
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


def _task(
    task_id,
    title,
    *,
    depends_on=None,
    files_or_modules=None,
    milestone="Foundation",
    estimated_complexity="medium",
    risk_level="medium",
):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [] if depends_on is None else depends_on,
        "files_or_modules": ["src/app.py"] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": estimated_complexity,
        "risk_level": risk_level,
        "status": "pending",
    }
