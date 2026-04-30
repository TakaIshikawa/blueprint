import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_ci_pipeline_impact import (
    TaskCiPipelineImpactPlan,
    build_task_ci_pipeline_impact_plan,
    recommend_task_ci_pipeline_impacts,
    task_ci_pipeline_impact_plan_to_dict,
    task_ci_pipeline_impact_plan_to_markdown,
)


def test_github_actions_file_produces_workflow_recommendation():
    result = build_task_ci_pipeline_impact_plan(
        _plan(
            [
                _task(
                    "task-actions",
                    title="Update CI workflow matrix",
                    description="Change GitHub Actions runner permissions and job matrix.",
                    files_or_modules=[
                        ".github/workflows/ci.yml",
                        ".github/workflows/ci.yml",
                    ],
                    acceptance_criteria=["Workflow syntax is validated."],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.task_id == "task-actions"
    assert recommendation.surface == "github_actions"
    assert "Workflow syntax" in recommendation.failure_mode
    assert recommendation.validation_command == (
        "actionlint .github/workflows/*.yml .github/workflows/*.yaml"
    )
    assert recommendation.owner_role == "CI owner"
    assert recommendation.evidence == (
        "files_or_modules: .github/workflows/ci.yml",
        "title: Update CI workflow matrix",
        "description: Change GitHub Actions runner permissions and job matrix.",
        "acceptance_criteria[0]: Workflow syntax is validated.",
    )
    assert result.ci_task_ids == ("task-actions",)
    assert result.no_impact_task_ids == ()


def test_pyproject_and_poetry_changes_are_packaging_impacts():
    result = build_task_ci_pipeline_impact_plan(
        _plan(
            [
                _task(
                    "task-packaging",
                    title="Update poetry build backend",
                    description="Change pyproject dependencies and refresh the lockfile.",
                    files_or_modules=["pyproject.toml", "poetry.lock"],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.surface == "python_packaging"
    assert recommendation.failure_mode.startswith("Dependency resolution")
    assert recommendation.validation_command == "poetry check && poetry lock --check"
    assert recommendation.owner_role == "build owner"
    assert result.summary["surface_counts"]["python_packaging"] == 1


def test_docker_changes_are_container_build_impacts():
    result = build_task_ci_pipeline_impact_plan(
        _plan(
            [
                _task(
                    "task-docker",
                    title="Update container base image",
                    description="Switch Docker image build to a new base image.",
                    files_or_modules=["Dockerfile", "docker/entrypoint.sh"],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.surface == "container_build"
    assert "Docker context" in recommendation.failure_mode
    assert recommendation.validation_command == "docker build ."
    assert recommendation.owner_role == "platform owner"


def test_test_runner_config_changes_are_test_runner_impacts():
    result = build_task_ci_pipeline_impact_plan(
        _plan(
            [
                _task(
                    "task-tests",
                    title="Tighten pytest coverage config",
                    description="Update addopts and junit output for the CI test runner.",
                    files_or_modules=["pytest.ini", ".coveragerc"],
                    test_command="poetry run pytest",
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.surface == "test_runner_config"
    assert "Test discovery" in recommendation.failure_mode
    assert recommendation.validation_command == "poetry run pytest -o addopts=''"
    assert recommendation.owner_role == "test owner"


def test_make_pre_commit_and_release_automation_are_detected_in_stable_order():
    result = build_task_ci_pipeline_impact_plan(
        _plan(
            [
                _task(
                    "task-build-hooks-release",
                    title="Update make target, pre-commit hooks, and release automation",
                    description="Change make ci, pre-commit stages, and semantic-release publishing.",
                    files_or_modules=[
                        "Makefile",
                        ".pre-commit-config.yaml",
                        ".github/workflows/release.yml",
                        ".releaserc.json",
                    ],
                )
            ]
        )
    )

    assert [item.surface for item in result.recommendations] == [
        "github_actions",
        "make_targets",
        "pre_commit",
        "release_automation",
    ]
    assert _recommendation(result, "make_targets").validation_command == "make -n test"
    assert _recommendation(result, "pre_commit").validation_command == (
        "poetry run pre-commit run --all-files"
    )
    assert _recommendation(result, "release_automation").owner_role == "release owner"


def test_non_ci_tasks_are_marked_no_impact_without_recommendations():
    result = build_task_ci_pipeline_impact_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard empty state",
                    description="Clarify user-facing copy in the dashboard.",
                    files_or_modules=["src/ui/empty_state.py"],
                )
            ]
        )
    )

    assert result.recommendations == ()
    assert result.ci_task_ids == ()
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary == {
        "task_count": 1,
        "ci_task_count": 0,
        "recommendation_count": 0,
        "surface_counts": {
            "github_actions": 0,
            "test_runner_config": 0,
            "python_packaging": 0,
            "container_build": 0,
            "make_targets": 0,
            "pre_commit": 0,
            "release_automation": 0,
        },
    }


def test_iterable_task_input_and_single_task_alias_are_supported():
    iterable = build_task_ci_pipeline_impact_plan(
        [
            _task("task-one", title="Adjust workflow", files_or_modules=[".github/workflows/checks.yaml"]),
            _task("task-two", title="Edit docs", files_or_modules=["docs/readme.md"]),
        ]
    )
    task = ExecutionTask.model_validate(
        _task(
            "task-single",
            title="Update pre-commit hook versions",
            files_or_modules=[".pre-commit-config.yaml"],
        )
    )
    single = recommend_task_ci_pipeline_impacts(task)

    assert iterable.plan_id is None
    assert iterable.ci_task_ids == ("task-one",)
    assert iterable.no_impact_task_ids == ("task-two",)
    assert isinstance(single, TaskCiPipelineImpactPlan)
    assert single.plan_id is None
    assert single.recommendations[0].surface == "pre_commit"


def test_execution_plan_input_serializes_without_mutation():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Update package metadata",
                description="Change package metadata in pyproject.",
                files_or_modules=["pyproject.toml"],
            )
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_task_ci_pipeline_impact_plan(model)
    payload = task_ci_pipeline_impact_plan_to_dict(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["recommendations"]
    assert list(payload) == [
        "plan_id",
        "recommendations",
        "ci_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "surface",
        "failure_mode",
        "validation_command",
        "owner_role",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_markdown_formatting_and_empty_markdown():
    result = build_task_ci_pipeline_impact_plan(
        _plan(
            [
                _task(
                    "task-docker",
                    title="Update Dockerfile",
                    description="Change Docker build arguments.",
                    files_or_modules=["Dockerfile"],
                )
            ]
        )
    )
    markdown = task_ci_pipeline_impact_plan_to_markdown(result)
    empty = build_task_ci_pipeline_impact_plan({"id": "plan-empty", "tasks": []})

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task CI Pipeline Impact Plan: plan-ci")
    assert (
        "| Task | Surface | Failure Mode | Validation Command | Owner Role |\n"
        "| --- | --- | --- | --- | --- |"
    ) in markdown
    assert "| `task-docker` | container_build | Docker context" in markdown
    assert "`docker build .` | platform owner |" in markdown
    assert empty.to_markdown() == (
        "# Task CI Pipeline Impact Plan: plan-empty\n\nNo CI pipeline impacts were derived."
    )


def _recommendation(result, surface):
    return next(item for item in result.recommendations if item.surface == surface)


def _plan(tasks):
    return {
        "id": "plan-ci",
        "implementation_brief_id": "brief-ci",
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
    metadata=None,
    tags=None,
    test_command=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    if test_command is not None:
        task["test_command"] = test_command
    return task
