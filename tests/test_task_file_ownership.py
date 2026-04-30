import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_file_ownership import (
    SharedTaskPath,
    TaskFileOwnershipReport,
    TaskOwnerClarification,
    TaskPathOwnership,
    estimate_task_file_ownership,
    task_file_ownership_report_to_dict,
)


def test_reports_per_task_ownership_status_for_every_file_entry():
    report = estimate_task_file_ownership(
        _plan(
            [
                _task(
                    "task-api",
                    "Build API",
                    owner_type="agent",
                    suggested_engine="codex",
                    files_or_modules=["src/api.py", "./src/schema.py"],
                ),
                _task(
                    "task-docs",
                    "Write docs",
                    owner_type=None,
                    suggested_engine="codex",
                    files_or_modules=["docs/api.md"],
                ),
            ]
        )
    )

    assert isinstance(report, TaskFileOwnershipReport)
    assert report.path_count == 3
    assert report.owned_paths == ("src/api.py", "src/schema.py")
    assert report.unowned_paths == ("docs/api.md",)
    assert report.task_paths == (
        TaskPathOwnership(
            task_id="task-api",
            title="Build API",
            path="src/api.py",
            ownership_status="owned",
            owner_type="agent",
            suggested_engine="codex",
            metadata_owner=None,
            owner_label="agent: codex",
            shared_with_task_ids=(),
        ),
        TaskPathOwnership(
            task_id="task-api",
            title="Build API",
            path="src/schema.py",
            ownership_status="owned",
            owner_type="agent",
            suggested_engine="codex",
            metadata_owner=None,
            owner_label="agent: codex",
            shared_with_task_ids=(),
        ),
        TaskPathOwnership(
            task_id="task-docs",
            title="Write docs",
            path="docs/api.md",
            ownership_status="unowned",
            owner_type=None,
            suggested_engine="codex",
            metadata_owner=None,
            owner_label=None,
            shared_with_task_ids=(),
        ),
    )


def test_marks_paths_touched_by_multiple_owners_as_shared_paths():
    report = estimate_task_file_ownership(
        _plan(
            [
                _task(
                    "task-api",
                    "Build API",
                    owner_type="agent",
                    suggested_engine="codex",
                    files_or_modules=["src/app.py"],
                ),
                _task(
                    "task-review",
                    "Review API",
                    owner_type="human",
                    suggested_engine="manual",
                    files_or_modules=["src/app.py"],
                ),
                _task(
                    "task-tests",
                    "Test API",
                    owner_type="agent",
                    suggested_engine="codex",
                    files_or_modules=["src/app.py"],
                ),
            ]
        )
    )

    assert report.shared_paths == (
        SharedTaskPath(
            path="src/app.py",
            owners=("agent: codex", "human: manual"),
            task_ids=("task-api", "task-review", "task-tests"),
        ),
    )
    assert [task_path.ownership_status for task_path in report.task_paths] == [
        "shared",
        "shared",
        "shared",
    ]
    assert report.task_paths[0].shared_with_task_ids == ("task-review", "task-tests")


def test_metadata_owner_assigns_paths_when_owner_type_is_missing():
    report = estimate_task_file_ownership(
        _plan(
            [
                _task(
                    "task-ui",
                    "Build UI",
                    owner_type=None,
                    suggested_engine="codex",
                    files_or_modules=["src/ui.py"],
                    metadata={"owner": "frontend-owner"},
                ),
                _task(
                    "task-docs",
                    "Write docs",
                    owner_type=None,
                    suggested_engine="manual",
                    files_or_modules=["docs/ui.md"],
                    metadata={"responsible": "docs-owner"},
                ),
            ]
        )
    )

    assert report.owned_paths == ("docs/ui.md", "src/ui.py")
    assert report.unowned_paths == ()
    assert [task_path.metadata_owner for task_path in report.task_paths] == [
        "frontend-owner",
        "docs-owner",
    ]
    assert report.tasks_needing_owner_clarification == ()


def test_flags_tasks_with_files_but_no_owner_type_or_metadata_owner():
    report = estimate_task_file_ownership(
        _plan(
            [
                _task(
                    "task-unknown",
                    "Clarify owner",
                    owner_type=" ",
                    suggested_engine="codex",
                    files_or_modules=["src/unknown.py", "tests/test_unknown.py"],
                    metadata={"labels": ["backend"]},
                ),
                _task(
                    "task-empty",
                    "No files",
                    owner_type=None,
                    files_or_modules=[],
                ),
            ]
        )
    )

    assert report.unowned_paths == ("src/unknown.py", "tests/test_unknown.py")
    assert report.tasks_needing_owner_clarification == (
        TaskOwnerClarification(
            task_id="task-unknown",
            title="Clarify owner",
            paths=("src/unknown.py", "tests/test_unknown.py"),
            suggested_engine="codex",
        ),
    )


def test_accepts_execution_plan_models_and_serializes_stably():
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    "Build model path",
                    owner_type="agent",
                    suggested_engine="codex",
                    files_or_modules=["src/model.py"],
                )
            ]
        )
    )

    report = estimate_task_file_ownership(plan_model)
    payload = task_file_ownership_report_to_dict(report)

    assert payload == report.to_dict()
    assert list(payload) == [
        "plan_id",
        "task_count",
        "path_count",
        "owned_paths",
        "unowned_paths",
        "shared_paths",
        "task_paths",
        "tasks_needing_owner_clarification",
    ]
    assert list(payload["task_paths"][0]) == [
        "task_id",
        "title",
        "path",
        "ownership_status",
        "owner_type",
        "suggested_engine",
        "metadata_owner",
        "owner_label",
        "shared_with_task_ids",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks):
    return {
        "id": "plan-file-ownership",
        "implementation_brief_id": "brief-file-ownership",
        "target_repo": "example/repo",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    owner_type="agent",
    suggested_engine="codex",
    files_or_modules=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "owner_type": owner_type,
        "suggested_engine": suggested_engine,
        "depends_on": [],
        "files_or_modules": (
            ["src/blueprint/task_file_ownership.py"]
            if files_or_modules is None
            else files_or_modules
        ),
        "acceptance_criteria": ["Tests pass"],
        "risk_level": "low",
        "status": "pending",
        "metadata": metadata or {},
    }
