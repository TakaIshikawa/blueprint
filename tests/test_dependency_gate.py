import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.dependency_gate import audit_dependency_gate
from blueprint.cli import cli
from blueprint.store import init_db


def test_dependency_gate_marks_task_ready_with_completed_dependencies():
    plan = _plan_with_tasks(
        [
            _task("task-setup", status="completed"),
            _task("task-api", status="pending", depends_on=["task-setup"]),
        ]
    )

    result = audit_dependency_gate(plan)

    assert result.passed is True
    assert result.ready_count == 1
    assert result.to_dict()["ready_task_ids"] == ["task-api"]
    assert result.tasks[0].to_dict()["reasons"] == [
        {
            "code": "all_dependencies_completed",
            "message": (
                "Task task-api is ready; all dependencies are completed: task-setup."
            ),
            "error": False,
        }
    ]


def test_dependency_gate_marks_task_ready_without_dependencies():
    plan = _plan_with_tasks([_task("task-setup", status="pending")])

    result = audit_dependency_gate(plan)

    assert result.ready_count == 1
    assert result.tasks[0].status == "ready"
    assert result.tasks[0].reasons[0].code == "all_dependencies_completed"


def test_dependency_gate_marks_task_waiting_for_incomplete_dependency():
    plan = _plan_with_tasks(
        [
            _task("task-setup", status="in_progress"),
            _task("task-api", status="pending", depends_on=["task-setup"]),
        ]
    )

    result = audit_dependency_gate(plan)

    assert result.passed is True
    assert result.waiting_count == 1
    assert result.to_dict()["waiting_task_ids"] == ["task-api"]
    assert result.tasks[0].to_dict()["reasons"] == [
        {
            "code": "dependency_incomplete",
            "dependency_id": "task-setup",
            "dependency_status": "in_progress",
            "message": (
                "Task task-api is waiting for dependency task-setup to complete; "
                "current status is in_progress."
            ),
            "error": False,
        }
    ]


def test_dependency_gate_marks_task_blocked_by_blocked_dependency():
    plan = _plan_with_tasks(
        [
            _task("task-setup", status="blocked"),
            _task("task-api", status="pending", depends_on=["task-setup"]),
        ]
    )

    result = audit_dependency_gate(plan)

    assert result.passed is False
    assert result.blocked_count == 1
    assert result.to_dict()["blocked_task_ids"] == ["task-api"]
    assert result.tasks[0].reasons[0].code == "dependency_blocked"


def test_dependency_gate_marks_task_blocked_by_skipped_dependency():
    plan = _plan_with_tasks(
        [
            _task("task-setup", status="skipped"),
            _task("task-api", status="pending", depends_on=["task-setup"]),
        ]
    )

    result = audit_dependency_gate(plan)

    assert result.passed is False
    assert result.blocked_count == 1
    assert result.tasks[0].to_dict()["reasons"] == [
        {
            "code": "dependency_skipped",
            "dependency_id": "task-setup",
            "dependency_status": "skipped",
            "message": "Task task-api is blocked by skipped dependency task-setup.",
            "error": False,
        }
    ]


def test_dependency_gate_reports_unknown_dependency_as_error():
    plan = _plan_with_tasks(
        [_task("task-api", status="pending", depends_on=["task-missing"])]
    )

    result = audit_dependency_gate(plan)

    assert result.passed is False
    assert result.blocked_count == 1
    assert result.error_count == 1
    assert result.tasks[0].to_dict()["reasons"] == [
        {
            "code": "unknown_dependency",
            "dependency_id": "task-missing",
            "message": "Task task-api depends on unknown task task-missing.",
            "error": True,
        }
    ]


def test_dependency_gate_cli_outputs_machine_readable_json(tmp_path, monkeypatch):
    _seed_plan(
        tmp_path,
        monkeypatch,
        [
            _task("task-setup", status="completed"),
            _task("task-api", status="pending", depends_on=["task-setup"]),
            _task("task-ui", status="pending", depends_on=["task-api"]),
            _task("task-deploy", status="pending", depends_on=["task-missing"]),
        ],
    )

    result = CliRunner().invoke(
        cli,
        ["task", "dependency-gate", "plan-test", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-test"
    assert payload["summary"] == {
        "blocked": 1,
        "errors": 1,
        "ready": 1,
        "tasks": 3,
        "waiting": 1,
    }
    assert payload["ready_task_ids"] == ["task-api"]
    assert payload["waiting_task_ids"] == ["task-ui"]
    assert payload["blocked_task_ids"] == ["task-deploy"]
    assert payload["tasks"][2]["reasons"][0] == {
        "code": "unknown_dependency",
        "dependency_id": "task-missing",
        "message": "Task task-deploy depends on unknown task task-missing.",
        "error": True,
    }


def test_dependency_gate_cli_human_output_summarizes_ready_and_blocked_counts(
    tmp_path,
    monkeypatch,
):
    _seed_plan(
        tmp_path,
        monkeypatch,
        [
            _task("task-setup", status="completed"),
            _task("task-api", status="pending", depends_on=["task-setup"]),
            _task("task-deploy", status="pending", depends_on=["task-missing"]),
        ],
    )

    result = CliRunner().invoke(cli, ["task", "dependency-gate", "plan-test"])

    assert result.exit_code == 1
    assert "Dependency gate audit: plan-test" in result.output
    assert "Result: failed (1 ready, 0 waiting, 1 blocked, 1 errors)" in result.output
    assert "Ready tasks:" in result.output
    assert "task-api" in result.output
    assert "Blocked tasks:" in result.output
    assert "[unknown_dependency] Task task-deploy depends on unknown task task-missing." in (
        result.output
    )


def _seed_plan(tmp_path, monkeypatch, tasks):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), tasks)


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path}
"""
    )
    blueprint_config.reload_config()


def _plan_with_tasks(tasks):
    plan = _execution_plan()
    plan["tasks"] = tasks
    return plan


def _task(task_id, *, status, depends_on=None):
    return {
        "id": task_id,
        "title": task_id.replace("-", " ").title(),
        "description": f"Implement {task_id}",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{task_id} works"],
        "estimated_complexity": "medium",
        "status": status,
    }


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Set up the project"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need task management",
        "mvp_goal": "Expose tasks in the CLI",
        "product_surface": "CLI",
        "scope": ["Task commands"],
        "non_goals": ["Task execution"],
        "validation_plan": "Run dependency gate tests",
        "definition_of_done": ["Dependency gate CLI works"],
        "assumptions": ["SQLite is available"],
        "risks": ["Task drift"],
        "status": "planned",
    }
