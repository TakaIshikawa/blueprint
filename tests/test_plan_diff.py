import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.plan_diff import diff_execution_plans
from blueprint.cli import cli
from blueprint.store import init_db


def test_plan_diff_reports_added_removed_and_changed_items_in_stable_order(tmp_path, monkeypatch):
    left_plan = _plan(
        "plan-left",
        milestones=[
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Interface", "description": "Build the user-facing flow"},
        ],
        tasks=[
            _task("task-setup", "Setup project", "completed", milestone="Foundation"),
            _task(
                "task-api",
                "Build API",
                "pending",
                milestone="Foundation",
                depends_on=["task-setup"],
            ),
            _task("task-ui", "Build UI", "blocked", milestone="Interface", depends_on=["task-api"]),
        ],
    )
    right_plan = _plan(
        "plan-right",
        milestones=[
            {"name": "Foundation", "description": "Set up the project foundation"},
            {"name": "Delivery", "description": "Prepare the release"},
        ],
        tasks=[
            _task("task-setup-rev", "Setup project", "completed", milestone="Foundation"),
            _task(
                "task-api-rev",
                "Build API",
                "in_progress",
                milestone="Delivery",
                depends_on=["task-setup-rev", "task-docs"],
            ),
            _task("task-docs", "Write docs", "pending", milestone="Foundation"),
        ],
    )

    result = diff_execution_plans(left_plan, right_plan)

    assert result.left_plan_id == "plan-left"
    assert result.right_plan_id == "plan-right"
    assert [milestone["name"] for milestone in result.added_milestones] == ["Delivery"]
    assert [milestone["name"] for milestone in result.removed_milestones] == ["Interface"]
    assert [change.milestone_key for change in result.changed_milestones] == ["Foundation"]
    assert [task["id"] for task in result.added_tasks] == ["task-docs"]
    assert [task["id"] for task in result.removed_tasks] == ["task-ui"]
    assert [change.task_key for change in result.changed_tasks] == ["title:build api"]
    assert [change.left_task_id for change in result.changed_tasks] == ["task-api"]
    assert [change.right_task_id for change in result.changed_tasks] == ["task-api-rev"]
    assert [change.field for change in result.changed_tasks[0].changes] == [
        "milestone",
        "depends_on",
        "status",
    ]


def test_plan_diff_cli_outputs_json(tmp_path, monkeypatch):
    _seed_plans(tmp_path, monkeypatch)

    result = CliRunner().invoke(cli, ["plan", "diff", "plan-left", "plan-right", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["left_plan_id"] == "plan-left"
    assert payload["right_plan_id"] == "plan-right"
    assert payload["summary"] == {
        "added_milestones": 1,
        "removed_milestones": 1,
        "changed_milestones": 1,
        "added_tasks": 1,
        "removed_tasks": 1,
        "changed_tasks": 1,
    }
    assert [milestone["name"] for milestone in payload["milestones"]["added"]] == ["Delivery"]
    assert [task["id"] for task in payload["tasks"]["added"]] == ["task-docs"]
    assert payload["tasks"]["changed"][0]["task_key"] == "title:build api"
    assert payload["tasks"]["changed"][0]["left_task_id"] == "task-api"
    assert payload["tasks"]["changed"][0]["right_task_id"] == "task-api-rev"
    assert [change["field"] for change in payload["tasks"]["changed"][0]["changes"]] == [
        "milestone",
        "depends_on",
        "status",
    ]


def test_plan_diff_cli_outputs_human_readable_changes(tmp_path, monkeypatch):
    _seed_plans(tmp_path, monkeypatch)

    result = CliRunner().invoke(cli, ["plan", "diff", "plan-left", "plan-right"])

    assert result.exit_code == 0, result.output
    assert "Execution plan diff: plan-left -> plan-right" in result.output
    assert "Milestones added:" in result.output
    assert "Delivery" in result.output
    assert "Milestones removed:" in result.output
    assert "Interface" in result.output
    assert "Milestones changed:" in result.output
    assert "description: Set up the project -> Set up the project foundation" in result.output
    assert "Tasks added:" in result.output
    assert "task-docs" in result.output
    assert "Tasks removed:" in result.output
    assert "task-ui" in result.output
    assert "Tasks changed:" in result.output
    assert "milestone: Foundation -> Delivery" in result.output
    assert "depends_on: task-setup -> task-setup-rev, task-docs" in result.output
    assert "status: pending -> in_progress" in result.output


def test_plan_diff_cli_reports_missing_plan(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["plan", "diff", "plan-missing", "plan-right"])

    assert result.exit_code != 0
    assert "Execution plan not found: plan-missing" in result.output


def _seed_plans(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(
        _plan(
            "plan-left",
            milestones=[
                {"name": "Foundation", "description": "Set up the project"},
                {"name": "Interface", "description": "Build the user-facing flow"},
            ],
        ),
        _tasks_left(),
    )
    store.insert_execution_plan(
        _plan(
            "plan-right",
            milestones=[
                {"name": "Foundation", "description": "Set up the project foundation"},
                {"name": "Delivery", "description": "Prepare the release"},
            ],
        ),
        _tasks_right(),
    )


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


def _plan(plan_id, *, milestones=None, tasks=None):
    plan = {
        "id": plan_id,
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": milestones or [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Interface", "description": "Build the user-facing flow"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
    if tasks is not None:
        plan["tasks"] = tasks
    return plan


def _tasks_left():
    return [
        _task("task-setup", "Setup project", "completed", milestone="Foundation"),
        _task(
            "task-api",
            "Build API",
            "pending",
            milestone="Foundation",
            depends_on=["task-setup"],
        ),
        _task("task-ui", "Build UI", "blocked", milestone="Interface", depends_on=["task-api"]),
    ]


def _tasks_right():
    return [
        _task("task-setup-rev", "Setup project", "completed", milestone="Foundation"),
        _task(
            "task-api-rev",
            "Build API",
            "in_progress",
            milestone="Delivery",
            depends_on=["task-setup-rev", "task-docs"],
        ),
        _task("task-docs", "Write docs", "pending", milestone="Foundation"),
    ]


def _task(task_id, title, status, *, milestone, depends_on=None):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": "low",
        "status": status,
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
        "problem_statement": "Need plan diffing",
        "mvp_goal": "Expose plan diffs in the CLI",
        "product_surface": "CLI",
        "scope": ["Plan commands"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Incorrect diff output"],
        "validation_plan": "Run plan diff tests",
        "definition_of_done": ["CLI compares two execution plans"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
