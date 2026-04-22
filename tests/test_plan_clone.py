import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import Store, init_db


def test_clone_execution_plan_rewrites_dependencies_to_cloned_task_ids(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    cloned_plan_id = store.clone_execution_plan("plan-source")
    cloned_plan = store.get_execution_plan(cloned_plan_id)

    task_id_map = cloned_plan["metadata"]["lineage"]["task_id_map"]
    cloned_by_title = {task["title"]: task for task in cloned_plan["tasks"]}

    assert cloned_plan_id != "plan-source"
    assert set(task_id_map) == {"task-setup", "task-api", "task-ui"}
    assert set(task_id_map.values()).isdisjoint(task_id_map.keys())
    assert cloned_by_title["Build API"]["depends_on"] == [task_id_map["task-setup"]]
    assert cloned_by_title["Build UI"]["depends_on"] == [task_id_map["task-api"]]
    assert all(task["execution_plan_id"] == cloned_plan_id for task in cloned_plan["tasks"])


def test_clone_execution_plan_resets_statuses_by_default(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(status="in_progress"), _tasks())

    cloned_plan_id = store.clone_execution_plan("plan-source")
    cloned_plan = store.get_execution_plan(cloned_plan_id)

    assert cloned_plan["status"] == "draft"
    assert {task["status"] for task in cloned_plan["tasks"]} == {"pending"}
    assert all("blocked_reason" not in task["metadata"] for task in cloned_plan["tasks"])


def test_clone_execution_plan_can_preserve_statuses(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(status="in_progress"), _tasks())

    cloned_plan_id = store.clone_execution_plan("plan-source", reset_statuses=False)
    cloned_plan = store.get_execution_plan(cloned_plan_id)
    cloned_by_title = {task["title"]: task for task in cloned_plan["tasks"]}

    assert cloned_plan["status"] == "in_progress"
    assert cloned_by_title["Setup project"]["status"] == "completed"
    assert cloned_by_title["Build API"]["status"] == "blocked"
    assert cloned_by_title["Build API"]["metadata"]["blocked_reason"] == "Waiting on setup"
    assert cloned_by_title["Build UI"]["status"] == "in_progress"


def test_clone_execution_plan_preserves_plan_fields_and_adds_lineage(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    cloned_plan_id = store.clone_execution_plan("plan-source")
    cloned_plan = store.get_execution_plan(cloned_plan_id)
    source_plan = store.get_execution_plan("plan-source")

    assert cloned_plan["implementation_brief_id"] == source_plan["implementation_brief_id"]
    assert cloned_plan["target_engine"] == source_plan["target_engine"]
    assert cloned_plan["target_repo"] == source_plan["target_repo"]
    assert cloned_plan["project_type"] == source_plan["project_type"]
    assert cloned_plan["milestones"] == source_plan["milestones"]
    assert cloned_plan["test_strategy"] == source_plan["test_strategy"]
    assert cloned_plan["handoff_prompt"] == source_plan["handoff_prompt"]
    assert cloned_plan["metadata"]["owner"] == "planning"
    assert cloned_plan["metadata"]["lineage"]["revised_from_plan_id"] == "plan-original"
    assert cloned_plan["metadata"]["lineage"]["cloned_from_plan_id"] == "plan-source"
    assert cloned_plan["metadata"]["lineage"]["task_id_map"]["task-api"].startswith("task-")


def test_plan_clone_cli_outputs_text_and_json(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(status="queued"), _tasks())

    text_result = CliRunner().invoke(cli, ["plan", "clone", "plan-source"])

    assert text_result.exit_code == 0, text_result.output
    assert "Cloned execution plan plan-source to plan-" in text_result.output
    assert "Statuses: reset" in text_result.output

    json_result = CliRunner().invoke(
        cli,
        ["plan", "clone", "plan-source", "--preserve-statuses", "--json"],
    )

    assert json_result.exit_code == 0, json_result.output
    payload = json.loads(json_result.output)
    assert payload["id"].startswith("plan-")
    assert payload["cloned_from_plan_id"] == "plan-source"
    assert payload["statuses_reset"] is False
    assert set(payload["task_id_map"]) == {"task-setup", "task-api", "task-ui"}

    cloned_plan = Store(str(tmp_path / "blueprint.db")).get_execution_plan(payload["id"])
    assert cloned_plan["status"] == "queued"


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


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need plan cloning",
        "mvp_goal": "Branch plans before experimentation",
        "product_surface": "CLI",
        "scope": ["Plan cloning"],
        "non_goals": ["Plan execution"],
        "assumptions": ["Original plan exists"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Losing lineage"],
        "validation_plan": "Run plan clone tests",
        "definition_of_done": ["Cloned plans link to originals"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _execution_plan(status="ready"):
    return {
        "id": "plan-source",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Interface", "description": "Build the user-facing flow"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": status,
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "metadata": {
            "owner": "planning",
            "lineage": {"revised_from_plan_id": "plan-original"},
        },
    }


def _tasks():
    return [
        {
            "id": "task-setup",
            "title": "Setup project",
            "description": "Create the baseline project structure",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["pyproject.toml"],
            "acceptance_criteria": ["Project installs"],
            "estimated_complexity": "low",
            "status": "completed",
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "status": "blocked",
            "metadata": {"blocked_reason": "Waiting on setup"},
        },
        {
            "id": "task-ui",
            "title": "Build UI",
            "description": "Render the interface",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI renders"],
            "estimated_complexity": "medium",
            "status": "in_progress",
        },
    ]
