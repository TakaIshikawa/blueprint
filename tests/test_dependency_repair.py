import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.dependency_repair import suggest_dependency_repairs
from blueprint.cli import cli
from blueprint.store import init_db


def test_dependency_repair_suggests_replace_for_close_missing_dependency():
    plan = _plan_with_tasks()
    plan["tasks"][2]["depends_on"] = ["task-ap"]

    result = suggest_dependency_repairs(plan)

    assert result.ok is False
    assert [suggestion.to_dict() for suggestion in result.suggestions] == [
        {
            "action": "replace_dependency",
            "task_id": "task-ui",
            "dependency_id": "task-ap",
            "confidence": 0.86,
            "rationale": (
                "Dependency task-ap is not in the plan; replace it with task-api, "
                "the closest existing task ID."
            ),
            "replacement_dependency_id": "task-api",
            "affected_task_ids": ["task-ui", "task-api"],
        }
    ]


def test_dependency_repair_suggests_remove_for_unknown_dependency_without_match():
    plan = _plan_with_tasks()
    plan["tasks"][2]["depends_on"] = ["external-launch-gate"]

    result = suggest_dependency_repairs(plan)

    assert [suggestion.to_dict() for suggestion in result.suggestions] == [
        {
            "action": "remove_dependency",
            "task_id": "task-ui",
            "dependency_id": "external-launch-gate",
            "confidence": 0.76,
            "rationale": (
                "Dependency external-launch-gate is not in the plan and no clear "
                "replacement task ID was found; remove it from depends_on."
            ),
            "affected_task_ids": ["task-ui"],
        }
    ]


def test_dependency_repair_suggests_self_dependency_removal():
    plan = _plan_with_tasks()
    plan["tasks"][1]["depends_on"] = ["task-api"]

    result = suggest_dependency_repairs(plan)

    assert [suggestion.to_dict() for suggestion in result.suggestions] == [
        {
            "action": "remove_dependency",
            "task_id": "task-api",
            "dependency_id": "task-api",
            "confidence": 1.0,
            "rationale": (
                "Task task-api cannot depend on itself; remove task-api from depends_on."
            ),
            "affected_task_ids": ["task-api"],
        }
    ]


def test_dependency_repair_suggests_deterministic_cycle_split():
    plan = _plan_with_tasks()
    plan["tasks"][0]["depends_on"] = ["task-ui"]

    result = suggest_dependency_repairs(plan)

    assert [suggestion.to_dict() for suggestion in result.suggestions] == [
        {
            "action": "split_cycle",
            "task_id": "task-api",
            "dependency_id": "task-setup",
            "confidence": 0.72,
            "rationale": (
                "Dependency cycle detected; remove or replace task-setup from "
                "task-api.depends_on to split task-api -> task-setup -> task-ui -> task-api."
            ),
            "affected_task_ids": ["task-api", "task-setup", "task-ui"],
        }
    ]


def test_dependency_repair_suggests_terminal_blocker_removal_without_mutating_plan():
    plan = _plan_with_tasks()
    plan["tasks"][0]["status"] = "completed"

    result = suggest_dependency_repairs(plan)

    assert [suggestion.to_dict() for suggestion in result.suggestions] == [
        {
            "action": "remove_dependency",
            "task_id": "task-api",
            "dependency_id": "task-setup",
            "confidence": 0.58,
            "rationale": (
                "Dependency task-setup is already completed; remove it from "
                "task-api.depends_on if it no longer blocks task execution."
            ),
            "affected_task_ids": ["task-api", "task-setup"],
        }
    ]
    assert plan["tasks"][1]["depends_on"] == ["task-setup"]


def test_dependency_repair_cli_outputs_stable_json(tmp_path, monkeypatch):
    tasks = _tasks()
    tasks[2]["depends_on"] = ["task-ap"]
    _seed_plan(tmp_path, monkeypatch, tasks)

    result = CliRunner().invoke(cli, ["plan", "dependency-repair", "plan-test", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-test"
    assert payload["ok"] is False
    assert payload["summary"] == {"suggestions": 1}
    assert payload["suggestions"][0]["action"] == "replace_dependency"
    assert payload["suggestions"][0]["task_id"] == "task-ui"
    assert payload["suggestions"][0]["dependency_id"] == "task-ap"
    assert payload["suggestions"][0]["confidence"] == 0.86
    assert payload["suggestions"][0]["rationale"]
    assert payload["suggestions"][0]["replacement_dependency_id"] == "task-api"


def test_dependency_repair_cli_human_output_is_actionable_and_nonzero(tmp_path, monkeypatch):
    tasks = _tasks()
    tasks[1]["depends_on"] = ["task-api"]
    _seed_plan(tmp_path, monkeypatch, tasks)

    result = CliRunner().invoke(cli, ["plan", "dependency-repair", "plan-test"])

    assert result.exit_code == 1
    assert "Dependency repair suggestions: plan-test" in result.output
    assert "Result: repairs suggested (1 suggestions)" in result.output
    assert "[remove_dependency] Task task-api: remove task-api" in result.output
    assert "Rationale: Task task-api cannot depend on itself" in result.output


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


def _plan_with_tasks():
    plan = _execution_plan()
    plan["tasks"] = _tasks()
    return plan


def _execution_plan():
    return {
        "id": "plan-test",
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
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
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
            "status": "pending",
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
            "status": "in_progress",
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
            "acceptance_criteria": ["UI displays API data"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
    ]


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
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Incorrect status updates"],
        "validation_plan": "Run task CLI tests",
        "definition_of_done": ["CLI lists, inspects, and updates tasks"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
