import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import init_db


def test_dependency_matrix_outputs_deterministic_json(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch, _tasks())

    result = CliRunner().invoke(
        cli,
        ["plan", "dependency-matrix", "plan-test", "--format", "json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == {
        "plan_id": "plan-test",
        "implementation_brief_id": "ib-test",
        "nodes": [
            {
                "id": "task-api",
                "title": "Build API",
                "status": "in_progress",
                "milestone": "Foundation",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "estimated_complexity": "medium",
            },
            {
                "id": "task-setup",
                "title": "Setup project",
                "status": "completed",
                "milestone": "Foundation",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "estimated_complexity": "low",
            },
            {
                "id": "task-ui",
                "title": "Build UI",
                "status": "pending",
                "milestone": "Interface",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "estimated_complexity": "medium",
            },
        ],
        "edges": [
            {"from": "task-setup", "to": "task-api"},
            {"from": "task-api", "to": "task-ui"},
            {"from": "task-setup", "to": "task-ui"},
        ],
        "blocked_by": {
            "task-api": ["task-setup"],
            "task-setup": [],
            "task-ui": ["task-api", "task-setup"],
        },
        "unblocks": {
            "task-api": ["task-ui"],
            "task-setup": ["task-api", "task-ui"],
            "task-ui": [],
        },
    }


def test_dependency_matrix_rejects_unknown_dependency_ids(tmp_path, monkeypatch):
    tasks = _tasks()
    tasks[1]["depends_on"] = ["task-missing"]
    _seed_plan(tmp_path, monkeypatch, tasks)

    result = CliRunner().invoke(
        cli,
        ["plan", "dependency-matrix", "plan-test", "--format", "json"],
    )

    assert result.exit_code != 0
    assert "Unknown dependency IDs found" in result.output
    assert "task-api: task-missing" in result.output


def test_dependency_matrix_output_creates_parent_directories(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch, _tasks())
    output_path = tmp_path / "exports" / "matrices" / "plan-test.json"

    result = CliRunner().invoke(
        cli,
        [
            "plan",
            "dependency-matrix",
            "plan-test",
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output == ""
    assert output_path.exists()
    payload = json.loads(output_path.read_text())
    assert payload["blocked_by"]["task-setup"] == []
    assert payload["unblocks"]["task-setup"] == ["task-api", "task-ui"]


def _seed_plan(tmp_path, monkeypatch, tasks):
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
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), tasks)


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
            "status": "in_progress",
        },
        {
            "id": "task-ui",
            "title": "Build UI",
            "description": "Render the interface",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-api"],
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
