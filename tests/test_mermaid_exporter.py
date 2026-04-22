from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.mermaid import MermaidExporter
from blueprint.store import Store, init_db


def test_mermaid_exporter_renders_dependencies_across_milestones(tmp_path):
    output_path = tmp_path / "graph.mmd"

    exporter = MermaidExporter()
    exporter.export(_execution_plan(), _implementation_brief(), str(output_path))

    graph = output_path.read_text()

    assert graph.startswith("flowchart TD\n")
    assert 'subgraph milestone_1_Foundation["Foundation"]' in graph
    assert 'subgraph milestone_2_Interface["Interface"]' in graph
    assert 'task_setup["task-setup<br/>Setup project<br/>Status: completed"]' in graph
    assert 'task_api["task-api<br/>Build API<br/>Status: in_progress"]' in graph
    assert 'task_ui["task-ui<br/>Build UI<br/>Status: pending"]' in graph
    assert "task_setup --> task_api" in graph
    assert "task_api --> task_ui" in graph
    assert ":::status_completed" in graph
    assert ":::status_in_progress" in graph
    assert ":::status_pending" in graph


def test_export_graph_cli_writes_mermaid_file_and_records_export(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "blueprint.db"
    export_path = tmp_path / "plan-graph.mmd"
    Path(".blueprint.yaml").write_text(
        f"""
database:
  path: {db_path}
exports:
  output_dir: {tmp_path}
"""
    )
    blueprint_config.reload_config()

    store = init_db(str(db_path))
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "graph", plan_id, "--output", str(export_path)])

    assert result.exit_code == 0, result.output
    assert "Exported graph to:" in result.output
    assert export_path.exists()
    assert "task_setup --> task_api" in export_path.read_text()

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "mermaid"
    assert records[0]["export_format"] == "mermaid"
    assert records[0]["output_path"] == str(export_path)


def _execution_plan(include_tasks=True):
    plan = {
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

    if include_tasks:
        plan["tasks"] = _tasks()

    return plan


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
        "problem_statement": "Need a graph export",
        "mvp_goal": "Export plans as Mermaid graphs",
        "product_surface": "CLI",
        "scope": ["Mermaid exporter"],
        "non_goals": ["Rendering Mermaid"],
        "assumptions": ["Mermaid consumers parse flowcharts"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Invalid graph syntax"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Graph contains dependencies"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
