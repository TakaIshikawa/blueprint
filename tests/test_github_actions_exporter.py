from pathlib import Path

import yaml
from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_rendered_export
from blueprint.exporters.github_actions import GitHubActionsExporter
from blueprint.store import Store, init_db


def test_github_actions_exporter_writes_parseable_workflow_yaml(tmp_path):
    output_path = tmp_path / "workflow.yml"

    GitHubActionsExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    payload = yaml.safe_load(output_path.read_text())
    assert payload["name"] == "Blueprint CI: Test Brief"
    assert set(payload) >= {"name", "on", "jobs"}
    assert set(payload["jobs"]) == {"setup_project", "build_api", "validation"}
    assert payload["jobs"]["setup_project"]["steps"][1]["run"] == "poetry install"


def test_github_actions_exporter_maps_commands_and_dependencies(tmp_path):
    output_path = tmp_path / "workflow.yml"

    GitHubActionsExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    jobs = yaml.safe_load(output_path.read_text())["jobs"]
    assert [step["run"] for step in jobs["build_api"]["steps"][1:]] == [
        "python -m pytest tests/test_api.py",
        "ruff check src/app.py",
    ]
    assert jobs["build_api"]["needs"] == "setup_project"
    assert jobs["validation"]["needs"] == ["setup_project", "build_api"]
    assert jobs["validation"]["steps"][1]["run"] == "pytest"


def test_github_actions_exporter_falls_back_to_validation_job_from_test_strategy(tmp_path):
    output_path = tmp_path / "workflow.yml"
    plan = _execution_plan()
    for task in plan["tasks"]:
        task["metadata"] = {}

    GitHubActionsExporter().export(plan, _implementation_brief(), str(output_path))

    jobs = yaml.safe_load(output_path.read_text())["jobs"]
    assert set(jobs) == {"validation"}
    assert "needs" not in jobs["validation"]
    assert jobs["validation"]["steps"][1]["run"] == "pytest"


def test_github_actions_exporter_uses_basic_test_job_without_strategy_or_commands(tmp_path):
    output_path = tmp_path / "workflow.yml"
    plan = _execution_plan()
    plan["test_strategy"] = None
    for task in plan["tasks"]:
        task["metadata"] = {}

    GitHubActionsExporter().export(plan, _implementation_brief(), str(output_path))

    jobs = yaml.safe_load(output_path.read_text())["jobs"]
    assert set(jobs) == {"test"}
    assert jobs["test"]["steps"][1]["run"] == "pytest"


def test_github_actions_export_validation_passes_for_rendered_export(tmp_path):
    output_path = tmp_path / "workflow.yml"
    plan = _execution_plan()

    GitHubActionsExporter().export(plan, _implementation_brief(), str(output_path))

    findings = validate_rendered_export(
        target="github-actions",
        artifact_path=output_path,
        execution_plan=plan,
        implementation_brief=_implementation_brief(),
    )

    assert findings == []


def test_github_actions_export_validation_rejects_unknown_needs(tmp_path):
    output_path = tmp_path / "workflow.yml"
    output_path.write_text(
        yaml.safe_dump(
            {
                "name": "Bad workflow",
                "on": {"push": {}},
                "jobs": {
                    "test": {
                        "runs-on": "ubuntu-latest",
                        "needs": ["missing"],
                        "steps": [{"run": "pytest"}],
                    }
                },
            },
            sort_keys=False,
        )
    )

    findings = validate_rendered_export(
        target="github-actions",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == ["github_actions.unknown_needs"]


def test_export_preview_run_and_validate_support_github_actions(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "blueprint.db"
    export_dir = tmp_path / "exports"
    Path(".blueprint.yaml").write_text(
        f"""
database:
  path: {db_path}
exports:
  output_dir: {export_dir}
"""
    )
    blueprint_config.reload_config()

    store = init_db(str(db_path))
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    preview = CliRunner().invoke(cli, ["export", "preview", plan_id, "--target", "github-actions"])
    assert preview.exit_code == 0, preview.output
    preview_payload = yaml.safe_load(preview.output)
    assert "setup_project" in preview_payload["jobs"]

    run = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "github-actions"])
    assert run.exit_code == 0, run.output
    output_path = export_dir / f"{plan_id}-github-actions.yml"
    assert output_path.exists()
    assert "Exported to:" in run.output

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "github-actions"
    assert records[0]["export_format"] == "yaml"
    assert records[0]["output_path"] == str(output_path)

    validate = CliRunner().invoke(
        cli,
        ["export", "validate", plan_id, "--target", "github-actions"],
    )
    assert validate.exit_code == 0, validate.output
    assert "Validation passed for github-actions" in validate.output


def _execution_plan(include_tasks=True):
    plan = {
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
    if include_tasks:
        plan["tasks"] = _tasks()
    return plan


def _tasks():
    return [
        {
            "id": "setup-project",
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
            "metadata": {"command": "poetry install"},
        },
        {
            "id": "build-schema",
            "title": "Build schema",
            "description": "Create persistence schema",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/schema.py"],
            "acceptance_criteria": ["Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "build-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["setup-project", "build-schema", "task-missing"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "status": "pending",
            "metadata": {
                "commands": [
                    "python -m pytest tests/test_api.py",
                    "ruff check src/app.py",
                ]
            },
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
        "problem_statement": "Need GitHub Actions workflows for execution plans",
        "mvp_goal": "Run execution plans in CI",
        "product_surface": "CLI",
        "scope": ["GitHub Actions exporter"],
        "non_goals": ["GitHub API integration"],
        "assumptions": ["Developers use GitHub Actions"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Invalid workflow YAML"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Workflow exports as GitHub Actions YAML"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
