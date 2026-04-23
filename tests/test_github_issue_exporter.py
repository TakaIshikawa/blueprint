import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export
from blueprint.exporters.github_issues import GitHubIssuesExporter
from blueprint.store import Store, init_db


def test_github_issue_exporter_writes_manifest_and_issue_drafts(tmp_path):
    output_dir = tmp_path / "plan-test-github-issues"

    result_path = GitHubIssuesExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_dir),
    )

    assert result_path == str(output_dir)
    assert output_dir.is_dir()
    assert (output_dir / "manifest.json").exists()
    assert (output_dir / "issues" / "001-task-setup.md").exists()
    assert (output_dir / "issues" / "002-task-api.md").exists()


def test_github_issue_exporter_renders_manifest_and_issue_content(tmp_path):
    output_dir = tmp_path / "plan-test-github-issues"

    GitHubIssuesExporter().export(_execution_plan(), _implementation_brief(), str(output_dir))

    manifest = json.loads((output_dir / "manifest.json").read_text())
    assert manifest["schema_version"] == "blueprint.github-issues.v1"
    assert manifest["repository"] == {
        "raw_target_repo": "acme/widgets",
        "owner": "acme",
        "name": "widgets",
        "full_name": "acme/widgets",
        "html_url": "https://github.com/acme/widgets",
        "issues_url": "https://github.com/acme/widgets/issues",
    }
    assert manifest["plan"]["id"] == "plan-test"
    assert manifest["milestone_groups"][0]["name"] == "Foundation"
    assert manifest["milestone_groups"][0]["issue_files"] == [
        "issues/001-task-setup.md",
        "issues/002-task-api.md",
    ]
    assert manifest["issues"][1]["labels"] == ["backend", "api"]
    assert manifest["issues"][1]["depends_on"] == ["task-setup"]

    issue = (output_dir / "issues" / "002-task-api.md").read_text()
    assert "# Build API" in issue
    assert "- Task ID: `task-api`" in issue
    assert "- Labels: backend, api" in issue
    assert "- Dependencies: task-setup" in issue
    assert "- API returns data" in issue
    assert "## Milestone Group" in issue


def test_github_issue_exporter_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "github-issues")

    assert result.passed
    assert result.findings == []


def test_export_preview_supports_github_issue_bundles(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "preview", plan_id, "--target", "github-issues"],
    )

    assert result.exit_code == 0, result.output
    assert "## manifest.json" in result.output
    assert "## issues/001-task-setup.md" in result.output
    assert Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id) == []


def test_export_run_writes_github_issue_bundle_and_records_export(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "github-issues"])

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-github-issues"
    assert output_path.is_dir()
    assert (output_path / "manifest.json").exists()
    assert (output_path / "issues" / "001-task-setup.md").exists()

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "github-issues"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)


def _setup_store(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "blueprint.db"
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {db_path}
exports:
  output_dir: {tmp_path / "exports"}
"""
    )
    blueprint_config.reload_config()
    return init_db(str(db_path))


def _execution_plan(include_tasks: bool = True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "relay",
        "target_repo": "acme/widgets",
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
            "metadata": {"labels": ["setup", "foundation"]},
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/app.py", "src/schema.py"],
            "acceptance_criteria": ["API returns data", "Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "pending",
            "metadata": {"labels": ["backend", "api"]},
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
        "problem_statement": "Need a GitHub issue bundle export",
        "mvp_goal": "Export tasks for GitHub issue drafts",
        "product_surface": "CLI",
        "scope": ["GitHub issue bundle exporter"],
        "non_goals": ["Issue submission automation"],
        "assumptions": ["Markdown consumers can follow links"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Plan and task dictionaries",
        "integration_points": ["CLI export command"],
        "risks": ["Task filenames must stay stable"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as Markdown issue drafts"],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
