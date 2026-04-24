import json
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.exporters.gitlab_issues import GitLabIssuesExporter
from blueprint.store import Store, init_db


def test_gitlab_issue_exporter_writes_one_issue_per_task(tmp_path):
    output_path = tmp_path / "gitlab-issues.json"

    result_path = GitLabIssuesExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    assert result_path == str(output_path)
    issues = json.loads(output_path.read_text())
    assert [issue["metadata"]["task_id"] for issue in issues] == [
        "task-setup",
        "task-api",
    ]


def test_gitlab_issue_exporter_renders_issue_fields_and_planning_context(tmp_path):
    output_path = tmp_path / "gitlab-issues.json"

    GitLabIssuesExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    setup, api = json.loads(output_path.read_text())

    assert setup["title"] == "Setup project"
    assert setup["labels"] == ["codex", "agent", "Foundation", "setup", "backend"]
    assert setup["milestone"] == "Foundation"
    assert setup["weight"] == 1
    assert "due_date" not in setup

    assert api["title"] == "Build API"
    assert api["labels"] == ["codex", "agent", "Delivery", "gitlab", "api", "planning"]
    assert api["milestone"] == "Delivery"
    assert api["weight"] == 8
    assert api["due_date"] == "2026-05-15"
    assert api["metadata"]["depends_on"] == ["task-setup"]
    assert "## Planning Context" in api["description"]
    assert "- Plan ID: `plan-test`" in api["description"]
    assert "## Acceptance Criteria\n- API returns data" in api["description"]
    assert "## Files/Modules\n- `src/app.py`\n- `src/schema.py`" in api["description"]
    assert "## Dependencies\n- Blocked by Blueprint task `task-setup`" in api["description"]


def test_gitlab_issue_exporter_writes_empty_array_for_empty_task_plan(tmp_path):
    output_path = tmp_path / "gitlab-issues.json"

    GitLabIssuesExporter().export(
        _execution_plan(include_tasks=False),
        _implementation_brief(),
        str(output_path),
    )

    assert json.loads(output_path.read_text()) == []


def test_gitlab_issue_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "gitlab-issues")

    assert result.passed
    assert result.findings == []


def test_gitlab_issue_validation_passes_for_empty_task_plan():
    result = validate_export(
        _execution_plan(include_tasks=False),
        _implementation_brief(),
        "gitlab-issues",
    )

    assert result.passed
    assert result.findings == []


def test_gitlab_issue_validation_rejects_incomplete_export(monkeypatch):
    class BadGitLabIssuesExporter:
        def get_extension(self) -> str:
            return ".json"

        def export(self, execution_plan, implementation_brief, output_path):
            Path(output_path).write_text(
                json.dumps(
                    [
                        {
                            "title": "Setup project",
                            "description": "Missing required fields",
                            "metadata": {"task_id": "task-setup"},
                        }
                    ]
                )
            )
            return output_path

    monkeypatch.setattr(
        "blueprint.exporters.export_validation.create_exporter",
        lambda target: BadGitLabIssuesExporter(),
    )

    result = validate_export(_execution_plan(), _implementation_brief(), "gitlab-issues")

    assert not result.passed
    assert any(
        finding.code == "gitlab_issues.task_count_mismatch" for finding in result.findings
    )
    assert any(finding.code == "gitlab_issues.issue.missing_key" for finding in result.findings)
    assert any(
        finding.code == "gitlab_issues.task_occurrence_mismatch"
        and "task-api appears 0 times" in finding.message
        for finding in result.findings
    )


def test_gitlab_issue_validation_rejects_malformed_json(tmp_path):
    output_path = tmp_path / "gitlab-issues.json"
    output_path.write_text("{not json")

    findings = validate_rendered_export(
        target="gitlab-issues",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == ["gitlab_issues.invalid_json"]


def test_export_run_gitlab_issues_writes_file_and_records_export(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "gitlab-issues"])

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-gitlab-issues.json"
    assert output_path.exists()

    issues = json.loads(output_path.read_text())
    assert issues[0]["title"] == "Setup project"

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "gitlab-issues"
    assert records[0]["export_format"] == "json"
    assert records[0]["output_path"] == str(output_path)


def test_export_validate_gitlab_issues_supports_positional_target_json(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "validate", plan_id, "gitlab-issues", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == {"target": "gitlab-issues", "passed": True, "findings": []}


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
        "target_engine": "codex",
        "target_repo": "acme/widgets",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Delivery", "description": "Ship the API"},
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
            "status": "pending",
            "metadata": {"labels": ["setup"], "components": ["backend"]},
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/app.py", "src/schema.py"],
            "acceptance_criteria": ["API returns data", "Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "pending",
            "metadata": {
                "gitlab_weight": 8,
                "gitlab_due_date": "2026-05-15",
                "gitlab_labels": ["gitlab"],
                "labels": ["api"],
                "tags": ["planning"],
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
        "problem_statement": "Need a GitLab issue export",
        "mvp_goal": "Export execution tasks for GitLab import",
        "product_surface": "CLI",
        "scope": ["GitLab issue exporter"],
        "non_goals": ["GitLab API integration"],
        "assumptions": ["Import tooling can map JSON fields"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": ["CLI export command"],
        "risks": ["Dependency notes must stay reconstructable"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as GitLab issue JSON"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
