from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_rendered_export
from blueprint.exporters.slack_digest import SlackDigestExporter
from blueprint.store import Store, init_db


def test_slack_digest_exporter_renders_compact_digest(tmp_path):
    output_path = tmp_path / "slack.md"

    SlackDigestExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    digest = output_path.read_text()
    assert digest.startswith("# Slack Digest: plan-test\n")
    assert "*Plan:* `plan-test` - Test Brief" in digest
    assert "*Implementation Brief:* <blueprint://implementation-brief/ib-test|Test Brief>" in digest
    assert "## Status Counts" in digest
    assert "*pending:* 2" in digest
    assert "*in_progress:* 1" in digest
    assert "*completed:* 1" in digest
    assert "*blocked:* 1" in digest
    assert "*skipped:* 1" in digest


def test_slack_digest_groups_ready_blocked_and_recommended_tasks(tmp_path):
    output_path = tmp_path / "slack.md"

    SlackDigestExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    digest = output_path.read_text()
    assert "## Ready Tasks\n- *Foundation*\n  - `task-api` Build API" in digest
    assert "- *Interface*\n  - `task-docs` Write docs" in digest
    assert (
        "## Blocked Tasks\n- *Interface*\n"
        "  - `task-copy` Write copy - Waiting for product direction"
        in digest
    )
    assert "## Next Recommended Tasks" in digest
    assert "  - `task-api` Build API (deps satisfied: task-setup, task-schema)" in digest
    assert "  - `task-docs` Write docs (no dependencies)" in digest
    assert "`task-ui`" not in digest


def test_export_run_slack_digest_writes_file_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "slack-digest"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-slack-digest.md"
    assert output_path.exists()
    assert "# Slack Digest:" in output_path.read_text()

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "slack-digest"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)


def test_slack_digest_rendered_validation_fails_for_missing_required_content(tmp_path):
    output_path = tmp_path / "bad-slack.md"
    output_path.write_text(
        "# Slack Digest: plan-test\n"
        "*Plan:* `plan-test`\n"
        "## Status Counts\n"
        "*pending:* 2 | *in_progress:* 1 | *completed:* 1 | *blocked:* 1 | *skipped:* 1\n"
        "## Ready Tasks\n"
        "## Next Recommended Tasks\n"
    )

    findings = validate_rendered_export(
        target="slack-digest",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    codes = [finding.code for finding in findings]
    assert "markdown.missing_heading" in codes
    assert "slack_digest.missing_blocked_task" in codes


def test_slack_digest_rendered_validation_passes_for_rendered_export(tmp_path):
    output_path = tmp_path / "slack.md"
    SlackDigestExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    findings = validate_rendered_export(
        target="slack-digest",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert findings == []


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
        "status": "in_progress",
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
            "id": "task-schema",
            "title": "Build schema",
            "description": "Create persistence schema",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/schema.py"],
            "acceptance_criteria": ["Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "skipped",
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-schema"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "task-copy",
            "title": "Write copy",
            "description": "Draft interface copy",
            "milestone": "Interface",
            "owner_type": "human",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["Copy is approved"],
            "estimated_complexity": "low",
            "status": "blocked",
            "blocked_reason": "Waiting for product direction",
        },
        {
            "id": "task-ui",
            "title": "Build UI",
            "description": "Create the interface",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI renders"],
            "estimated_complexity": "medium",
            "status": "in_progress",
        },
        {
            "id": "task-docs",
            "title": "Write docs",
            "description": "Document the digest output",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["README.md"],
            "acceptance_criteria": ["Docs describe usage"],
            "estimated_complexity": "low",
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
        "problem_statement": "Need a Markdown digest export",
        "mvp_goal": "Export Slack digests for execution plans",
        "product_surface": "CLI",
        "scope": ["Slack digest exporter"],
        "non_goals": ["Full status report"],
        "assumptions": ["Tasks already have statuses"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Missing task status data"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Digest contains progress and blockers"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
