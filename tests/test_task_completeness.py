import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.task_completeness import audit_task_completeness
from blueprint.cli import cli
from blueprint.store import init_db


def test_task_completeness_accepts_ready_plan():
    plan = _ready_plan()
    plan["tasks"][1]["suggested_engine"] = "codex"

    result = audit_task_completeness(plan)

    assert result.passed is True
    assert result.score == 100
    assert result.blocking_count == 0
    assert result.warning_count == 0
    assert result.findings == []
    assert [task.score for task in result.tasks] == [100, 100]


def test_task_completeness_reports_per_task_findings_and_score():
    plan = _ready_plan()
    plan["tasks"] = [
        {
            "id": "task-a",
            "title": "Build API",
            "description": " ",
            "depends_on": ["task-missing"],
            "files_or_modules": [],
            "acceptance_criteria": [],
            "suggested_engine": "",
            "status": "blocked",
        },
        {
            "id": "task-b",
            "title": "build api",
            "description": "Implement the endpoint",
            "depends_on": ["task-a"],
            "files_or_modules": ["src/api.py"],
            "acceptance_criteria": ["Endpoint returns data"],
            "suggested_engine": "codex",
            "status": "pending",
        },
    ]

    result = audit_task_completeness(plan)

    assert result.passed is False
    assert result.score == 45
    assert result.blocking_count == 4
    assert result.warning_count == 4
    assert _finding_codes(result) == {
        "missing_description",
        "empty_acceptance_criteria",
        "missing_files_or_modules",
        "unresolved_dependency",
        "duplicate_task_title",
        "missing_suggested_engine",
        "blocked_without_reason",
    }
    assert result.tasks[0].score == 0
    assert result.tasks[1].score == 90
    assert all(finding.task_id for finding in result.findings)
    assert all(finding.task_title for finding in result.findings)
    assert all(finding.remediation for finding in result.findings)


def test_task_completeness_cli_json_has_stable_machine_readable_output(
    tmp_path,
    monkeypatch,
):
    _seed_plan(tmp_path, monkeypatch)

    result = CliRunner().invoke(cli, ["task", "completeness", "plan-ready", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == {
        "findings": [
            {
                "code": "missing_suggested_engine",
                "field": "suggested_engine",
                "message": "Task task-api has no suggested execution engine.",
                "remediation": "Set the engine best suited for the task.",
                "severity": "warning",
                "task_id": "task-api",
                "task_title": "Build API",
            }
        ],
        "passed": True,
        "plan_id": "plan-ready",
        "score": 95,
        "summary": {
            "blocking": 0,
            "tasks": 2,
            "warning": 1,
        },
        "tasks": [
            {
                "findings": [],
                "ready": True,
                "score": 100,
                "summary": {
                    "blocking": 0,
                    "warning": 0,
                },
                "task_id": "task-setup",
                "title": "Setup project",
            },
            {
                "findings": [
                    {
                        "code": "missing_suggested_engine",
                        "field": "suggested_engine",
                        "message": "Task task-api has no suggested execution engine.",
                        "remediation": "Set the engine best suited for the task.",
                        "severity": "warning",
                        "task_id": "task-api",
                        "task_title": "Build API",
                    }
                ],
                "ready": True,
                "score": 90,
                "summary": {
                    "blocking": 0,
                    "warning": 1,
                },
                "task_id": "task-api",
                "title": "Build API",
            },
        ],
    }


def test_task_completeness_cli_human_output_lists_blocking_before_warnings(
    tmp_path,
    monkeypatch,
):
    plan = _ready_plan()
    plan["tasks"][0]["acceptance_criteria"] = []
    plan["tasks"][1]["suggested_engine"] = " "
    _seed_plan(tmp_path, monkeypatch, plan=plan)

    result = CliRunner().invoke(cli, ["task", "completeness", "plan-ready"])

    assert result.exit_code == 1, result.output
    assert "Task completeness audit: plan-ready" in result.output
    assert "Result: failed (score 82/100, 1 blocking, 1 warnings)" in result.output
    assert result.output.index("Blocking findings:") < result.output.index("Warnings:")
    assert "[empty_acceptance_criteria]" in result.output
    assert "[missing_suggested_engine]" in result.output
    assert "Task scores:" in result.output


def test_task_completeness_cli_rejects_missing_plan(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["task", "completeness", "missing-plan"])

    assert result.exit_code != 0
    assert "Execution plan not found: missing-plan" in result.output


def _finding_codes(result):
    return {finding.code for finding in result.findings}


def _seed_plan(tmp_path, monkeypatch, plan=None):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    plan_payload = plan or _ready_plan()
    store.insert_execution_plan(plan_payload, plan_payload["tasks"])


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


def _ready_plan():
    return {
        "id": "plan-ready",
        "implementation_brief_id": "ib-ready",
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
        "tasks": [
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
                "suggested_engine": "",
                "depends_on": ["task-setup"],
                "files_or_modules": ["src/app.py"],
                "acceptance_criteria": ["API returns data"],
                "estimated_complexity": "medium",
                "status": "pending",
            },
        ],
    }


def _implementation_brief():
    return {
        "id": "ib-ready",
        "source_brief_id": "sb-ready",
        "title": "Task CLI",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need task management",
        "mvp_goal": "Expose task commands in the CLI",
        "product_surface": "CLI",
        "scope": ["Task commands"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["CLI output changes could break existing automation"],
        "validation_plan": "Run task CLI tests",
        "definition_of_done": ["Task commands are implemented"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
