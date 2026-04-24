import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.env_readiness import audit_env_readiness
from blueprint.cli import cli
from blueprint.store import init_db


def test_env_readiness_accepts_clean_plan():
    result = audit_env_readiness(_ready_plan())

    assert result.passed is True
    assert result.blocking_count == 0
    assert result.warning_count == 0
    assert result.findings == []
    assert [task.ready for task in result.tasks] == [True, True]


def test_env_readiness_reports_missing_setup_test_and_env_details():
    plan = _ready_plan()
    plan["test_strategy"] = "Run the relevant tests"
    plan["metadata"] = {}
    plan["tasks"] = [
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "status": "pending",
            "metadata": {},
        }
    ]

    result = audit_env_readiness(plan)

    assert result.passed is True
    assert result.blocking_count == 0
    assert result.warning_count == 3
    assert _finding_codes(result) == {
        "missing_setup_files",
        "missing_test_command",
        "missing_env_var_notes",
    }
    assert all(finding.remediation for finding in result.findings)


def test_env_readiness_blocks_ci_deploy_tasks_without_rollback_or_verification():
    plan = _ready_plan()
    plan["test_strategy"] = "Run the relevant tests"
    plan["tasks"] = [
        {
            "id": "task-ci",
            "title": "Update release workflow",
            "description": "Change deployment automation",
            "milestone": "Release",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": [".github/workflows/release.yml"],
            "acceptance_criteria": ["Workflow file is updated"],
            "estimated_complexity": "medium",
            "status": "pending",
            "metadata": {
                "setup_files": [".github/workflows/release.yml"],
                "env_vars": ["GITHUB_TOKEN is provided by Actions"],
            },
        }
    ]

    result = audit_env_readiness(plan)

    assert result.passed is False
    assert result.blocking_count == 2
    assert _finding_codes(result) == {
        "missing_test_command",
        "missing_rollback_criteria",
        "missing_deploy_verification",
    }
    assert result.findings_by_severity()["blocking"][0].severity == "blocking"
    assert result.findings_by_severity()["warning"][0].severity == "warning"


def test_env_readiness_cli_json_has_stable_machine_readable_output(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch)

    result = CliRunner().invoke(cli, ["task", "env-readiness", "plan-ready", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == {
        "findings": [],
        "passed": True,
        "plan_id": "plan-ready",
        "summary": {
            "blocking": 0,
            "tasks": 2,
            "warning": 0,
        },
        "tasks": [
            {
                "findings": [],
                "ready": True,
                "summary": {
                    "blocking": 0,
                    "warning": 0,
                },
                "task_id": "task-setup",
                "title": "Setup project",
            },
            {
                "findings": [],
                "ready": True,
                "summary": {
                    "blocking": 0,
                    "warning": 0,
                },
                "task_id": "task-api",
                "title": "Build API",
            },
        ],
    }


def test_env_readiness_cli_human_output_groups_findings_and_exits_nonzero(
    tmp_path,
    monkeypatch,
):
    plan = _ready_plan()
    plan["test_strategy"] = "Run checks"
    plan["metadata"] = {"setup_files": ["pyproject.toml"]}
    plan["tasks"][0]["files_or_modules"] = [".github/workflows/release.yml"]
    plan["tasks"][0]["acceptance_criteria"] = ["Workflow is updated"]
    plan["tasks"][0]["metadata"] = {"env_vars": ["GITHUB_TOKEN is provided by Actions"]}
    plan["tasks"][1]["metadata"] = {}
    _seed_plan(tmp_path, monkeypatch, plan=plan)

    result = CliRunner().invoke(cli, ["task", "env-readiness", "plan-ready"])

    assert result.exit_code == 1, result.output
    assert "Task environment readiness audit: plan-ready" in result.output
    assert "Result: failed (2 blocking, 3 warnings)" in result.output
    assert result.output.index("Blocking findings:") < result.output.index("Warnings:")
    assert "[missing_rollback_criteria]" in result.output
    assert "[missing_deploy_verification]" in result.output
    assert "[missing_test_command]" in result.output
    assert "[missing_env_var_notes]" in result.output


def test_env_readiness_cli_rejects_missing_plan(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["task", "env-readiness", "missing-plan"])

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
        "test_strategy": "Run `poetry run pytest tests/test_env_readiness.py`.",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "metadata": {
            "setup_files": ["pyproject.toml", ".env.example"],
            "env_vars": ["No environment variables required."],
        },
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
                "acceptance_criteria": [
                    "Project installs with poetry",
                    "Rollback by reverting dependency changes",
                ],
                "estimated_complexity": "low",
                "status": "completed",
                "metadata": {
                    "setup_files": ["pyproject.toml"],
                    "env_vars": ["No environment variables required."],
                },
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
                "status": "pending",
                "metadata": {
                    "setup_files": ["pyproject.toml"],
                    "env_vars": ["No environment variables required."],
                },
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
