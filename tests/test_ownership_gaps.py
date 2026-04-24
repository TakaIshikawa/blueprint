import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.ownership_gaps import audit_ownership_gaps
from blueprint.cli import cli
from blueprint.store import init_db


def test_ownership_gap_audit_reports_unassigned_tasks():
    plan = _plan(
        tasks=[
            _task("task-ready"),
            _task("task-no-owner", owner_type=None),
            _task("task-no-engine", suggested_engine=" "),
        ]
    )

    result = audit_ownership_gaps(plan)

    assert result.passed is False
    assert result.blocking_count == 2
    assert _finding_codes(result) == {"missing_owner_type", "missing_suggested_engine"}
    assert [finding.to_dict() for finding in result.findings] == [
        {
            "severity": "blocking",
            "code": "missing_owner_type",
            "message": "Task task-no-owner has no owner_type assignment.",
            "remediation": "Set owner_type to human, agent, or the responsible ownership lane.",
            "task_ids": ["task-no-owner"],
            "suggested_engine": "codex",
        },
        {
            "severity": "blocking",
            "code": "missing_suggested_engine",
            "message": "Task task-no-engine has no suggested_engine assignment.",
            "remediation": "Set suggested_engine to the execution engine or manual lane expected to do the work.",
            "task_ids": ["task-no-engine"],
            "owner_type": "agent",
        },
    ]


def test_ownership_gap_audit_reports_conflicting_owner_engine_pairs():
    plan = _plan(
        tasks=[
            _task("task-human-agent", owner_type="human", suggested_engine="codex"),
            _task("task-agent-manual", owner_type="agent", suggested_engine="manual"),
        ]
    )

    result = audit_ownership_gaps(plan)

    assert result.warning_count == 2
    assert result.blocking_count == 0
    assert [finding.code for finding in result.findings] == [
        "conflicting_owner_engine",
        "conflicting_owner_engine",
    ]
    assert result.findings[0].task_ids == ["task-human-agent"]
    assert result.findings[1].task_ids == ["task-agent-manual"]


def test_ownership_gap_audit_reports_overloaded_owner_groups():
    plan = _plan(tasks=[_task(f"task-{index}") for index in range(6)])

    result = audit_ownership_gaps(plan)

    assert result.passed is False
    assert result.warning_count == 1
    assert result.findings[0].to_dict() == {
        "severity": "warning",
        "code": "overloaded_owner_group",
        "message": "Owner group agent has 6 tasks, above the threshold of 5.",
        "remediation": "Split this work across additional owner groups or raise the threshold if this lane is intentional.",
        "task_ids": [
            "task-0",
            "task-1",
            "task-2",
            "task-3",
            "task-4",
            "task-5",
        ],
        "owner_type": "agent",
        "task_count": 6,
        "threshold": 5,
    }


def test_ownership_gap_cli_uses_configurable_threshold(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch, _plan(tasks=[_task(f"task-{index}") for index in range(4)]))

    result = CliRunner().invoke(
        cli,
        ["task", "ownership-gaps", "plan-ownership", "--threshold", "3", "--json"],
    )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.output)
    assert payload["threshold"] == 3
    assert payload["findings"][0]["code"] == "overloaded_owner_group"
    assert payload["findings"][0]["task_count"] == 4


def test_ownership_gap_cli_json_has_stable_finding_codes(tmp_path, monkeypatch):
    plan = _plan(
        tasks=[
            _task("task-no-owner", owner_type=" "),
            _task("task-no-engine", suggested_engine=None),
            _task("task-conflict", owner_type="human", suggested_engine="codex"),
        ]
    )
    _seed_plan(tmp_path, monkeypatch, plan)

    result = CliRunner().invoke(
        cli,
        ["task", "ownership-gaps", "plan-ownership", "--json"],
    )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-ownership"
    assert payload["passed"] is False
    assert payload["summary"] == {"blocking": 2, "findings": 3, "warning": 1}
    assert [finding["code"] for finding in payload["findings"]] == [
        "missing_owner_type",
        "missing_suggested_engine",
        "conflicting_owner_engine",
    ]


def test_ownership_gap_cli_human_output_lists_tasks_and_guidance(tmp_path, monkeypatch):
    plan = _plan(
        tasks=[
            _task("task-no-owner", owner_type=None),
            _task("task-conflict", owner_type="human", suggested_engine="codex"),
        ]
    )
    _seed_plan(tmp_path, monkeypatch, plan)

    result = CliRunner().invoke(cli, ["task", "ownership-gaps", "plan-ownership"])

    assert result.exit_code == 1, result.output
    assert "Task ownership gap audit: plan-ownership" in result.output
    assert "Result: failed (1 blocking, 1 warnings, threshold 5)" in result.output
    assert "[missing_owner_type]" in result.output
    assert "[conflicting_owner_engine]" in result.output
    assert "Task IDs: task-no-owner" in result.output
    assert "Task IDs: task-conflict" in result.output
    assert "Remediation:" in result.output


def test_ownership_gap_cli_exit_zero_when_clean(tmp_path, monkeypatch):
    _seed_plan(
        tmp_path,
        monkeypatch,
        _plan(
            tasks=[
                _task("task-agent", owner_type="agent", suggested_engine="codex"),
                _task("task-human", owner_type="human", suggested_engine="manual"),
            ]
        ),
    )

    result = CliRunner().invoke(cli, ["task", "ownership-gaps", "plan-ownership"])

    assert result.exit_code == 0, result.output
    assert "No ownership gaps found." in result.output


def test_ownership_gap_cli_rejects_missing_plan(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["task", "ownership-gaps", "missing-plan"])

    assert result.exit_code != 0
    assert "Execution plan not found: missing-plan" in result.output


def _finding_codes(result):
    return {finding.code for finding in result.findings}


def _seed_plan(tmp_path, monkeypatch, plan):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_brief())
    store.insert_execution_plan(plan, plan["tasks"])


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


def _brief():
    return {
        "id": "ib-ownership",
        "source_brief_id": "sb-ownership",
        "title": "Ownership audit",
        "domain": "planning",
        "target_user": "Delivery leads",
        "buyer": "Engineering",
        "workflow_context": "Execution planning",
        "problem_statement": "Need to inspect ownership gaps",
        "mvp_goal": "Expose ambiguous ownership",
        "product_surface": "CLI",
        "scope": ["Ownership audit"],
        "non_goals": ["Task execution"],
        "assumptions": ["Plans already exist"],
        "architecture_notes": "Use audit helpers",
        "data_requirements": "Execution plan tasks",
        "integration_points": [],
        "risks": ["Ambiguous ownership may block execution"],
        "validation_plan": "Run ownership tests",
        "definition_of_done": ["Ownership CLI works"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _plan(tasks=None):
    return {
        "id": "plan-ownership",
        "implementation_brief_id": "ib-ownership",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Build", "description": "Build the feature"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Audit ownership clarity",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": tasks
        if tasks is not None
        else [
            _task("task-api"),
            _task("task-tests", owner_type="human", suggested_engine="manual"),
        ],
    }


def _task(
    task_id,
    *,
    owner_type="agent",
    suggested_engine="codex",
):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "description": f"Implement {task_id}",
        "milestone": "Build",
        "owner_type": owner_type,
        "suggested_engine": suggested_engine,
        "depends_on": [],
        "files_or_modules": ["src/blueprint/cli.py"],
        "acceptance_criteria": [f"{task_id} is complete"],
        "estimated_complexity": "medium",
        "status": "pending",
    }
