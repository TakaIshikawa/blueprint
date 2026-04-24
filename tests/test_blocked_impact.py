import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.blocked_impact import audit_blocked_impact
from blueprint.cli import cli
from blueprint.store import init_db


def test_blocked_impact_returns_empty_for_plan_without_blocked_tasks():
    result = audit_blocked_impact(
        _plan(
            [
                _task("task-setup", "Setup", "Foundation", status="completed"),
                _task(
                    "task-api",
                    "Build API",
                    "Build",
                    depends_on=["task-setup"],
                ),
            ]
        )
    )

    assert result.to_dict() == {
        "plan_id": "plan-blocked-impact",
        "blocked_count": 0,
        "has_impact": False,
        "blocked_tasks": [],
    }


def test_blocked_impact_reports_direct_dependency_impact():
    result = audit_blocked_impact(
        _plan(
            [
                _task(
                    "task-schema",
                    "Finalize schema",
                    "Foundation",
                    status="blocked",
                    blocked_reason="Waiting for product decision",
                ),
                _task(
                    "task-api",
                    "Build API",
                    "Build",
                    depends_on=["task-schema"],
                ),
            ]
        )
    )

    [blocked_task] = result.blocked_tasks
    assert blocked_task.blocked_task_id == "task-schema"
    assert blocked_task.blocked_reason == "Waiting for product decision"
    assert blocked_task.direct_dependents == ["task-api"]
    assert blocked_task.transitive_dependents == []
    assert blocked_task.impacted_milestones == ["Build"]
    assert blocked_task.impacted_count == 1
    assert blocked_task.critical_dependency_position is False
    assert blocked_task.severity == "medium"


def test_blocked_impact_reports_transitive_dependency_impact():
    result = audit_blocked_impact(
        _plan(
            [
                _task("task-auth", "Auth", "Foundation", status="blocked"),
                _task(
                    "task-api",
                    "API",
                    "Build",
                    depends_on=["task-auth"],
                ),
                _task(
                    "task-ui",
                    "UI",
                    "Interface",
                    depends_on=["task-api"],
                ),
                _task(
                    "task-release",
                    "Release",
                    "Launch",
                    depends_on=["task-ui"],
                ),
            ]
        )
    )

    [blocked_task] = result.blocked_tasks
    assert blocked_task.direct_dependents == ["task-api"]
    assert blocked_task.transitive_dependents == ["task-ui", "task-release"]
    assert blocked_task.impacted_milestones == ["Build", "Interface", "Launch"]
    assert blocked_task.impacted_count == 3
    assert blocked_task.critical_dependency_position is True
    assert blocked_task.severity == "critical"


def test_blocked_impact_reports_blocked_task_with_no_dependents():
    result = audit_blocked_impact(
        _plan(
            [
                _task(
                    "task-copy",
                    "Draft copy",
                    "Launch",
                    status="blocked",
                    metadata={"blocked_reason": "Legal review"},
                ),
                _task("task-api", "API", "Build"),
            ]
        )
    )

    [blocked_task] = result.blocked_tasks
    assert blocked_task.blocked_task_id == "task-copy"
    assert blocked_task.blocked_reason == "Legal review"
    assert blocked_task.direct_dependents == []
    assert blocked_task.transitive_dependents == []
    assert blocked_task.impacted_milestones == []
    assert blocked_task.impacted_count == 0
    assert blocked_task.severity == "low"


def test_blocked_impact_cli_outputs_deterministic_json(tmp_path, monkeypatch):
    plan = _plan(
        [
            _task("task-auth", "Auth", "Foundation", status="blocked"),
            _task("task-api", "API", "Build", depends_on=["task-auth"]),
            _task("task-ui", "UI", "Interface", depends_on=["task-api"]),
        ]
    )
    _seed_plan(tmp_path, monkeypatch, plan)

    result = CliRunner().invoke(
        cli,
        ["task", "blocked-impact", "plan-blocked-impact", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "plan_id": "plan-blocked-impact",
        "blocked_count": 1,
        "has_impact": True,
        "blocked_tasks": [
            {
                "blocked_task_id": "task-auth",
                "blocked_task_title": "Auth",
                "blocked_reason": None,
                "milestone": "Foundation",
                "direct_dependents": ["task-api"],
                "transitive_dependents": ["task-ui"],
                "impacted_milestones": ["Build", "Interface"],
                "impacted_count": 2,
                "critical_dependency_position": True,
                "severity": "critical",
            }
        ],
    }


def test_blocked_impact_cli_human_output_highlights_milestones_and_severity(
    tmp_path,
    monkeypatch,
):
    plan = _plan(
        [
            _task("task-auth", "Auth", "Foundation", status="blocked"),
            _task("task-api", "API", "Build", depends_on=["task-auth"]),
            _task("task-ui", "UI", "Interface", depends_on=["task-api"]),
        ]
    )
    _seed_plan(tmp_path, monkeypatch, plan)

    result = CliRunner().invoke(
        cli,
        ["task", "blocked-impact", "plan-blocked-impact"],
    )

    assert result.exit_code == 0, result.output
    assert "Blocked task impact audit: plan-blocked-impact" in result.output
    assert "Severity: critical" in result.output
    assert "Impacted milestones: Build, Interface" in result.output


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
        "id": "ib-blocked-impact",
        "source_brief_id": "sb-blocked-impact",
        "title": "Blocked impact",
        "domain": "planning",
        "target_user": "Delivery leads",
        "buyer": "Engineering",
        "workflow_context": "Execution planning",
        "problem_statement": "Need blocked task impact visibility",
        "mvp_goal": "Expose downstream blocker impact",
        "product_surface": "CLI",
        "scope": ["Blocked impact audit"],
        "non_goals": ["Task execution"],
        "assumptions": ["Plans already exist"],
        "architecture_notes": "Use audit helpers",
        "data_requirements": "Execution plan tasks",
        "integration_points": [],
        "risks": ["Blocked dependencies may delay execution"],
        "validation_plan": "Run blocked impact tests",
        "definition_of_done": ["Blocked impact CLI works"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _plan(tasks):
    return {
        "id": "plan-blocked-impact",
        "implementation_brief_id": "ib-blocked-impact",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Prepare the work"},
            {"name": "Build", "description": "Build the feature"},
            {"name": "Interface", "description": "Expose the feature"},
            {"name": "Launch", "description": "Release the feature"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Audit blocked impact",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    milestone,
    *,
    status="pending",
    depends_on=None,
    blocked_reason=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": f"{title} description",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": ["src/blueprint/cli.py"],
        "acceptance_criteria": [f"{title} is complete"],
        "estimated_complexity": "medium",
        "status": status,
        "blocked_reason": blocked_reason,
        "metadata": metadata or {},
    }
