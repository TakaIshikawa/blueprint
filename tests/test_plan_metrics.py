import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.plan_metrics import calculate_plan_metrics
from blueprint.cli import cli
from blueprint.store import init_db


def test_plan_metrics_counts_mixed_statuses_engines_complexities_and_dependencies():
    result = calculate_plan_metrics(_plan_with_tasks(_mixed_tasks()))

    assert result.to_dict() == {
        "plan_id": "plan-test",
        "task_count": 5,
        "milestone_count": 2,
        "counts_by_status": {
            "blocked": 1,
            "completed": 2,
            "pending": 2,
        },
        "counts_by_suggested_engine": {
            "claude": 2,
            "codex": 2,
            "unspecified": 1,
        },
        "counts_by_estimated_complexity": {
            "high": 1,
            "low": 2,
            "medium": 1,
            "unspecified": 1,
        },
        "ready_task_count": 2,
        "blocked_task_count": 1,
        "completed_percent": 40.0,
        "dependency_edge_count": 4,
        "average_dependencies_per_task": 0.8,
    }


def test_plan_metrics_ready_tasks_require_pending_status_and_satisfied_dependencies():
    tasks = [
        _task("task-complete", "Complete", "completed"),
        _task("task-skipped", "Skipped", "skipped"),
        _task("task-ready", "Ready", "pending", depends_on=["task-complete"]),
        _task(
            "task-ready-after-skip",
            "Ready after skip",
            "pending",
            depends_on=["task-skipped"],
        ),
        _task("task-not-ready", "Not ready", "pending", depends_on=["task-blocked"]),
        _task("task-missing-dep", "Missing dep", "pending", depends_on=["task-missing"]),
        _task("task-blocked", "Blocked", "blocked"),
    ]

    result = calculate_plan_metrics(_plan_with_tasks(tasks))

    assert result.ready_task_count == 2
    assert result.blocked_task_count == 1


def test_plan_metrics_handles_empty_plans():
    result = calculate_plan_metrics(_plan_with_tasks([]))

    assert result.to_dict() == {
        "plan_id": "plan-test",
        "task_count": 0,
        "milestone_count": 2,
        "counts_by_status": {},
        "counts_by_suggested_engine": {},
        "counts_by_estimated_complexity": {},
        "ready_task_count": 0,
        "blocked_task_count": 0,
        "completed_percent": 0.0,
        "dependency_edge_count": 0,
        "average_dependencies_per_task": 0.0,
    }


def test_plan_metrics_cli_outputs_json(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch, _mixed_tasks())

    result = CliRunner().invoke(cli, ["plan", "metrics", "plan-test", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-test"
    assert payload["task_count"] == 5
    assert payload["ready_task_count"] == 2
    assert payload["blocked_task_count"] == 1
    assert payload["dependency_edge_count"] == 4
    assert payload["average_dependencies_per_task"] == 0.8
    assert payload["completed_percent"] == 40.0
    assert payload["counts_by_status"] == {
        "blocked": 1,
        "completed": 2,
        "pending": 2,
    }


def test_plan_metrics_cli_outputs_human_readable_table(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch, _mixed_tasks())

    result = CliRunner().invoke(cli, ["plan", "metrics", "plan-test"])

    assert result.exit_code == 0, result.output
    assert "Execution plan metrics: plan-test" in result.output
    assert "Metric" in result.output
    assert "Tasks" in result.output
    assert "5" in result.output
    assert "Ready tasks" in result.output
    assert "2" in result.output
    assert "Status:" in result.output
    assert "completed: 2" in result.output
    assert "Suggested engine:" in result.output
    assert "codex: 2" in result.output
    assert "Estimated complexity:" in result.output
    assert "low: 2" in result.output


def test_plan_metrics_cli_reports_missing_plan(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["plan", "metrics", "plan-missing"])

    assert result.exit_code != 0
    assert "Execution plan not found: plan-missing" in result.output


def _seed_plan(tmp_path, monkeypatch, tasks):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), tasks)


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


def _plan_with_tasks(tasks):
    plan = _execution_plan()
    plan["tasks"] = tasks
    return plan


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


def _mixed_tasks():
    return [
        _task(
            "task-setup",
            "Setup project",
            "completed",
            suggested_engine="codex",
            estimated_complexity="low",
        ),
        _task(
            "task-api",
            "Build API",
            "completed",
            suggested_engine="codex",
            estimated_complexity="medium",
            depends_on=["task-setup"],
        ),
        _task(
            "task-ui",
            "Build UI",
            "pending",
            suggested_engine="claude",
            estimated_complexity="high",
            depends_on=["task-api"],
        ),
        _task(
            "task-docs",
            "Write docs",
            "pending",
            suggested_engine=None,
            estimated_complexity="low",
        ),
        _task(
            "task-release",
            "Release",
            "blocked",
            suggested_engine="claude",
            estimated_complexity=None,
            depends_on=["task-ui", "task-docs"],
        ),
    ]


def _task(
    task_id,
    title,
    status,
    *,
    suggested_engine="codex",
    estimated_complexity="low",
    depends_on=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": suggested_engine,
        "depends_on": depends_on or [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": estimated_complexity,
        "status": status,
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need plan metrics",
        "mvp_goal": "Expose plan metrics in the CLI",
        "product_surface": "CLI",
        "scope": ["Plan commands"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Incorrect metrics"],
        "validation_plan": "Run plan metrics tests",
        "definition_of_done": ["CLI reports metrics"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
