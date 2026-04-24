import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.workload import analyze_workload
from blueprint.cli import cli
from blueprint.store import init_db


def test_workload_audit_summarizes_balanced_plan():
    result = analyze_workload(_plan())

    assert result.plan_id == "plan-workload"
    assert result.task_count == 4
    assert result.counts_by_owner_type == {"agent": 2, "human": 2}
    assert result.counts_by_suggested_engine == {"codex": 2, "manual": 2}
    assert result.counts_by_milestone == {"Build": 2, "Validate": 2}
    assert result.counts_by_status == {"completed": 1, "pending": 3}
    assert result.complexity_buckets == {"high": 1, "low": 1, "medium": 2}
    assert result.overloaded_groups == []
    assert result.unassigned_task_ids == []
    assert [item.to_dict() for item in result.cross_milestone_dependencies] == [
        {
            "from_milestone": "Build",
            "to_milestone": "Validate",
            "count": 1,
            "dependency_pairs": [
                {
                    "dependency_task_id": "task-api",
                    "dependent_task_id": "task-docs",
                }
            ],
        }
    ]


def test_workload_audit_flags_overloaded_owner_and_engine_groups():
    plan = _plan(
        tasks=[
            _task(f"task-{index}", owner_type="agent", suggested_engine="codex")
            for index in range(6)
        ]
    )

    result = analyze_workload(plan)

    assert [group.to_dict() for group in result.overloaded_groups] == [
        {
            "dimension": "owner_type",
            "group": "agent",
            "task_count": 6,
            "threshold": 5,
            "task_ids": [
                "task-0",
                "task-1",
                "task-2",
                "task-3",
                "task-4",
                "task-5",
            ],
        },
        {
            "dimension": "suggested_engine",
            "group": "codex",
            "task_count": 6,
            "threshold": 5,
            "task_ids": [
                "task-0",
                "task-1",
                "task-2",
                "task-3",
                "task-4",
                "task-5",
            ],
        },
    ]
    assert result.has_flags is True


def test_workload_audit_flags_unassigned_tasks():
    plan = _plan(
        tasks=[
            _task("task-owned", owner_type="agent"),
            _task("task-none", owner_type=None),
            _task("task-blank", owner_type=" "),
        ]
    )

    result = analyze_workload(plan)

    assert result.counts_by_owner_type == {"agent": 1, "unspecified": 2}
    assert result.unassigned_task_ids == ["task-none", "task-blank"]
    assert result.has_flags is True


def test_workload_cli_outputs_json(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch, _plan())

    result = CliRunner().invoke(
        cli,
        ["task", "workload", "plan-workload", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-workload"
    assert payload["task_count"] == 4
    assert payload["overload_threshold"] == 5
    assert payload["counts_by_owner_type"] == {"agent": 2, "human": 2}
    assert payload["complexity_buckets"] == {"high": 1, "low": 1, "medium": 2}
    assert payload["unassigned_task_ids"] == []
    assert payload["cross_milestone_dependencies"][0]["count"] == 1


def test_workload_cli_outputs_readable_summary(tmp_path, monkeypatch):
    plan = _plan(tasks=[_task(f"task-{index}") for index in range(6)])
    _seed_plan(tmp_path, monkeypatch, plan)

    result = CliRunner().invoke(cli, ["task", "workload", "plan-workload"])

    assert result.exit_code == 0, result.output
    assert "Task workload audit: plan-workload" in result.output
    assert "overload threshold: 5 per owner/engine group" in result.output
    assert "owner_type" in result.output
    assert "suggested_engine" in result.output
    assert "complexity" in result.output
    assert "Overloaded owner_type=agent: 6 tasks" in result.output
    assert "Overloaded suggested_engine=codex: 6 tasks" in result.output


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
        "id": "ib-workload",
        "source_brief_id": "sb-workload",
        "title": "Workload audit",
        "domain": "planning",
        "target_user": "Delivery leads",
        "buyer": "Engineering",
        "workflow_context": "Execution planning",
        "problem_statement": "Need to inspect workload skew",
        "mvp_goal": "Expose workload distribution",
        "product_surface": "CLI",
        "scope": ["Workload audit"],
        "non_goals": ["Task execution"],
        "assumptions": ["Plans already exist"],
        "architecture_notes": "Use audit helpers",
        "data_requirements": "Execution plan tasks",
        "integration_points": [],
        "risks": ["Workload skew may block execution"],
        "validation_plan": "Run workload tests",
        "definition_of_done": ["Workload CLI works"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _plan(tasks=None):
    return {
        "id": "plan-workload",
        "implementation_brief_id": "ib-workload",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Build", "description": "Build the feature"},
            {"name": "Validate", "description": "Validate the feature"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Audit workload distribution",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": tasks
        if tasks is not None
        else [
            _task(
                "task-setup",
                milestone="Build",
                owner_type="human",
                suggested_engine="manual",
                estimated_complexity="low",
                status="completed",
            ),
            _task(
                "task-api",
                milestone="Build",
                owner_type="agent",
                suggested_engine="codex",
                estimated_complexity="medium",
                depends_on=["task-setup"],
            ),
            _task(
                "task-docs",
                milestone="Validate",
                owner_type="human",
                suggested_engine="manual",
                estimated_complexity="medium",
                depends_on=["task-api"],
            ),
            _task(
                "task-tests",
                milestone="Validate",
                owner_type="agent",
                suggested_engine="codex",
                estimated_complexity="high",
                depends_on=["task-docs"],
            ),
        ],
    }


def _task(
    task_id,
    *,
    milestone="Build",
    owner_type="agent",
    suggested_engine="codex",
    estimated_complexity="medium",
    status="pending",
    depends_on=None,
):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "description": f"Implement {task_id}",
        "milestone": milestone,
        "owner_type": owner_type,
        "suggested_engine": suggested_engine,
        "depends_on": depends_on or [],
        "files_or_modules": ["src/blueprint/cli.py"],
        "acceptance_criteria": [f"{task_id} is complete"],
        "estimated_complexity": estimated_complexity,
        "status": status,
    }
