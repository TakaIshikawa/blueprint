import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits import TaskSplittingResult, audit_task_splitting
from blueprint.cli import cli
from blueprint.store import init_db


def test_task_splitting_accepts_focused_tasks():
    result = audit_task_splitting(_focused_plan())

    assert isinstance(result, TaskSplittingResult)
    assert result.passed is True
    assert result.recommendation_count == 0
    assert result.recommendations == []


def test_task_splitting_flags_broad_tasks_and_suggests_file_groups():
    plan = _focused_plan()
    plan["tasks"] = [
        {
            "id": "task-broad",
            "title": "Build, update, and validate API workflow",
            "description": (
                "Implement API routes, refactor UI wiring, and document behavior "
                "as needed."
            ),
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": [
                "src/api/routes.py",
                "src/api/schema.py",
                "src/ui/page.py",
                "tests/api/test_routes.py",
                "docs/api.md",
            ],
            "acceptance_criteria": [
                "API routes return validation errors",
                "API schema is documented",
                "UI page renders API errors",
                "Tests verify route failures",
            ],
            "estimated_complexity": "high",
            "status": "pending",
        }
    ]

    result = audit_task_splitting(plan)

    assert result.passed is False
    assert result.recommendation_count == 1
    recommendation = result.recommendations[0]
    assert recommendation.task_id == "task-broad"
    assert [reason.code for reason in recommendation.reasons] == [
        "high_complexity",
        "broad_file_scope",
        "many_acceptance_criteria",
        "vague_description",
        "multiple_independent_verbs",
    ]
    assert [subtask.title for subtask in recommendation.suggested_subtasks] == [
        "Split src/api changes",
        "Split src/ui changes",
        "Split tests/api changes",
        "Split docs changes",
    ]
    assert recommendation.suggested_subtasks[0].files_or_modules == [
        "src/api/routes.py",
        "src/api/schema.py",
    ]
    assert recommendation.suggested_subtasks[0].acceptance_criteria == [
        "API routes return validation errors",
        "API schema is documented",
        "UI page renders API errors",
    ]


def test_task_splitting_uses_acceptance_criteria_when_files_cannot_be_grouped():
    plan = _focused_plan()
    plan["tasks"][0].update(
        {
            "id": "task-criteria",
            "title": "Implement reporting",
            "description": "Implement reporting behavior",
            "files_or_modules": ["src/reporting.py"],
            "acceptance_criteria": [
                "Report writes CSV rows",
                "Report returns validation errors",
                "Report displays totals",
                "Report tests pass",
            ],
        }
    )

    result = audit_task_splitting(plan)

    assert [reason.code for reason in result.recommendations[0].reasons] == [
        "many_acceptance_criteria"
    ]
    assert [
        subtask.acceptance_criteria
        for subtask in result.recommendations[0].suggested_subtasks
    ] == [
        ["Report writes CSV rows"],
        ["Report returns validation errors"],
        ["Report displays totals"],
        ["Report tests pass"],
    ]


def test_task_split_audit_cli_json_exits_cleanly_for_no_findings(
    tmp_path,
    monkeypatch,
):
    _seed_plan(tmp_path, monkeypatch, _focused_plan())

    result = CliRunner().invoke(cli, ["task", "split-audit", "plan-split", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "passed": True,
        "plan_id": "plan-split",
        "recommendations": [],
        "summary": {
            "recommendations": 0,
        },
    }


def test_task_split_audit_cli_human_output_lists_recommendations(
    tmp_path,
    monkeypatch,
):
    plan = _focused_plan()
    plan["tasks"][0]["estimated_complexity"] = "large"
    plan["tasks"][0]["acceptance_criteria"] = [
        "Endpoint writes data",
        "Endpoint validates input",
        "Endpoint renders errors",
        "Endpoint tests pass",
    ]
    _seed_plan(tmp_path, monkeypatch, plan)

    result = CliRunner().invoke(cli, ["task", "split-audit", "plan-split"])

    assert result.exit_code == 1, result.output
    assert "Task split audit: plan-split" in result.output
    assert "Result: recommendations found (1 recommendations)" in result.output
    assert "[high_complexity]" in result.output
    assert "[many_acceptance_criteria]" in result.output
    assert "Suggested subtasks:" in result.output


def _seed_plan(tmp_path, monkeypatch, plan):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
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


def _focused_plan():
    return {
        "id": "plan-split",
        "implementation_brief_id": "ib-split",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Delivery", "description": "Deliver focused work"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": [
            {
                "id": "task-api",
                "title": "Build API endpoint",
                "description": "Implement the command API endpoint",
                "milestone": "Delivery",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["src/app.py"],
                "acceptance_criteria": ["API returns data"],
                "estimated_complexity": "medium",
                "status": "pending",
            }
        ],
    }


def _implementation_brief():
    return {
        "id": "ib-split",
        "source_brief_id": "sb-split",
        "title": "Split Audit",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need task split recommendations",
        "mvp_goal": "Expose split audit in the CLI",
        "product_surface": "CLI",
        "scope": ["Task split audit"],
        "non_goals": ["Automatic task rewriting"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use audit dataclasses",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Broad tasks reduce autonomous success"],
        "validation_plan": "Run task split tests",
        "definition_of_done": ["Task split audit is implemented"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
