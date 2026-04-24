import json
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_rendered_export
from blueprint.exporters.task_queue_jsonl import TaskQueueJsonlExporter
from blueprint.store import Store, init_db


def test_task_queue_jsonl_exporter_writes_one_task_per_line_in_id_order(tmp_path):
    output_path = tmp_path / "queue.jsonl"

    TaskQueueJsonlExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_jsonl(output_path)
    assert [row["task_id"] for row in rows] == [
        "task-api",
        "task-blocked",
        "task-setup",
        "task-ui",
    ]
    assert rows[0] == {
        "plan_id": "plan-test",
        "task_id": "task-api",
        "title": "Build API",
        "description": "Implement the command API",
        "milestone": "Foundation",
        "suggested_engine": "codex",
        "dependency_ids": ["task-setup"],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": ["API returns data"],
        "status": "pending",
        "ready": True,
    }


def test_task_queue_jsonl_readiness_requires_completed_dependencies_and_non_blocked_status(
    tmp_path,
):
    output_path = tmp_path / "queue.jsonl"

    TaskQueueJsonlExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    ready_by_task_id = {row["task_id"]: row["ready"] for row in _read_jsonl(output_path)}
    assert ready_by_task_id == {
        "task-api": True,
        "task-blocked": False,
        "task-setup": True,
        "task-ui": False,
    }


def test_export_run_task_queue_jsonl_writes_file_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(
        cli,
        ["export", "run", plan_id, "--target", "task-queue-jsonl"],
    )

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-task-queue-jsonl.jsonl"
    assert output_path.exists()
    assert "Exported to:" in result.output

    rows = _read_jsonl(output_path)
    assert rows[0]["plan_id"] == plan_id
    assert rows[0]["task_id"] == "task-api"

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "task-queue-jsonl"
    assert records[0]["export_format"] == "jsonl"
    assert records[0]["output_path"] == str(output_path)


def test_task_queue_jsonl_validation_passes_for_rendered_export(tmp_path):
    output_path = tmp_path / "queue.jsonl"
    plan = _execution_plan()
    brief = _implementation_brief()
    TaskQueueJsonlExporter().export(plan, brief, str(output_path))

    findings = validate_rendered_export(
        target="task-queue-jsonl",
        artifact_path=output_path,
        execution_plan=plan,
        implementation_brief=brief,
    )

    assert findings == []


def test_task_queue_jsonl_validation_catches_malformed_and_missing_task_ids(tmp_path):
    output_path = tmp_path / "queue.jsonl"
    output_path.write_text(
        "\n".join(
            [
                json.dumps({"plan_id": "plan-test", "task_id": "task-api"}),
                "{not-json",
                json.dumps({"plan_id": "plan-test"}),
            ]
        )
        + "\n"
    )

    findings = validate_rendered_export(
        target="task-queue-jsonl",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    codes = [finding.code for finding in findings]
    assert "task_queue_jsonl.invalid_json" in codes
    assert "task_queue_jsonl.missing_task_id" in codes
    assert "task_queue_jsonl.line_count_mismatch" in codes
    assert "task_queue_jsonl.missing_task" in codes


def test_task_queue_jsonl_validation_catches_duplicate_task_ids(tmp_path):
    output_path = tmp_path / "queue.jsonl"
    output_path.write_text(
        "\n".join(
            [
                json.dumps({"plan_id": "plan-test", "task_id": "task-api"}),
                json.dumps({"plan_id": "plan-test", "task_id": "task-api"}),
                json.dumps({"plan_id": "plan-test", "task_id": "task-blocked"}),
                json.dumps({"plan_id": "plan-test", "task_id": "task-setup"}),
            ]
        )
        + "\n"
    )

    findings = validate_rendered_export(
        target="task-queue-jsonl",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    codes = [finding.code for finding in findings]
    assert "task_queue_jsonl.duplicate_task_id" in codes
    assert "task_queue_jsonl.missing_task" in codes


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def _execution_plan(include_tasks=True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
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
            "id": "task-ui",
            "title": "Build UI",
            "description": "Implement the user interface",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI renders"],
            "estimated_complexity": "medium",
            "status": "pending",
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
        },
        {
            "id": "task-blocked",
            "title": "Resolve blocker",
            "description": "Wait for an external dependency",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/blocker.py"],
            "acceptance_criteria": ["Blocker is resolved"],
            "estimated_complexity": "low",
            "status": "blocked",
            "blocked_reason": "Waiting on credentials",
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
        "problem_statement": "Need a machine-readable task queue",
        "mvp_goal": "Expose tasks as JSON Lines",
        "product_surface": "CLI",
        "scope": ["Task queue export"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use exporter validation helpers",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Malformed JSONL"],
        "validation_plan": "Run task queue JSONL tests",
        "definition_of_done": ["Each task exports once"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
