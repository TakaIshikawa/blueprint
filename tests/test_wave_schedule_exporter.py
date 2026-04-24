import json
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.exporters.wave_schedule import WaveScheduleExporter
from blueprint.store import Store, init_db


def test_wave_schedule_exporter_writes_dependency_waves(tmp_path):
    output_path = tmp_path / "waves.json"

    WaveScheduleExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    payload = json.loads(output_path.read_text())
    assert payload["schema_version"] == "blueprint.wave_schedule.v1"
    assert payload["plan_id"] == "plan-test"
    assert payload["total_waves"] == 3
    assert payload["task_count"] == 5
    assert [wave["task_ids"] for wave in payload["waves"]] == [
        ["task-setup", "task-blocked"],
        ["task-api", "task-copy"],
        ["task-ui"],
    ]
    assert payload["waves"][1]["tasks"][0] == {
        "id": "task-api",
        "title": "Build API",
        "wave_number": 2,
        "suggested_engine": "codex",
        "owner_type": "agent",
        "files_or_modules": ["src/app.py"],
        "dependencies": ["task-setup"],
        "status": "pending",
        "status_metadata": {
            "blocked": False,
            "skipped": False,
            "blocked_reason": None,
        },
    }


def test_wave_schedule_exporter_includes_blocked_and_skipped_metadata(tmp_path):
    output_path = tmp_path / "waves.json"

    WaveScheduleExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    tasks = {
        task["id"]: task
        for wave in json.loads(output_path.read_text())["waves"]
        for task in wave["tasks"]
    }
    assert tasks["task-blocked"]["status_metadata"] == {
        "blocked": True,
        "skipped": False,
        "blocked_reason": "Waiting on credentials",
    }
    assert tasks["task-copy"]["status_metadata"] == {
        "blocked": False,
        "skipped": True,
        "blocked_reason": None,
    }


def test_wave_schedule_validation_passes_for_rendered_export(tmp_path):
    output_path = tmp_path / "waves.json"
    plan = _execution_plan()
    brief = _implementation_brief()
    WaveScheduleExporter().export(plan, brief, str(output_path))

    findings = validate_rendered_export(
        target="wave-schedule",
        artifact_path=output_path,
        execution_plan=plan,
        implementation_brief=brief,
    )

    assert findings == []


def test_wave_schedule_validation_catches_missing_duplicate_and_order_errors(tmp_path):
    output_path = tmp_path / "waves.json"
    payload = {
        "schema_version": "blueprint.wave_schedule.v1",
        "plan_id": "plan-test",
        "total_waves": 2,
        "waves": [
            {
                "wave_number": 1,
                "task_ids": ["task-api", "task-api"],
                "tasks": [
                    _scheduled_task("task-api"),
                    _scheduled_task("task-api"),
                ],
            },
            {
                "wave_number": 2,
                "task_ids": ["task-setup"],
                "tasks": [_scheduled_task("task-setup")],
            },
        ],
    }
    output_path.write_text(json.dumps(payload))

    findings = validate_rendered_export(
        target="wave-schedule",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    codes = [finding.code for finding in findings]
    assert "wave_schedule.duplicate_task" in codes
    assert "wave_schedule.missing_task" in codes
    assert "wave_schedule.dependency_order" in codes


def test_export_validate_supports_wave_schedule():
    result = validate_export(_execution_plan(), _implementation_brief(), "wave-schedule")

    assert result.passed
    assert result.findings == []


def test_export_run_preview_and_validate_support_wave_schedule(tmp_path, monkeypatch):
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

    run_result = CliRunner().invoke(
        cli,
        ["export", "run", plan_id, "--target", "wave-schedule"],
    )
    assert run_result.exit_code == 0, run_result.output
    output_path = export_dir / f"{plan_id}-wave-schedule.json"
    assert output_path.exists()
    assert json.loads(output_path.read_text())["total_waves"] == 3

    preview_result = CliRunner().invoke(
        cli,
        ["export", "preview", plan_id, "--target", "wave-schedule"],
    )
    assert preview_result.exit_code == 0, preview_result.output
    preview_payload = json.loads(preview_result.output)
    assert preview_payload["plan_id"] == plan_id
    assert preview_payload["waves"][0]["task_ids"] == ["task-setup", "task-blocked"]

    validate_result = CliRunner().invoke(
        cli,
        ["export", "validate", plan_id, "--target", "wave-schedule", "--json"],
    )
    assert validate_result.exit_code == 0, validate_result.output
    validation_payload = json.loads(validate_result.output)
    assert validation_payload["target"] == "wave-schedule"
    assert validation_payload["passed"] is True

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "wave-schedule"
    assert records[0]["export_format"] == "json"
    assert records[0]["output_path"] == str(output_path)


def _scheduled_task(task_id):
    return {
        "id": task_id,
        "suggested_engine": "codex",
        "owner_type": "agent",
        "files_or_modules": [],
        "dependencies": [],
        "status": "pending",
        "status_metadata": {
            "blocked": False,
            "skipped": False,
            "blocked_reason": None,
        },
    }


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
            "id": "task-ui",
            "title": "Build UI",
            "description": "Implement the user interface",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-api", "task-copy"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI renders"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "task-copy",
            "title": "Skip copy",
            "description": "Skip already completed copy work",
            "milestone": "Foundation",
            "owner_type": "human",
            "suggested_engine": None,
            "depends_on": ["task-setup"],
            "files_or_modules": ["docs/copy.md"],
            "acceptance_criteria": ["Copy is not required"],
            "estimated_complexity": "low",
            "status": "skipped",
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
        "problem_statement": "Need a dependency wave schedule",
        "mvp_goal": "Expose tasks grouped by dependency wave",
        "product_surface": "CLI",
        "scope": ["Wave schedule export"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use execution wave audit",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Malformed schedule"],
        "validation_plan": "Run wave schedule tests",
        "definition_of_done": ["Each task exports once"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
