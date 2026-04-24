import copy
import json

from blueprint.exporters.export_validation import (
    validate_export,
    validate_rendered_export,
)
from blueprint.exporters.plan_snapshot import PlanSnapshotExporter


def test_plan_snapshot_export_includes_required_sections(tmp_path):
    output_path = tmp_path / "snapshot.json"

    PlanSnapshotExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    payload = json.loads(output_path.read_text())
    assert payload["schema_version"] == "blueprint.plan_snapshot.v1"
    assert payload["exported_at"]
    assert payload["content_hash"]
    assert payload["hash_algorithm"] == "sha256"
    assert set(payload) >= {
        "plan",
        "brief",
        "milestones",
        "tasks",
        "dependencies",
        "metrics",
    }
    assert payload["plan"]["id"] == "plan-test"
    assert payload["brief"]["id"] == "ib-test"
    assert payload["milestones"] == [
        {
            "id": "m1",
            "name": "Foundation",
            "description": "Set up the project",
            "order": 1,
        },
        {
            "id": "m2",
            "name": "Interface",
            "description": "Build the user-facing flow",
            "order": 2,
        },
    ]
    assert [task["id"] for task in payload["tasks"]] == ["task-setup", "task-ui"]
    assert payload["dependencies"] == [{"from": "task-setup", "to": "task-ui"}]
    assert payload["metrics"]["task_count"] == 2
    assert payload["metrics"]["dependency_edge_count"] == 1


def test_plan_snapshot_content_hash_is_stable_for_equivalent_input(tmp_path):
    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    plan = _execution_plan()
    brief = _implementation_brief()
    reordered_plan = dict(reversed(list(copy.deepcopy(plan).items())))
    reordered_brief = dict(reversed(list(copy.deepcopy(brief).items())))

    exporter = PlanSnapshotExporter()
    exporter.export(plan, brief, str(first_path))
    exporter.export(reordered_plan, reordered_brief, str(second_path))

    first = json.loads(first_path.read_text())
    second = json.loads(second_path.read_text())
    assert first["content_hash"] == second["content_hash"]


def test_plan_snapshot_content_hash_changes_when_plan_content_changes(tmp_path):
    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    changed_plan = _execution_plan()
    changed_plan["tasks"][1]["acceptance_criteria"].append("Dashboard is responsive")

    exporter = PlanSnapshotExporter()
    exporter.export(_execution_plan(), _implementation_brief(), str(first_path))
    exporter.export(changed_plan, _implementation_brief(), str(second_path))

    first = json.loads(first_path.read_text())
    second = json.loads(second_path.read_text())
    assert first["content_hash"] != second["content_hash"]


def test_plan_snapshot_export_validation_passes():
    result = validate_export(_execution_plan(), _implementation_brief(), "plan-snapshot")

    assert result.passed
    assert result.findings == []


def test_plan_snapshot_validation_requires_sections_and_hash(tmp_path):
    output_path = tmp_path / "snapshot.json"
    PlanSnapshotExporter().export(_execution_plan(), _implementation_brief(), str(output_path))
    payload = json.loads(output_path.read_text())
    del payload["content_hash"]
    del payload["metrics"]
    output_path.write_text(json.dumps(payload))

    findings = validate_rendered_export(
        target="plan-snapshot",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert {finding.code for finding in findings} >= {
        "plan_snapshot.missing_key",
        "plan_snapshot.invalid_content_hash",
    }
    assert any("metrics" in finding.message for finding in findings)


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
        "tasks": [
            _task("task-setup", "Setup project", "completed"),
            _task(
                "task-ui",
                "Build dashboard",
                "pending",
                milestone="Interface",
                depends_on=["task-setup"],
                estimated_complexity="medium",
            ),
        ],
    }


def _task(
    task_id,
    title,
    status,
    *,
    milestone="Foundation",
    depends_on=None,
    estimated_complexity="low",
):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
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
        "problem_statement": "Need stable snapshots",
        "mvp_goal": "Archive execution plan state",
        "product_surface": "CLI",
        "scope": ["Plan commands"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use exporter registry",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Incorrect hashes"],
        "validation_plan": "Run snapshot tests",
        "definition_of_done": ["Snapshot is valid"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
