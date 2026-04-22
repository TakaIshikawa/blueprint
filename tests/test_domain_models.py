import pytest
from pydantic import ValidationError

from blueprint.domain import (
    ExecutionPlan,
    ExecutionTask,
    ExportRecord,
    ImplementationBrief,
    SourceBrief,
)
from blueprint.exporters.mermaid import MermaidExporter
from blueprint.store import init_db


def test_domain_models_validate_supported_records():
    SourceBrief.model_validate(_source_brief())
    ImplementationBrief.model_validate(_implementation_brief())
    ExecutionPlan.model_validate({**_execution_plan(), "tasks": [_execution_task()]})
    ExecutionTask.model_validate(_execution_task())
    ExportRecord.model_validate(
        {
            "id": "exp-test",
            "execution_plan_id": "plan-test",
            "target_engine": "mermaid",
            "export_format": "mermaid",
            "output_path": "plan.mmd",
            "export_metadata": {"brief_id": "ib-test"},
        }
    )


def test_store_validates_before_source_brief_insert(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    invalid_source_brief = {**_source_brief(), "source_payload": ["not", "a", "dict"]}

    with pytest.raises(ValidationError):
        store.insert_source_brief(invalid_source_brief)

    assert store.list_source_briefs() == []


def test_store_keeps_return_values_as_dicts(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), [_execution_task()])

    plan = store.get_execution_plan("plan-test")

    assert isinstance(plan, dict)
    assert isinstance(plan["tasks"][0], dict)
    assert plan["tasks"][0]["metadata"] == {}


def test_exporter_validates_before_rendering(tmp_path):
    output_path = tmp_path / "graph.mmd"
    invalid_plan = {**_execution_plan(), "milestones": "not-a-list", "tasks": []}

    with pytest.raises(ValidationError):
        MermaidExporter().export(invalid_plan, _implementation_brief(), str(output_path))

    assert not output_path.exists()


def _source_brief():
    return {
        "id": "sb-test",
        "title": "Source Brief",
        "domain": "testing",
        "summary": "Normalize source data",
        "source_project": "manual",
        "source_entity_type": "note",
        "source_id": "note-1",
        "source_payload": {"title": "Source Brief"},
        "source_links": {"path": "notes/source.md"},
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Implementation Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need validated records",
        "mvp_goal": "Validate dictionaries at boundaries",
        "product_surface": "CLI",
        "scope": ["Domain validation"],
        "non_goals": ["Schema migrations"],
        "assumptions": ["Callers still expect dictionaries"],
        "architecture_notes": "Use Pydantic at boundaries",
        "data_requirements": "Store and exporter dictionaries",
        "integration_points": [],
        "risks": ["Breaking compatibility"],
        "validation_plan": "Run domain model tests",
        "definition_of_done": ["Invalid payloads are rejected"],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Set up validation"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement validation",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _execution_task():
    return {
        "id": "task-test",
        "execution_plan_id": "plan-test",
        "title": "Validate payload",
        "description": "Reject invalid dictionaries",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": ["src/blueprint/domain/models.py"],
        "acceptance_criteria": ["Invalid dictionaries raise validation errors"],
        "estimated_complexity": "low",
        "status": "pending",
    }
