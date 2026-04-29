import json

import pytest

from blueprint.domain import ImplementationBrief
from blueprint.generators.brief_generator import BriefGenerator


def test_valid_generated_json_becomes_complete_implementation_brief_payload():
    provider = FakeLLMProvider(
        {
            "title": "Validated Import Preview Workflow",
            "target_user": "Operations teams importing backlog records",
            "buyer": "Engineering operations",
            "workflow_context": "Before committing imported records",
            "problem_statement": "Operators need confidence before storing imports.",
            "mvp_goal": "Provide a preview and then persist accepted rows.",
            "product_surface": "CLI",
            "scope": ["Preview parsed rows", "Persist accepted rows"],
            "non_goals": ["Interactive row editing"],
            "assumptions": ["The source parser remains unchanged"],
            "architecture_notes": "Add validation at the generator boundary.",
            "data_requirements": "Source brief and generated implementation details.",
            "risks": ["Preview output could diverge from persisted rows"],
            "validation_plan": "Run focused brief generator validation tests.",
            "definition_of_done": ["A validated brief payload is returned"],
        }
    )

    brief = BriefGenerator(provider).generate(_source_brief(), model="test-model")

    assert brief["id"].startswith("ib-")
    assert brief["source_brief_id"] == "sb-test"
    assert brief["title"] == "Validated Import Preview Workflow"
    assert brief["domain"] == "operations"
    assert brief["integration_points"] == []
    assert brief["status"] == "draft"
    assert brief["generation_model"] == "test-model"
    assert brief["generation_tokens"] == 123
    assert ImplementationBrief.model_validate(brief)


def test_missing_required_generated_fields_raise_clear_validation_error():
    provider = FakeLLMProvider(
        {
            "target_user": "Operations teams",
            "problem_statement": "Operators need confidence before storing imports.",
            "mvp_goal": "Provide a preview before committing rows.",
            "scope": ["Preview parsed rows"],
            "non_goals": ["Interactive row editing"],
            "assumptions": ["The source parser remains unchanged"],
            "risks": ["Preview output could diverge from persisted rows"],
            "validation_plan": "Run focused brief generator validation tests.",
            "definition_of_done": ["A validated brief payload is returned"],
        },
        raw_suffix="\nsecret-token-should-not-expand-beyond-preview",
    )

    with pytest.raises(ValueError) as exc_info:
        BriefGenerator(provider).generate(_source_brief(), model="test-model")

    message = str(exc_info.value)
    assert "Generated implementation brief failed domain validation" in message
    assert "title: Field required" in message
    assert "LLM output preview:" in message
    assert '"target_user": "Operations teams"' in message


def test_optional_list_fields_default_to_empty_lists_when_missing_or_null():
    payload = _valid_generated_payload()
    payload["integration_points"] = None
    provider = FakeLLMProvider(payload)

    brief = BriefGenerator(provider).generate(_source_brief(), model="test-model")

    assert brief["integration_points"] == []


class FakeLLMProvider:
    def __init__(self, payload, raw_suffix=""):
        self.payload = payload
        self.raw_suffix = raw_suffix

    def generate(self, prompt, model, temperature, max_tokens, system):
        return {
            "content": json.dumps(self.payload) + self.raw_suffix,
            "model": model,
            "usage": {"total_tokens": 123},
        }


def _source_brief():
    return {
        "id": "sb-test",
        "title": "Source import workflow",
        "domain": "operations",
        "summary": "Source context with constraints",
        "source_project": "manual",
        "source_entity_type": "markdown_brief",
        "source_id": "source-import-workflow",
        "source_payload": {},
        "source_links": {"file": "brief.md"},
    }


def _valid_generated_payload():
    return {
        "title": "Validated Import Preview Workflow",
        "target_user": "Operations teams importing backlog records",
        "buyer": "Engineering operations",
        "workflow_context": "Before committing imported records",
        "problem_statement": "Operators need confidence before storing imports.",
        "mvp_goal": "Provide a preview and then persist accepted rows.",
        "product_surface": "CLI",
        "scope": ["Preview parsed rows", "Persist accepted rows"],
        "non_goals": ["Interactive row editing"],
        "assumptions": ["The source parser remains unchanged"],
        "architecture_notes": "Add validation at the generator boundary.",
        "data_requirements": "Source brief and generated implementation details.",
        "risks": ["Preview output could diverge from persisted rows"],
        "validation_plan": "Run focused brief generator validation tests.",
        "definition_of_done": ["A validated brief payload is returned"],
    }
