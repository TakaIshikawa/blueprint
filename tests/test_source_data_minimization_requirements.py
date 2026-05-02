import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_data_minimization_requirements import (
    SourceDataMinimizationRequirement,
    SourceDataMinimizationRequirementsReport,
    build_source_data_minimization_requirements,
    derive_source_data_minimization_requirements,
    extract_source_data_minimization_requirements,
    generate_source_data_minimization_requirements,
    source_data_minimization_requirements_to_dict,
    source_data_minimization_requirements_to_dicts,
    source_data_minimization_requirements_to_markdown,
    summarize_source_data_minimization_requirements,
)


def test_extracts_all_minimization_categories_from_nested_source_payloads():
    result = build_source_data_minimization_requirements(
        _source_brief(
            source_payload={
                "privacy": {
                    "data_minimization": [
                        "Signup must collect only necessary fields for account creation.",
                        "Optional profile attributes should be skipped unless the user adds them.",
                        "Mask unused sensitive data before logs or exports.",
                        "Do not store raw webhook payloads after required fields are extracted.",
                        "Use personal data only for the stated onboarding purpose.",
                        "Field-level retention must purge unused attributes after 30 days.",
                        "Telemetry minimization must drop PII from analytics events and traces.",
                        "Tracking and personalization must be default-off until opt-in.",
                    ]
                }
            }
        )
    )

    assert isinstance(result, SourceDataMinimizationRequirementsReport)
    assert result.source_id == "sb-minimization"
    assert all(isinstance(record, SourceDataMinimizationRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "necessary_fields",
        "optional_attributes",
        "masking_redaction",
        "raw_payload_storage",
        "purpose_limitation",
        "field_level_retention",
        "telemetry_minimization",
        "default_off_tracking",
    ]
    by_category = {record.category: record for record in result.records}
    assert "Product owner" in by_category["necessary_fields"].owner_suggestion
    assert "optional profile attributes" in by_category["optional_attributes"].requirement_text.lower()
    assert "raw request" in by_category["raw_payload_storage"].planning_note
    assert "analytics owner" in by_category["default_off_tracking"].owner_suggestion
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"] == {
        "necessary_fields": 1,
        "optional_attributes": 1,
        "masking_redaction": 1,
        "raw_payload_storage": 1,
        "purpose_limitation": 1,
        "field_level_retention": 1,
        "telemetry_minimization": 1,
        "default_off_tracking": 1,
    }


def test_structured_payload_and_implementation_brief_scope_done_are_supported():
    structured = build_source_data_minimization_requirements(
        _source_brief(
            source_payload={
                "requirements": [
                    {
                        "necessary_fields": "Collect only email and workspace name during signup.",
                        "purpose_limitation": "Use onboarding attributes solely for account setup.",
                    },
                    {
                        "raw_payload_storage": "Raw vendor payloads must not be persisted.",
                        "field_level_retention": "Delete optional attributes after 90 days.",
                        "telemetry_minimization": "Analytics events must omit PII.",
                    },
                ]
            }
        )
    )
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Scope requires default-off tracking for analytics until tenant opt-in.",
                "Mask unused PII before diagnostic logs are stored.",
            ],
            definition_of_done=[
                "Definition of done: optional attributes remain skippable and are not required for launch.",
            ],
        )
    )
    object_result = build_source_data_minimization_requirements(
        SimpleNamespace(
            id="object-minimization",
            summary="Telemetry minimization must exclude personal data from traces.",
        )
    )

    assert [record.category for record in structured.records] == [
        "necessary_fields",
        "raw_payload_storage",
        "purpose_limitation",
        "field_level_retention",
        "telemetry_minimization",
    ]
    assert any("source_payload.requirements[0]" in item for item in structured.records[0].evidence)
    assert structured.records[0].source_field == "source_payload.requirements[0]"

    model_result = extract_source_data_minimization_requirements(brief)
    assert model_result.source_id == "impl-minimization"
    assert [record.category for record in model_result.records] == [
        "optional_attributes",
        "masking_redaction",
        "default_off_tracking",
    ]
    assert model_result.records[0].source_field == "definition_of_done[0]"
    assert object_result.records[0].category == "telemetry_minimization"


def test_negated_invalid_and_no_signal_inputs_return_stable_empty_reports():
    empty = build_source_data_minimization_requirements(
        _source_brief(
            summary="Privacy copy update.",
            source_payload={"body": "No new data collection is required and no tracking changes are needed."},
        )
    )
    repeat = build_source_data_minimization_requirements(
        _source_brief(
            summary="Privacy copy update.",
            source_payload={"body": "No new data collection is required and no tracking changes are needed."},
        )
    )
    malformed = build_source_data_minimization_requirements({"id": "brief-empty", "source_payload": {"notes": object()}})
    invalid = build_source_data_minimization_requirements(42)

    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "sb-minimization"
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "necessary_fields": 0,
            "optional_attributes": 0,
            "masking_redaction": 0,
            "raw_payload_storage": 0,
            "purpose_limitation": 0,
            "field_level_retention": 0,
            "telemetry_minimization": 0,
            "default_off_tracking": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
    }
    assert "No data minimization requirements were found" in empty.to_markdown()
    assert malformed.records == ()
    assert invalid.records == ()


def test_aliases_serialization_markdown_json_ordering_and_no_input_mutation():
    source = _source_brief(
        source_id="minimization-model",
        source_payload={
            "requirements": [
                "Collect only necessary fields and escape plan | review notes.",
                "Telemetry minimization must omit account id from analytics events.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_data_minimization_requirements(source)
    model_result = generate_source_data_minimization_requirements(model)
    derived = derive_source_data_minimization_requirements(model)
    extracted = extract_source_data_minimization_requirements(model)
    text_result = build_source_data_minimization_requirements("Raw request payloads must not be stored.")
    payload = source_data_minimization_requirements_to_dict(model_result)
    markdown = source_data_minimization_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_data_minimization_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert extracted.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_data_minimization_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_data_minimization_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_data_minimization_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "requirement_text",
        "owner_suggestion",
        "planning_note",
        "source_field",
        "evidence",
        "confidence",
        "unresolved_questions",
    ]
    assert [record.category for record in model_result.records] == [
        "necessary_fields",
        "telemetry_minimization",
    ]
    assert model_result.records[0].requirement_category == "necessary_fields"
    assert model_result.records[0].planning_notes == (model_result.records[0].planning_note,)
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Category | Requirement | Owner Suggestion | Planning Note | Source Field | Confidence | Unresolved Questions | Evidence |" in markdown
    assert "plan \\| review notes" in markdown
    assert text_result.records[0].category == "raw_payload_storage"


def _source_brief(
    *,
    source_id="sb-minimization",
    title="Data minimization requirements",
    domain="privacy",
    summary="General privacy requirements.",
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "impl-minimization",
        "source_brief_id": "source-minimization",
        "title": "Data minimization rollout",
        "domain": "privacy",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need data minimization requirements before task generation.",
        "problem_statement": "Data minimization requirements need to be extracted early.",
        "mvp_goal": "Plan data minimization work from source briefs.",
        "product_surface": "privacy",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for data minimization coverage.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
    }
