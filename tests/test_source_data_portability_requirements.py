import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_data_portability_requirements import (
    SourceDataPortabilityRequirement,
    SourceDataPortabilityRequirementsReport,
    build_source_data_portability_requirements,
    derive_source_data_portability_requirements,
    extract_source_data_portability_requirements,
    generate_source_data_portability_requirements,
    source_data_portability_requirements_to_dict,
    source_data_portability_requirements_to_dicts,
    source_data_portability_requirements_to_markdown,
    summarize_source_data_portability_requirements,
)


def test_nested_source_payload_extracts_portability_categories_with_evidence():
    result = build_source_data_portability_requirements(
        _source_brief(
            source_payload={
                "portability": {
                    "user": "Users must be able to export my data from profile settings.",
                    "account": "Account data download must include projects, invoices, and audit logs.",
                    "format": "Exports must use machine-readable JSON and CSV files in a zip archive.",
                    "tenant": "Tenant export is required for enterprise workspace migrations.",
                    "gdpr": "GDPR data portability under Article 20 must be supported for data subjects.",
                    "retention": "Download links expire after 7 days and export files are purged after 14 days.",
                    "delivery": "Async export delivery emails a download link when the export job completes.",
                }
            }
        )
    )

    assert isinstance(result, SourceDataPortabilityRequirementsReport)
    assert result.source_id == "source-portability"
    assert all(isinstance(record, SourceDataPortabilityRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "user_export",
        "account_data_download",
        "machine_readable_format",
        "tenant_export",
        "gdpr_portability",
        "export_retention_window",
        "async_export_delivery",
    ]
    by_category = {record.category: record for record in result.records}
    assert any("source_payload.portability.user" in item for item in by_category["user_export"].evidence)
    assert by_category["gdpr_portability"].suggested_owner == "privacy_owner"
    assert result.summary["requirement_count"] == 7
    assert result.summary["category_counts"]["tenant_export"] == 1
    assert result.summary["high_confidence_count"] >= 5
    assert result.summary["categories"] == [record.category for record in result.records]
    assert result.summary["owner_counts"]["privacy_owner"] == 1


def test_implementation_brief_fields_contribute_evidence():
    model = ImplementationBrief.model_validate(
        _implementation(
            scope=[
                "Account export must provide a self-service account data download.",
                "Workspace export must support tenant export for enterprise admins.",
            ],
            architecture_notes=(
                "Background export jobs generate JSON and CSV archives and email download links."
            ),
            data_requirements=(
                "GDPR data portability requests include user export of personal data."
            ),
            definition_of_done=[
                "Download links expire after 72 hours.",
                "Users receive a notification when the async export is ready.",
            ],
        )
    )

    result = build_source_data_portability_requirements(model)

    assert result.source_id == "impl-portability"
    assert [record.category for record in result.records] == [
        "user_export",
        "account_data_download",
        "machine_readable_format",
        "tenant_export",
        "gdpr_portability",
        "export_retention_window",
        "async_export_delivery",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["account_data_download"].evidence == (
        "scope[0]: Account export must provide a self-service account data download.",
    )
    assert any("architecture_notes" in item for item in by_category["machine_readable_format"].evidence)
    assert by_category["export_retention_window"].confidence >= 0.85
    assert any("definition_of_done[1]" in item for item in by_category["async_export_delivery"].evidence)


def test_duplicate_category_evidence_merges_deterministically():
    result = build_source_data_portability_requirements(
        {
            "id": "dupe-portability",
            "source_payload": {
                "exports": {
                    "account_export": "Account data download must include projects and audit logs.",
                    "same_account_export": "Account data download must include projects and audit logs.",
                    "gdpr": "GDPR data portability must support Article 20 requests.",
                },
                "acceptance_criteria": [
                    "Account data download must include projects and audit logs.",
                    "GDPR data portability must support Article 20 requests.",
                ],
            },
        }
    )

    assert [record.category for record in result.records] == [
        "account_data_download",
        "gdpr_portability",
    ]
    assert result.records[0].evidence == (
        "source_payload.acceptance_criteria[0]: Account data download must include projects and audit logs.",
    )
    assert result.records[0].confidence == 0.95
    assert result.records[1].confidence == 0.95
    assert result.summary["categories"] == ["account_data_download", "gdpr_portability"]


def test_sourcebrief_object_serialization_markdown_and_summary_helpers_are_stable():
    source = _source_brief(
        source_id="source-portability-model",
        summary="Account export must provide machine-readable downloads.",
        source_payload={
            "requirements": [
                "User export must allow people to download my data.",
                "Async export delivery emails a link when ready.",
            ],
            "privacy": {"gdpr_portability": "GDPR portability support is required."},
            "retention": "Export retention window keeps files available for 30 days.",
        },
    )
    original = copy.deepcopy(source)
    mapping_result = build_source_data_portability_requirements(source)
    model_result = generate_source_data_portability_requirements(SourceBrief.model_validate(source))
    derived = derive_source_data_portability_requirements(SourceBrief.model_validate(source))
    extracted = extract_source_data_portability_requirements(SourceBrief.model_validate(source))
    object_result = build_source_data_portability_requirements(
        SimpleNamespace(
            id="object-portability",
            summary="Tenant export must provide workspace export archives.",
            metadata={"format": "Machine-readable CSV export is required."},
        )
    )
    payload = source_data_portability_requirements_to_dict(model_result)
    markdown = source_data_portability_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert extracted == model_result.requirements
    assert [record.category for record in object_result.records] == [
        "machine_readable_format",
        "tenant_export",
    ]
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_data_portability_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_data_portability_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_data_portability_requirements(model_result) == model_result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "category",
        "confidence",
        "evidence",
        "suggested_owner",
        "suggested_planning_note",
    ]
    assert markdown.startswith("# Source Data Portability Requirements Report: source-portability-model")
    assert (
        "| Category | Confidence | Evidence | Suggested Owner | Suggested Planning Note |"
        in markdown
    )


def test_empty_invalid_and_negated_inputs_return_stable_empty_reports():
    empty = build_source_data_portability_requirements(
        _source_brief(source_id="empty-portability", summary="Update export button copy only.")
    )
    repeat = build_source_data_portability_requirements(
        _source_brief(source_id="empty-portability", summary="Update export button copy only.")
    )
    negated = build_source_data_portability_requirements(
        {"id": "negated-portability", "summary": "No data portability or account export changes are required."}
    )
    invalid = build_source_data_portability_requirements("not a source brief")

    expected_summary = {
        "requirement_count": 0,
        "category_counts": {
            "user_export": 0,
            "account_data_download": 0,
            "machine_readable_format": 0,
            "tenant_export": 0,
            "gdpr_portability": 0,
            "export_retention_window": 0,
            "async_export_delivery": 0,
        },
        "high_confidence_count": 0,
        "categories": [],
        "owner_counts": {},
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-portability"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No data portability requirements were found" in empty.to_markdown()
    assert negated.requirements == ()
    assert invalid.source_id is None
    assert invalid.requirements == ()
    assert invalid.summary == expected_summary


def _source_brief(
    *,
    source_id="source-portability",
    title="Data portability requirements",
    domain="exports",
    summary="General data portability requirements.",
    source_payload=None,
    source_links=None,
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
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }


def _implementation(
    *,
    scope=None,
    architecture_notes=None,
    data_requirements=None,
    definition_of_done=None,
):
    return {
        "id": "impl-portability",
        "source_brief_id": "source-portability",
        "title": "Data export portability",
        "domain": "exports",
        "target_user": "workspace admins",
        "buyer": "enterprise",
        "workflow_context": "Normalize data portability obligations before export job planning.",
        "problem_statement": "Customers need portable account data.",
        "mvp_goal": "Capture export portability requirements in the execution plan.",
        "product_surface": "settings exports",
        "scope": scope or [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": data_requirements,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review release scenarios.",
        "definition_of_done": definition_of_done or [],
    }
