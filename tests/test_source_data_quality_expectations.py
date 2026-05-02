import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_data_quality_expectations import (
    SourceDataQualityExpectation,
    SourceDataQualityExpectationsReport,
    build_source_data_quality_expectations,
    build_source_data_quality_expectations_report,
    extract_source_data_quality_expectations,
    generate_source_data_quality_expectations,
    source_data_quality_expectations_to_dict,
    source_data_quality_expectations_to_dicts,
    source_data_quality_expectations_to_markdown,
    summarize_source_data_quality_expectations,
)


def test_structured_source_payload_extracts_all_quality_dimensions_with_nested_paths():
    result = build_source_data_quality_expectations(
        _source(
            source_payload={
                "data_quality": {
                    "completeness": "Customer records must have all required fields present.",
                    "accuracy": "Payment amounts must be accurate against the billing source of truth.",
                    "deduplication": "Imports must de-duplicate duplicate customer records by external id.",
                    "validation": "Reject invalid order rows at the validation gate.",
                    "freshness": "Data quality requires inventory snapshots no older than 15 minutes.",
                    "consistency": "Order totals must be consistent across checkout and invoices.",
                    "nulls": "Null handling must convert blank values to explicit unknown values.",
                    "schema": {"contracts": "Payloads must pass JSON schema conformance checks."},
                    "reconciliation": "Ledger entries must reconcile to payment control totals.",
                }
            }
        )
    )

    by_dimension = {record.dimension: record for record in result.records}

    assert isinstance(result, SourceDataQualityExpectationsReport)
    assert all(isinstance(record, SourceDataQualityExpectation) for record in result.records)
    assert [record.dimension for record in result.records] == [
        "completeness",
        "accuracy",
        "deduplication",
        "validation",
        "freshness_threshold",
        "consistency",
        "null_handling",
        "schema_conformance",
        "reconciliation",
    ]
    assert by_dimension["freshness_threshold"].threshold == "15 minutes"
    assert by_dimension["schema_conformance"].evidence == (
        "source_payload.data_quality.schema.contracts: Payloads must pass JSON schema conformance checks.",
    )
    assert by_dimension["reconciliation"].data_subject == "ledger entries"
    assert result.summary["expectation_count"] == 9
    assert result.summary["dimension_counts"]["schema_conformance"] == 1
    assert result.summary["source_counts"]["quality-source"] == 9
    assert result.summary["confidence_counts"]["high"] >= 7


def test_markdown_string_input_and_freshness_guard_are_stable():
    result = build_source_data_quality_expectations(
        """
# Data quality

- Completeness checks must verify every imported order.
- Data quality validation requires source data no older than 10 minutes.
- Schema conformance should reject payload shape drift.
"""
    )
    freshness_only = build_source_data_quality_expectations("Inventory freshness SLA of 5 minutes.")
    markdown = source_data_quality_expectations_to_markdown(result)

    assert [record.dimension for record in result.records] == [
        "completeness",
        "validation",
        "freshness_threshold",
        "schema_conformance",
    ]
    assert freshness_only.records == ()
    assert result.source_id is None
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source Data Quality Expectations Report")
    assert "| Source | Dimension | Data Subject | Threshold | Confidence | Evidence | Planning Note |" in markdown
    assert "body: Data quality validation requires source data no older than 10 minutes." in markdown


def test_model_mapping_object_and_list_inputs_aggregate_without_mutation():
    source = _source(
        source_id="quality-model",
        summary="Data quality expectations for imports.",
        source_payload={
            "requirements": [
                "Imports must de-duplicate duplicate customer records.",
                "imports must de-duplicate duplicate customer records.",
                "Customer records require null handling for blank values.",
            ],
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    implementation = ImplementationBrief.model_validate(
        _implementation(
            data_requirements="Invoice accuracy must match the billing source of truth.",
            validation_plan="Reconcile payment control totals before launch.",
            definition_of_done=["Schema validation passes for exported payloads."],
        )
    )
    object_result = build_source_data_quality_expectations(
        SimpleNamespace(
            id="object-quality",
            body="Consistency checks should keep order totals aligned across systems.",
        )
    )

    mapping_result = build_source_data_quality_expectations(source)
    model_result = generate_source_data_quality_expectations(model)
    list_result = build_source_data_quality_expectations([model, implementation])
    extracted = extract_source_data_quality_expectations(model)
    payload = source_data_quality_expectations_to_dict(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert extracted == model_result.expectations
    assert list_result.source_id is None
    assert list_result.summary["source_counts"]["quality-model"] == 2
    assert list_result.summary["source_counts"]["impl-quality"] == 4
    assert object_result.source_id == "object-quality"
    assert object_result.records[0].dimension == "consistency"
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.expectations
    assert model_result.to_dicts() == payload["expectations"]
    assert source_data_quality_expectations_to_dicts(model_result) == payload["expectations"]
    assert source_data_quality_expectations_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_data_quality_expectations(model_result) == model_result.summary
    assert list(payload) == ["source_id", "expectations", "summary", "records"]
    assert list(payload["expectations"][0]) == [
        "source_id",
        "dimension",
        "data_subject",
        "threshold",
        "confidence",
        "evidence",
        "planning_note",
    ]
    dedupe = next(record for record in model_result.records if record.dimension == "deduplication")
    assert dedupe.evidence == (
        "source_payload.requirements[0]: Imports must de-duplicate duplicate customer records.",
    )


def test_empty_invalid_and_deterministic_summaries():
    empty = build_source_data_quality_expectations(
        _source(summary="Polish onboarding copy.", source_payload={"body": "No data quality changes are needed."})
    )
    repeat = build_source_data_quality_expectations(
        _source(summary="Polish onboarding copy.", source_payload={"body": "No data quality changes are needed."})
    )
    invalid = build_source_data_quality_expectations(17)
    report_alias = build_source_data_quality_expectations_report(
        {"id": "raw-quality", "data_quality": "Validation must reject invalid payloads."}
    )

    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "quality-source"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "expectation_count": 0,
        "input_count": 1,
        "dimension_counts": {
            "completeness": 0,
            "accuracy": 0,
            "deduplication": 0,
            "validation": 0,
            "freshness_threshold": 0,
            "consistency": 0,
            "null_handling": 0,
            "schema_conformance": 0,
            "reconciliation": 0,
        },
        "source_counts": {},
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "dimensions": [],
        "source_ids": [],
    }
    assert "No source data quality expectations were found" in empty.to_markdown()
    assert invalid.source_id is None
    assert invalid.records == ()
    assert report_alias.records[0].dimension == "validation"


def _source(*, source_id="quality-source", summary="Data quality constraints.", source_payload=None):
    return {
        "id": source_id,
        "title": "Data quality expectations",
        "domain": "data",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "issue",
        "source_id": source_id,
        "source_payload": source_payload or {},
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation(*, data_requirements=None, validation_plan=None, definition_of_done=None):
    return {
        "id": "impl-quality",
        "source_brief_id": "quality-model",
        "title": "Quality planning",
        "domain": "data",
        "target_user": "ops",
        "buyer": "enterprise",
        "workflow_context": "Preserve source-backed quality constraints before task generation.",
        "problem_statement": "Imports need validation and reconciliation.",
        "mvp_goal": "Capture quality constraints in the plan.",
        "product_surface": "data platform",
        "scope": ["Data quality"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": data_requirements,
        "integration_points": [],
        "risks": [],
        "validation_plan": validation_plan or "Review validation evidence.",
        "definition_of_done": definition_of_done or [],
    }
