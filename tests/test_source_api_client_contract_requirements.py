import json

from blueprint.domain.models import SourceBrief
from blueprint.source_api_client_contract_requirements import (
    SourceApiClientContractRequirement,
    SourceApiClientContractRequirementsReport,
    build_source_api_client_contract_requirements,
    extract_source_api_client_contract_requirements,
    source_api_client_contract_requirements_to_dict,
    source_api_client_contract_requirements_to_dicts,
    source_api_client_contract_requirements_to_markdown,
    summarize_source_api_client_contract_requirements,
)


def test_extracts_multi_signal_api_client_contract_requirements_with_evidence():
    result = build_source_api_client_contract_requirements(
        _source_brief(
            summary=(
                "Publish OpenAPI 3.0 spec and generate Python and Java clients. "
                "Consumer contract tests must validate all client SDKs."
            ),
            source_payload={
                "requirements": [
                    "Provide exponential backoff retry guidance for all API operations.",
                    "Webhook callbacks must include signature verification and idempotency tokens.",
                    "Sample requests and curl examples for each endpoint.",
                ],
                "acceptance_criteria": [
                    "Deprecation window of 6 months with migration guides for legacy clients.",
                ],
            },
        )
    )

    assert isinstance(result, SourceApiClientContractRequirementsReport)
    assert all(isinstance(record, SourceApiClientContractRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "openapi_schema",
        "generated_client",
        "sdk_compatibility",
        "consumer_contract_tests",
        "sample_requests",
        "retry_guidance",
        "webhook_callbacks",
        "deprecation_window",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert any("OpenAPI 3.0" in item for item in by_type["openapi_schema"].evidence)
    assert any("Python and Java clients" in item for item in by_type["generated_client"].evidence)
    assert any("signature verification" in item for item in by_type["webhook_callbacks"].evidence)
    assert "source_payload.requirements[1]" in by_type["webhook_callbacks"].source_field_paths
    assert "exponential backoff" in by_type["retry_guidance"].matched_terms
    assert result.summary["requirement_count"] == 8
    assert result.summary["type_counts"]["consumer_contract_tests"] == 1
    assert result.summary["client_artifact_coverage"] > 0
    assert result.summary["contract_test_coverage"] > 0
    assert result.summary["deprecation_coverage"] > 0


def test_brief_without_client_contract_language_returns_stable_empty_report():
    result = build_source_api_client_contract_requirements(
        _source_brief(
            title="Database migration",
            summary="Update user table schema to add new columns.",
            source_payload={
                "requirements": [
                    "Add email_verified column with default false.",
                    "Backfill existing user records.",
                ],
            },
        )
    )
    repeat = build_source_api_client_contract_requirements(
        _source_brief(
            title="Database migration",
            summary="Update user table schema to add new columns.",
            source_payload={
                "requirements": [
                    "Add email_verified column with default false.",
                    "Backfill existing user records.",
                ],
            },
        )
    )

    expected_summary = {
        "requirement_count": 0,
        "source_count": 1,
        "type_counts": {
            "openapi_schema": 0,
            "generated_client": 0,
            "sdk_compatibility": 0,
            "consumer_contract_tests": 0,
            "sample_requests": 0,
            "retry_guidance": 0,
            "webhook_callbacks": 0,
            "deprecation_window": 0,
        },
        "requirement_types": [],
        "follow_up_question_count": 0,
        "client_artifact_coverage": 0,
        "contract_test_coverage": 0,
        "deprecation_coverage": 0,
    }
    assert result.summary == expected_summary
    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.to_dict() == repeat.to_dict()


def test_requirement_deduplication_merges_evidence_without_losing_source_fields():
    result = build_source_api_client_contract_requirements(
        _source_brief(
            summary="Generate SDK clients for Python, Java, and Node.",
            source_payload={
                "requirements": [
                    "SDK compatibility for Python client library.",
                    "Maintain backwards compatibility for Java SDK.",
                ],
                "acceptance": "All SDK clients must pass consumer contract tests.",
            },
        )
    )

    by_type = {record.requirement_type: record for record in result.records}
    sdk_req = by_type["sdk_compatibility"]
    # Multiple source fields should be captured
    assert len(sdk_req.source_field_paths) >= 2
    assert "summary" in sdk_req.source_field_paths
    assert "source_payload.requirements[0]" in sdk_req.source_field_paths or "source_payload.requirements[1]" in sdk_req.source_field_paths
    assert len(sdk_req.evidence) >= 2
    assert "Python" in " ".join(sdk_req.evidence)
    assert "Java" in " ".join(sdk_req.evidence)


def test_dict_serialization_round_trips_without_mutation():
    original = build_source_api_client_contract_requirements(
        _source_brief(
            summary="Publish OpenAPI spec and webhook callback documentation.",
            source_payload={
                "requirements": [
                    "Consumer contract tests using Pact framework.",
                ],
            },
        )
    )

    serialized = original.to_dict()
    assert isinstance(serialized, dict)
    assert serialized["source_brief_id"] == "client-contract-source"
    assert len(serialized["requirements"]) == len(original.requirements)
    assert serialized["summary"]["requirement_count"] == len(original.requirements)

    # Repeat to verify no mutation
    repeat = original.to_dict()
    assert repeat == serialized


def test_to_dicts_helper_serializes_requirements_list():
    report = build_source_api_client_contract_requirements(
        _source_brief(
            summary="SDK generation and retry guidance.",
            source_payload={
                "requirements": [
                    "Exponential backoff retry policy for rate limits.",
                ],
            },
        )
    )

    dicts = source_api_client_contract_requirements_to_dicts(report)
    assert isinstance(dicts, list)
    assert all(isinstance(item, dict) for item in dicts)
    assert len(dicts) == report.summary["requirement_count"]

    # Also test tuple input
    tuple_dicts = source_api_client_contract_requirements_to_dicts(report.requirements)
    assert tuple_dicts == dicts


def test_markdown_output_renders_deterministic_table():
    report = build_source_api_client_contract_requirements(
        _source_brief(
            source_id="client-contract-markdown-test",
            summary="OpenAPI schema with sample requests.",
            source_payload={
                "requirements": [
                    "Provide curl examples for all endpoints.",
                ],
            },
        )
    )

    markdown = source_api_client_contract_requirements_to_markdown(report)
    assert isinstance(markdown, str)
    assert "# Source API Client Contract Requirements Report: client-contract-markdown-test" in markdown
    assert "## Summary" in markdown
    assert "## Requirements" in markdown
    assert "| Type | Source Field Paths | Evidence | Follow-up Questions |" in markdown
    assert "openapi_schema" in markdown
    assert "sample_requests" in markdown

    # Repeat to verify deterministic output
    repeat_markdown = report.to_markdown()
    assert repeat_markdown == markdown


def test_empty_report_markdown_includes_no_requirements_message():
    report = build_source_api_client_contract_requirements(
        _source_brief(
            summary="Internal refactoring with no API changes.",
        )
    )

    markdown = report.to_markdown()
    assert "No source API client contract requirements were inferred." in markdown
    assert "## Requirements" not in markdown


def test_extracts_from_raw_text_input():
    result = build_source_api_client_contract_requirements(
        "Ship OpenAPI spec and generate Java clients with retry guidance."
    )

    assert len(result.requirements) >= 2
    types = {req.requirement_type for req in result.requirements}
    assert "openapi_schema" in types
    assert "generated_client" in types or "sdk_compatibility" in types


def test_extracts_from_mapping_input():
    result = build_source_api_client_contract_requirements(
        {
            "id": "mapping-source",
            "title": "API client contracts",
            "summary": "Consumer contract tests and webhook callbacks.",
            "source_payload": {
                "requirements": "Provide sample requests for all endpoints.",
            },
        }
    )

    assert result.source_brief_id == "mapping-source"
    types = {req.requirement_type for req in result.requirements}
    assert "consumer_contract_tests" in types
    assert "sample_requests" in types or "webhook_callbacks" in types


def test_extracts_from_pydantic_model():
    model = SourceBrief(
        id="pydantic-source",
        title="SDK compatibility requirements",
        domain="api",
        summary="Python SDK with deprecation window.",
        source_project="test",
        source_entity_type="issue",
        source_id="pydantic-source",
        source_payload={
            "requirements": "6 month deprecation window for v1 endpoints.",
        },
        source_links={},
    )

    result = build_source_api_client_contract_requirements(model)
    assert result.source_brief_id == "pydantic-source"
    types = {req.requirement_type for req in result.requirements}
    assert "sdk_compatibility" in types
    assert "deprecation_window" in types


def test_extract_helper_returns_tuple_of_requirements():
    requirements = extract_source_api_client_contract_requirements(
        _source_brief(
            summary="OpenAPI spec and consumer contract tests.",
        )
    )

    assert isinstance(requirements, tuple)
    assert all(isinstance(req, SourceApiClientContractRequirement) for req in requirements)
    assert len(requirements) >= 2


def test_summarize_helper_returns_summary_dict():
    summary = summarize_source_api_client_contract_requirements(
        _source_brief(
            summary="Webhook callbacks with retry guidance.",
        )
    )

    assert isinstance(summary, dict)
    assert "requirement_count" in summary
    assert "type_counts" in summary
    assert summary["requirement_count"] >= 2


def test_summarize_accepts_report_object():
    report = build_source_api_client_contract_requirements(
        _source_brief(summary="OpenAPI spec.")
    )
    summary = summarize_source_api_client_contract_requirements(report)

    assert summary == report.summary


def test_coverage_metrics_calculated_correctly():
    result = build_source_api_client_contract_requirements(
        _source_brief(
            summary="OpenAPI spec and generate Python clients for SDK compatibility.",
            source_payload={
                "requirements": [
                    "Consumer contract tests using Pact.",
                    "6 month deprecation window.",
                ],
            },
        )
    )

    summary = result.summary
    # All client artifacts present (openapi, generated_client, sdk)
    assert summary["client_artifact_coverage"] == 100
    # Contract tests present
    assert summary["contract_test_coverage"] == 100
    # Deprecation present
    assert summary["deprecation_coverage"] == 100


def test_follow_up_questions_reduced_when_evidence_is_specific():
    result = build_source_api_client_contract_requirements(
        _source_brief(
            summary="Generate Python and Java clients using OpenAPI 3.0 spec.",
            source_payload={
                "requirements": [
                    "Deprecation window of 90 days with sunset date 2026-06-01.",
                ],
            },
        )
    )

    by_type = {req.requirement_type: req for req in result.requirements}
    # Generated client mentions languages, so should have fewer questions
    gen_client = by_type.get("generated_client")
    if gen_client:
        assert len(gen_client.follow_up_questions) < 2
    # OpenAPI mentions version, so should have no questions
    openapi = by_type.get("openapi_schema")
    if openapi:
        assert len(openapi.follow_up_questions) == 0
    # Deprecation mentions timeline, so should have no questions
    deprecation = by_type.get("deprecation_window")
    if deprecation:
        assert len(deprecation.follow_up_questions) == 0


def test_matched_terms_captured_for_each_requirement():
    result = build_source_api_client_contract_requirements(
        _source_brief(
            summary="OpenAPI schema with webhook callbacks and retry logic.",
        )
    )

    for req in result.requirements:
        assert len(req.matched_terms) > 0
        # Matched terms should be deduplicated
        assert len(req.matched_terms) == len(set(term.casefold() for term in req.matched_terms))


def _source_brief(
    *,
    source_id="client-contract-source",
    title="API client contract requirements",
    domain="platform",
    summary="General API client contract requirements.",
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
