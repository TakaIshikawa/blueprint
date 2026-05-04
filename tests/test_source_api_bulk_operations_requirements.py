from blueprint.domain.models import SourceBrief
from blueprint.source_api_bulk_operations_requirements import (
    SourceApiBulkOperationsRequirement,
    SourceApiBulkOperationsRequirementsReport,
    build_source_api_bulk_operations_requirements,
    extract_source_api_bulk_operations_requirements,
    source_api_bulk_operations_requirements_to_dicts,
    source_api_bulk_operations_requirements_to_markdown,
    summarize_source_api_bulk_operations_requirements,
)


def test_extracts_multi_signal_bulk_operations_requirements_with_evidence():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary=(
                "Implement bulk user creation API with batch size limits and partial success handling. "
                "Support atomic transactions with rollback on failure."
            ),
            source_payload={
                "requirements": [
                    "Maximum batch size of 100 items with validation before processing.",
                    "Track progress and report completion status for each batch.",
                    "Handle partial failures by continuing processing and reporting individual errors.",
                    "Optimize performance for large batches with parallel processing.",
                ],
                "acceptance_criteria": [
                    "Support batch pagination for operations exceeding 1000 records.",
                    "Provide detailed error reporting for each failed item.",
                ],
            },
        )
    )

    assert isinstance(result, SourceApiBulkOperationsRequirementsReport)
    assert all(isinstance(record, SourceApiBulkOperationsRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "batch_size_limits",
        "partial_success_handling",
        "transaction_semantics",
        "progress_tracking",
        "validation_checks",
        "performance_requirements",
        "batch_pagination",
        "error_reporting",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert any("100 items" in item or "batch size" in item.lower() for item in by_type["batch_size_limits"].evidence)
    assert any("partial" in item.lower() or "continue" in item.lower() for item in by_type["partial_success_handling"].evidence)
    assert any("atomic" in item.lower() or "rollback" in item.lower() for item in by_type["transaction_semantics"].evidence)
    assert result.summary["requirement_count"] == 8
    assert result.summary["type_counts"]["batch_size_limits"] == 1
    assert result.summary["reliability_coverage"] > 0
    assert result.summary["observability_coverage"] > 0
    assert result.summary["data_integrity_coverage"] > 0


def test_brief_without_bulk_language_returns_stable_empty_report():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            title="Single record API",
            summary="Create a single user via REST API.",
            source_payload={
                "requirements": [
                    "Validate user email and password.",
                    "Return user ID on success.",
                ],
            },
        )
    )
    repeat = build_source_api_bulk_operations_requirements(
        _source_brief(
            title="Single record API",
            summary="Create a single user via REST API.",
            source_payload={
                "requirements": [
                    "Validate user email and password.",
                    "Return user ID on success.",
                ],
            },
        )
    )

    expected_summary = {
        "requirement_count": 0,
        "source_count": 1,
        "type_counts": {
            "batch_size_limits": 0,
            "partial_success_handling": 0,
            "transaction_semantics": 0,
            "progress_tracking": 0,
            "validation_checks": 0,
            "performance_requirements": 0,
            "batch_pagination": 0,
            "error_reporting": 0,
        },
        "requirement_types": [],
        "follow_up_question_count": 0,
        "reliability_coverage": 0,
        "observability_coverage": 0,
        "data_integrity_coverage": 0,
    }
    assert result.summary == expected_summary
    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.to_dict() == repeat.to_dict()


def test_rest_batch_api_requirements_detected():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Implement REST batch API for bulk updates.",
            source_payload={
                "requirements": [
                    "Support batch size up to 500 records per request.",
                    "Continue processing on individual failures.",
                    "Return detailed error for each failed item.",
                    "Validate all items before processing (pre-flight check).",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "batch_size_limits" in types
    assert "partial_success_handling" in types
    assert "error_reporting" in types
    assert "validation_checks" in types


def test_graphql_bulk_mutation_requirements_detected():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Add GraphQL bulk mutation for creating multiple records.",
            source_payload={
                "requirements": [
                    "Support atomic transactions with all-or-nothing semantics.",
                    "Rollback entire batch if any mutation fails.",
                    "Track mutation progress and report completion status.",
                ],
                "acceptance_criteria": [
                    "Maximum 100 items per bulk mutation.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "transaction_semantics" in types
    assert "batch_size_limits" in types
    assert "progress_tracking" in types


def test_database_import_patterns_detected():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Implement bulk database import with progress tracking.",
            source_payload={
                "requirements": [
                    "Process large imports in chunks of 1000 records.",
                    "Report progress with processed count and remaining items.",
                    "Optimize throughput for importing millions of records.",
                    "Support dry-run mode for validation before import.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "batch_pagination" in types
    assert "progress_tracking" in types
    assert "performance_requirements" in types
    assert "validation_checks" in types


def test_partial_success_handling_strategies_detected():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Handle partial failures in bulk operations.",
            source_payload={
                "requirements": [
                    "Continue processing remaining items when some fail.",
                    "Return mixed results with succeeded and failed items.",
                    "Provide individual error messages for each failed item.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "partial_success_handling" in types
    assert "error_reporting" in types


def test_transaction_semantics_requirements_detected():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Implement transactional bulk updates.",
            source_payload={
                "requirements": [
                    "Use atomic transactions for all-or-nothing updates.",
                    "Rollback all changes if any item fails.",
                    "Ensure ACID compliance for bulk operations.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "transaction_semantics" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    trans_req = by_type["transaction_semantics"]
    assert any("atomic" in term.lower() or "rollback" in term.lower() or "acid" in term.lower() for term in trans_req.matched_terms)


def test_progress_tracking_mechanisms_detected():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Track and report batch processing progress.",
            source_payload={
                "requirements": [
                    "Report progress status with percentage completion.",
                    "Update job status as processing continues.",
                    "Show processed count and remaining items.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "progress_tracking" in types


def test_validation_and_preflight_checks_detected():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Validate batch input before processing.",
            source_payload={
                "requirements": [
                    "Perform pre-flight validation checks on all items.",
                    "Run dry-run mode to test batch without committing.",
                    "Validate schema and business rules before processing.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "validation_checks" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    val_req = by_type["validation_checks"]
    assert any("pre-flight" in term.lower() or "dry-run" in term.lower() or "validation" in term.lower() for term in val_req.matched_terms)


def test_batch_pagination_strategies_detected():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Paginate large bulk operations into chunks.",
            source_payload={
                "requirements": [
                    "Split large batches into chunks of 500 items.",
                    "Iterate over batch chunks for processing.",
                    "Support batch pagination for operations exceeding limits.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "batch_pagination" in types


def test_performance_optimization_requirements_detected():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Optimize bulk operation performance.",
            source_payload={
                "requirements": [
                    "Improve throughput for bulk inserts.",
                    "Process batches in parallel for better performance.",
                    "Optimize processing time for large datasets.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "performance_requirements" in types


def test_error_reporting_detail_requirements_detected():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Provide detailed error reporting for bulk operations.",
            source_payload={
                "requirements": [
                    "Include error details for each failed item.",
                    "Report which records failed and why.",
                    "Format error response with per-item failure messages.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "error_reporting" in types


def test_requirement_deduplication_merges_evidence():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Batch size limit of 100 items per request.",
            source_payload={
                "requirements": [
                    "Maximum batch size of 100 records.",
                    "Enforce 100 item limit per batch.",
                ],
                "acceptance": "Batch operations must not exceed 100 items.",
            },
        )
    )

    by_type = {record.requirement_type: record for record in result.records}
    batch_req = by_type["batch_size_limits"]
    assert len(batch_req.source_field_paths) >= 2
    assert "summary" in batch_req.source_field_paths
    assert any("requirements" in field for field in batch_req.source_field_paths)
    assert len(batch_req.evidence) >= 2


def test_dict_serialization_round_trips():
    original = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Bulk operations with batch limits and progress tracking.",
            source_payload={
                "requirements": [
                    "Maximum 200 items per batch.",
                    "Report progress status.",
                ],
            },
        )
    )

    serialized = original.to_dict()
    assert isinstance(serialized, dict)
    assert serialized["source_brief_id"] == "bulk-ops-source"
    assert len(serialized["requirements"]) == len(original.requirements)
    assert serialized["summary"]["requirement_count"] == len(original.requirements)

    repeat = original.to_dict()
    assert repeat == serialized


def test_markdown_output_renders_table():
    report = build_source_api_bulk_operations_requirements(
        _source_brief(
            source_id="bulk-ops-markdown-test",
            summary="Bulk operations with batch size limits.",
            source_payload={
                "requirements": ["Maximum 100 items per batch."],
            },
        )
    )

    markdown = source_api_bulk_operations_requirements_to_markdown(report)
    assert isinstance(markdown, str)
    assert "# Source API Bulk Operations Requirements Report: bulk-ops-markdown-test" in markdown
    assert "## Summary" in markdown
    assert "## Requirements" in markdown
    assert "batch_size_limits" in markdown

    repeat_markdown = report.to_markdown()
    assert repeat_markdown == markdown


def test_empty_report_markdown_message():
    report = build_source_api_bulk_operations_requirements(
        _source_brief(summary="Single record update.")
    )

    markdown = report.to_markdown()
    assert "No source API bulk operations requirements were inferred." in markdown


def test_extracts_from_raw_text_input():
    result = build_source_api_bulk_operations_requirements(
        "Implement bulk import with maximum batch size of 500 records, "
        "atomic transactions with rollback, and progress tracking."
    )

    assert len(result.requirements) >= 3
    types = {req.requirement_type for req in result.requirements}
    assert "batch_size_limits" in types
    assert "transaction_semantics" in types
    assert "progress_tracking" in types


def test_extracts_from_mapping_input():
    result = build_source_api_bulk_operations_requirements(
        {
            "id": "mapping-source",
            "title": "Bulk operations",
            "summary": "Batch delete with partial success handling.",
            "source_payload": {
                "requirements": "Continue processing if some items fail.",
            },
        }
    )

    assert result.source_brief_id == "mapping-source"
    types = {req.requirement_type for req in result.requirements}
    assert "partial_success_handling" in types


def test_extract_helper_returns_tuple():
    requirements = extract_source_api_bulk_operations_requirements(
        _source_brief(summary="Bulk operations with batch limits.")
    )

    assert isinstance(requirements, tuple)
    assert all(isinstance(req, SourceApiBulkOperationsRequirement) for req in requirements)


def test_summarize_helper_returns_dict():
    summary = summarize_source_api_bulk_operations_requirements(
        _source_brief(summary="Bulk operations with progress tracking.")
    )

    assert isinstance(summary, dict)
    assert "requirement_count" in summary
    assert "type_counts" in summary


def test_coverage_metrics_calculated():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Bulk operations with partial success, error reporting, progress tracking, transactions, and validation.",
            source_payload={
                "requirements": [
                    "Handle partial failures.",
                    "Report detailed errors.",
                    "Track progress.",
                    "Use atomic transactions.",
                    "Validate before processing.",
                ],
            },
        )
    )

    summary = result.summary
    assert summary["reliability_coverage"] == 100
    assert summary["observability_coverage"] == 100
    assert summary["data_integrity_coverage"] == 100


def test_follow_up_questions_reduced_with_specifics():
    result = build_source_api_bulk_operations_requirements(
        _source_brief(
            summary="Maximum batch size of 100 items.",
            source_payload={
                "requirements": [
                    "Use atomic all-or-nothing transactions with rollback.",
                    "Continue processing remaining items when some fail.",
                ],
            },
        )
    )

    by_type = {req.requirement_type: req for req in result.requirements}
    batch_limits = by_type.get("batch_size_limits")
    if batch_limits:
        assert len(batch_limits.follow_up_questions) == 0
    trans_sem = by_type.get("transaction_semantics")
    if trans_sem:
        assert len(trans_sem.follow_up_questions) < 2
    partial = by_type.get("partial_success_handling")
    if partial:
        assert len(partial.follow_up_questions) < 2


def _source_brief(
    *,
    source_id="bulk-ops-source",
    title="Bulk operations requirements",
    domain="platform",
    summary="General bulk operations requirements.",
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
