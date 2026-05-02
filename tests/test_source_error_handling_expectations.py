import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_error_handling_expectations import (
    SourceErrorHandlingExpectation,
    SourceErrorHandlingExpectationsReport,
    build_source_error_handling_expectations_report,
    extract_source_error_handling_expectation_records,
    extract_source_error_handling_expectations,
    source_error_handling_expectations_report_to_dict,
    source_error_handling_expectations_report_to_markdown,
    source_error_handling_expectations_to_dicts,
)


def test_detects_error_handling_expectations_across_supported_fields():
    result = build_source_error_handling_expectations_report(
        _source_brief(
            title="Checkout error handling",
            summary="The payment flow needs explicit error handling and a fallback to manual review.",
            source_payload={
                "problem_statement": "Retries with exponential backoff are required for gateway calls.",
                "context": "Requests time out after 3 seconds and then use limited degraded mode.",
                "constraints": [
                    "Validation errors must identify invalid input fields.",
                ],
                "risks": [
                    "Partial failure is possible when some items fail in batch settlement.",
                ],
                "acceptance_criteria": [
                    "Users see a friendly error message that explains the failure.",
                ],
                "metadata": {"failure_behavior": "Handle failures without losing successful items."},
            },
        )
    )

    by_type = {record.expectation_type: record for record in result.records}

    assert isinstance(result, SourceErrorHandlingExpectationsReport)
    assert all(isinstance(record, SourceErrorHandlingExpectation) for record in result.records)
    assert list(by_type) == [
        "error_handling",
        "fallback",
        "retry",
        "timeout",
        "degraded_mode",
        "validation_error",
        "partial_failure",
        "user_facing_failure",
    ]
    assert by_type["fallback"].recommended_controls
    assert by_type["retry"].review_questions
    assert by_type["timeout"].confidence >= 0.7
    assert "source_payload.acceptance_criteria[0]" in by_type["user_facing_failure"].evidence[0]
    assert result.summary["expectation_count"] == 8
    assert result.summary["expectation_type_counts"]["partial_failure"] == 1


def test_duplicate_evidence_is_normalized_capped_and_stably_ordered():
    result = build_source_error_handling_expectations_report(
        {
            "id": "dupe-errors",
            "title": "Retry requirements",
            "summary": "Retry failed requests with backoff.",
            "constraints": [
                "Retry failed requests with backoff.",
                "retry failed requests with backoff.",
                "Retry writes only when idempotent.",
                "Retry reads after transient errors.",
                "Retry notifications after provider failures.",
                "Retry webhook delivery after 5xx.",
            ],
            "metadata": {"retry": "Retry failed requests with backoff."},
        }
    )

    retry = next(record for record in result.expectations if record.expectation_type == "retry")

    assert len(retry.evidence) == 4
    assert len(retry.evidence) == len(
        {_statement(evidence).casefold() for evidence in retry.evidence}
    )
    assert retry.evidence == tuple(sorted(retry.evidence, key=lambda item: item.casefold()))


def test_mapping_and_sourcebrief_inputs_match_without_mutating_source_data():
    source = _source_brief(
        source_id="source-errors",
        summary="Uploads should fall back to local queue when storage times out.",
        source_payload={
            "acceptance_criteria": [
                "Validation errors show field-level messages.",
                "Partial success responses list failed items.",
            ],
            "metadata": {"degraded_mode": "Use read-only mode during provider outage."},
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_error_handling_expectations_report(source)
    model_result = build_source_error_handling_expectations_report(model)
    full_records = extract_source_error_handling_expectation_records(model)
    compact_records = extract_source_error_handling_expectations(model)
    payload = source_error_handling_expectations_report_to_dict(model_result)
    markdown = source_error_handling_expectations_report_to_markdown(model_result)

    assert source == original
    assert payload == source_error_handling_expectations_report_to_dict(mapping_result)
    assert full_records == model_result.expectations
    assert compact_records == tuple(record.to_tuple() for record in model_result.expectations)
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.expectations
    assert model_result.to_dicts() == payload["expectations"]
    assert source_error_handling_expectations_to_dicts(model_result.records) == payload["records"]
    assert list(payload) == ["source_brief_id", "title", "summary", "expectations", "records"]
    assert list(payload["expectations"][0]) == [
        "expectation_type",
        "confidence",
        "evidence",
        "recommended_controls",
        "review_questions",
    ]
    assert markdown.startswith("# Source Error Handling Expectations: source-errors")
    assert "## Summary" in markdown
    assert (
        "| Expectation Type | Confidence | Evidence | Recommended Controls | Review Questions |"
        in markdown
    )


def test_scans_direct_mapping_fields_and_metadata_keys():
    result = build_source_error_handling_expectations_report(
        {
            "id": "direct",
            "title": "Search results",
            "summary": "Search handles failures gracefully.",
            "problem_statement": "Users need clear error messages when search fails.",
            "context": "Provider timeout should return cached results.",
            "constraints": ["Fallback results cannot expose stale prices."],
            "risks": ["Partial failure may hide one supplier."],
            "acceptance_criteria": ["Invalid input returns validation errors."],
            "metadata": {"retry_policy": "Retry provider calls twice with backoff."},
        }
    )

    assert [record.expectation_type for record in result.records] == [
        "error_handling",
        "fallback",
        "retry",
        "timeout",
        "validation_error",
        "partial_failure",
        "user_facing_failure",
    ]


def test_no_signal_empty_and_invalid_inputs_return_no_records():
    empty = build_source_error_handling_expectations_report(
        {"id": "empty", "title": "Copy update", "summary": "Update onboarding copy."}
    )
    invalid = build_source_error_handling_expectations_report(object())

    assert empty.source_brief_id == "empty"
    assert empty.expectations == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary["expectation_count"] == 0
    assert "No source error-handling expectations were found" in empty.to_markdown()
    assert invalid.source_brief_id is None
    assert invalid.expectations == ()


def _statement(evidence):
    return evidence.partition(": ")[2] or evidence


def _source_brief(
    *,
    source_id="source-error-handling",
    title="Error handling expectations",
    domain="platform",
    summary="General error handling expectations.",
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
