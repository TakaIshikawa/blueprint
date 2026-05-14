import json

from blueprint.source_background_job_retry_requirements import (
    build_source_background_job_retry_requirements,
    derive_source_background_job_retry_requirements,
    extract_source_background_job_retry_requirements,
    generate_source_background_job_retry_requirements,
    source_background_job_retry_requirements_to_dict,
    source_background_job_retry_requirements_to_dicts,
    source_background_job_retry_requirements_to_markdown,
    summarize_source_background_job_retry_requirements,
)


def test_extracts_all_retry_categories_and_summary_counts():
    result = build_source_background_job_retry_requirements(_source([
        "Background job retry policy must retry transient timeout and 5xx exceptions.",
        "Background job retry backoff and jitter must use exponential delay with jitter.",
        "Background job retry max attempts must limit to 5 attempts.",
        "Background job retry idempotency must use an idempotency key for safe replay.",
        "Background job retry dead-letter handling must route failures to a DLQ topic.",
        "Background job retry poison message detection must detect permanent non-retryable validation errors.",
        "Background job retry observability must publish attempt metrics and alerts.",
        "Background job retry manual replay must allow operator replay from the admin tool.",
    ]))

    assert [record.requirement_type for record in result.records] == ["retry_policy", "backoff_jitter", "max_attempts", "idempotency", "dead_letter_handling", "poison_message_detection", "retry_observability", "manual_replay"]
    assert result.summary["type_counts"]["retry_policy"] == 1
    assert result.summary["missing_detail_flags"] == []


def test_partial_brief_flags_backoff_idempotency_and_dead_letter():
    result = derive_source_background_job_retry_requirements("Background job retry backoff is required. Background job retry idempotency is required. Background job retry dead-letter handling is required.")

    assert result.summary["missing_detail_flags"] == ["missing_backoff", "missing_idempotency", "missing_dead_letter"]


def test_serializers_aliases_markdown_title_and_negation():
    result = extract_source_background_job_retry_requirements(_source(["Background job retry manual replay must support operator rerun."], "retry-1"))
    payload = source_background_job_retry_requirements_to_dict(result)

    assert generate_source_background_job_retry_requirements("Background job retry observability must emit retry metrics.").summary["requirement_count"] == 1
    assert summarize_source_background_job_retry_requirements(result)["requirement_count"] == 1
    assert json.loads(json.dumps(payload))["source_id"] == "retry-1"
    assert source_background_job_retry_requirements_to_dicts(result) == payload["records"]
    assert source_background_job_retry_requirements_to_dicts(result.records) == payload["records"]
    assert "# Source Background Job Retry Requirements Report: retry-1" in source_background_job_retry_requirements_to_markdown(result)
    assert build_source_background_job_retry_requirements("No background job retry changes are required.").records == ()


def _source(lines, source_id="retry-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Background job retry", "summary": "Background job retry planning", "source_payload": {"requirements": lines}, "source_links": {}}
