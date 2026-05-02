import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_rate_limit_requirements import (
    SourceRateLimitRequirement,
    SourceRateLimitRequirementsReport,
    build_source_rate_limit_requirements,
    derive_source_rate_limit_requirements,
    extract_source_rate_limit_requirements,
    generate_source_rate_limit_requirements,
    source_rate_limit_requirements_to_dict,
    source_rate_limit_requirements_to_dicts,
    source_rate_limit_requirements_to_markdown,
    summarize_source_rate_limit_requirements,
)


def test_extracts_explicit_quotas_rate_limits_retry_after_and_scopes():
    result = build_source_rate_limit_requirements(
        _source_brief(
            summary=(
                "Public API must enforce 100 requests per minute per tenant. "
                "Return HTTP 429 with Retry-After header of 30 seconds."
            ),
            source_payload={
                "acceptance_criteria": [
                    "Daily quota is 10,000 API calls per day per user.",
                    "Burst capacity allows 500 requests per minute for short spikes.",
                ],
            },
        )
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert isinstance(result, SourceRateLimitRequirementsReport)
    assert all(isinstance(record, SourceRateLimitRequirement) for record in result.records)
    assert {"rate_limit", "quota", "burst", "retry_after", "tenant_scope", "user_scope"} <= set(
        by_type
    )
    assert by_type["rate_limit"].value == "100 requests per minute"
    assert by_type["rate_limit"].limit_scope == "tenant"
    assert by_type["quota"].value == "10,000 API calls per day"
    assert by_type["quota"].limit_scope == "user"
    assert by_type["retry_after"].value == "Retry-After header of 30 seconds"
    assert any(
        "100 requests per minute per tenant" in item for item in by_type["rate_limit"].evidence
    )
    assert result.summary["requirement_type_counts"]["retry_after"] == 1
    assert result.summary["scopes"] == ["tenant", "user"]


def test_infers_throttling_and_retry_concerns_without_numeric_limits():
    result = build_source_rate_limit_requirements(
        {
            "id": "inferred-throttle",
            "title": "Partner API resilience",
            "risks": [
                "Partner APIs may throttle clients during traffic spikes.",
                "Clients need exponential backoff when too many requests are returned.",
            ],
            "metadata": {
                "tenant_limit": "Tenant scoped throttling must avoid noisy-neighbor impact."
            },
        }
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert {"throttling", "burst", "retry_after", "tenant_scope"} <= set(by_type)
    assert by_type["throttling"].confidence in {"medium", "low"}
    assert by_type["retry_after"].planning_note.startswith("Specify Retry-After")
    assert result.summary["requirement_count"] == len(result.records)


def test_empty_malformed_dict_and_plain_text_inputs_do_not_raise():
    empty = build_source_rate_limit_requirements(
        {"id": "empty", "title": "Copy update", "summary": "Update onboarding copy."}
    )
    malformed = build_source_rate_limit_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_rate_limit_requirements(object())
    text = build_source_rate_limit_requirements("Throttle API writes and return 429s.")

    assert empty.source_brief_id == "empty"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary["requirement_count"] == 0
    assert malformed.records == ()
    assert invalid.source_brief_id is None
    assert invalid.records == ()
    assert {record.requirement_type for record in text.records} >= {"rate_limit", "throttling"}


def test_sourcebrief_model_input_aliases_serialization_and_no_source_mutation():
    source = _source_brief(
        source_id="rate-limit-model",
        title="Rate limit model",
        summary="API limits are tenant scoped.",
        source_payload={
            "constraints": [
                "Requests are limited to 60 calls per minute per account.",
                "Retry after 5 seconds when throttled.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_rate_limit_requirements(source)
    model_result = summarize_source_rate_limit_requirements(model)
    generated = generate_source_rate_limit_requirements(model)
    derived = derive_source_rate_limit_requirements(model)
    extracted = extract_source_rate_limit_requirements(model)
    payload = source_rate_limit_requirements_to_dict(model_result)

    assert source == original
    assert payload == source_rate_limit_requirements_to_dict(mapping_result)
    assert generated.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert extracted == model_result.requirements
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_rate_limit_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_rate_limit_requirements_to_dicts(model_result.records) == payload["records"]
    assert list(payload) == ["source_brief_id", "title", "summary", "requirements", "records"]
    assert list(payload["requirements"][0]) == [
        "requirement_type",
        "value",
        "limit_scope",
        "source_field",
        "evidence",
        "confidence",
        "planning_note",
    ]


def test_markdown_escapes_pipes_and_serialization_order_is_stable():
    result = build_source_rate_limit_requirements(
        _source_brief(
            source_id="rate-limit-markdown",
            title="Partner | quota",
            summary="Partner | API quota must enforce 120 requests per minute per workspace.",
            source_payload={
                "requirements": [
                    "Retry-After | cooldown is 15 seconds after throttling.",
                    "Burst | spike handling must tolerate short bursts.",
                ]
            },
        )
    )

    payload = source_rate_limit_requirements_to_dict(result)
    markdown = source_rate_limit_requirements_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source Rate Limit Requirements Report: rate-limit-markdown")
    assert (
        "| Type | Value | Scope | Confidence | Source Field | Evidence | Planning Note |"
        in markdown
    )
    assert "Partner \\| API quota" in markdown
    assert "Retry-After \\| cooldown" in markdown
    assert [record.confidence for record in result.records] == sorted(
        (record.confidence for record in result.records),
        key={"high": 0, "medium": 1, "low": 2}.get,
    )


def _source_brief(
    *,
    source_id="source-rate-limit",
    title="Rate limit requirements",
    domain="platform",
    summary="General rate limit requirements.",
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
