import json

from blueprint.domain.models import SourceBrief
from blueprint.source_api_versioning_requirements import (
    SourceAPIVersioningRequirement,
    SourceAPIVersioningRequirementsReport,
    build_source_api_versioning_requirements,
    extract_source_api_versioning_requirements,
    generate_source_api_versioning_requirements,
    source_api_versioning_requirements_to_dict,
    source_api_versioning_requirements_to_dicts,
    source_api_versioning_requirements_to_markdown,
    summarize_source_api_versioning_requirements,
)


def test_extracts_multi_signal_api_versioning_requirements_with_evidence():
    result = build_source_api_versioning_requirements(
        _source_brief(
            summary=(
                "Ship v2 APIs while preserving backwards compatibility with existing clients. "
                "Deprecated endpoints in v1 must return sunset guidance."
            ),
            source_payload={
                "requirements": [
                    "Support both /v1/orders and /v2/orders for a 6 month compatibility window.",
                    "Client migration must include SDK upgrade notes for partner integrations.",
                ],
                "acceptance_criteria": [
                    "Deprecated endpoints publish a sunset date before launch.",
                ],
            },
        )
    )

    assert isinstance(result, SourceAPIVersioningRequirementsReport)
    assert all(isinstance(record, SourceAPIVersioningRequirement) for record in result.records)
    assert [record.finding_type for record in result.records] == [
        "versioned_endpoint",
        "compatibility_window",
        "deprecation_timeline",
        "client_migration",
        "backwards_compatibility",
    ]
    by_type = {record.finding_type: record for record in result.records}
    assert by_type["versioned_endpoint"].readiness == "ready_for_planning"
    assert by_type["compatibility_window"].readiness == "ready_for_planning"
    assert by_type["deprecation_timeline"].confidence == "high"
    assert any("v2 APIs" in item for item in by_type["versioned_endpoint"].evidence)
    assert any(
        "/v1/orders and /v2/orders" in item for item in by_type["compatibility_window"].evidence
    )
    assert "source_payload.requirements[0]" in by_type["compatibility_window"].source_field_paths
    assert "existing clients" in by_type["backwards_compatibility"].matched_terms
    assert result.summary["requirement_count"] == 5
    assert result.summary["type_counts"]["client_migration"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_brief_without_api_versioning_language_returns_stable_empty_report():
    result = build_source_api_versioning_requirements(
        _source_brief(
            title="Profile settings copy",
            summary="Improve onboarding copy and button labels.",
            source_payload={
                "requirements": [
                    "Update account settings labels.",
                    "Keep the existing form submission behavior unchanged.",
                ],
            },
        )
    )
    repeat = build_source_api_versioning_requirements(
        _source_brief(
            title="Profile settings copy",
            summary="Improve onboarding copy and button labels.",
            source_payload={
                "requirements": [
                    "Update account settings labels.",
                    "Keep the existing form submission behavior unchanged.",
                ],
            },
        )
    )

    expected_summary = {
        "requirement_count": 0,
        "type_counts": {
            "versioned_endpoint": 0,
            "compatibility_window": 0,
            "deprecation_timeline": 0,
            "client_migration": 0,
            "backwards_compatibility": 0,
            "unknown_versioning": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "readiness_counts": {"ready_for_planning": 0, "needs_clarification": 0},
        "finding_types": [],
        "follow_up_question_count": 0,
        "status": "no_versioning_language",
    }
    assert result.to_dict() == repeat.to_dict()
    assert result.source_brief_id == "api-versioning-source"
    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == expected_summary
    assert "No source API versioning requirements were inferred" in result.to_markdown()


def test_unknown_versioning_language_and_serialization_helpers_are_stable():
    source = _source_brief(
        source_id="api-versioning-unknown",
        summary="API versioning is TBD and compatibility expectations are unclear.",
    )
    model = SourceBrief.model_validate(source)

    result = build_source_api_versioning_requirements(model)
    generated = generate_source_api_versioning_requirements(model)
    extracted = extract_source_api_versioning_requirements(model)
    payload = source_api_versioning_requirements_to_dict(result)
    markdown = source_api_versioning_requirements_to_markdown(result)

    assert generated.to_dict() == result.to_dict()
    assert extracted == result.requirements
    assert [record.finding_type for record in result.records] == ["unknown_versioning"]
    assert result.records[0].readiness == "needs_clarification"
    assert result.summary["status"] == "needs_clarification"
    assert summarize_source_api_versioning_requirements(result) == result.summary
    assert source_api_versioning_requirements_to_dicts(result) == payload["requirements"]
    assert source_api_versioning_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_brief_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "finding_type",
        "evidence",
        "source_field_paths",
        "matched_terms",
        "follow_up_questions",
        "confidence",
        "readiness",
    ]
    assert markdown.startswith(
        "# Source API Versioning Requirements Report: api-versioning-unknown"
    )


def _source_brief(
    *,
    source_id="api-versioning-source",
    title="API versioning requirements",
    domain="platform",
    summary="General API versioning requirements.",
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
