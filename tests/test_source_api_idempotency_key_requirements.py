import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_idempotency_key_requirements import (
    SourceAPIIdempotencyKeyRequirement,
    SourceAPIIdempotencyKeyRequirementsReport,
    build_source_api_idempotency_key_requirements,
    derive_source_api_idempotency_key_requirements,
    extract_source_api_idempotency_key_requirements,
    generate_source_api_idempotency_key_requirements,
    source_api_idempotency_key_requirements_to_dict,
    source_api_idempotency_key_requirements_to_dicts,
    source_api_idempotency_key_requirements_to_markdown,
    summarize_source_api_idempotency_key_requirements,
)


def test_extracts_api_idempotency_key_categories_in_stable_order():
    result = build_source_api_idempotency_key_requirements(
        _source_brief(
            source_payload={
                "idempotency": {
                    "key": "POST APIs must require a client supplied Idempotency-Key header.",
                    "window": "Duplicate request replay must use the same key within 24 hours.",
                    "fingerprint": "Request fingerprint must hash method and path with the payload hash.",
                    "conflict": "A reused key with a mismatched payload must return 409 Conflict.",
                    "persistence": "Persist idempotency records with TTL and cached response storage.",
                    "retry": "Timeout retry semantics must replay the same response for duplicate submissions.",
                    "monitoring": "Monitoring must emit metrics and logs for replay and conflict rates.",
                }
            },
        )
    )

    assert isinstance(result, SourceAPIIdempotencyKeyRequirementsReport)
    assert all(isinstance(record, SourceAPIIdempotencyKeyRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "idempotency_key",
        "replay_window",
        "request_fingerprint",
        "conflict_response",
        "persistence_ttl",
        "retry_semantics",
        "observability",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["idempotency_key"].source_field == "source_payload.idempotency.key"
    assert by_category["idempotency_key"].suggested_owners == ("api_platform", "backend")
    assert by_category["persistence_ttl"].suggested_owners == ("api_platform", "storage")
    assert result.summary["requirement_count"] == 7
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_supports_models_mappings_objects_strings_and_missing_detail_summary():
    implementation_payload = _implementation_brief(
        scope=[
            "API idempotency key support must allow safe retry after network timeouts.",
            "Persist idempotency records with a TTL for duplicate request replay.",
        ],
        definition_of_done=["409 Conflict must be returned when the same idempotency key has a different payload hash."],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = SourceBrief.model_validate(_source_brief(summary="Idempotency-Key header is required for create APIs."))
    object_result = build_source_api_idempotency_key_requirements(
        SimpleNamespace(
            id="object-idempotency",
            summary="Idempotency key must be logged with metrics for duplicate replay monitoring.",
        )
    )
    text_result = build_source_api_idempotency_key_requirements(
        "Idempotency key requirements must define request fingerprint behavior."
    )

    implementation_result = generate_source_api_idempotency_key_requirements(implementation)
    source_result = derive_source_api_idempotency_key_requirements(source)

    assert implementation_payload == original
    assert {"idempotency_key", "retry_semantics", "persistence_ttl", "conflict_response"} <= {
        record.category for record in implementation_result.records
    }
    assert source_result.brief_id == "source-idempotency"
    assert source_result.summary["missing_detail_flags"] == [
        "missing_replay_window",
        "missing_fingerprint",
        "missing_persistence",
    ]
    assert "observability" in [record.category for record in object_result.records]
    assert [record.category for record in text_result.records] == ["idempotency_key", "request_fingerprint"]


def test_serialization_markdown_aliases_and_sourcebrief_equivalence_are_stable():
    source = _source_brief(
        source_id="idem-model",
        summary="Idempotency-Key header must be accepted and retained with a 24 hour replay window.",
        source_payload={"requirements": ["Idempotency-Key header must be accepted and retained with a 24 hour replay window."]},
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_api_idempotency_key_requirements(source)
    model_result = build_source_api_idempotency_key_requirements(model)
    extracted = extract_source_api_idempotency_key_requirements(model)
    payload = source_api_idempotency_key_requirements_to_dict(mapping_result)
    markdown = source_api_idempotency_key_requirements_to_markdown(mapping_result)

    assert source == original
    assert model_result.to_dict() == mapping_result.to_dict()
    assert extracted == mapping_result.requirements
    assert summarize_source_api_idempotency_key_requirements(mapping_result) == mapping_result.summary
    assert source_api_idempotency_key_requirements_to_dicts(mapping_result) == payload["requirements"]
    assert source_api_idempotency_key_requirements_to_dicts(mapping_result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert mapping_result.records == mapping_result.requirements
    assert mapping_result.findings == mapping_result.requirements
    assert mapping_result.to_dicts() == payload["requirements"]
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owners",
        "planning_notes",
        "gap_messages",
    ]
    assert mapping_result.records[0].requirement_category == mapping_result.records[0].category
    assert mapping_result.records[0].concern == mapping_result.records[0].category
    assert mapping_result.records[0].suggested_plan_impacts == mapping_result.records[0].planning_notes
    assert markdown.startswith("# Source API Idempotency Key Requirements Report: idem-model")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |" in markdown


def test_unrelated_negated_blank_and_invalid_inputs_return_empty_reports():
    negated = build_source_api_idempotency_key_requirements(
        _source_brief(summary="API idempotency key support is out of scope and no replay work is required.")
    )
    unrelated = build_source_api_idempotency_key_requirements(
        _source_brief(
            title="Deployment cleanup",
            summary="Idempotent migration scripts should be rerunnable.",
            source_payload={"requirements": ["Search settings copy update is required."]},
        )
    )
    blank = build_source_api_idempotency_key_requirements("")
    invalid = build_source_api_idempotency_key_requirements(42)

    expected_counts = {category: 0 for category in [
        "idempotency_key",
        "replay_window",
        "request_fingerprint",
        "conflict_response",
        "persistence_ttl",
        "retry_semantics",
        "observability",
    ]}
    assert negated.records == ()
    assert unrelated.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary["category_counts"] == expected_counts
    assert unrelated.summary["requirement_count"] == 0
    assert unrelated.summary["status"] == "no_api_idempotency_key_language"
    assert "No source API idempotency key requirements were inferred" in unrelated.to_markdown()


def _source_brief(
    *,
    source_id="source-idempotency",
    title="API idempotency key requirements",
    domain="api",
    summary="General API requirements.",
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


def _implementation_brief(*, brief_id="implementation-idempotency", scope=None, definition_of_done=None):
    return {
        "id": brief_id,
        "source_brief_id": "source-idempotency",
        "title": "API idempotency key implementation",
        "domain": "api",
        "target_user": "developer",
        "buyer": "platform",
        "workflow_context": "API clients need safe retry behavior.",
        "problem_statement": "Duplicate creates need deterministic idempotency handling.",
        "mvp_goal": "Plan idempotency key handling.",
        "product_surface": "api",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run idempotency extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
