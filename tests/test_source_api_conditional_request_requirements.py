import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_conditional_request_requirements import (
    SourceAPIConditionalRequestRequirement,
    SourceAPIConditionalRequestRequirementsReport,
    build_source_api_conditional_request_requirements,
    derive_source_api_conditional_request_requirements,
    extract_source_api_conditional_request_requirements,
    generate_source_api_conditional_request_requirements,
    source_api_conditional_request_requirements_to_dict,
    source_api_conditional_request_requirements_to_dicts,
    source_api_conditional_request_requirements_to_markdown,
    summarize_source_api_conditional_request_requirements,
)


def test_nested_source_payload_extracts_conditional_request_categories_in_order():
    result = build_source_api_conditional_request_requirements(
        _source_brief(
            source_payload={
                "conditional": {
                    "if_match": "API must validate If-Match header for conditional PUT and PATCH operations.",
                    "if_unmodified": "If-Unmodified-Since validation must check timestamp preconditions.",
                    "precondition_failed": "API must return 412 Precondition Failed when If-Match fails.",
                    "mutations": "Conditional PUT and DELETE must require If-Match header for safe updates.",
                    "optimistic": "Optimistic locking must use ETag-based version checking for concurrent updates.",
                    "lost_update": "Lost update prevention must detect concurrent modifications via If-Match.",
                    "if_range": "If-Range requests must validate precondition before returning partial content.",
                    "idempotency": "Conditional operations must be idempotent for safe client retry.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceAPIConditionalRequestRequirementsReport)
    assert all(isinstance(record, SourceAPIConditionalRequestRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "if_match_precondition",
        "if_unmodified_since_validation",
        "precondition_failed_responses",
        "conditional_mutations",
        "optimistic_locking",
        "lost_update_prevention",
        "if_range_requests",
        "conditional_idempotency",
    ]
    assert by_category["if_match_precondition"].value in {"if-match", "precondition"}
    assert by_category["precondition_failed_responses"].value in {"412", "precondition failed", "failed"}
    assert by_category["optimistic_locking"].value in {"optimistic lock", "optimistic"}
    assert by_category["conditional_idempotency"].value in {"idempotent", "idempotency"}
    assert by_category["if_match_precondition"].source_field == "source_payload.conditional.if_match"
    assert by_category["if_match_precondition"].suggested_owners == ("api_platform", "backend")
    assert by_category["if_match_precondition"].planning_notes[0].startswith("Define If-Match precondition")
    assert result.summary["requirement_count"] == 8
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "API must validate If-Match header for conditional PUT operations.",
            "412 Precondition Failed must be returned when If-Match validation fails.",
        ],
        definition_of_done=[
            "Optimistic locking prevents lost updates via ETag comparison.",
            "Conditional idempotency enables safe client retry for update operations.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "If-Unmodified-Since validation must check timestamp preconditions.",
            "Lost update prevention must detect concurrent modifications.",
        ],
        api={"conditional": "Conditional mutations must require If-Match for safe updates."},
        source_payload={"metadata": {"precondition": "If-Range requests must validate before partial content."}},
    )

    source_result = build_source_api_conditional_request_requirements(source)
    implementation_result = generate_source_api_conditional_request_requirements(implementation)

    assert implementation_payload == original
    # The extractor finds additional signals based on context
    source_categories = [record.category for record in source_result.records]
    assert "if_unmodified_since_validation" in source_categories
    assert "lost_update_prevention" in source_categories
    assert "if_range_requests" in source_categories
    # At least one of these two fields should be the source for one of the records
    source_fields = {r.source_field for r in source_result.records}
    assert any(field.startswith("requirements") or field.startswith("api.") for field in source_fields)
    assert {
        "if_match_precondition",
        "precondition_failed_responses",
        "optimistic_locking",
        "conditional_idempotency",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-conditional"
    assert implementation_result.title == "Conditional request implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_conditional_requests():
    result = build_source_api_conditional_request_requirements(
        _source_brief(
            summary="API needs conditional request support for safe updates.",
            source_payload={
                "requirements": [
                    "API must support conditional updates for concurrent modification detection.",
                    "Precondition checking should prevent lost updates.",
                    "Optimistic locking may be used for version control.",
                ]
            },
        )
    )

    categories = [record.category for record in result.records]
    assert "optimistic_locking" in categories
    assert "lost_update_prevention" in categories
    assert result.summary["missing_detail_flags"] == [
        "missing_precondition_logic",
        "missing_conflict_handling",
    ]
    assert "Specify precondition validation logic (If-Match, If-Unmodified-Since) and comparison semantics." in result.summary["gap_messages"]
    assert "Define conflict handling strategy and 412 Precondition Failed response behavior." in result.summary["gap_messages"]
    assert all(record.gap_messages == tuple(result.summary["gap_messages"]) for record in result.records)
    assert result.summary["missing_detail_counts"]["missing_precondition_logic"] >= 1
    assert result.summary["status"] == "needs_conditional_request_details"


def test_duplicate_evidence_serialization_aliases_and_markdown_are_stable():
    source = _source_brief(
        source_id="conditional-model",
        title="Conditional request source",
        summary="Conditional request source.",
        source_payload={
            "conditional": {
                "if_match": "If-Match precondition must validate ETag before updates.",
                "same_if_match": "If-Match precondition must validate ETag before updates.",
                "precondition": "412 Precondition Failed must be returned on validation failure.",
            },
            "acceptance_criteria": [
                "If-Match precondition must validate ETag before updates.",
                "Optimistic locking must prevent concurrent update conflicts.",
            ],
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(
        {
            key: value
            for key, value in source.items()
            if key not in {"requirements", "api"}
        }
    )

    result = build_source_api_conditional_request_requirements(source)
    extracted = extract_source_api_conditional_request_requirements(model)
    derived = derive_source_api_conditional_request_requirements(model)
    payload = source_api_conditional_request_requirements_to_dict(result)
    markdown = source_api_conditional_request_requirements_to_markdown(result)
    if_match = next(record for record in result.records if record.category == "if_match_precondition")

    assert source == original
    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_api_conditional_request_requirements(result) == result.summary
    assert source_api_conditional_request_requirements_to_dicts(result) == payload["requirements"]
    assert source_api_conditional_request_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert result.to_dicts() == payload["requirements"]
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
    # Evidence should be deduplicated and sorted
    assert len(if_match.evidence) == 1
    assert "If-Match precondition must validate ETag before updates" in if_match.evidence[0]
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert result.records[0].suggested_plan_impacts == result.records[0].planning_notes
    assert markdown.startswith("# Source API Conditional Request Requirements Report: conditional-model")
    assert "if-match" in markdown.casefold() or "conditional" in markdown.casefold()


def test_out_of_scope_unrelated_invalid_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-conditional"
        summary = "No conditional request or precondition work is required for this release."

    object_result = build_source_api_conditional_request_requirements(
        SimpleNamespace(
            id="object-conditional",
            summary="API must validate If-Match header for conditional updates.",
            conditional={"if_match": "If-Match precondition requires ETag validation."},
        )
    )
    negated = build_source_api_conditional_request_requirements(BriefLike())
    no_scope = build_source_api_conditional_request_requirements(
        _source_brief(summary="Conditional requests are out of scope and no precondition work is planned.")
    )
    unrelated = build_source_api_conditional_request_requirements(
        _source_brief(
            title="Business conditions",
            summary="Terms and conditions and weather conditions should be reviewed.",
            source_payload={"requirements": ["Update filter conditions and SQL WHERE conditions."]},
        )
    )
    malformed = build_source_api_conditional_request_requirements({"source_payload": {"conditional": {"notes": object()}}})
    blank = build_source_api_conditional_request_requirements("")
    invalid = build_source_api_conditional_request_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "if_match_precondition": 0,
            "if_unmodified_since_validation": 0,
            "precondition_failed_responses": 0,
            "conditional_mutations": 0,
            "optimistic_locking": 0,
            "lost_update_prevention": 0,
            "if_range_requests": 0,
            "conditional_idempotency": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
        "missing_detail_counts": {
            "missing_precondition_logic": 0,
            "missing_conflict_handling": 0,
        },
        "gap_messages": [],
        "status": "no_conditional_request_language",
    }
    assert "if_match_precondition" in [record.category for record in object_result.records]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source API conditional request requirements were inferred" in unrelated.to_markdown()
    assert summarize_source_api_conditional_request_requirements(unrelated) == expected_summary


def test_optimistic_locking_and_lost_update_prevention():
    result = build_source_api_conditional_request_requirements(
        _source_brief(
            summary="API must prevent lost updates via optimistic locking.",
            requirements=[
                "Optimistic locking must use If-Match with ETag for version control.",
                "Lost update prevention must detect concurrent modifications.",
                "412 Precondition Failed must be returned on version conflict.",
            ],
            source_payload={
                "conditional": {
                    "optimistic": "Optimistic concurrency control via ETag-based If-Match validation.",
                    "idempotency": "Conditional operations must be idempotent for safe client retry.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert "if_match_precondition" in by_category
    assert "precondition_failed_responses" in by_category
    assert "optimistic_locking" in by_category
    assert "lost_update_prevention" in by_category
    assert "conditional_idempotency" in by_category
    assert result.summary["requirement_count"] >= 5
    assert result.summary["status"] in {"ready_for_planning", "needs_conditional_request_details"}


def test_conditional_mutations_and_if_range_requests():
    result = build_source_api_conditional_request_requirements(
        _source_brief(
            requirements=[
                "Conditional PUT and PATCH must require If-Match header for safe updates.",
                "Conditional DELETE operations must validate If-Match before removal.",
                "If-Range requests must validate precondition before returning partial content.",
            ],
            source_payload={
                "conditional": {
                    "mutations": "Conditional mutations prevent unintended overwrites via precondition checks.",
                    "if_range": "If-Range partial requests combine range and precondition validation.",
                }
            },
        )
    )

    mutations = next((r for r in result.records if r.category == "conditional_mutations"), None)
    if_range = next((r for r in result.records if r.category == "if_range_requests"), None)
    assert mutations is not None
    assert if_range is not None
    assert mutations.value in {"conditional put", "conditional patch", "conditional delete", "conditional"}
    assert mutations.suggested_owners == ("api_platform", "backend")
    assert "conditional" in mutations.planning_notes[0].casefold()


def _source_brief(
    *,
    source_id="source-conditional",
    title="Conditional request requirements",
    domain="api",
    summary="General conditional request requirements.",
    requirements=None,
    api=None,
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "requirements": [] if requirements is None else requirements,
        "api": {} if api is None else api,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(
    *,
    brief_id="implementation-conditional",
    title="Conditional request implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-conditional",
        "title": title,
        "domain": "api",
        "target_user": "developer",
        "buyer": "platform",
        "workflow_context": "API developers need conditional request planning.",
        "problem_statement": "Conditional request requirements need to be extracted early.",
        "mvp_goal": "Plan If-Match, If-Unmodified-Since, optimistic locking, and lost update prevention.",
        "product_surface": "api",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run conditional request extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
