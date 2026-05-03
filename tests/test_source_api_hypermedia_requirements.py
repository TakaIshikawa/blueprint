import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_hypermedia_requirements import (
    SourceAPIHypermediaRequirement,
    SourceAPIHypermediaRequirementsReport,
    build_source_api_hypermedia_requirements,
    derive_source_api_hypermedia_requirements,
    extract_source_api_hypermedia_requirements,
    generate_source_api_hypermedia_requirements,
    source_api_hypermedia_requirements_to_dict,
    source_api_hypermedia_requirements_to_dicts,
    source_api_hypermedia_requirements_to_markdown,
    summarize_source_api_hypermedia_requirements,
)


def test_nested_source_payload_extracts_hypermedia_categories_in_order():
    result = build_source_api_hypermedia_requirements(
        _source_brief(
            source_payload={
                "hypermedia": {
                    "hal": "API responses must include HAL _links with self and related hrefs.",
                    "jsonapi": "API must support JSON:API relationships with type and id.",
                    "relations": "Link relations must include self, next, prev, and related.",
                    "templates": "API must support URI templates per RFC 6570 with variable expansion.",
                    "controls": "Hypermedia controls must expose allowed methods and affordances.",
                    "transitions": "Resource state transitions must be documented via workflow links.",
                    "discovery": "API must be discoverable via self-describing link headers.",
                    "embedded": "API responses may include _embedded resources to reduce round trips.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceAPIHypermediaRequirementsReport)
    assert all(isinstance(record, SourceAPIHypermediaRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "hal_links",
        "jsonapi_relationships",
        "link_relations",
        "uri_templates",
        "hypermedia_controls",
        "state_transitions",
        "link_discovery",
        "embedded_resources",
    ]
    assert by_category["hal_links"].value in {"_links", "_embedded", "hal"}
    assert by_category["jsonapi_relationships"].value in {"relationships", "jsonapi", "json:api"}
    assert by_category["link_relations"].value in {"self", "next", "prev", "related"}
    assert by_category["uri_templates"].value in {"rfc 6570", "uri templates", "templated"}
    assert by_category["link_discovery"].source_field == "source_payload.hypermedia.discovery"
    assert by_category["hal_links"].suggested_owners == ("api_platform", "backend")
    assert by_category["link_relations"].planning_notes[0].startswith("Document link relation types")
    assert result.summary["requirement_count"] == 8
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "API responses must include HAL _links with self, next, and collection hrefs.",
            "JSON:API relationships should include type and id for related resources.",
        ],
        definition_of_done=[
            "URI templates support RFC 6570 variable expansion for query parameters.",
            "Link discovery works via OPTIONS responses and self-describing link headers.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "Hypermedia controls must expose allowed methods and state transitions.",
            "Embedded resources should sideload related entities to reduce API calls.",
        ],
        api={"links": "Link relations must follow IANA standard relation types."},
        source_payload={"metadata": {"state": "State transitions must reflect resource lifecycle."}},
    )

    source_result = build_source_api_hypermedia_requirements(source)
    implementation_result = generate_source_api_hypermedia_requirements(implementation)

    assert implementation_payload == original
    assert [record.category for record in source_result.records] == [
        "link_relations",
        "hypermedia_controls",
        "state_transitions",
        "embedded_resources",
    ]
    assert source_result.records[0].source_field == "api.links"
    assert {
        "hal_links",
        "jsonapi_relationships",
        "uri_templates",
        "link_discovery",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-hypermedia"
    assert implementation_result.title == "Hypermedia implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_hypermedia():
    result = build_source_api_hypermedia_requirements(
        _source_brief(
            summary="API needs hypermedia support for discoverability.",
            source_payload={
                "requirements": [
                    "API responses must include links for navigation.",
                    "Hypermedia controls should expose available actions.",
                    "Embedded resources may be included to reduce round trips.",
                ]
            },
        )
    )

    categories = [record.category for record in result.records]
    assert "hypermedia_controls" in categories
    assert "embedded_resources" in categories
    assert result.summary["missing_detail_flags"] == [
        "missing_link_format",
        "missing_relation_types",
    ]
    assert "Specify link format (HAL, JSON:API, custom) and link structure conventions." in result.summary["gap_messages"]
    assert "Define link relation types (self, next, prev, related) and relation semantics." in result.summary["gap_messages"]
    assert all(record.gap_messages == tuple(result.summary["gap_messages"]) for record in result.records)
    assert result.summary["missing_detail_counts"]["missing_link_format"] >= 1
    assert result.summary["status"] == "needs_hypermedia_details"


def test_duplicate_evidence_serialization_aliases_and_markdown_are_stable():
    source = _source_brief(
        source_id="hypermedia-model",
        title="Hypermedia source",
        summary="Hypermedia source.",
        source_payload={
            "hypermedia": {
                "hal": "HAL _links must include self and related hrefs.",
                "same_hal": "HAL _links must include self and related hrefs.",
                "relations": "Link relations must include self, next, prev for pagination.",
            },
            "acceptance_criteria": [
                "HAL _links must include self and related hrefs.",
                "JSON:API relationships must include type and id.",
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

    result = build_source_api_hypermedia_requirements(source)
    extracted = extract_source_api_hypermedia_requirements(model)
    derived = derive_source_api_hypermedia_requirements(model)
    payload = source_api_hypermedia_requirements_to_dict(result)
    markdown = source_api_hypermedia_requirements_to_markdown(result)
    hal = next(record for record in result.records if record.category == "hal_links")

    assert source == original
    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_api_hypermedia_requirements(result) == result.summary
    assert source_api_hypermedia_requirements_to_dicts(result) == payload["requirements"]
    assert source_api_hypermedia_requirements_to_dicts(result.records) == payload["records"]
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
    assert len(hal.evidence) == 1
    assert "HAL _links must include self and related hrefs" in hal.evidence[0]
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert result.records[0].suggested_plan_impacts == result.records[0].planning_notes
    assert markdown.startswith("# Source API Hypermedia Requirements Report: hypermedia-model")
    assert "_links" in markdown


def test_out_of_scope_unrelated_invalid_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-hypermedia"
        summary = "No hypermedia or HATEOAS work is required for this release."

    object_result = build_source_api_hypermedia_requirements(
        SimpleNamespace(
            id="object-hypermedia",
            summary="API responses must include HAL _links with self hrefs.",
            hypermedia={"hal": "HAL format requires _links and _embedded sections."},
        )
    )
    negated = build_source_api_hypermedia_requirements(BriefLike())
    no_scope = build_source_api_hypermedia_requirements(
        _source_brief(summary="Hypermedia is out of scope and no HATEOAS work is planned.")
    )
    unrelated = build_source_api_hypermedia_requirements(
        _source_brief(
            title="Link copy",
            summary="Hyperlink labels and permalink copy should be updated.",
            source_payload={"requirements": ["Update email link text and unsubscribe link wording."]},
        )
    )
    malformed = build_source_api_hypermedia_requirements({"source_payload": {"hypermedia": {"notes": object()}}})
    blank = build_source_api_hypermedia_requirements("")
    invalid = build_source_api_hypermedia_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "hal_links": 0,
            "jsonapi_relationships": 0,
            "link_relations": 0,
            "uri_templates": 0,
            "hypermedia_controls": 0,
            "state_transitions": 0,
            "link_discovery": 0,
            "embedded_resources": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
        "missing_detail_counts": {
            "missing_link_format": 0,
            "missing_relation_types": 0,
        },
        "gap_messages": [],
        "status": "no_hypermedia_language",
    }
    assert "hal_links" in [record.category for record in object_result.records]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source API hypermedia requirements were inferred" in unrelated.to_markdown()
    assert summarize_source_api_hypermedia_requirements(unrelated) == expected_summary


def test_mixed_link_formats_and_conditional_links():
    result = build_source_api_hypermedia_requirements(
        _source_brief(
            summary="API must support both HAL and JSON:API formats for compatibility.",
            requirements=[
                "HAL _links must include self, next, prev for collections.",
                "JSON:API relationships should include links.self and links.related.",
                "Conditional links may be included based on user permissions.",
            ],
            source_payload={
                "hypermedia": {
                    "formats": "Support both application/hal+json and application/vnd.api+json.",
                    "controls": "Hypermedia controls should reflect available actions per resource state.",
                    "templates": "URI templates with {?page,limit} for pagination.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert "hal_links" in by_category
    assert "jsonapi_relationships" in by_category
    # link_relations may or may not be detected depending on context
    assert "uri_templates" in by_category
    assert "hypermedia_controls" in by_category
    assert result.summary["requirement_count"] >= 4
    assert result.summary["status"] in {"ready_for_planning", "needs_hypermedia_details"}


def test_embedded_resources_and_circular_references():
    result = build_source_api_hypermedia_requirements(
        _source_brief(
            requirements=[
                "API responses may include _embedded resources to reduce round trips.",
                "Embedded resources should support expansion via ?expand query parameter.",
                "Circular references in embedded resources must be handled gracefully.",
            ],
            source_payload={
                "hypermedia": {
                    "embedded": "Sideloaded resources via _embedded reduce client round trips.",
                    "nesting": "Limit embedding depth to 2 levels to prevent circular references.",
                }
            },
        )
    )

    embedded = next((r for r in result.records if r.category == "embedded_resources"), None)
    assert embedded is not None
    assert embedded.value in {"_embedded", "embedded", "sideloaded", "expand", "expanded"}
    assert embedded.suggested_owners == ("api_platform", "backend")
    assert "embed" in embedded.planning_notes[0].casefold()


def _source_brief(
    *,
    source_id="source-hypermedia",
    title="Hypermedia requirements",
    domain="api",
    summary="General hypermedia requirements.",
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
    brief_id="implementation-hypermedia",
    title="Hypermedia implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-hypermedia",
        "title": title,
        "domain": "api",
        "target_user": "developer",
        "buyer": "platform",
        "workflow_context": "API developers need hypermedia planning.",
        "problem_statement": "Hypermedia requirements need to be extracted early.",
        "mvp_goal": "Plan HAL links, JSON:API relationships, URI templates, and link discovery.",
        "product_surface": "api",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run hypermedia extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
