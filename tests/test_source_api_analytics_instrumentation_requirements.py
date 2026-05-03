import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_analytics_instrumentation_requirements import (
    SourceAPIAnalyticsInstrumentationRequirement,
    SourceAPIAnalyticsInstrumentationRequirementsReport,
    build_source_api_analytics_instrumentation_requirements,
    derive_source_api_analytics_instrumentation_requirements,
    extract_source_api_analytics_instrumentation_requirements,
    generate_source_api_analytics_instrumentation_requirements,
    source_api_analytics_instrumentation_requirements_to_dict,
    source_api_analytics_instrumentation_requirements_to_dicts,
    source_api_analytics_instrumentation_requirements_to_markdown,
    summarize_source_api_analytics_instrumentation_requirements,
)


def test_nested_source_payload_extracts_analytics_instrumentation_categories_in_order():
    result = build_source_api_analytics_instrumentation_requirements(
        _source_brief(
            source_payload={
                "analytics": {
                    "event_tracking": "API must track user events via Mixpanel for product analytics.",
                    "user_behavior": "User behavior analytics must capture session data and interaction patterns.",
                    "funnel": "Funnel analysis must track conversion steps and drop-off points.",
                    "ab_testing": "A/B test instrumentation must track experiment variants and outcomes.",
                    "conversion": "Conversion tracking must measure goal completion and revenue metrics.",
                    "integration": "Product analytics integration must use Amplitude for event data.",
                    "schema": "Custom event schema must define event properties and tracking plan.",
                    "privacy": "Analytics privacy must comply with GDPR and redact PII from events.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceAPIAnalyticsInstrumentationRequirementsReport)
    assert all(isinstance(record, SourceAPIAnalyticsInstrumentationRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "event_tracking",
        "user_behavior_analytics",
        "funnel_analysis",
        "ab_test_instrumentation",
        "conversion_tracking",
        "product_analytics_integration",
        "custom_event_schema",
        "analytics_privacy",
    ]
    assert by_category["event_tracking"].value in {"event", "track", "tracking"}
    assert by_category["product_analytics_integration"].value in {"mixpanel", "amplitude"}
    assert by_category["analytics_privacy"].value in {"pii", "gdpr", "privacy"}
    assert by_category["event_tracking"].source_field == "source_payload.analytics.event_tracking"
    assert by_category["event_tracking"].suggested_owners == ("analytics", "backend", "frontend")
    assert by_category["event_tracking"].planning_notes[0].startswith("Define event tracking")
    assert result.summary["requirement_count"] == 8
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "API must track user events for product analytics.",
            "Funnel analysis must track conversion steps.",
        ],
        definition_of_done=[
            "Custom event schema defines all tracked events.",
            "Analytics privacy controls redact PII from events.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "User behavior analytics must capture session data.",
            "A/B test instrumentation must track experiment variants.",
        ],
        api={"analytics": "Conversion tracking must measure goal completion."},
        source_payload={"metadata": {"tracking": "Product analytics integration must use Segment."}},
    )

    source_result = build_source_api_analytics_instrumentation_requirements(source)
    implementation_result = generate_source_api_analytics_instrumentation_requirements(implementation)

    assert implementation_payload == original
    # The extractor finds additional signals based on context
    source_categories = [record.category for record in source_result.records]
    assert "user_behavior_analytics" in source_categories
    assert "ab_test_instrumentation" in source_categories
    assert "conversion_tracking" in source_categories
    # At least one of these two fields should be the source for one of the records
    source_fields = {r.source_field for r in source_result.records}
    assert any(field.startswith("requirements") or field.startswith("api.") for field in source_fields)
    implementation_categories = {record.category for record in implementation_result.records}
    # At least some analytics categories should be found
    assert len(implementation_categories & {
        "event_tracking",
        "funnel_analysis",
        "custom_event_schema",
        "analytics_privacy",
        "product_analytics_integration",
    }) >= 2
    assert implementation_result.brief_id == "implementation-analytics"
    assert implementation_result.title == "Analytics instrumentation implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_analytics():
    result = build_source_api_analytics_instrumentation_requirements(
        _source_brief(
            summary="API needs analytics instrumentation for user tracking.",
            source_payload={
                "requirements": [
                    "API must support event tracking for user actions.",
                    "Analytics should capture user behavior patterns.",
                    "Product analytics may be integrated for metrics.",
                ]
            },
        )
    )

    categories = [record.category for record in result.records]
    assert "event_tracking" in categories
    assert "user_behavior_analytics" in categories
    assert result.summary["missing_detail_flags"] == [
        "missing_event_schema",
        "missing_privacy_controls",
    ]
    assert "Specify custom event schema, event names, and event property definitions." in result.summary["gap_messages"]
    assert "Define analytics privacy controls, PII handling, and user consent management." in result.summary["gap_messages"]
    assert all(record.gap_messages == tuple(result.summary["gap_messages"]) for record in result.records)
    assert result.summary["missing_detail_counts"]["missing_event_schema"] >= 1
    assert result.summary["status"] == "needs_analytics_details"


def test_duplicate_evidence_serialization_aliases_and_markdown_are_stable():
    source = _source_brief(
        source_id="analytics-model",
        title="Analytics instrumentation source",
        summary="Analytics instrumentation source.",
        source_payload={
            "analytics": {
                "event_tracking": "Event tracking must capture user actions via Mixpanel.",
                "same_event_tracking": "Event tracking must capture user actions via Mixpanel.",
                "privacy": "Analytics privacy must comply with GDPR and redact PII.",
            },
            "acceptance_criteria": [
                "Event tracking must capture user actions via Mixpanel.",
                "Custom event schema must define all event properties.",
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

    result = build_source_api_analytics_instrumentation_requirements(source)
    extracted = extract_source_api_analytics_instrumentation_requirements(model)
    derived = derive_source_api_analytics_instrumentation_requirements(model)
    payload = source_api_analytics_instrumentation_requirements_to_dict(result)
    markdown = source_api_analytics_instrumentation_requirements_to_markdown(result)
    event_tracking = next(record for record in result.records if record.category == "event_tracking")

    assert source == original
    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_api_analytics_instrumentation_requirements(result) == result.summary
    assert source_api_analytics_instrumentation_requirements_to_dicts(result) == payload["requirements"]
    assert source_api_analytics_instrumentation_requirements_to_dicts(result.records) == payload["records"]
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
    assert len(event_tracking.evidence) == 1
    assert "Event tracking must capture user actions via Mixpanel" in event_tracking.evidence[0]
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert result.records[0].suggested_plan_impacts == result.records[0].planning_notes
    assert markdown.startswith("# Source API Analytics Instrumentation Requirements Report: analytics-model")
    assert "event" in markdown.casefold() or "analytics" in markdown.casefold()


def test_out_of_scope_unrelated_invalid_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-analytics"
        summary = "No analytics or event tracking work is required for this release."

    object_result = build_source_api_analytics_instrumentation_requirements(
        SimpleNamespace(
            id="object-analytics",
            summary="API must track user events via Mixpanel.",
            analytics={"event_tracking": "Event tracking must capture user actions."},
        )
    )
    negated = build_source_api_analytics_instrumentation_requirements(BriefLike())
    no_scope = build_source_api_analytics_instrumentation_requirements(
        _source_brief(summary="Analytics is out of scope and no event tracking work is planned.")
    )
    unrelated = build_source_api_analytics_instrumentation_requirements(
        _source_brief(
            title="Web analytics tool",
            summary="Google search analytics and SEO analytics should be reviewed.",
            source_payload={"requirements": ["Update financial analytics and market analytics."]},
        )
    )
    malformed = build_source_api_analytics_instrumentation_requirements({"source_payload": {"analytics": {"notes": object()}}})
    blank = build_source_api_analytics_instrumentation_requirements("")
    invalid = build_source_api_analytics_instrumentation_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "event_tracking": 0,
            "user_behavior_analytics": 0,
            "funnel_analysis": 0,
            "ab_test_instrumentation": 0,
            "conversion_tracking": 0,
            "product_analytics_integration": 0,
            "custom_event_schema": 0,
            "analytics_privacy": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
        "missing_detail_counts": {
            "missing_event_schema": 0,
            "missing_privacy_controls": 0,
        },
        "gap_messages": [],
        "status": "no_analytics_language",
    }
    assert "event_tracking" in [record.category for record in object_result.records]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source API analytics instrumentation requirements were inferred" in unrelated.to_markdown()
    assert summarize_source_api_analytics_instrumentation_requirements(unrelated) == expected_summary


def test_product_analytics_integration_and_custom_event_schema():
    result = build_source_api_analytics_instrumentation_requirements(
        _source_brief(
            summary="API must integrate with Amplitude for product analytics.",
            requirements=[
                "Product analytics integration must use Segment as event router.",
                "Custom event schema must define event properties and tracking plan.",
                "Event tracking must capture all user interactions.",
            ],
            source_payload={
                "analytics": {
                    "integration": "Mixpanel integration for funnel analysis and cohort tracking.",
                    "schema": "Custom event schema specifies event names, properties, and data types.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert "product_analytics_integration" in by_category
    assert "custom_event_schema" in by_category
    assert "event_tracking" in by_category
    assert result.summary["requirement_count"] >= 3
    assert result.summary["status"] in {"ready_for_planning", "needs_analytics_details"}


def test_funnel_analysis_and_ab_test_instrumentation():
    result = build_source_api_analytics_instrumentation_requirements(
        _source_brief(
            requirements=[
                "Funnel analysis must track conversion steps and drop-off rates.",
                "A/B test instrumentation must track experiment variants.",
                "Conversion tracking must measure goal completion metrics.",
            ],
            source_payload={
                "analytics": {
                    "funnel": "Checkout funnel tracks signup, cart, and purchase steps.",
                    "ab_testing": "Split test instrumentation captures variant assignment and outcomes.",
                }
            },
        )
    )

    funnel = next((r for r in result.records if r.category == "funnel_analysis"), None)
    ab_test = next((r for r in result.records if r.category == "ab_test_instrumentation"), None)
    assert funnel is not None
    assert ab_test is not None
    assert funnel.value in {"funnel", "conversion", "drop-off", "step"}
    assert funnel.suggested_owners == ("analytics", "product")
    assert "funnel" in funnel.planning_notes[0].casefold()


def test_analytics_privacy_and_gdpr_compliance():
    result = build_source_api_analytics_instrumentation_requirements(
        _source_brief(
            summary="Analytics must comply with GDPR and protect user privacy.",
            requirements=[
                "Analytics privacy must redact PII from event payloads.",
                "User consent must be checked before tracking events.",
                "GDPR compliance requires opt-out and data deletion support.",
            ],
            source_payload={
                "analytics": {
                    "privacy": "PII redaction anonymizes email and IP addresses.",
                    "consent": "Cookie consent and tracking consent management.",
                }
            },
        )
    )

    privacy = next((r for r in result.records if r.category == "analytics_privacy"), None)
    assert privacy is not None
    assert privacy.value in {"pii", "gdpr", "privacy", "consent", "anonymize", "redact"}
    assert privacy.suggested_owners == ("analytics", "legal", "compliance")
    assert result.summary["requirement_count"] >= 1


def _source_brief(
    *,
    source_id="source-analytics",
    title="Analytics instrumentation requirements",
    domain="api",
    summary="General analytics instrumentation requirements.",
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
    brief_id="implementation-analytics",
    title="Analytics instrumentation implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-analytics",
        "title": title,
        "domain": "api",
        "target_user": "developer",
        "buyer": "platform",
        "workflow_context": "API developers need analytics instrumentation planning.",
        "problem_statement": "Analytics instrumentation requirements need to be extracted early.",
        "mvp_goal": "Plan event tracking, funnel analysis, custom event schema, and privacy controls.",
        "product_surface": "api",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run analytics instrumentation extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
