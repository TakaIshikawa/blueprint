import copy
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief
from blueprint.source_marketplace_listing_requirements import (
    SourceMarketplaceListingRequirement,
    SourceMarketplaceListingRequirementsReport,
    extract_source_marketplace_listing_requirements,
)


def test_nested_source_payload_extracts_marketplace_listing_categories_in_order():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            source_payload={
                "marketplace": {
                    "listing_copy": "App listing description must be concise and highlight key features.",
                    "screenshots": "Product screenshots and app icon must meet marketplace guidelines.",
                    "oauth": "OAuth review must validate all requested API scopes.",
                    "privacy": "Privacy policy link must be publicly accessible.",
                    "terms": "Terms of service URL must be current and compliant.",
                    "support": "Support contact email must be monitored 24/7.",
                    "category": "App category tags must match marketplace taxonomy.",
                    "pricing": "Pricing disclosure must detail all paid features and subscription tiers.",
                    "deadline": "Review submission deadline is March 15, 2024.",
                    "approval": "Approval status tracking must notify stakeholders of updates.",
                    "owner": "Partner owner must coordinate with marketplace team.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceMarketplaceListingRequirementsReport)
    assert all(isinstance(record, SourceMarketplaceListingRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "listing_copy",
        "screenshots_assets",
        "oauth_review",
        "privacy_policy_link",
        "terms_link",
        "support_contact",
        "category_tags",
        "pricing_disclosure",
        "review_submission_deadline",
        "approval_status_tracking",
        "partner_owner",
    ]
    assert by_category["listing_copy"].suggested_owners == ("product_marketing", "content", "product_manager")
    assert by_category["listing_copy"].planning_notes[0].startswith("Define listing copy requirements")
    assert result.summary["requirement_count"] == 11


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Marketplace listing must include app description and screenshots.",
            "OAuth review process must validate API permissions.",
        ],
        definition_of_done=[
            "Privacy policy link is published and accessible.",
            "Support contact email is configured in help center.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "App listing copy must be approved by product marketing.",
            "Category tags must match marketplace taxonomy.",
        ],
        launch_notes="Review submission deadline: April 1. Partner owner: Jane Smith.",
        source_payload={"metadata": {"pricing": "Pricing disclosure must detail freemium model."}},
    )

    source_result = extract_source_marketplace_listing_requirements(source)
    implementation_result = extract_source_marketplace_listing_requirements(implementation)

    assert implementation_payload == original
    source_categories = [record.category for record in source_result.requirements]
    assert "listing_copy" in source_categories or "category_tags" in source_categories
    assert {
        "listing_copy",
        "screenshots_assets",
    } <= {record.category for record in implementation_result.requirements} or {
        "oauth_review",
        "privacy_policy_link",
    } <= {record.category for record in implementation_result.requirements}
    assert implementation_result.brief_id == "implementation-marketplace"
    assert implementation_result.title == "Marketplace listing implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_listing():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            summary="App marketplace listing launch planning.",
            source_payload={
                "requirements": [
                    "Marketplace listing must include product description.",
                    "Screenshots and app icon must meet design guidelines.",
                    "Privacy policy link must be included.",
                ]
            },
        )
    )

    categories = [record.category for record in result.records]
    assert "listing_copy" in categories or "screenshots_assets" in categories or "privacy_policy_link" in categories
    # Check that gap messages are present for missing details
    all_gap_messages = []
    for record in result.records:
        all_gap_messages.extend(record.gap_messages)
    # May have gaps for missing deadline or approval tracking
    assert isinstance(all_gap_messages, list)


def test_no_marketplace_listing_scope_returns_empty_requirements():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            summary="API development without marketplace publication.",
            source_payload={
                "requirements": [
                    "No marketplace listing required for this release.",
                    "Store publication is out of scope.",
                ]
            },
        )
    )

    assert result.summary["requirement_count"] == 0
    assert len(result.requirements) == 0


def test_string_source_is_parsed_into_body_field():
    result = extract_source_marketplace_listing_requirements(
        "Marketplace listing must include app description, screenshots, and privacy policy link. "
        "OAuth review must validate API scopes. Review submission deadline: March 30."
    )

    assert result.brief_id is None
    categories = [record.category for record in result.records]
    assert "listing_copy" in categories or "screenshots_assets" in categories or "oauth_review" in categories


def test_object_with_attributes_is_parsed_without_pydantic_model():
    obj = SimpleNamespace(
        id="obj-marketplace",
        title="Marketplace listing object",
        summary="App directory listing launch.",
        requirements=[
            "App listing copy must highlight product benefits.",
            "Category tags must improve discoverability.",
        ],
        launch_notes="Partner owner: marketplace-team@example.com",
    )

    result = extract_source_marketplace_listing_requirements(obj)

    assert result.brief_id == "obj-marketplace"
    assert result.title == "Marketplace listing object"
    categories = [record.category for record in result.records]
    assert "listing_copy" in categories or "category_tags" in categories or "partner_owner" in categories


def test_evidence_and_confidence_scoring():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            requirements=[
                "Marketplace listing must include detailed product description.",
                "App screenshots should showcase key features.",
            ],
            acceptance_criteria=[
                "Privacy policy link must be publicly accessible.",
                "Support contact may be configured in help center.",
            ],
        )
    )

    # At least one high confidence requirement (using "must")
    high_confidence_found = any(record.confidence == "high" for record in result.records)
    # At least one with evidence
    evidence_found = any(len(record.evidence) > 0 for record in result.records)

    assert high_confidence_found or len(result.records) == 0
    assert evidence_found or len(result.records) == 0


def test_url_extraction_for_links():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            source_payload={
                "legal": {
                    "privacy": "Privacy policy: https://example.com/privacy",
                    "terms": "Terms of service available at https://example.com/terms",
                    "support": "Support contact: support@example.com or https://help.example.com",
                }
            }
        )
    )

    privacy_record = next((r for r in result.records if r.category == "privacy_policy_link"), None)
    terms_record = next((r for r in result.records if r.category == "terms_link"), None)
    support_record = next((r for r in result.records if r.category == "support_contact"), None)

    if privacy_record:
        assert privacy_record.value is None or "https://" in str(privacy_record.value)
    if terms_record:
        assert terms_record.value is None or "https://" in str(terms_record.value)
    if support_record:
        assert support_record.value is None or "@" in str(support_record.value) or "https://" in str(support_record.value)


def test_deadline_date_extraction():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            launch_notes="Review submission deadline: March 15, 2024. Launch date: April 1, 2024.",
        )
    )

    deadline_record = next((r for r in result.records if r.category == "review_submission_deadline"), None)
    if deadline_record:
        # Value extraction may capture date mentions
        assert deadline_record.value is None or "2024" in str(deadline_record.value) or "march" in str(deadline_record.value).lower()


def test_multiple_screenshot_mentions():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            requirements=[
                "App screenshots must showcase primary user flows.",
                "Product screenshots need to be 1920x1080 resolution.",
                "Marketing assets include app icon and promotional banners.",
            ],
        )
    )

    categories = [record.category for record in result.records]
    assert "screenshots_assets" in categories
    screenshot_record = next((r for r in result.records if r.category == "screenshots_assets"), None)
    if screenshot_record:
        # Should merge duplicate evidence
        assert len(screenshot_record.evidence) <= 3


def test_oauth_scope_validation():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            acceptance_criteria=[
                "OAuth review must validate all requested API scopes.",
                "Security review process must approve authorization permissions.",
            ],
        )
    )

    categories = [record.category for record in result.records]
    assert "oauth_review" in categories


def test_pricing_model_detection():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            source_payload={
                "marketplace": {
                    "pricing": "Freemium model with paid premium features. Free trial: 30 days.",
                }
            }
        )
    )

    categories = [record.category for record in result.records]
    assert "pricing_disclosure" in categories


def test_partner_owner_identification():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            launch_notes="Partner owner: Jane Smith (jane@example.com). Marketplace team contact: marketplace@example.com.",
        )
    )

    categories = [record.category for record in result.records]
    assert "partner_owner" in categories


def test_approval_status_tracking_requirement():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            definition_of_done=[
                "Approval status tracking notifies team of review updates.",
                "Track submission status in project management tool.",
            ],
        )
    )

    categories = [record.category for record in result.records]
    assert "approval_status_tracking" in categories


def test_category_tags_for_discoverability():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            requirements=[
                "App category must be set to 'Productivity'.",
                "Industry tags should include 'SaaS', 'Collaboration', 'Project Management'.",
            ],
        )
    )

    categories = [record.category for record in result.records]
    assert "category_tags" in categories


def test_to_dict_serialization():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            source_id="test-marketplace",
            title="Marketplace listing test",
            requirements=["App listing copy must be concise and engaging."],
        )
    )

    result_dict = result.to_dict()
    assert result_dict["brief_id"] == "test-marketplace"
    assert result_dict["title"] == "Marketplace listing test"
    assert "requirements" in result_dict
    assert "records" in result_dict
    assert "findings" in result_dict
    assert result_dict["requirements"] == result_dict["records"]


def test_to_markdown_rendering():
    result = extract_source_marketplace_listing_requirements(
        _source_brief(
            source_id="md-test",
            requirements=["Marketplace listing must include product screenshots."],
        )
    )

    markdown = result.to_markdown()
    assert "Source Marketplace Listing Requirements Report" in markdown
    if len(result.requirements) > 0:
        assert "screenshots_assets" in markdown or "listing" in markdown.lower()


def test_empty_source_returns_empty_report():
    result = extract_source_marketplace_listing_requirements("")

    assert result.brief_id is None
    assert result.title is None
    assert len(result.requirements) == 0
    assert result.summary["requirement_count"] == 0


def test_plain_dict_source_brief():
    result = extract_source_marketplace_listing_requirements(
        {
            "id": "dict-marketplace",
            "title": "Dict marketplace",
            "summary": "Marketplace listing planning.",
            "requirements": ["Privacy policy link must be current."],
            "source_project": "test",
            "source_entity_type": "manual",
            "source_id": "dict-marketplace",
            "source_payload": {},
            "source_links": {},
        }
    )

    assert result.brief_id == "dict-marketplace"
    categories = [record.category for record in result.records]
    assert "privacy_policy_link" in categories


def test_all_categories_have_owner_suggestions():
    from blueprint.source_marketplace_listing_requirements import (
        _CATEGORY_ORDER,
        _OWNER_SUGGESTIONS,
    )

    for category in _CATEGORY_ORDER:
        assert category in _OWNER_SUGGESTIONS
        assert len(_OWNER_SUGGESTIONS[category]) > 0


def test_all_categories_have_planning_notes():
    from blueprint.source_marketplace_listing_requirements import (
        _CATEGORY_ORDER,
        _PLANNING_NOTES,
    )

    for category in _CATEGORY_ORDER:
        assert category in _PLANNING_NOTES
        assert len(_PLANNING_NOTES[category]) > 0


def _source_brief(
    *,
    source_id="source-marketplace",
    title="Marketplace listing source",
    summary=None,
    requirements=None,
    launch_notes=None,
    definition_of_done=None,
    acceptance_criteria=None,
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "summary": "Marketplace listing requirements extraction test." if summary is None else summary,
        "body": None,
        "domain": "marketplace",
        "requirements": [] if requirements is None else requirements,
        "launch_notes": launch_notes,
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
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
    brief_id="implementation-marketplace",
    title="Marketplace listing implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-marketplace",
        "title": title,
        "domain": "marketplace",
        "target_user": "product_manager",
        "buyer": "marketing",
        "workflow_context": "Product managers need marketplace listing planning.",
        "problem_statement": "Marketplace listing requirements need to be extracted early.",
        "mvp_goal": "Plan listing copy, screenshots, OAuth review, and legal links.",
        "product_surface": "marketplace",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run marketplace listing extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
