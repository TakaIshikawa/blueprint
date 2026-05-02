import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_mobile_app_release_requirements import (
    SourceMobileAppReleaseRequirement,
    SourceMobileAppReleaseRequirementsReport,
    build_source_mobile_app_release_requirements,
    derive_source_mobile_app_release_requirements,
    extract_source_mobile_app_release_requirements,
    generate_source_mobile_app_release_requirements,
    source_mobile_app_release_requirements_to_dict,
    source_mobile_app_release_requirements_to_dicts,
    source_mobile_app_release_requirements_to_markdown,
    summarize_source_mobile_app_release_requirements,
)


def test_structured_fields_group_mobile_release_categories_with_stable_ordering():
    result = build_source_mobile_app_release_requirements(
        _source_brief(
            source_payload={
                "mobile_release": {
                    "store_review": "App Store Connect and Google Play review submission must include reviewer access and store metadata.",
                    "versioning": "iOS build number and Android version code must be bumped for release version 4.8.0.",
                    "rollout": "Use a phased rollout starting at 10% and pause rollout if metrics regress.",
                    "devices": "Support iOS 16+, Android 10+, phones, tablets, and target SDK 35.",
                    "stability": "Crash-free threshold must stay above 99.5% in Crashlytics before widening rollout.",
                    "privacy": "Privacy manifest and Google Play Data Safety disclosures are required for the SDK update.",
                    "notes": "Release notes and What's New copy must be published in store listings.",
                    "rollback": "Hotfix rollback plan must include expedited review and kill switch instructions.",
                }
            }
        )
    )

    assert isinstance(result, SourceMobileAppReleaseRequirementsReport)
    assert all(isinstance(record, SourceMobileAppReleaseRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "app_store_review",
        "versioning_build_numbers",
        "phased_release",
        "device_os_support",
        "crash_free_threshold",
        "privacy_manifest",
        "release_notes",
        "hotfix_rollback",
    ]
    by_category = {record.category: record for record in result.records}
    assert (
        by_category["app_store_review"].source_field == "source_payload.mobile_release.store_review"
    )
    assert by_category["app_store_review"].matched_terms == (
        "app store connect",
        "google play review",
        "reviewer access",
        "store metadata",
    )
    assert by_category["crash_free_threshold"].suggested_owner == "Quality engineering"
    assert by_category["privacy_manifest"].planning_note.startswith("Plan privacy manifests")
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"]["device_os_support"] == 1
    assert result.summary["status"] == "ready_for_mobile_release_planning"


def test_markdown_bullets_extract_release_requirements_and_merge_duplicate_evidence():
    result = build_source_mobile_app_release_requirements(
        _source_brief(
            source_id="mobile-bullets",
            source_payload={
                "body": """
# Mobile launch checklist

- App Store review submission must include demo credentials.
- App Store review submission must include demo credentials.
- Android version code and iOS build number must be incremented.
- Staged rollout should start at 5% for mobile users.
- Release notes should mention the account setup fix.
"""
            },
        )
    )

    assert [record.category for record in result.records] == [
        "app_store_review",
        "versioning_build_numbers",
        "phased_release",
        "release_notes",
    ]
    app_review = result.records[0]
    assert app_review.evidence == (
        "source_payload.body: App Store review submission must include demo credentials.",
    )
    assert "app store review" in app_review.matched_terms
    assert result.records[2].confidence == "high"
    assert result.records[2].matched_terms == ("staged rollout",)


def test_negated_out_of_scope_text_prevents_mobile_release_extraction_and_empty_summary_is_stable():
    empty = build_source_mobile_app_release_requirements(
        _source_brief(
            title="Backend launch copy",
            summary="Mobile release work is out of scope and no iOS, Android, App Store, or Play Store changes are planned for this release.",
            source_payload={"requirements": ["Web release notes are handled separately."]},
        )
    )
    repeat = build_source_mobile_app_release_requirements(
        _source_brief(
            title="Backend launch copy",
            summary="Mobile release work is out of scope and no iOS, Android, App Store, or Play Store changes are planned for this release.",
            source_payload={"requirements": ["Web release notes are handled separately."]},
        )
    )
    object_empty = build_source_mobile_app_release_requirements(
        SimpleNamespace(id="object-empty", summary="No mobile app release work is required.")
    )
    malformed = build_source_mobile_app_release_requirements(
        {"source_payload": {"notes": object()}}
    )

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "app_store_review": 0,
            "versioning_build_numbers": 0,
            "phased_release": 0,
            "device_os_support": 0,
            "crash_free_threshold": 0,
            "privacy_manifest": 0,
            "release_notes": 0,
            "hotfix_rollback": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_mobile_app_release_language",
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert object_empty.records == ()
    assert malformed.records == ()
    assert "No source mobile app release requirements were inferred" in empty.to_markdown()


def test_confidence_ordering_prefers_structured_requirement_fields_for_source_field():
    result = build_source_mobile_app_release_requirements(
        {
            "id": "confidence-mobile",
            "summary": "Mobile rollout mentions crash-free stability for the launch.",
            "source_payload": {
                "notes": "Mobile app crash-free metrics are being watched.",
                "acceptance_criteria": [
                    "Mobile release must gate rollout on 99.7% crash-free sessions in Crashlytics.",
                ],
            },
        }
    )

    assert [record.category for record in result.records] == ["crash_free_threshold"]
    record = result.records[0]
    assert record.confidence == "high"
    assert record.source_field == "source_payload.acceptance_criteria[0]"
    assert record.evidence == (
        "source_payload.acceptance_criteria[0]: Mobile release must gate rollout on 99.7% crash-free sessions in Crashlytics.",
        "source_payload.notes: Mobile app crash-free metrics are being watched.",
        "summary: Mobile rollout mentions crash-free stability for the launch.",
    )


def test_serialization_aliases_json_markdown_and_no_input_mutation_are_stable():
    source = _source_brief(
        source_id="mobile-model",
        title="Mobile release",
        source_payload={
            "requirements": [
                "Privacy manifest must update Required Reason API disclosures for mobile app launch.",
                "Release notes must escape support | billing copy in What's New.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    result = build_source_mobile_app_release_requirements(model)
    mapping_result = generate_source_mobile_app_release_requirements(source)
    derived = derive_source_mobile_app_release_requirements(model)
    extracted = extract_source_mobile_app_release_requirements(model)
    payload = source_mobile_app_release_requirements_to_dict(result)
    markdown = source_mobile_app_release_requirements_to_markdown(result)
    object_result = build_source_mobile_app_release_requirements(
        SimpleNamespace(
            id="object-mobile",
            metadata={"hotfix": "Mobile hotfix rollback must use expedited review."},
        )
    )

    assert source == original
    assert mapping_result.to_dict() == result.to_dict()
    assert derived.to_dict() == result.to_dict()
    assert extracted == result.requirements
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert source_mobile_app_release_requirements_to_dicts(result) == payload["requirements"]
    assert source_mobile_app_release_requirements_to_dicts(result.records) == payload["records"]
    assert summarize_source_mobile_app_release_requirements(result) == result.summary
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "suggested_owner",
        "planning_note",
    ]
    assert [record.category for record in result.records] == ["privacy_manifest", "release_notes"]
    assert markdown == result.to_markdown()
    assert (
        "| Category | Confidence | Source Field | Matched Terms | Suggested Owner | Planning Note | Evidence |"
        in markdown
    )
    assert "support \\| billing copy" in markdown
    assert object_result.records[0].category == "hotfix_rollback"


def test_implementation_brief_domain_model_inputs_are_supported():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Mobile app release must submit App Store review notes and Google Play review assets.",
                "Device OS support requires iOS 17+, Android 11+, and target SDK 35.",
            ],
            definition_of_done=[
                "Release notes are published before the phased rollout reaches 50%.",
                "Hotfix rollback plan includes previous build recovery.",
            ],
        )
    )

    result = build_source_mobile_app_release_requirements(implementation)

    assert result.brief_id == "implementation-mobile-release"
    assert result.title == "Mobile app release implementation"
    assert [record.category for record in result.records] == [
        "app_store_review",
        "phased_release",
        "device_os_support",
        "release_notes",
        "hotfix_rollback",
    ]
    assert result.records[0].source_field == "scope[0]"
    assert result.records[2].matched_terms == ("os support", "ios 17", "android 11", "target sdk")


def _source_brief(
    *,
    source_id="source-mobile-release",
    title="Mobile app release requirements",
    domain="mobile",
    summary="General mobile app release requirements.",
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


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "implementation-mobile-release",
        "source_brief_id": "source-mobile-release",
        "title": "Mobile app release implementation",
        "domain": "mobile",
        "target_user": "mobile release manager",
        "buyer": "product",
        "workflow_context": "Mobile launch planning",
        "problem_statement": "Mobile app launch constraints need source-backed planning.",
        "mvp_goal": "Ship mobile release requirement extraction.",
        "product_surface": "mobile",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run source mobile release extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
