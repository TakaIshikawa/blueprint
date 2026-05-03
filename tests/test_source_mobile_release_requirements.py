import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_mobile_release_requirements import (
    SourceMobileReleaseRequirement,
    SourceMobileReleaseRequirementsReport,
    build_source_mobile_release_requirements,
    derive_source_mobile_release_requirements,
    extract_source_mobile_release_requirements,
    generate_source_mobile_release_requirements,
    source_mobile_release_requirements_to_dict,
    source_mobile_release_requirements_to_dicts,
    source_mobile_release_requirements_to_markdown,
    summarize_source_mobile_release_requirements,
)


def test_multi_platform_brief_extracts_mobile_release_categories_in_stable_order():
    result = build_source_mobile_release_requirements(
        _source_brief(
            source_payload={
                "mobile_release": {
                    "ios": "iOS App Store release must use App Store Connect with release owner Mobile DRI.",
                    "android": "Android Play Store release must use Play Console production track.",
                    "signing": "App signing must validate signing certificate, provisioning profile, keystore, upload key, and APNs entitlement.",
                    "review": "Store review submission must include reviewer access and review owner Release Ops.",
                    "rollout": "Phased rollout should start at 10% daily over 5 days on the production track.",
                    "os": "Minimum OS versions are iOS 16+ and Android 12+.",
                    "testers": "TestFlight and Play Console internal testing are required before production release.",
                    "links": "Deep links must support universal links and Android App Links.",
                    "version": "App versioning requires version 4.8.0, iOS build number, and Android version code.",
                    "rollback": "Rollback update plan must pause rollout, ship hotfix, and use expedited review.",
                }
            }
        )
    )

    assert isinstance(result, SourceMobileReleaseRequirementsReport)
    assert all(isinstance(record, SourceMobileReleaseRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "ios_app_store",
        "android_play_store",
        "signing",
        "provisioning_profile",
        "store_review",
        "phased_rollout",
        "minimum_os_version",
        "push_notification_entitlement",
        "test_distribution",
        "deep_links",
        "app_versioning",
        "rollback_update",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["ios_app_store"].platform == "ios"
    assert by_category["android_play_store"].platform == "android"
    assert by_category["minimum_os_version"].platform == "cross_platform"
    assert by_category["signing"].readiness == "ready"
    assert by_category["store_review"].readiness == "ready"
    assert by_category["phased_rollout"].readiness == "ready"
    assert by_category["push_notification_entitlement"].matched_terms == ("apns",)
    assert result.summary["requirement_count"] == 12
    assert result.summary["platform_counts"]["ios"] == 1
    assert result.summary["category_counts"]["app_versioning"] == 1
    assert result.summary["status"] == "ready_for_mobile_release_planning"


def test_missing_signing_and_review_details_mark_readiness_as_needing_details():
    result = build_source_mobile_release_requirements(
        _source_brief(
            source_payload={
                "requirements": [
                    "Mobile app signing must be ready before release.",
                    "Store review submission must be completed before launch.",
                    "Phased rollout must be planned for mobile users.",
                ]
            }
        )
    )

    by_category = {record.category: record for record in result.records}
    assert by_category["signing"].missing_details == ("missing_signing_materials",)
    assert by_category["signing"].readiness == "needs_details"
    assert by_category["store_review"].missing_details == ("missing_review_owner",)
    assert by_category["store_review"].readiness == "needs_details"
    assert by_category["phased_rollout"].missing_details == ("missing_rollout_timing",)
    assert result.summary["readiness_counts"]["needs_details"] == 3
    assert result.summary["missing_detail_counts"]["missing_review_owner"] == 1
    assert result.summary["status"] == "needs_mobile_release_details"


def test_duplicate_evidence_merges_deterministically_and_object_input_is_supported():
    result = build_source_mobile_release_requirements(
        {
            "id": "mobile-dupe",
            "source_payload": {
                "requirements": [
                    "App versioning requires version 4.8.0 and build number 40800.",
                    "App versioning requires version 4.8.0 and build number 40800.",
                    "Rollback update plan must pause rollout and ship hotfix.",
                ],
                "metadata": {
                    "same_version": "App versioning requires version 4.8.0 and build number 40800.",
                    "same_rollback": "Rollback update plan must pause rollout and ship hotfix.",
                },
            },
        }
    )
    object_result = build_source_mobile_release_requirements(
        SimpleNamespace(
            id="object-mobile-release",
            release_plan="Android Play Store staged rollout must use beta track over 48 hours.",
        )
    )

    assert [record.category for record in result.records] == [
        "phased_rollout",
        "app_versioning",
        "rollback_update",
    ]
    assert result.records[1].evidence == (
        "source_payload.metadata.same_version: App versioning requires version 4.8.0 and build number 40800.",
    )
    assert result.records[2].evidence == (
        "source_payload.metadata.same_rollback: Rollback update plan must pause rollout and ship hotfix.",
    )
    assert [record.category for record in object_result.records] == [
        "android_play_store",
        "phased_rollout",
        "test_distribution",
    ]
    assert object_result.records[0].platform == "android"


def test_list_input_source_models_and_implementation_brief_are_supported_without_mutation():
    source = _source_brief(
        source_id="mobile-list-1",
        source_payload={
            "requirements": [
                "iOS minimum OS requirement is iOS 17+ before mobile app release.",
                "TestFlight internal testing must include beta testers.",
            ]
        },
    )
    second = _source_brief(
        source_id="mobile-list-2",
        source_payload={
            "requirements": [
                "Android deep links must support Android App Links.",
                "Push notification entitlement requires FCM device tokens.",
            ]
        },
    )
    original = copy.deepcopy(source)
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Mobile app release must submit App Store review with review owner Release Ops.",
                "App versioning must use semantic version and build number before release.",
            ],
        )
    )

    list_result = derive_source_mobile_release_requirements([SourceBrief.model_validate(source), second])
    implementation_result = generate_source_mobile_release_requirements(implementation)

    assert source == original
    assert list_result.source_id is None
    assert "minimum_os_version" in list_result.summary["categories"]
    assert "push_notification_entitlement" in list_result.summary["categories"]
    assert implementation_result.source_id == "implementation-mobile-release"
    assert [record.category for record in implementation_result.records] == [
        "ios_app_store",
        "store_review",
        "app_versioning",
    ]


def test_empty_invalid_and_unrelated_inputs_return_stable_empty_report():
    empty = build_source_mobile_release_requirements(
        _source_brief(
            title="Web copy release",
            summary="Update onboarding web copy and invoice labels only.",
            source_payload={"requirements": ["No mobile release requirements are needed."]},
        )
    )
    repeat = build_source_mobile_release_requirements(
        _source_brief(
            title="Web copy release",
            summary="Update onboarding web copy and invoice labels only.",
            source_payload={"requirements": ["No mobile release requirements are needed."]},
        )
    )
    invalid = build_source_mobile_release_requirements(42)
    blank = build_source_mobile_release_requirements("")

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "platforms": [],
        "categories": [],
        "platform_counts": {"ios": 0, "android": 0, "cross_platform": 0, "unspecified": 0},
        "category_counts": {
            "ios_app_store": 0,
            "android_play_store": 0,
            "signing": 0,
            "provisioning_profile": 0,
            "store_review": 0,
            "phased_rollout": 0,
            "minimum_os_version": 0,
            "push_notification_entitlement": 0,
            "test_distribution": 0,
            "deep_links": 0,
            "app_versioning": 0,
            "rollback_update": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "readiness_counts": {"ready": 0, "needs_details": 0, "not_ready": 0},
        "missing_detail_counts": {
            "missing_platform": 0,
            "missing_signing_materials": 0,
            "missing_review_owner": 0,
            "missing_release_track": 0,
            "missing_rollout_timing": 0,
            "missing_minimum_os_version": 0,
            "missing_test_distribution_track": 0,
            "missing_versioning_scheme": 0,
            "missing_rollback_path": 0,
        },
        "status": "no_mobile_release_language",
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert invalid.records == ()
    assert blank.records == ()
    assert "No mobile release requirements were found" in empty.to_markdown()


def test_serialization_markdown_summary_helpers_and_aliases_are_stable():
    source = _source_brief(
        source_id="mobile-json",
        title="Mobile release JSON",
        source_payload={
            "requirements": [
                "App versioning must escape partner | customer notes with version 4.8.0.",
                "Rollback update plan must pause rollout and ship hotfix.",
            ]
        },
    )
    model_result = build_source_mobile_release_requirements(SourceBrief.model_validate(source))
    extracted = extract_source_mobile_release_requirements(source)
    payload = source_mobile_release_requirements_to_dict(model_result)
    markdown = source_mobile_release_requirements_to_markdown(model_result)

    assert extracted.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_mobile_release_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_mobile_release_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_mobile_release_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "platform",
        "category",
        "requirement_text",
        "source_field",
        "evidence",
        "matched_terms",
        "missing_details",
        "confidence",
        "readiness",
        "owner_suggestion",
        "planning_note",
    ]
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source Mobile Release Requirements Report: mobile-json")
    assert "| Source Brief | Platform | Category | Requirement | Source Field | Confidence | Readiness | Missing Details | Evidence |" in markdown
    assert "partner \\| customer notes" in markdown
    app_versioning = next(
        record for record in model_result.records if record.category == "app_versioning"
    )
    assert app_versioning.requirement_category == "app_versioning"
    assert app_versioning.missing_detail_flags == app_versioning.missing_details
    assert app_versioning.planning_notes == (app_versioning.planning_note,)


def _source_brief(
    *,
    source_id="source-mobile-release",
    title="Mobile release requirements",
    domain="mobile",
    summary="General source mobile release requirements.",
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
        "title": "Mobile release implementation",
        "domain": "mobile",
        "target_user": "mobile release manager",
        "buyer": "product",
        "workflow_context": "Native mobile release planning.",
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
