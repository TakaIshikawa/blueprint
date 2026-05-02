import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_mobile_app_store_requirements import (
    SourceMobileAppStoreRequirement,
    SourceMobileAppStoreRequirementsReport,
    build_source_mobile_app_store_requirements,
    derive_source_mobile_app_store_requirements,
    extract_source_mobile_app_store_requirements,
    generate_source_mobile_app_store_requirements,
    source_mobile_app_store_requirements_to_dict,
    source_mobile_app_store_requirements_to_dicts,
    source_mobile_app_store_requirements_to_markdown,
    summarize_source_mobile_app_store_requirements,
)


def test_free_text_extracts_mobile_store_requirements_with_missing_detail_flags():
    result = build_source_mobile_app_store_requirements(
        _source_brief(
            source_payload={
                "body": """
# Mobile store release

- iOS App Store submission must go through App Store Connect before launch.
- Android Play Store release should use the production track.
- Review submission owner: Mobile Release DRI.
- Phased release starts at 10% daily over 5 days with a pause rollout rollback path.
- Minimum OS versions are iOS 16+ and Android 12+.
- App signing must validate provisioning profile and Play App Signing upload key.
- Screenshots and metadata include release notes and localized store listing copy.
- Privacy nutrition labels and Google Play data safety evidence must be approved.
- Rollback path uses feature flag disable, hotfix, and resubmit if store review blocks rollback.
"""
            }
        )
    )

    assert isinstance(result, SourceMobileAppStoreRequirementsReport)
    assert all(isinstance(record, SourceMobileAppStoreRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "ios_app_store",
        "android_play_store",
        "review_submission",
        "phased_release",
        "minimum_os_version",
        "app_signing",
        "screenshots_metadata",
        "privacy_nutrition_labels",
        "rollback_constraint",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["ios_app_store"].platforms == ("ios",)
    assert by_type["android_play_store"].release_track == "production track"
    assert by_type["review_submission"].review_owner == "Mobile Release DRI"
    assert by_type["phased_release"].rollout_timing == "10% daily"
    assert by_type["phased_release"].rollback_path == "pause rollout"
    assert by_type["minimum_os_version"].platforms == ("ios", "android")
    assert "provisioning profile" in by_type["app_signing"].store_compliance_evidence
    assert "data safety" in by_type["privacy_nutrition_labels"].store_compliance_evidence
    assert "missing_store_compliance_evidence" not in by_type["privacy_nutrition_labels"].missing_detail_flags
    assert result.summary["requirement_count"] == 9
    assert result.summary["platform_counts"] == {"ios": 2, "android": 4}
    assert result.summary["status"] == "ready_for_mobile_app_store_planning"


def test_structured_fields_and_implementation_brief_inputs_are_supported():
    structured = build_source_mobile_app_store_requirements(
        {
            "id": "structured-mobile-release",
            "title": "Native mobile release",
            "metadata": {
                "store_release": {
                    "platforms": "iOS and Android",
                    "review_submission": "Store review must be submitted by release ops owner.",
                    "release_track": "Use TestFlight and Play Console closed testing track.",
                    "rollout_timing": "Phased release over 7 days.",
                    "rollback": "Rollback path is pause rollout and ship hotfix.",
                    "compliance": "Privacy nutrition label, data safety, screenshots, and release notes are evidence.",
                }
            },
        }
    )
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Minimum OS requirement must document iOS 17+ and Android 13+ support.",
                "App signing requires the keystore and provisioning profile before store submission.",
            ],
            definition_of_done=[
                "Store metadata includes screenshots and release notes.",
                "Privacy nutrition labels and data safety evidence are attached before review submission.",
            ],
        )
    )
    object_result = build_source_mobile_app_store_requirements(
        SimpleNamespace(
            id="object-mobile-release",
            release_plan="Android Play Store staged rollout must use beta track over 48 hours.",
        )
    )

    assert [record.requirement_type for record in structured.records] == [
        "ios_app_store",
        "android_play_store",
        "review_submission",
        "phased_release",
        "screenshots_metadata",
        "privacy_nutrition_labels",
        "rollback_constraint",
    ]
    by_type = {record.requirement_type: record for record in structured.records}
    assert by_type["review_submission"].source_field == "metadata.store_release.review_submission"
    assert by_type["review_submission"].review_owner == "release ops owner"
    assert by_type["phased_release"].release_track == "closed testing"
    assert by_type["phased_release"].rollout_timing == "over 7 days"
    assert by_type["rollback_constraint"].rollback_path == "pause rollout"
    assert "privacy nutrition label" in by_type["privacy_nutrition_labels"].store_compliance_evidence

    impl_result = generate_source_mobile_app_store_requirements(implementation)
    assert impl_result.source_id == "implementation-mobile-release"
    assert [record.requirement_type for record in impl_result.records] == [
        "ios_app_store",
        "android_play_store",
        "review_submission",
        "minimum_os_version",
        "app_signing",
        "screenshots_metadata",
        "privacy_nutrition_labels",
    ]
    assert impl_result.records[3].source_field == "scope[0]"
    assert object_result.records[0].requirement_type == "android_play_store"
    assert object_result.records[1].release_track == "beta track"


def test_non_mobile_briefs_return_empty_report():
    class BriefLike:
        id = "object-no-mobile"
        summary = "No mobile, iOS, Android, App Store, or Play Store work is required for this release."

    empty = build_source_mobile_app_store_requirements(
        _source_brief(
            title="Web copy release",
            summary="No mobile, iOS, Android, App Store, or Play Store work is required for this release.",
            source_payload={"requirements": ["Update onboarding copy and web invoice labels."]},
        )
    )
    repeat = build_source_mobile_app_store_requirements(
        _source_brief(
            title="Web copy release",
            summary="No mobile, iOS, Android, App Store, or Play Store work is required for this release.",
            source_payload={"requirements": ["Update onboarding copy and web invoice labels."]},
        )
    )
    malformed = build_source_mobile_app_store_requirements({"source_payload": {"notes": object()}})
    blank = build_source_mobile_app_store_requirements("")

    expected_summary = {
        "requirement_count": 0,
        "requirement_types": [],
        "requirement_type_counts": {
            "ios_app_store": 0,
            "android_play_store": 0,
            "review_submission": 0,
            "phased_release": 0,
            "minimum_os_version": 0,
            "app_signing": 0,
            "screenshots_metadata": 0,
            "privacy_nutrition_labels": 0,
            "rollback_constraint": 0,
        },
        "platform_counts": {"ios": 0, "android": 0},
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flag_counts": {
            "missing_platform": 0,
            "missing_review_owner": 0,
            "missing_release_track": 0,
            "missing_rollout_timing": 0,
            "missing_rollback_path": 0,
            "missing_store_compliance_evidence": 0,
        },
        "status": "no_mobile_app_store_language",
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert build_source_mobile_app_store_requirements(BriefLike()).summary == expected_summary
    assert malformed.records == ()
    assert blank.records == ()
    assert "No mobile app store release requirements were found" in empty.to_markdown()


def test_deduped_evidence_stable_order_and_markdown_escaping():
    result = build_source_mobile_app_store_requirements(
        _source_brief(
            source_id="mobile-dedupe",
            source_payload={
                "requirements": [
                    "Screenshots and metadata must include release notes for customer | partner markets.",
                    "Screenshots and metadata must include release notes for customer | partner markets.",
                    "Android Play Store phased release should use beta track over 48 hours.",
                    "iOS App Store phased release should use TestFlight over 48 hours.",
                ]
            },
        )
    )

    assert [record.requirement_type for record in result.records] == [
        "ios_app_store",
        "android_play_store",
        "phased_release",
        "screenshots_metadata",
    ]
    screenshots = result.records[3]
    assert screenshots.evidence == (
        "source_payload.requirements[0]: Screenshots and metadata must include release notes for customer | partner markets.",
    )
    assert result.records[2].platforms == ("android", "ios")
    assert result.records[2].release_track == "beta track"
    markdown = result.to_markdown()
    assert "| Source Brief | Requirement Type | Requirement | Platforms | Owner | Track | Timing | Rollback | Compliance Evidence | Source Field | Confidence | Missing Details | Evidence |" in markdown
    assert "customer \\| partner markets" in markdown


def test_serialization_aliases_json_and_no_input_mutation_are_stable():
    source = _source_brief(
        source_id="mobile-model",
        title="Mobile store release",
        summary="Native app release requirements.",
        source_payload={
            "release": [
                "iOS App Store review submission must be owned by Release Ops.",
                "Android Play Store staged rollout should use production track over 7 days.",
                "Privacy nutrition labels and data safety evidence are required.",
                "Rollback path is pause rollout, disable via feature flag, and submit hotfix.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_mobile_app_store_requirements(source)
    model_result = extract_source_mobile_app_store_requirements(model)
    derived = derive_source_mobile_app_store_requirements(model)
    payload = source_mobile_app_store_requirements_to_dict(model_result)
    markdown = source_mobile_app_store_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_mobile_app_store_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_mobile_app_store_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_mobile_app_store_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_mobile_app_store_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_type",
        "requirement_text",
        "platforms",
        "review_owner",
        "release_track",
        "rollout_timing",
        "rollback_path",
        "store_compliance_evidence",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "missing_detail_flags",
        "suggested_plan_impacts",
    ]
    assert [record.requirement_type for record in model_result.records] == [
        "ios_app_store",
        "android_play_store",
        "review_submission",
        "phased_release",
        "privacy_nutrition_labels",
        "rollback_constraint",
    ]
    assert model_result.records[0].category == "ios_app_store"
    assert model_result.records[0].planning_notes == model_result.records[0].suggested_plan_impacts
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source Mobile App Store Requirements Report: mobile-model")


def _source_brief(
    *,
    source_id="source-mobile-release",
    title="Mobile app store requirements",
    domain="mobile",
    summary="General native mobile app store release requirements.",
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


def _implementation_brief(
    *,
    brief_id="implementation-mobile-release",
    title="Mobile store release implementation",
    problem_statement="Implement source-backed mobile app store planning support.",
    mvp_goal="Ship mobile app store extraction.",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-mobile-release",
        "title": title,
        "domain": "mobile",
        "target_user": "release manager",
        "buyer": "mobile engineering",
        "workflow_context": "Native mobile App Store and Play Store release",
        "problem_statement": problem_statement,
        "mvp_goal": mvp_goal,
        "product_surface": "native mobile app",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run mobile store extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
