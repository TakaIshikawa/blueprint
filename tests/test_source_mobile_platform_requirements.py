import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_mobile_platform_requirements import (
    SourceMobilePlatformRequirement,
    SourceMobilePlatformRequirementsReport,
    build_source_mobile_platform_requirements,
    derive_source_mobile_platform_requirements,
    extract_source_mobile_platform_requirements,
    generate_source_mobile_platform_requirements,
    source_mobile_platform_requirements_to_dict,
    source_mobile_platform_requirements_to_dicts,
    source_mobile_platform_requirements_to_markdown,
    summarize_source_mobile_platform_requirements,
)


def test_nested_metadata_extracts_mobile_platform_requirement_types():
    result = build_source_mobile_platform_requirements(
        _source_brief(
            source_payload={
                "mobile_requirements": {
                    "ios": "Must support iOS 17+ and Face ID login for clinicians.",
                    "android": "Android API 29+ must support fingerprint unlock for clinicians.",
                    "tablet": "Tablet layout is required for iPad check-in.",
                    "app_store": "App Store review and Google Play store listing are launch blockers.",
                    "push": "Push notifications for appointment reminders require device tokens.",
                    "links": "Universal links must deep link to account recovery.",
                    "offline": "Offline mode for document upload should sync when online.",
                    "permissions": "Camera permission and location permission prompts are required.",
                }
            }
        )
    )

    assert isinstance(result, SourceMobilePlatformRequirementsReport)
    assert result.source_id == "sb-mobile"
    assert all(isinstance(record, SourceMobilePlatformRequirement) for record in result.records)

    keys = {(record.platform, record.requirement_type) for record in result.records}
    assert ("ios", "os_version") in keys
    assert ("ios", "biometric") in keys
    assert ("android", "os_version") in keys
    assert ("android", "biometric") in keys
    assert ("tablet", "platform_support") in keys
    assert ("ios", "app_store") in keys
    assert ("android", "app_store") in keys
    assert ("unspecified", "push_notification") in keys
    assert ("unspecified", "deep_link") in keys
    assert ("unspecified", "offline_mode") in keys
    assert ("unspecified", "native_permission") in keys

    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["push_notification"].actor is None
    assert by_type["push_notification"].capability == "appointment reminders require device tokens"
    assert by_type["deep_link"].capability == "account recovery"
    assert by_type["offline_mode"].capability == "document upload should sync"
    assert by_type["native_permission"].capability == "camera permission"
    assert any(
        record.requirement_type == "biometric"
        and any("source_payload.mobile_requirements.ios" in item for item in record.evidence)
        for record in result.records
    )
    assert result.summary["requirement_count"] == len(result.records)
    assert result.summary["platform_counts"]["ios"] >= 2
    assert result.summary["requirement_type_counts"]["native_permission"] == 1


def test_acceptance_criteria_and_implementation_brief_are_supported():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Mobile app must support iOS and Android.",
                "Deep links open checkout for customers.",
                "Push notifications can be sent to drivers.",
                "Native location permission rationale is shown.",
            ],
            definition_of_done=["Minimum Android 12 and iOS 16 are validated."],
        )
    )

    result = extract_source_mobile_platform_requirements(brief)

    assert result.source_id == "impl-mobile"
    assert ("ios", "platform_support") in {
        (record.platform, record.requirement_type) for record in result.records
    }
    assert ("android", "platform_support") in {
        (record.platform, record.requirement_type) for record in result.records
    }
    assert any(
        record.requirement_type == "push_notification" and record.actor == "drivers"
        for record in result.records
    )
    assert any(record.requirement_type == "native_permission" for record in result.records)
    assert result.summary["confidence_counts"]["high"] == len(result.records)


def test_plain_text_and_file_path_hints_extract_without_model_validation():
    text = build_source_mobile_platform_requirements(
        "Mobile client must handle app links and offline access for users."
    )
    paths = build_source_mobile_platform_requirements(
        {
            "id": "paths-mobile",
            "files": [
                "apps/ios/NotificationPermissionView.swift",
                "apps/android/src/main/DeepLinkActivity.kt",
            ],
            "summary": "Implementation must request notification permission.",
        }
    )

    assert [(record.platform, record.requirement_type) for record in text.records] == [
        ("unspecified", "deep_link"),
        ("unspecified", "offline_mode"),
    ]
    assert ("ios", "native_permission") in {
        (record.platform, record.requirement_type) for record in paths.records
    }
    assert ("android", "deep_link") in {
        (record.platform, record.requirement_type) for record in paths.records
    }


def test_no_signal_and_malformed_inputs_return_empty_reports():
    class BriefLike:
        id = "object-mobile"
        summary = "No mobile or native app changes are in scope."

    empty = build_source_mobile_platform_requirements(
        _source_brief(summary="This browser-only admin report has no mobile platform requirements.")
    )
    negated = build_source_mobile_platform_requirements(BriefLike())
    malformed = build_source_mobile_platform_requirements({"source_payload": {"notes": object()}})

    assert empty.source_id == "sb-mobile"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "platform_counts": {
            "ios": 0,
            "android": 0,
            "tablet": 0,
            "cross_platform": 0,
            "unspecified": 0,
        },
        "requirement_type_counts": {
            "platform_support": 0,
            "app_store": 0,
            "push_notification": 0,
            "deep_link": 0,
            "offline_mode": 0,
            "biometric": 0,
            "os_version": 0,
            "native_permission": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "platforms": [],
        "requirement_types": [],
    }
    assert "No mobile platform requirements were found" in empty.to_markdown()
    assert negated.records == ()
    assert malformed.records == ()


def test_sourcebrief_aliases_json_serialization_and_no_mutation():
    source = _source_brief(
        source_id="mobile-model",
        summary="Mobile app must support iOS and Android push notifications.",
        source_payload={
            "acceptance_criteria": [
                "Deep links must open invoice details.",
                "Offline mode is required for field technicians.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_mobile_platform_requirements(source)
    model_result = generate_source_mobile_platform_requirements(model)
    derived = derive_source_mobile_platform_requirements(model)
    payload = source_mobile_platform_requirements_to_dict(model_result)

    assert source == original
    assert payload == source_mobile_platform_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert source_mobile_platform_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_mobile_platform_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_mobile_platform_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "platform",
        "requirement_type",
        "evidence",
        "confidence",
        "actor",
        "capability",
    ]


def test_stable_ordering_summary_and_markdown_escaping():
    result = build_source_mobile_platform_requirements(
        [
            _source_brief(
                source_id="brief-b",
                summary="Android push notifications must support dispatch | alerts.",
            ),
            _source_brief(
                source_id="brief-a",
                summary="iOS deep links must open account | settings. Tablet support is required.",
            ),
        ]
    )
    markdown = source_mobile_platform_requirements_to_markdown(result)

    assert [
        (record.source_brief_id, record.platform, record.requirement_type)
        for record in result.records
    ] == [
        ("brief-a", "ios", "deep_link"),
        ("brief-a", "tablet", "platform_support"),
        ("brief-b", "android", "push_notification"),
    ]
    assert result.source_id is None
    assert result.summary["source_count"] == 2
    assert result.summary["platform_counts"]["ios"] == 1
    assert result.summary["requirement_type_counts"]["deep_link"] == 1
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source Mobile Platform Requirements Report")
    assert (
        "| Source Brief | Platform | Requirement Type | Confidence | Actor | Capability | Evidence |"
        in markdown
    )
    assert "account \\| settings" in markdown
    assert "dispatch \\| alerts" in markdown


def _source_brief(
    *,
    source_id="sb-mobile",
    title="Mobile platform requirements",
    domain="mobile",
    summary="General mobile platform requirements.",
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
    scope=None,
    definition_of_done=None,
):
    return {
        "id": "impl-mobile",
        "source_brief_id": "source-mobile",
        "title": "Mobile checkout",
        "domain": "mobile",
        "target_user": "customers",
        "buyer": None,
        "workflow_context": "Mobile checkout workflow.",
        "problem_statement": "Customers need mobile checkout.",
        "mvp_goal": "Ship the mobile checkout MVP.",
        "product_surface": "mobile app",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run mobile validation.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
