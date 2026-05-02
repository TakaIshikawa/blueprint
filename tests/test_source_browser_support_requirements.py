import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_browser_support_requirements import (
    SourceBrowserSupportRequirement,
    SourceBrowserSupportRequirementsReport,
    build_source_browser_support_requirements,
    extract_source_browser_support_requirements,
    generate_source_browser_support_requirements,
    source_browser_support_requirements_to_dict,
    source_browser_support_requirements_to_dicts,
    source_browser_support_requirements_to_markdown,
)


def test_detects_browser_device_and_client_targets_across_brief_fields():
    result = build_source_browser_support_requirements(
        _source_brief(
            summary=(
                "The dashboard must support Chrome, Safari, Firefox, and Edge on desktop browsers."
            ),
            source_payload={
                "requirements": [
                    "Mobile browsers and WebView containers are required for field approvals.",
                    "Responsive tablet layouts must work at 768px and 1024px breakpoints.",
                    "Use progressive enhancement with polyfills for older legacy browsers.",
                ],
                "validation_plan": [
                    "Run cross-browser tests and device viewport checks before launch."
                ],
            },
        )
    )

    by_target = {record.client_target: record for record in result.records}

    assert isinstance(result, SourceBrowserSupportRequirementsReport)
    assert all(isinstance(record, SourceBrowserSupportRequirement) for record in result.records)
    assert list(by_target) == [
        "chrome",
        "safari",
        "firefox",
        "edge",
        "mobile_browser",
        "webview",
        "legacy_browser",
        "responsive",
        "tablet",
        "desktop",
        "screen_size",
        "progressive_enhancement",
        "polyfill",
    ]
    assert by_target["chrome"].source_brief_id == "source-browser"
    assert by_target["chrome"].requirement_level == "required"
    assert "chrome" in by_target["chrome"].matched_terms
    assert by_target["webview"].requirement_level == "required"
    assert "webview" in by_target["webview"].matched_terms
    assert "cross-browser tests" in by_target["firefox"].recommended_validation[0]
    assert "device viewport checks" in by_target["screen_size"].recommended_validation[0]
    assert "polyfills" in by_target["polyfill"].recommended_validation[0]
    assert result.summary["requirement_count"] == 13
    assert result.summary["client_target_counts"]["responsive"] == 1


def test_requirement_levels_distinguish_required_recommended_and_mentions():
    result = build_source_browser_support_requirements(
        {
            "id": "levels",
            "summary": "Safari is mentioned in stakeholder notes.",
            "requirements": ["Chrome support is required for launch."],
            "source_payload": {
                "compatibility_notes": [
                    "Firefox should work where possible.",
                    "Edge appears in analytics but is not a committed target.",
                ]
            },
        }
    )

    by_target = {record.client_target: record for record in result.records}

    assert by_target["chrome"].requirement_level == "required"
    assert by_target["firefox"].requirement_level == "recommended"
    assert by_target["safari"].requirement_level == "mentioned"
    assert by_target["edge"].requirement_level == "mentioned"
    assert result.summary["requirement_level_counts"] == {
        "required": 1,
        "recommended": 1,
        "mentioned": 2,
    }


def test_duplicate_terms_are_merged_with_deduplicated_evidence():
    result = build_source_browser_support_requirements(
        {
            "id": "dupe-browser",
            "summary": "Responsive layout must support mobile browsers. Responsive layout must support mobile browsers.",
            "requirements": [
                "Responsive layout must support mobile browsers.",
                "responsive layout must support mobile browsers.",
            ],
            "metadata": {"responsive": "Responsive layout must support mobile browsers."},
        }
    )

    responsive = next(
        record for record in result.requirements if record.client_target == "responsive"
    )

    assert responsive.evidence == tuple(
        sorted(set(responsive.evidence), key=lambda item: item.casefold())
    )
    assert len(responsive.evidence) == len(set(responsive.evidence))
    assert responsive.matched_terms == ("responsive",)
    assert responsive.requirement_level == "required"


def test_mapping_and_sourcebrief_inputs_match_and_serialize_to_json_compatible_payload():
    source = _source_brief(
        source_id="browser-model",
        summary="Mobile web should support Safari and Chrome across responsive breakpoints.",
        source_payload={
            "requirements": ["WebView support is required for embedded approvals."],
            "metadata": {"polyfills": "Polyfills are needed for older browsers."},
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_browser_support_requirements(source)
    model_result = generate_source_browser_support_requirements(model)
    extracted = extract_source_browser_support_requirements(model)
    payload = source_browser_support_requirements_to_dict(model_result)
    markdown = source_browser_support_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_browser_support_requirements_to_dict(mapping_result)
    assert extracted == model_result.requirements
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_browser_support_requirements_to_dicts(model_result.records) == payload["records"]
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "client_target",
        "requirement_level",
        "matched_terms",
        "evidence",
        "recommended_validation",
    ]
    assert markdown.startswith("# Source Browser Support Requirements Report: browser-model")
    assert (
        "| Source Brief | Client Target | Level | Matched Terms | Evidence | Recommended Validation |"
        in markdown
    )


def test_multiple_briefs_are_handled_with_stable_ordering():
    result = build_source_browser_support_requirements(
        [
            _source_brief(
                source_id="brief-b",
                summary="WebView support is required, with Chrome for Android checks.",
            ),
            _source_brief(
                source_id="brief-a",
                summary="Responsive desktop and tablet layouts should handle 1440px screens.",
            ),
        ]
    )

    assert [(record.source_brief_id, record.client_target) for record in result.records] == [
        ("brief-a", "responsive"),
        ("brief-a", "tablet"),
        ("brief-a", "desktop"),
        ("brief-a", "screen_size"),
        ("brief-b", "chrome"),
        ("brief-b", "mobile_browser"),
        ("brief-b", "webview"),
    ]
    assert result.source_id is None
    assert result.summary["source_count"] == 2


def test_no_signal_empty_and_invalid_inputs_return_no_records():
    empty = build_source_browser_support_requirements(
        {"id": "empty", "summary": "Update onboarding copy only."}
    )
    invalid = build_source_browser_support_requirements(object())

    assert empty.source_id == "empty"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary["requirement_count"] == 0
    assert "No browser, device, or client support requirements were found" in empty.to_markdown()
    assert invalid.source_id is None
    assert invalid.requirements == ()


def _source_brief(
    *,
    source_id="source-browser",
    title="Browser support requirements",
    domain="frontend",
    summary="General browser support requirements.",
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
