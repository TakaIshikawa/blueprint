import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_localization_requirements import (
    SourceAPILocalizationRequirement,
    SourceAPILocalizationRequirementsReport,
    build_source_api_localization_requirements,
    derive_source_api_localization_requirements,
    extract_source_api_localization_requirements,
    generate_source_api_localization_requirements,
    source_api_localization_requirements_to_dict,
    source_api_localization_requirements_to_dicts,
    source_api_localization_requirements_to_markdown,
    summarize_source_api_localization_requirements,
)


def test_nested_source_payload_extracts_localization_categories_in_order():
    result = build_source_api_localization_requirements(
        _source_brief(
            source_payload={
                "localization": {
                    "multi_language": "API must support multiple languages including English, French, Spanish, and Japanese.",
                    "locale_content": "Locale-based content delivery must serve region-specific variants.",
                    "accept_language": "Accept-Language header must be parsed for locale negotiation.",
                    "translation": "Translation management must use translation files with key-based lookup.",
                    "currency": "Currency and number formatting must follow locale-specific rules.",
                    "datetime": "Date and time formats must be localized based on user locale and timezone.",
                    "rtl": "RTL language support must handle Arabic and Hebrew bidirectional text.",
                    "fallback": "Locale fallback strategy must default to English when translation is missing.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceAPILocalizationRequirementsReport)
    assert all(isinstance(record, SourceAPILocalizationRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "multi_language_support",
        "locale_based_content",
        "accept_language_header",
        "translation_management",
        "currency_number_formatting",
        "datetime_localization",
        "rtl_language_support",
        "locale_fallback_strategy",
    ]
    assert by_category["multi_language_support"].value in {"multi-language", "multilingual", "multiple languages", "language support"}
    assert by_category["accept_language_header"].value in {"accept-language", "language header", "negotiation"}
    assert by_category["rtl_language_support"].value in {"rtl", "right-to-left", "bidirectional", "bidi"}
    assert by_category["locale_fallback_strategy"].value in {"fallback", "fallback locale", "default locale"}
    assert by_category["multi_language_support"].source_field == "source_payload.localization.multi_language"
    assert by_category["multi_language_support"].suggested_owners == ("api_platform", "backend", "frontend")
    assert by_category["multi_language_support"].planning_notes[0].startswith("Define supported languages")
    assert result.summary["requirement_count"] == 8
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "API must support multi-language content with Accept-Language header parsing.",
            "Currency and number formatting must be locale-specific.",
        ],
        definition_of_done=[
            "RTL language support for Arabic and Hebrew is implemented.",
            "Locale fallback strategy defaults to English for missing translations.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "Translation management must use key-based translation files.",
            "Date and time localization must respect user timezone preferences.",
        ],
        api={"localization": "Locale-based content delivery must serve region-specific variants."},
        source_payload={"metadata": {"i18n": "Multi-language support must include French, German, and Spanish."}},
    )

    source_result = build_source_api_localization_requirements(source)
    implementation_result = generate_source_api_localization_requirements(implementation)

    assert implementation_payload == original
    # The extractor finds additional signals based on context
    source_categories = [record.category for record in source_result.records]
    assert "translation_management" in source_categories
    assert "datetime_localization" in source_categories
    # At least one of these two fields should be the source for one of the records
    source_fields = {r.source_field for r in source_result.records}
    assert any(field.startswith("requirements") or field.startswith("api.") for field in source_fields)
    assert {
        "multi_language_support",
        "accept_language_header",
        "currency_number_formatting",
        "rtl_language_support",
        "locale_fallback_strategy",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-localization"
    assert implementation_result.title == "Localization implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_localization():
    result = build_source_api_localization_requirements(
        _source_brief(
            summary="API needs localization support for multi-language content.",
            source_payload={
                "requirements": [
                    "API must support multiple languages for global users.",
                    "Content should be localized based on user preferences.",
                    "Currency and date formats may vary by region.",
                ]
            },
        )
    )

    categories = [record.category for record in result.records]
    assert "multi_language_support" in categories
    assert result.summary["missing_detail_flags"] == [
        "missing_locale_detection",
        "missing_translation_strategy",
    ]
    assert "Specify locale detection logic (Accept-Language parsing, user preferences, browser locale) and locale negotiation strategy." in result.summary["gap_messages"]
    assert "Define translation management approach (file structure, translation keys, translation services) and update workflow." in result.summary["gap_messages"]
    assert all(record.gap_messages == tuple(result.summary["gap_messages"]) for record in result.records)
    assert result.summary["missing_detail_counts"]["missing_locale_detection"] >= 1
    assert result.summary["status"] == "needs_localization_details"


def test_duplicate_evidence_serialization_aliases_and_markdown_are_stable():
    source = _source_brief(
        source_id="localization-model",
        title="Localization source",
        summary="Localization source.",
        source_payload={
            "localization": {
                "multi_language": "Multi-language support must handle all supported locales.",
                "same_multi_language": "Multi-language support must handle all supported locales.",
                "accept_language": "Accept-Language header parsing must support locale negotiation.",
            },
            "acceptance_criteria": [
                "Multi-language support must handle all supported locales.",
                "RTL language support must handle Arabic bidirectional text.",
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

    result = build_source_api_localization_requirements(source)
    extracted = extract_source_api_localization_requirements(model)
    derived = derive_source_api_localization_requirements(model)
    payload = source_api_localization_requirements_to_dict(result)
    markdown = source_api_localization_requirements_to_markdown(result)
    multi_language = next(record for record in result.records if record.category == "multi_language_support")

    assert source == original
    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_api_localization_requirements(result) == result.summary
    assert source_api_localization_requirements_to_dicts(result) == payload["requirements"]
    assert source_api_localization_requirements_to_dicts(result.records) == payload["records"]
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
    assert len(multi_language.evidence) == 1
    assert "Multi-language support must handle all supported locales" in multi_language.evidence[0]
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert result.records[0].suggested_plan_impacts == result.records[0].planning_notes
    assert markdown.startswith("# Source API Localization Requirements Report: localization-model")
    assert "multi" in markdown.casefold() or "language" in markdown.casefold()


def test_out_of_scope_unrelated_invalid_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-localization"
        summary = "No localization or internationalization work is required for this release."

    object_result = build_source_api_localization_requirements(
        SimpleNamespace(
            id="object-localization",
            summary="API must support multi-language content with Accept-Language header.",
            localization={"multi_language": "Multi-language support must include French and German."},
        )
    )
    negated = build_source_api_localization_requirements(BriefLike())
    no_scope = build_source_api_localization_requirements(
        _source_brief(summary="Localization is out of scope and no i18n work is planned.")
    )
    unrelated = build_source_api_localization_requirements(
        _source_brief(
            title="Physical locations",
            summary="Server location and geolocation services should be reviewed.",
            source_payload={"requirements": ["Update location tracking and GPS location features."]},
        )
    )
    malformed = build_source_api_localization_requirements({"source_payload": {"localization": {"notes": object()}}})
    blank = build_source_api_localization_requirements("")
    invalid = build_source_api_localization_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "multi_language_support": 0,
            "locale_based_content": 0,
            "accept_language_header": 0,
            "translation_management": 0,
            "currency_number_formatting": 0,
            "datetime_localization": 0,
            "rtl_language_support": 0,
            "locale_fallback_strategy": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
        "missing_detail_counts": {
            "missing_locale_detection": 0,
            "missing_translation_strategy": 0,
        },
        "gap_messages": [],
        "status": "no_localization_language",
    }
    assert "multi_language_support" in [record.category for record in object_result.records]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source API localization requirements were inferred" in unrelated.to_markdown()
    assert summarize_source_api_localization_requirements(unrelated) == expected_summary


def test_translation_management_and_locale_fallback():
    result = build_source_api_localization_requirements(
        _source_brief(
            summary="API must support translation management with locale fallback.",
            requirements=[
                "Translation files must use key-based lookup with JSON format.",
                "Translation service API must support dynamic translation updates.",
                "Locale fallback strategy must default to English for missing translations.",
                "Fallback locale hierarchy must be configurable per deployment.",
            ],
            source_payload={
                "localization": {
                    "translation": "Translation management must include version control and approval workflow.",
                    "rtl": "RTL language support must handle Arabic and Hebrew text direction.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert "translation_management" in by_category
    assert "locale_fallback_strategy" in by_category
    assert "rtl_language_support" in by_category
    assert result.summary["requirement_count"] >= 3
    assert result.summary["status"] in {"ready_for_planning", "needs_localization_details"}


def test_currency_number_and_datetime_formatting():
    result = build_source_api_localization_requirements(
        _source_brief(
            requirements=[
                "Currency format must respect locale-specific symbols and decimal separators.",
                "Number formatting must use thousand separators based on user locale.",
                "Date format must follow locale conventions (MM/DD/YYYY vs DD/MM/YYYY).",
                "Time format must support 12-hour and 24-hour display based on locale.",
                "Timezone must be detected from user preferences or Accept-Language header.",
            ],
            source_payload={
                "localization": {
                    "currency": "Currency display must show EUR for European locales and USD for US locale.",
                    "datetime": "Datetime localization must handle timezone conversion and DST adjustments.",
                }
            },
        )
    )

    currency = next((r for r in result.records if r.category == "currency_number_formatting"), None)
    datetime = next((r for r in result.records if r.category == "datetime_localization"), None)
    assert currency is not None
    assert datetime is not None
    assert currency.value in {"currency", "number format", "currency format", "decimal"}
    assert currency.suggested_owners == ("api_platform", "backend", "frontend")
    assert "currency" in currency.planning_notes[0].casefold() or "number" in currency.planning_notes[0].casefold()


def _source_brief(
    *,
    source_id="source-localization",
    title="Localization requirements",
    domain="api",
    summary="General localization requirements.",
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
    brief_id="implementation-localization",
    title="Localization implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-localization",
        "title": title,
        "domain": "api",
        "target_user": "developer",
        "buyer": "platform",
        "workflow_context": "API developers need localization planning.",
        "problem_statement": "Localization requirements need to be extracted early.",
        "mvp_goal": "Plan multi-language support, Accept-Language header parsing, translation management, and locale fallback.",
        "product_surface": "api",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run localization extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
