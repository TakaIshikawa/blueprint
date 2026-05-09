"""Tests for internationalization requirements extractor."""

import pytest

from blueprint.source_internationalization_requirements import (
    InternationalizationRequirements,
    extract_internationalization_requirements,
)


def test_empty_source_data_returns_all_false():
    """Empty source data should return all fields as False."""
    result = extract_internationalization_requirements({})

    assert isinstance(result, InternationalizationRequirements)
    assert result.target_locales_defined is False
    assert result.translation_workflow_specified is False
    assert result.rtl_support_required is False
    assert result.locale_formatting_addressed is False
    assert result.string_externalization_planned is False
    assert result.pluralization_handled is False
    assert result.content_management_specified is False
    assert result.translation_testing_included is False
    assert result.completeness_score == 0.0


def test_target_locales_detected():
    """Detect target locales in source data."""
    source = {
        "title": "Support multiple languages",
        "description": "Add support for target locales: en-US, es-ES, fr-FR",
    }

    result = extract_internationalization_requirements(source)

    assert result.target_locales_defined is True
    assert result.completeness_score == 0.125


def test_translation_workflow_detected():
    """Detect translation workflow in source data."""
    source = {
        "description": "Integrate with Crowd in for translation management workflow",
        "requirements": ["Professional translation service", "Translation pipeline configured"],
    }

    result = extract_internationalization_requirements(source)

    assert result.translation_workflow_specified is True


def test_rtl_support_detected():
    """Detect RTL support requirements in source data."""
    source = {
        "description": "Support right-to-left layout for Arabic and Hebrew",
        "requirements": ["RTL support implemented", "Bidirectional text handling"],
    }

    result = extract_internationalization_requirements(source)

    assert result.rtl_support_required is True


def test_locale_formatting_detected():
    """Detect locale-specific formatting in source data."""
    source = {
        "description": "Implement date formatting and currency formatting for each locale",
        "requirements": ["Locale-specific number formatting", "Date picker localization"],
    }

    result = extract_internationalization_requirements(source)

    assert result.locale_formatting_addressed is True


def test_string_externalization_detected():
    """Detect string externalization in source data."""
    source = {
        "description": "Externalize strings to resource files for translation",
        "requirements": ["Extract strings to .properties files", "Translation keys defined"],
    }

    result = extract_internationalization_requirements(source)

    assert result.string_externalization_planned is True


def test_pluralization_detected():
    """Detect pluralization handling in source data."""
    source = {
        "description": "Implement ICU message format for pluralization rules",
        "requirements": ["Plural forms supported", "Intl plural rules configured"],
    }

    result = extract_internationalization_requirements(source)

    assert result.pluralization_handled is True


def test_content_management_detected():
    """Detect content management for i18n in source data."""
    source = {
        "description": "Set up CMS internationalization for dynamic content translation",
        "requirements": ["Content localization workflow", "UGC translation support"],
    }

    result = extract_internationalization_requirements(source)

    assert result.content_management_specified is True


def test_translation_testing_detected():
    """Detect translation testing in source data."""
    source = {
        "description": "Include i18n testing and pseudo-localization for QA",
        "requirements": ["Translation quality testing", "Test all locales"],
    }

    result = extract_internationalization_requirements(source)

    assert result.translation_testing_included is True


def test_comprehensive_i18n_all_detected():
    """Test comprehensive i18n requirements with all aspects present."""
    source = {
        "title": "Complete internationalization implementation",
        "description": (
            "Support target locales en-US, es-ES, fr-FR, ar-SA with translation workflow via Crowdin. "
            "Implement RTL support for Arabic with locale-specific date and currency formatting. "
            "Externalize strings to resource bundles with ICU pluralization support. "
            "Configure CMS i18n for content management and include translation testing with pseudo-localization."
        ),
        "requirements": [
            "Multi-language support enabled",
            "Translation pipeline established",
            "Bidirectional text handling",
            "Number formatting per locale",
            "String externalization complete",
            "Plural forms implemented",
            "Dynamic content translation",
            "I18n testing included",
        ],
    }

    result = extract_internationalization_requirements(source)

    assert result.target_locales_defined is True
    assert result.translation_workflow_specified is True
    assert result.rtl_support_required is True
    assert result.locale_formatting_addressed is True
    assert result.string_externalization_planned is True
    assert result.pluralization_handled is True
    assert result.content_management_specified is True
    assert result.translation_testing_included is True
    assert result.completeness_score == 1.0


def test_invalid_source_data_none():
    """Test with None input."""
    result = extract_internationalization_requirements(None)  # type: ignore

    assert isinstance(result, InternationalizationRequirements)
    assert result.target_locales_defined is False
    assert result.completeness_score == 0.0


def test_partial_i18n_requirements():
    """Test partial i18n requirements with some aspects covered."""
    source = {
        "title": "Basic internationalization",
        "description": "Support locales en-US and es-ES with translation workflow",
        "requirements": [
            "Target languages defined",
            "Translation service integrated",
        ],
    }

    result = extract_internationalization_requirements(source)

    assert result.target_locales_defined is True
    assert result.translation_workflow_specified is True
    assert result.rtl_support_required is False
    assert result.locale_formatting_addressed is False
    assert result.string_externalization_planned is False
    assert result.pluralization_handled is False
    assert result.content_management_specified is False
    assert result.translation_testing_included is False
    assert result.completeness_score == 0.25


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    source = {
        "description": "TARGET LOCALES with TRANSLATION WORKFLOW and RTL SUPPORT",
        "requirements": ["STRING EXTERNALIZATION", "PLURALIZATION support"],
    }

    result = extract_internationalization_requirements(source)

    assert result.target_locales_defined is True
    assert result.translation_workflow_specified is True
    assert result.rtl_support_required is True
    assert result.string_externalization_planned is True
    assert result.pluralization_handled is True


def test_language_codes_detected():
    """Test that language/locale codes trigger target locales detection."""
    source = {
        "description": "Support en-US, ja-JP, and zh-CN markets",
    }

    result = extract_internationalization_requirements(source)

    assert result.target_locales_defined is True


def test_translation_services_detected():
    """Test various translation service names."""
    services = ["Crowdin", "Lokalise", "Phrase", "Transifex"]

    for service in services:
        source = {"description": f"Use {service} for translations"}
        result = extract_internationalization_requirements(source)
        assert result.translation_workflow_specified is True, f"Failed for {service}"


def test_resource_file_formats_detected():
    """Test various resource file formats."""
    formats = [".properties", ".resx", ".po", ".pot", ".json i18n"]

    for fmt in formats:
        source = {"description": f"Store translations in {fmt} files"}
        result = extract_internationalization_requirements(source)
        assert result.string_externalization_planned is True, f"Failed for {fmt}"


def test_to_dict_method():
    """Test InternationalizationRequirements.to_dict() serialization."""
    requirements = InternationalizationRequirements(
        target_locales_defined=True,
        translation_workflow_specified=True,
        rtl_support_required=False,
        locale_formatting_addressed=True,
        string_externalization_planned=False,
        pluralization_handled=True,
        content_management_specified=False,
        translation_testing_included=True,
    )

    result = requirements.to_dict()

    assert isinstance(result, dict)
    assert result["target_locales_defined"] is True
    assert result["translation_workflow_specified"] is True
    assert result["rtl_support_required"] is False
    assert result["locale_formatting_addressed"] is True
    assert result["string_externalization_planned"] is False
    assert result["pluralization_handled"] is True
    assert result["content_management_specified"] is False
    assert result["translation_testing_included"] is True
    assert result["completeness_score"] == 0.625


def test_dataclass_immutability():
    """Test that InternationalizationRequirements is frozen/immutable."""
    requirements = InternationalizationRequirements(target_locales_defined=True)

    with pytest.raises(AttributeError):
        requirements.target_locales_defined = False  # type: ignore


def test_completeness_score_calculation():
    """Test completeness score calculation with different combinations."""
    # 0/8 = 0.0
    source1 = {"description": "Generic task"}
    result1 = extract_internationalization_requirements(source1)
    assert result1.completeness_score == 0.0

    # 1/8 = 0.125
    source2 = {"description": "Support en-US locale"}
    result2 = extract_internationalization_requirements(source2)
    assert result2.completeness_score == 0.125

    # 4/8 = 0.5
    source3 = {
        "description": "Target locales with translation workflow, RTL support, and date formatting"
    }
    result3 = extract_internationalization_requirements(source3)
    assert result3.completeness_score == 0.5


def test_multiple_fields_in_different_sections():
    """Test detection across multiple source data sections."""
    source = {
        "title": "I18n implementation",
        "description": "Support multiple languages",
        "requirements": ["Translation workflow needed"],
        "constraints": ["RTL layout required"],
        "notes": ["Currency formatting per locale"],
    }

    result = extract_internationalization_requirements(source)

    assert result.target_locales_defined is True
    assert result.translation_workflow_specified is True
    assert result.rtl_support_required is True
    assert result.locale_formatting_addressed is True


def test_dynamic_content_translation():
    """Test dynamic content translation detection."""
    source = {
        "description": "Translate user-generated content dynamically",
    }

    result = extract_internationalization_requirements(source)

    assert result.content_management_specified is True


def test_fallback_locales():
    """Test fallback locale detection."""
    source = {
        "description": "Multi-lingual application with internationalization",
    }

    result = extract_internationalization_requirements(source)

    assert result.target_locales_defined is True


def test_decimal_separator_formatting():
    """Test decimal separator as locale formatting."""
    source = {
        "description": "Handle decimal separator and thousand separator per locale",
    }

    result = extract_internationalization_requirements(source)

    assert result.locale_formatting_addressed is True


def test_machine_translation():
    """Test machine translation workflow detection."""
    source = {
        "description": "Use machine translation as initial translation step",
    }

    result = extract_internationalization_requirements(source)

    assert result.translation_workflow_specified is True


def test_icu_message_format():
    """Test ICU message format detection."""
    source = {
        "description": "Use ICU message format for complex pluralization",
    }

    result = extract_internationalization_requirements(source)

    assert result.pluralization_handled is True


def test_linguistic_validation():
    """Test linguistic validation as translation testing."""
    source = {
        "description": "Perform linguistic testing for translation quality",
    }

    result = extract_internationalization_requirements(source)

    assert result.translation_testing_included is True
