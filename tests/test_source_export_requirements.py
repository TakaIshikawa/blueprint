"""Tests for source export requirements extractor."""

import pytest

from blueprint.source_export_requirements import (
    ExportRequirements,
    extract_export_requirements,
)


def test_empty_source_data_returns_all_false():
    """Empty source data should return all fields as False."""
    result = extract_export_requirements({})

    assert isinstance(result, ExportRequirements)
    assert result.export_formats_defined is False
    assert result.data_scope_specified is False
    assert result.filtering_options_planned is False
    assert result.scheduling_capabilities_planned is False
    assert result.large_dataset_handling_addressed is False
    assert result.incremental_exports_supported is False
    assert result.data_masking_planned is False
    assert result.format_compatibility_ensured is False
    assert result.delivery_methods_defined is False
    assert result.user_experience_considered is False
    assert result.completeness_score == 0.0


def test_export_formats_detected():
    """Detect export formats in source data."""
    source = {
        "title": "Data export feature",
        "description": "Support CSV export",
    }

    result = extract_export_requirements(source)

    assert result.export_formats_defined is True
    assert result.completeness_score == 0.1


def test_data_scope_detected():
    """Detect data scope in source data."""
    source = {
        "description": "Allow users to export all records or selected data",
        "requirements": ["Full export support", "Partial data export"],
    }

    result = extract_export_requirements(source)

    assert result.data_scope_specified is True


def test_filtering_options_detected():
    """Detect filtering options in source data."""
    source = {
        "description": "Provide filtering options to select data before export",
        "requirements": ["Custom filters", "Date range filter"],
    }

    result = extract_export_requirements(source)

    assert result.filtering_options_planned is True


def test_scheduling_capabilities_detected():
    """Detect scheduling capabilities in source data."""
    source = {
        "description": "Support scheduled exports with automated daily export",
        "requirements": ["Export automation", "Recurring exports"],
    }

    result = extract_export_requirements(source)

    assert result.scheduling_capabilities_planned is True


def test_large_dataset_handling_detected():
    """Detect large dataset handling in source data."""
    source = {
        "description": "Handle large datasets with chunked export and streaming",
        "requirements": ["Export millions of records", "Memory efficient export"],
    }

    result = extract_export_requirements(source)

    assert result.large_dataset_handling_addressed is True


def test_incremental_exports_detected():
    """Detect incremental exports in source data."""
    source = {
        "description": "Support incremental exports with delta export of changes",
        "requirements": ["Export only updated data", "Change detection"],
    }

    result = extract_export_requirements(source)

    assert result.incremental_exports_supported is True


def test_data_masking_detected():
    """Detect data masking in source data."""
    source = {
        "description": "Implement data masking for sensitive data and PII protection",
        "requirements": ["Redact sensitive information", "Anonymize PII"],
    }

    result = extract_export_requirements(source)

    assert result.data_masking_planned is True


def test_format_compatibility_detected():
    """Detect format compatibility in source data."""
    source = {
        "description": "Ensure format compatibility with Excel and industry standards",
        "requirements": ["Standard formats", "Cross-platform compatibility"],
    }

    result = extract_export_requirements(source)

    assert result.format_compatibility_ensured is True


def test_delivery_methods_detected():
    """Detect delivery methods in source data."""
    source = {
        "description": "Support export delivery via email and S3 upload",
        "requirements": ["Download option", "FTP delivery"],
    }

    result = extract_export_requirements(source)

    assert result.delivery_methods_defined is True


def test_user_experience_detected():
    """Detect user experience considerations in source data."""
    source = {
        "description": "Provide user-friendly export interface with progress indicator",
        "requirements": ["Export wizard", "Download progress tracking"],
    }

    result = extract_export_requirements(source)

    assert result.user_experience_considered is True


def test_comprehensive_export_all_detected():
    """Test comprehensive export with all aspects present."""
    source = {
        "title": "Complete export system",
        "description": (
            "Support CSV and JSON export formats. "
            "Allow full and partial data export scope. "
            "Provide filtering options for custom data selection. "
            "Enable scheduled exports with automation. "
            "Handle large datasets with chunked export. "
            "Support incremental exports for changed data. "
            "Implement data masking for sensitive information. "
            "Ensure format compatibility with Excel. "
            "Provide delivery methods including S3 upload. "
            "Design user-friendly export interface with progress."
        ),
        "requirements": [
            "Export formats",
            "Data scope",
            "Filtering",
            "Scheduling",
            "Large datasets",
            "Incremental",
            "Data masking",
            "Compatibility",
            "Delivery",
            "User experience",
        ],
    }

    result = extract_export_requirements(source)

    assert result.export_formats_defined is True
    assert result.data_scope_specified is True
    assert result.filtering_options_planned is True
    assert result.scheduling_capabilities_planned is True
    assert result.large_dataset_handling_addressed is True
    assert result.incremental_exports_supported is True
    assert result.data_masking_planned is True
    assert result.format_compatibility_ensured is True
    assert result.delivery_methods_defined is True
    assert result.user_experience_considered is True
    assert result.completeness_score == 1.0


def test_invalid_source_data_none():
    """Test with None input."""
    result = extract_export_requirements(None)  # type: ignore

    assert isinstance(result, ExportRequirements)
    assert result.completeness_score == 0.0


def test_invalid_source_data_list():
    """Test with list input instead of mapping."""
    result = extract_export_requirements([{"key": "value"}])  # type: ignore

    assert isinstance(result, ExportRequirements)
    assert result.completeness_score == 0.0


def test_dataclass_immutability():
    """Test that ExportRequirements is frozen/immutable."""
    requirements = ExportRequirements(export_formats_defined=True)

    with pytest.raises(AttributeError):
        requirements.export_formats_defined = False  # type: ignore


def test_to_dict_method():
    """Test ExportRequirements.to_dict() serialization."""
    requirements = ExportRequirements(
        export_formats_defined=True,
        data_scope_specified=True,
        filtering_options_planned=False,
        scheduling_capabilities_planned=True,
        large_dataset_handling_addressed=False,
        incremental_exports_supported=True,
        data_masking_planned=False,
        format_compatibility_ensured=True,
        delivery_methods_defined=False,
        user_experience_considered=True,
    )

    result = requirements.to_dict()

    assert isinstance(result, dict)
    assert result["export_formats_defined"] is True
    assert result["data_scope_specified"] is True
    assert result["filtering_options_planned"] is False
    assert result["scheduling_capabilities_planned"] is True
    assert result["large_dataset_handling_addressed"] is False
    assert result["incremental_exports_supported"] is True
    assert result["data_masking_planned"] is False
    assert result["format_compatibility_ensured"] is True
    assert result["delivery_methods_defined"] is False
    assert result["user_experience_considered"] is True
    assert result["completeness_score"] == 0.6


def test_excel_export_pattern():
    """Test Excel export pattern detection."""
    source = {
        "description": "Export to Excel format",
    }

    result = extract_export_requirements(source)

    assert result.export_formats_defined is True


def test_pdf_export_pattern():
    """Test PDF export pattern detection."""
    source = {
        "description": "Generate PDF export",
    }

    result = extract_export_requirements(source)

    assert result.export_formats_defined is True


def test_streaming_export_edge_case():
    """Test streaming export detection."""
    source = {
        "description": "Use streaming export for efficient large file handling",
        "requirements": [
            "Chunked processing",
            "Export in batches",
        ],
    }

    result = extract_export_requirements(source)

    assert result.large_dataset_handling_addressed is True


def test_compressed_exports_edge_case():
    """Test compressed exports mentioned in description."""
    source = {
        "description": "Support compressed exports for large files with multi-format export",
        "requirements": [
            "CSV and JSON formats",
            "Export pagination",
        ],
    }

    result = extract_export_requirements(source)

    assert result.export_formats_defined is True
    assert result.large_dataset_handling_addressed is True


def test_multi_format_exports_edge_case():
    """Test multi-format export detection."""
    source = {
        "description": "Provide multiple export formats including CSV, JSON, and XML",
        "requirements": [
            "Support Excel export",
            "Format compatibility",
        ],
    }

    result = extract_export_requirements(source)

    assert result.export_formats_defined is True
    assert result.format_compatibility_ensured is True


def test_partial_completeness():
    """Test partial completeness with some aspects covered."""
    source = {
        "title": "Basic export",
        "description": "Export to CSV format",
        "requirements": [
            "Filter data before export",
            "Download export file",
        ],
    }

    result = extract_export_requirements(source)

    assert result.export_formats_defined is True
    assert result.filtering_options_planned is True
    assert result.delivery_methods_defined is True
    assert result.data_scope_specified is False
    assert result.scheduling_capabilities_planned is False
    assert result.large_dataset_handling_addressed is False
    assert result.incremental_exports_supported is False
    assert result.data_masking_planned is False
    assert result.format_compatibility_ensured is False
    assert result.user_experience_considered is False
    assert result.completeness_score == 0.3


def test_multiple_fields_in_different_sections():
    """Test detection across multiple source data sections."""
    source = {
        "title": "Export system",
        "description": "CSV export format",
        "requirements": ["Filter records"],
        "notes": ["Mask sensitive data"],
        "features": ["Progress indicator"],
    }

    result = extract_export_requirements(source)

    assert result.export_formats_defined is True
    assert result.filtering_options_planned is True
    assert result.data_masking_planned is True
    assert result.user_experience_considered is True
