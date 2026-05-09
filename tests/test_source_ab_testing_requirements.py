"""Tests for A/B testing requirements extractor."""

import pytest

from blueprint.source_ab_testing_requirements import (
    AbTestingRequirement,
    AbTestingRequirementsReport,
    build_ab_testing_requirements_report,
    derive_ab_testing_requirements,
    extract_ab_testing_requirements,
)


def test_empty_source_returns_empty_report():
    """Empty source should return empty requirements."""
    result = build_ab_testing_requirements_report({})

    assert isinstance(result, AbTestingRequirementsReport)
    assert len(result.requirements) == 0
    assert result.summary["total_requirements"] == 0


def test_experiment_design_detected():
    """Detect experiment design requirements."""
    source = {
        "title": "Test Feature X",
        "summary": "Design A/B test to validate hypothesis about feature adoption",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "experiment_design"


def test_variant_specification_detected():
    """Detect variant specification requirements."""
    source = {
        "summary": "Test control group vs treatment variant with new UI",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 1
    types = [r.requirement_type for r in result.requirements]
    assert "variant_specification" in types


def test_success_metrics_detected():
    """Detect success metrics requirements."""
    source = {
        "success_criteria": "Measure conversion rate and click-through rate improvement",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 1
    types = [r.requirement_type for r in result.requirements]
    assert "success_metrics" in types


def test_sample_size_detected():
    """Detect sample size requirements."""
    source = {
        "context": "Need sufficient sample size to detect 5% effect",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 1
    types = [r.requirement_type for r in result.requirements]
    assert "sample_size" in types


def test_duration_detected():
    """Detect duration requirements."""
    source = {
        "constraints": "Test duration should be 2 weeks minimum",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 1
    types = [r.requirement_type for r in result.requirements]
    assert "duration" in types


def test_randomization_detected():
    """Detect randomization requirements."""
    source = {
        "goal": "Implement stratified randomization for user assignment",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 1
    types = [r.requirement_type for r in result.requirements]
    assert "randomization" in types


def test_statistical_significance_detected():
    """Detect statistical significance requirements."""
    source = {
        "acceptance_criteria": "Ensure 95% confidence level and p-value < 0.05",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 1
    types = [r.requirement_type for r in result.requirements]
    assert "statistical_significance" in types


def test_metric_selection_detected():
    """Detect metric selection requirements."""
    source = {
        "goals": "Select primary metric for conversion and guardrail metrics for quality",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 1
    types = [r.requirement_type for r in result.requirements]
    assert "metric_selection" in types


def test_segment_definition_detected():
    """Detect segment definition requirements."""
    source = {
        "context": "Target mobile user segment and premium cohort",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 1
    types = [r.requirement_type for r in result.requirements]
    assert "segment_definition" in types


def test_bias_prevention_detected():
    """Detect bias prevention requirements."""
    source = {
        "risks": "Prevent selection bias and confounding variables",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 1
    types = [r.requirement_type for r in result.requirements]
    assert "bias_prevention" in types


def test_comprehensive_ab_testing_requirements():
    """Test comprehensive A/B testing requirements extraction."""
    source = {
        "title": "Feature Launch A/B Test",
        "summary": "A/B test design for new checkout flow",
        "goal": "Measure conversion impact with stratified randomization",
        "success_criteria": "Target metrics: conversion rate and engagement",
        "context": "Test 10k users over 2 weeks across mobile and desktop segments",
        "constraints": "Achieve 95% confidence level with statistical significance",
        "acceptance_criteria": ["Primary metric selection documented", "Variant specifications defined"],
        "risks": "Selection bias prevention required",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 7
    types = {r.requirement_type for r in result.requirements}
    assert "experiment_design" in types
    assert "success_metrics" in types
    assert "randomization" in types
    assert "statistical_significance" in types
    assert "metric_selection" in types


def test_to_dict_method():
    """Test AbTestingRequirement.to_dict() serialization."""
    requirement = AbTestingRequirement(
        requirement_type="experiment_design",
        source_field="summary",
        evidence=("A/B test design",),
        confidence=1.0,
        recommended_follow_up="Confirm hypothesis",
    )

    result = requirement.to_dict()

    assert isinstance(result, dict)
    assert result["requirement_type"] == "experiment_design"
    assert result["source_field"] == "summary"
    assert result["evidence"] == ["A/B test design"]
    assert result["confidence"] == 1.0


def test_report_to_dict():
    """Test AbTestingRequirementsReport.to_dict() serialization."""
    source = {
        "id": "test-123",
        "title": "Test Report",
        "summary": "Experiment design with variant specification",
    }

    result = build_ab_testing_requirements_report(source)
    report_dict = result.to_dict()

    assert isinstance(report_dict, dict)
    assert report_dict["source_brief_id"] == "test-123"
    assert report_dict["title"] == "Test Report"
    assert "summary" in report_dict
    assert "requirements" in report_dict
    assert "records" in report_dict


def test_derive_ab_testing_requirements_alias():
    """Test derive_ab_testing_requirements alias function."""
    source = {"summary": "A/B test with variants"}

    result = derive_ab_testing_requirements(source)

    assert isinstance(result, tuple)
    assert all(isinstance(r, AbTestingRequirement) for r in result)


def test_extract_ab_testing_requirements_alias():
    """Test extract_ab_testing_requirements alias function."""
    source = {"summary": "Experiment design for feature"}

    result = extract_ab_testing_requirements(source)

    assert isinstance(result, tuple)
    assert all(isinstance(r, AbTestingRequirement) for r in result)


def test_invalid_input_none():
    """Test with None input."""
    result = build_ab_testing_requirements_report(None)  # type: ignore

    assert isinstance(result, AbTestingRequirementsReport)
    assert len(result.requirements) == 0


def test_invalid_input_string():
    """Test with string input instead of mapping."""
    result = build_ab_testing_requirements_report("not a mapping")  # type: ignore

    assert isinstance(result, AbTestingRequirementsReport)
    assert len(result.requirements) == 0


def test_source_brief_model_input():
    """Test with SourceBrief model input."""
    from blueprint.domain.models import SourceBrief

    source_brief = SourceBrief(
        id="brief-456",
        title="A/B Test Brief",
        summary="Experiment design with randomization",
        source_project="test-project",
        source_entity_type="feature",
        source_id="source-123",
        source_payload={},
        source_links={},
    )

    result = build_ab_testing_requirements_report(source_brief)

    assert result.source_brief_id == "brief-456"
    assert len(result.requirements) >= 1


def test_object_like_input():
    """Test with object-like input."""
    class MockSource:
        id = "obj-789"
        title = "Test Object"
        summary = "Statistical significance testing"

    result = build_ab_testing_requirements_report(MockSource())

    assert result.source_brief_id == "obj-789"
    assert len(result.requirements) >= 1


def test_dataclass_immutability():
    """Test that AbTestingRequirement is frozen/immutable."""
    requirement = AbTestingRequirement(requirement_type="experiment_design")

    with pytest.raises(AttributeError):
        requirement.requirement_type = "variant_specification"  # type: ignore


def test_summary_statistics():
    """Test summary statistics generation."""
    source = {
        "summary": "Experiment design with variants and success metrics",
    }

    result = build_ab_testing_requirements_report(source)

    assert "total_requirements" in result.summary
    assert "requirement_types_found" in result.summary
    assert "completeness_score" in result.summary
    assert "by_type" in result.summary


def test_multi_variate_testing_edge_case():
    """Test multi-variate testing detection."""
    source = {
        "summary": "Multi-variate test with control and 3 treatment variants",
    }

    result = build_ab_testing_requirements_report(source)

    types = [r.requirement_type for r in result.requirements]
    assert "variant_specification" in types


def test_sequential_testing_edge_case():
    """Test sequential testing detection."""
    source = {
        "context": "Sequential test with early stopping rules for statistical significance",
    }

    result = build_ab_testing_requirements_report(source)

    types = [r.requirement_type for r in result.requirements]
    assert "statistical_significance" in types


def test_holdout_group_edge_case():
    """Test holdout group detection."""
    source = {
        "acceptance_criteria": "Define target segment and maintain cohort for validation",
    }

    result = build_ab_testing_requirements_report(source)

    types = [r.requirement_type for r in result.requirements]
    assert "segment_definition" in types


def test_list_field_extraction():
    """Test extraction from list fields."""
    source = {
        "acceptance_criteria": [
            "Experiment design documented",
            "Randomization strategy defined",
            "Success metrics tracked",
        ],
    }

    result = build_ab_testing_requirements_report(source)

    types = {r.requirement_type for r in result.requirements}
    assert "experiment_design" in types
    assert "randomization" in types
    assert "success_metrics" in types


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    source = {
        "summary": "EXPERIMENT DESIGN with RANDOMIZATION and STATISTICAL SIGNIFICANCE",
    }

    result = build_ab_testing_requirements_report(source)

    types = {r.requirement_type for r in result.requirements}
    assert "experiment_design" in types
    assert "randomization" in types
    assert "statistical_significance" in types


def test_records_property():
    """Test records property compatibility."""
    source = {
        "summary": "Experiment with variants",
    }

    result = build_ab_testing_requirements_report(source)

    assert result.records == result.requirements
    assert isinstance(result.records, tuple)


def test_to_dicts_method():
    """Test to_dicts() method."""
    source = {
        "summary": "A/B test design",
    }

    result = build_ab_testing_requirements_report(source)
    dicts = result.to_dicts()

    assert isinstance(dicts, list)
    assert all(isinstance(d, dict) for d in dicts)


def test_empty_string_fields_ignored():
    """Test that empty string fields are ignored."""
    source = {
        "title": "",
        "summary": "",
        "context": "   ",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) == 0


def test_recommended_follow_up_included():
    """Test that recommended follow-up is included."""
    source = {
        "summary": "Experiment design required",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 1
    assert result.requirements[0].recommended_follow_up != ""


def test_confidence_score():
    """Test confidence score is set."""
    source = {
        "summary": "Variant specification needed",
    }

    result = build_ab_testing_requirements_report(source)

    assert len(result.requirements) >= 1
    assert result.requirements[0].confidence > 0.0


def test_requirement_ordering():
    """Test that requirements are ordered consistently."""
    source = {
        "summary": "Sample size calculation with experiment design and success metrics",
    }

    result = build_ab_testing_requirements_report(source)

    if len(result.requirements) >= 2:
        # experiment_design should come before sample_size
        types = [r.requirement_type for r in result.requirements]
        exp_idx = types.index("experiment_design") if "experiment_design" in types else -1
        size_idx = types.index("sample_size") if "sample_size" in types else -1
        if exp_idx >= 0 and size_idx >= 0:
            assert exp_idx < size_idx
