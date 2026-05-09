"""Tests for analytics requirements extractor."""

import json

from blueprint.domain.models import SourceBrief
from blueprint.source_analytics_requirements import (
    AnalyticsRequirement,
    AnalyticsRequirementsReport,
    build_analytics_requirements_report,
    extract_analytics_requirements,
    analyze_analytics_requirements,
    derive_analytics_requirements,
    generate_analytics_requirements,
    summarize_analytics_requirements,
)


def test_extracts_multi_signal_analytics_requirements_with_evidence():
    """Test extraction of multiple analytics requirement types."""
    result = build_analytics_requirements_report(
        _source_brief(
            summary=(
                "Implement event tracking for user actions with funnel analysis. "
                "Track custom dimensions for segmentation and support A/B testing."
            ),
            source_payload={
                "requirements": [
                    "Define event schema for page views and click events.",
                    "Track conversion funnel from signup to purchase.",
                    "Segment users into cohorts based on signup date.",
                    "Support A/B test tracking with experiment variants.",
                    "Capture custom properties for user demographics.",
                    "Scrub PII from analytics events before sending.",
                    "Implement consent management for GDPR compliance.",
                    "Set data retention policy to 90 days.",
                    "Enable cross-device tracking with device stitching.",
                    "Ensure data governance compliance with audit logs.",
                ],
            },
        )
    )

    assert isinstance(result, AnalyticsRequirementsReport)
    assert all(isinstance(record, AnalyticsRequirement) for record in result.records)
    assert set(record.requirement_type for record in result.records) == {
        "event_tracking",
        "funnel_analysis",
        "cohort_analysis",
        "ab_testing",
        "custom_dimensions",
        "pii_handling",
        "consent_management",
        "retention_policies",
        "cross_device_tracking",
        "data_governance",
    }

    by_type = {record.requirement_type: record for record in result.records}
    assert any("event" in item.lower() for item in by_type["event_tracking"].evidence)
    assert any("funnel" in item.lower() for item in by_type["funnel_analysis"].evidence)
    assert any("cohort" in item.lower() for item in by_type["cohort_analysis"].evidence)
    assert any("a/b" in item.lower() or "test" in item.lower() for item in by_type["ab_testing"].evidence)
    assert any("pii" in item.lower() for item in by_type["pii_handling"].evidence)
    assert result.summary["requirement_count"] == 10
    assert result.summary["event_coverage"] == 100  # All event types covered
    assert result.summary["privacy_coverage"] == 100  # All privacy types covered
    assert result.summary["implementation_clarity"] == 100  # All types covered


def test_brief_without_analytics_language_returns_stable_empty_report():
    """Test that briefs without analytics terms return empty report."""
    result = build_analytics_requirements_report(
        _source_brief(
            title="Database migration",
            summary="Add new column to users table with default value.",
            source_payload={
                "requirements": [
                    "Create migration script for schema change.",
                    "Ensure backward compatibility with existing code.",
                ],
            },
        )
    )
    repeat = build_analytics_requirements_report(
        _source_brief(
            title="Database migration",
            summary="Add new column to users table with default value.",
            source_payload={
                "requirements": [
                    "Create migration script for schema change.",
                    "Ensure backward compatibility with existing code.",
                ],
            },
        )
    )

    expected_summary = {
        "requirement_count": 0,
        "source_count": 1,
        "type_counts": {
            "event_tracking": 0,
            "funnel_analysis": 0,
            "cohort_analysis": 0,
            "ab_testing": 0,
            "custom_dimensions": 0,
            "pii_handling": 0,
            "data_governance": 0,
            "retention_policies": 0,
            "cross_device_tracking": 0,
            "consent_management": 0,
        },
        "event_coverage": 0,
        "privacy_coverage": 0,
        "implementation_clarity": 0,
    }
    assert result.summary == expected_summary
    assert result.requirements == ()
    assert result.records == ()
    assert result.summary == repeat.summary
    assert result.to_dict() == repeat.to_dict()
    assert json.dumps(result.to_dict(), sort_keys=True) == json.dumps(repeat.to_dict(), sort_keys=True)


def test_detects_event_tracking_requirements():
    """Test detection of event tracking patterns."""
    result = build_analytics_requirements_report(
        {"description": "Implement event tracking for user actions and page views"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "event_tracking"
    assert any("event" in term.lower() for term in result.requirements[0].matched_terms)


def test_detects_funnel_analysis_requirements():
    """Test detection of funnel analysis patterns."""
    result = build_analytics_requirements_report(
        {"description": "Track conversion funnel from signup to purchase with drop-off analysis"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "funnel_analysis"
    assert any("funnel" in term.lower() for term in result.requirements[0].matched_terms)


def test_detects_cohort_analysis_requirements():
    """Test detection of cohort analysis patterns."""
    result = build_analytics_requirements_report(
        {"description": "Analyze user cohorts based on signup date with cohort retention metrics"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "cohort_analysis"
    assert any("cohort" in term.lower() for term in result.requirements[0].matched_terms)


def test_detects_ab_testing_requirements():
    """Test detection of A/B testing patterns."""
    result = build_analytics_requirements_report(
        {"description": "Track A/B test experiments with variant assignments and treatment groups"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "ab_testing"


def test_detects_custom_dimensions_requirements():
    """Test detection of custom dimensions patterns."""
    result = build_analytics_requirements_report(
        {"description": "Capture custom properties and user attributes for dimensional analysis"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "custom_dimensions"


def test_detects_pii_handling_requirements():
    """Test detection of PII handling patterns."""
    result = build_analytics_requirements_report(
        {"description": "Scrub PII and personally identifiable information from analytics data"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "pii_handling"
    assert any("pii" in term.lower() for term in result.requirements[0].matched_terms)


def test_detects_data_governance_requirements():
    """Test detection of data governance patterns."""
    result = build_analytics_requirements_report(
        {"description": "Ensure data governance and compliance with data quality standards"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "data_governance"


def test_detects_retention_policy_requirements():
    """Test detection of retention policy patterns."""
    result = build_analytics_requirements_report(
        {"description": "Set data retention policy to expire old data after 90 days"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "retention_policies"
    assert any("retention" in term.lower() or "expire" in term.lower() for term in result.requirements[0].matched_terms)


def test_detects_cross_device_tracking_requirements():
    """Test detection of cross-device tracking patterns."""
    result = build_analytics_requirements_report(
        {"description": "Enable cross-device tracking with device stitching and unified profiles"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "cross_device_tracking"


def test_detects_consent_management_requirements():
    """Test detection of consent management patterns."""
    result = build_analytics_requirements_report(
        {"description": "Implement GDPR consent management with user opt-in and consent tracking"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "consent_management"
    assert any("consent" in term.lower() for term in result.requirements[0].matched_terms)


def test_to_dict_serialization():
    """Test to_dict() method produces valid JSON."""
    result = build_analytics_requirements_report(
        {
            "title": "Analytics implementation",
            "summary": "Track events and handle PII with consent management",
        }
    )
    data = result.to_dict()
    assert isinstance(data, dict)
    assert "source_brief_id" in data
    assert "requirements" in data
    assert "summary" in data
    assert "records" in data
    json_str = json.dumps(data)
    assert json.loads(json_str) == data


def test_to_dicts_returns_list_of_dicts():
    """Test to_dicts() returns list of requirement dictionaries."""
    result = build_analytics_requirements_report(
        {"description": "Track events and analyze funnels"}
    )
    dicts = result.to_dicts()
    assert isinstance(dicts, list)
    assert all(isinstance(item, dict) for item in dicts)
    assert all("requirement_type" in item for item in dicts)


def test_to_markdown_rendering():
    """Test to_markdown() renders valid Markdown."""
    result = build_analytics_requirements_report(
        {
            "id": "test-source-123",
            "summary": "Implement event tracking with PII scrubbing",
        }
    )
    markdown = result.to_markdown()
    assert isinstance(markdown, str)
    assert "# Analytics Requirements Report: test-source-123" in markdown
    assert "## Summary" in markdown
    assert "## Requirements" in markdown
    assert "| Type | Source Field Paths | Evidence | Follow-up Questions |" in markdown


def test_to_markdown_empty_report():
    """Test to_markdown() with empty report."""
    result = build_analytics_requirements_report({"description": "Database migration"})
    markdown = result.to_markdown()
    assert "No analytics requirements were inferred" in markdown


def test_extract_analytics_requirements_alias():
    """Test extract_analytics_requirements alias function."""
    requirements = extract_analytics_requirements(
        {"description": "Track event schema for user actions"}
    )
    assert isinstance(requirements, tuple)
    assert all(isinstance(req, AnalyticsRequirement) for req in requirements)


def test_compatibility_aliases():
    """Test that compatibility alias functions work."""
    source = {"description": "Track events with PII handling"}

    result1 = generate_analytics_requirements(source)
    result2 = analyze_analytics_requirements(source)
    result3 = derive_analytics_requirements(source)

    assert result1 == result2 == result3
    assert all(isinstance(req, AnalyticsRequirement) for req in result1)


def test_summarize_analytics_requirements():
    """Test summarize_analytics_requirements function."""
    summary = summarize_analytics_requirements(
        {"description": "Track events with funnel analysis and PII handling"}
    )
    assert isinstance(summary, dict)
    assert "requirement_count" in summary
    assert "event_coverage" in summary
    assert "privacy_coverage" in summary
    assert summary["requirement_count"] >= 2


def test_source_brief_model_input():
    """Test that SourceBrief model instances are handled."""
    brief = SourceBrief(
        id="brief-123",
        source_project="test-project",
        source_entity_type="feature",
        source_id="feature-001",
        title="Analytics setup",
        summary="Track events and handle consent",
        source_payload={},
        source_links={},
    )
    result = build_analytics_requirements_report(brief)
    assert result.source_brief_id == "brief-123"
    assert len(result.requirements) >= 1


def test_string_input():
    """Test that plain string input is handled."""
    result = build_analytics_requirements_report(
        "Track event schema with PII scrubbing and consent management"
    )
    assert len(result.requirements) >= 2
    types = {req.requirement_type for req in result.requirements}
    assert "event_tracking" in types


def test_object_input():
    """Test that object-like input is handled."""
    class MockSource:
        title = "Analytics feature"
        summary = "Track events with funnel analysis"

    result = build_analytics_requirements_report(MockSource())
    assert len(result.requirements) >= 1


def test_invalid_input_returns_empty():
    """Test that invalid inputs return empty report."""
    result = build_analytics_requirements_report(b"bytes input")
    assert result.requirements == ()
    assert result.summary["requirement_count"] == 0


def test_evidence_includes_source_field_paths():
    """Test that evidence includes source field paths."""
    result = build_analytics_requirements_report(
        {
            "title": "Analytics setup",
            "summary": "Track events",
            "requirements": ["Implement event tracking with funnel analysis"],
        }
    )
    for req in result.requirements:
        assert len(req.source_field_paths) > 0
        assert all(isinstance(path, str) for path in req.source_field_paths)


def test_matched_terms_captured():
    """Test that matched terms are captured."""
    result = build_analytics_requirements_report(
        {"description": "Track page views and click events for event tracking"}
    )
    event_req = next(req for req in result.requirements if req.requirement_type == "event_tracking")
    assert len(event_req.matched_terms) > 0
    # Should match "click events" and "event tracking"
    terms_lower = [term.lower() for term in event_req.matched_terms]
    assert any("event" in term for term in terms_lower)


def test_follow_up_questions_included():
    """Test that follow-up questions are included."""
    result = build_analytics_requirements_report(
        {"description": "Track events with funnel analysis"}
    )
    for req in result.requirements:
        assert len(req.follow_up_questions) > 0
        assert all(isinstance(q, str) for q in req.follow_up_questions)


def test_requirement_type_stable_ordering():
    """Test that requirements are returned in stable type order."""
    result = build_analytics_requirements_report(
        {
            "description": (
                "Implement consent management with event tracking, "
                "PII handling, funnel analysis, and cohort analysis"
            )
        }
    )
    types = [req.requirement_type for req in result.requirements]
    # Should be in TYPE_ORDER sequence
    expected_order = ["event_tracking", "funnel_analysis", "cohort_analysis", "pii_handling", "consent_management"]
    assert types == expected_order


def test_dataclass_immutability():
    """Test that AnalyticsRequirement is immutable."""
    req = AnalyticsRequirement(
        requirement_type="event_tracking",
        evidence=("test",),
    )
    try:
        req.requirement_type = "funnel_analysis"  # type: ignore
        assert False, "Should not allow mutation"
    except AttributeError:
        pass  # Expected


def test_real_time_analytics_detection():
    """Test detection of real-time analytics requirements."""
    result = build_analytics_requirements_report(
        {"description": "Real-time event tracking with custom dimensions for analytics"}
    )
    types = {req.requirement_type for req in result.requirements}
    assert "event_tracking" in types
    assert "custom_dimensions" in types


def test_custom_attribution_models():
    """Test detection patterns don't over-match."""
    result = build_analytics_requirements_report(
        {"description": "Track custom dimensions for attribution model"}
    )
    # Should detect custom dimensions when explicitly mentioned
    types = {req.requirement_type for req in result.requirements}
    assert "custom_dimensions" in types


def test_data_warehouse_integration():
    """Test that data warehouse mentions don't create false positives."""
    result = build_analytics_requirements_report(
        {"description": "Integrate with data warehouse for storage"}
    )
    # Should not detect analytics requirements without explicit analytics terms
    assert len(result.requirements) == 0


def test_summary_type_counts():
    """Test that summary includes type counts."""
    result = build_analytics_requirements_report(
        {
            "description": "Track event schema, analyze conversion funnels, handle PII data",
        }
    )
    type_counts = result.summary["type_counts"]
    assert isinstance(type_counts, dict)
    assert type_counts["event_tracking"] >= 1
    assert type_counts["funnel_analysis"] >= 1
    assert type_counts["pii_handling"] >= 1


def test_coverage_metrics_calculated():
    """Test that coverage metrics are calculated."""
    result = build_analytics_requirements_report(
        {
            "description": (
                "Track events with funnel analysis, cohort tracking, A/B testing, "
                "and custom dimensions. Handle PII with consent management."
            )
        }
    )
    assert 0 <= result.summary["event_coverage"] <= 100
    assert 0 <= result.summary["privacy_coverage"] <= 100
    assert 0 <= result.summary["implementation_clarity"] <= 100
    # With comprehensive coverage, should be high
    assert result.summary["event_coverage"] >= 80
    assert result.summary["privacy_coverage"] >= 25


def test_no_mutation_from_analysis():
    """Test that analyzing source doesn't mutate input."""
    source = {
        "title": "Test",
        "description": "Track events with PII handling",
    }
    original = dict(source)
    build_analytics_requirements_report(source)
    assert source == original


def test_nested_source_payload():
    """Test that nested source_payload is scanned."""
    result = build_analytics_requirements_report(
        {
            "title": "Analytics feature",
            "source_payload": {
                "requirements": ["Track event schema for user actions"],
            },
        }
    )
    assert len(result.requirements) >= 1
    assert any(req.requirement_type == "event_tracking" for req in result.requirements)


def _source_brief(**kwargs) -> dict:
    """Create a test source brief dictionary."""
    return {
        "id": kwargs.get("id", "test-brief-001"),
        "title": kwargs.get("title", "Test Brief"),
        "summary": kwargs.get("summary", ""),
        "source_payload": kwargs.get("source_payload", {}),
    }
