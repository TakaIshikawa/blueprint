"""Tests for API versioning strategy analyzer."""

import pytest

from blueprint.task_api_versioning_strategy import (
    ApiVersioningStrategy,
    analyze_api_versioning_strategy,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_api_versioning_strategy({})

    assert isinstance(result, ApiVersioningStrategy)
    assert result.url_versioning_used is False
    assert result.header_versioning_used is False
    assert result.content_negotiation_used is False
    assert result.breaking_changes_identified is False
    assert result.backwards_compatibility_maintained is False
    assert result.deprecation_timeline_defined is False
    assert result.migration_path_documented is False
    assert result.semantic_versioning_followed is False
    assert result.readiness_score == 0.0


def test_url_versioning_detected():
    """Detect URL versioning in task data."""
    task = {
        "title": "Implement API v2",
        "description": "Add URL versioning with /v2/ endpoint paths",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True
    assert result.header_versioning_used is False


def test_header_versioning_detected():
    """Detect header versioning in task data."""
    task = {
        "description": "Use custom API-Version header for versioning",
        "acceptance_criteria": ["Header versioning implemented"],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.header_versioning_used is True
    assert result.url_versioning_used is False


def test_content_negotiation_detected():
    """Detect content negotiation in task data."""
    task = {
        "description": "Implement content negotiation via Accept header",
        "acceptance_criteria": ["Media type versioning configured"],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.content_negotiation_used is True


def test_breaking_changes_identified():
    """Detect breaking change identification in task data."""
    task = {
        "title": "Major version upgrade",
        "description": "Identify breaking changes for v2 API release",
        "acceptance_criteria": ["Breaking API changes documented"],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.breaking_changes_identified is True


def test_backwards_compatibility_maintained():
    """Detect backwards compatibility considerations in task data."""
    task = {
        "description": "Maintain backward compatibility with v1 clients",
        "acceptance_criteria": ["Non-breaking changes only", "API compatibility preserved"],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.backwards_compatibility_maintained is True


def test_deprecation_timeline_defined():
    """Detect deprecation timeline in task data."""
    task = {
        "description": "Define deprecation timeline for v1 API",
        "acceptance_criteria": ["Sunset schedule published", "EOL date communicated"],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.deprecation_timeline_defined is True


def test_migration_path_documented():
    """Detect migration path documentation in task data."""
    task = {
        "title": "Create migration guide",
        "description": "Document migration path from v1 to v2",
        "acceptance_criteria": ["Client migration strategy defined"],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.migration_path_documented is True


def test_semantic_versioning_followed():
    """Detect semantic versioning practices in task data."""
    task = {
        "description": "Follow semver for API version numbering",
        "acceptance_criteria": ["Semantic versioning scheme adopted"],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.semantic_versioning_followed is True


def test_comprehensive_versioning_all_detected():
    """Test comprehensive API versioning with all aspects present."""
    task = {
        "title": "Complete API versioning strategy",
        "description": (
            "Implement URL versioning with /v2/ paths. "
            "Use API-Version header for header-versioning. "
            "Support content negotiation via Accept header. "
            "Identify breaking changes and maintain backwards compatibility. "
            "Define deprecation timeline with sunset schedule. "
            "Document migration path for client upgrades. "
            "Follow semantic versioning (semver) for version numbering."
        ),
        "acceptance_criteria": [
            "URL versioning implemented",
            "Header versioning supported",
            "Content negotiation enabled",
            "Breaking changes identified",
            "Backwards compatibility maintained",
            "Deprecation timeline published",
            "Migration guide documented",
            "Semantic versioning adopted",
        ],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True
    assert result.header_versioning_used is True
    assert result.content_negotiation_used is True
    assert result.breaking_changes_identified is True
    assert result.backwards_compatibility_maintained is True
    assert result.deprecation_timeline_defined is True
    assert result.migration_path_documented is True
    assert result.semantic_versioning_followed is True
    assert abs(result.readiness_score - 1.0) < 0.01


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_api_versioning_strategy(None)  # type: ignore

    assert isinstance(result, ApiVersioningStrategy)
    assert result.url_versioning_used is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_api_versioning_strategy([{"key": "value"}])  # type: ignore

    assert isinstance(result, ApiVersioningStrategy)
    assert result.readiness_score == 0.0


def test_invalid_task_data_string():
    """Test with string input instead of mapping."""
    result = analyze_api_versioning_strategy("not a mapping")  # type: ignore

    assert isinstance(result, ApiVersioningStrategy)
    assert result.url_versioning_used is False


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "URL VERSIONING with HEADER VERSION and CONTENT NEGOTIATION",
        "acceptance_criteria": ["BREAKING CHANGES documented", "BACKWARDS COMPATIBILITY maintained"],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True
    assert result.header_versioning_used is True
    assert result.content_negotiation_used is True
    assert result.breaking_changes_identified is True
    assert result.backwards_compatibility_maintained is True


def test_url_versioning_variations():
    """Test various URL versioning patterns."""
    patterns = [
        "API v2 endpoints",
        "path version /v1/",
        "versioned URL structure",
        "endpoint version strategy",
    ]

    for pattern in patterns:
        task = {"description": pattern}
        result = analyze_api_versioning_strategy(task)
        assert result.url_versioning_used is True, f"Failed for pattern: {pattern}"


def test_header_versioning_variations():
    """Test various header versioning patterns."""
    patterns = [
        "Accept-Version header",
        "custom header version",
        "API version header",
        "version header approach",
    ]

    for pattern in patterns:
        task = {"description": pattern}
        result = analyze_api_versioning_strategy(task)
        assert result.header_versioning_used is True, f"Failed for pattern: {pattern}"


def test_deprecation_variations():
    """Test various deprecation timeline patterns."""
    patterns = [
        "deprecation schedule",
        "sunset timeline",
        "end of life policy",
        "EOL schedule",
        "phase out plan",
        "retirement schedule",
    ]

    for pattern in patterns:
        task = {"description": pattern}
        result = analyze_api_versioning_strategy(task)
        assert result.deprecation_timeline_defined is True, f"Failed for pattern: {pattern}"


def test_migration_path_variations():
    """Test various migration path patterns."""
    patterns = [
        "upgrade path documentation",
        "migration guide",
        "client migration strategy",
        "version migration plan",
        "gradual migration approach",
    ]

    for pattern in patterns:
        task = {"description": pattern}
        result = analyze_api_versioning_strategy(task)
        assert result.migration_path_documented is True, f"Failed for pattern: {pattern}"


def test_readiness_score_versioning_approach_only():
    """Test score with only versioning approach (30%)."""
    task = {
        "description": "Implement URL versioning for API",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True
    assert result.readiness_score == 0.3


def test_readiness_score_change_management_only():
    """Test score with only change management (60%)."""
    task = {
        "description": (
            "Identify breaking changes, maintain backwards compatibility, "
            "define deprecation timeline, and document migration path"
        ),
    }

    result = analyze_api_versioning_strategy(task)

    # All change management aspects present
    assert result.breaking_changes_identified is True
    assert result.backwards_compatibility_maintained is True
    assert result.deprecation_timeline_defined is True
    assert result.migration_path_documented is True
    assert result.readiness_score == 0.6


def test_readiness_score_semver_only():
    """Test score with only semantic versioning (10%)."""
    task = {
        "description": "Follow semantic versioning for API",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.semantic_versioning_followed is True
    assert result.readiness_score == 0.1


def test_readiness_score_combined():
    """Test score with combined aspects."""
    task = {
        "description": (
            "Implement URL-versioning with backward-compatibility "
            "and semantic-versioning"
        ),
    }

    result = analyze_api_versioning_strategy(task)

    # Approach: 30%, Change management: 1/4*60%=15%, Semver: 10%
    expected_score = 0.3 + 0.15 + 0.1
    assert abs(result.readiness_score - expected_score) < 0.01


def test_to_dict_method():
    """Test ApiVersioningStrategy.to_dict() serialization."""
    strategy = ApiVersioningStrategy(
        url_versioning_used=True,
        header_versioning_used=False,
        content_negotiation_used=True,
        breaking_changes_identified=True,
        backwards_compatibility_maintained=False,
        deprecation_timeline_defined=True,
        migration_path_documented=True,
        semantic_versioning_followed=False,
    )

    result = strategy.to_dict()

    assert isinstance(result, dict)
    assert result["url_versioning_used"] is True
    assert result["header_versioning_used"] is False
    assert result["content_negotiation_used"] is True
    assert result["breaking_changes_identified"] is True
    assert result["backwards_compatibility_maintained"] is False
    assert result["deprecation_timeline_defined"] is True
    assert result["migration_path_documented"] is True
    assert result["semantic_versioning_followed"] is False
    # Approach: 1*30%=30%, Change management: 3/4*60%=45%, Semver: 0*10%=0%
    expected_score = 0.3 + 0.45
    assert abs(result["readiness_score"] - expected_score) < 0.01


def test_dataclass_immutability():
    """Test that ApiVersioningStrategy is frozen/immutable."""
    strategy = ApiVersioningStrategy(url_versioning_used=True)

    with pytest.raises(AttributeError):
        strategy.url_versioning_used = False  # type: ignore


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "API versioning improvements",
        "description": "Implement URL-versioning",
        "acceptance_criteria": ["Header-versioning supported"],
        "requirements": ["Breaking-changes documented"],
        "notes": ["Deprecation-timeline needed"],
        "risks": ["No migration-path defined"],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True
    assert result.header_versioning_used is True
    assert result.breaking_changes_identified is True
    assert result.deprecation_timeline_defined is True
    assert result.migration_path_documented is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    task = {
        "validation_command": "pytest test-url-version.py test-header-version.py",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True
    assert result.header_versioning_used is True


def test_api_v1_v2_detection():
    """Test detection of versioned API paths."""
    task = {
        "description": "Migrate from API-v1 to API-v2",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True


def test_major_version_as_breaking_change():
    """Test major version increment as breaking change indicator."""
    task = {
        "description": "Release major version with breaking API changes",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.breaking_changes_identified is True


def test_non_breaking_change_detection():
    """Test non-breaking change detection."""
    task = {
        "description": "Ensure non-breaking changes for backward compatibility",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.backwards_compatibility_maintained is True
    assert result.breaking_changes_identified is True  # "non-breaking" matches pattern


def test_accept_header_content_negotiation():
    """Test Accept header as content negotiation."""
    task = {
        "description": "Use Accept header for API version selection",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.content_negotiation_used is True


def test_mime_type_versioning():
    """Test MIME type versioning detection."""
    task = {
        "description": "Version API via MIME-type-version in media type",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.content_negotiation_used is True


def test_sunset_policy():
    """Test sunset policy as deprecation timeline."""
    task = {
        "description": "Establish sunset policy for deprecated endpoints",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.deprecation_timeline_defined is True


def test_calver_versioning():
    """Test CalVer (calendar versioning) detection."""
    task = {
        "description": "Use CalVer version numbering scheme",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.semantic_versioning_followed is True


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is False
    assert result.readiness_score == 0.0


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Implement URL-versioning with header-versioning support",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True
    assert result.header_versioning_used is True


def test_partial_versioning_strategy():
    """Test partial versioning strategy with some aspects covered."""
    task = {
        "title": "Initial API versioning",
        "description": "Implement URL-versioning for new endpoints",
        "acceptance_criteria": [
            "Versioned endpoints created",
            "Backwards-compatibility maintained",
        ],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True
    assert result.backwards_compatibility_maintained is True
    assert result.breaking_changes_identified is False
    assert result.deprecation_timeline_defined is False
    # Approach: 30%, Change: 1/4*60%=15%, Semver: 0%
    expected_score = 0.3 + 0.15
    assert abs(result.readiness_score - expected_score) < 0.01


def test_rapid_deprecation_concern():
    """Test rapid deprecation as a versioning concern."""
    task = {
        "risks": ["Rapid deprecation-timeline may impact clients"],
    }

    result = analyze_api_versioning_strategy(task)

    assert result.deprecation_timeline_defined is True


def test_semantic_versioning_violation_detection():
    """Test semantic versioning violation detection."""
    task = {
        "description": "Avoid semantic versioning violations in API releases",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.semantic_versioning_followed is True


def test_versioning_consistency():
    """Test versioning consistency as a concern."""
    task = {
        "description": "Maintain URL-versioning consistency across API endpoints",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True


def test_multiple_concurrent_versions():
    """Test support for multiple concurrent API versions."""
    task = {
        "description": "Support multiple concurrent API-v1, API-v2, and API-v3",
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True


def test_all_versioning_approaches():
    """Test all three versioning approaches detected together."""
    task = {
        "description": (
            "Support URL-versioning, header-versioning, "
            "and content-negotiation simultaneously"
        ),
    }

    result = analyze_api_versioning_strategy(task)

    assert result.url_versioning_used is True
    assert result.header_versioning_used is True
    assert result.content_negotiation_used is True
    # Only one approach needed for full 30% score
    assert result.readiness_score == 0.3
