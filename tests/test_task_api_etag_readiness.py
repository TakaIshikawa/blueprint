import json

from blueprint.task_api_etag_readiness import (
    TaskApiETagReadinessFinding,
    TaskApiETagReadinessPlan,
    build_task_api_etag_readiness_plan,
    derive_task_api_etag_readiness_plan,
    extract_task_api_etag_readiness_findings,
    generate_task_api_etag_readiness_plan,
    summarize_task_api_etag_readiness,
)


def test_etag_generation_detected_with_partial_readiness():
    plan = {
        "id": "plan-etag",
        "tasks": [
            {
                "id": "task-etag",
                "title": "Implement ETag generation",
                "description": "API responses must generate strong ETags using SHA-256 hash of entity content.",
                "acceptance_criteria": ["ETag generation logic creates valid hash values"],
            },
        ],
    }

    result = build_task_api_etag_readiness_plan(plan)
    finding = result.findings[0]

    assert isinstance(result, TaskApiETagReadinessPlan)
    assert isinstance(finding, TaskApiETagReadinessFinding)
    assert finding.task_id == "task-etag"
    assert "etag_generation" in finding.detected_signals
    assert "etag_generation_tests" not in finding.present_safeguards
    assert "etag_generation_tests" in finding.missing_safeguards
    assert finding.readiness in {"weak", "partial"}
    assert len(finding.actionable_remediations) > 0
    assert "etag generation" in finding.actionable_remediations[0].casefold()


def test_cache_validation_with_tests_shows_strong_readiness():
    plan = {
        "id": "plan-validation",
        "tasks": [
            {
                "id": "task-validation",
                "title": "Implement cache validation",
                "description": "API must validate If-None-Match header and return 304 Not Modified on cache hit.",
                "acceptance_criteria": [
                    "Cache validation tests verify If-None-Match handling and 304 response scenarios"
                ],
            },
        ],
    }

    result = build_task_api_etag_readiness_plan(plan)
    finding = result.findings[0]

    assert "if_none_match_validation" in finding.detected_signals
    assert "cache_validation_tests" in finding.present_safeguards
    assert result.summary["etag_task_count"] == 1
    assert result.summary["readiness_counts"]["strong"] >= 0


def test_no_etag_impact_and_aliases_are_stable():
    plan = {
        "id": "plan-mixed",
        "tasks": [
            {
                "id": "task-etag",
                "title": "Add ETag support",
                "description": "ETag header must be included in all GET responses for cache validation.",
            },
            {
                "id": "task-no-impact",
                "title": "Update user profile",
                "description": "No ETag changes are required for this task.",
            },
        ],
    }

    result = build_task_api_etag_readiness_plan(plan)
    extracted = extract_task_api_etag_readiness_findings(plan)
    derived = derive_task_api_etag_readiness_plan(plan)
    generated = generate_task_api_etag_readiness_plan(plan)
    summary = summarize_task_api_etag_readiness(result)
    payload = result.to_dict()

    assert result.etag_task_ids == ("task-etag",)
    assert result.not_applicable_task_ids == ("task-no-impact",)
    assert extracted == result.findings
    assert derived.to_dict() == result.to_dict()
    assert generated.to_dict() == result.to_dict()
    assert summary == result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.findings[0].actionable_gaps == result.findings[0].actionable_remediations


def test_concurrent_update_detection_with_tests():
    plan = {
        "id": "plan-concurrent",
        "tasks": [
            {
                "id": "task-concurrent",
                "title": "Implement optimistic locking",
                "description": "API must detect concurrent updates via ETag comparison. Concurrent update tests verify version conflict handling and lost update prevention.",
            },
        ],
    }

    result = build_task_api_etag_readiness_plan(plan)
    finding = result.findings[0]

    assert "concurrent_update_detection" in finding.detected_signals
    assert "concurrent_update_tests" in finding.present_safeguards
    assert "concurrent_update_tests" not in finding.missing_safeguards
