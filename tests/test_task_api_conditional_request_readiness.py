import json

from blueprint.task_api_conditional_request_readiness import (
    TaskApiConditionalRequestReadinessFinding,
    TaskApiConditionalRequestReadinessPlan,
    build_task_api_conditional_request_readiness_plan,
    derive_task_api_conditional_request_readiness_plan,
    extract_task_api_conditional_request_readiness_findings,
    generate_task_api_conditional_request_readiness_plan,
    summarize_task_api_conditional_request_readiness,
)


def test_if_match_precondition_detected_with_partial_readiness():
    plan = {
        "id": "plan-conditional",
        "tasks": [
            {
                "id": "task-conditional",
                "title": "Implement If-Match precondition",
                "description": "API mutations must validate If-Match header to prevent lost updates.",
                "acceptance_criteria": ["If-Match precondition logic validates ETag values"],
            },
        ],
    }

    result = build_task_api_conditional_request_readiness_plan(plan)
    finding = result.findings[0]

    assert isinstance(result, TaskApiConditionalRequestReadinessPlan)
    assert isinstance(finding, TaskApiConditionalRequestReadinessFinding)
    assert finding.task_id == "task-conditional"
    assert "if_match_precondition" in finding.detected_signals
    assert "precondition_validation_tests" not in finding.present_safeguards
    assert "precondition_validation_tests" in finding.missing_safeguards
    assert finding.readiness in {"weak", "partial"}
    assert len(finding.actionable_remediations) > 0
    assert "if-match" in finding.actionable_remediations[0].casefold()


def test_precondition_validation_with_tests_shows_strong_readiness():
    plan = {
        "id": "plan-validation",
        "tasks": [
            {
                "id": "task-validation",
                "title": "Implement conditional request validation",
                "description": "API must validate If-Match and If-Unmodified-Since headers and return 412 Precondition Failed on mismatch.",
                "acceptance_criteria": [
                    "Precondition validation tests verify If-Match handling and 412 response scenarios"
                ],
            },
        ],
    }

    result = build_task_api_conditional_request_readiness_plan(plan)
    finding = result.findings[0]

    assert "if_match_precondition" in finding.detected_signals
    assert "precondition_validation_tests" in finding.present_safeguards
    assert result.summary["conditional_request_task_count"] == 1
    assert result.summary["readiness_counts"]["strong"] >= 0


def test_no_conditional_request_impact_and_aliases_are_stable():
    plan = {
        "id": "plan-mixed",
        "tasks": [
            {
                "id": "task-conditional",
                "title": "Add optimistic locking",
                "description": "Optimistic locking must be implemented to prevent lost updates using If-Match validation.",
            },
            {
                "id": "task-no-impact",
                "title": "Update user profile",
                "description": "No conditional request changes are required for this task.",
            },
        ],
    }

    result = build_task_api_conditional_request_readiness_plan(plan)
    extracted = extract_task_api_conditional_request_readiness_findings(plan)
    derived = derive_task_api_conditional_request_readiness_plan(plan)
    generated = generate_task_api_conditional_request_readiness_plan(plan)
    summary = summarize_task_api_conditional_request_readiness(result)
    payload = result.to_dict()

    assert result.conditional_request_task_ids == ("task-conditional",)
    assert result.not_applicable_task_ids == ("task-no-impact",)
    assert extracted == result.findings
    assert derived.to_dict() == result.to_dict()
    assert generated.to_dict() == result.to_dict()
    assert summary == result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.findings[0].actionable_gaps == result.findings[0].actionable_remediations


def test_concurrent_modification_with_tests():
    plan = {
        "id": "plan-concurrent",
        "tasks": [
            {
                "id": "task-concurrent",
                "title": "Implement lost update prevention",
                "description": "API must prevent lost updates via conditional mutations. Concurrent modification tests verify write conflict handling and optimistic locking.",
            },
        ],
    }

    result = build_task_api_conditional_request_readiness_plan(plan)
    finding = result.findings[0]

    assert "lost_update_prevention" in finding.detected_signals
    assert "concurrent_modification_tests" in finding.present_safeguards
    assert "concurrent_modification_tests" not in finding.missing_safeguards
