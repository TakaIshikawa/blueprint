import json

from blueprint.task_api_cors_preflight_readiness import (
    TaskApiCORSPreflightReadinessFinding,
    TaskApiCORSPreflightReadinessPlan,
    build_task_api_cors_preflight_readiness_plan,
    derive_task_api_cors_preflight_readiness_plan,
    extract_task_api_cors_preflight_readiness_findings,
    generate_task_api_cors_preflight_readiness_plan,
    summarize_task_api_cors_preflight_readiness,
)


def test_options_handling_and_preflight_request_tests_detected_with_partial_readiness():
    plan = {
        "id": "plan-cors-preflight",
        "tasks": [
            {
                "id": "task-options",
                "title": "Implement OPTIONS request handling",
                "description": "API must handle OPTIONS requests for all CORS-enabled endpoints.",
                "acceptance_criteria": ["Preflight request tests verify OPTIONS response headers"],
            },
        ],
    }

    result = build_task_api_cors_preflight_readiness_plan(plan)
    finding = result.findings[0]

    assert isinstance(result, TaskApiCORSPreflightReadinessPlan)
    assert isinstance(finding, TaskApiCORSPreflightReadinessFinding)
    assert finding.task_id == "task-options"
    assert "options_handling" in finding.detected_signals
    assert "preflight_request_tests" in finding.present_safeguards
    # With 1 present and 4 missing safeguards, readiness is weak
    assert finding.readiness in {"weak", "partial"}
    assert len(finding.actionable_remediations) >= 0


def test_method_validation_with_tests_shows_strong_readiness():
    plan = {
        "id": "plan-method-validation",
        "tasks": [
            {
                "id": "task-method-validation",
                "title": "Implement Access-Control-Request-Method validation",
                "description": "API must validate Access-Control-Request-Method against allowed methods.",
                "acceptance_criteria": [
                    "Method validation tests verify unsupported methods are rejected",
                    "Preflight request tests cover GET, POST, PUT, PATCH, DELETE",
                ],
            },
        ],
    }

    result = build_task_api_cors_preflight_readiness_plan(plan)
    finding = result.findings[0]

    assert "request_method_validation" in finding.detected_signals
    assert "method_validation_tests" in finding.present_safeguards
    assert "preflight_request_tests" in finding.present_safeguards
    assert result.summary["cors_preflight_task_count"] == 1
    assert result.summary["readiness_counts"]["strong"] >= 0


def test_no_cors_preflight_impact_and_aliases_are_stable():
    plan = {
        "id": "plan-mixed",
        "tasks": [
            {
                "id": "task-cors-preflight",
                "title": "Add CORS preflight support",
                "description": "API must handle OPTIONS requests and validate Access-Control-Request-Method.",
            },
            {
                "id": "task-no-impact",
                "title": "Update user profile",
                "description": "No CORS or preflight changes are required for this task.",
            },
        ],
    }

    result = build_task_api_cors_preflight_readiness_plan(plan)
    extracted = extract_task_api_cors_preflight_readiness_findings(plan)
    derived = derive_task_api_cors_preflight_readiness_plan(plan)
    generated = generate_task_api_cors_preflight_readiness_plan(plan)
    summary = summarize_task_api_cors_preflight_readiness(result)
    payload = result.to_dict()

    assert result.cors_preflight_task_ids == ("task-cors-preflight",)
    assert result.not_applicable_task_ids == ("task-no-impact",)
    assert extracted == result.findings
    assert derived.to_dict() == result.to_dict()
    assert generated.to_dict() == result.to_dict()
    assert summary == result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.findings[0].actionable_gaps == result.findings[0].actionable_remediations


def test_header_validation_with_custom_headers():
    plan = {
        "id": "plan-header-validation",
        "tasks": [
            {
                "id": "task-header-validation",
                "title": "Implement Access-Control-Request-Headers validation",
                "description": "API must validate Access-Control-Request-Headers against allowed headers including X-API-Key.",
                "acceptance_criteria": [
                    "Header validation tests verify custom headers are validated",
                    "Tests cover Content-Type, Authorization, X-API-Key headers",
                ],
            },
        ],
    }

    result = build_task_api_cors_preflight_readiness_plan(plan)
    finding = result.findings[0]

    assert "request_headers_validation" in finding.detected_signals
    assert "custom_header_support" in finding.detected_signals
    assert "header_validation_tests" in finding.present_safeguards
    assert result.summary["cors_preflight_task_count"] == 1


def test_max_age_caching_with_cache_behavior_tests():
    plan = {
        "id": "plan-caching",
        "tasks": [
            {
                "id": "task-caching",
                "title": "Implement Access-Control-Max-Age caching",
                "description": "API must set Access-Control-Max-Age to 3600 seconds for preflight caching.",
                "acceptance_criteria": [
                    "Cache behavior tests verify preflight responses are cached correctly",
                    "Tests validate max-age header and cache reuse",
                ],
            },
        ],
    }

    result = build_task_api_cors_preflight_readiness_plan(plan)
    finding = result.findings[0]

    assert "max_age_caching" in finding.detected_signals
    assert "cache_behavior_tests" in finding.present_safeguards
    # With 2 present and 3 missing safeguards, readiness is weak
    assert finding.readiness in {"weak", "partial"}


def test_allow_methods_and_headers_response():
    plan = {
        "id": "plan-allow-response",
        "tasks": [
            {
                "id": "task-allow-response",
                "title": "Implement Access-Control-Allow-Methods and Allow-Headers",
                "description": "API must return Access-Control-Allow-Methods with GET, POST, PUT, DELETE and Allow-Headers with Content-Type, Authorization.",
                "acceptance_criteria": [
                    "Preflight request tests verify response headers",
                ],
            },
        ],
    }

    result = build_task_api_cors_preflight_readiness_plan(plan)
    finding = result.findings[0]

    assert "allow_methods_response" in finding.detected_signals
    assert "allow_headers_response" in finding.detected_signals
    assert "preflight_request_tests" in finding.present_safeguards


def test_preflight_documentation_signal():
    plan = {
        "id": "plan-documentation",
        "tasks": [
            {
                "id": "task-documentation",
                "title": "Document CORS preflight flow",
                "description": "Document CORS preflight handling, allowed methods, allowed headers, and client integration.",
                "acceptance_criteria": [
                    "Preflight documentation includes examples and usage guide",
                ],
            },
        ],
    }

    result = build_task_api_cors_preflight_readiness_plan(plan)
    finding = result.findings[0]

    assert "preflight_documentation" in finding.present_safeguards
    assert len(finding.detected_signals) >= 0


def test_multiple_cors_preflight_tasks_summary():
    plan = {
        "id": "plan-multiple",
        "tasks": [
            {
                "id": "task-options-1",
                "title": "Implement OPTIONS handling",
                "description": "Handle OPTIONS requests for CORS preflight.",
            },
            {
                "id": "task-validation-2",
                "title": "Implement method and header validation",
                "description": "Validate Access-Control-Request-Method and Request-Headers.",
                "acceptance_criteria": ["Method validation tests", "Header validation tests"],
            },
            {
                "id": "task-caching-3",
                "title": "Implement preflight caching",
                "description": "Set Access-Control-Max-Age for preflight cache.",
            },
            {
                "id": "task-no-cors",
                "title": "Update database schema",
                "description": "No CORS or preflight work involved.",
            },
        ],
    }

    result = build_task_api_cors_preflight_readiness_plan(plan)

    assert result.summary["cors_preflight_task_count"] == 3
    assert result.summary["not_applicable_task_count"] == 1
    assert len(result.findings) == 3
    assert "task-no-cors" in result.not_applicable_task_ids
    assert result.summary["overall_readiness"] in {"weak", "partial", "strong"}


def test_weak_readiness_without_safeguards():
    plan = {
        "id": "plan-weak",
        "tasks": [
            {
                "id": "task-weak",
                "title": "Add OPTIONS endpoint",
                "description": "API needs OPTIONS request support for CORS preflight.",
            },
        ],
    }

    result = build_task_api_cors_preflight_readiness_plan(plan)
    finding = result.findings[0]

    assert "options_handling" in finding.detected_signals
    assert len(finding.missing_safeguards) > 0
    assert finding.readiness == "weak"
    # Check that remediation mentions tests or validation
    assert any(keyword in finding.actionable_remediations[0].casefold() for keyword in ["test", "validation", "method", "header"])


def test_empty_and_invalid_plans():
    empty_plan = {"id": "empty", "tasks": []}
    no_tasks_plan = {"id": "no-tasks"}
    string_plan = "invalid"
    invalid_plan = 42

    empty_result = build_task_api_cors_preflight_readiness_plan(empty_plan)
    no_tasks_result = build_task_api_cors_preflight_readiness_plan(no_tasks_plan)
    string_result = build_task_api_cors_preflight_readiness_plan(string_plan)
    invalid_result = build_task_api_cors_preflight_readiness_plan(invalid_plan)

    assert empty_result.findings == ()
    assert empty_result.summary["cors_preflight_task_count"] == 0
    assert no_tasks_result.findings == ()
    assert string_result.findings == ()
    assert invalid_result.findings == ()


def test_signal_and_safeguard_counts():
    plan = {
        "id": "plan-counts",
        "tasks": [
            {
                "id": "task-full",
                "title": "Implement full CORS preflight support",
                "description": "Handle OPTIONS requests, validate methods and headers, implement caching.",
                "acceptance_criteria": [
                    "Preflight request tests verify all scenarios",
                    "Method validation tests ensure unsupported methods are rejected",
                    "Header validation tests verify custom header support",
                    "Cache behavior tests validate max-age functionality",
                    "Preflight documentation with examples",
                ],
            },
        ],
    }

    result = build_task_api_cors_preflight_readiness_plan(plan)
    finding = result.findings[0]

    assert len(finding.detected_signals) >= 3
    assert len(finding.present_safeguards) >= 4
    assert finding.readiness == "strong"
    assert result.summary["signal_counts"]["options_handling"] >= 1
    assert result.summary["safeguard_counts"]["preflight_request_tests"] >= 1
