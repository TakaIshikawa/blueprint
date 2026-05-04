import json
from types import SimpleNamespace

from blueprint.task_api_cache_header_readiness import (
    TaskApiCacheHeaderReadinessFinding,
    TaskApiCacheHeaderReadinessPlan,
    build_task_api_cache_header_readiness_plan,
    derive_task_api_cache_header_readiness_plan,
    extract_task_api_cache_header_readiness_findings,
    generate_task_api_cache_header_readiness_plan,
    summarize_task_api_cache_header_readiness,
)


def test_cache_control_header_detected_with_partial_readiness():
    plan = {
        "id": "plan-cache-control",
        "tasks": [
            {
                "id": "task-cache-control",
                "title": "Add Cache-Control headers to API responses",
                "description": "API responses must include Cache-Control headers with appropriate max-age directives.",
                "acceptance_criteria": ["Cache-Control headers are set on all GET endpoints"],
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    finding = result.findings[0]

    assert isinstance(result, TaskApiCacheHeaderReadinessPlan)
    assert isinstance(finding, TaskApiCacheHeaderReadinessFinding)
    assert finding.task_id == "task-cache-control"
    assert "cache_control_header" in finding.detected_signals
    assert "cache_control_tests" not in finding.present_safeguards
    assert "cache_control_tests" in finding.missing_safeguards
    assert finding.readiness in {"weak", "partial"}
    assert len(finding.actionable_remediations) > 0
    assert "cache-control" in finding.actionable_remediations[0].casefold()


def test_private_no_store_required_for_authenticated_endpoints():
    plan = {
        "id": "plan-authenticated",
        "tasks": [
            {
                "id": "task-auth-cache",
                "title": "Add caching to authenticated user profile endpoint",
                "description": "Implement Cache-Control headers for user profile API endpoint. This endpoint returns authenticated user data.",
                "acceptance_criteria": ["Cache headers are configured"],
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    finding = result.findings[0]

    assert "private_no_store_for_sensitive_data" in finding.missing_safeguards
    # Sensitive endpoints without private/no-store should be weak readiness
    assert finding.readiness == "weak"
    assert any("sensitive" in r.casefold() or "private" in r.casefold() for r in finding.actionable_remediations)


def test_strong_readiness_with_all_safeguards():
    plan = {
        "id": "plan-complete",
        "tasks": [
            {
                "id": "task-complete",
                "title": "Implement complete cache header strategy for authenticated API",
                "description": (
                    "Add Cache-Control headers with max-age and private directives for authenticated endpoints. "
                    "Include ETag and Last-Modified headers for conditional requests. "
                    "Support If-None-Match validation and 304 Not Modified responses. "
                    "Configure Vary headers for Accept and Authorization. "
                    "Implement cache invalidation hooks on data updates."
                ),
                "acceptance_criteria": [
                    "Cache control tests verify max-age and private directives for authenticated user data",
                    "Conditional request tests verify If-None-Match handling and 304 response scenarios",
                    "Vary header configuration ensures proper cache keying by Accept and Authorization headers",
                    "Cache invalidation tests verify purge hooks trigger on user profile updates",
                    "Cache documentation includes TTL policy and sensitive data handling guidelines",
                ],
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    finding = result.findings[0]

    assert "cache_control_tests" in finding.present_safeguards
    assert "private_no_store_for_sensitive_data" in finding.present_safeguards
    assert "conditional_request_tests" in finding.present_safeguards
    assert "vary_header_configuration" in finding.present_safeguards
    assert "cache_invalidation_tests" in finding.present_safeguards
    assert "cache_documentation" in finding.present_safeguards
    assert len(finding.missing_safeguards) == 0
    assert finding.readiness == "strong"


def test_weak_readiness_without_safeguards():
    plan = {
        "id": "plan-weak",
        "tasks": [
            {
                "id": "task-weak",
                "title": "Add cache headers",
                "description": "Set Cache-Control max-age on API responses.",
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    finding = result.findings[0]

    assert len(finding.present_safeguards) == 0
    assert len(finding.missing_safeguards) == 6
    assert finding.readiness == "weak"


def test_path_hints_detect_cache_signals():
    plan = {
        "id": "plan-path",
        "tasks": [
            {
                "id": "task-path",
                "title": "Update middleware",
                "description": "Modify response handling",
                "expected_files": [
                    "src/middleware/cache_control.py",
                    "src/middleware/etag_generator.py",
                    "src/api/vary_headers.py",
                ],
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    finding = result.findings[0]

    assert "cache_control_header" in finding.detected_signals or "etag_header" in finding.detected_signals


def test_no_impact_tasks_excluded():
    plan = {
        "id": "plan-mixed",
        "tasks": [
            {
                "id": "task-cache",
                "title": "Add Cache-Control headers",
                "description": "Implement cache control for API responses with max-age directive.",
            },
            {
                "id": "task-no-impact",
                "title": "Update user model",
                "description": "No cache header changes are required for this task.",
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)

    assert result.cache_header_task_ids == ("task-cache",)
    assert result.not_applicable_task_ids == ("task-no-impact",)
    assert len(result.findings) == 1
    assert result.findings[0].task_id == "task-cache"


def test_validation_command_evidence_detected():
    plan = {
        "id": "plan-validation",
        "tasks": [
            {
                "id": "task-validation",
                "title": "Update API caching with Cache-Control headers",
                "description": "Modify response headers to include cache control",
                "validation_commands": [
                    "pytest tests/test_cache_control.py",
                    "pytest tests/test_conditional_requests.py -k test_304_response",
                ],
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)

    # Should detect cache signals from title/description and safeguards from validation commands
    assert len(result.findings) > 0
    finding = result.findings[0]
    assert "cache_control_tests" in finding.present_safeguards or "conditional_request_tests" in finding.present_safeguards


def test_conditional_get_and_304_responses():
    plan = {
        "id": "plan-conditional",
        "tasks": [
            {
                "id": "task-conditional",
                "title": "Implement conditional GET support",
                "description": "API must support If-None-Match headers and return 304 Not Modified when content hasn't changed.",
                "acceptance_criteria": [
                    "If-None-Match validation is implemented",
                    "304 Not Modified responses are returned for matching ETags",
                    "Conditional request tests verify proper behavior",
                ],
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    finding = result.findings[0]

    assert "conditional_get_request" in finding.detected_signals
    assert "not_modified_304_response" in finding.detected_signals
    assert "conditional_request_tests" in finding.present_safeguards


def test_vary_header_configuration():
    plan = {
        "id": "plan-vary",
        "tasks": [
            {
                "id": "task-vary",
                "title": "Configure Vary headers for API caching",
                "description": "Set Vary: Accept-Encoding, Authorization to prevent serving cached responses with different encodings or auth states.",
                "acceptance_criteria": [
                    "Vary header configuration is documented and implemented for Accept and Authorization"
                ],
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    finding = result.findings[0]

    assert "vary_header" in finding.detected_signals
    assert "vary_header_configuration" in finding.present_safeguards


def test_cdn_caching_and_invalidation():
    plan = {
        "id": "plan-cdn",
        "tasks": [
            {
                "id": "task-cdn",
                "title": "Enable CDN caching for static API responses",
                "description": "Configure CloudFront CDN caching with appropriate Cache-Control headers and implement cache purge hooks.",
                "acceptance_criteria": [
                    "CDN cache behavior is configured",
                    "Cache invalidation tests verify purge on content updates",
                ],
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    finding = result.findings[0]

    assert "cdn_caching_behavior" in finding.detected_signals
    assert "cache_invalidation_hooks" in finding.detected_signals
    assert "cache_invalidation_tests" in finding.present_safeguards


def test_summary_counts_are_stable():
    plan = {
        "id": "plan-summary",
        "tasks": [
            {
                "id": "task-1",
                "title": "Add cache headers",
                "description": "Implement Cache-Control with max-age for public API responses. Cache control tests verify behavior.",
            },
            {
                "id": "task-2",
                "title": "Add conditional GET",
                "description": "Support If-None-Match and 304 responses",
            },
            {
                "id": "task-3",
                "title": "Update user settings",
                "description": "No caching changes needed",
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    summary = summarize_task_api_cache_header_readiness(result)

    assert summary["cache_header_task_count"] == 2
    assert summary["not_applicable_task_count"] == 1
    assert "readiness_counts" in summary
    assert "signal_counts" in summary
    assert "safeguard_counts" in summary
    assert summary["overall_readiness"] in {"weak", "partial", "strong"}


def test_dict_list_markdown_helper_aliases():
    plan = {
        "id": "plan-helpers",
        "tasks": [
            {
                "id": "task-helpers",
                "title": "Implement cache headers",
                "description": "Add Cache-Control and ETag headers",
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    payload = result.to_dict()
    dicts = result.to_dicts()
    markdown = result.to_markdown()

    # Verify JSON-compatible serialization
    assert json.loads(json.dumps(payload)) == payload
    assert isinstance(dicts, list)
    assert len(dicts) == 1
    assert isinstance(dicts[0], dict)
    assert isinstance(markdown, str)
    assert "# Task API Cache Header Readiness" in markdown


def test_compatibility_aliases_and_properties():
    plan = {
        "id": "plan-compat",
        "tasks": [
            {
                "id": "task-compat",
                "title": "Add cache control",
                "description": "Set Cache-Control max-age on GET endpoints",
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    derived = derive_task_api_cache_header_readiness_plan(plan)
    generated = generate_task_api_cache_header_readiness_plan(plan)
    extracted = extract_task_api_cache_header_readiness_findings(plan)
    summary = summarize_task_api_cache_header_readiness(result)

    assert derived.to_dict() == result.to_dict()
    assert generated.to_dict() == result.to_dict()
    assert extracted == result.findings
    assert summary == result.summary
    assert result.records == result.findings
    assert result.findings[0].actionable_gaps == result.findings[0].actionable_remediations


def test_no_source_mutation():
    original_plan = {
        "id": "plan-mutation",
        "tasks": [
            {
                "id": "task-mutation",
                "title": "Add cache headers",
                "description": "Implement Cache-Control",
                "acceptance_criteria": ["Cache headers are set"],
            },
        ],
    }
    plan_copy = json.loads(json.dumps(original_plan))

    build_task_api_cache_header_readiness_plan(original_plan)

    assert original_plan == plan_copy


def test_simplenamespace_input_support():
    task = SimpleNamespace(
        id="task-ns",
        title="Add cache control",
        description="Set Cache-Control headers with max-age directive",
        acceptance_criteria=["Cache control tests verify max-age behavior"],
    )
    plan = SimpleNamespace(id="plan-ns", tasks=[task])

    result = build_task_api_cache_header_readiness_plan(plan)

    assert len(result.findings) == 1
    assert result.findings[0].task_id == "task-ns"


def test_execution_task_and_plan_model_support():
    from blueprint.domain.models import ExecutionTask, ExecutionPlan

    task = ExecutionTask(
        id="task-model",
        title="Implement API caching",
        description="Add Cache-Control headers with max-age and private directives",
        acceptance_criteria=["Cache control tests verify headers"],
    )
    plan = ExecutionPlan(
        id="plan-model",
        implementation_brief_id="brief-001",
        milestones=[],
        tasks=[task],
    )

    result = build_task_api_cache_header_readiness_plan(plan)

    assert result.plan_id == "plan-model"
    assert len(result.findings) == 1
    assert result.findings[0].task_id == "task-model"


def test_stable_sorting_across_inputs():
    dict_plan = {
        "id": "plan-sort",
        "tasks": [
            {"id": "task-a", "title": "Add cache headers", "description": "Cache-Control implementation"},
            {"id": "task-b", "title": "Add ETag support", "description": "ETag generation"},
        ],
    }

    ns_plan = SimpleNamespace(
        id="plan-sort",
        tasks=[
            SimpleNamespace(id="task-a", title="Add cache headers", description="Cache-Control implementation"),
            SimpleNamespace(id="task-b", title="Add ETag support", description="ETag generation"),
        ],
    )

    dict_result = build_task_api_cache_header_readiness_plan(dict_plan)
    ns_result = build_task_api_cache_header_readiness_plan(ns_plan)

    dict_ids = [f.task_id for f in dict_result.findings]
    ns_ids = [f.task_id for f in ns_result.findings]

    # Both should have the same tasks identified
    assert set(dict_ids) == set(ns_ids)


def test_evidence_snippets_are_limited():
    plan = {
        "id": "plan-evidence",
        "tasks": [
            {
                "id": "task-evidence",
                "title": "Long description task",
                "description": (
                    "This is a very long description that goes on and on about implementing Cache-Control headers "
                    "with max-age directives and private/public settings for different API endpoints, including "
                    "authenticated endpoints that require no-store or private directives to prevent caching of "
                    "user-specific data and also includes ETag generation for conditional requests and 304 responses"
                ),
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    finding = result.findings[0]

    # Evidence snippets should be truncated
    for evidence in finding.evidence:
        assert len(evidence) <= 120  # Signal name + snippet


def test_max_age_and_no_store_directives():
    plan = {
        "id": "plan-directives",
        "tasks": [
            {
                "id": "task-directives",
                "title": "Configure cache directives",
                "description": "Set max-age for public endpoints and no-store for sensitive data endpoints",
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    finding = result.findings[0]

    assert "max_age_directive" in finding.detected_signals
    assert "no_store_directive" in finding.detected_signals


def test_last_modified_and_etag_headers():
    plan = {
        "id": "plan-validators",
        "tasks": [
            {
                "id": "task-validators",
                "title": "Add validation headers",
                "description": "Include Last-Modified and ETag headers for cache validation",
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    finding = result.findings[0]

    assert "etag_header" in finding.detected_signals
    assert "last_modified_header" in finding.detected_signals


def test_empty_plan_returns_empty_results():
    plan = {"id": "plan-empty", "tasks": []}

    result = build_task_api_cache_header_readiness_plan(plan)

    assert len(result.findings) == 0
    assert len(result.cache_header_task_ids) == 0
    assert len(result.not_applicable_task_ids) == 0
    assert result.summary["cache_header_task_count"] == 0


def test_markdown_output_format():
    plan = {
        "id": "plan-markdown",
        "tasks": [
            {
                "id": "task-md",
                "title": "Add cache | headers",
                "description": "Cache-Control with max-age",
            },
        ],
    }

    result = build_task_api_cache_header_readiness_plan(plan)
    markdown = result.to_markdown()

    # Should escape pipe characters in markdown cells
    assert "\\|" in markdown
    assert "# Task API Cache Header Readiness: plan-markdown" in markdown
    assert "| Task | Title | Readiness |" in markdown
