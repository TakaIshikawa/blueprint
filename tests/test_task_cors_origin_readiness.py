import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_cors_origin_readiness import (
    TaskCORSOriginReadinessPlan,
    TaskCORSOriginReadinessRecord,
    analyze_task_cors_origin_readiness,
    build_task_cors_origin_readiness_plan,
    derive_task_cors_origin_readiness,
    generate_task_cors_origin_readiness,
    recommend_task_cors_origin_readiness,
    summarize_task_cors_origin_readiness,
    task_cors_origin_readiness_plan_to_dict,
    task_cors_origin_readiness_plan_to_dicts,
    task_cors_origin_readiness_plan_to_markdown,
    task_cors_origin_readiness_to_dicts,
)


def test_weak_cors_origin_task_sorts_first_and_separates_no_impact_ids():
    result = build_task_cors_origin_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Add browser client CORS headers",
                    description="Configure CORS for browser clients making cross-origin API calls.",
                    acceptance_criteria=[
                        "Trusted origin allowlist configured for production.",
                        "Preflight headers configured with allowed methods and headers.",
                        "Browser regression tests cover CORS headers and cross-origin requests.",
                    ],
                ),
                _task(
                    "task-copy",
                    title="Polish settings copy",
                    description="Update labels in the admin settings page.",
                ),
                _task(
                    "task-weak",
                    title="Enable CORS for SPA",
                    description="Enable cross-origin requests for single page app with credentialed requests.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskCORSOriginReadinessPlan)
    assert result.cors_task_ids == ("task-weak", "task-partial")
    assert result.impacted_task_ids == result.cors_task_ids
    assert result.no_impact_task_ids == ("task-copy",)
    assert [record.task_id for record in result.records] == ["task-weak", "task-partial"]
    weak = result.records[0]
    assert isinstance(weak, TaskCORSOriginReadinessRecord)
    assert weak.readiness == "weak"
    assert weak.impact == "high"
    assert {"browser_client", "cross_origin_api", "credentialed_request"} <= set(weak.detected_signals)
    assert "trusted_origin_allowlist" in weak.missing_safeguards
    assert "credentials_policy" in weak.missing_safeguards


def test_strong_readiness_reflects_all_safeguards_and_summary_counts():
    result = analyze_task_cors_origin_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Add CORS middleware for browser clients",
                    description=(
                        "Browser clients make cross-origin API calls with credentialed requests. "
                        "Support preflight headers and wildcard origin blocking. "
                        "Configure environment-specific origins for dev, staging, and production."
                    ),
                    acceptance_criteria=[
                        "Trusted origin allowlist configured with explicit allowed origins.",
                        "Credentials policy defines SameSite and cookie behavior.",
                        "Preflight headers handle OPTIONS requests with allowed methods and headers.",
                        "Wildcard blocking rejects wildcard origins on browser surfaces.",
                        "Environment coverage maintains dev, staging, and production origin lists.",
                        "Browser regression tests cover CORS, preflight, and credentials handling.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "strong"
    assert record.impact == "medium"
    assert record.present_safeguards == (
        "trusted_origin_allowlist",
        "credentials_policy",
        "preflight_headers",
        "wildcard_blocking",
        "environment_coverage",
        "browser_regression_tests",
    )
    assert record.missing_safeguards == ()
    assert record.recommended_checks == ()
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "strong": 1}
    assert result.summary["impact_counts"] == {"high": 0, "medium": 1, "low": 0}
    assert result.summary["signal_counts"]["browser_client"] == 1
    assert result.summary["present_safeguard_counts"]["browser_regression_tests"] == 1


def test_mapping_input_collects_title_description_paths_and_validation_command_evidence():
    result = build_task_cors_origin_readiness_plan(
        {
            "id": "task-mapping",
            "title": "Add CORS configuration for frontend",
            "description": "Configure trusted origins for browser clients with credentialed requests and preflight support.",
            "files_or_modules": [
                "src/api/cors_middleware.py",
                "src/api/trusted_origin_allowlist.py",
                "tests/test_browser_regression.py",
            ],
            "validation_commands": {
                "tests": [
                    "poetry run pytest tests/test_cors_headers.py",
                    "poetry run pytest tests/test_browser_regression.py",
                ]
            },
        }
    )

    record = result.records[0]
    assert {"browser_client", "cross_origin_api", "trusted_origin", "credentialed_request", "preflight_headers"} <= set(
        record.detected_signals
    )
    assert {"trusted_origin_allowlist", "browser_regression_tests"} <= set(record.present_safeguards)
    assert any(item == "title: Add CORS configuration for frontend" for item in record.evidence)
    assert any("description: Configure trusted origins" in item for item in record.evidence)
    assert any(item == "files_or_modules: src/api/trusted_origin_allowlist.py" for item in record.evidence)
    assert any("validation_commands: poetry run pytest tests/test_browser_regression.py" in item for item in record.evidence)


def test_execution_plan_execution_task_and_object_inputs_are_supported_without_mutation():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add CORS headers for SPA",
        description="Enable browser client cross-origin requests with trusted origins.",
        acceptance_criteria=["Browser regression tests cover CORS behavior."],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Configure environment-specific origins",
            description="Maintain dev, staging, and production origin lists.",
            acceptance_criteria=["Environment coverage includes all deployment targets."],
        )
    )
    source = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-covered",
                title="Add CORS middleware",
                description="Browser client cross-origin API with credentialed requests.",
                acceptance_criteria=[
                    "Trusted origin allowlist configured.",
                    "Credentials policy defined.",
                    "Preflight headers configured.",
                    "Wildcard blocking enabled.",
                    "Environment coverage complete.",
                    "Browser regression tests passing.",
                ],
            ),
        ],
        plan_id="plan-cors-objects",
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    result = summarize_task_cors_origin_readiness(source)

    assert source == original
    assert build_task_cors_origin_readiness_plan(object_task).records[0].task_id == "task-object"
    assert generate_task_cors_origin_readiness(model).plan_id == "plan-cors-objects"
    assert derive_task_cors_origin_readiness(source).to_dict() == result.to_dict()
    assert recommend_task_cors_origin_readiness(source).to_dict() == result.to_dict()
    assert result.records == result.findings
    assert result.records == result.recommendations


def test_empty_state_markdown_and_summary_are_stable():
    result = build_task_cors_origin_readiness_plan(
        _plan([_task("task-ui", title="Polish dashboard", description="Adjust table spacing.")])
    )

    assert result.records == ()
    assert result.cors_task_ids == ()
    assert result.no_impact_task_ids == ("task-ui",)
    assert result.summary["cors_task_count"] == 0
    assert result.to_markdown() == "\n".join(
        [
            "# Task CORS Origin Readiness: plan-cors-origin",
            "",
            "## Summary",
            "",
            "- Task count: 1",
            "- CORS task count: 0",
            "- Missing safeguard count: 0",
            "- Readiness counts: weak 0, partial 0, strong 0",
            "- Impact counts: high 0, medium 0, low 0",
            "",
            "No task CORS origin readiness records were inferred.",
            "",
            "No-impact tasks: task-ui",
        ]
    )


def test_serialization_aliases_to_dict_output_and_markdown_are_json_safe():
    result = build_task_cors_origin_readiness_plan(
        _plan(
            [
                _task(
                    "task-cors",
                    title="Add CORS for browser clients",
                    description="Browser clients make cross-origin API calls with trusted origins and preflight support.",
                    acceptance_criteria=[
                        "Trusted origin allowlist configured.",
                        "Preflight headers configured.",
                        "Browser regression tests passing.",
                    ],
                )
            ],
            plan_id="plan-serialization",
        )
    )
    payload = task_cors_origin_readiness_plan_to_dict(result)
    markdown = task_cors_origin_readiness_plan_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_cors_origin_readiness_plan_to_dicts(result) == payload["records"]
    assert task_cors_origin_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_cors_origin_readiness_to_dicts(result) == payload["records"]
    assert task_cors_origin_readiness_to_dicts(result.records) == payload["records"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task CORS Origin Readiness: plan-serialization")
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "cors_task_ids",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness",
        "impact",
        "recommended_checks",
        "evidence",
    ]


def test_wildcard_origin_signal_detection_and_impact_assessment():
    result = build_task_cors_origin_readiness_plan(
        _plan(
            [
                _task(
                    "task-wildcard",
                    title="Remove wildcard CORS origins",
                    description="Block wildcard origins on browser-facing API endpoints.",
                    acceptance_criteria=[
                        "Wildcard blocking prevents Access-Control-Allow-Origin: *.",
                        "Trusted origin allowlist replaces wildcard behavior.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "wildcard_origin" in record.detected_signals
    assert "wildcard_blocking" in record.present_safeguards
    assert "trusted_origin_allowlist" in record.present_safeguards
    # Only 2 safeguards present, needs 3+ for partial
    assert record.readiness == "weak"


def test_credentialed_request_signal_and_credentials_policy_safeguard():
    result = build_task_cors_origin_readiness_plan(
        _plan(
            [
                _task(
                    "task-credentials",
                    title="Add CORS credentials support",
                    description="Support credentialed requests with cookies and authorization headers.",
                    acceptance_criteria=[
                        "Credentials policy defines SameSite and cookie behavior.",
                        "Access-Control-Allow-Credentials policy configured.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "credentialed_request" in record.detected_signals
    assert "credentials_policy" in record.present_safeguards


def test_environment_origin_signal_and_environment_coverage_safeguard():
    result = build_task_cors_origin_readiness_plan(
        _plan(
            [
                _task(
                    "task-env",
                    title="Configure environment-specific CORS origins",
                    description="Maintain dev, staging, and production origin lists for localhost, staging.example.com, and example.com.",
                    acceptance_criteria=[
                        "Environment coverage includes dev, staging, and production origins.",
                        "Per-environment origin configuration maintained.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "environment_origin" in record.detected_signals
    assert "environment_coverage" in record.present_safeguards


def test_preflight_headers_signal_and_safeguard_detection():
    result = build_task_cors_origin_readiness_plan(
        _plan(
            [
                _task(
                    "task-preflight",
                    title="Add OPTIONS preflight handling",
                    description="Handle preflight requests with allowed methods and custom headers.",
                    acceptance_criteria=[
                        "Preflight headers include Access-Control-Allow-Methods and Access-Control-Allow-Headers.",
                        "OPTIONS endpoint configured.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "preflight_headers" in record.detected_signals
    assert "preflight_headers" in record.present_safeguards


def test_out_of_scope_tasks_produce_no_records():
    result = build_task_cors_origin_readiness_plan(
        _plan(
            [
                _task("task-db", title="Optimize database queries", description="Add indexes to user table."),
                _task("task-cache", title="Add Redis cache", description="Cache frequently accessed data."),
                _task("task-logging", title="Improve logging", description="Add structured logging."),
            ]
        )
    )

    assert result.records == ()
    assert result.cors_task_ids == ()
    assert result.no_impact_task_ids == ("task-db", "task-cache", "task-logging")
    assert result.summary["cors_task_count"] == 0


def test_invalid_inputs_produce_empty_results():
    for invalid in [None, "", [], {}, 42, b"bytes"]:
        result = build_task_cors_origin_readiness_plan(invalid)
        assert result.records == ()
        assert result.cors_task_ids == ()
        assert result.summary["cors_task_count"] == 0


def test_path_signal_detection_from_files_or_modules():
    result = build_task_cors_origin_readiness_plan(
        _task(
            "task-files",
            title="Update CORS middleware",
            files_or_modules=[
                "src/middleware/cors_middleware.py",
                "src/config/trusted_origins.py",
                "tests/browser/test_cors.py",
            ],
        )
    )

    record = result.records[0]
    assert {"cross_origin_api", "trusted_origin", "browser_client"} <= set(record.detected_signals)
    assert any("files_or_modules: src/middleware/cors_middleware.py" in item for item in record.evidence)


def test_evidence_extraction_from_task_fields_and_validation_commands():
    result = build_task_cors_origin_readiness_plan(
        _task(
            "task-evidence",
            title="Add CORS for React app",
            description="Configure cross-origin requests for React single page app.",
            acceptance_criteria=[
                "Browser clients can make credentialed requests.",
                "Trusted origin allowlist includes production domains.",
            ],
            validation_commands=["npm run test:browser", "npm run test:cors"],
        )
    )

    record = result.records[0]
    assert any("title: Add CORS for React app" in item for item in record.evidence)
    assert any("description: Configure cross-origin requests" in item for item in record.evidence)
    assert any("acceptance_criteria[0]: Browser clients can make credentialed requests" in item for item in record.evidence)
    assert any("validation_commands: npm run test:cors" in item for item in record.evidence)


def test_metadata_field_signal_detection():
    result = build_task_cors_origin_readiness_plan(
        _task(
            "task-metadata",
            title="Update API",
            metadata={
                "cors_config": {
                    "trusted_origins": ["https://app.example.com"],
                    "credentials": "include",
                    "preflight": True,
                },
                "browser_client": True,
            },
        )
    )

    record = result.records[0]
    # Metadata keys trigger signals for browser_client, trusted_origin, preflight_headers
    # Note: "credentials: include" doesn't match credentialed_request pattern
    assert {"browser_client", "trusted_origin", "preflight_headers"} <= set(record.detected_signals)


def test_recommended_checks_match_missing_safeguards():
    result = build_task_cors_origin_readiness_plan(
        _task(
            "task-recommendations",
            title="Add CORS for browser API",
            description="Enable cross-origin requests for browser clients.",
        )
    )

    record = result.records[0]
    assert len(record.recommended_checks) == len(record.missing_safeguards)
    assert all(check for check in record.recommended_checks)
    assert record.recommended_checks == record.recommendations
    assert record.recommended_checks == record.recommended_actions


def test_signal_and_safeguard_ordering_is_deterministic():
    result = build_task_cors_origin_readiness_plan(
        _task(
            "task-order",
            title="CORS configuration",
            description=(
                "Environment-specific origins, wildcard blocking, preflight headers, "
                "credentialed requests, trusted origins, cross-origin API, browser client."
            ),
            acceptance_criteria=[
                "Environment coverage, wildcard blocking, preflight headers, "
                "credentials policy, trusted origin allowlist, browser regression tests."
            ],
        )
    )

    record = result.records[0]
    # Signals should be in _SIGNAL_ORDER
    # Note: wildcard_origin signal requires more specific wildcard origin mentions
    expected_signals = (
        "browser_client",
        "cross_origin_api",
        "trusted_origin",
        "credentialed_request",
        "preflight_headers",
        "environment_origin",
    )
    assert record.detected_signals == expected_signals

    # Safeguards should be in _SAFEGUARD_ORDER
    expected_safeguards = (
        "trusted_origin_allowlist",
        "credentials_policy",
        "preflight_headers",
        "wildcard_blocking",
        "environment_coverage",
        "browser_regression_tests",
    )
    assert record.present_safeguards == expected_safeguards


def test_summary_counts_match_records():
    result = build_task_cors_origin_readiness_plan(
        _plan(
            [
                _task("weak-1", title="Add CORS", description="Browser client cross-origin API."),
                _task(
                    "partial-1",
                    title="CORS middleware",
                    description="Browser client with trusted origins.",
                    acceptance_criteria=["Trusted origin allowlist.", "Preflight headers.", "Browser tests."],
                ),
                _task(
                    "strong-1",
                    title="Full CORS",
                    description="Browser client cross-origin API.",
                    acceptance_criteria=[
                        "Trusted origin allowlist.",
                        "Credentials policy.",
                        "Preflight headers.",
                        "Wildcard blocking.",
                        "Environment coverage.",
                        "Browser regression tests.",
                    ],
                ),
                _task("no-impact", title="Database migration", description="Add column."),
            ]
        )
    )

    assert result.summary["task_count"] == 4
    assert result.summary["cors_task_count"] == 3
    assert result.summary["no_impact_task_count"] == 1
    assert result.summary["readiness_counts"]["weak"] == 1
    assert result.summary["readiness_counts"]["partial"] == 1
    assert result.summary["readiness_counts"]["strong"] == 1
    assert result.summary["signal_counts"]["browser_client"] == 3
    # Both partial and strong tasks have browser regression tests
    assert result.summary["present_safeguard_counts"]["browser_regression_tests"] == 2


def _plan(tasks, *, plan_id="plan-cors-origin"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-cors-origin",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    validation_commands=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    if validation_commands is not None:
        task["validation_commands"] = validation_commands
    return task
