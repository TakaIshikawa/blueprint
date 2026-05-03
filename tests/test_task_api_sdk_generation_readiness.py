import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_sdk_generation_readiness import (
    TaskApiSdkGenerationReadinessFinding,
    TaskApiSdkGenerationReadinessPlan,
    analyze_task_api_sdk_generation_readiness,
    build_task_api_sdk_generation_readiness_plan,
    extract_task_api_sdk_generation_readiness,
    generate_task_api_sdk_generation_readiness,
    recommend_task_api_sdk_generation_readiness,
    summarize_task_api_sdk_generation_readiness,
    task_api_sdk_generation_readiness_plan_to_dict,
    task_api_sdk_generation_readiness_plan_to_dicts,
)


def test_ready_sdk_generation_task_has_no_recommended_checks():
    result = analyze_task_api_sdk_generation_readiness(
        _plan(
            [
                _task(
                    "task-sdk-gen",
                    title="Generate Python and JavaScript SDKs",
                    description=(
                        "Generate SDKs for Python and JavaScript using OpenAPI Generator. "
                        "Configure generator with custom templates and validate OpenAPI spec completeness. "
                        "Align SDK versions with API semantic versioning strategy. "
                        "Set up publishing workflow for PyPI and npm with automated releases. "
                        "Generate SDK documentation with PyDoc and JSDoc, including quickstart guides. "
                        "Implement authentication helpers for API keys and OAuth flows. "
                        "Add retry logic with exponential backoff and jitter. "
                        "Include example code for common integration patterns."
                    ),
                    files_or_modules=["sdk/generator/config.yaml", "sdk/templates/python/", "sdk/publish_workflow.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskApiSdkGenerationReadinessPlan)
    assert result.plan_id == "plan-sdk-gen"
    assert result.impacted_task_ids == ("task-sdk-gen",)
    finding = result.findings[0]
    assert isinstance(finding, TaskApiSdkGenerationReadinessFinding)
    assert "sdk_generation" in finding.detected_signals
    assert "language_targets" in finding.detected_signals
    assert "generator_tooling" in finding.detected_signals
    assert finding.present_safeguards == (
        "generator_configuration",
        "openapi_spec_completeness",
        "version_alignment",
        "publishing_workflow",
        "documentation_quality",
        "authentication_helpers",
        "retry_implementation",
        "example_code_coverage",
    )
    assert finding.missing_safeguards == ()
    assert finding.recommended_checks == ()
    assert finding.readiness == "ready"
    assert "files_or_modules: sdk/generator/config.yaml" in finding.evidence
    assert result.summary["impacted_task_count"] == 1
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "ready": 1}


def test_partial_sdk_generation_task_reports_specific_recommended_checks():
    result = build_task_api_sdk_generation_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial-sdk",
                    title="Generate Python SDK",
                    description=(
                        "Generate Python SDK using OpenAPI Generator. "
                        "Publish to PyPI with semantic versioning."
                    ),
                    files_or_modules=["sdk/generator/python_config.yaml"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-partial-sdk"
    assert "sdk_generation" in finding.detected_signals
    assert "language_targets" in finding.detected_signals
    assert "version_alignment" in finding.present_safeguards
    assert "publishing_workflow" in finding.present_safeguards
    assert "openapi_spec_completeness" in finding.missing_safeguards
    assert "documentation_quality" in finding.missing_safeguards
    assert "authentication_helpers" in finding.missing_safeguards
    assert "retry_implementation" in finding.missing_safeguards
    assert "example_code_coverage" in finding.missing_safeguards
    assert finding.readiness in ("partial", "weak")
    assert len(finding.recommended_checks) >= 4
    assert any("openapi spec" in check.lower() for check in finding.recommended_checks)


def test_weak_sdk_generation_task_has_multiple_recommended_checks():
    result = build_task_api_sdk_generation_readiness_plan(
        _plan(
            [
                _task(
                    "task-weak-sdk",
                    title="Setup SDK generation",
                    description="Initialize SDK generation infrastructure",
                    files_or_modules=["sdk/README.md"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.readiness == "weak"
    assert len(finding.missing_safeguards) >= 5
    assert len(finding.recommended_checks) >= 5


def test_not_applicable_tasks_excluded_from_findings():
    result = build_task_api_sdk_generation_readiness_plan(
        _plan(
            [
                _task("task-sdk", title="Generate SDKs", description="Generate Python SDK"),
                _task("task-ui", title="Update UI", description="Fix button alignment"),
                _task("task-db", title="Database migration", description="Add user table"),
            ]
        )
    )

    assert len(result.findings) == 1
    assert result.findings[0].task_id == "task-sdk"
    assert len(result.not_applicable_task_ids) == 2
    assert "task-ui" in result.not_applicable_task_ids
    assert "task-db" in result.not_applicable_task_ids
    assert result.summary["impacted_task_count"] == 1
    assert result.summary["not_applicable_task_count"] == 2
    assert result.summary["total_task_count"] == 3


def test_explicitly_no_sdk_generation_impact_excluded():
    result = build_task_api_sdk_generation_readiness_plan(
        _plan(
            [
                _task(
                    "task-no-sdk",
                    title="Internal API implementation",
                    description="This implementation does not involve SDK generation or client library distribution changes.",
                )
            ]
        )
    )

    assert result.findings == () or (len(result.findings) == 1 and result.findings[0].readiness == "weak")
    if result.findings == ():
        assert result.not_applicable_task_ids == ("task-no-sdk",)


def test_serialization_and_deserialization():
    plan = _plan(
        [
            _task(
                "task-sdk",
                title="Generate multi-language SDKs",
                description="Generate Python and JavaScript SDKs with OpenAPI Generator",
                files_or_modules=["sdk/config.yaml"],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_api_sdk_generation_readiness_plan(plan)
    payload = task_api_sdk_generation_readiness_plan_to_dict(result)
    dicts = task_api_sdk_generation_readiness_plan_to_dicts(result)

    assert plan == original
    assert analyze_task_api_sdk_generation_readiness(plan).to_dict() == result.to_dict()
    assert extract_task_api_sdk_generation_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_api_sdk_generation_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_api_sdk_generation_readiness(plan).to_dict() == result.to_dict()
    assert summarize_task_api_sdk_generation_readiness(plan).to_dict() == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert dicts == payload["findings"]
    assert task_api_sdk_generation_readiness_plan_to_dicts(result.findings) == dicts


def test_path_signals_detect_sdk_generation_patterns():
    result = build_task_api_sdk_generation_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="SDK infrastructure",
                    description="Setup SDK generation",
                    files_or_modules=[
                        "sdk/openapi-generator/config.yaml",
                        "sdk/python/setup.py",
                        "sdk/javascript/package.json",
                        "sdk/docs/quickstart.md",
                        "sdk/examples/auth_example.py",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "sdk_generation" in finding.detected_signals
    assert "language_targets" in finding.detected_signals
    assert "generator_tooling" in finding.detected_signals
    assert "sdk_documentation" in finding.detected_signals
    assert any("openapi-generator" in ev for ev in finding.evidence)


def test_summary_aggregates_missing_safeguards():
    result = build_task_api_sdk_generation_readiness_plan(
        _plan(
            [
                _task("task-1", title="SDK gen task 1", description="Generate Python SDK"),
                _task("task-2", title="SDK gen task 2", description="Generate JavaScript SDK"),
            ]
        )
    )

    assert result.summary["total_task_count"] == 2
    assert result.summary["impacted_task_count"] == 2
    assert "missing_safeguard_counts" in result.summary
    assert "most_common_gaps" in result.summary
    assert len(result.summary["most_common_gaps"]) <= 5


def test_execution_plan_model_as_input():
    tasks = [
        ExecutionTask(
            id="task-1",
            title="Generate SDKs",
            description="Generate Python and JavaScript SDKs using OpenAPI Generator",
            files_or_modules=["sdk/config.yaml"],
            acceptance_criteria=["SDKs generated"],
        )
    ]
    plan = ExecutionPlan(
        id="exec-plan-1",
        implementation_brief_id="brief-1",
        tasks=tasks,
        milestones=[],
    )

    result = build_task_api_sdk_generation_readiness_plan(plan)

    assert result.plan_id == "exec-plan-1"
    assert len(result.findings) == 1
    assert result.findings[0].task_id == "task-1"


def test_multi_language_sdk_task_detection():
    result = build_task_api_sdk_generation_readiness_plan(
        _plan(
            [
                _task(
                    "task-multi-lang",
                    title="Multi-language SDK generation",
                    description=(
                        "Generate SDKs for Python, JavaScript, TypeScript, Go, Java, and Ruby. "
                        "Publish to PyPI, npm, RubyGems, and Maven Central."
                    ),
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "language_targets" in finding.detected_signals
    assert "package_distribution" in finding.detected_signals


def _plan(tasks):
    return {"id": "plan-sdk-gen", "tasks": tasks}


def _task(task_id, *, title=None, description=None, files_or_modules=None):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    return task
