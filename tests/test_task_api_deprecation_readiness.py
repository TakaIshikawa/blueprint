import copy
import json

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord
from blueprint.domain.models import ExecutionPlan
from blueprint.task_api_deprecation_readiness import (
    TaskApiDeprecationReadinessPlan,
    analyze_task_api_deprecation_readiness,
    build_task_api_deprecation_readiness_plan,
    recommend_task_api_deprecation_readiness,
    summarize_task_api_deprecation_readiness,
    summarize_task_api_deprecation_readiness_plan,
    task_api_deprecation_readiness_plan_to_dict,
    task_api_deprecation_readiness_plan_to_dicts,
    task_api_deprecation_readiness_plan_to_markdown,
)


def test_complete_api_deprecation_task_is_ready_with_all_requirements():
    result = build_task_api_deprecation_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Deprecate legacy user endpoint and schema field",
                    description="API deprecation retires the legacy endpoint and deprecated field.",
                    acceptance_criteria=[
                        "Replacement endpoint and field mapping are documented in the migration guide.",
                        "Client communication includes developer notice, changelog, and release notes.",
                        "Sunset timeline sets the deprecation date, deadline, and final removal date.",
                        "Compatibility behavior keeps dual support with warning headers during the grace period.",
                        "Telemetry dashboard tracks remaining traffic, client adoption, and error rate.",
                        "Removal gate requires zero traffic and migration complete approval.",
                    ],
                    files_or_modules=["openapi/deprecations/user_endpoint.yaml"],
                ),
                _task("task-ui", title="Update UI copy", description="Refresh button label."),
            ]
        )
    )

    assert isinstance(result, TaskApiDeprecationReadinessPlan)
    assert isinstance(result, SimpleReadinessPlan)
    assert result.impacted_task_ids == ("task-ready",)
    assert result.ignored_task_ids == ("task-ui",)
    record = result.records[0]
    assert isinstance(record, SimpleReadinessRecord)
    assert record.detected_signals == ("api_deprecation", "deprecated_endpoint", "deprecated_field")
    assert record.present_criteria == (
        "replacement_path",
        "client_communication",
        "sunset_timeline",
        "compatibility_behavior",
        "telemetry",
        "removal_gate",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_partial_api_deprecation_reports_missing_actionable_gaps():
    result = analyze_task_api_deprecation_readiness(
        [
            _task(
                "task-partial",
                title="Sunset legacy route",
                description="Endpoint sunset has an owner and usage inventory.",
            )
        ]
    )

    record = result.records[0]
    assert record.readiness == "needs_planning"
    assert record.present_criteria == ()
    assert record.missing_criteria == (
        "replacement_path",
        "client_communication",
        "sunset_timeline",
        "compatibility_behavior",
        "telemetry",
        "removal_gate",
    )
    assert record.recommended_follow_up_actions == (
        "Document the replacement endpoint, field, route, SDK, or migration mapping clients should use.",
        "Prepare client, customer, developer, partner, changelog, release-note, or email communication.",
        "Set the sunset, deprecation, EOL, deadline, notice period, or final removal timeline.",
        "Define compatibility behavior such as dual support, legacy mode, shim, fallback, or warning headers.",
        "Track telemetry for usage, remaining traffic, client adoption, errors, dashboards, logs, or alerts.",
        "Define the removal gate such as zero traffic, migration complete, adoption threshold, or final approval.",
    )


def test_endpoint_route_openapi_sdk_and_schema_path_hints_contribute_evidence_without_mutation():
    source = _plan(
        [
            _task(
                "task-paths",
                title="Retire SDK schema property",
                description="Remove field after API clients migrate.",
                files_or_modules=[
                    "src/routes/deprecated_user_route.py",
                    "openapi/user_sunset.yaml",
                    "sdk/schema_field_removal.md",
                ],
                metadata={
                    "migration": {
                        "replacement": "Replacement field mapping points clients to profile.displayName.",
                        "compatibility": "Legacy mode remains backward compatible until the grace period ends.",
                    },
                    "observability": "Usage tracking dashboard monitors remaining traffic.",
                },
            )
        ]
    )
    original = copy.deepcopy(source)

    result = build_task_api_deprecation_readiness_plan(ExecutionPlan.model_validate(source))

    assert source == original
    record = result.records[0]
    assert record.detected_signals == ("api_deprecation", "deprecated_endpoint", "deprecated_field")
    assert record.present_criteria == (
        "replacement_path",
        "sunset_timeline",
        "compatibility_behavior",
        "telemetry",
    )
    assert record.missing_criteria == ("client_communication", "removal_gate")
    assert any("metadata.migration.replacement" in item for item in record.evidence)
    assert any("files_or_modules: src/routes/deprecated_user_route.py" in item for item in record.evidence)
    assert any("files_or_modules: openapi/user_sunset.yaml" in item for item in record.evidence)
    assert any("files_or_modules: sdk/schema_field_removal.md" in item for item in record.evidence)


def test_no_impact_tasks_are_excluded_and_serialization_aliases_are_stable():
    result = summarize_task_api_deprecation_readiness(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Refresh API docs introduction",
                    description="No API endpoint deprecation, sunset, or removal impact is planned.",
                ),
                _task(
                    "task-partial",
                    title="Deprecate webhook field",
                    description="Deprecated field removal has client communication and telemetry.",
                ),
            ],
            plan_id="plan-api-deprecation-sort",
        )
    )

    payload = task_api_deprecation_readiness_plan_to_dict(result)
    markdown = task_api_deprecation_readiness_plan_to_markdown(result)

    assert [record.task_id for record in result.records] == ["task-partial"]
    assert result.ignored_task_ids == ("task-copy",)
    assert analyze_task_api_deprecation_readiness(result) is result
    assert summarize_task_api_deprecation_readiness_plan(result) is result
    assert recommend_task_api_deprecation_readiness(result) == result.records
    assert result.to_dicts() == payload["records"]
    assert task_api_deprecation_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-api-deprecation-sort"
    assert markdown.startswith("# Task API Deprecation Readiness: plan-api-deprecation-sort")
    assert "| Task | Title | Readiness |" in markdown


def test_invalid_inputs_return_empty_simple_plan():
    assert build_task_api_deprecation_readiness_plan(42).records == ()
    assert build_task_api_deprecation_readiness_plan({"id": "bad", "tasks": "not a list"}).records == ()
    assert build_task_api_deprecation_readiness_plan({"tasks": []}).summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-api-deprecation"):
    return {"id": plan_id, "implementation_brief_id": "brief-api-deprecation", "milestones": [], "tasks": tasks}


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
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
    return task
