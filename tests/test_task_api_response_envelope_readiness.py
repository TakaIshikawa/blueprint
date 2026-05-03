import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_response_envelope_readiness import (
    TaskApiResponseEnvelopeReadinessFinding,
    TaskApiResponseEnvelopeReadinessPlan,
    analyze_task_api_response_envelope_readiness,
    build_task_api_response_envelope_readiness_plan,
    extract_task_api_response_envelope_readiness,
    generate_task_api_response_envelope_readiness,
    recommend_task_api_response_envelope_readiness,
    summarize_task_api_response_envelope_readiness,
    task_api_response_envelope_readiness_plan_to_dict,
    task_api_response_envelope_readiness_plan_to_dicts,
)


def test_ready_api_response_envelope_task_has_no_actionable_gaps():
    result = analyze_task_api_response_envelope_readiness(
        _plan(
            [
                _task(
                    "task-envelope",
                    title="Add standardized response envelope to REST APIs",
                    description=(
                        "Implement response envelope with top-level data field for success payloads, "
                        "top-level errors field for error arrays, top-level meta field for metadata, "
                        "pagination metadata with next_cursor, prev_cursor, has_more, total_count, "
                        "hypermedia links with self link, next link, prev link, "
                        "warnings field for deprecation warnings, "
                        "request_id correlation ID for tracing, "
                        "partial success handling for mixed results, "
                        "batch results with item-level status. "
                        "Add contract tests for envelope schema validation. "
                        "Test SDK compatibility and backwards compatibility with existing clients. "
                        "Define consistent error envelope structure. "
                        "Add pagination metadata tests for cursor handling and has_more flags. "
                        "Document response envelope in OpenAPI specs and SDK guides."
                    ),
                    files_or_modules=["src/api/response_envelope.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskApiResponseEnvelopeReadinessPlan)
    assert result.plan_id == "plan-envelope"
    assert result.envelope_task_ids == ("task-envelope",)
    finding = result.findings[0]
    assert isinstance(finding, TaskApiResponseEnvelopeReadinessFinding)
    assert finding.detected_signals == (
        "data_field",
        "errors_field",
        "meta_field",
        "pagination_metadata",
        "links",
        "warnings",
        "request_id",
        "partial_success",
        "batch_results",
    )
    assert finding.present_requirements == (
        "contract_tests",
        "sdk_compatibility",
        "backwards_compatibility",
        "error_envelope",
        "pagination_metadata_tests",
        "documentation",
    )
    assert finding.missing_requirements == ()
    assert finding.actionable_gaps == ()
    assert finding.risk_level == "low"
    assert "files_or_modules: src/api/response_envelope.py" in finding.evidence
    assert result.summary["envelope_task_count"] == 1
    assert result.summary["missing_requirement_count"] == 0
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_partial_envelope_task_reports_specific_actionable_gaps():
    result = build_task_api_response_envelope_readiness_plan(
        _plan(
            [
                _task(
                    "task-data-wrapper",
                    title="Add data field wrapper to API responses",
                    description=(
                        "Wrap all API response payloads with top-level data field and errors field. "
                        "Add meta field for pagination metadata with next_cursor and has_more."
                    ),
                    files_or_modules=["src/api/data_wrapper.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-data-wrapper"
    assert finding.detected_signals == (
        "data_field",
        "errors_field",
        "meta_field",
        "pagination_metadata",
    )
    assert finding.present_requirements == ()
    assert finding.missing_requirements == (
        "contract_tests",
        "sdk_compatibility",
        "backwards_compatibility",
        "error_envelope",
        "pagination_metadata_tests",
        "documentation",
    )
    assert finding.risk_level == "high"
    assert finding.actionable_gaps == (
        "Add contract tests validating response schema, envelope structure, required fields, and optional fields.",
        "Test SDK deserialization, client compatibility, and backwards compatibility with existing SDK versions.",
        "Verify backwards compatibility with existing clients, ensure additive changes, and document breaking changes.",
        "Define consistent error envelope structure, error field format, and problem details schema.",
        "Add tests for pagination metadata fields, cursor tokens, has_more flags, and page info completeness.",
        "Document response envelope structure, field meanings, optional/required fields, and SDK usage examples.",
    )
    assert result.summary["missing_requirement_counts"]["contract_tests"] == 1
    assert result.summary["present_requirement_counts"]["contract_tests"] == 0


def test_path_hints_contribute_to_detection():
    result = build_task_api_response_envelope_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Wire envelope",
                    description="Add response schema.",
                    files_or_modules=[
                        "src/api/response_envelope/schema.py",
                        "src/api/data_wrapper.py",
                        "src/api/error_envelope.py",
                        "src/api/pagination_meta.py",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert {"data_field", "errors_field", "pagination_metadata"} <= set(finding.detected_signals)
    assert "files_or_modules: src/api/response_envelope/schema.py" in finding.evidence


def test_explicitly_no_impact_excludes_task():
    result = build_task_api_response_envelope_readiness_plan(
        _plan(
            [
                _task(
                    "task-no-impact",
                    title="Update API documentation",
                    description="Update docs, no response envelope changes required.",
                )
            ]
        )
    )

    assert result.envelope_task_ids == ()
    assert result.not_applicable_task_ids == ("task-no-impact",)
    assert result.findings == ()


def test_serialization_aliases_model_input_mutation_iterable_object_json():
    plan = _plan(
        [
            _task(
                "task-complete-envelope",
                title="Response envelope with data, errors, meta, links",
                description=(
                    "Add data field, errors field, meta field, pagination metadata, "
                    "hypermedia links, warnings, request ID, partial success, and batch results. "
                    "Include contract tests, SDK compatibility, backwards compatibility, "
                    "error envelope structure, pagination metadata tests, and documentation."
                ),
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_api_response_envelope_readiness_plan(ExecutionPlan.model_validate(plan))
    payload = task_api_response_envelope_readiness_plan_to_dict(result)

    assert plan == original
    assert analyze_task_api_response_envelope_readiness(plan).to_dict() == result.to_dict()
    assert summarize_task_api_response_envelope_readiness(plan).to_dict() == result.to_dict()
    assert extract_task_api_response_envelope_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_api_response_envelope_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_api_response_envelope_readiness(plan).to_dict() == result.to_dict()
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["findings"]
    assert task_api_response_envelope_readiness_plan_to_dicts(result.records) == payload["findings"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "findings",
        "envelope_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["findings"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_requirements",
        "missing_requirements",
        "risk_level",
        "evidence",
        "actionable_gaps",
    ]

    empty = build_task_api_response_envelope_readiness_plan({"id": "empty-plan", "tasks": []})
    iterable = build_task_api_response_envelope_readiness_plan(plan["tasks"])
    object_plan = build_task_api_response_envelope_readiness_plan(_ObjectPlan("object-plan", plan["tasks"]))
    namespace_plan = build_task_api_response_envelope_readiness_plan(_NamespacePlan(id="namespace-plan", tasks=plan["tasks"]))
    single_task = build_task_api_response_envelope_readiness_plan(plan["tasks"][0])

    assert empty.plan_id == "empty-plan"
    assert empty.findings == ()
    assert empty.summary["total_task_count"] == 0
    assert iterable.plan_id is None
    assert iterable.envelope_task_ids == ("task-complete-envelope",)
    assert object_plan.plan_id == "object-plan"
    assert object_plan.to_dict()["findings"] == result.to_dict()["findings"]
    assert namespace_plan.plan_id == "namespace-plan"
    assert namespace_plan.to_dict()["findings"] == result.to_dict()["findings"]
    assert single_task.plan_id is None
    assert single_task.envelope_task_ids == ("task-complete-envelope",)


def test_weak_partial_strong_task_ordering():
    result = build_task_api_response_envelope_readiness_plan(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Response envelope with all safeguards",
                    description=(
                        "Add data field, errors field, meta field, pagination metadata, links, "
                        "warnings, request ID, partial success, batch results, contract tests, "
                        "SDK compatibility, backwards compatibility, error envelope, "
                        "pagination metadata tests, and documentation."
                    ),
                ),
                _task(
                    "task-weak",
                    title="Add data field wrapper",
                    description="Wrap responses with data field.",
                ),
                _task(
                    "task-partial",
                    title="Response envelope with contract tests",
                    description="Add data field, errors field, meta field, and contract tests.",
                ),
            ]
        )
    )

    assert len(result.findings) == 3
    assert result.findings[0].task_id == "task-partial"
    assert result.findings[0].risk_level == "high"
    assert result.findings[1].task_id == "task-weak"
    assert result.findings[1].risk_level == "high"
    assert result.findings[2].task_id == "task-strong"
    assert result.findings[2].risk_level == "low"


def _plan(tasks, *, plan_id="plan-envelope"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-envelope",
        "milestones": [],
        "tasks": tasks,
    }


def _task(task_id, *, title=None, description=None, files_or_modules=None):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": ["Done"],
        "status": "pending",
    }
    return task


class _ObjectPlan:
    def __init__(self, plan_id, tasks):
        self.id = plan_id
        self.tasks = tasks


class _NamespacePlan(SimpleNamespace):
    pass
