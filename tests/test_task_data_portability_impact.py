import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_data_portability_impact import (
    TaskDataPortabilityImpactFinding,
    TaskDataPortabilityImpactPlan,
    build_task_data_portability_impact_plan,
    recommend_task_data_portability_impact,
    summarize_task_data_portability_impact,
    task_data_portability_impact_plan_to_dict,
)


def test_portability_tasks_produce_risk_ranked_recommendations():
    result = build_task_data_portability_impact_plan(
        _plan(
            [
                _task(
                    "task-low",
                    title="Add export my data endpoint",
                    description="Export my data with a versioned JSON schema.",
                    acceptance_criteria=[
                        "Requester authorization check verifies the authenticated user owns the account.",
                        "PII redaction excludes sensitive fields.",
                        "Async delivery uses a background job and email when ready.",
                        "Audit log event records each data export.",
                        "Expiration policy gives signed URLs a 24 hour TTL.",
                        "Large export handling uses chunking, pagination, and throttling.",
                    ],
                ),
                _task(
                    "task-high",
                    title="Build account transfer archive",
                    description="Account transfer creates a download archive for all customer account data.",
                ),
                _task(
                    "task-medium",
                    title="Add customer CSV export",
                    description="CSV export for customer profile data with a documented CSV schema.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskDataPortabilityImpactPlan)
    assert result.impacted_task_ids == ("task-high", "task-medium", "task-low")
    by_id = {finding.task_id: finding for finding in result.findings}
    assert by_id["task-high"].risk_level == "high"
    assert {"account_transfer", "portable_archive"} <= set(by_id["task-high"].impacted_surfaces)
    assert by_id["task-medium"].risk_level == "medium"
    assert by_id["task-medium"].missing_safeguards == (
        "authorization_check",
        "pii_redaction",
        "async_delivery",
        "audit_logging",
        "expiration_policy",
        "large_export_handling",
    )
    assert by_id["task-low"].risk_level == "low"
    assert by_id["task-low"].missing_safeguards == ()
    assert result.summary["risk_counts"] == {"high": 1, "medium": 1, "low": 1}


def test_generic_admin_reporting_exports_are_ignored_without_user_data_signals():
    result = build_task_data_portability_impact_plan(
        _plan(
            [
                _task(
                    "task-report",
                    title="Add admin data export",
                    description="Data export for aggregate reporting metrics on the admin dashboard.",
                ),
                _task(
                    "task-customer",
                    title="Add admin CSV export for customer data",
                    description="CSV export for customer profile data from the admin dashboard.",
                ),
            ]
        )
    )

    assert result.impacted_task_ids == ("task-customer",)
    assert result.ignored_task_ids == ("task-report",)
    assert result.findings[0].impacted_surfaces == ("structured_file_export",)


def test_fallback_task_ids_and_multiple_evidence_sources_are_stable():
    result = build_task_data_portability_impact_plan(
        _plan(
            [
                _task(
                    None,
                    title="Self-service data copy",
                    description="Self-service data copy for user account data.",
                    files_or_modules=["src/users/self_service_data_copy.py"],
                    metadata={"privacy": {"request": "GDPR data access request for data subject exports"}},
                ),
                _task(
                    "task-json",
                    title="JSON export",
                    description="JSON export for user profile data with authorization check.",
                ),
            ]
        )
    )

    assert result.impacted_task_ids == ("task-1", "task-json")
    fallback = result.findings[0]
    assert fallback.task_id == "task-1"
    assert {"self_service_copy", "gdpr_access_request"} <= set(fallback.impacted_surfaces)
    assert any("files_or_modules" in item for item in fallback.evidence)
    assert any("metadata.privacy.request" in item for item in fallback.evidence)
    assert task_data_portability_impact_plan_to_dict(result)["summary"]["impacted_task_count"] == 2


def test_mapping_and_execution_plan_inputs_serialize_equivalently_without_mutation():
    source = _plan(
        [
            _task(
                "task-a",
                title="Migration out support",
                description="Migration out exports customer data as a download archive.",
                metadata={
                    "safeguards": {
                        "format_contract": "versioned format manifest",
                        "authorization_check": "requester authorization check",
                        "pii_redaction": "redact secrets",
                        "async_delivery": "queued background job",
                        "audit_logging": "audit log event",
                        "expiration_policy": "download expires after 7 days",
                        "large_export_handling": "chunking and backpressure",
                    }
                },
            ),
            _task(
                "task-b",
                title="Update admin dashboard",
                description="Polish reporting filters.",
            ),
        ]
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    mapping_result = build_task_data_portability_impact_plan(source)
    model_result = build_task_data_portability_impact_plan(model)
    alias_result = summarize_task_data_portability_impact(source)
    recommendations = recommend_task_data_portability_impact(model)
    payload = task_data_portability_impact_plan_to_dict(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert alias_result.to_dict() == mapping_result.to_dict()
    assert recommendations == model_result.findings
    assert model_result.records == model_result.findings
    assert model_result.to_dicts() == payload["findings"]
    assert isinstance(model_result.findings[0], TaskDataPortabilityImpactFinding)
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "findings",
        "impacted_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["findings"][0]) == [
        "task_id",
        "title",
        "impacted_surfaces",
        "missing_safeguards",
        "risk_level",
        "evidence",
    ]


def _plan(tasks):
    return {
        "id": "plan-data-portability-impact",
        "implementation_brief_id": "brief-data-portability-impact",
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
    tags=None,
    metadata=None,
):
    task = {
        "title": title or task_id or "Untitled task",
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if task_id is not None:
        task["id"] = task_id
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if tags is not None:
        task["tags"] = tags
    if metadata is not None:
        task["metadata"] = metadata
    return task
