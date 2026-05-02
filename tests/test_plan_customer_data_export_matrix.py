import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_customer_data_export_matrix import (
    PlanCustomerDataExportMatrix,
    PlanCustomerDataExportMatrixRow,
    build_plan_customer_data_export_matrix,
    derive_plan_customer_data_export_matrix,
    generate_plan_customer_data_export_matrix,
    plan_customer_data_export_matrix_to_dict,
    plan_customer_data_export_matrix_to_markdown,
    summarize_plan_customer_data_export_matrix,
)


def test_export_signals_create_rows_with_missing_field_recommendations():
    result = build_plan_customer_data_export_matrix(
        _plan(
            [
                _task(
                    "task-self-service",
                    title="Build self-service customer data CSV download",
                    description=(
                        "Customers can download my data as CSV from account settings. Include "
                        "redaction for PII and export expiry."
                    ),
                    owner_type="privacy-platform",
                    acceptance_criteria=[
                        "Access is audited and encrypted export files expire after 24 hours."
                    ],
                ),
                _task(
                    "task-admin",
                    title="Admin bulk tenant export",
                    description="Add admin export for all tenant data and account records.",
                    files_or_modules=["src/admin/tenant_bulk_export.py"],
                ),
                _task(
                    "task-audit-report",
                    title="Audit PDF report export",
                    description="Generate PDF audit report exports for tenant administrators.",
                    metadata={"data_owner": "compliance"},
                ),
                _task(
                    "task-docs",
                    title="Refresh onboarding copy",
                    description="Clarify documentation and dashboard labels.",
                ),
            ]
        )
    )

    assert isinstance(result, PlanCustomerDataExportMatrix)
    assert result.plan_id == "plan-export"
    assert result.export_task_ids == ("task-self-service", "task-admin", "task-audit-report")
    assert result.no_signal_task_ids == ("task-docs",)
    assert result.summary["task_count"] == 4
    assert result.summary["export_task_count"] == 3
    assert result.summary["export_surface_counts"]["self_service_export"] == 1
    assert result.summary["export_surface_counts"]["admin_export"] == 1
    assert result.summary["export_surface_counts"]["audit_export"] == 1
    assert result.summary["export_surface_counts"]["bulk_export"] == 1
    assert result.summary["export_surface_counts"]["file_download"] >= 2
    assert result.summary["missing_field_counts"]["actor_or_owner"] >= 1
    assert result.summary["missing_field_counts"]["privacy_safeguards"] >= 1

    self_service = _row(result, "task-self-service", "self_service_export")
    assert isinstance(self_service, PlanCustomerDataExportMatrixRow)
    assert self_service.data_scope == "personal_data"
    assert self_service.actor_or_owner == "privacy-platform"
    assert "csv" in self_service.format_or_destination
    assert "redaction" in self_service.privacy_safeguards
    assert "privacy_safeguards" not in self_service.missing_fields
    assert any(
        "title: Build self-service customer data CSV download" in item
        for item in self_service.evidence
    )

    admin = _row(result, "task-admin", "admin_export")
    assert admin.data_scope == "customer_data"
    assert admin.actor_or_owner is None
    assert admin.format_or_destination is None
    assert admin.missing_fields == ("actor_or_owner", "format_or_destination", "privacy_safeguards")
    assert "approval, access logging" in admin.recommendation

    audit = _row(result, "task-audit-report", "audit_export")
    assert audit.data_scope == "audit_records"
    assert audit.actor_or_owner == "compliance"
    assert "pdf" in audit.format_or_destination


def test_low_risk_docs_and_negated_export_language_are_empty():
    result = build_plan_customer_data_export_matrix(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Document report viewer",
                    description="Low-risk docs only. No CSV export, PDF report, or download work is required.",
                ),
                _task(
                    "task-api",
                    title="Refactor account search",
                    description="Do not add exports. Improve pagination for customer lookup only.",
                ),
            ]
        )
    )

    assert result.rows == ()
    assert result.export_task_ids == ()
    assert result.no_signal_task_ids == ("task-docs", "task-api")
    assert result.summary["row_count"] == 0
    assert "No customer data export planning rows were inferred." in result.to_markdown()
    assert "No export signals: task-docs, task-api" in result.to_markdown()


def test_serialization_alias_model_mapping_and_prebuilt_round_trip_without_mutation():
    plan = _plan(
        [
            _task(
                "task-pipe | export",
                title="Tenant data report | CSV",
                description="Export tenant data report as CSV to S3 bucket with masked emails.",
                metadata={"owner": "reporting-platform"},
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_plan_customer_data_export_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_customer_data_export_matrix_to_dict(result)
    markdown = plan_customer_data_export_matrix_to_markdown(result)
    generated = generate_plan_customer_data_export_matrix(payload)
    derived = derive_plan_customer_data_export_matrix(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert result.records == result.rows
    assert generated.to_dict() == payload
    assert derived is result
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "export_task_ids",
        "no_signal_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "export_surface",
        "data_scope",
        "actor_or_owner",
        "format_or_destination",
        "privacy_safeguards",
        "missing_fields",
        "evidence",
        "recommendation",
    ]
    assert markdown.startswith("# Plan Customer Data Export Matrix: plan-export")
    assert "`task-pipe \\| export`" in markdown
    assert "Tenant data report \\| CSV" in markdown


def test_execution_task_object_invalid_and_empty_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Bulk CSV export customer records",
        description="Create CSV bulk export download for customer data.",
        files_or_modules=["src/exports/customers.py"],
        acceptance_criteria=["Done"],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Retention-bound PDF exports",
            description="Expire exported PDF customer reports according to retention bounds.",
        )
    )

    object_result = build_plan_customer_data_export_matrix([object_task])
    task_result = build_plan_customer_data_export_matrix(task_model)
    empty = build_plan_customer_data_export_matrix({"id": "empty-plan", "tasks": []})
    invalid = build_plan_customer_data_export_matrix(13)

    assert object_result.export_task_ids == ("task-object",)
    assert {"bulk_export", "file_download"} <= {row.export_surface for row in object_result.rows}
    assert task_result.plan_id is None
    assert _row(task_result, "task-model", "retention_bound_export").data_scope == "customer_data"
    assert empty.to_dict() == {
        "plan_id": "empty-plan",
        "rows": [],
        "export_task_ids": [],
        "no_signal_task_ids": [],
        "summary": {
            "task_count": 0,
            "row_count": 0,
            "export_task_count": 0,
            "no_signal_task_count": 0,
            "export_surface_counts": {
                "self_service_export": 0,
                "admin_export": 0,
                "audit_export": 0,
                "reporting_export": 0,
                "file_download": 0,
                "bulk_export": 0,
                "retention_bound_export": 0,
                "redacted_export": 0,
            },
            "missing_field_counts": {
                "data_scope": 0,
                "actor_or_owner": 0,
                "format_or_destination": 0,
                "privacy_safeguards": 0,
            },
            "missing_field_total": 0,
        },
    }
    assert invalid.plan_id is None
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0


def _row(result, task_id, export_surface):
    return next(
        row
        for row in result.rows
        if row.task_id == task_id and row.export_surface == export_surface
    )


def _plan(tasks, *, plan_id="plan-export"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-export",
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
    owner_type=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if owner_type is not None:
        task["owner_type"] = owner_type
    if metadata is not None:
        task["metadata"] = metadata
    return task
