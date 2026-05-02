import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_audit_log_coverage import (
    TaskAuditLogCoveragePlan,
    TaskAuditLogCoverageRecord,
    build_task_audit_log_coverage_plan,
    derive_task_audit_log_coverage_plan,
    task_audit_log_coverage_to_dict,
    task_audit_log_coverage_to_markdown,
)


def test_detects_audit_events_from_title_description_files_and_metadata():
    result = build_task_audit_log_coverage_plan(
        _plan(
            [
                _task(
                    "task-sensitive",
                    title="Admin billing export permission changes",
                    description=(
                        "Allow support admins to grant access, export data to CSV, delete stale invoices, "
                        "and update customer-visible account settings."
                    ),
                    files_or_modules=[
                        "src/admin/billing_exports.py",
                        "src/auth/rbac/permissions.py",
                        "src/jobs/delete_invoices.py",
                    ],
                    metadata={
                        "auth_session": "API key rotation and SSO login events are affected.",
                        "audit_log": {"coverage": "Existing audit trail records who did what."},
                    },
                )
            ]
        )
    )

    assert isinstance(result, TaskAuditLogCoveragePlan)
    assert result.audit_task_ids == ("task-sensitive",)
    record = result.records[0]
    assert isinstance(record, TaskAuditLogCoverageRecord)
    assert record.audit_events == (
        "user_visible",
        "admin",
        "auth",
        "billing",
        "data_export",
        "destructive",
        "permission_change",
    )
    assert record.coverage_risk == "medium"
    assert record.existing_audit_coverage is True
    assert len(record.evidence_requirements) == 7
    assert any("billing audit events" in hint for hint in record.validation_hints)
    assert any("compensating grant or revoke" in item for item in record.rollback_considerations)
    assert any("files_or_modules: src/admin/billing_exports.py" == item for item in record.evidence)
    assert any("metadata.auth_session" in item for item in record.evidence)
    assert result.summary["event_counts"]["permission_change"] == 1


def test_non_sensitive_tasks_produce_low_noise_guidance():
    result = build_task_audit_log_coverage_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard and login copy",
                    description="Adjust report-only dashboard labels, tooltip wording, and login helper text.",
                    files_or_modules=["src/ui/dashboard_copy.tsx"],
                    acceptance_criteria=["Copy matches product language."],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.audit_task_ids == ()
    assert result.ignored_task_ids == ("task-copy",)
    assert result.summary == {
        "task_count": 1,
        "audit_task_count": 0,
        "ignored_task_ids": ["task-copy"],
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "event_counts": {
            "user_visible": 0,
            "admin": 0,
            "auth": 0,
            "billing": 0,
            "data_export": 0,
            "destructive": 0,
            "permission_change": 0,
        },
        "existing_audit_coverage_count": 0,
    }
    assert "No audit log coverage requirements were inferred." in result.to_markdown()


def test_model_and_iterable_inputs_are_supported_without_mutation():
    plan = _plan(
        [
            _task(
                "task-auth",
                title="Rotate API key sessions",
                description="Change auth token and session handling for login flows.",
                acceptance_criteria=["Audit logs capture token rotation security events."],
            ),
            _task("task-docs", title="Update docs", description="Document the setup guide."),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)
    task_model = ExecutionTask.model_validate(
        _task(
            "task-export",
            title="Build data export download",
            description="Create CSV export for customer account data.",
            files_or_modules=["src/exports/customer_download.py"],
        )
    )

    result = derive_task_audit_log_coverage_plan(model)
    iterable_dict_result = build_task_audit_log_coverage_plan([plan["tasks"][0]])
    iterable_model_result = build_task_audit_log_coverage_plan([task_model])

    assert plan == original
    assert result.plan_id == "plan-audit-coverage"
    assert result.audit_task_ids == ("task-auth",)
    assert result.ignored_task_ids == ("task-docs",)
    assert result.records[0].coverage_risk == "low"
    assert iterable_dict_result.plan_id is None
    assert iterable_dict_result.audit_task_ids == ("task-auth",)
    assert iterable_model_result.audit_task_ids == ("task-export",)
    assert iterable_model_result.records[0].audit_events == ("user_visible", "data_export")


def test_empty_input_serializes_stably():
    result = build_task_audit_log_coverage_plan([])
    payload = task_audit_log_coverage_to_dict(result)

    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == []
    assert payload == {
        "plan_id": None,
        "records": [],
        "recommendations": [],
        "audit_task_ids": [],
        "ignored_task_ids": [],
        "summary": {
            "task_count": 0,
            "audit_task_count": 0,
            "ignored_task_ids": [],
            "risk_counts": {"high": 0, "medium": 0, "low": 0},
            "event_counts": {
                "user_visible": 0,
                "admin": 0,
                "auth": 0,
                "billing": 0,
                "data_export": 0,
                "destructive": 0,
                "permission_change": 0,
            },
            "existing_audit_coverage_count": 0,
        },
    }


def test_markdown_output_is_stable_and_escapes_pipes():
    result = build_task_audit_log_coverage_plan(
        _plan(
            [
                _task(
                    "task-role",
                    title="Role | permission update",
                    description="Grant access for admins and update RBAC roles.",
                    acceptance_criteria=["Permissions update correctly."],
                )
            ]
        )
    )

    markdown = task_audit_log_coverage_to_markdown(result)

    assert markdown.startswith("# Task Audit Log Coverage Plan: plan-audit-coverage")
    assert "Role \\| permission update" in markdown
    assert (
        "| Task | Title | Risk | Audit Events | Existing Audit Coverage | Evidence Requirements | "
        "Validation Hints | Rollback Considerations | Evidence |"
    ) in markdown
    assert "`task-role`" in markdown
    assert "permission_change" in markdown


def _plan(tasks, plan_id="plan-audit-coverage"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-audit-coverage",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-audit-coverage",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "medium",
        "estimated_hours": 2.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
