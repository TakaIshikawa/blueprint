import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_compliance_evidence import (
    TaskComplianceEvidencePlan,
    TaskComplianceEvidenceRecord,
    build_task_compliance_evidence_plan,
    summarize_task_compliance_evidence,
    task_compliance_evidence_plan_to_dict,
    task_compliance_evidence_plan_to_markdown,
)


def test_sensitive_tasks_receive_targeted_evidence_requirements():
    result = build_task_compliance_evidence_plan(
        _plan(
            [
                _task(
                    "task-auth",
                    title="Harden admin authorization",
                    description=("Require RBAC authorization and audit logs for billing refunds."),
                    files_or_modules=[
                        "src/auth/admin_permissions.py",
                        "src/billing/refunds.py",
                    ],
                    acceptance_criteria=[
                        "Tests verify allowed and denied roles with audit evidence.",
                    ],
                ),
                _task(
                    "task-privacy",
                    title="Encrypt patient retention exports",
                    description=("Encrypt patient PII exports and enforce retention deletion."),
                    files_or_modules=[
                        "src/patients/export_retention.py",
                        "src/encryption/kms.py",
                    ],
                    acceptance_criteria=[],
                ),
            ]
        )
    )

    assert result.plan_id == "plan-compliance"
    assert result.records[0] == TaskComplianceEvidenceRecord(
        task_id="task-auth",
        task_title="Harden admin authorization",
        severity="high",
        compliance_domains=(
            "access_control",
            "financial_workflows",
            "audit_logs",
        ),
        evidence_requirements=(
            "Capture authorization matrix or role coverage for allowed and denied paths.",
            "Provide negative access tests for unauthorized users or roles.",
            "Capture ledger, invoice, payment, refund, or reconciliation evidence for the changed workflow.",
            "Show totals, currency, idempotency, and rollback behavior are validated.",
            "Capture before and after audit event examples with actor, action, target, and timestamp fields.",
            "Show audit records are reviewable and protected from unauthorized edits.",
            "Link the evidence to the task id and changed files or modules.",
        ),
        missing_acceptance_criteria=(),
        validation_hints=(
            "Keep command output, screenshots, fixtures, or review links with stable identifiers.",
            "Exercise at least one allowed role and one denied role.",
            "Verify audit records contain actor, action, target, result, and timestamp.",
            "Validate rounding, idempotency, reconciliation, and failure handling.",
        ),
        evidence=(
            "files_or_modules: src/auth/admin_permissions.py",
            "title: Harden admin authorization",
            "description: Require RBAC authorization and audit logs for billing refunds.",
            "files_or_modules: src/billing/refunds.py",
        ),
    )
    assert result.records[1].severity == "high"
    assert result.records[1].compliance_domains == (
        "healthcare_pii",
        "encryption",
        "retention",
        "data_processing",
    )
    assert result.records[1].missing_acceptance_criteria == (
        "Add acceptance criteria requiring validation evidence for the compliance-sensitive path.",
        "Add acceptance criteria requiring audit or reviewable evidence for the changed compliance control.",
        "Add acceptance criteria proving secrets, keys, and plaintext exposure are controlled.",
    )
    assert result.summary["severity_counts"] == {"high": 2, "medium": 0, "low": 0}
    assert result.summary["domain_counts"]["access_control"] == 1
    assert result.summary["domain_counts"]["healthcare_pii"] == 1
    assert result.summary["missing_acceptance_criteria_count"] == 3


def test_consent_policy_and_data_processing_signals_are_detected():
    result = build_task_compliance_evidence_plan(
        _plan(
            [
                _task(
                    "task-consent",
                    title="Update marketing consent policy",
                    description=(
                        "Change privacy policy rules for opt-out consent propagation "
                        "through analytics data processing."
                    ),
                    files_or_modules=["src/privacy/consent_preferences.py"],
                    acceptance_criteria=[
                        "Validate opt-out consent withdrawal and approval owner evidence.",
                    ],
                    metadata={"policy": "Governance approval required."},
                )
            ]
        )
    )

    assert result.records[0].severity == "medium"
    assert result.records[0].compliance_domains == (
        "consent",
        "data_processing",
        "policy_changes",
    )
    assert any(
        requirement.startswith("Capture consent state transitions")
        for requirement in result.records[0].evidence_requirements
    )
    assert result.records[0].missing_acceptance_criteria == ()
    assert result.summary["sensitive_task_count"] == 1


def test_non_sensitive_task_returns_low_severity_record_with_validation_guidance():
    result = build_task_compliance_evidence_plan(
        _plan(
            [
                _task(
                    "task-ui",
                    title="Polish dashboard empty state",
                    description="Adjust copy and spacing in the dashboard empty state.",
                    files_or_modules=["src/ui/dashboard_empty_state.tsx"],
                    acceptance_criteria=["Screenshot reviewed."],
                )
            ]
        )
    )

    assert result.records == (
        TaskComplianceEvidenceRecord(
            task_id="task-ui",
            task_title="Polish dashboard empty state",
            severity="low",
            compliance_domains=("policy_changes",),
            evidence_requirements=(
                "Leave validation output showing the intended behavior still works.",
                "Document why no regulated data, access, billing, retention, or policy evidence is required.",
            ),
            missing_acceptance_criteria=(),
            validation_hints=(
                "Run the smallest relevant automated or manual check and keep the command or checklist result.",
            ),
            evidence=(),
        ),
    )
    assert result.summary["sensitive_task_count"] == 0
    assert result.summary["severity_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_model_input_serializes_stably_and_renders_markdown():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-audit",
                    title="Add audit record export",
                    description="Export audit logs for compliance review.",
                    files_or_modules=["src/audit/export.py"],
                    acceptance_criteria=["Validation covers exported audit records."],
                )
            ]
        )
    )

    result = summarize_task_compliance_evidence(plan)
    payload = task_compliance_evidence_plan_to_dict(result)

    assert isinstance(result, TaskComplianceEvidencePlan)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert list(payload) == ["plan_id", "records", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "task_title",
        "severity",
        "compliance_domains",
        "evidence_requirements",
        "missing_acceptance_criteria",
        "validation_hints",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert task_compliance_evidence_plan_to_markdown(result) == "\n".join(
        [
            "# Task Compliance Evidence: plan-compliance",
            "",
            "| Task | Severity | Domains | Evidence Requirements | Missing Acceptance Criteria | Validation Hints |",
            "| --- | --- | --- | --- | --- | --- |",
            "| task-audit | medium | audit_logs, data_processing | Capture before and after audit event examples with actor, action, target, and timestamp fields.; Show audit records are reviewable and protected from unauthorized edits.; Record source, transformation, destination, and reconciliation evidence for processed data.; Provide sample validation proving derived data matches documented rules.; Link the evidence to the task id and changed files or modules. |  | Keep command output, screenshots, fixtures, or review links with stable identifiers.; Verify audit records contain actor, action, target, result, and timestamp.; Compare input, output, and exception samples for processing correctness. |",
        ]
    )


def _plan(tasks):
    return {
        "id": "plan-compliance",
        "implementation_brief_id": "brief-compliance",
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
):
    return {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": (
            acceptance_criteria if acceptance_criteria is not None else ["Done"]
        ),
        "status": "pending",
        "metadata": metadata or {},
    }
