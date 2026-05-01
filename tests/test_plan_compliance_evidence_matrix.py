import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.plan_compliance_evidence_matrix import (
    PlanComplianceEvidenceMatrixRow,
    build_plan_compliance_evidence_matrix,
    plan_compliance_evidence_matrix_to_dict,
    plan_compliance_evidence_matrix_to_dicts,
    plan_compliance_evidence_matrix_to_markdown,
    summarize_plan_compliance_evidence,
)


def test_brief_risks_and_constraints_group_compliance_domains():
    result = build_plan_compliance_evidence_matrix(
        _brief(
            data_requirements=(
                "Process customer data under GDPR with a data inventory and DPIA review."
            ),
            risks=[
                "SOC2 and ISO27001 control mapping must show validation output.",
                "HIPAA PHI handling needs an approval record before launch.",
            ],
        )
    )

    assert result.brief_id == "brief-compliance"
    assert result.plan_id is None
    assert [row.domain for row in result.rows] == [
        "soc2",
        "iso27001",
        "gdpr",
        "hipaa",
        "approval_records",
        "data_processing",
    ]
    assert result.rows[0] == PlanComplianceEvidenceMatrixRow(
        domain="soc2",
        affected_task_ids=(),
        required_evidence=(
            "control mapping",
            "validation output",
            "approval record",
        ),
        missing_evidence=("approval record",),
        evidence_sources=(
            "brief.risks[0]: SOC2 and ISO27001 control mapping must show validation output",
        ),
        risk_level="low",
        review_owner="security compliance",
    )
    assert _row(result, "gdpr").missing_evidence == ("validation output",)
    assert _row(result, "hipaa").review_owner == "privacy security"
    assert _row(result, "approval_records").missing_evidence == ("change record",)
    assert result.summary["domain_count"] == 6
    assert result.summary["missing_evidence_count"] == 8
    assert result.summary["risk_counts"] == {"high": 2, "medium": 0, "low": 4}


def test_task_text_assigns_affected_tasks_and_scores_evidence_gaps():
    result = build_plan_compliance_evidence_matrix(
        _plan(
            [
                _task(
                    "task-payments",
                    title="Harden PCI checkout change management",
                    description=(
                        "Update cardholder data checkout flow with a change ticket, "
                        "release approval, rollback plan, PCI scan, and pytest validation."
                    ),
                    acceptance_criteria=["Validation output links the change record."],
                ),
                _task(
                    "task-access",
                    title="Quarterly access review",
                    description="Review RBAC permissions for privileged access.",
                ),
            ]
        )
    )

    assert result.brief_id is None
    assert result.plan_id == "plan-compliance"
    assert _row(result, "pci").affected_task_ids == ("task-payments",)
    assert _row(result, "pci").missing_evidence == ("data inventory",)
    assert _row(result, "pci").risk_level == "high"
    assert _row(result, "change_management").missing_evidence == ()
    assert _row(result, "change_management").risk_level == "low"
    assert _row(result, "access_review").affected_task_ids == ("task-access",)
    assert _row(result, "access_review").missing_evidence == (
        "validation output",
        "approval record",
    )
    assert _row(result, "access_review").risk_level == "high"
    assert result.summary["affected_task_count"] == 2


def test_markdown_and_serializer_helpers_render_stable_counts():
    result = build_plan_compliance_evidence_matrix(
        _brief(risks=["SOX financial control requires approval record."]),
        _plan(
            [
                _task(
                    "task-audit",
                    title="Emit audit logs",
                    description="Add audit event records and pytest validation output.",
                )
            ]
        ),
    )

    payload = plan_compliance_evidence_matrix_to_dict(result)

    assert payload == result.to_dict()
    assert plan_compliance_evidence_matrix_to_dicts(result) == payload["rows"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "plan_id", "summary", "rows"]
    assert list(payload["rows"][0]) == [
        "domain",
        "affected_task_ids",
        "required_evidence",
        "missing_evidence",
        "evidence_sources",
        "risk_level",
        "review_owner",
    ]
    assert plan_compliance_evidence_matrix_to_markdown(result) == "\n".join(
        [
            "# Plan Compliance Evidence Matrix: brief brief-compliance, plan plan-compliance",
            "",
            "Summary: 3 domains, 1 affected tasks, 2 missing evidence artifacts.",
            "",
            "| Domain | Risk | Affected Tasks | Required Evidence | Missing Evidence | Evidence Sources | Review Owner |",
            "| --- | --- | --- | --- | --- | --- | --- |",
            "| sox | high | brief | sox control; approval record; validation output | validation output | brief.risks[0]: SOX financial control requires approval record | finance controls |",
            "| audit_logging | low | task-audit | audit log sample; validation output | none | task.task-audit.title: Emit audit logs; task.task-audit.description: Add audit event records and pytest validation output | security engineering |",
            "| approval_records | low | brief | approval record; change record | change record | brief.risks[0]: SOX financial control requires approval record | program owner |",
        ]
    )


def test_empty_input_returns_stable_no_op_report():
    result = build_plan_compliance_evidence_matrix(
        _brief(data_requirements="Store dashboard display preferences."),
        _plan(
            [
                _task(
                    "task-ui",
                    title="Polish dashboard",
                    description="Render saved widgets and theme controls.",
                )
            ]
        ),
    )

    assert result.rows == ()
    assert result.summary == {
        "domain_count": 0,
        "affected_task_count": 0,
        "required_evidence_count": 0,
        "missing_evidence_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "domain_counts": {
            "soc2": 0,
            "iso27001": 0,
            "gdpr": 0,
            "hipaa": 0,
            "pci": 0,
            "sox": 0,
            "audit_logging": 0,
            "approval_records": 0,
            "data_processing": 0,
            "access_review": 0,
            "change_management": 0,
        },
    }
    assert result.to_dict()["rows"] == []
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Compliance Evidence Matrix: brief brief-compliance, plan plan-compliance",
            "",
            "Summary: 0 domains, 0 affected tasks, 0 missing evidence artifacts.",
            "",
            "No compliance evidence rows were inferred.",
        ]
    )


def test_pydantic_models_and_plan_only_alias_are_supported():
    brief = ImplementationBrief.model_validate(
        _brief(risks=["Approval records are required for SOX change control."])
    )
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-change",
                    title="Document change management",
                    description="Create change request with CAB approval and validation checklist.",
                )
            ]
        )
    )

    combined = summarize_plan_compliance_evidence(brief, plan)
    plan_only = build_plan_compliance_evidence_matrix(plan)

    assert combined.brief_id == "brief-compliance"
    assert combined.plan_id == "plan-compliance"
    assert _row(combined, "approval_records").affected_task_ids == ("task-change",)
    assert _row(combined, "change_management").missing_evidence == ()
    assert plan_only.brief_id is None
    assert plan_only.plan_id == "plan-compliance"
    assert [row.domain for row in plan_only.rows] == [
        "approval_records",
        "change_management",
    ]


def _row(result, domain):
    return next(row for row in result.rows if row.domain == domain)


def _brief(*, data_requirements=None, risks=None):
    return {
        "id": "brief-compliance",
        "source_brief_id": "source-compliance",
        "title": "Compliance-sensitive implementation",
        "domain": "operations",
        "target_user": "support admins",
        "buyer": "operations leadership",
        "workflow_context": "Administrative workflows",
        "problem_statement": "Support needs safer account operations.",
        "mvp_goal": "Ship the first workflow.",
        "product_surface": "Admin console",
        "scope": ["Admin workflow"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": "Use existing service boundaries.",
        "data_requirements": data_requirements or "Account state and workflow status.",
        "integration_points": [],
        "risks": risks or [],
        "validation_plan": "Run focused tests.",
        "definition_of_done": ["Tests pass"],
        "status": "draft",
    }


def _plan(tasks):
    return {
        "id": "plan-compliance",
        "implementation_brief_id": "brief-compliance",
        "milestones": [],
        "tasks": tasks,
    }


def _task(task_id, *, title=None, description=None, acceptance_criteria=None, metadata=None):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [],
        "acceptance_criteria": (
            acceptance_criteria if acceptance_criteria is not None else ["Done"]
        ),
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
