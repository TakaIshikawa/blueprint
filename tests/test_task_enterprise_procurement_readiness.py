import json

from blueprint.task_enterprise_procurement_readiness import (
    analyze_task_enterprise_procurement_readiness,
    build_task_enterprise_procurement_readiness_plan,
    recommend_task_enterprise_procurement_readiness,
    task_enterprise_procurement_readiness_plan_to_dict,
    task_enterprise_procurement_readiness_plan_to_dicts,
    task_enterprise_procurement_readiness_plan_to_markdown,
)


def test_complete_enterprise_procurement_task_is_ready():
    result = build_task_enterprise_procurement_readiness_plan(
        {
            "id": "plan-procurement",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Enterprise procurement approval and contract onboarding",
                    "description": "Vendor procurement includes security questionnaire, legal review, and purchase order flow.",
                    "acceptance_criteria": [
                        "Buyer admin workflow covers buyer workflow, admin workflow, requester workflow, and purchasing admin.",
                        "Approval authority names approver, approval chain, budget owner, legal approver, security approver, and sign-off.",
                        "Security legal artifacts include security questionnaire, SOC 2, ISO 27001, DPA, MSA, contract, and terms.",
                        "Purchase order invoicing path covers purchase order, PO number, invoice routing, billing contact, and procurement invoice.",
                        "Provisioning handoff includes account provisioning, tenant provisioning, implementation handoff, and customer success handoff.",
                        "Audit trail includes approval log, procurement record, contract record, decision log, and evidence log.",
                        "Timeline SLA defines service level, turnaround time, due date, procurement deadline, and response time.",
                        "Tests include workflow tests, procurement tests, contract tests, and approval tests.",
                    ],
                    "files_or_modules": ["src/procurement/vendor_contract_onboarding.py"],
                }
            ],
        }
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "buyer_admin_workflow",
        "approval_authority",
        "security_legal_artifacts",
        "purchase_order_invoicing_path",
        "provisioning_handoff",
        "audit_trail",
        "timeline_sla",
        "tests",
    )


def test_partial_enterprise_procurement_reports_gaps_and_ignores_no_impact():
    result = analyze_task_enterprise_procurement_readiness(
        [
            {
                "id": "task-partial",
                "title": "Add enterprise procurement workflow",
                "description": "Procurement approval has buyer workflow and purchase order invoice routing.",
                "metadata": {"procurement": {"artifacts": "Security questionnaire and legal artifacts are required."}},
                "validation_commands": ["python -m pytest tests/procurement/test_contract_onboarding.py"],
            },
            {
                "id": "task-copy",
                "title": "Vendor copy cleanup",
                "description": "No enterprise procurement, vendor procurement, security questionnaires, legal review, purchase orders, or contract onboarding changes are planned.",
            },
        ]
    )

    record = result.records[0]
    assert result.ignored_task_ids == ("task-copy",)
    assert record.readiness == "partial"
    assert record.present_criteria == ("buyer_admin_workflow", "approval_authority", "security_legal_artifacts", "purchase_order_invoicing_path", "tests")
    assert record.missing_criteria == ("provisioning_handoff", "audit_trail", "timeline_sla")
    assert any("metadata.procurement.artifacts" in item for item in record.evidence)
    assert any("validation_commands[0]" in item for item in record.evidence)


def test_enterprise_procurement_path_hints_serialization_and_markdown_are_stable():
    result = build_task_enterprise_procurement_readiness_plan(
        {"id": "plan-path", "tasks": [{"id": "task-path", "title": "Refactor vendor onboarding", "files_or_modules": ["src/procurement/vendor_contracts/purchase_order.py"]}]}
    )
    payload = task_enterprise_procurement_readiness_plan_to_dict(result)

    assert result.records[0].detected_signals == ("enterprise_procurement", "legal_review", "purchase_order")
    assert recommend_task_enterprise_procurement_readiness(result) == result.records
    assert task_enterprise_procurement_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-path"
    assert task_enterprise_procurement_readiness_plan_to_markdown(result).startswith("# Task Enterprise Procurement Readiness: plan-path")
