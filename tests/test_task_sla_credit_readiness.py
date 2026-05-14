import json

from blueprint.task_sla_credit_readiness import (
    analyze_task_sla_credit_readiness,
    build_task_sla_credit_readiness_plan,
    recommend_task_sla_credit_readiness,
    task_sla_credit_readiness_plan_to_dict,
    task_sla_credit_readiness_plan_to_dicts,
    task_sla_credit_readiness_plan_to_markdown,
)


def test_complete_sla_credit_task_is_ready():
    result = build_task_sla_credit_readiness_plan(
        {
            "id": "plan-sla-credit",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "SLA credits for uptime credit eligibility",
                    "description": "Service credits cover availability penalty and customer compensation for downtime.",
                    "acceptance_criteria": [
                        "Eligibility rules define eligible customers, qualifying outage, excluded outage, SLA threshold, and credit policy.",
                        "Outage measurement source identifies uptime source, availability source, monitoring source, and source of truth.",
                        "Credit calculation covers credit amount, credit percentage, monthly fee, service credit formula, and proration.",
                        "Claim approval workflow includes claim workflow, approver, support claim, and manual review.",
                        "Billing application applies invoice credit, account credit, next invoice, and ledger adjustment.",
                        "Customer communication includes customer notice, credit notice, notification, email, and terms disclosure.",
                        "Audit evidence captures audit trail, calculation evidence, outage evidence, approval log, and ledger audit.",
                        "Tests include SLA tests, credit tests, billing tests, and claim tests.",
                    ],
                    "files_or_modules": ["src/billing/sla/uptime_credits.py"],
                }
            ],
        }
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "eligibility_rules",
        "outage_measurement_source",
        "credit_calculation",
        "claim_approval_workflow",
        "billing_application",
        "customer_communication",
        "audit_evidence",
        "tests",
    )


def test_partial_sla_credit_reports_gaps_and_ignores_no_impact():
    result = analyze_task_sla_credit_readiness(
        [
            {
                "id": "task-partial",
                "title": "Add service credit eligibility",
                "description": "SLA credits define eligibility rules and billing application.",
                "metadata": {"sla": {"measurement": "Monitoring source is the source of truth."}},
                "validation_commands": ["python -m pytest tests/billing/test_sla_credits.py"],
            },
            {
                "id": "task-copy",
                "title": "SLA copy cleanup",
                "description": "No SLA credits, service credits, uptime credits, availability penalties, credit eligibility, or customer compensation changes are planned.",
            },
        ]
    )

    record = result.records[0]
    assert result.ignored_task_ids == ("task-copy",)
    assert record.readiness == "partial"
    assert record.present_criteria == ("eligibility_rules", "outage_measurement_source", "billing_application", "tests")
    assert record.missing_criteria == ("credit_calculation", "claim_approval_workflow", "customer_communication", "audit_evidence")
    assert any("metadata.sla.measurement" in item for item in record.evidence)
    assert any("validation_commands[0]" in item for item in record.evidence)


def test_sla_credit_path_hints_serialization_and_markdown_are_stable():
    result = build_task_sla_credit_readiness_plan(
        {"id": "plan-path", "tasks": [{"id": "task-path", "title": "Refactor billing", "files_or_modules": ["src/billing/sla/credit_eligibility.py"]}]}
    )
    payload = task_sla_credit_readiness_plan_to_dict(result)

    assert result.records[0].detected_signals == ("sla_credit", "credit_eligibility")
    assert recommend_task_sla_credit_readiness(result) == result.records
    assert task_sla_credit_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-path"
    assert task_sla_credit_readiness_plan_to_markdown(result).startswith("# Task SLA Credit Readiness: plan-path")
