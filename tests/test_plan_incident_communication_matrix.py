import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_incident_communication_matrix import (
    PlanIncidentCommunicationMatrix,
    PlanIncidentCommunicationRow,
    build_plan_incident_communication_matrix,
    derive_plan_incident_communication_matrix,
    generate_plan_incident_communication_matrix,
    plan_incident_communication_matrix_to_dict,
    plan_incident_communication_matrix_to_markdown,
    summarize_plan_incident_communication_matrix,
)


def test_data_migration_risk_creates_audience_specific_high_priority_rows():
    result = generate_plan_incident_communication_matrix(
        _plan(
            [
                _task(
                    "task-migration",
                    title="Run customer account data migration",
                    description=(
                        "Backfill existing customer records during cutover and halt on "
                        "data integrity validation failures."
                    ),
                    files_or_modules=["db/migrations/20260502_accounts.sql"],
                    metadata={"owner": "Data Platform"},
                )
            ]
        )
    )

    assert isinstance(result, PlanIncidentCommunicationMatrix)
    assert result.plan_id == "plan-incidents"
    assert result.incident_task_ids == ("task-migration",)
    assert all(isinstance(row, PlanIncidentCommunicationRow) for row in result.rows)

    by_audience = {row.audience: row for row in result.rows}
    assert set(by_audience) == {
        "customers",
        "admins",
        "support",
        "customer_success",
        "operations",
        "engineering",
        "data_governance",
        "executives",
    }
    data_row = by_audience["data_governance"]
    assert data_row.priority == "high"
    assert data_row.risk_categories == ("data_integrity", "migration", "customer_facing")
    assert "Data Platform" in data_row.owner_suggestions
    assert "database owner" in data_row.owner_suggestions
    assert any("validation finds missing" in trigger for trigger in data_row.trigger_conditions)
    assert "data validation and reconciliation status" in data_row.draft_message_topics
    assert "files_or_modules: db/migrations/20260502_accounts.sql" in data_row.evidence


def test_external_integration_customer_risk_includes_vendor_security_and_customer_rows():
    result = build_plan_incident_communication_matrix(
        _plan(
            [
                _task(
                    "task-webhook",
                    title="Launch customer-facing payment provider webhook",
                    description=(
                        "Enable external Stripe webhook integration for checkout and "
                        "notify if provider auth failures or dependency latency spike."
                    ),
                    files_or_modules=["src/integrations/stripe_webhook.py"],
                    metadata={"dri": "Payments Integrations"},
                )
            ]
        )
    )

    by_audience = {row.audience: row for row in result.rows}
    assert by_audience["customers"].priority == "high"
    assert by_audience["vendor_partner"].risk_categories == (
        "reliability",
        "external_integration",
        "customer_facing",
    )
    assert "vendor escalation owner" in by_audience["vendor_partner"].owner_suggestions
    assert "security" in by_audience
    assert "provider escalation status and auth/security impact" in by_audience[
        "security"
    ].draft_message_topics
    assert any("provider errors" in trigger for trigger in by_audience["vendor_partner"].trigger_conditions)
    assert result.summary["communication_row_count"] == len(result.rows)
    assert result.summary["audience_counts"]["vendor_partner"] == 1


def test_low_risk_documentation_task_returns_empty_matrix_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-docs",
                title="Update incident response README wording",
                description="Documentation-only copy edit for the runbook introduction.",
                files_or_modules=["docs/incident-response-readme.md"],
                risk_level="low",
            )
        ],
        plan_id="plan-docs",
    )
    original = copy.deepcopy(plan)

    result = generate_plan_incident_communication_matrix(plan)

    assert plan == original
    assert result.rows == ()
    assert result.records == ()
    assert result.incident_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "incident_task_count": 0,
        "communication_row_count": 0,
        "priority_counts": {"high": 0, "medium": 0, "low": 0},
        "audience_counts": {
            "customers": 0,
            "admins": 0,
            "support": 0,
            "customer_success": 0,
            "operations": 0,
            "engineering": 0,
            "data_governance": 0,
            "security": 0,
            "vendor_partner": 0,
            "executives": 0,
        },
        "risk_counts": {
            "reliability": 0,
            "data_integrity": 0,
            "migration": 0,
            "external_integration": 0,
            "customer_facing": 0,
        },
    }
    assert result.to_markdown() == (
        "# Plan Incident Communication Matrix: plan-docs\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Incident task count: 0\n"
        "- Communication row count: 0\n"
        "- Priority counts: high 0, medium 0, low 0\n"
        "\n"
        "No incident communication rows were detected."
    )


def test_serialization_markdown_aliases_and_model_input_are_stable():
    plan = _plan(
        [
            _task(
                "task-pipe",
                title="Customer | API degradation launch watch",
                description="Customer-facing external API integration can degrade during launch.",
                acceptance_criteria=["On-call owner validates rollback status."],
            )
        ],
        plan_id="plan-model",
    )
    model = ExecutionPlan.model_validate(plan)

    result = generate_plan_incident_communication_matrix(model)
    derived = derive_plan_incident_communication_matrix(result)
    summarized = summarize_plan_incident_communication_matrix(plan)
    payload = plan_incident_communication_matrix_to_dict(result)
    markdown = plan_incident_communication_matrix_to_markdown(result)

    assert derived is result
    assert summarized.to_dicts() == generate_plan_incident_communication_matrix(plan).to_dicts()
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["rows"]
    assert result.records == result.rows
    assert list(payload) == ["plan_id", "rows", "incident_task_ids", "summary"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "audience",
        "risk_categories",
        "trigger_conditions",
        "draft_message_topics",
        "owner_suggestions",
        "escalation_timing",
        "priority",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert "Customer \\| API degradation launch watch" in markdown
    assert build_plan_incident_communication_matrix(plan).summary == result.summary


def _plan(tasks, *, plan_id="plan-incidents", metadata=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-incidents",
        "milestones": [{"name": "Launch"}],
        "tasks": tasks,
        "metadata": {} if metadata is None else metadata,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    risk_level=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
        "risk_level": risk_level,
        "metadata": {} if metadata is None else metadata,
    }
