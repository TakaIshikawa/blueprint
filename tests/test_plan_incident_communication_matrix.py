import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_incident_communication_matrix import (
    PlanIncidentCommunicationMatrix,
    PlanIncidentCommunicationRow,
    build_plan_incident_communication_matrix,
    derive_plan_incident_communication_matrix,
    generate_plan_incident_communication_matrix,
    plan_incident_communication_matrix_to_dict,
    plan_incident_communication_matrix_to_dicts,
    plan_incident_communication_matrix_to_markdown,
    summarize_plan_incident_communication_matrix,
)


def test_complete_incident_communication_coverage_is_ready():
    result = generate_plan_incident_communication_matrix(
        _plan(
            [
                _task(
                    "task-comms",
                    title="Prepare incident communication plan",
                    description=(
                        "Define Sev 1 and Sev 2 severity thresholds, customer updates through "
                        "status page and email, internal escalation through Slack war room and "
                        "PagerDuty, and timing SLA of first update within 15 minutes then every "
                        "30 minutes."
                    ),
                    acceptance_criteria=[
                        "Incident commander and communications DRI are assigned.",
                        "Post-incident follow-up includes postmortem, RCA, and corrective action items.",
                    ],
                    metadata={"owner": "Incident Response"},
                )
            ]
        )
    )

    assert isinstance(result, PlanIncidentCommunicationMatrix)
    assert result.plan_id == "plan-incidents"
    assert result.incident_task_ids == ("task-comms",)
    assert all(isinstance(row, PlanIncidentCommunicationRow) for row in result.rows)

    row = result.rows[0]
    assert row.readiness_level == "ready"
    assert row.present_components == (
        "customer_updates",
        "internal_escalation",
        "owner_assignment",
        "severity_thresholds",
        "communication_channels",
        "follow_up_tasks",
        "timing_sla_expectations",
    )
    assert row.missing_components == ()
    assert row.gap_reasons == ()
    assert "Incident Response" in row.owner_hints
    assert "status page" in row.channel_hints
    assert result.summary["ready_task_count"] == 1
    assert result.summary["gap_count"] == 0


def test_missing_customer_communication_reports_gap_reason():
    result = build_plan_incident_communication_matrix(
        _plan(
            [
                _task(
                    "task-internal",
                    title="Draft production incident escalation runbook",
                    description=(
                        "Set Sev 1 incident declaration thresholds, on-call escalation, Slack war "
                        "room channel, incident commander owner, and update cadence every 20 minutes."
                    ),
                    files_or_modules=["docs/runbooks/incident-escalation.md"],
                    acceptance_criteria=["Schedule postmortem and action items after resolution."],
                    metadata={"dri": "SRE"},
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.readiness_level == "partial"
    assert "customer_updates" in row.missing_components
    assert any("customer-facing update plan" in reason for reason in row.gap_reasons)
    assert "internal_escalation" in row.present_components
    assert "communication_channels" in row.present_components
    assert result.summary["missing_component_counts"]["customer_updates"] == 1
    assert result.summary["component_coverage_counts"]["internal_escalation"] == 1


def test_partial_internal_only_plan_identifies_multiple_missing_readiness_items():
    plan = _plan(
        [
            _task(
                "task-partial",
                title="Create internal incident bridge",
                description="Create an internal escalation path and Slack war room for outage response.",
                metadata={"owner": "Operations"},
            )
        ],
        plan_id="plan-partial",
    )
    model = ExecutionPlan.model_validate(plan)

    result = generate_plan_incident_communication_matrix(model)
    derived = derive_plan_incident_communication_matrix(result)
    summarized = summarize_plan_incident_communication_matrix(plan)
    payload = plan_incident_communication_matrix_to_dict(result)
    markdown = plan_incident_communication_matrix_to_markdown(result)

    assert derived is result
    assert summarized.to_dicts() == result.to_dicts()
    assert plan_incident_communication_matrix_to_dicts(result) == result.to_dicts()
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "records", "incident_task_ids", "summary"]
    assert payload["rows"] == payload["records"]

    row = result.rows[0]
    assert row.readiness_level == "partial"
    assert row.present_components == (
        "internal_escalation",
        "owner_assignment",
        "communication_channels",
    )
    assert row.missing_components == (
        "customer_updates",
        "severity_thresholds",
        "follow_up_tasks",
        "timing_sla_expectations",
    )
    assert len(row.gap_reasons) == 4
    assert result.records == result.rows
    assert result.summary["partial_task_count"] == 1
    assert "Plan Incident Communication Readiness Matrix: plan-partial" in markdown


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
