import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_incident_severity_triage_matrix import (
    PlanIncidentSeverityTriageMatrix,
    PlanIncidentSeverityTriageMatrixRow,
    build_plan_incident_severity_triage_matrix,
    derive_plan_incident_severity_triage_matrix,
    generate_plan_incident_severity_triage_matrix,
    plan_incident_severity_triage_matrix_to_dict,
    plan_incident_severity_triage_matrix_to_dicts,
    plan_incident_severity_triage_matrix_to_markdown,
    summarize_plan_incident_severity_triage_matrix,
)


def test_detects_incident_outage_degradation_paging_and_customer_support_signals():
    result = build_plan_incident_severity_triage_matrix(
        _plan(
            [
                _task(
                    "task-outage",
                    title="Prepare checkout outage severity triage",
                    description=(
                        "SEV1 checkout outage with customer-visible impact. "
                        "Triage owner: Payments Oncall. Response SLA: 10 minutes. "
                        "Communication path: status page and support macro. "
                        "Escalation path: PagerDuty payments-primary then VP Engineering."
                    ),
                ),
                _task(
                    "task-degraded",
                    title="Handle degraded search latency",
                    description="SEV2 degradation with paging for elevated errors.",
                ),
                _task(
                    "task-security",
                    title="Security incident runbook",
                    description="P0 security incident for credential exposure.",
                ),
                _task(
                    "task-data",
                    title="Data incident reconciliation",
                    description="Data incident for PII export corruption and customer impact.",
                ),
                _task(
                    "task-support",
                    title="Support escalation for ticket spike",
                    description="Support escalation when billing cases exceed launch guardrails.",
                ),
                _task(
                    "task-docs",
                    title="Refresh docs",
                    description="Update API examples.",
                ),
            ]
        )
    )

    by_id = {row.task_id: row for row in result.rows}
    assert set(by_id) == {
        "task-outage",
        "task-degraded",
        "task-security",
        "task-data",
        "task-support",
    }
    assert isinstance(by_id["task-outage"], PlanIncidentSeverityTriageMatrixRow)
    assert by_id["task-outage"].severity_level == "sev1"
    assert by_id["task-outage"].trigger_signal == "customer impact"
    assert by_id["task-outage"].triage_owner == "Payments Oncall"
    assert by_id["task-outage"].response_sla == "10 minutes"
    assert by_id["task-outage"].communication_path == "status page and support macro"
    assert by_id["task-outage"].escalation_path == "PagerDuty payments-primary then VP Engineering"
    assert by_id["task-outage"].missing_fields == ()
    assert by_id["task-degraded"].severity_level == "sev2"
    assert by_id["task-degraded"].trigger_signal == "degradation"
    assert by_id["task-security"].severity_level == "sev0"
    assert by_id["task-security"].trigger_signal == "security incident"
    assert by_id["task-data"].trigger_signal == "data incident"
    assert by_id["task-support"].trigger_signal == "support escalation"
    assert result.summary["total_task_count"] == 6
    assert result.summary["incident_task_count"] == 5
    assert result.summary["row_count"] == 5
    assert result.summary["unrelated_task_count"] == 1
    assert result.summary["severity_counts"]["sev0"] == 2
    assert result.summary["severity_counts"]["sev1"] == 1
    assert result.summary["severity_counts"]["sev2"] == 1
    assert result.summary["severity_counts"]["unspecified"] == 1


def test_metadata_extracts_complete_triage_fields():
    result = build_plan_incident_severity_triage_matrix(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Incident triage for search outage",
                    metadata={
                        "incident_triage": {
                            "severity_level": "sev2",
                            "trigger": "degraded search responses",
                            "triage_owner": "Search Platform",
                            "ack_sla": "15 minutes",
                            "communication_path": "internal #incidents and status page",
                            "escalation_path": "PagerDuty search-primary then Support Lead",
                        }
                    },
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.severity_level == "sev2"
    assert row.trigger_signal == "outage"
    assert row.triage_owner == "Search Platform"
    assert row.response_sla == "15 minutes"
    assert row.communication_path == "internal #incidents and status page"
    assert row.escalation_path == "PagerDuty search-primary then Support Lead"
    assert row.missing_fields == ()
    assert row.recommendation.startswith("Ready:")
    assert "metadata.incident_triage.triage_owner: Search Platform" in row.evidence
    assert result.summary["missing_field_count"] == 0
    assert result.summary["tasks_missing_owner"] == 0


def test_incomplete_incident_triage_lists_missing_fields_and_counts():
    result = build_plan_incident_severity_triage_matrix(
        _plan(
            [
                _task(
                    "task-gaps",
                    title="Customer impact escalation for checkout",
                    description=(
                        "Customer impact during checkout degradation. "
                        "Communication path: status page and support escalation note."
                    ),
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.severity_level == "sev2"
    assert row.trigger_signal == "customer impact"
    assert row.communication_path == "status page and support escalation note"
    assert row.missing_fields == ("triage_owner", "response_sla", "escalation_path")
    assert "assign the incident commander" in row.recommendation
    assert "state the response SLA" in row.recommendation
    assert "document the paging or executive escalation path" in row.recommendation
    assert result.summary["tasks_missing_owner"] == 1
    assert result.summary["tasks_missing_sla"] == 1
    assert result.summary["tasks_missing_escalation"] == 1
    assert result.summary["tasks_missing_communication"] == 0
    assert result.summary["missing_field_counts"]["response_sla"] == 1


def test_non_incident_plan_returns_stable_empty_matrix_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-api",
                title="Add profile endpoint",
                description="Create backend route for profile reads.",
            )
        ],
        plan_id="plan-non-incident",
    )
    original = copy.deepcopy(plan)

    result = build_plan_incident_severity_triage_matrix(plan)

    assert plan == original
    assert isinstance(result, PlanIncidentSeverityTriageMatrix)
    assert result.plan_id == "plan-non-incident"
    assert result.rows == ()
    assert result.records == ()
    assert result.to_dict() == {
        "plan_id": "plan-non-incident",
        "summary": {
            "total_task_count": 1,
            "incident_task_count": 0,
            "unrelated_task_count": 1,
            "row_count": 0,
            "tasks_missing_owner": 0,
            "tasks_missing_sla": 0,
            "tasks_missing_escalation": 0,
            "tasks_missing_communication": 0,
            "missing_field_count": 0,
            "missing_field_counts": {
                "triage_owner": 0,
                "response_sla": 0,
                "communication_path": 0,
                "escalation_path": 0,
            },
            "severity_counts": {
                "sev0": 0,
                "sev1": 0,
                "sev2": 0,
                "sev3": 0,
                "sev4": 0,
                "unspecified": 0,
            },
        },
        "rows": [],
    }
    assert result.to_markdown() == (
        "# Plan Incident Severity Triage Matrix: plan-non-incident\n\n"
        "## Summary\n\n"
        "- Total tasks: 1\n"
        "- Incident triage tasks: 0\n"
        "- Triage row count: 0\n"
        "- Missing triage fields: 0\n"
        "- Severity counts: sev0 0, sev1 0, sev2 0, sev3 0, sev4 0, unspecified 0\n\n"
        "No incident severity triage rows were inferred."
    )
    assert generate_plan_incident_severity_triage_matrix({"tasks": "not a list"}) == ()
    assert build_plan_incident_severity_triage_matrix(None).summary["total_task_count"] == 0


def test_markdown_escapes_cells_and_serializes_deterministically():
    result = build_plan_incident_severity_triage_matrix(
        _plan(
            [
                _task(
                    "task-md",
                    title="SEV1 | checkout outage",
                    description=(
                        "SEV1 customer-visible outage. Triage owner: Payments | Oncall. "
                        "Response SLA: 5 minutes. Communication path: status page | support macro. "
                        "Escalation path: PagerDuty | exec bridge."
                    ),
                )
            ]
        )
    )

    markdown = plan_incident_severity_triage_matrix_to_markdown(result)
    payload = plan_incident_severity_triage_matrix_to_dict(result)

    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "rows"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "severity_level",
        "trigger_signal",
        "triage_owner",
        "response_sla",
        "communication_path",
        "escalation_path",
        "missing_fields",
        "evidence",
        "recommendation",
    ]
    assert "SEV1 \\| checkout outage" in markdown
    assert "Payments \\| Oncall" in markdown
    assert "status page \\| support macro" in markdown
    assert "PagerDuty \\| exec bridge" in markdown
    assert markdown == result.to_markdown()


def test_model_input_aliases_and_iterable_rows_match():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Paging and escalation for API degradation",
                description=(
                    "SEV3 elevated errors page within 20 minutes. Notify: #incidents. "
                    "Escalate to API lead."
                ),
                metadata={
                    "triage": {
                        "triage_owner": "API Platform",
                        "communication_path": "Slack #incidents and support handoff",
                    }
                },
            )
        ],
        plan_id="plan-model",
    )
    model = ExecutionPlan.model_validate(plan)

    result = build_plan_incident_severity_triage_matrix(model)
    derived = derive_plan_incident_severity_triage_matrix(result)
    summarized = summarize_plan_incident_severity_triage_matrix(plan)
    rows = generate_plan_incident_severity_triage_matrix(model)
    payload = plan_incident_severity_triage_matrix_to_dict(result)

    assert derived is result
    assert rows == result.rows
    assert summarized.to_dict() == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert plan_incident_severity_triage_matrix_to_dicts(result) == payload["rows"]
    assert plan_incident_severity_triage_matrix_to_dicts(rows) == payload["rows"]
    assert payload["rows"][0]["severity_level"] == "sev3"
    assert payload["rows"][0]["triage_owner"] == "API Platform"
    assert payload["rows"][0]["response_sla"] == "20 minutes"


def _plan(tasks, *, plan_id="plan-incident-triage"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-incident-triage",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    owner_type=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if owner_type is not None:
        task["owner_type"] = owner_type
    if metadata is not None:
        task["metadata"] = metadata
    return task
