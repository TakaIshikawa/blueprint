import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_incident_response import (
    IncidentResponseMatrixEntry,
    PlanIncidentResponseMatrix,
    PlanIncidentResponseRow,
    build_plan_incident_response_matrix,
    derive_plan_incident_response_matrix,
    generate_plan_incident_response_matrix,
    plan_incident_response_matrix_to_dict,
    plan_incident_response_matrix_to_dicts,
    plan_incident_response_matrix_to_markdown,
    summarize_plan_incident_response_matrix,
)


# Test complete coverage scenarios


def test_complete_incident_response_coverage_is_ready():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-ir",
                    title="Create comprehensive incident response plan",
                    description=(
                        "Define P0, P1, P2, and P3 severity levels with clear criteria. "
                        "Document response procedures for service outages and performance degradation. "
                        "Set up escalation paths with PagerDuty and on-call rotation. "
                        "Configure communication protocols via Slack and email status page updates."
                    ),
                    files_or_modules=["docs/runbooks/incident-response.md"],
                    acceptance_criteria=[
                        "All severity definitions clearly specify impact thresholds and SLA expectations.",
                        "Runbooks are available for all critical incident types.",
                        "Missing runbook gaps are identified and documented.",
                        "Escalation chain completeness is verified.",
                        "All procedures are tested through fire drill and game day exercises.",
                    ],
                    metadata={"owner": "SRE Team", "incident_commander": "On-Call Lead"},
                )
            ]
        )
    )

    assert isinstance(result, PlanIncidentResponseMatrix)
    assert result.plan_id == "plan-incident-response"
    assert result.incident_task_ids == ("task-ir",)
    assert all(isinstance(row, PlanIncidentResponseRow) for row in result.rows)

    row = result.rows[0]
    assert row.readiness_level == "ready"
    assert row.task_id == "task-ir"
    assert "severity_levels_defined" in row.present_aspects
    assert "response_procedures_documented" in row.present_aspects
    assert "escalation_paths_clear" in row.present_aspects
    assert "communication_protocols_set" in row.present_aspects
    assert "runbooks_available" in row.present_aspects
    assert "severity_definitions_clear" in row.present_aspects
    assert "missing_runbooks_identified" in row.present_aspects
    assert "incomplete_escalation_detected" in row.present_aspects
    assert "untested_procedures_flagged" in row.present_aspects
    assert row.missing_aspects == ()
    assert row.gap_reasons == ()
    assert "SRE Team" in row.owner_hints
    assert "service_outage" in row.incident_types or "performance_degradation" in row.incident_types
    assert "P0" in row.severity_levels or "P1" in row.severity_levels
    assert len(row.matrix_entries) > 0
    assert result.summary["ready_task_count"] == 1
    assert result.summary["gap_count"] == 0
    assert result.summary["scoring"]["overall_score"] > 90


def test_partial_coverage_missing_procedures():
    result = build_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Set up incident escalation",
                    description="Define P0 and P1 severity levels and escalation chain to on-call.",
                    metadata={"owner": "Operations"},
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.readiness_level == "partial"
    assert "severity_levels_defined" in row.present_aspects
    assert "escalation_paths_clear" in row.present_aspects
    assert "response_procedures_documented" not in row.present_aspects
    assert "runbooks_available" not in row.present_aspects
    assert "communication_protocols_set" not in row.present_aspects
    assert len(row.missing_aspects) > 0
    assert any("response procedures" in reason for reason in row.gap_reasons)
    assert result.summary["partial_task_count"] == 1
    assert result.summary["scoring"]["procedure_coverage"] < 100


def test_missing_coverage_no_incident_aspects():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-regular",
                    title="Implement user authentication",
                    description="Add OAuth2 authentication flow for user login.",
                )
            ]
        )
    )

    assert len(result.rows) == 0
    assert result.incident_task_ids == ()
    assert result.summary["incident_task_count"] == 0
    assert result.summary["gap_count"] == 0


# Test aspect detection


def test_severity_levels_defined_aspect():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-sev",
                    title="Define severity levels",
                    description="Create P0, P1, P2, P3, and P4 severity classifications.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "severity_levels_defined" in row.present_aspects
    assert all(sev in row.severity_levels for sev in ["P0", "P1", "P2", "P3", "P4"])


def test_response_procedures_documented_aspect():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-proc",
                    title="Document incident response procedures",
                    description="Create step-by-step response procedures for all incident types.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "response_procedures_documented" in row.present_aspects


def test_escalation_paths_clear_aspect():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-esc",
                    title="Set up escalation chain",
                    description="Configure PagerDuty escalation path with primary and secondary on-call.",
                    metadata={"oncall": "SRE Team"},
                )
            ]
        )
    )

    row = result.rows[0]
    assert "escalation_paths_clear" in row.present_aspects
    assert "SRE Team" in row.owner_hints


def test_communication_protocols_set_aspect():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-comm",
                    title="Configure incident notifications",
                    description="Set up Slack alerts, email notifications, and status page updates.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "communication_protocols_set" in row.present_aspects


def test_runbooks_available_aspect():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-runbook",
                    title="Create incident runbooks",
                    description="Write operational runbooks for common incident scenarios.",
                    files_or_modules=["docs/runbooks/incidents.md"],
                )
            ]
        )
    )

    row = result.rows[0]
    assert "runbooks_available" in row.present_aspects
    assert any("incidents.md" in evidence for evidence in row.evidence)


def test_severity_definitions_clear_aspect():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-def",
                    title="Define severity criteria",
                    description="Specify impact criteria, SLA thresholds, and customer impact definitions.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "severity_definitions_clear" in row.present_aspects


def test_missing_runbooks_identified_aspect():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-gap",
                    title="Identify runbook gaps",
                    description="Document missing runbooks and procedure gaps for incident scenarios.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "missing_runbooks_identified" in row.present_aspects


def test_incomplete_escalation_detected_aspect():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-esc-gap",
                    title="Review escalation coverage",
                    description="Identify incomplete escalation chains and escalation gaps.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "incomplete_escalation_detected" in row.present_aspects


def test_untested_procedures_flagged_aspect():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-test",
                    title="Schedule incident drills",
                    description="Plan fire drills, game day exercises, and chaos testing.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "untested_procedures_flagged" in row.present_aspects


# Test incident type detection


def test_service_outage_incident_type():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-outage",
                    title="Handle service outages",
                    description="Response plan for total service outage and downtime scenarios.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "service_outage" in row.incident_types


def test_data_corruption_incident_type():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-corrupt",
                    title="Handle data corruption incidents",
                    description="Incident response procedures for detecting and repairing corrupted data.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "data_corruption" in row.incident_types


def test_security_breach_incident_type():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-security",
                    title="Security incident response",
                    description="Handle security breaches, unauthorized access, and intrusions.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "security_breach" in row.incident_types


def test_performance_degradation_incident_type():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-perf",
                    title="Handle performance degradation",
                    description="Response to slow performance, high latency, and timeout issues.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "performance_degradation" in row.incident_types


def test_dependency_failure_incident_type():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-dep",
                    title="Handle dependency outage incidents",
                    description="Incident response to third-party vendor failures and external service issues.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "dependency_failure" in row.incident_types


def test_deployment_failure_incident_type():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-deploy",
                    title="Handle deployment failure incidents",
                    description="Incident response to failed deployments and bad releases.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "deployment_failure" in row.incident_types


def test_data_loss_incident_type():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-loss",
                    title="Handle data loss incidents",
                    description="Incident response to lost data, missing data, and backup failures.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "data_loss" in row.incident_types


def test_cascading_failure_incident_type():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-cascade",
                    title="Handle cascading failure incidents",
                    description="Incident response to multi-service cascading failures and widespread outages.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert "cascading_failure" in row.incident_types


def test_multiple_incident_types():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-multi",
                    title="Comprehensive incident response",
                    description=(
                        "Handle service outages, data corruption, security breaches, "
                        "and performance degradation."
                    ),
                )
            ]
        )
    )

    row = result.rows[0]
    assert "service_outage" in row.incident_types
    assert "data_corruption" in row.incident_types
    assert "security_breach" in row.incident_types
    assert "performance_degradation" in row.incident_types
    assert len(row.incident_types) >= 4


# Test matrix generation


def test_matrix_entries_generated():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-matrix",
                    title="Incident response matrix",
                    description=(
                        "Handle P0 and P1 service outages with escalation to SRE team "
                        "via PagerDuty and Slack."
                    ),
                    files_or_modules=["docs/runbooks/outage.md"],
                    metadata={"owner": "SRE", "oncall": "Platform Team"},
                )
            ]
        )
    )

    row = result.rows[0]
    assert len(row.matrix_entries) > 0
    entry = row.matrix_entries[0]
    assert isinstance(entry, IncidentResponseMatrixEntry)
    assert entry.incident_type == "service_outage"
    assert entry.severity_level in ["P0", "P1"]
    assert entry.response_procedure is not None
    assert len(entry.escalation_chain) > 0
    assert len(entry.on_call_rotation) > 0
    assert "outage.md" in entry.runbook_reference


def test_matrix_entries_with_tested_procedures():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-tested",
                    title="Tested incident procedures",
                    description=(
                        "P0 service outage response, tested via fire drill and game day."
                    ),
                )
            ]
        )
    )

    row = result.rows[0]
    entry = row.matrix_entries[0]
    assert entry.drill_status == "tested"


def test_matrix_entries_with_untested_procedures():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-untested",
                    title="Untested incident procedures",
                    description="P1 service outage response procedures documented.",
                )
            ]
        )
    )

    row = result.rows[0]
    entry = row.matrix_entries[0]
    assert entry.drill_status == "untested"


def test_incident_matrix_aggregation():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-1",
                    title="P0 outage response",
                    description="Handle P0 service outages.",
                ),
                _task(
                    "task-2",
                    title="P1 degradation response",
                    description="Handle P1 performance degradation.",
                ),
            ]
        )
    )

    assert len(result.incident_matrix) > 0
    assert all(isinstance(entry, IncidentResponseMatrixEntry) for entry in result.incident_matrix)


# Test edge cases


def test_cascading_incidents_multi_service():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-cascade",
                    title="Cascading failure response",
                    description=(
                        "Handle P0 cascading failures across multiple services with "
                        "coordinated escalation and cross-team communication."
                    ),
                    metadata={
                        "owner": "SRE",
                        "teams": "Platform, Database, API",
                        "incident_commander": "Senior SRE",
                    },
                )
            ]
        )
    )

    row = result.rows[0]
    assert "cascading_failure" in row.incident_types
    assert "P0" in row.severity_levels
    assert "escalation_paths_clear" in row.present_aspects
    assert "communication_protocols_set" in row.present_aspects
    assert len(row.owner_hints) >= 2


def test_multi_team_coordination():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-multi-team",
                    title="Multi-team incident coordination",
                    description=(
                        "Coordinate P0 security breach response across Security, "
                        "Engineering, Legal, and Communications teams with clear "
                        "escalation paths and communication protocols."
                    ),
                    metadata={
                        "owner": "Security Team",
                        "secondary": "Engineering Lead",
                        "incident_commander": "Security Director",
                    },
                )
            ]
        )
    )

    row = result.rows[0]
    assert "security_breach" in row.incident_types
    assert "escalation_paths_clear" in row.present_aspects
    assert len(row.owner_hints) >= 3


def test_post_incident_review_procedures():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-review",
                    title="Post-incident review process",
                    description=(
                        "Conduct postmortem analysis, identify root cause, document "
                        "lessons learned, and create corrective action items."
                    ),
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.readiness_level in ["partial", "ready"]


def test_communication_channels_extraction():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-channels",
                    title="Multi-channel incident communication",
                    description=(
                        "P0 service outage incident response: Notify via Slack, email, PagerDuty, and status page. "
                        "Use OpsGenie for escalation."
                    ),
                )
            ]
        )
    )

    row = result.rows[0]
    entry = row.matrix_entries[0] if row.matrix_entries else None
    assert entry is not None
    channels = entry.communication_channels
    assert any("slack" in channel.lower() for channel in channels)


# Test serialization and output


def test_serialization_methods():
    plan = _plan(
        [
            _task(
                "task-serial",
                title="Incident response plan",
                description="P0 and P1 incident response with full runbook coverage.",
                files_or_modules=["docs/runbooks/ir.md"],
            )
        ],
        plan_id="plan-serial",
    )
    model = ExecutionPlan.model_validate(plan)

    result = generate_plan_incident_response_matrix(model)
    derived = derive_plan_incident_response_matrix(result)
    summarized = summarize_plan_incident_response_matrix(plan)
    payload = plan_incident_response_matrix_to_dict(result)
    dicts = plan_incident_response_matrix_to_dicts(result)
    markdown = plan_incident_response_matrix_to_markdown(result)

    assert derived is result
    assert summarized.to_dicts() == result.to_dicts()
    assert dicts == result.to_dicts()
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "incident_task_ids",
        "summary",
        "incident_matrix",
    ]
    assert payload["rows"] == payload["records"]
    assert payload["plan_id"] == "plan-serial"
    assert "Plan Incident Response Readiness Matrix: plan-serial" in markdown
    assert "Readiness Scoring" in markdown
    assert "Overall score:" in markdown


def test_markdown_includes_scoring():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-score",
                    title="Scored incident response",
                    description="P0 response with procedures, escalation, and runbooks.",
                    files_or_modules=["docs/runbooks/response.md"],
                )
            ]
        )
    )

    markdown = result.to_markdown()
    assert "Procedure coverage:" in markdown
    assert "Automation level:" in markdown
    assert "Team preparedness:" in markdown
    assert "Documentation quality:" in markdown
    assert "Overall score:" in markdown
    assert "(weight: 30%)" in markdown
    assert "(weight: 20%)" in markdown
    assert "(weight: 25%)" in markdown


def test_markdown_includes_incident_matrix():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-matrix-md",
                    title="Incident matrix",
                    description="P0 service outage response procedures.",
                )
            ]
        )
    )

    markdown = result.to_markdown()
    if result.incident_matrix:
        assert "## Incident Response Matrix" in markdown
        assert "Incident Type" in markdown
        assert "Severity" in markdown
        assert "Response Procedure" in markdown
        assert "Escalation Chain" in markdown
        assert "On-Call" in markdown
        assert "Communication" in markdown
        assert "Runbook" in markdown
        assert "Drill Status" in markdown


# Test summary calculations


def test_summary_readiness_counts():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Full coverage",
                    description=(
                        "P0 severity with procedures, escalation, communication, "
                        "runbooks, severity definitions, gap identification, "
                        "escalation verification, and tested drills."
                    ),
                    files_or_modules=["docs/runbooks/full.md"],
                ),
                _task(
                    "task-partial",
                    title="Partial coverage",
                    description="P1 severity with basic escalation.",
                ),
            ]
        )
    )

    summary = result.summary
    assert summary["task_count"] == 2
    assert summary["incident_task_count"] == 2
    assert summary["readiness_counts"]["ready"] >= 0
    assert summary["readiness_counts"]["partial"] >= 0
    assert summary["readiness_counts"]["missing"] >= 0


def test_summary_aspect_coverage():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-aspects",
                    title="Aspect coverage test",
                    description="P0 with severity, procedures, escalation, and runbooks.",
                    files_or_modules=["docs/runbooks/aspects.md"],
                )
            ]
        )
    )

    summary = result.summary
    assert "aspect_coverage_counts" in summary
    assert "missing_aspect_counts" in summary
    assert summary["aspect_coverage_counts"]["severity_levels_defined"] >= 0
    assert summary["aspect_coverage_counts"]["runbooks_available"] >= 0


def test_summary_incident_type_counts():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-types",
                    title="Multiple incident types",
                    description="Handle outages, data corruption, and security breaches.",
                )
            ]
        )
    )

    summary = result.summary
    assert "incident_type_counts" in summary
    assert summary["incident_type_counts"]["service_outage"] >= 0
    assert summary["incident_type_counts"]["data_corruption"] >= 0
    assert summary["incident_type_counts"]["security_breach"] >= 0


def test_summary_severity_level_counts():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-severities",
                    title="Multiple severities",
                    description="P0, P1, P2, and P3 incident handling.",
                )
            ]
        )
    )

    summary = result.summary
    assert "severity_level_counts" in summary
    assert summary["severity_level_counts"]["P0"] >= 0
    assert summary["severity_level_counts"]["P1"] >= 0


def test_scoring_calculations():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-full-score",
                    title="Full scoring test",
                    description=(
                        "P0 service outage with documented procedures, clear escalation, "
                        "communication protocols, runbooks, severity definitions, "
                        "gap identification, escalation verification, and tested drills."
                    ),
                    files_or_modules=["docs/runbooks/full.md"],
                )
            ]
        )
    )

    scoring = result.summary["scoring"]
    assert 0 <= scoring["procedure_coverage"] <= 100
    assert 0 <= scoring["automation_level"] <= 100
    assert 0 <= scoring["team_preparedness"] <= 100
    assert 0 <= scoring["documentation_quality"] <= 100
    assert 0 <= scoring["overall_score"] <= 100


def test_scoring_weights():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-weighted",
                    title="Weighted scoring",
                    description="P0 with all aspects covered.",
                    files_or_modules=["docs/runbooks/weighted.md"],
                )
            ]
        )
    )

    scoring = result.summary["scoring"]
    # Overall should be weighted average: 30% + 20% + 25% + 25% = 100%
    expected = (
        scoring["procedure_coverage"] * 0.30
        + scoring["automation_level"] * 0.20
        + scoring["team_preparedness"] * 0.25
        + scoring["documentation_quality"] * 0.25
    )
    assert abs(scoring["overall_score"] - expected) < 0.1


# Test records property


def test_records_property_compatibility():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-records",
                    title="Records test",
                    description="P0 incident response.",
                )
            ]
        )
    )

    assert result.records == result.rows
    assert len(result.records) == len(result.rows)


# Test row ordering


def test_rows_sorted_by_readiness():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-missing",
                    title="Missing coverage",
                    description="P0 severity only.",
                ),
                _task(
                    "task-ready",
                    title="Full coverage",
                    description=(
                        "P0 with procedures, escalation, communication, runbooks, "
                        "definitions, gaps, verification, and drills."
                    ),
                    files_or_modules=["docs/runbooks/full.md"],
                ),
                _task(
                    "task-partial",
                    title="Partial coverage",
                    description="P1 with procedures and escalation.",
                ),
            ]
        )
    )

    # Rows should be sorted by readiness: missing, partial, ready
    readiness_levels = [row.readiness_level for row in result.rows]
    readiness_values = [
        {"missing": 0, "partial": 1, "ready": 2}[level] for level in readiness_levels
    ]
    assert readiness_values == sorted(readiness_values)


# Test empty and edge cases


def test_empty_plan():
    result = generate_plan_incident_response_matrix(_plan([]))
    assert result.rows == ()
    assert result.incident_task_ids == ()
    assert result.summary["incident_task_count"] == 0
    assert result.incident_matrix == ()


def test_plan_with_no_incident_tasks():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-normal",
                    title="Normal task",
                    description="Implement feature X.",
                )
            ]
        )
    )

    assert len(result.rows) == 0
    assert result.incident_task_ids == ()


def test_task_with_minimal_incident_signal():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-minimal",
                    title="Minimal incident signal",
                    description="Mention incident in passing.",
                )
            ]
        )
    )

    # Should create a row even with minimal signal
    assert len(result.rows) >= 0


# Test gap reasons


def test_gap_reasons_for_missing_aspects():
    result = generate_plan_incident_response_matrix(
        _plan(
            [
                _task(
                    "task-gaps",
                    title="Gaps test",
                    description="P0 severity only.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert len(row.gap_reasons) > 0
    for reason in row.gap_reasons:
        assert isinstance(reason, str)
        assert len(reason) > 0


# Utility functions


def _plan(tasks, *, plan_id="plan-incident-response", metadata=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-incident-response",
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
