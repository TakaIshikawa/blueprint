import copy
import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.plan_runbook_coverage_matrix import (
    PlanRunbookCoverageMatrix,
    PlanRunbookCoverageRow,
    build_plan_runbook_coverage_matrix,
    extract_plan_runbook_coverage_rows,
    generate_plan_runbook_coverage_matrix,
    plan_runbook_coverage_matrix_to_dict,
    plan_runbook_coverage_matrix_to_dicts,
    plan_runbook_coverage_matrix_to_markdown,
    summarize_plan_runbook_coverage_matrix,
)


def test_plan_only_input_groups_scenarios_with_affected_tasks_and_evidence():
    result = build_plan_runbook_coverage_matrix(
        _plan(
            [
                _task(
                    "task-deploy",
                    title="Deploy billing release",
                    description=(
                        "Production rollout runbook includes pre-checks, execution steps, "
                        "monitoring, rollback, and post-checks."
                    ),
                    files_or_modules=["deploy/billing.yml"],
                ),
                _task(
                    "task-repair",
                    title="Backfill invoice data repair",
                    description="Run a data repair backfill with monitoring only documented.",
                    files_or_modules=["src/backfills/invoices.py"],
                ),
            ],
            test_strategy="Release validation includes dashboards and rollback checks.",
        )
    )

    assert isinstance(result, PlanRunbookCoverageMatrix)
    assert [row.category for row in result.rows] == ["deploy", "rollback", "data_repair"]

    deploy = _row(result, "deploy")
    assert deploy.affected_task_ids == ("task-deploy",)
    assert deploy.coverage_status == "covered"
    assert deploy.missing_sections == ()
    assert any("task-deploy.task.description" in item for item in deploy.evidence)

    repair = _row(result, "data_repair")
    assert repair.affected_task_ids == ("task-repair",)
    assert repair.coverage_status == "partial"
    assert repair.missing_sections == ("pre_checks", "execution_steps", "rollback", "post_checks")
    assert result.summary["category_counts"]["data_repair"] == 1
    assert result.summary["affected_task_ids"] == ["task-deploy", "task-repair"]


def test_brief_and_task_level_evidence_contribute_to_same_row():
    plan = _plan(
        [
            _task(
                "task-vendor",
                title="Add vendor fallback mode",
                description="Handle third-party API outage with fallback and monitoring.",
                files_or_modules=["src/integrations/vendor_client.py"],
                metadata={
                    "runbook": {
                        "dependency_outage": {
                            "fallback": "Disable integration when vendor degradation breaches thresholds.",
                            "monitoring": "Watch vendor timeout dashboards.",
                        }
                    }
                },
            )
        ]
    )
    brief = _brief(
        risks=[
            "Vendor outage runbook must define escalation and customer communication.",
            "Support triage is needed for tickets during dependency outage.",
        ]
    )

    result = build_plan_runbook_coverage_matrix(
        ExecutionPlan.model_validate(plan),
        ImplementationBrief.model_validate(brief),
    )

    dependency = _row(result, "dependency_outage")
    assert dependency.affected_task_ids == ("task-vendor",)
    assert dependency.coverage_status == "covered"
    assert dependency.missing_sections == ()
    assert any("brief.risks[0]" in item for item in dependency.evidence)
    assert any(
        "task-vendor.task.metadata.runbook.dependency_outage.fallback" in item
        for item in dependency.evidence
    )

    support = _row(result, "support_triage")
    assert support.affected_task_ids == ()
    assert support.coverage_status == "missing"
    assert support.missing_sections == ("triage_steps", "escalation", "customer_communication")


def test_missing_and_covered_scenarios_are_statused_by_explicit_sections():
    result = build_plan_runbook_coverage_matrix(
        _plan(
            [
                _task(
                    "task-incident",
                    title="Incident response drill",
                    description="Incident response flow for checkout degradation.",
                ),
                _task(
                    "task-comms",
                    title="Customer communication templates",
                    description=(
                        "Customer communication runbook includes customer communication, "
                        "escalation, and post-checks."
                    ),
                    files_or_modules=["ops/comms/status_page.md"],
                ),
            ]
        )
    )

    incident = _row(result, "incident_response")
    comms = _row(result, "customer_communication")

    assert incident.coverage_status == "missing"
    assert incident.missing_sections == (
        "monitoring",
        "escalation",
        "customer_communication",
        "post_checks",
    )
    assert comms.coverage_status == "covered"
    assert comms.missing_sections == ()
    assert result.summary["missing_count"] == 1
    assert result.summary["covered_count"] == 1


def test_empty_invalid_and_low_signal_plans_return_stable_empty_matrix():
    empty = build_plan_runbook_coverage_matrix(
        {"id": "plan-empty", "implementation_brief_id": "brief", "tasks": []}
    )
    invalid = build_plan_runbook_coverage_matrix({"id": "plan-invalid", "tasks": "nope"})
    low_signal = build_plan_runbook_coverage_matrix(
        _plan([_task("task-copy", title="Update empty state", description="Clarify static copy.")])
    )

    expected_summary = {
        "scenario_count": 0,
        "covered_count": 0,
        "partial_count": 0,
        "missing_count": 0,
        "category_counts": {
            "deploy": 0,
            "rollback": 0,
            "incident_response": 0,
            "data_repair": 0,
            "support_triage": 0,
            "on_call_handoff": 0,
            "dependency_outage": 0,
            "customer_communication": 0,
        },
        "categories": [],
        "affected_task_ids": [],
    }
    assert empty.rows == ()
    assert invalid.rows == ()
    assert low_signal.rows == ()
    assert empty.summary == expected_summary
    assert "No operational runbook scenarios were found in the plan." in empty.to_markdown()


def test_serialization_markdown_and_helpers_are_stable():
    result = generate_plan_runbook_coverage_matrix(
        _plan(
            [
                _task(
                    "task-handoff",
                    title="On-call handoff runbook",
                    description="On-call handoff covers handoff, monitoring, and escalation.",
                    files_or_modules=["ops/oncall/handoff.md"],
                )
            ]
        )
    )
    extracted = extract_plan_runbook_coverage_rows(
        _plan([_task("task-handoff", title="On-call handoff runbook")])
    )
    payload = plan_runbook_coverage_matrix_to_dict(result)
    markdown = plan_runbook_coverage_matrix_to_markdown(result)

    assert all(isinstance(row, PlanRunbookCoverageRow) for row in result.records)
    assert result.to_dicts() == payload["rows"]
    assert plan_runbook_coverage_matrix_to_dicts(result) == payload["rows"]
    assert plan_runbook_coverage_matrix_to_dicts(result.rows) == payload["records"]
    assert summarize_plan_runbook_coverage_matrix(result) == result.summary
    assert extracted[0].category == "on_call_handoff"
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "implementation_brief_id", "rows", "summary", "records"]
    assert list(payload["rows"][0]) == [
        "category",
        "affected_task_ids",
        "evidence",
        "coverage_status",
        "missing_sections",
        "recommended_runbook_sections",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Plan Runbook Coverage Matrix: plan-runbook")
    assert "| on_call_handoff | covered | task-handoff | none |" in markdown


def test_execution_plan_and_implementation_brief_inputs_are_not_mutated():
    plan = _plan(
        [
            _task(
                "task-rollback",
                title="Document rollback process",
                description="Rollback runbook includes rollback, monitoring, escalation, and post-checks.",
            )
        ]
    )
    brief = _brief(
        definition_of_done=["Customer communication is ready for rollback status page updates."]
    )
    original_plan = copy.deepcopy(plan)
    original_brief = copy.deepcopy(brief)

    result = build_plan_runbook_coverage_matrix(plan, brief)

    assert plan == original_plan
    assert brief == original_brief
    assert _row(result, "rollback").coverage_status == "covered"
    assert _row(result, "customer_communication").coverage_status == "partial"


def _row(result, category):
    return next(row for row in result.rows if row.category == category)


def _plan(tasks, **overrides):
    plan = {
        "id": "plan-runbook",
        "implementation_brief_id": "brief-runbook",
        "milestones": [],
        "tasks": tasks,
    }
    plan.update(overrides)
    return plan


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task


def _brief(**overrides):
    brief = {
        "id": "brief-runbook",
        "source_brief_id": "source-runbook",
        "title": "Runbook coverage brief",
        "problem_statement": "Operational changes need documented runbook coverage.",
        "mvp_goal": "Plan operational readiness.",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Run tests.",
        "definition_of_done": [],
    }
    brief.update(overrides)
    return brief
