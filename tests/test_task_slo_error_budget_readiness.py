import json

from blueprint.task_slo_error_budget_readiness import (
    analyze_task_slo_error_budget_readiness,
    build_task_slo_error_budget_readiness_plan,
    task_slo_error_budget_readiness_plan_to_dict,
    task_slo_error_budget_readiness_plan_to_dicts,
    task_slo_error_budget_readiness_plan_to_markdown,
)


def test_complete_error_budget_policy_task_is_ready():
    result = build_task_slo_error_budget_readiness_plan(
        _plan(
            [
                _task(
                    "slo-ready",
                    "Add checkout error budget policy",
                    (
                        "Error budget and burn rate policy for checkout SLO gate. SLI is request success rate "
                        "with a 99.9% SLO over 30 days. Burn rate thresholds cover fast burn and slow burn. "
                        "Alert routing sends PagerDuty and Slack notifications to on-call. Release gate behavior "
                        "blocks deploys when budget is exhausted. Service owner escalation is assigned. Monthly "
                        "reporting cadence goes to the service review. Exception process requires waiver approval."
                    ),
                    ["sre/error_budget/slo_gate.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.present_criteria == (
        "sli_slo_definition",
        "burn_rate_thresholds",
        "alert_routing",
        "release_gate_behavior",
        "owner_escalation",
        "reporting_cadence",
        "exception_process",
    )
    assert record.missing_criteria == ()


def test_partial_plan_reports_missing_safeguards_without_exact_wording():
    result = analyze_task_slo_error_budget_readiness(
        _plan(
            [
                _task(
                    "slo-partial",
                    "Reliability budget gate",
                    "Reliability budget work defines an SLO target and release gate but no routing or exception details.",
                    ["ops/reliability_budget_gate.py"],
                ),
                _task("copy", "Docs", "No error budget changes are required.", []),
            ]
        )
    )

    record = result.records[0]
    assert result.impacted_task_ids == ("slo-partial",)
    assert result.ignored_task_ids == ("copy",)
    assert {"error_budget", "slo_gate"} <= set(record.detected_signals)
    assert "sli_slo_definition" in record.present_criteria
    assert "release_gate_behavior" in record.present_criteria
    assert "burn_rate_thresholds" in record.missing_criteria
    assert "alert_routing" in record.missing_criteria
    assert any(action.startswith("Set burn-rate thresholds") for action in record.recommended_follow_up_actions)


def test_detects_error_budget_burn_rate_slo_gate_and_reliability_budget_tasks():
    result = build_task_slo_error_budget_readiness_plan(
        _plan(
            [
                _task("budget", "Error budget", "Track remaining budget and reporting cadence.", []),
                _task("burn", "Burn rate", "Add multi-window burn rate alerts.", ["sre/burn_rate.py"]),
                _task("gate", "SLO gate", "Block deploy with service owner escalation.", ["deploy/slo_gate.py"]),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert "error_budget" in by_id["budget"].detected_signals
    assert "burn_rate" in by_id["burn"].detected_signals
    assert "slo_gate" in by_id["gate"].detected_signals


def test_serialization_and_markdown_are_stable():
    result = build_task_slo_error_budget_readiness_plan(_plan([_task("alias", "Budget", "Error budget with SLO target.", [])]))
    payload = task_slo_error_budget_readiness_plan_to_dict(result)

    assert task_slo_error_budget_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Task SLO Error Budget Readiness: plan-slo-error-budget" in task_slo_error_budget_readiness_plan_to_markdown(result)


def _plan(tasks):
    return {"id": "plan-slo-error-budget", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
