from blueprint.task_runbook_update_readiness import (
    build_task_runbook_update_readiness_plan,
    task_runbook_update_readiness_plan_to_dicts,
)


def test_detects_runbook_playbook_sop_and_operational_procedure_updates():
    result = build_task_runbook_update_readiness_plan(
        _plan(
            [
                _task("runbook", "Update incident runbook", "Update the incident runbook."),
                _task("playbook", "Refresh playbook", "Revise the payment outage playbook."),
                _task("sop", "SOP edit", "Document the standard operating procedure."),
                _task("ops", "Operational procedure", "Add operational procedure notes."),
            ]
        )
    )

    assert result.impacted_task_ids == ("ops", "playbook", "runbook", "sop")
    assert result.summary["impacted_task_count"] == 4


def test_scores_trigger_owner_escalation_verification_mitigation_and_review_cadence():
    result = build_task_runbook_update_readiness_plan(
        _plan(
            [
                _task(
                    "ready",
                    "Update queue backpressure runbook",
                    "Update the runbook for queue saturation.",
                    acceptance_criteria=[
                        "Trigger conditions include alert threshold and customer-facing symptoms.",
                        "Owner escalation names the platform on-call and support tier contact.",
                        "Verification steps confirm queue depth and health checks recover.",
                        "Mitigation instructions include rollback, fallback, and recovery steps.",
                        "Review cadence is quarterly with next review owner.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.present_criteria == (
        "trigger_conditions",
        "owner_escalation",
        "verification_steps",
        "mitigation_instructions",
        "review_cadence",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_unrelated_tasks_are_ignored_and_partial_records_include_actions():
    result = build_task_runbook_update_readiness_plan(
        _plan(
            [
                _task("partial", "Runbook update", "Add owner and escalation contact to the runbook."),
                _task("docs", "Product docs", "Update user-facing product docs."),
            ]
        )
    )

    record = result.records[0]
    assert record.task_id == "partial"
    assert record.readiness == "partial"
    assert record.present_criteria == ("owner_escalation",)
    assert record.missing_criteria == (
        "trigger_conditions",
        "verification_steps",
        "mitigation_instructions",
        "review_cadence",
    )
    assert result.ignored_task_ids == ("docs",)
    assert task_runbook_update_readiness_plan_to_dicts(result) == result.to_dict()["records"]


def _plan(tasks):
    return {"id": "plan-runbook", "tasks": tasks}


def _task(task_id, title, description, **extra):
    return {"id": task_id, "title": title, "description": description, **extra}

