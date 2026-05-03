import json

from blueprint.plan_customer_communication_preferences_matrix import (
    PlanCustomerCommunicationPreferencesMatrix,
    PlanCustomerCommunicationPreferencesRow,
    analyze_plan_customer_communication_preferences_matrix,
    build_plan_customer_communication_preferences_matrix,
    plan_customer_communication_preferences_matrix_to_dict,
    plan_customer_communication_preferences_matrix_to_dicts,
    plan_customer_communication_preferences_matrix_to_markdown,
    summarize_plan_customer_communication_preferences_matrix,
)


def test_customer_communication_preference_rows_capture_required_signals():
    result = build_plan_customer_communication_preferences_matrix(
        _plan(
            [
                _task(
                    "task-preferences",
                    title="Add customer notification preferences",
                    description="Let tenant admins configure customer email, SMS, push, and in-app notification channels.",
                    acceptance_criteria=[
                        "Owner: Lifecycle team.",
                        "Audience segments and preference source include opt-out consent and frequency.",
                        "Dependency impact covers Braze and CRM integrations.",
                        "Verification uses preview seeds, suppression checks, deliverability tests, and audit logs.",
                        "Rollback disables sends with a kill switch and restores suppression lists.",
                        "Evidence records consent exports and approval ticket.",
                    ],
                ),
                _task("task-copy", title="Refresh dashboard copy", description="Update headings."),
            ]
        )
    )

    assert isinstance(result, PlanCustomerCommunicationPreferencesMatrix)
    assert isinstance(result.rows[0], PlanCustomerCommunicationPreferencesRow)
    assert result.communication_task_ids == ("task-preferences",)
    assert result.no_communication_task_ids == ("task-copy",)
    assert result.rows[0].readiness == "ready"
    assert result.rows[0].preference_source == "present"
    assert result.summary["readiness_counts"] == {"blocked": 0, "partial": 0, "ready": 1}


def test_missing_preference_source_or_rollback_blocks_and_aliases_work():
    result = build_plan_customer_communication_preferences_matrix(
        _plan(
            [
                _task(
                    "task-email-comms",
                    title="Send customer email communication",
                    description="Owner sends customer email notifications to account admins through SendGrid.",
                    acceptance_criteria=[
                        "Audience and channel are documented.",
                        "Dependency impact covers ESP integration.",
                        "Verification validates seed delivery and audit evidence.",
                    ],
                )
            ]
        )
    )

    row = result.rows[0]
    payload = plan_customer_communication_preferences_matrix_to_dict(result)

    assert row.readiness == "blocked"
    assert "Missing preference source." in row.gaps
    assert "Missing rollback." in row.gaps
    assert analyze_plan_customer_communication_preferences_matrix(result) is result
    assert summarize_plan_customer_communication_preferences_matrix(result) == result.summary
    assert plan_customer_communication_preferences_matrix_to_dicts(result) == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert "Plan Customer Communication Preferences Matrix" in plan_customer_communication_preferences_matrix_to_markdown(result)


def test_unrelated_plan_is_all_clear():
    result = build_plan_customer_communication_preferences_matrix(_plan([_task("task-api", title="Add API endpoint", description="Add a route.")]))

    assert result.rows == ()
    assert result.summary["communication_task_count"] == 0
    assert "No customer communication preference rows were inferred." in result.to_markdown()


def _plan(tasks):
    return {"id": "plan-customer-comms", "implementation_brief_id": "brief-comms", "milestones": [], "tasks": tasks}


def _task(task_id, *, title=None, description=None, acceptance_criteria=None):
    return {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
