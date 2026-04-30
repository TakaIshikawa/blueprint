import copy
import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.plan_assumption_tracker import (
    build_plan_assumption_tracker,
    plan_assumption_tracker_to_dict,
    plan_assumption_tracker_to_markdown,
)


def test_reports_fully_covered_assumptions_from_acceptance_criteria():
    result = build_plan_assumption_tracker(
        _brief(
            assumptions=[
                "Warehouse inventory feed is available during order allocation.",
            ]
        ),
        _plan(
            [
                _task(
                    "task-inventory",
                    title="Integrate warehouse inventory feed",
                    description="Use inventory feed values during order allocation.",
                    acceptance_criteria=[
                        "Validation covers warehouse inventory feed availability during allocation.",
                    ],
                )
            ]
        ),
    )

    assert len(result.coverage) == 1
    item = result.coverage[0]
    assert item.assumption_id == "assumption-1"
    assert item.assumption == "Warehouse inventory feed is available during order allocation."
    assert item.matched_task_ids == ("task-inventory",)
    assert item.validation_evidence == (
        "acceptance_criteria[0]: Validation covers warehouse inventory feed availability during allocation.",
    )
    assert item.status == "validated"
    assert item.follow_up_task == ""
    assert item.matched_terms == (
        "allocation",
        "during",
        "feed",
        "inventory",
        "order",
        "warehouse",
    )
    assert result.summary == {
        "assumption_count": 1,
        "validated_count": 1,
        "unvalidated_count": 0,
        "unowned_count": 0,
        "contradicted_count": 0,
        "follow_up_count": 0,
    }


def test_reports_uncovered_assumptions_with_actionable_follow_up():
    result = build_plan_assumption_tracker(
        _brief(assumptions=["Customers can receive SMS notifications in Canada."]),
        _plan(
            [
                _task(
                    "task-email",
                    title="Send email notifications",
                    description="Deliver email receipts for completed orders.",
                )
            ]
        ),
    )

    item = result.coverage[0]
    assert item.matched_task_ids == ()
    assert item.validation_evidence == ()
    assert item.status == "unowned"
    assert item.follow_up_task == (
        "Add a task to own and validate assumption: Customers can receive SMS notifications in Canada."
    )


def test_reports_unvalidated_assumptions_when_task_owns_without_evidence():
    result = build_plan_assumption_tracker(
        _brief(assumptions=["Admin users have billing export permission."]),
        _plan(
            [
                _task(
                    "task-billing",
                    title="Add billing export permission checks",
                    description="Gate billing export actions to admin users.",
                    acceptance_criteria=["Billing export appears in the admin menu."],
                )
            ]
        ),
    )

    item = result.coverage[0]
    assert item.matched_task_ids == ("task-billing",)
    assert item.validation_evidence == ()
    assert item.status == "unvalidated"
    assert item.follow_up_task == (
        "Add acceptance criteria or a test command to tasks task-billing "
        "that validates assumption: Admin users have billing export permission."
    )


def test_extracts_validation_evidence_from_test_command():
    result = build_plan_assumption_tracker(
        _brief(assumptions=["Stripe customer ids are present on renewal invoices."]),
        _plan(
            [
                _task(
                    "task-renewals",
                    title="Parse renewal invoices",
                    description="Read Stripe customer ids from renewal invoice payloads.",
                    acceptance_criteria=["Renewal invoice parsing is implemented."],
                    test_command="pytest tests/test_renewal_invoices.py --validate-stripe-customer-ids",
                )
            ]
        ),
    )

    item = result.coverage[0]
    assert item.status == "validated"
    assert item.validation_evidence == (
        "test_command: pytest tests/test_renewal_invoices.py --validate-stripe-customer-ids",
    )


def test_reports_contradictory_task_language_before_missing_validation():
    result = build_plan_assumption_tracker(
        _brief(assumptions=["Mobile checkout supports guest coupon redemption."]),
        _plan(
            [
                _task(
                    "task-mobile-checkout",
                    title="Update mobile checkout coupon flow",
                    description="Wire coupon entry into mobile checkout.",
                    acceptance_criteria=[
                        "Guest coupon redemption is not supported in mobile checkout.",
                    ],
                )
            ]
        ),
    )

    item = result.coverage[0]
    assert item.matched_task_ids == ("task-mobile-checkout",)
    assert item.status == "contradicted"
    assert item.validation_evidence == (
        "acceptance_criteria[0]: Guest coupon redemption is not supported in mobile checkout.",
    )
    assert item.follow_up_task == (
        "Resolve contradictory acceptance criteria in tasks task-mobile-checkout "
        "or update the brief assumption: Mobile checkout supports guest coupon redemption."
    )


def test_model_inputs_do_not_mutate_and_serialize_stably():
    brief = _brief(
        assumptions=[
            "Warehouse inventory feed is available during order allocation.",
            "Customers can receive SMS notifications in Canada.",
            "Admin users have billing export permission.",
        ]
    )
    plan = _plan(
        [
            _task(
                "task-inventory",
                title="Integrate warehouse inventory feed",
                description="Use inventory feed values during order allocation.",
                acceptance_criteria=[
                    "Validation covers warehouse inventory feed availability during allocation.",
                ],
            ),
            _task(
                "task-billing",
                title="Add billing export permission checks",
                description="Gate billing export actions to admin users.",
                acceptance_criteria=["Billing export appears in the admin menu."],
            ),
        ]
    )
    original_brief = copy.deepcopy(brief)
    original_plan = copy.deepcopy(plan)

    result = build_plan_assumption_tracker(
        ImplementationBrief.model_validate(brief),
        ExecutionPlan.model_validate(plan),
    )
    payload = plan_assumption_tracker_to_dict(result)

    assert brief == original_brief
    assert plan == original_plan
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["coverage"]
    assert list(payload) == ["brief_id", "plan_id", "coverage", "summary"]
    assert list(payload["coverage"][0]) == [
        "assumption_id",
        "assumption",
        "matched_task_ids",
        "validation_evidence",
        "status",
        "follow_up_task",
        "matched_terms",
    ]
    assert [item["status"] for item in payload["coverage"]] == [
        "unowned",
        "unvalidated",
        "validated",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert payload["summary"] == {
        "assumption_count": 3,
        "validated_count": 1,
        "unvalidated_count": 1,
        "unowned_count": 1,
        "contradicted_count": 0,
        "follow_up_count": 2,
    }


def test_stable_markdown_output_and_empty_state():
    result = build_plan_assumption_tracker(
        _brief(
            assumptions=[
                "Analytics export includes finance approval status.",
                "Partner data import remains under five minutes.",
            ]
        ),
        _plan(
            [
                _task(
                    "task-analytics",
                    title="Update analytics export",
                    description="Add finance approval status to analytics export.",
                    acceptance_criteria=[
                        "Tests verify analytics export includes finance approval status.",
                    ],
                )
            ]
        ),
    )

    assert plan_assumption_tracker_to_markdown(result) == "\n".join(
        [
            "# Plan Assumption Tracker: plan-assumptions",
            "",
            "| Assumption | Status | Tasks | Validation Evidence | Follow-up |",
            "| --- | --- | --- | --- | --- |",
            "| Partner data import remains under five minutes. | unowned | None | None | Add a task to own and validate assumption: Partner data import remains under five minutes. |",
            "| Analytics export includes finance approval status. | validated | task-analytics | acceptance_criteria[0]: Tests verify analytics export includes finance approval status. | None |",
        ]
    )

    empty = build_plan_assumption_tracker(_brief(assumptions=[]), _plan([]))
    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Assumption Tracker: plan-assumptions",
            "",
            "No brief assumptions were provided.",
        ]
    )


def _brief(**overrides):
    payload = {
        "id": "brief-assumptions",
        "source_brief_id": "source-assumptions",
        "title": "Assumption tracker plan",
        "domain": "retail",
        "target_user": "Operations manager",
        "buyer": "VP Operations",
        "workflow_context": "Daily exception review",
        "problem_statement": "The current workflow buries assumptions in prose.",
        "mvp_goal": "Make assumptions explicit and validated.",
        "product_surface": "Admin dashboard",
        "scope": ["Track assumption coverage"],
        "non_goals": [],
        "assumptions": ["Warehouse inventory feed is available."],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Validate assumptions through tasks.",
        "definition_of_done": ["Assumption coverage is reported."],
    }
    payload.update(overrides)
    return payload


def _plan(tasks):
    return {
        "id": "plan-assumptions",
        "implementation_brief_id": "brief-assumptions",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    acceptance_criteria=None,
    files_or_modules=None,
    test_command=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": description,
        "acceptance_criteria": acceptance_criteria or ["Behavior is implemented."],
        "files_or_modules": files_or_modules or [],
        "metadata": {"tags": tags or []},
        "status": "pending",
    }
    if test_command is not None:
        task["test_command"] = test_command
    return task
