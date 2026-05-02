import copy
import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.plan_release_communication_matrix import (
    PlanReleaseCommunicationMatrixRow,
    build_plan_release_communication_matrix,
    generate_plan_release_communication_matrix,
    plan_release_communication_matrix_to_dict,
    plan_release_communication_matrix_to_dicts,
    plan_release_communication_matrix_to_markdown,
)


def test_brief_only_input_groups_release_communication_categories():
    result = build_plan_release_communication_matrix(
        _brief(
            scope=[
                "Prepare an internal announcement and customer announcement for admins.",
                "Publish release notes and a help center article.",
            ],
            risks=[
                "Support enablement needs a FAQ before launch.",
                "Sales enablement needs a battlecard for expansion conversations.",
            ],
        )
    )

    assert result.brief_id == "brief-release"
    assert result.plan_id is None
    assert [row.category for row in result.rows] == [
        "internal_announcement",
        "customer_announcement",
        "release_notes",
        "support_enablement",
        "sales_enablement",
    ]
    assert result.rows[0] == PlanReleaseCommunicationMatrixRow(
        category="internal_announcement",
        communication_status="needs_draft",
        affected_task_ids=(),
        evidence=(
            "brief.scope[0]: Prepare an internal announcement and customer announcement for admins",
        ),
        recommended_owner="product manager",
        recommended_channel="internal launch channel",
        required_questions=(
            "Which internal teams need launch timing, scope, owner, and escalation context?",
            "What decision, action, or awareness is expected from each internal audience?",
        ),
    )
    assert _row(result, "customer_announcement").recommended_owner == "customer marketing"
    assert _row(result, "sales_enablement").recommended_channel == "sales enablement workspace"


def test_plan_input_uses_task_ids_and_task_evidence():
    result = build_plan_release_communication_matrix(
        _plan(
            [
                _task(
                    "task-status",
                    title="Schedule status page maintenance notice",
                    description=(
                        "Launch includes downtime and an incident comms rollback message."
                    ),
                ),
                _task(
                    "task-support",
                    title="Draft support macro and release notes",
                    description="Support agents need triage guidance for customer-facing changes.",
                ),
            ]
        )
    )

    assert result.brief_id is None
    assert result.plan_id == "plan-release"
    assert _row(result, "status_page_update").affected_task_ids == ("task-status",)
    assert _row(result, "incident_comms").affected_task_ids == ("task-status",)
    assert _row(result, "support_enablement").affected_task_ids == ("task-support",)
    assert _row(result, "release_notes").affected_task_ids == ("task-support",)
    assert _row(result, "customer_announcement").affected_task_ids == ("task-support",)
    assert "task.task-status.title: Schedule status page maintenance notice" in _row(
        result, "status_page_update"
    ).evidence


def test_combined_brief_plus_plan_merges_evidence_without_mutating_inputs():
    brief = _brief(
        scope=["Customer announcement is required for the launch."],
        definition_of_done=["Release notes are approved and scheduled."],
    )
    plan = _plan(
        [
            _task(
                "task-customer",
                title="Draft customer email",
                description="Customer email is drafted and approved before rollout.",
                metadata={"comms": "In-app message is ready for customer announcement."},
            )
        ]
    )
    original_brief = copy.deepcopy(brief)
    original_plan = copy.deepcopy(plan)

    result = build_plan_release_communication_matrix(
        ImplementationBrief.model_validate(brief),
        ExecutionPlan.model_validate(plan),
    )
    payload = plan_release_communication_matrix_to_dict(result)

    assert brief == original_brief
    assert plan == original_plan
    assert result.brief_id == "brief-release"
    assert result.plan_id == "plan-release"
    assert _row(result, "customer_announcement").affected_task_ids == ("task-customer",)
    assert _row(result, "customer_announcement").communication_status == "needs_draft"
    assert len(_row(result, "customer_announcement").evidence) == 4
    assert _row(result, "release_notes").affected_task_ids == ()
    assert _row(result, "release_notes").communication_status == "needs_draft"
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert plan_release_communication_matrix_to_dicts(result) == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "plan_id", "summary", "rows"]
    assert list(payload["rows"][0]) == [
        "category",
        "communication_status",
        "affected_task_ids",
        "evidence",
        "recommended_owner",
        "recommended_channel",
        "required_questions",
    ]


def test_empty_and_invalid_inputs_return_empty_matrix_with_stable_summary():
    empty = build_plan_release_communication_matrix(
        _brief(scope=["Implement dashboard preferences."]),
        _plan(
            [
                _task(
                    "task-ui",
                    title="Add dashboard layout",
                    description="Render saved widgets and theme controls.",
                )
            ]
        ),
    )
    invalid = generate_plan_release_communication_matrix(object())

    expected_summary = {
        "category_count": 0,
        "affected_task_count": 0,
        "needs_plan_count": 0,
        "needs_draft_count": 0,
        "ready_to_schedule_count": 0,
    }
    assert empty.rows == ()
    assert empty.summary == expected_summary
    assert invalid.to_dict() == {
        "brief_id": None,
        "plan_id": None,
        "summary": expected_summary,
        "rows": [],
    }
    assert plan_release_communication_matrix_to_markdown(empty) == (
        "# Plan Release Communication Matrix: brief brief-release, plan plan-release\n\n"
        "No release communication rows were inferred."
    )


def test_stable_category_ordering_markdown_and_category_statuses():
    result = build_plan_release_communication_matrix(
        _plan(
            [
                _task(
                    "task-all",
                    title=(
                        "Internal announcement, customer announcement, status page, release notes, "
                        "support enablement, sales enablement, and incident comms are ready"
                    ),
                    description="Prepare launch communication matrix.",
                )
            ]
        )
    )

    assert [row.category for row in result.rows] == [
        "internal_announcement",
        "customer_announcement",
        "status_page_update",
        "release_notes",
        "support_enablement",
        "sales_enablement",
        "incident_comms",
    ]
    assert [row.communication_status for row in result.rows] == [
        "needs_draft",
        "needs_draft",
        "needs_plan",
        "needs_draft",
        "needs_draft",
        "needs_draft",
        "needs_plan",
    ]
    markdown = result.to_markdown()
    assert markdown.startswith("# Plan Release Communication Matrix: plan plan-release")
    assert (
        "| internal_announcement | needs_draft | task-all | task.task-all.title: "
        "Internal announcement"
    ) in markdown
    assert "| incident_comms | needs_plan | task-all |" in markdown


def _row(result, category):
    return next(row for row in result.rows if row.category == category)


def _brief(*, scope=None, risks=None, definition_of_done=None):
    return {
        "id": "brief-release",
        "source_brief_id": "source-release",
        "title": "Launch communications",
        "domain": "operations",
        "target_user": "support admins",
        "buyer": "support leadership",
        "workflow_context": "Account operations",
        "problem_statement": "Support needs safer account operations.",
        "mvp_goal": "Ship the first workflow.",
        "product_surface": "Admin console",
        "scope": scope or ["Admin workflow"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": "Use existing service boundaries.",
        "data_requirements": "Account state and workflow status.",
        "integration_points": [],
        "risks": risks or [],
        "validation_plan": "Run focused tests.",
        "definition_of_done": definition_of_done or ["Tests pass"],
        "status": "draft",
    }


def _plan(tasks):
    return {
        "id": "plan-release",
        "implementation_brief_id": "brief-release",
        "milestones": [],
        "tasks": tasks,
    }


def _task(task_id, *, title=None, description=None, metadata=None):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [],
        "acceptance_criteria": ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
