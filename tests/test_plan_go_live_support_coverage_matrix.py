import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_go_live_support_coverage_matrix import (
    PlanGoLiveSupportCoverageMatrix,
    PlanGoLiveSupportCoverageRow,
    build_plan_go_live_support_coverage_matrix,
    derive_plan_go_live_support_coverage_matrix,
    generate_plan_go_live_support_coverage_matrix,
    plan_go_live_support_coverage_matrix_to_dict,
    plan_go_live_support_coverage_matrix_to_markdown,
    summarize_plan_go_live_support_coverage_matrix,
)


def test_complete_go_live_support_coverage_returns_complete_row_and_area_counts():
    result = build_plan_go_live_support_coverage_matrix(
        _plan(
            [
                _task(
                    "task-launch",
                    title="Launch customer-facing billing dashboard",
                    description=(
                        "Go-live for customers includes a launch support owner, on-call "
                        "coverage, escalation route, runbook, status page updates, "
                        "office hours, known issue triage, and post-launch monitoring."
                    ),
                    acceptance_criteria=[
                        "Support macros are ready for expected billing questions.",
                        "Customer success handoff is complete for account teams.",
                    ],
                    metadata={"support_owner": "Support Launch DRI"},
                )
            ]
        )
    )

    assert isinstance(result, PlanGoLiveSupportCoverageMatrix)
    assert result.plan_id == "plan-go-live-support"
    assert len(result.rows) == 1
    row = result.rows[0]
    assert isinstance(row, PlanGoLiveSupportCoverageRow)
    assert row.task_id == "task-launch"
    assert row.coverage_status == "complete"
    assert row.present_coverage == (
        "launch_support_owner",
        "on_call_coverage",
        "support_macros",
        "escalation_routes",
        "customer_success_handoff",
        "status_page_updates",
        "runbook_links",
        "office_hours",
        "known_issue_triage",
        "post_launch_monitoring",
    )
    assert row.missing_coverage == ()
    assert row.missing_coverage_flags == ()
    assert "Support Launch DRI" in row.team_owner_hints
    assert result.summary["complete_count"] == 1
    assert result.summary["partial_count"] == 0
    assert result.summary["missing_count"] == 0
    assert all(count == 1 for count in result.summary["support_area_counts"].values())


def test_missing_go_live_coverage_flags_each_absent_support_area():
    result = build_plan_go_live_support_coverage_matrix(
        _plan(
            [
                _task(
                    "task-release",
                    title="Release customer notification workflow",
                    description="Launch user-visible email notifications to customers.",
                    acceptance_criteria=["Notification sends successfully."],
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.coverage_status == "missing"
    assert row.present_coverage == ()
    assert row.missing_coverage == (
        "launch_support_owner",
        "on_call_coverage",
        "support_macros",
        "escalation_routes",
        "customer_success_handoff",
        "status_page_updates",
        "runbook_links",
        "office_hours",
        "known_issue_triage",
        "post_launch_monitoring",
    )
    assert row.missing_coverage_flags == (
        "Assign a launch support owner or DRI.",
        "Define on-call or launch-watch coverage.",
        "Prepare support macros or agent scripts.",
        "Document escalation routes for launch issues.",
        "Hand off customer success context and account guidance.",
        "Plan status page or customer status updates.",
        "Link the launch support runbook or playbook.",
        "Schedule office hours or a support clinic.",
        "Document known issue triage and workarounds.",
        "Define post-launch monitoring dashboards, alerts, or checks.",
    )
    assert row.team_owner_hints == ("Launch support lead",)
    assert result.summary["missing_support_area_counts"]["support_macros"] == 1
    assert result.summary["missing_coverage_flag_count"] == 10


def test_partial_coverage_and_no_impact_task_ids_are_summarized():
    result = build_plan_go_live_support_coverage_matrix(
        _plan(
            [
                _task(
                    "task-visible",
                    title="Rollout customer checkout changes",
                    description="Customer-facing rollout with support macros and runbook link.",
                    acceptance_criteria=["Post-launch monitoring dashboard is ready."],
                ),
                _task(
                    "task-internal",
                    title="Refactor parser internals",
                    description="Internal only refactor with no customer impact.",
                    files_or_modules=["src/blueprint/parser.py"],
                ),
            ]
        )
    )

    assert [row.task_id for row in result.rows] == ["task-visible"]
    assert result.rows[0].coverage_status == "partial"
    assert result.rows[0].present_coverage == (
        "support_macros",
        "runbook_links",
        "post_launch_monitoring",
    )
    assert result.summary["total_task_count"] == 2
    assert result.summary["launch_facing_task_count"] == 1
    assert result.summary["no_impact_task_ids"] == ["task-internal"]
    assert result.summary["support_area_counts"]["runbook_links"] == 1
    assert result.summary["support_area_counts"]["office_hours"] == 0


def test_no_impact_plan_returns_empty_matrix_and_markdown_empty_state():
    result = build_plan_go_live_support_coverage_matrix(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update README copy",
                    description="Documentation only wording cleanup with no customer impact.",
                    files_or_modules=["docs/readme.md"],
                )
            ],
            plan_id="plan-empty",
        )
    )

    assert result.rows == ()
    assert result.summary == {
        "total_task_count": 1,
        "launch_facing_task_count": 0,
        "no_impact_task_ids": ["task-docs"],
        "support_area_counts": {
            "launch_support_owner": 0,
            "on_call_coverage": 0,
            "support_macros": 0,
            "escalation_routes": 0,
            "customer_success_handoff": 0,
            "status_page_updates": 0,
            "runbook_links": 0,
            "office_hours": 0,
            "known_issue_triage": 0,
            "post_launch_monitoring": 0,
        },
        "missing_support_area_counts": {
            "launch_support_owner": 0,
            "on_call_coverage": 0,
            "support_macros": 0,
            "escalation_routes": 0,
            "customer_success_handoff": 0,
            "status_page_updates": 0,
            "runbook_links": 0,
            "office_hours": 0,
            "known_issue_triage": 0,
            "post_launch_monitoring": 0,
        },
        "complete_count": 0,
        "partial_count": 0,
        "missing_count": 0,
        "status_counts": {"complete": 0, "partial": 0, "missing": 0},
        "missing_coverage_flag_count": 0,
        "launch_facing_task_ids": [],
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Go-Live Support Coverage Matrix: plan-empty",
            "",
            "## Summary",
            "",
            "- Total tasks: 1",
            "- Launch-facing tasks: 0",
            "- Complete coverage: 0",
            "- Partial coverage: 0",
            "- Missing coverage: 0",
            "- Support area counts: launch_support_owner 0, on_call_coverage 0, support_macros 0, escalation_routes 0, customer_success_handoff 0, status_page_updates 0, runbook_links 0, office_hours 0, known_issue_triage 0, post_launch_monitoring 0",
            "- No-impact tasks: task-docs",
            "",
            "No launch-facing tasks were found for go-live support coverage.",
        ]
    )


def test_model_mapping_object_inputs_aliases_serialization_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Launch payment settings | wave 1",
                description="Release customer-facing payment settings with support macros.",
                metadata={"on_call_coverage": "Payments on-call"},
            )
        ],
        plan_id="plan-model",
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)
    task_model = ExecutionTask.model_validate(
        _task(
            "task-task-model",
            title="Customer status page release",
            description="Go-live customer status page updates and escalation route.",
        )
    )
    object_result = build_plan_go_live_support_coverage_matrix(
        _TaskObject(
            id="task-object",
            title="Launch admin office hours",
            description="Launch customer-visible admin feature with office hours.",
        )
    )

    result = build_plan_go_live_support_coverage_matrix(model)
    generated = generate_plan_go_live_support_coverage_matrix(plan)
    derived = derive_plan_go_live_support_coverage_matrix(result)
    summarized = summarize_plan_go_live_support_coverage_matrix(task_model)
    payload = plan_go_live_support_coverage_matrix_to_dict(result)
    markdown = plan_go_live_support_coverage_matrix_to_markdown(result)

    assert plan == original
    assert generated.to_dict() == result.to_dict()
    assert derived is result
    assert summarized.rows[0].present_coverage == ("escalation_routes", "status_page_updates")
    assert object_result.rows[0].present_coverage == ("office_hours",)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert result.records == result.rows
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "summary"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "task_title",
        "coverage_status",
        "present_coverage",
        "missing_coverage",
        "missing_coverage_flags",
        "team_owner_hints",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Plan Go-Live Support Coverage Matrix: plan-model")
    assert "Launch payment settings \\| wave 1" in markdown


def test_invalid_input_and_invalid_task_collection_return_empty_matrix():
    invalid_plan = build_plan_go_live_support_coverage_matrix(
        {"id": "plan-invalid", "tasks": "not a list"}
    )
    invalid_source = build_plan_go_live_support_coverage_matrix(object())

    assert invalid_plan.plan_id == "plan-invalid"
    assert invalid_plan.rows == ()
    assert invalid_plan.summary["total_task_count"] == 0
    assert invalid_source.rows == ()
    assert invalid_source.summary["total_task_count"] == 0


class _TaskObject:
    def __init__(self, id, title, description):
        self.id = id
        self.title = title
        self.description = description
        self.acceptance_criteria = ["Done"]
        self.files_or_modules = []


def _plan(tasks, *, plan_id="plan-go-live-support", metadata=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-go-live-support",
        "milestones": [{"name": "Launch"}],
        "metadata": {} if metadata is None else metadata,
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
        "metadata": {} if metadata is None else metadata,
    }
