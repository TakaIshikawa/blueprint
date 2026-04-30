from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.plan_scope_drift import (
    PlanScopeDrift,
    detect_plan_scope_drift,
    plan_scope_drift_to_dicts,
    summarize_plan_scope_drift,
)


def test_non_goal_matching_tasks_are_high_severity_drift():
    drifts = detect_plan_scope_drift(
        _brief(non_goals=["Add Slack notification integration"]),
        _plan(
            tasks=[
                _task(
                    "task-slack",
                    "Add Slack notifications",
                    description="Build the Slack notification integration for release events.",
                    acceptance=["Slack messages are sent for every release."],
                )
            ]
        ),
    )

    assert [drift.to_dict() for drift in drifts] == [
        {
            "task_id": "task-slack",
            "title": "Add Slack notifications",
            "drift_type": "non_goal_conflict",
            "matched_phrase": "Add Slack notification integration",
            "evidence": [
                "title: Add Slack notifications",
                "description: Build the Slack notification integration for release events.",
            ],
            "severity": "high",
        }
    ]


def test_tasks_without_scope_or_done_overlap_are_potential_drift_in_plan_order():
    drifts = detect_plan_scope_drift(
        _brief(),
        _plan(
            tasks=[
                _task(
                    "task-theme",
                    "Refresh marketing theme",
                    description="Update the homepage color palette and campaign copy.",
                    files=["src/marketing/theme.css"],
                    acceptance=["Campaign page has new styling."],
                ),
                _task(
                    "task-billing",
                    "Add invoice export",
                    description="Generate downloadable invoice CSV files.",
                    files=["src/billing/export.py"],
                    acceptance=["Invoice CSV export includes invoice status totals."],
                ),
                _task(
                    "task-mobile",
                    "Create mobile onboarding",
                    description="Build a new native onboarding checklist.",
                    files=["mobile/onboarding.swift"],
                    acceptance=["Native onboarding checklist is visible."],
                ),
            ]
        ),
    )

    assert [drift.task_id for drift in drifts] == ["task-theme", "task-mobile"]
    assert all(isinstance(drift, PlanScopeDrift) for drift in drifts)
    assert all(drift.drift_type == "potential_scope_drift" for drift in drifts)
    assert all(drift.severity == "medium" for drift in drifts)
    assert drifts[0].matched_phrase is None
    assert drifts[0].evidence == (
        "title: Refresh marketing theme",
        "description: Update the homepage color palette and campaign copy.",
        "files_or_modules: src/marketing/theme.css",
    )


def test_explicit_scope_or_definition_of_done_evidence_is_not_flagged():
    drifts = detect_plan_scope_drift(
        _brief(),
        _plan(
            tasks=[
                _task(
                    "task-report",
                    "Render invoice status report",
                    description="Implement the status report rows for billing reviewers.",
                    acceptance=["Report rows show invoice status for reviewers."],
                ),
                _task(
                    "task-export",
                    "Validate invoice CSV export",
                    description="Add tests for the final CSV output.",
                    acceptance=["Invoice CSV export includes invoice status totals."],
                ),
            ]
        ),
    )

    assert drifts == []


def test_model_inputs_and_summary_are_serializable_and_stable():
    drifts = detect_plan_scope_drift(
        ImplementationBrief(
            id="brief-scope",
            source_brief_id="source-scope",
            title="Billing reporting",
            problem_statement="Billing reviewers cannot inspect invoice status.",
            mvp_goal="Show invoice status reports.",
            scope=["Render invoice status report rows"],
            non_goals=["Add native mobile onboarding"],
            assumptions=[],
            risks=[],
            validation_plan="Run focused report tests.",
            definition_of_done=["Invoice CSV export includes invoice status totals."],
        ),
        ExecutionPlan(
            id="plan-scope",
            implementation_brief_id="brief-scope",
            milestones=[],
            tasks=[
                {
                    "id": "task-mobile",
                    "title": "Add mobile onboarding",
                    "description": "Create native mobile onboarding for billing users.",
                    "files_or_modules": ["mobile/onboarding.swift"],
                    "acceptance_criteria": ["Mobile onboarding appears after login."],
                },
                {
                    "id": "task-theme",
                    "title": "Refresh marketing theme",
                    "description": "Update the campaign landing page.",
                    "files_or_modules": ["src/marketing/theme.css"],
                    "acceptance_criteria": ["Landing page uses the new campaign style."],
                },
            ],
        ),
    )

    assert summarize_plan_scope_drift(drifts) == {"high": 1, "medium": 1, "total": 2}
    assert plan_scope_drift_to_dicts(drifts) == [
        {
            "task_id": "task-mobile",
            "title": "Add mobile onboarding",
            "drift_type": "non_goal_conflict",
            "matched_phrase": "Add native mobile onboarding",
            "evidence": [
                "title: Add mobile onboarding",
                "description: Create native mobile onboarding for billing users.",
            ],
            "severity": "high",
        },
        {
            "task_id": "task-theme",
            "title": "Refresh marketing theme",
            "drift_type": "potential_scope_drift",
            "matched_phrase": None,
            "evidence": [
                "title: Refresh marketing theme",
                "description: Update the campaign landing page.",
                "files_or_modules: src/marketing/theme.css",
            ],
            "severity": "medium",
        },
    ]


def _brief(*, non_goals: list[str] | None = None) -> dict:
    return {
        "id": "brief-scope",
        "mvp_goal": "Show invoice status reports.",
        "scope": ["Render invoice status report rows for billing reviewers"],
        "non_goals": non_goals or [],
        "definition_of_done": ["Invoice CSV export includes invoice status totals."],
    }


def _plan(*, tasks: list[dict]) -> dict:
    return {
        "id": "plan-scope",
        "tasks": tasks,
    }


def _task(
    task_id: str,
    title: str,
    *,
    description: str | None = None,
    files: list[str] | None = None,
    acceptance: list[str] | None = None,
) -> dict:
    return {
        "id": task_id,
        "title": title,
        "description": description or f"Implement {title}.",
        "files_or_modules": files or [],
        "acceptance_criteria": acceptance or [f"{title} is complete."],
    }
