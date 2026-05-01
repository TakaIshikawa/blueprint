import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_accessibility_impact import (
    TaskAccessibilityImpactPlan,
    TaskAccessibilityImpactRecord,
    build_task_accessibility_impact_plan,
    summarize_task_accessibility_impacts,
    task_accessibility_impact_plan_to_dict,
    task_accessibility_impact_plan_to_markdown,
)


def test_frontend_form_navigation_and_error_tasks_receive_targeted_review_areas():
    result = build_task_accessibility_impact_plan(
        _plan(
            [
                _task(
                    "task-checkout",
                    title="Checkout form validation",
                    description=(
                        "Add a payment form with inline error messages, focus restoration, "
                        "keyboard support, and screen reader announcements."
                    ),
                    files_or_modules=[
                        "src/frontend/routes/checkout/PaymentForm.tsx",
                        "src/frontend/forms/payment_errors.ts",
                    ],
                    tags=["a11y", "navigation"],
                    metadata={"review": {"assistive_tech": "VoiceOver screen reader"}},
                ),
                _task(
                    "task-theme",
                    title="Refresh dashboard colors",
                    description="Update UI theme colors and status icon contrast.",
                    files_or_modules=["src/frontend/styles/theme.css"],
                    acceptance_criteria=["WCAG contrast checks pass and status is not color-only."],
                ),
            ]
        )
    )

    checkout = _record(result, "task-checkout")
    assert checkout.severity == "high"
    assert checkout.review_areas == (
        "ui_semantics",
        "navigation",
        "keyboard",
        "focus_management",
        "forms",
        "error_messages",
        "screen_reader",
    )
    assert any(
        check.startswith("Verify every changed interactive path works with keyboard alone")
        for check in checkout.required_checks
    )
    assert (
        "Add acceptance criteria proving the changed interaction works with keyboard alone."
        in checkout.missing_acceptance_criteria
    )
    assert "metadata.review.assistive_tech: VoiceOver screen reader" in checkout.evidence

    theme = _record(result, "task-theme")
    assert theme.severity == "medium"
    assert theme.review_areas == ("ui_semantics", "color_contrast", "media")
    assert theme.missing_acceptance_criteria == (
        "Add acceptance criteria covering semantic HTML, landmarks, headings, roles, and accessible names.",
        "Add acceptance criteria covering alt text, captions, transcripts, decorative media, and motion controls.",
    )
    assert result.summary["severity_counts"] == {"high": 1, "medium": 1, "low": 0}
    assert result.summary["review_area_counts"]["forms"] == 1
    assert result.summary["review_area_counts"]["color_contrast"] == 1


def test_media_document_and_screen_reader_signals_are_detected_from_all_task_fields():
    result = build_task_accessibility_impact_plan(
        _plan(
            [
                _task(
                    "task-report",
                    title="Accessible PDF report export",
                    description=(
                        "Generate documents with semantic headings, reading order, link text, "
                        "alt text, captions, and screen reader labels."
                    ),
                    files_or_modules=[
                        "src/reports/exports/account_report.pdf",
                        "docs/reports/accessibility.md",
                    ],
                    acceptance_criteria=[
                        "Accessibility review verifies document headings, reading order, "
                        "alt text, captions, and screen reader labels."
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.severity == "high"
    assert record.review_areas == ("ui_semantics", "media", "documents", "screen_reader")
    assert record.missing_acceptance_criteria == ()
    assert result.accessibility_task_ids == ("task-report",)
    assert result.low_impact_task_ids == ()


def test_no_accessibility_signals_returns_deterministic_low_impact_result():
    result = build_task_accessibility_impact_plan(
        _plan(
            [
                _task(
                    "task-cache",
                    title="Tune cache expiration",
                    description="Adjust backend cache TTL for expensive report queries.",
                    files_or_modules=["src/cache/report_cache.py"],
                    acceptance_criteria=["Unit tests cover cache refresh behavior."],
                )
            ]
        )
    )

    assert result.records == (
        TaskAccessibilityImpactRecord(
            task_id="task-cache",
            task_title="Tune cache expiration",
            severity="low",
            review_areas=("general_accessibility",),
            required_checks=(
                "Confirm the task does not change user-facing accessibility behavior.",
                "Keep the smallest relevant validation note with the task evidence.",
            ),
            missing_acceptance_criteria=(),
            evidence=(),
        ),
    )
    assert result.summary["accessibility_task_count"] == 0
    assert result.summary["low_impact_task_count"] == 1
    assert result.summary["missing_acceptance_criteria_count"] == 0


def test_model_input_serializes_stably_and_renders_markdown():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-nav",
                    title="Keyboard navigation menu",
                    description="Update menu navigation and focus visible behavior.",
                    files_or_modules=["src/frontend/navigation/Menu.tsx"],
                    acceptance_criteria=["Keyboard navigation and focus behavior are verified."],
                )
            ]
        )
    )

    result = summarize_task_accessibility_impacts(plan)
    payload = task_accessibility_impact_plan_to_dict(result)

    assert isinstance(result, TaskAccessibilityImpactPlan)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert list(payload) == [
        "plan_id",
        "records",
        "accessibility_task_ids",
        "low_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "task_title",
        "severity",
        "review_areas",
        "required_checks",
        "missing_acceptance_criteria",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert task_accessibility_impact_plan_to_markdown(result) == "\n".join(
        [
            "# Task Accessibility Impact Plan: plan-accessibility",
            "",
            "| Task | Severity | Review Areas | Required Checks | Missing Acceptance Criteria |",
            "| --- | --- | --- | --- | --- |",
            "| task-nav | high | ui_semantics, navigation, keyboard, focus_management | Verify semantic structure, heading order, landmarks, roles, and accessible names follow WCAG 2.2 expectations.; Check interactive controls expose clear state, purpose, and name to assistive technology.; Verify navigation order, current location, skip-link behavior, and route changes are perceivable.; Check menus, breadcrumbs, pagination, and steppers can be understood without visual-only cues.; Verify every changed interactive path works with keyboard alone and has a logical tab order.; Check custom shortcuts do not block browser or assistive technology keyboard commands.; Verify visible focus indication, focus trapping, focus restoration, and route or dialog focus placement.; Check focus does not move unexpectedly during validation, loading, or dynamic updates.; Record the accessibility evidence with the task id and changed files. | Add acceptance criteria covering semantic HTML, landmarks, headings, roles, and accessible names. |",
        ]
    )
    assert task_accessibility_impact_plan_to_markdown(result) == result.to_markdown()


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks):
    return {
        "id": "plan-accessibility",
        "implementation_brief_id": "brief-accessibility",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    tags=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": (
            acceptance_criteria if acceptance_criteria is not None else ["Done"]
        ),
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        task["tags"] = tags
    return task
