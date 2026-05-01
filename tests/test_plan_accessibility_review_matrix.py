import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_accessibility_review_matrix import (
    PlanAccessibilityReviewMatrix,
    PlanAccessibilityReviewRecord,
    build_plan_accessibility_review_matrix,
    plan_accessibility_review_matrix_to_dict,
    plan_accessibility_review_matrix_to_markdown,
    summarize_plan_accessibility_review_matrix,
)


def test_ui_mobile_and_design_system_tasks_receive_accessibility_review_records():
    result = build_plan_accessibility_review_matrix(
        _plan(
            [
                _task(
                    "task-checkout",
                    title="Checkout form keyboard flow",
                    description=(
                        "Add a frontend payment form with inline validation errors, focus restoration, "
                        "keyboard support, and screen reader announcements."
                    ),
                    files_or_modules=[
                        "src/frontend/routes/checkout/PaymentForm.tsx",
                        "src/frontend/forms/payment_errors.ts",
                    ],
                    metadata={"review": {"assistive_tech": "VoiceOver screen reader"}},
                ),
                _task(
                    "task-mobile-theme",
                    title="Mobile design system color tokens",
                    description=(
                        "Update the mobile design system palette, icon contrast, touch target sizing, "
                        "and Dynamic Type handling."
                    ),
                    files_or_modules=[
                        "src/mobile/design-system/tokens/colors.ts",
                        "src/android/ui/ProfileScreen.kt",
                    ],
                ),
                _task(
                    "task-api",
                    title="Optimize account cache",
                    description="Tune backend cache TTL for account summary queries.",
                    files_or_modules=["src/backend/cache/account_cache.py"],
                ),
            ]
        )
    )

    assert isinstance(result, PlanAccessibilityReviewMatrix)
    assert result.plan_id == "plan-accessibility"
    assert result.accessibility_task_ids == ("task-checkout", "task-mobile-theme")
    assert result.no_review_task_ids == ("task-api",)

    checkout = _record(result, "task-checkout")
    assert isinstance(checkout, PlanAccessibilityReviewRecord)
    assert checkout.severity == "critical"
    assert checkout.review_areas == (
        "ui_semantics",
        "forms",
        "keyboard_navigation",
        "focus_management",
        "screen_reader",
    )
    assert any("keyboard-only completion" in item for item in checkout.required_evidence)
    assert any("keyboard alone" in item for item in checkout.suggested_validation)
    assert "metadata.review.assistive_tech: VoiceOver screen reader" in checkout.text_evidence

    mobile = _record(result, "task-mobile-theme")
    assert mobile.severity == "high"
    assert mobile.review_areas == (
        "ui_semantics",
        "mobile_accessibility",
        "design_system",
        "color_contrast",
        "media_alternatives",
    )
    assert any("VoiceOver or TalkBack" in item for item in mobile.required_evidence)
    assert any("component-level accessibility stories" in item for item in mobile.suggested_validation)

    assert result.summary["task_count"] == 3
    assert result.summary["accessibility_task_count"] == 2
    assert result.summary["no_review_task_count"] == 1
    assert result.summary["severity_counts"] == {
        "critical": 1,
        "high": 1,
        "medium": 0,
        "low": 0,
    }
    assert result.summary["review_area_counts"]["forms"] == 1
    assert result.summary["review_area_counts"]["mobile_accessibility"] == 1
    assert result.summary["review_area_counts"]["color_contrast"] == 1


def test_media_caption_focus_i18n_and_screen_reader_signals_are_detected():
    result = build_plan_accessibility_review_matrix(
        _plan(
            [
                _task(
                    "task-video-locale",
                    title="Localized onboarding video modal",
                    description=(
                        "Add translated onboarding video captions, transcript links, RTL layout, "
                        "modal focus trap, and aria-live completion announcement."
                    ),
                    files_or_modules=[
                        "src/frontend/modals/OnboardingVideoModal.tsx",
                        "src/frontend/i18n/rtl/onboarding.json",
                    ],
                    acceptance_criteria=[
                        "Captions, transcripts, RTL copy expansion, and focus restoration are verified."
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.severity == "critical"
    assert record.review_areas == (
        "ui_semantics",
        "focus_management",
        "screen_reader",
        "media_alternatives",
        "internationalization",
    )
    assert any("captions" in item.lower() for item in record.required_evidence)
    assert any("long translations" in item for item in record.suggested_validation)
    assert result.summary["review_area_counts"]["internationalization"] == 1
    assert result.summary["review_area_counts"]["screen_reader"] == 1


def test_backend_only_empty_and_invalid_inputs_are_stable_and_documented_as_no_review():
    backend = build_plan_accessibility_review_matrix(
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
    empty = build_plan_accessibility_review_matrix({"id": "empty-plan", "tasks": []})
    invalid = build_plan_accessibility_review_matrix(7)

    assert backend.records == ()
    assert backend.accessibility_task_ids == ()
    assert backend.no_review_task_ids == ("task-cache",)
    assert backend.summary["task_count"] == 1
    assert backend.summary["accessibility_task_count"] == 0
    assert "No accessibility review needed: task-cache" in backend.to_markdown()
    assert "No accessibility review records were inferred." in backend.to_markdown()

    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert empty.summary["task_count"] == 0
    assert invalid.plan_id is None
    assert invalid.summary["task_count"] == 0


def test_serialization_markdown_alias_model_input_and_deterministic_ordering():
    plan = _plan(
        [
            _task(
                "task-z-medium",
                title="Marketing image alt text | locale",
                description="Add translated image alt text and locale-specific date formats.",
                files_or_modules=["src/frontend/i18n/gallery.ts"],
            ),
            _task(
                "task-a-critical",
                title="Dialog focus management",
                description="Fix modal focus trap, restore focus on close, and announce errors to screen reader.",
                files_or_modules=["src/frontend/components/Dialog.tsx"],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_accessibility_review_matrix(ExecutionPlan.model_validate(plan))
    alias = summarize_plan_accessibility_review_matrix(plan)
    payload = plan_accessibility_review_matrix_to_dict(result)
    markdown = plan_accessibility_review_matrix_to_markdown(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert alias.to_dict() == result.to_dict()
    assert [record.task_id for record in result.records] == [
        "task-a-critical",
        "task-z-medium",
    ]
    assert [record.severity for record in result.records] == ["critical", "medium"]
    assert list(payload) == [
        "plan_id",
        "records",
        "accessibility_task_ids",
        "no_review_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "severity",
        "review_areas",
        "required_evidence",
        "suggested_validation",
        "text_evidence",
    ]
    assert markdown.startswith("# Plan Accessibility Review Matrix: plan-accessibility")
    assert "## Summary" in markdown
    assert "| Task | Severity | Review Areas | Required Evidence | Suggested Validation | Text Evidence |" in markdown
    assert "Marketing image alt text \\| locale" in markdown
    assert plan_accessibility_review_matrix_to_markdown(result) == result.to_markdown()


def test_execution_task_and_object_like_task_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="React Native language switcher",
        description="Add RTL locale switching with TalkBack validation.",
        files_or_modules=["src/mobile/i18n/LanguageSwitcher.tsx"],
        acceptance_criteria=["TalkBack and RTL validation pass."],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Theme status colors",
            description="Update UI status colors and contrast.",
            files_or_modules=["src/frontend/styles/theme.css"],
        )
    )

    first = build_plan_accessibility_review_matrix([object_task])
    second = build_plan_accessibility_review_matrix(task_model)

    assert first.records[0].task_id == "task-object"
    assert first.records[0].review_areas == (
        "ui_semantics",
        "mobile_accessibility",
        "screen_reader",
        "internationalization",
    )
    assert second.records[0].task_id == "task-model"
    assert second.records[0].severity == "medium"


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks, *, plan_id="plan-accessibility"):
    return {
        "id": plan_id,
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
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
