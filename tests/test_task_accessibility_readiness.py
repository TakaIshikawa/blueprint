import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_accessibility_readiness import (
    AccessibilityReadinessTask,
    TaskAccessibilityReadinessPlan,
    TaskAccessibilityReadinessRecord,
    analyze_task_accessibility_readiness,
    build_task_accessibility_readiness_plan,
    derive_task_accessibility_readiness,
    extract_task_accessibility_readiness,
    generate_task_accessibility_readiness,
    recommend_task_accessibility_readiness,
    summarize_task_accessibility_readiness,
    task_accessibility_readiness_plan_to_dict,
    task_accessibility_readiness_plan_to_dicts,
    task_accessibility_readiness_plan_to_markdown,
    task_accessibility_readiness_to_dicts,
)


def test_wcag_aa_brief_generates_implementation_and_validation_tasks():
    result = build_task_accessibility_readiness_plan(
        _plan(
            [
                _task(
                    "task-checkout-a11y",
                    title="Make checkout WCAG AA compliant",
                    description=(
                        "Checkout must meet WCAG 2.2 AA. Address semantic HTML, keyboard navigation, "
                        "visible focus states, screen reader labels, color contrast, and reduced motion."
                    ),
                    files_or_modules=["src/ui/checkout/accessibility.tsx"],
                    metadata={"qa": "Run axe checks and manual VoiceOver screen reader QA."},
                )
            ]
        )
    )

    assert isinstance(result, TaskAccessibilityReadinessPlan)
    assert len(result.records) == 1
    record = result.records[0]
    assert isinstance(record, TaskAccessibilityReadinessRecord)
    assert record.task_id == "task-checkout-a11y"
    assert record.detected_signals == (
        "accessibility",
        "wcag_aa",
        "semantic_structure",
        "keyboard_access",
        "focus_states",
        "screen_reader_labels",
        "contrast",
        "motion_preferences",
        "automated_checks",
        "manual_assistive_qa",
    )
    assert _categories(record) == (
        "semantic_structure",
        "keyboard_access",
        "focus_states",
        "screen_reader_labels",
        "contrast",
        "motion_preferences",
        "automated_checks",
        "manual_assistive_qa",
    )
    assert "media_alternatives" not in _categories(record)
    assert all(isinstance(task, AccessibilityReadinessTask) for task in record.generated_tasks)
    assert all(len(task.acceptance_criteria) >= 3 for task in record.generated_tasks)
    assert any(task.category == "automated_checks" for task in record.generated_tasks)
    assert any(task.category == "manual_assistive_qa" for task in record.generated_tasks)
    assert any("description: Checkout must meet WCAG 2.2 AA" in item for item in record.evidence)
    assert any("Rationale: description: Checkout must meet WCAG 2.2 AA" in task.description for task in record.generated_tasks)
    assert result.accessibility_task_ids == ("task-checkout-a11y",)
    assert result.impacted_task_ids == result.accessibility_task_ids
    assert result.summary["generated_task_category_counts"]["automated_checks"] == 1
    assert result.summary["generated_task_category_counts"]["manual_assistive_qa"] == 1


def test_keyboard_only_requirement_generates_core_accessibility_tasks_without_media_task():
    result = analyze_task_accessibility_readiness(
        _plan(
            [
                _task(
                    "task-keyboard-menu",
                    title="Keyboard-only command menu",
                    description=(
                        "Users must complete the command menu with keyboard only. Tab order, arrow keys, "
                        "Escape, and visible focus ring behavior are required."
                    ),
                    acceptance_criteria=[
                        "Keyboard QA covers opening, navigating, selecting, and dismissing the menu.",
                        "Playwright accessibility check validates labels and focus restoration.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    by_category = {task.category: task for task in record.generated_tasks}
    assert "keyboard_access" in record.detected_signals
    assert "focus_states" in record.detected_signals
    assert "automated_checks" in record.detected_signals
    assert "manual_assistive_qa" in record.detected_signals
    assert "media_alternatives" not in record.detected_signals
    assert "media_alternatives" not in by_category
    assert any("All actionable controls are reachable" in item for item in by_category["keyboard_access"].acceptance_criteria)
    assert any("Focus indicators meet WCAG AA" in item for item in by_category["focus_states"].acceptance_criteria)
    assert record.readiness == "ready"


def test_media_caption_requirements_create_media_specific_task_and_support_text_input():
    result = build_task_accessibility_readiness_plan(
        "Product demo video requires closed captions, transcript, accessible media controls, "
        "and WCAG AA accessibility validation."
    )

    record = result.records[0]
    by_category = {task.category: task for task in record.generated_tasks}
    assert record.task_id == "requirement-text"
    assert "media_alternatives" in record.detected_signals
    assert "media_alternatives" in by_category
    assert any("synchronized captions" in item for item in by_category["media_alternatives"].acceptance_criteria)
    assert any("transcripts" in item for item in by_category["media_alternatives"].acceptance_criteria)
    assert result.summary["generated_task_category_counts"]["media_alternatives"] == 1


def test_unrelated_briefs_models_objects_serialization_markdown_and_aliases_are_stable():
    plan = _plan(
        [
            _task("task-copy", title="Update onboarding copy", description="Static copy only."),
            _task(
                "task-explicit-none",
                title="Profile chart polish",
                description="No accessibility requirements are in scope for this visual copy change.",
            ),
        ]
    )
    original = copy.deepcopy(plan)
    empty = build_task_accessibility_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_accessibility_readiness_plan(13)
    object_task = SimpleNamespace(
        id="task-object",
        title="Accessible settings form",
        description="Form labels and screen reader descriptions are required.",
        files_or_modules=["src/ui/settings/a11y_form.tsx"],
        acceptance_criteria=["Done."],
        status="pending",
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="High contrast empty state",
            description="Color contrast must meet WCAG AA and automated axe checks must pass.",
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([model_task.model_dump(mode="python")], plan_id="plan-model"))

    result = build_task_accessibility_readiness_plan(plan)
    object_result = extract_task_accessibility_readiness([object_task])
    model_result = generate_task_accessibility_readiness(plan_model)
    payload = task_accessibility_readiness_plan_to_dict(result)
    markdown = task_accessibility_readiness_plan_to_markdown(result)

    assert plan == original
    assert result.records == ()
    assert result.accessibility_task_ids == ()
    assert result.no_impact_task_ids == ("task-copy", "task-explicit-none")
    assert result.to_dicts() == []
    assert json.loads(json.dumps(payload)) == payload
    assert task_accessibility_readiness_plan_to_dicts(result) == payload["records"]
    assert task_accessibility_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_accessibility_readiness_to_dicts(result.records) == payload["records"]
    assert summarize_task_accessibility_readiness(result) is result
    assert recommend_task_accessibility_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_accessibility_readiness(plan).to_dict() == result.to_dict()
    assert "No task accessibility readiness records were inferred." in markdown
    assert "No-impact tasks: task-copy, task-explicit-none" in markdown
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert invalid.records == ()
    assert object_result.records[0].task_id == "task-object"
    assert model_result.plan_id == "plan-model"
    assert model_result.records[0].task_id == "task-model"
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "accessibility_task_ids",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]


def _categories(record):
    return tuple(task.category for task in record.generated_tasks)


def _plan(tasks, plan_id="plan-accessibility"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-accessibility",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "frontend",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
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
        "execution_plan_id": "plan-accessibility",
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
