import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_browser_compatibility import (
    TaskBrowserCompatibilityPlan,
    TaskBrowserCompatibilityRecord,
    build_task_browser_compatibility_plan,
    derive_task_browser_compatibility_plan,
    task_browser_compatibility_plan_to_dict,
    task_browser_compatibility_plan_to_markdown,
)


def test_frontend_css_api_form_media_mobile_and_legacy_signals_are_classified():
    result = build_task_browser_compatibility_plan(
        _plan(
            [
                _task(
                    "task-layout",
                    title="Update responsive checkout CSS grid",
                    description="Adjust CSS container queries and breakpoints.",
                    files_or_modules=["src/components/Checkout.css"],
                ),
                _task(
                    "task-api",
                    title="Use Clipboard browser API",
                    description="Call navigator.clipboard and keep a fallback for unsupported permissions.",
                    files_or_modules=["src/components/ShareButton.tsx"],
                ),
                _task(
                    "task-form",
                    title="Revise payment form behavior",
                    description="Handle input autofill, keyboard focus, and native validation messages.",
                    files_or_modules=["src/forms/PaymentForm.tsx"],
                ),
                _task(
                    "task-media",
                    title="Render invoice preview on canvas",
                    description="Draw images to canvas and support Safari rendering.",
                    files_or_modules=["src/canvas/InvoicePreview.tsx"],
                ),
                _task(
                    "task-legacy",
                    title="Add Firefox and Safari browser compatibility coverage",
                    description="Review Browserslist and polyfill handling for older browser support.",
                    files_or_modules=["browserslist"],
                ),
            ]
        )
    )

    assert _categories(result, "task-layout") == ("layout_css", "mobile_responsive")
    assert _categories(result, "task-api") == ("javascript_api",)
    assert "form_behavior" in _categories(result, "task-form")
    assert "media_canvas" in _categories(result, "task-media")
    assert set(_categories(result, "task-legacy")) == {"javascript_api", "legacy_browser"}
    assert result.summary["category_counts"]["layout_css"] == 1
    assert result.summary["category_counts"]["legacy_browser"] == 2


def test_flagged_task_includes_severity_rationale_checks_evidence_and_a11y_priority():
    result = build_task_browser_compatibility_plan(
        _plan(
            [
                _task(
                    "task-form",
                    title="Build accessible mobile signup form",
                    description="Add form validation with keyboard focus states.",
                    files_or_modules=["src/forms/SignupForm.tsx"],
                    acceptance_criteria=[
                        "Verify WCAG keyboard navigation and focus order.",
                        "Run responsive viewport smoke tests.",
                    ],
                    metadata={"browser_matrix": "Safari and Firefox validation required."},
                )
            ],
            test_strategy="Run Playwright mobile viewport checks.",
        )
    )

    record = next(item for item in result.records if item.category == "form_behavior")

    assert record.severity == "high"
    assert "Accessibility-related evidence raises the priority" in record.rationale
    assert record.suggested_checks == (
        "Verify keyboard navigation, focus order, autofill, and native validation behavior.",
        "Check form submission and validation in Safari and Firefox.",
        "Include keyboard and focus checks in the browser compatibility pass.",
    )
    assert record.evidence == (
        "files_or_modules: src/forms/SignupForm.tsx",
        "title: Build accessible mobile signup form",
        "description: Add form validation with keyboard focus states.",
        "acceptance_criteria[0]: Verify WCAG keyboard navigation and focus order.",
        "metadata.browser_matrix: Safari and Firefox validation required.",
        "test_strategy: Run Playwright mobile viewport checks.",
    )


def test_no_browser_signals_return_empty_plan_and_markdown():
    result = build_task_browser_compatibility_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Polish service copy",
                    description="Update backend-only notification wording.",
                    files_or_modules=["src/services/notifications.py"],
                    acceptance_criteria=["Copy matches product guidance."],
                )
            ]
        )
    )

    assert result.plan_id == "plan-browser-compatibility"
    assert result.records == ()
    assert result.browser_task_ids == ()
    assert result.to_dict() == {
        "plan_id": "plan-browser-compatibility",
        "records": [],
        "browser_task_ids": [],
        "summary": {
            "task_count": 1,
            "record_count": 0,
            "browser_task_count": 0,
            "severity_counts": {"high": 0, "medium": 0, "low": 0},
            "category_counts": {
                "layout_css": 0,
                "javascript_api": 0,
                "form_behavior": 0,
                "media_canvas": 0,
                "mobile_responsive": 0,
                "legacy_browser": 0,
            },
        },
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Task Browser Compatibility Plan: plan-browser-compatibility",
            "",
            "No browser compatibility signals detected.",
        ]
    )


def test_sorting_serialization_aliases_model_inputs_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Update responsive dashboard layout",
                description="CSS grid changes for mobile breakpoints.",
                files_or_modules=["src/pages/Dashboard.tsx"],
                acceptance_criteria=["Run responsive viewport smoke tests."],
            ),
            _task(
                "task-a",
                title="Add camera canvas capture",
                description="Use camera permissions and canvas preview.",
                files_or_modules=["src/media/Capture.tsx"],
            ),
        ],
        plan_id="plan-model",
        test_strategy=None,
    )

    result = build_task_browser_compatibility_plan(ExecutionPlan.model_validate(plan))
    alias_result = derive_task_browser_compatibility_plan(plan)
    single = build_task_browser_compatibility_plan(ExecutionTask.model_validate(plan["tasks"][0]))
    payload = task_browser_compatibility_plan_to_dict(result)

    assert isinstance(result, TaskBrowserCompatibilityPlan)
    assert isinstance(TaskBrowserCompatibilityRecord, type)
    assert [record.task_id for record in result.records] == [
        "task-a",
        "task-a",
        "task-z",
        "task-z",
    ]
    assert [record.category for record in result.records] == [
        "javascript_api",
        "media_canvas",
        "layout_css",
        "mobile_responsive",
    ]
    assert [record.severity for record in result.records] == [
        "high",
        "high",
        "medium",
        "medium",
    ]
    assert payload == result.to_dict()
    assert alias_result.to_dict() == result.to_dict()
    assert single.plan_id is None
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "browser_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "category",
        "severity",
        "rationale",
        "suggested_checks",
        "evidence",
    ]
    assert task_browser_compatibility_plan_to_markdown(result) == "\n".join(
        [
            "# Task Browser Compatibility Plan: plan-model",
            "",
            "| Task | Severity | Category | Suggested Checks | Rationale | Evidence |",
            "| --- | --- | --- | --- | --- | --- |",
            (
                "| task-a | high | javascript_api | Review browser support and required polyfills for the "
                "web APIs used.; Validate behavior in Chromium, Firefox, and Safari with unsupported-permission "
                "fallbacks. | Browser API usage may have permission, support, or polyfill differences. | title: "
                "Add camera canvas capture; description: Use camera permissions and canvas preview. |"
            ),
            (
                "| task-a | high | media_canvas | Smoke test canvas or media playback on Safari, Firefox, and Chromium.; "
                "Validate device permission, codec, sizing, and fallback behavior where applicable. | Canvas, media, "
                "and animation behavior is sensitive to browser and device support. | files_or_modules: "
                "src/media/Capture.tsx; title: Add camera canvas capture; description: Use camera permissions and "
                "canvas preview. |"
            ),
            (
                "| task-z | medium | layout_css | Run responsive viewport smoke tests at mobile, tablet, and desktop "
                "widths.; Validate layout in Safari and Firefox for the affected route or component. | Layout or "
                "styling changes can render differently across engines and viewport sizes. Existing validation "
                "notes already mention browser or device coverage. | title: Update responsive dashboard layout; "
                "description: CSS grid changes for mobile breakpoints.; "
                "acceptance_criteria[0]: Run responsive viewport smoke tests. |"
            ),
            (
                "| task-z | medium | mobile_responsive | Run responsive viewport smoke tests at mobile, tablet, and "
                "desktop widths.; Validate touch targets, orientation changes, and iOS Safari behavior. | Responsive "
                "and touch behavior needs explicit device and viewport validation. Existing validation notes already "
                "mention browser or device coverage. | title: Update responsive dashboard layout; description: CSS "
                "grid changes for mobile breakpoints.; acceptance_criteria[0]: Run responsive viewport smoke tests. |"
            ),
        ]
    )


def test_suggested_checks_are_deduped_for_responsive_accessibility_overlap():
    result = build_task_browser_compatibility_plan(
        _task(
            "task-mobile",
            title="Responsive mobile form focus polish",
            description="Improve mobile form keyboard focus and viewport behavior.",
            files_or_modules=["src/mobile/forms/ProfileForm.tsx"],
            acceptance_criteria=["Accessibility keyboard focus must pass."],
        )
    )

    responsive = next(record for record in result.records if record.category == "mobile_responsive")

    assert len(responsive.suggested_checks) == len(set(responsive.suggested_checks))
    assert responsive.suggested_checks == (
        "Run responsive viewport smoke tests at mobile, tablet, and desktop widths.",
        "Validate touch targets, orientation changes, and iOS Safari behavior.",
        "Include keyboard and focus checks in the browser compatibility pass.",
    )


def _categories(result, task_id):
    return tuple(record.category for record in result.records if record.task_id == task_id)


def _plan(tasks, *, plan_id="plan-browser-compatibility", test_strategy="Run browser checks."):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-browser-compatibility",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "web",
        "milestones": [],
        "test_strategy": test_strategy,
        "handoff_prompt": "Implement the plan",
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
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "milestone": "Frontend",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": None,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
