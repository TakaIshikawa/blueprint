import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_csrf_protection_readiness import (
    TaskCsrfProtectionReadinessPlan,
    TaskCsrfProtectionReadinessRecord,
    analyze_task_csrf_protection_readiness,
    build_task_csrf_protection_readiness_plan,
    extract_task_csrf_protection_readiness,
    generate_task_csrf_protection_readiness,
    recommend_task_csrf_protection_readiness,
    task_csrf_protection_readiness_plan_to_dict,
    task_csrf_protection_readiness_plan_to_dicts,
    task_csrf_protection_readiness_plan_to_markdown,
)


def test_detects_csrf_relevant_signals_from_task_fields_paths_validation_and_metadata():
    result = build_task_csrf_protection_readiness_plan(
        _plan(
            [
                _task(
                    "task-session",
                    title="Protect cookie-backed settings form",
                    description="Browser form posts update an authenticated session profile.",
                    files_or_modules=["src/web/forms/profile_settings.py"],
                    acceptance_criteria=["The session cookie uses SameSite=Lax policy."],
                ),
                _task(
                    "task-admin",
                    title="Harden admin DELETE action",
                    description="Admin actions perform DELETE requests against privileged accounts.",
                    files_or_modules=["src/admin/users_controller.py"],
                    validation_plan=["CSRF token integration tests reject missing tokens."],
                ),
                _task(
                    "task-origin",
                    title="Validate unsafe method origins",
                    description="State-changing PATCH endpoints require authenticated browser sessions.",
                    validation_commands=["poetry run pytest tests/test_origin_check.py"],
                    metadata={
                        "origin_check": "Origin validation covers trusted origins.",
                        "csrf": {"double_submit_cookie": "XSRF cookie-to-header token is accepted."},
                    },
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert isinstance(result, TaskCsrfProtectionReadinessPlan)
    assert all(isinstance(record, TaskCsrfProtectionReadinessRecord) for record in result.records)
    assert by_id["task-session"].csrf_signals == (
        "cookie_session",
        "form_post",
        "same_site_cookie",
    )
    assert by_id["task-admin"].csrf_signals == (
        "unsafe_method",
        "admin_mutation",
        "csrf_token",
    )
    assert by_id["task-origin"].csrf_signals == (
        "cookie_session",
        "unsafe_method",
        "origin_check",
        "double_submit_cookie",
    )
    assert by_id["task-admin"].present_safeguards == (
        "csrf_token_validation",
        "integration_tests",
    )
    assert "admin_action_coverage" in by_id["task-admin"].missing_safeguards
    assert "files_or_modules: src/web/forms/profile_settings.py" in by_id["task-session"].evidence
    assert any("validation_plan[0]" in item for item in by_id["task-admin"].evidence)
    assert any("metadata.origin_check" in item for item in by_id["task-origin"].evidence)
    assert result.summary["signal_counts"]["unsafe_method"] == 2
    assert result.summary["csrf_task_count"] == 3


def test_unsafe_state_changing_task_without_token_or_origin_is_high_risk():
    result = build_task_csrf_protection_readiness_plan(
        _plan(
            [
                _task(
                    "task-unsafe",
                    title="Add authenticated POST endpoint",
                    description="Cookie-backed session users submit a form post to delete saved cards.",
                    files_or_modules=["src/routes/payment_methods.py"],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.risk_level == "high"
    assert "csrf_token_validation" in record.missing_safeguards
    assert "origin_referer_validation" in record.missing_safeguards
    assert record.recommended_safeguards == tuple(
        [
            "Require CSRF token validation or an equivalent double-submit token for unsafe browser requests.",
            "Define SameSite cookie policy for session cookies and document any cross-site exceptions.",
            "Validate Origin or Referer headers for unsafe authenticated browser requests.",
            "Add idempotency, confirmation, or undo coverage for destructive state changes.",
            "Add integration tests that reject missing, invalid, and cross-origin CSRF attempts.",
            "Cover admin and privileged state changes with explicit CSRF acceptance criteria.",
        ]
    )
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}


def test_present_safeguards_lower_risk_and_missing_recommendations_are_deterministic():
    result = analyze_task_csrf_protection_readiness(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Protect checkout form",
                    description="Checkout form POST uses a cookie-backed session.",
                    acceptance_criteria=[
                        "CSRF token validation is required on every unsafe request.",
                        "SameSite=Lax cookie policy is documented for session cookies.",
                        "Origin and Referer validation covers trusted origins.",
                        "Idempotency key or confirmation prevents repeated destructive submits.",
                        "CSRF integration tests cover missing and invalid tokens.",
                        "Admin action coverage is not relevant but documented.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.present_safeguards == (
        "csrf_token_validation",
        "same_site_policy",
        "origin_referer_validation",
        "idempotency_or_confirmation",
        "integration_tests",
        "admin_action_coverage",
    )
    assert record.missing_safeguards == ()
    assert record.recommended_safeguards == ()
    assert record.risk_level == "low"
    assert result.summary["missing_safeguards_count"] == 0


def test_empty_no_impact_and_invalid_inputs_have_stable_empty_markdown_behavior():
    result = build_task_csrf_protection_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update onboarding copy",
                    description="Change empty state labels with no browser form or authenticated state changes in scope.",
                    files_or_modules=["src/ui/onboarding_empty_state.tsx"],
                )
            ]
        )
    )
    invalid = build_task_csrf_protection_readiness_plan(42)

    assert result.records == ()
    assert result.recommendations == ()
    assert result.findings == ()
    assert result.csrf_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "csrf_task_count": 0,
        "not_applicable_task_ids": ["task-copy"],
        "missing_safeguards_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "cookie_session": 0,
            "form_post": 0,
            "unsafe_method": 0,
            "admin_mutation": 0,
            "same_site_cookie": 0,
            "csrf_token": 0,
            "origin_check": 0,
            "double_submit_cookie": 0,
        },
        "missing_safeguard_counts": {
            "csrf_token_validation": 0,
            "same_site_policy": 0,
            "origin_referer_validation": 0,
            "idempotency_or_confirmation": 0,
            "integration_tests": 0,
            "admin_action_coverage": 0,
        },
        "csrf_task_ids": [],
    }
    assert "No CSRF protection readiness records" in result.to_markdown()
    assert "Not applicable tasks: task-copy" in result.to_markdown()
    assert invalid.records == ()
    assert invalid.to_markdown().startswith("# Task CSRF Protection Readiness")


def test_serialization_markdown_alias_order_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Protect profile form | session",
                description="Cookie-backed profile form POST uses CSRF token validation.",
            ),
            _task(
                "task-a",
                title="Protect admin delete",
                description="Admin DELETE action uses Origin validation and SameSite cookie policy.",
            ),
            _task("task-copy", title="Update copy", description="Change button text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = recommend_task_csrf_protection_readiness(plan)
    payload = task_csrf_protection_readiness_plan_to_dict(result)
    markdown = task_csrf_protection_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_csrf_protection_readiness_plan_to_dicts(result) == payload["records"]
    assert task_csrf_protection_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_csrf_protection_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_csrf_protection_readiness(plan).to_dict() == result.to_dict()
    assert result.recommendations == result.records
    assert result.findings == result.records
    assert result.csrf_task_ids == ("task-a", "task-z")
    assert result.not_applicable_task_ids == ("task-copy",)
    assert list(payload) == [
        "plan_id",
        "records",
        "csrf_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "csrf_signals",
        "risk_level",
        "present_safeguards",
        "missing_safeguards",
        "recommended_safeguards",
        "evidence",
    ]
    assert markdown.startswith("# Task CSRF Protection Readiness: plan-csrf")
    assert "Protect profile form \\| session" in markdown
    assert "| Task | Title | Risk | CSRF Signals | Present Safeguards | Missing Safeguards | Recommended Safeguards | Evidence |" in markdown


def test_execution_plan_execution_task_mapping_iterable_and_object_inputs_are_supported():
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Protect form model",
            description="Browser form post uses a cookie-backed session and CSRF token.",
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-plan",
                    title="Protect admin plan",
                    description="Admin mutation performs a PATCH request with Origin validation.",
                )
            ],
            plan_id="plan-model",
        )
    )
    iterable_result = build_task_csrf_protection_readiness_plan(
        [
            _task(
                "task-iter",
                title="Protect delete route",
                description="Authenticated session DELETE endpoint changes account state.",
            )
        ]
    )
    object_result = build_task_csrf_protection_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="Protect object form",
            description="Settings form submission uses a browser session.",
            acceptance_criteria=["Double-submit cookie coverage is required."],
        )
    )

    task_result = build_task_csrf_protection_readiness_plan(task_model)
    plan_result = build_task_csrf_protection_readiness_plan(plan_model)

    assert task_result.plan_id is None
    assert task_result.records[0].task_id == "task-model"
    assert task_result.records[0].present_safeguards == ("csrf_token_validation",)
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-plan"
    assert plan_result.records[0].csrf_signals == ("unsafe_method", "admin_mutation", "origin_check")
    assert iterable_result.csrf_task_ids == ("task-iter",)
    assert object_result.records[0].csrf_signals == (
        "cookie_session",
        "form_post",
        "double_submit_cookie",
    )


def _plan(tasks, plan_id="plan-csrf"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-csrf",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "web",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    validation_plan=None,
    validation_commands=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-csrf",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if validation_plan is not None:
        payload["validation_plan"] = validation_plan
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
