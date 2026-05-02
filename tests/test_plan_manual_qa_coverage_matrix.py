import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_manual_qa_coverage_matrix import (
    PlanManualQACoverageMatrix,
    PlanManualQACoverageRecord,
    build_plan_manual_qa_coverage_matrix,
    generate_plan_manual_qa_coverage_matrix,
    plan_manual_qa_coverage_matrix_to_dict,
    plan_manual_qa_coverage_matrix_to_dicts,
    plan_manual_qa_coverage_matrix_to_markdown,
    summarize_plan_manual_qa_coverage_matrix,
)


def test_multiple_task_types_are_classified_with_missing_coverage_notes_and_owner_hints():
    result = build_plan_manual_qa_coverage_matrix(
        _plan(
            [
                _task(
                    "task-ui",
                    title="Refresh checkout confirmation screen",
                    description="Update the frontend checkout screen layout and icon states.",
                    files_or_modules=["src/frontend/pages/CheckoutSuccess.tsx"],
                    metadata={"qa_owner": "Design QA"},
                ),
                _task(
                    "task-mobile-admin",
                    title="Mobile admin permission workflow",
                    description=(
                        "Change the iOS admin settings flow for role and permission management "
                        "with VoiceOver focus behavior."
                    ),
                    files_or_modules=["src/mobile/admin/PermissionSettings.tsx"],
                    owner_type="admin-platform",
                    acceptance_criteria=[
                        "Manual QA covers real device checks and accessibility spot check."
                    ],
                ),
                _task(
                    "task-migration",
                    title="Backfill account records",
                    description="Run a data migration and reconcile existing customer account records.",
                    files_or_modules=["migrations/versions/20260502_accounts.sql"],
                    metadata={"owner": "Data Platform"},
                ),
                _task(
                    "task-rollback",
                    title="Feature flag rollback runbook",
                    description="Document the kill switch and rollback drill for launch watch.",
                    files_or_modules=["runbooks/checkout_rollback.md"],
                ),
                _task(
                    "task-api",
                    title="Tune cache TTL",
                    description="Update backend cache behavior for profile lookups.",
                    files_or_modules=["src/backend/cache/profile.py"],
                ),
            ]
        )
    )

    assert isinstance(result, PlanManualQACoverageMatrix)
    assert result.plan_id == "plan-manual-qa"
    assert result.manual_qa_task_ids == (
        "task-ui",
        "task-migration",
        "task-rollback",
        "task-mobile-admin",
    )
    assert all(isinstance(record, PlanManualQACoverageRecord) for record in result.records)

    ui = _record(result, "task-ui")
    assert ui.qa_needs == ("visual_review",)
    assert ui.coverage_status == "missing_coverage"
    assert ui.missing_coverage_notes == (
        "Add manual visual QA evidence for changed UI, layout, copy, screenshots, or visual states.",
    )
    assert ui.owner_hints == ("Design QA",)
    assert "files_or_modules: src/frontend/pages/CheckoutSuccess.tsx" in ui.detected_signals

    mobile = _record(result, "task-mobile-admin")
    assert mobile.qa_needs == (
        "visual_review",
        "mobile_device_check",
        "accessibility_spot_check",
        "admin_workflow_verification",
    )
    assert mobile.coverage_status == "covered"
    assert mobile.missing_coverage_notes == ()
    assert mobile.owner_hints == ("admin-platform",)

    migration = _record(result, "task-migration")
    assert migration.qa_needs == ("migration_verification",)
    assert migration.coverage_status == "missing_coverage"
    assert migration.owner_hints == ("Data Platform",)

    rollback = _record(result, "task-rollback")
    assert rollback.qa_needs == ("rollback_rehearsal",)
    assert rollback.coverage_status == "missing_coverage"

    assert result.summary["task_count"] == 5
    assert result.summary["manual_qa_task_count"] == 4
    assert result.summary["missing_coverage_task_count"] == 3
    assert result.summary["coverage_status_counts"] == {
        "covered": 1,
        "missing_coverage": 3,
    }
    assert result.summary["qa_need_counts"]["visual_review"] == 2
    assert result.summary["qa_need_counts"]["mobile_device_check"] == 1
    assert result.summary["qa_need_counts"]["accessibility_spot_check"] == 1
    assert result.summary["qa_need_counts"]["migration_verification"] == 1
    assert result.summary["qa_need_counts"]["admin_workflow_verification"] == 1
    assert result.summary["qa_need_counts"]["rollback_rehearsal"] == 1
    assert result.summary["status"] == "missing_manual_qa_coverage"


def test_cross_browser_and_all_covered_plan_serializes_and_renders_stable_markdown():
    plan = _plan(
        [
            _task(
                "task-browser",
                title="Search results browser matrix | visual polish",
                description="Update the browser responsive results page.",
                files_or_modules=["web/browser/SearchResults.tsx"],
                acceptance_criteria=[
                    "Manual QA includes screenshot review and cross-browser browser matrix sign-off."
                ],
            ),
            _task(
                "task-cutover",
                title="Tenant export cutover",
                description="Export existing tenant data during the migration cutover.",
                files_or_modules=["src/exports/tenant_export.py"],
                acceptance_criteria=[
                    "Manual QA includes migration verification before launch."
                ],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_manual_qa_coverage_matrix(ExecutionPlan.model_validate(plan))
    generated = generate_plan_manual_qa_coverage_matrix(plan)
    summarized = summarize_plan_manual_qa_coverage_matrix(result)
    payload = plan_manual_qa_coverage_matrix_to_dict(result)
    markdown = plan_manual_qa_coverage_matrix_to_markdown(result)

    assert plan == original
    assert generated.to_dict() == result.to_dict()
    assert summarized is result
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert plan_manual_qa_coverage_matrix_to_dicts(result) == payload["records"]
    assert plan_manual_qa_coverage_matrix_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.summary["missing_coverage_task_count"] == 0
    assert result.summary["status"] == "covered"
    assert list(payload) == ["plan_id", "records", "manual_qa_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "qa_needs",
        "detected_signals",
        "coverage_status",
        "missing_coverage_notes",
        "owner_hints",
    ]
    assert markdown.startswith("# Plan Manual QA Coverage Matrix: plan-manual-qa")
    assert "## Summary" in markdown
    assert "| Task | Title | QA Needs | Coverage | Missing Coverage Notes | Owner Hints | Detected Signals |" in markdown
    assert "Search results browser matrix \\| visual polish" in markdown
    assert "| `task-browser` |" in markdown
    assert "covered" in markdown
    assert plan_manual_qa_coverage_matrix_to_markdown(result) == result.to_markdown()


def test_empty_no_match_and_malformed_inputs_are_stable():
    no_match = build_plan_manual_qa_coverage_matrix(
        _plan(
            [
                _task(
                    "task-backend",
                    title="Optimize profile cache",
                    description="Adjust backend cache TTL for service lookups.",
                    files_or_modules=["src/services/profile_cache.py"],
                    acceptance_criteria=["Unit tests cover cache refresh behavior."],
                )
            ]
        )
    )
    empty = build_plan_manual_qa_coverage_matrix({"id": "empty-plan", "tasks": []})
    malformed = build_plan_manual_qa_coverage_matrix(42)

    assert no_match.records == ()
    assert no_match.manual_qa_task_ids == ()
    assert no_match.summary["task_count"] == 1
    assert no_match.summary["manual_qa_task_count"] == 0
    assert no_match.summary["status"] == "no_manual_qa_obligations"
    assert "No manual QA coverage obligations were detected." in no_match.to_markdown()

    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert empty.summary["task_count"] == 0
    assert malformed.plan_id is None
    assert malformed.summary["task_count"] == 0
    assert malformed.to_markdown().startswith("# Plan Manual QA Coverage Matrix")


def test_object_like_and_execution_task_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Android admin role screen",
        description="Update Android admin role settings and touch gestures.",
        files_or_modules=["src/android/admin/RoleScreen.kt"],
        acceptance_criteria=["Manual QA covers admin workflow verification on a real device."],
        metadata={"qa_lead": "Mobile QA"},
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Rollback switch",
            description="Add a feature flag rollback runbook and rehearse the rollback drill.",
            files_or_modules=["ops/runbooks/rollback.md"],
            acceptance_criteria=["Rollback rehearsal is signed off."],
        )
    )

    first = build_plan_manual_qa_coverage_matrix([object_task])
    second = build_plan_manual_qa_coverage_matrix(task_model)

    assert first.records[0].task_id == "task-object"
    assert first.records[0].qa_needs == (
        "mobile_device_check",
        "admin_workflow_verification",
    )
    assert first.records[0].coverage_status == "covered"
    assert first.records[0].owner_hints == ("Mobile QA",)

    assert second.plan_id is None
    assert second.records[0].task_id == "task-model"
    assert second.records[0].qa_needs == ("rollback_rehearsal",)
    assert second.records[0].coverage_status == "covered"


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks, *, plan_id="plan-manual-qa"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-manual-qa",
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
    owner_type=None,
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
    if owner_type is not None:
        task["owner_type"] = owner_type
    if metadata is not None:
        task["metadata"] = metadata
    return task
