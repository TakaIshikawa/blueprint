import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_release_note_impact import (
    PlanReleaseNoteImpactRecord,
    build_plan_release_note_impact_report,
    plan_release_note_impact_report_to_dict,
    plan_release_note_impact_report_to_markdown,
    summarize_plan_release_note_impact,
)


def test_mixed_plan_classifies_customer_facing_and_internal_categories():
    report = build_plan_release_note_impact_report(
        _plan(
            [
                _task(
                    "task-feature",
                    title="Add saved filters dashboard feature",
                    description="Customers can save dashboard filters for repeated workflows.",
                    files_or_modules=["web/pages/dashboard/filters.tsx"],
                ),
                _task(
                    "task-bug",
                    title="Fix checkout retry error",
                    description="Resolve broken retry behavior for failed customer payments.",
                    files_or_modules=["app/checkout/retry.py"],
                    risk_level="medium",
                ),
                _task(
                    "task-docs",
                    title="Update release documentation",
                    description="Refresh the user guide and changelog.",
                    files_or_modules=["docs/release-notes.md"],
                ),
                _task(
                    "task-internal",
                    title="Refactor CI cache",
                    description="Internal-only cleanup for build pipeline cache.",
                    files_or_modules=[".github/workflows/ci.yml"],
                    metadata={"internal_only": True},
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in report.records}
    assert by_id["task-feature"].category == "user_facing_feature"
    assert by_id["task-feature"].audience == "customers"
    assert by_id["task-feature"].customer_facing_communication_recommended is True
    assert by_id["task-bug"].category == "bug_fix"
    assert by_id["task-bug"].customer_facing_communication_recommended is True
    assert by_id["task-docs"].category == "documentation_only"
    assert by_id["task-docs"].audience == "docs"
    assert by_id["task-internal"].category == "operational_internal"
    assert by_id["task-internal"].audience == "internal"
    assert by_id["task-internal"].customer_facing_communication_recommended is False
    assert report.summary["summary_counts"] == {
        "user_facing_feature": 1,
        "behavior_change": 0,
        "breaking_change": 0,
        "bug_fix": 1,
        "operational_internal": 1,
        "documentation_only": 1,
        "migration_required": 0,
    }
    assert report.summary["customer_facing_count"] == 2


def test_breaking_change_signals_are_elevated_and_target_developers():
    report = build_plan_release_note_impact_report(
        _plan(
            [
                _task(
                    "task-breaking",
                    title="Remove deprecated API field",
                    description=(
                        "Breaking change: delete the legacy customer_id field from the "
                        "public API response."
                    ),
                    files_or_modules=["api/routes/customers.py"],
                    risk_level="high",
                )
            ]
        )
    )

    assert report.records == (
        PlanReleaseNoteImpactRecord(
            task_id="task-breaking",
            title="Remove deprecated API field",
            category="breaking_change",
            severity="critical",
            audience="developers",
            customer_facing_communication_recommended=True,
            evidence=(
                "Classified as breaking_change.",
                "Breaking-change signal found.",
                "Customer-facing surface signal found.",
                "Risk level: high.",
                "Severity: critical.",
            ),
            dependencies=(),
            files_or_modules=("api/routes/customers.py",),
        ),
    )
    assert report.summary["high_or_critical_count"] == 1


def test_migration_signals_are_classified_and_recommended_for_release_notes():
    report = build_plan_release_note_impact_report(
        _plan(
            [
                _task(
                    "task-migration",
                    title="Add tenant schema migration",
                    description=(
                        "Customer accounts require a manual upgrade path and data "
                        "migration before enabling the new limits."
                    ),
                    files_or_modules=["api/schema/tenant_limits.sql"],
                    depends_on=["task-feature"],
                )
            ]
        )
    )

    record = report.records[0]
    assert record.category == "migration_required"
    assert record.severity == "high"
    assert record.customer_facing_communication_recommended is True
    assert record.dependencies == ("task-feature",)
    assert "Migration-required signal found." in record.evidence


def test_internal_only_tasks_stay_separate_even_with_operational_signals():
    report = build_plan_release_note_impact_report(
        _plan(
            [
                _task(
                    "task-runbook",
                    title="Update internal deployment runbook",
                    description="Internal-only rollout telemetry and deployment monitoring.",
                    files_or_modules=["ops/runbooks/deploy.md"],
                    metadata={"customer_facing": False},
                    risk_level="high",
                )
            ]
        )
    )

    record = report.records[0]
    assert record.category == "operational_internal"
    assert record.severity == "medium"
    assert record.audience == "internal"
    assert record.customer_facing_communication_recommended is False
    assert report.summary["internal_only_count"] == 1


def test_stable_ordering_prioritizes_severity_category_and_task_id():
    report = build_plan_release_note_impact_report(
        _plan(
            [
                _task("task-z", title="Refactor build scripts", files_or_modules=["scripts/build.sh"]),
                _task(
                    "task-b",
                    title="Add customer profile page",
                    files_or_modules=["web/pages/profile.tsx"],
                ),
                _task(
                    "task-a",
                    title="Breaking change for public API",
                    description="Breaking change for API clients.",
                    files_or_modules=["api/routes/profile.py"],
                ),
                _task(
                    "task-c",
                    title="Migrate customer notification settings",
                    description="Requires customer data migration.",
                    files_or_modules=["api/schema/notifications.sql"],
                ),
            ]
        )
    )

    assert [record.task_id for record in report.records] == [
        "task-a",
        "task-c",
        "task-b",
        "task-z",
    ]


def test_dictionary_serialization_and_markdown_are_json_compatible():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Change admin notification defaults",
                description="Admins now receive weekly digest notifications by default.",
                metadata={"release_note_audience": "admins"},
            )
        ]
    )
    model = ExecutionPlan.model_validate(plan)

    report = build_plan_release_note_impact_report(model)
    payload = plan_release_note_impact_report_to_dict(report)
    markdown = plan_release_note_impact_report_to_markdown(report)

    assert payload == report.to_dict()
    assert report.to_dicts() == payload["records"]
    assert summarize_plan_release_note_impact(report) == report.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "records"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "category",
        "severity",
        "audience",
        "customer_facing_communication_recommended",
        "evidence",
        "dependencies",
        "files_or_modules",
    ]
    assert markdown.startswith("# Plan Release Note Impact: plan-release-notes")
    assert "| medium | behavior_change | admins | yes |" in markdown


def _plan(tasks):
    return {
        "id": "plan-release-notes",
        "implementation_brief_id": "brief-release-notes",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    depends_on=None,
    metadata=None,
    risk_level=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "depends_on": depends_on or [],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if risk_level is not None:
        task["risk_level"] = risk_level
    return task
