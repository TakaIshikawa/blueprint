import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_data_retention_checklist import (
    PlanDataRetentionChecklist,
    PlanDataRetentionChecklistItem,
    build_plan_data_retention_checklist,
    plan_data_retention_checklist_to_dict,
    plan_data_retention_checklist_to_markdown,
    summarize_plan_data_retention_checklist,
)


def test_logs_exports_and_pii_tasks_build_checklist_items():
    result = build_plan_data_retention_checklist(
        _plan(
            [
                _task(
                    "task-logs",
                    title="Add audit logging",
                    description=(
                        "Persist audit logs with user email addresses for support review."
                    ),
                    files_or_modules=["src/audit/logs.py"],
                    acceptance_criteria=[
                        "Retention period and access control are documented.",
                    ],
                    metadata={"owner_type": "security"},
                ),
                _task(
                    "task-export",
                    title="Customer CSV export",
                    description="Generate CSV exports of customer data for admins.",
                    files_or_modules=["src/exports/customer_csv.py"],
                    metadata={"retention_owner": "data governance"},
                ),
            ]
        )
    )

    assert result.plan_id == "plan-retention"
    assert result.brief_id == "brief-retention"
    assert result.items == (
        PlanDataRetentionChecklistItem(
            scope="task-export: Customer CSV export",
            task_id="task-export",
            data_categories=("pii", "exports"),
            retention_question=(
                "What retention period and purge trigger apply to pii, exports "
                "in task-export: Customer CSV export?"
            ),
            deletion_verification=(
                "Verify generated artifacts are removed or expired after the retention period."
            ),
            owner_role="data governance",
            access_control=(
                "Limit access to roles approved for personal data handling."
            ),
            auditability=(
                "Record who created, downloaded, deleted, or restored each file artifact."
            ),
            backup_handling=(
                "Confirm backup restore procedures do not reintroduce deleted or expired data."
            ),
            documentation_update=(
                "Update data inventory, privacy notes, and retention documentation."
            ),
            evidence=(
                "description: Generate CSV exports of customer data for admins.",
                "files_or_modules: src/exports/customer_csv.py",
                "title: Customer CSV export",
            ),
        ),
        PlanDataRetentionChecklistItem(
            scope="task-logs: Add audit logging",
            task_id="task-logs",
            data_categories=("pii", "logs", "audit_records"),
            retention_question=(
                "What retention period and purge trigger apply to pii, logs, "
                "audit records in task-logs: Add audit logging?"
            ),
            deletion_verification=(
                "Verify deletion policy preserves required audit history while "
                "removing disallowed fields."
            ),
            owner_role="security",
            access_control=(
                "Limit access to roles approved for personal data handling."
            ),
            auditability=(
                "Document immutable audit events, allowed redactions, and review access."
            ),
            backup_handling=(
                "Confirm backup restore procedures do not reintroduce deleted or expired data."
            ),
            documentation_update=(
                "Update data inventory, privacy notes, and retention documentation."
            ),
            evidence=(
                "description: Persist audit logs with user email addresses for support review.",
                "files_or_modules: src/audit/logs.py",
                "title: Add audit logging",
            ),
        ),
    )
    assert result.summary["item_count"] == 2
    assert result.summary["category_counts"]["logs"] == 1
    assert result.summary["category_counts"]["exports"] == 1
    assert result.summary["category_counts"]["pii"] == 2


def test_brief_fields_and_plan_metadata_detect_uploads_analytics_and_backups():
    brief = {
        "id": "brief-retention",
        "title": "Lifecycle planning",
        "data_requirements": (
            "Uploaded attachments and analytics events include personal data."
        ),
        "scope": ["Document retention and deletion obligations."],
    }
    plan = _plan(
        [
            _task(
                "task-backups",
                title="Backup restore policy",
                description="Define database backup snapshots and restore exclusions.",
                files_or_modules=["infra/backups/database_snapshots.tf"],
                metadata={"archive": "Disaster recovery backup retention is 30 days."},
            )
        ]
    )

    result = build_plan_data_retention_checklist(brief, plan)

    assert [item.scope for item in result.items] == [
        "Lifecycle planning",
        "task-backups: Backup restore policy",
    ]
    assert result.items[0].data_categories == ("pii", "analytics", "uploads")
    assert result.items[0].owner_role == "privacy owner"
    assert result.items[1].data_categories == ("backups", "databases")
    assert result.items[1].deletion_verification == (
        "Verify deletion behavior across primary stores and backup restore windows."
    )
    assert result.summary["category_counts"]["uploads"] == 1
    assert result.summary["category_counts"]["analytics"] == 1
    assert result.summary["category_counts"]["backups"] == 1


def test_cache_data_and_generated_files_have_specific_verification():
    result = build_plan_data_retention_checklist(
        _plan(
            [
                _task(
                    "task-cache",
                    title="Cache generated reports",
                    description=(
                        "Cache generated report artifacts with a TTL after deletion."
                    ),
                    files_or_modules=[
                        "src/cache/report_cache.py",
                        "var/generated/report_artifacts.json",
                    ],
                )
            ]
        )
    )

    assert result.items[0].data_categories == ("caches", "generated_files")
    assert result.items[0].deletion_verification == (
        "Verify cache invalidation or TTL expiry removes retained data after deletion."
    )
    assert result.items[0].owner_role == "feature owner"
    assert result.summary["category_counts"]["caches"] == 1
    assert result.summary["category_counts"]["generated_files"] == 1


def test_no_retention_plan_has_empty_summary_and_stable_markdown():
    result = build_plan_data_retention_checklist(
        {"id": "plan-empty", "implementation_brief_id": "brief-empty", "tasks": []}
    )

    assert result.items == ()
    assert result.summary == {
        "item_count": 0,
        "scope_count": 0,
        "task_count": 0,
        "category_counts": {
            "pii": 0,
            "logs": 0,
            "audit_records": 0,
            "analytics": 0,
            "exports": 0,
            "uploads": 0,
            "backups": 0,
            "databases": 0,
            "caches": 0,
            "generated_files": 0,
        },
        "owner_role_counts": {},
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Data Retention Checklist: plan-empty",
            "",
            "No data retention obligations were found.",
        ]
    )


def test_model_input_serializes_stably_and_renders_markdown():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-upload",
                    title="Receipt upload",
                    description="Upload customer receipt files into object storage.",
                    files_or_modules=["src/uploads/receipts.py"],
                )
            ]
        )
    )

    result = summarize_plan_data_retention_checklist(plan)
    payload = plan_data_retention_checklist_to_dict(result)

    assert isinstance(result, PlanDataRetentionChecklist)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["items"]
    assert list(payload) == ["plan_id", "brief_id", "items", "summary"]
    assert list(payload["items"][0]) == [
        "scope",
        "task_id",
        "data_categories",
        "retention_question",
        "deletion_verification",
        "owner_role",
        "access_control",
        "auditability",
        "backup_handling",
        "documentation_update",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert plan_data_retention_checklist_to_markdown(result) == "\n".join(
        [
            "# Plan Data Retention Checklist: plan-retention",
            "",
            "| Scope | Categories | Retention Question | Deletion Verification | Owner Role | Evidence |",
            "| --- | --- | --- | --- | --- | --- |",
            "| task-upload: Receipt upload | pii, uploads | What retention period and purge "
            "trigger apply to pii, uploads in task-upload: Receipt upload? | Verify "
            "deleted records cannot be restored by jobs, syncs, caches, or derived "
            "artifacts. | privacy owner | description: Upload customer receipt files "
            "into object storage.; files_or_modules: src/uploads/receipts.py; "
            "title: Receipt upload |",
        ]
    )


def _plan(tasks):
    return {
        "id": "plan-retention",
        "implementation_brief_id": "brief-retention",
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
    return {
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
