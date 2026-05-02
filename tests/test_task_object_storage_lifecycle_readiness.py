import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_object_storage_lifecycle_readiness import (
    TaskObjectStorageLifecycleReadinessPlan,
    TaskObjectStorageLifecycleReadinessRecord,
    analyze_task_object_storage_lifecycle_readiness,
    build_task_object_storage_lifecycle_readiness_plan,
    extract_task_object_storage_lifecycle_readiness,
    generate_task_object_storage_lifecycle_readiness,
    summarize_task_object_storage_lifecycle_readiness,
    task_object_storage_lifecycle_readiness_plan_to_dict,
    task_object_storage_lifecycle_readiness_plan_to_markdown,
)


def test_bucket_lifecycle_change_requires_all_lifecycle_safeguards():
    result = build_task_object_storage_lifecycle_readiness_plan(
        _plan(
            [
                _task(
                    "task-bucket",
                    title="Add S3 bucket cleanup rules",
                    description="Configure rules that expire uploaded files in the customer bucket.",
                    files_or_modules=["infra/s3/buckets/customer_uploads_lifecycle.tf"],
                    acceptance_criteria=["Old objects are deleted automatically."],
                )
            ]
        )
    )

    assert isinstance(result, TaskObjectStorageLifecycleReadinessPlan)
    assert result.object_storage_task_ids == ("task-bucket",)
    record = result.records[0]
    assert isinstance(record, TaskObjectStorageLifecycleReadinessRecord)
    assert record.storage_providers == ("s3", "generic_object_storage")
    assert record.detected_signals == (
        "object_storage",
        "bucket",
        "upload_storage",
        "retention_sensitive",
        "destructive_lifecycle",
    )
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "retention_policy",
        "lifecycle_expiration",
        "access_tier_archive_behavior",
        "deletion_recovery",
        "encryption_owner_evidence",
        "cost_quota_monitoring",
    )
    assert record.risk_level == "high"
    assert "files_or_modules: infra/s3/buckets/customer_uploads_lifecycle.tf" in record.evidence
    assert result.summary["task_count"] == 1
    assert result.summary["object_storage_task_count"] == 1
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}
    assert result.summary["missing_safeguard_counts"]["deletion_recovery"] == 1
    assert result.summary["signal_counts"]["bucket"] == 1


def test_generated_file_archive_paths_detect_archive_tier_and_retention_risk():
    result = analyze_task_object_storage_lifecycle_readiness(
        _plan(
            [
                _task(
                    "task-archive",
                    title="Archive generated report exports",
                    description="Move generated reports to a GCS archive path for long-term retention.",
                    files_or_modules=["src/exports/generated/archive_paths.py"],
                    tags=["cloud storage"],
                    metadata={"storage": {"provider": "gcs", "bucket": "report-archive"}},
                )
            ]
        )
    )

    record = result.records[0]
    assert record.storage_providers == ("gcs", "generic_object_storage")
    assert record.detected_signals == (
        "object_storage",
        "bucket",
        "generated_files",
        "archive_path",
        "retention_sensitive",
    )
    assert "access_tier_archive_behavior" in record.missing_safeguards
    assert record.risk_level == "high"
    assert any("metadata.storage.bucket" in item for item in record.evidence)
    assert result.summary["storage_provider_counts"]["gcs"] == 1


def test_partial_safeguards_leave_medium_risk_when_not_destructive():
    result = build_task_object_storage_lifecycle_readiness_plan(
        _plan(
            [
                _task(
                    "task-media",
                    title="Store media assets in blob storage",
                    description="Save media assets in Azure Blob Storage for the gallery.",
                    files_or_modules=["src/media/blob_store/assets.py"],
                    acceptance_criteria=[
                        "Retention policy keeps originals for 90 days.",
                        "Lifecycle expiration is configured for thumbnails.",
                        "Encryption uses KMS and data owner approval is attached.",
                    ],
                    metadata={"monitoring": "Cost monitoring and storage quota alerts are configured."},
                )
            ]
        )
    )

    record = result.records[0]
    assert record.storage_providers == ("azure_blob", "generic_object_storage")
    assert record.detected_signals == ("object_storage", "blob_store", "media_assets", "retention_sensitive")
    assert record.present_safeguards == (
        "retention_policy",
        "lifecycle_expiration",
        "encryption_owner_evidence",
        "cost_quota_monitoring",
    )
    assert record.missing_safeguards == (
        "access_tier_archive_behavior",
        "deletion_recovery",
    )
    assert record.risk_level == "medium"
    assert any("metadata.monitoring" in item for item in record.evidence)


def test_fully_covered_object_storage_lifecycle_safeguards_are_low_risk():
    result = build_task_object_storage_lifecycle_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Ship object storage lifecycle policy",
                    description="Configure S3 bucket lifecycle rules for generated invoice PDFs.",
                    files_or_modules=["infra/s3/buckets/invoice_lifecycle.tf"],
                    acceptance_criteria=[
                        "Retention policy keeps invoices for seven years and documents legal hold exceptions.",
                        "Lifecycle expiration moves and expires objects after the approved period.",
                        "Access tier archive behavior uses Glacier and documents restore from archive.",
                        "Deletion recovery uses versioning and object lock with a recovery window.",
                        "Encryption uses KMS and bucket owner approval is attached.",
                        "Cost monitoring tracks storage metrics, object count, quota, and budget alerts.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.present_safeguards == (
        "retention_policy",
        "lifecycle_expiration",
        "access_tier_archive_behavior",
        "deletion_recovery",
        "encryption_owner_evidence",
        "cost_quota_monitoring",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "low"
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}
    assert result.summary["missing_safeguard_count"] == 0


def test_unrelated_tasks_return_empty_records_and_stable_not_applicable_summary():
    result = build_task_object_storage_lifecycle_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="Adjust settings labels and loading states.",
                    files_or_modules=["src/ui/settings_panel.tsx"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.object_storage_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "object_storage_task_count": 0,
        "not_applicable_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_safeguard_counts": {
            "retention_policy": 0,
            "lifecycle_expiration": 0,
            "access_tier_archive_behavior": 0,
            "deletion_recovery": 0,
            "encryption_owner_evidence": 0,
            "cost_quota_monitoring": 0,
        },
        "signal_counts": {},
        "storage_provider_counts": {},
    }
    assert "No object storage lifecycle readiness records" in result.to_markdown()
    assert "Not-applicable tasks: task-copy" in result.to_markdown()


def test_deterministic_serialization_markdown_aliases_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Object storage lifecycle | ready",
                description="S3 bucket lifecycle rules include retention policy, lifecycle expiration, archive tier behavior, deletion recovery, encryption owner approval, and cost monitoring.",
                files_or_modules=["infra/s3/buckets/archive_lifecycle.tf"],
            ),
            _task(
                "task-a",
                title="Purge generated files",
                description="Delete old generated report exports from object storage.",
            ),
            _task("task-copy", title="Update empty state", description="Change UI copy."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_object_storage_lifecycle_readiness(plan)
    payload = task_object_storage_lifecycle_readiness_plan_to_dict(result)
    markdown = task_object_storage_lifecycle_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert extract_task_object_storage_lifecycle_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_object_storage_lifecycle_readiness(plan).to_dict() == result.to_dict()
    assert result.object_storage_task_ids == ("task-a", "task-z")
    assert result.not_applicable_task_ids == ("task-copy",)
    assert list(payload) == [
        "plan_id",
        "records",
        "object_storage_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "storage_providers",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "recommended_readiness_steps",
    ]
    assert [record.risk_level for record in result.records] == ["high", "low"]
    assert markdown.startswith("# Task Object Storage Lifecycle Readiness: plan-object-storage")
    assert "Object storage lifecycle \\| ready" in markdown
    assert "| Task | Title | Risk | Providers | Signals | Missing Safeguards | Evidence |" in markdown


def test_execution_plan_input_is_supported():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Add upload archive bucket lifecycle",
                    description="Object storage lifecycle rules retain uploaded files before archive expiration.",
                    files_or_modules=["infra/gcs/buckets/upload_archive_lifecycle.tf"],
                    acceptance_criteria=[
                        "Retention policy is owner approved.",
                        "Lifecycle expiration is configured.",
                        "Archive tier behavior documents rehydration.",
                        "Soft delete versioning supports deletion recovery.",
                        "KMS encryption and service owner sign-off are attached.",
                        "Quota monitoring and budget alerts are configured.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )

    result = build_task_object_storage_lifecycle_readiness_plan(model)

    assert result.plan_id == "plan-model"
    assert result.records[0].task_id == "task-model"
    assert result.records[0].risk_level == "low"
    assert "archive_path" in result.records[0].detected_signals


def _plan(tasks, plan_id="plan-object-storage"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-object-storage",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
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
    metadata=None,
    tags=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-object-storage",
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
    if tags is not None:
        payload["tags"] = tags
    return payload
