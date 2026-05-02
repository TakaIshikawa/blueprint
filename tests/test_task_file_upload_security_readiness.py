import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_file_upload_security_readiness import (
    TaskFileUploadSecurityReadinessPlan,
    TaskFileUploadSecurityReadinessRecord,
    analyze_task_file_upload_security_readiness,
    build_task_file_upload_security_readiness_plan,
    extract_task_file_upload_security_readiness,
    generate_task_file_upload_security_readiness,
    recommend_task_file_upload_security_readiness,
    summarize_task_file_upload_security_readiness,
    task_file_upload_security_readiness_plan_to_dict,
    task_file_upload_security_readiness_plan_to_markdown,
)


def test_signed_avatar_upload_requires_core_security_safeguards():
    result = build_task_file_upload_security_readiness_plan(
        _plan(
            [
                _task(
                    "task-avatar",
                    title="Add avatar upload with signed upload URL",
                    description="Let users upload a profile photo using a presigned upload URL.",
                    files_or_modules=["src/uploads/avatars/presigned_uploads.py"],
                    acceptance_criteria=["Avatar renders after upload."],
                )
            ]
        )
    )

    assert isinstance(result, TaskFileUploadSecurityReadinessPlan)
    assert result.upload_task_ids == ("task-avatar",)
    record = result.records[0]
    assert isinstance(record, TaskFileUploadSecurityReadinessRecord)
    assert record.upload_surfaces == ("avatar_upload", "signed_upload_url", "user_generated_files")
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "file_type_allowlist",
        "size_limit",
        "malware_scan",
        "storage_acl",
        "signed_url_expiry",
        "content_moderation",
        "audit_logging",
    )
    assert record.risk_level == "high"
    assert "files_or_modules: src/uploads/avatars/presigned_uploads.py" in record.evidence
    assert result.summary["task_count"] == 1
    assert result.summary["upload_task_count"] == 1
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}
    assert result.summary["missing_safeguard_counts"]["malware_scan"] == 1
    assert result.summary["surface_counts"]["signed_upload_url"] == 1


def test_metadata_risks_and_files_detect_document_attachment_with_partial_safeguards():
    result = analyze_task_file_upload_security_readiness(
        _plan(
            [
                _task(
                    "task-attachments",
                    title="Add claim document attachments",
                    description="Support document attachments for customer claims.",
                    files_or_modules=["src/documents/attachments/storage_acl.py"],
                    acceptance_criteria=[
                        "Allowed file types are limited to PDF and PNG.",
                        "Maximum file size is 10 MB.",
                    ],
                    risks=["Attached files need threat handling before storage."],
                    metadata={
                        "storage": {"bucket_acl": "private bucket with tenant isolation"},
                        "audit_logging": "Log upload events and rejection reasons.",
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.upload_surfaces == ("document_attachment",)
    assert record.present_safeguards == (
        "file_type_allowlist",
        "size_limit",
        "storage_acl",
        "audit_logging",
    )
    assert record.missing_safeguards == ("malware_scan",)
    assert record.risk_level == "medium"
    assert any("risks[0]" in item for item in record.evidence)
    assert any("metadata.audit_logging" in item for item in record.evidence)


def test_bulk_import_and_media_upload_are_high_risk_when_missing_safeguards():
    result = build_task_file_upload_security_readiness_plan(
        _plan(
            [
                _task(
                    "task-import",
                    title="Build bulk CSV import",
                    description="Add a bulk import flow for customer spreadsheet files.",
                    files_or_modules=["src/imports/customer_csv_importer.py"],
                    acceptance_criteria=["CSV rows are parsed."],
                ),
                _task(
                    "task-media",
                    title="Add media upload moderation",
                    description="Users can upload videos to the gallery.",
                    files_or_modules=["src/media/video_upload.ts"],
                    acceptance_criteria=[
                        "Content moderation sends unsafe media to a moderation queue.",
                        "Audit logging records upload and moderation decisions.",
                    ],
                ),
            ]
        )
    )

    assert result.upload_task_ids == ("task-import", "task-media")
    import_record, media_record = result.records
    assert import_record.upload_surfaces == ("bulk_import",)
    assert import_record.risk_level == "high"
    assert "file_type_allowlist" in import_record.missing_safeguards
    assert media_record.upload_surfaces == ("media_upload", "user_generated_files")
    assert media_record.present_safeguards == ("content_moderation", "audit_logging")
    assert media_record.risk_level == "high"


def test_fully_covered_upload_safeguards_are_low_risk():
    result = build_task_file_upload_security_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Ship direct upload flow",
                    description="Create signed upload URLs for user-generated files.",
                    files_or_modules=["src/uploads/direct_uploads.py"],
                    acceptance_criteria=[
                        "File type allowlist validates MIME type and extension.",
                        "File size limit is 25 MB.",
                        "Malware scanning quarantines infected files.",
                        "Storage ACL keeps objects private with tenant isolation.",
                        "Signed URL expiry is 5 minutes.",
                        "Content moderation reviews user-visible files.",
                        "Audit logging records upload, scan, access, and delete events.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.present_safeguards == (
        "file_type_allowlist",
        "size_limit",
        "malware_scan",
        "storage_acl",
        "signed_url_expiry",
        "content_moderation",
        "audit_logging",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "low"
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}
    assert result.summary["missing_safeguard_count"] == 0


def test_non_upload_tasks_are_ignored_and_summary_is_stable():
    result = build_task_file_upload_security_readiness_plan(
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
    assert result.upload_task_ids == ()
    assert result.ignored_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "upload_task_count": 0,
        "ignored_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_safeguard_counts": {
            "file_type_allowlist": 0,
            "size_limit": 0,
            "malware_scan": 0,
            "storage_acl": 0,
            "signed_url_expiry": 0,
            "content_moderation": 0,
            "audit_logging": 0,
        },
        "surface_counts": {},
    }
    assert "No file upload security readiness records" in result.to_markdown()
    assert "Ignored tasks: task-copy" in result.to_markdown()


def test_deterministic_serialization_markdown_aliases_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Upload files | secure path",
                description="Add file upload with allowed file types, size limit, malware scanning, storage ACL, signed URL expiry, content moderation, and audit logging.",
                files_or_modules=["src/uploads/files.py"],
            ),
            _task(
                "task-a",
                title="Add signed upload URL",
                description="Create presigned upload URLs for user-generated files.",
            ),
            _task("task-copy", title="Update empty state", description="Change UI copy."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_file_upload_security_readiness(plan)
    payload = task_file_upload_security_readiness_plan_to_dict(result)
    markdown = task_file_upload_security_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert extract_task_file_upload_security_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_file_upload_security_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_file_upload_security_readiness(plan).to_dict() == result.to_dict()
    assert result.upload_task_ids == ("task-a", "task-z")
    assert result.ignored_task_ids == ("task-copy",)
    assert list(payload) == [
        "plan_id",
        "records",
        "upload_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "upload_surfaces",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "recommended_readiness_steps",
    ]
    assert [record.risk_level for record in result.records] == ["high", "low"]
    assert markdown.startswith("# Task File Upload Security Readiness: plan-upload")
    assert "Upload files \\| secure path" in markdown
    assert "| Task | Title | Risk | Surfaces | Missing Safeguards | Evidence |" in markdown


def test_execution_plan_input_is_supported():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Add profile photo upload",
                    description="Avatar upload supports file type allowlist and file size limit.",
                    files_or_modules=["src/uploads/avatar_upload.py"],
                    acceptance_criteria=[
                        "Malware scan runs before use.",
                        "Storage ACL keeps private objects isolated by tenant.",
                        "Signed URL expiry is five minutes.",
                        "Content moderation reviews user-visible images.",
                        "Audit log records upload events.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )

    result = build_task_file_upload_security_readiness_plan(model)

    assert result.plan_id == "plan-model"
    assert result.records[0].task_id == "task-model"
    assert result.records[0].risk_level == "low"
    assert result.records[0].upload_surfaces == ("avatar_upload", "media_upload", "user_generated_files")


def _plan(tasks, plan_id="plan-upload"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-upload",
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
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-upload",
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
    if risks is not None:
        payload["risks"] = risks
    return payload
