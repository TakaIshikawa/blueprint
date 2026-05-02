import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_file_download_security_readiness import (
    TaskFileDownloadSecurityReadinessPlan,
    TaskFileDownloadSecurityReadinessRecord,
    analyze_task_file_download_security_readiness,
    build_task_file_download_security_readiness_plan,
    derive_task_file_download_security_readiness,
    extract_task_file_download_security_readiness,
    generate_task_file_download_security_readiness,
    recommend_task_file_download_security_readiness,
    summarize_task_file_download_security_readiness,
    task_file_download_security_readiness_plan_to_dict,
    task_file_download_security_readiness_plan_to_dicts,
    task_file_download_security_readiness_plan_to_markdown,
    task_file_download_security_readiness_to_dicts,
)


def test_signed_document_download_requires_core_security_safeguards():
    result = build_task_file_download_security_readiness_plan(
        _plan(
            [
                _task(
                    "task-doc",
                    title="Add signed document download URLs",
                    description="Let users download documents through presigned download URLs.",
                    files_or_modules=["src/downloads/documents/presigned_downloads.py"],
                    acceptance_criteria=["Document downloads return the requested PDF."],
                )
            ]
        )
    )

    assert isinstance(result, TaskFileDownloadSecurityReadinessPlan)
    assert result.download_task_ids == ("task-doc",)
    record = result.records[0]
    assert isinstance(record, TaskFileDownloadSecurityReadinessRecord)
    assert record.download_surfaces == ("signed_download_url", "document_download")
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "authorization_check",
        "short_lived_signed_url",
        "storage_acl",
        "cache_control",
        "audit_logging",
        "content_disposition_safety",
        "malware_quarantine_check",
        "revocation_behavior",
    )
    assert record.readiness == "weak"
    assert "files_or_modules: src/downloads/documents/presigned_downloads.py" in record.evidence
    assert result.summary["task_count"] == 1
    assert result.summary["download_task_count"] == 1
    assert result.summary["readiness_counts"] == {"weak": 1, "partial": 0, "strong": 0}
    assert result.summary["missing_safeguard_counts"]["authorization_check"] == 1
    assert result.summary["surface_counts"]["signed_download_url"] == 1


def test_tags_metadata_and_acceptance_criteria_detect_private_attachment_with_partial_safeguards():
    result = analyze_task_file_download_security_readiness(
        _plan(
            [
                _task(
                    "task-attachments",
                    title="Serve customer private attachments",
                    description="Support secure attachment downloads for customer claims.",
                    files_or_modules=["src/claims/private_files/storage_acl.py"],
                    acceptance_criteria=[
                        "Authorization checks verify owner and tenant access before download.",
                        "Cache-Control uses no-store for private responses.",
                    ],
                    tags=["download attachments", "private-files"],
                    metadata={
                        "audit_logging": "Log download events and denied access.",
                        "headers": {"content_disposition": "Sanitize filenames before setting attachment filename."},
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.download_surfaces == ("private_attachment",)
    assert record.present_safeguards == (
        "authorization_check",
        "storage_acl",
        "cache_control",
        "audit_logging",
        "content_disposition_safety",
    )
    assert record.missing_safeguards == ("malware_quarantine_check", "revocation_behavior")
    assert record.readiness == "partial"
    assert any("tags[0]" in item for item in record.evidence)
    assert any("metadata.audit_logging" in item for item in record.evidence)


def test_bulk_archive_and_media_delivery_are_weak_when_missing_safeguards():
    result = build_task_file_download_security_readiness_plan(
        _plan(
            [
                _task(
                    "task-archive",
                    title="Build bulk archive download",
                    description="Users can download all files as a ZIP archive.",
                    files_or_modules=["src/archives/bulk_downloads.py"],
                ),
                _task(
                    "task-media",
                    title="Add media delivery endpoint",
                    description="Serve media and video downloads through the CDN.",
                    files_or_modules=["src/media/cdn_delivery.ts"],
                    acceptance_criteria=["Audit logging records media download access."],
                ),
            ]
        )
    )

    assert result.download_task_ids == ("task-archive", "task-media")
    archive_record, media_record = result.records
    assert archive_record.download_surfaces == ("bulk_archive_download",)
    assert archive_record.readiness == "weak"
    assert "malware_quarantine_check" in archive_record.missing_safeguards
    assert media_record.download_surfaces == ("media_delivery",)
    assert media_record.present_safeguards == ("audit_logging",)
    assert media_record.readiness == "weak"


def test_fully_covered_download_safeguards_are_strong_readiness():
    result = build_task_file_download_security_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Ship signed export downloads",
                    description="Create signed download URLs for report export downloads.",
                    files_or_modules=["src/exports/signed_downloads.py"],
                    acceptance_criteria=[
                        "Authorization checks enforce tenant access.",
                        "Signed URL expiry is five minutes.",
                        "Storage ACL keeps objects in a private bucket.",
                        "Cache-Control no-store is set on private downloads.",
                        "Audit logging records download, denial, and storage lookup events.",
                        "Content-Disposition uses sanitized filenames.",
                        "Malware scan status blocks quarantined or infected files.",
                        "Revocation behavior invalidates links when permissions change.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.download_surfaces == ("signed_download_url", "export_download")
    assert record.present_safeguards == (
        "authorization_check",
        "short_lived_signed_url",
        "storage_acl",
        "cache_control",
        "audit_logging",
        "content_disposition_safety",
        "malware_quarantine_check",
        "revocation_behavior",
    )
    assert record.missing_safeguards == ()
    assert record.readiness == "strong"
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "strong": 1}
    assert result.summary["missing_safeguard_count"] == 0


def test_non_download_tasks_are_no_signal_without_false_positives():
    result = build_task_file_download_security_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="Adjust settings labels and loading states.",
                    files_or_modules=["src/ui/settings_panel.tsx"],
                    tags=["dependencies", "onboarding"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.download_task_ids == ()
    assert result.no_signal_task_ids == ("task-copy",)
    assert result.ignored_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "download_task_count": 0,
        "download_task_ids": [],
        "impacted_task_ids": [],
        "no_signal_task_ids": ["task-copy"],
        "surface_count": 0,
        "missing_safeguard_count": 0,
        "readiness_counts": {"weak": 0, "partial": 0, "strong": 0},
        "surface_counts": {
            "signed_download_url": 0,
            "document_download": 0,
            "export_download": 0,
            "media_delivery": 0,
            "private_attachment": 0,
            "bulk_archive_download": 0,
        },
        "present_safeguard_counts": {
            "authorization_check": 0,
            "short_lived_signed_url": 0,
            "storage_acl": 0,
            "cache_control": 0,
            "audit_logging": 0,
            "content_disposition_safety": 0,
            "malware_quarantine_check": 0,
            "revocation_behavior": 0,
        },
        "missing_safeguard_counts": {
            "authorization_check": 0,
            "short_lived_signed_url": 0,
            "storage_acl": 0,
            "cache_control": 0,
            "audit_logging": 0,
            "content_disposition_safety": 0,
            "malware_quarantine_check": 0,
            "revocation_behavior": 0,
        },
    }
    assert "No task file download security readiness records" in result.to_markdown()
    assert "No-signal tasks: task-copy" in result.to_markdown()


def test_deterministic_serialization_markdown_aliases_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Download files | secure path",
                description="Add file download with authorization checks, signed URL expiry, storage ACL, cache-control no-store, audit logging, content-disposition, malware scan status, and revocation behavior.",
                files_or_modules=["src/downloads/files.py"],
            ),
            _task(
                "task-a",
                title="Add signed download URL",
                description="Create presigned download URLs for private attachments.",
            ),
            _task("task-copy", title="Update empty state", description="Change UI copy."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_file_download_security_readiness(plan)
    payload = task_file_download_security_readiness_plan_to_dict(result)
    markdown = task_file_download_security_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_file_download_security_readiness_plan_to_dicts(result) == payload["records"]
    assert task_file_download_security_readiness_to_dicts(result.records) == payload["records"]
    assert extract_task_file_download_security_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_file_download_security_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_file_download_security_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_file_download_security_readiness(plan).to_dict() == result.to_dict()
    assert result.findings == result.records
    assert result.recommendations == result.records
    assert result.impacted_task_ids == result.download_task_ids
    assert result.download_task_ids == ("task-a", "task-z")
    assert result.no_signal_task_ids == ("task-copy",)
    assert result.records[0].matched_signals == result.records[0].download_surfaces
    assert result.records[0].recommendations == result.records[0].recommended_readiness_steps
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "download_task_ids",
        "impacted_task_ids",
        "no_signal_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "download_surfaces",
        "present_safeguards",
        "missing_safeguards",
        "readiness",
        "evidence",
        "recommended_readiness_steps",
    ]
    assert [record.readiness for record in result.records] == ["weak", "strong"]
    assert markdown.startswith("# Task File Download Security Readiness: plan-download")
    assert "Download files \\| secure path" in markdown
    assert "| Task | Title | Readiness | Surfaces | Present Safeguards | Missing Safeguards | Recommendations | Evidence |" in markdown


def test_execution_plan_input_is_supported():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Add report export download",
                    description="Export downloads support authorization checks and storage ACL.",
                    files_or_modules=["src/exports/report_download.py"],
                    acceptance_criteria=[
                        "Cache-Control no-store is set.",
                        "Audit log records download events.",
                        "Content-Disposition uses safe filenames.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )

    result = build_task_file_download_security_readiness_plan(model)

    assert result.plan_id == "plan-model"
    assert result.records[0].task_id == "task-model"
    assert result.records[0].readiness == "strong"
    assert result.records[0].download_surfaces == ("export_download",)


def _plan(tasks, plan_id="plan-download"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-download",
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
        "execution_plan_id": "plan-download",
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
