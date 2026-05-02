import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_file_upload_safety import (
    TaskFileUploadSafetyPlan,
    TaskFileUploadSafetyRecord,
    analyze_task_file_upload_safety,
    build_task_file_upload_safety_plan,
    extract_task_file_upload_safety,
    generate_task_file_upload_safety,
    recommend_task_file_upload_safety,
    summarize_task_file_upload_safety,
    task_file_upload_safety_plan_to_dict,
    task_file_upload_safety_plan_to_markdown,
)


def test_upload_task_reports_required_and_missing_safeguards():
    result = build_task_file_upload_safety_plan(
        _plan(
            [
                _task(
                    "task-upload",
                    title="Add customer file upload",
                    description="Allow users to upload files for support review.",
                    files_or_modules=["src/uploads/customer_files.py"],
                    acceptance_criteria=["Files can be attached to a ticket."],
                )
            ]
        )
    )

    assert isinstance(result, TaskFileUploadSafetyPlan)
    assert result.upload_task_ids == ("task-upload",)
    record = result.records[0]
    assert isinstance(record, TaskFileUploadSafetyRecord)
    assert record.upload_vectors == ("file_upload", "user_supplied_file")
    assert record.required_safeguards == (
        "file_type_validation",
        "size_limit",
        "malware_scanning",
        "storage_isolation",
        "storage_cleanup",
        "failure_handling",
    )
    assert record.present_safeguards == ()
    assert record.missing_safeguards == record.required_safeguards
    assert record.readiness_level == "missing"
    assert "files_or_modules: src/uploads/customer_files.py" in record.evidence
    assert result.summary["task_count"] == 1
    assert result.summary["upload_task_count"] == 1
    assert result.summary["missing_safeguard_counts"]["malware_scanning"] == 1
    assert result.summary["vector_counts"]["file_upload"] == 1


def test_antivirus_validation_size_and_cleanup_are_strong():
    result = build_task_file_upload_safety_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Ship safe CSV ingestion",
                    description="Ingest CSV files uploaded by users.",
                    files_or_modules=["src/imports/csv/uploader.py"],
                    acceptance_criteria=[
                        "File type validation allows only CSV MIME type and extension.",
                        "File size limit is 20 MB at API and parser boundaries.",
                        "Antivirus scanning quarantines infected files before parsing.",
                        "Storage isolation uses a private tenant-specific quarantine bucket.",
                        "Storage cleanup deletes temporary, rejected, and orphaned uploads with a TTL lifecycle rule.",
                        "Failure handling covers upload failure, scan failure, parser failure, retries, and cleanup on failure.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.upload_vectors == ("file_upload", "csv_ingestion", "bulk_import")
    assert record.present_safeguards == (
        "file_type_validation",
        "size_limit",
        "malware_scanning",
        "storage_isolation",
        "storage_cleanup",
        "failure_handling",
    )
    assert record.missing_safeguards == ()
    assert record.readiness_level == "strong"
    assert result.summary["readiness_counts"] == {"missing": 0, "partial": 0, "ready": 0, "strong": 1}
    assert result.summary["missing_safeguard_count"] == 0


def test_partial_document_attachment_detects_metadata_and_risks():
    result = analyze_task_file_upload_safety(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Add document attachments",
                    description="Customers attach claim documents and invoice PDFs.",
                    acceptance_criteria=[
                        "Allowed file types are PDF and PNG.",
                        "Maximum file size is 10 MB.",
                    ],
                    risks=["User-supplied files need scan failure behavior."],
                    metadata={
                        "storage_isolation": "Private bucket with tenant isolation.",
                        "cleanup": {"storage_cleanup": "Purge rejected uploads daily."},
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.upload_vectors == ("attachment", "document_upload", "user_supplied_file")
    assert record.present_safeguards == (
        "file_type_validation",
        "size_limit",
        "storage_isolation",
        "storage_cleanup",
        "failure_handling",
    )
    assert record.missing_safeguards == ("malware_scanning",)
    assert record.readiness_level == "partial"
    assert any("risks[0]" in item for item in record.evidence)
    assert any("metadata.storage_isolation" in item for item in record.evidence)


def test_non_upload_tasks_return_empty_deterministic_result():
    result = build_task_file_upload_safety_plan(
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
        "readiness_counts": {"missing": 0, "partial": 0, "ready": 0, "strong": 0},
        "missing_safeguard_counts": {
            "file_type_validation": 0,
            "size_limit": 0,
            "malware_scanning": 0,
            "storage_isolation": 0,
            "storage_cleanup": 0,
            "failure_handling": 0,
        },
        "vector_counts": {},
    }
    assert "No file upload safety records" in result.to_markdown()
    assert "Ignored tasks: task-copy" in result.to_markdown()


def test_deterministic_serialization_markdown_aliases_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Upload files | safe path",
                description=(
                    "Add file upload with file type validation, file size limit, malware scanning, "
                    "storage isolation, storage cleanup, and failure handling."
                ),
                files_or_modules=["src/uploads/files.py"],
            ),
            _task(
                "task-a",
                title="Import CSV attachments",
                description="Build CSV import for user-supplied attached files.",
            ),
            _task("task-copy", title="Update empty state", description="Change UI copy."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_file_upload_safety(plan)
    payload = task_file_upload_safety_plan_to_dict(result)
    markdown = task_file_upload_safety_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert extract_task_file_upload_safety(plan).to_dict() == result.to_dict()
    assert generate_task_file_upload_safety(plan).to_dict() == result.to_dict()
    assert recommend_task_file_upload_safety(plan).to_dict() == result.to_dict()
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
        "upload_vectors",
        "required_safeguards",
        "present_safeguards",
        "missing_safeguards",
        "readiness_level",
        "evidence",
        "recommended_follow_up_actions",
    ]
    assert [record.readiness_level for record in result.records] == ["missing", "strong"]
    assert markdown.startswith("# Task File Upload Safety: plan-upload-safety")
    assert "Upload files \\| safe path" in markdown
    assert "| Task | Title | Readiness | Upload Vectors | Missing Safeguards | Recommended Follow-up Actions | Evidence |" in markdown


def test_execution_plan_and_iterable_inputs_are_supported():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Add media upload",
                    description="Media upload accepts user-provided videos.",
                    acceptance_criteria=[
                        "File type validation rejects unsupported media types.",
                        "File size limit is 50 MB.",
                        "Malware scan runs before processing.",
                        "Storage isolation uses private object storage.",
                        "Storage cleanup removes partial upload files.",
                        "Failure handling shows user feedback and retries transient storage failures.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )
    iterable_result = build_task_file_upload_safety_plan(
        [
            _task(
                "task-one",
                title="Parse uploaded CSV",
                description="CSV ingestion handles uploaded CSV files with antivirus scanning.",
            )
        ]
    )

    result = build_task_file_upload_safety_plan(model)

    assert result.plan_id == "plan-model"
    assert result.records[0].task_id == "task-model"
    assert result.records[0].readiness_level == "strong"
    assert result.records[0].upload_vectors == ("file_upload", "media_upload", "user_supplied_file")
    assert iterable_result.plan_id is None
    assert iterable_result.upload_task_ids == ("task-one",)


def _plan(tasks, plan_id="plan-upload-safety"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-upload-safety",
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
        "execution_plan_id": "plan-upload-safety",
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
