import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_file_upload_security_readiness import build_task_file_upload_security_readiness_plan
from blueprint.task_large_file_upload_resumability_readiness import (
    TaskLargeFileUploadResumabilityReadinessFinding,
    TaskLargeFileUploadResumabilityReadinessPlan,
    analyze_task_large_file_upload_resumability_readiness,
    build_task_large_file_upload_resumability_readiness_plan,
    extract_task_large_file_upload_resumability_readiness,
    generate_task_large_file_upload_resumability_readiness,
    summarize_task_large_file_upload_resumability_readiness,
    task_large_file_upload_resumability_readiness_plan_to_dict,
    task_large_file_upload_resumability_readiness_plan_to_dicts,
)


def test_ready_resumable_large_upload_task_has_no_actionable_gaps():
    result = analyze_task_large_file_upload_resumability_readiness(
        _plan(
            [
                _task(
                    "task-video-resume",
                    title="Implement resumable large video uploads",
                    description=(
                        "Build resumable uploads for large video files using upload chunks. Define the chunk "
                        "protocol with part size, byte ranges, upload offset, and complete multipart behavior. "
                        "Create upload sessions with upload id, session lifecycle expiry, complete and abort states. "
                        "Validate SHA-256 checksums and ETags, retry failed chunks with backoff after network "
                        "interruption, clean up orphaned chunks and stale upload sessions, enforce tenant quota "
                        "and file size limits, handoff completed files to malware scanning and quarantine, emit "
                        "upload progress events, metrics, logs, and traces, and show paused, failed, resume, "
                        "cancel, and completed user recovery states."
                    ),
                    files_or_modules=["src/uploads/resumable_large_file_upload.py"],
                    acceptance_criteria=[
                        "Tests cover chunk protocol, upload session lifecycle, checksum mismatch, retry, cleanup, quota, scanning handoff, telemetry, and recovery states.",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskLargeFileUploadResumabilityReadinessPlan)
    assert result.plan_id == "plan-large-upload"
    assert result.upload_task_ids == ("task-video-resume",)
    finding = result.findings[0]
    assert isinstance(finding, TaskLargeFileUploadResumabilityReadinessFinding)
    assert finding.detected_signals == (
        "large_file_upload",
        "resumable_upload",
        "chunked_upload",
        "multipart_upload",
    )
    assert finding.present_requirements == (
        "chunk_protocol",
        "session_lifecycle",
        "integrity_validation",
        "retry_behavior",
        "partial_cleanup",
        "quota_enforcement",
        "security_scanning_handoff",
        "progress_telemetry",
        "user_recovery_states",
    )
    assert finding.missing_requirements == ()
    assert finding.actionable_gaps == ()
    assert finding.risk_level == "low"
    assert "files_or_modules: src/uploads/resumable_large_file_upload.py" in finding.evidence
    assert result.summary["upload_task_count"] == 1
    assert result.summary["missing_requirement_count"] == 0
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_partial_large_upload_task_reports_specific_actionable_gaps():
    result = build_task_large_file_upload_resumability_readiness_plan(
        _plan(
            [
                _task(
                    "task-s3-multipart",
                    title="Add S3 multipart uploads for large datasets",
                    description=(
                        "Support large file uploads with S3 multipart uploads and signed upload URLs. "
                        "Use upload chunks with part size and part numbers, and validate ETag checksums."
                    ),
                    metadata={"storage": "Direct object storage upload path"},
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-s3-multipart"
    assert finding.detected_signals == (
        "large_file_upload",
        "chunked_upload",
        "multipart_upload",
        "direct_object_upload",
    )
    assert finding.present_requirements == ("chunk_protocol", "integrity_validation")
    assert finding.missing_requirements == (
        "session_lifecycle",
        "retry_behavior",
        "partial_cleanup",
        "quota_enforcement",
        "security_scanning_handoff",
        "progress_telemetry",
        "user_recovery_states",
    )
    assert finding.risk_level == "high"
    assert finding.actionable_gaps == (
        "Specify upload session creation, persistence, expiry, completion, abort, and ownership rules.",
        "Describe bounded retry and resume behavior for failed chunks, network drops, and duplicate submissions.",
        "Plan cleanup for orphaned chunks, stale sessions, abandoned multipart uploads, and failed assemblies.",
        "Enforce file, user, tenant, and storage quotas before and during resumable uploads.",
        "Define handoff from completed upload assembly to malware/security scanning and quarantine states.",
        "Emit progress events, metrics, logs, and traces for upload progress, failures, and completion.",
        "Define user-facing paused, failed, resumable, retryable, cancelled, and completed recovery states.",
    )
    assert result.summary["missing_requirement_counts"]["session_lifecycle"] == 1
    assert result.summary["present_requirement_counts"]["chunk_protocol"] == 1


def test_unrelated_basic_upload_and_explicit_no_impact_tasks_are_not_applicable():
    result = build_task_large_file_upload_resumability_readiness_plan(
        _plan(
            [
                _task(
                    "task-avatar",
                    title="Update avatar upload validation",
                    description="No resumable or chunked large file upload changes are required for this release.",
                    files_or_modules=["src/uploads/avatar_upload.py"],
                ),
                _task(
                    "task-copy",
                    title="Polish uploader copy",
                    description="Adjust helper text near the file picker.",
                ),
            ]
        )
    )

    assert result.findings == ()
    assert result.records == ()
    assert result.upload_task_ids == ()
    assert result.not_applicable_task_ids == ("task-avatar", "task-copy")
    assert result.to_dicts() == []
    assert result.summary == {
        "total_task_count": 2,
        "upload_task_count": 0,
        "not_applicable_task_ids": ["task-avatar", "task-copy"],
        "missing_requirement_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "large_file_upload": 0,
            "resumable_upload": 0,
            "chunked_upload": 0,
            "multipart_upload": 0,
            "direct_object_upload": 0,
        },
        "present_requirement_counts": {
            "chunk_protocol": 0,
            "session_lifecycle": 0,
            "integrity_validation": 0,
            "retry_behavior": 0,
            "partial_cleanup": 0,
            "quota_enforcement": 0,
            "security_scanning_handoff": 0,
            "progress_telemetry": 0,
            "user_recovery_states": 0,
        },
        "missing_requirement_counts": {
            "chunk_protocol": 0,
            "session_lifecycle": 0,
            "integrity_validation": 0,
            "retry_behavior": 0,
            "partial_cleanup": 0,
            "quota_enforcement": 0,
            "security_scanning_handoff": 0,
            "progress_telemetry": 0,
            "user_recovery_states": 0,
        },
    }


def test_model_object_aliases_serialization_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Resumable upload telemetry",
                description="Add resumable upload progress events and metrics for large file uploads.",
            ),
            _task(
                "task-a",
                title="Chunked upload retry recovery",
                description=(
                    "Chunked uploads use upload sessions with session ttl, retry failed chunks after network "
                    "interruption, cleanup orphaned chunks, and show retry button recovery states."
                ),
                metadata={
                    "chunk_protocol": "Chunk size and upload offset are part of the protocol.",
                    "integrity_validation": "Checksum validation rejects mismatched chunks.",
                },
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_task_large_file_upload_resumability_readiness(model)
    payload = task_large_file_upload_resumability_readiness_plan_to_dict(result)
    task_result = build_task_large_file_upload_resumability_readiness_plan(
        ExecutionTask.model_validate(plan["tasks"][1])
    )
    object_result = build_task_large_file_upload_resumability_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="Multipart resumable uploads",
            description="Multipart resumable upload sessions retry failed parts and cleanup abandoned uploads.",
        )
    )

    assert plan == original
    assert result.upload_task_ids == ("task-z", "task-a")
    assert result.records == result.findings
    assert task_result.findings[0].task_id == "task-a"
    assert object_result.findings[0].task_id == "task-object"
    assert (
        extract_task_large_file_upload_resumability_readiness(plan).to_dict()
        == summarize_task_large_file_upload_resumability_readiness(plan).to_dict()
    )
    assert (
        generate_task_large_file_upload_resumability_readiness(plan).to_dict()
        == summarize_task_large_file_upload_resumability_readiness(plan).to_dict()
    )
    assert task_large_file_upload_resumability_readiness_plan_to_dicts(result) == payload["findings"]
    assert task_large_file_upload_resumability_readiness_plan_to_dicts(result.findings) == payload["findings"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "findings",
        "upload_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["findings"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_requirements",
        "missing_requirements",
        "risk_level",
        "evidence",
        "actionable_gaps",
    ]


def test_existing_file_upload_security_analyzer_remains_independent():
    plan = _plan(
        [
            _task(
                "task-resume-only",
                title="Resumable upload protocol",
                description=(
                    "Implement resumable upload sessions with chunk protocol, checksum validation, "
                    "retry behavior, partial cleanup, progress telemetry, and user recovery states."
                ),
                files_or_modules=["src/transfers/resumable_protocol.py"],
            )
        ]
    )

    resumability = build_task_large_file_upload_resumability_readiness_plan(plan)
    security = build_task_file_upload_security_readiness_plan(plan)

    assert resumability.upload_task_ids == ("task-resume-only",)
    assert security.upload_task_ids == ()
    assert security.records == ()


def _plan(tasks, plan_id="plan-large-upload"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-large-upload",
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
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
