import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_media_processing_safety_readiness import (
    TaskMediaProcessingSafetyReadinessPlan,
    TaskMediaProcessingSafetyReadinessRecord,
    build_task_media_processing_safety_readiness,
    derive_task_media_processing_safety_readiness,
    generate_task_media_processing_safety_readiness,
    summarize_task_media_processing_safety_readiness,
    task_media_processing_safety_readiness_to_dict,
    task_media_processing_safety_readiness_to_dicts,
    task_media_processing_safety_readiness_to_markdown,
)


def test_media_processing_with_missing_controls_is_blocked():
    result = build_task_media_processing_safety_readiness(
        _plan(
            [
                _task(
                    "task-video-transcode",
                    title="Transcode uploaded videos",
                    description="Use ffmpeg to transcode uploaded videos and generate poster frame thumbnails.",
                    acceptance_criteria=[
                        "Background worker creates MP4 renditions and thumbnail previews.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert isinstance(record, TaskMediaProcessingSafetyReadinessRecord)
    assert record.media_operations == (
        "video_processing",
        "thumbnail_generation",
        "transcoding",
    )
    assert record.readiness_level == "blocked"
    assert record.required_controls == (
        "sandboxing",
        "size_duration_limits",
        "codec_allowlist",
        "metadata_privacy",
        "queue_failure_handling",
        "fallback_ux",
    )
    assert record.present_controls == ()
    assert record.missing_controls == record.required_controls
    assert any("Block implementation until" in action for action in record.recommended_actions)
    assert result.summary["blocked_task_count"] == 1
    assert result.summary["missing_control_count"] == 6


def test_ready_image_metadata_stripping_detects_all_required_controls():
    result = build_task_media_processing_safety_readiness(
        _plan(
            [
                _task(
                    "task-image-ready",
                    title="Image thumbnail and EXIF stripping",
                    description=(
                        "Process images in an isolated sandbox container. "
                        "Enforce max file size, pixel limit, and worker timeout. "
                        "Validate MIME, magic bytes, and supported codecs before decode. "
                        "Strip EXIF, drop GPS metadata, and sanitize metadata before thumbnail output. "
                        "Show processing state, placeholder, retry button, and user-visible error on failure."
                    ),
                )
            ]
        )
    )

    record = result.records[0]
    assert record.media_operations == (
        "image_processing",
        "thumbnail_generation",
        "transcoding",
        "metadata_stripping",
    )
    assert record.present_controls == (
        "sandboxing",
        "size_duration_limits",
        "codec_allowlist",
        "metadata_privacy",
        "queue_failure_handling",
        "fallback_ux",
    )
    assert record.missing_controls == ()
    assert record.readiness_level == "ready"
    assert record.recommended_actions == (
        "Ready to implement after preserving the documented media processing controls.",
    )
    assert result.summary["ready_task_count"] == 1


def test_metadata_and_path_hints_detect_scanning_async_audio_and_metadata_extraction():
    result = build_task_media_processing_safety_readiness(
        _plan(
            [
                _task(
                    "task-media-metadata",
                    title="Media ingestion safety pipeline",
                    files_or_modules=[
                        "src/workers/audio_waveform_pipeline.py",
                        "src/security/media_virus_scanner.py",
                        "src/media/metadata_extract.py",
                    ],
                    metadata={
                        "media_processing": {
                            "queue": "Worker queue uses retries, idempotent jobs, backoff, and a dead-letter queue.",
                            "limits": "Max file size and duration limit are enforced before processing.",
                            "isolation": "Sandboxed worker has no network access.",
                            "fallback": "Failed state and placeholder are visible to the user.",
                        }
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.media_operations == (
        "audio_processing",
        "metadata_extraction",
        "malware_scanning",
        "async_media_pipeline",
    )
    assert record.present_controls == (
        "sandboxing",
        "size_duration_limits",
        "queue_failure_handling",
        "fallback_ux",
    )
    assert record.missing_controls == ("codec_allowlist", "metadata_privacy")
    assert record.readiness_level == "needs_controls"
    assert any("files_or_modules[0]" in item for item in record.evidence)
    assert any("metadata.media_processing.queue" in item for item in record.evidence)


def test_attachment_only_and_empty_inputs_return_stable_empty_plan_without_mutation():
    plan = _plan(
        [
            _task(
                "task-attachments",
                title="Add support attachments",
                description="Allow users to upload and download generic attachments with signed URLs.",
                files_or_modules=["src/attachments/upload_controller.py"],
            ),
            _task(
                "task-negated",
                title="File storage API",
                description="No media processing, thumbnails, transcoding, EXIF, or virus scan work is in scope.",
            ),
        ],
        plan_id="plan-ignore",
    )
    original = copy.deepcopy(plan)

    result = build_task_media_processing_safety_readiness(plan)

    assert plan == original
    assert isinstance(result, TaskMediaProcessingSafetyReadinessPlan)
    assert result.records == ()
    assert result.findings == ()
    assert result.ignored_task_ids == ("task-attachments", "task-negated")
    assert result.to_dict() == {
        "plan_id": "plan-ignore",
        "summary": {
            "total_task_count": 2,
            "media_processing_task_count": 0,
            "ignored_task_count": 2,
            "ready_task_count": 0,
            "needs_controls_task_count": 0,
            "blocked_task_count": 0,
            "missing_control_count": 0,
            "operation_counts": {
                "image_processing": 0,
                "video_processing": 0,
                "audio_processing": 0,
                "thumbnail_generation": 0,
                "transcoding": 0,
                "metadata_extraction": 0,
                "metadata_stripping": 0,
                "malware_scanning": 0,
                "async_media_pipeline": 0,
            },
            "missing_control_counts": {
                "sandboxing": 0,
                "size_duration_limits": 0,
                "codec_allowlist": 0,
                "metadata_privacy": 0,
                "queue_failure_handling": 0,
                "fallback_ux": 0,
            },
        },
        "records": [],
        "ignored_task_ids": ["task-attachments", "task-negated"],
    }
    assert result.to_markdown() == (
        "# Task Media Processing Safety Readiness Plan: plan-ignore\n\n"
        "## Summary\n\n"
        "- Total tasks: 2\n"
        "- Media processing tasks: 0\n"
        "- Ignored tasks: 2\n"
        "- Ready tasks: 0\n"
        "- Tasks needing controls: 0\n"
        "- Blocked tasks: 0\n"
        "- Missing controls: 0\n\n"
        "No media processing safety readiness records were inferred."
    )
    assert (
        build_task_media_processing_safety_readiness({"tasks": "not a list"}).ignored_task_ids == ()
    )
    assert build_task_media_processing_safety_readiness(None).summary["total_task_count"] == 0


def test_deterministic_ranking_serialization_aliases_markdown_and_model_input():
    plan = _plan(
        [
            _task(
                "task-ready",
                title="Audio waveform | preview",
                description=(
                    "Audio processing runs in a sandbox with max file size, duration limit, supported codecs, "
                    "metadata privacy sanitization, and placeholder fallback."
                ),
            ),
            _task(
                "task-blocked",
                title="Thumbnail generation",
                description="Generate thumbnails for uploaded images.",
            ),
        ],
        plan_id="plan-model",
    )
    model = ExecutionPlan.model_validate(plan)

    result = build_task_media_processing_safety_readiness(model)
    derived = derive_task_media_processing_safety_readiness(result)
    summarized = summarize_task_media_processing_safety_readiness(plan)
    records = generate_task_media_processing_safety_readiness(model)
    payload = task_media_processing_safety_readiness_to_dict(result)
    markdown = task_media_processing_safety_readiness_to_markdown(result)

    assert [record.task_id for record in result.records] == ["task-blocked", "task-ready"]
    assert derived is result
    assert summarized.to_dict() == result.to_dict()
    assert records == result.records
    assert result.to_dicts() == payload["records"]
    assert task_media_processing_safety_readiness_to_dicts(result) == payload["records"]
    assert task_media_processing_safety_readiness_to_dicts(records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "records", "ignored_task_ids"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "media_operations",
        "required_controls",
        "present_controls",
        "missing_controls",
        "readiness_level",
        "evidence",
        "recommended_actions",
    ]
    assert payload["records"][0]["readiness_level"] == "blocked"
    assert "Audio waveform \\| preview" in markdown
    assert markdown == result.to_markdown()


def _plan(tasks, *, plan_id="plan-media"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-media",
        "milestones": [{"name": "Launch"}],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
    risks=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if risks is not None:
        task["risks"] = risks
    return task
