"""Assess task-level media processing safety readiness for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


MediaProcessingOperation = Literal[
    "image_processing",
    "video_processing",
    "audio_processing",
    "thumbnail_generation",
    "transcoding",
    "metadata_extraction",
    "metadata_stripping",
    "malware_scanning",
    "async_media_pipeline",
]
MediaProcessingControl = Literal[
    "sandboxing",
    "size_duration_limits",
    "codec_allowlist",
    "metadata_privacy",
    "queue_failure_handling",
    "fallback_ux",
]
MediaProcessingReadinessLevel = Literal["ready", "needs_controls", "blocked"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_OPERATION_ORDER: tuple[MediaProcessingOperation, ...] = (
    "image_processing",
    "video_processing",
    "audio_processing",
    "thumbnail_generation",
    "transcoding",
    "metadata_extraction",
    "metadata_stripping",
    "malware_scanning",
    "async_media_pipeline",
)
_CONTROL_ORDER: tuple[MediaProcessingControl, ...] = (
    "sandboxing",
    "size_duration_limits",
    "codec_allowlist",
    "metadata_privacy",
    "queue_failure_handling",
    "fallback_ux",
)
_MEDIA_PROCESSING_RE = re.compile(
    r"\b(?:image|photo|picture|video|audio|media|thumbnail|poster frame|waveform|codec|"
    r"transcod(?:e|es|ed|ing)|encode|encoding|decode|decoding|ffmpeg|imagemagick|"
    r"sharp|pillow|exif|metadata extraction|extract metadata|metadata stripping|strip metadata|"
    r"virus scan|malware scan|clamav|media pipeline|processing pipeline|render preview)\b",
    re.I,
)
_ATTACHMENT_ONLY_RE = re.compile(
    r"\b(?:upload|uploads|download|downloads|attach|attachment|attachments|file picker|"
    r"file upload|file download|storage|presigned url|signed url)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,100}"
    r"\b(?:media processing|thumbnail|transcoding|metadata extraction|exif|virus scan|malware scan|"
    r"image processing|video processing|audio processing)\b|"
    r"\b(?:media processing|thumbnail|transcoding|metadata extraction|exif|virus scan|malware scan|"
    r"image processing|video processing|audio processing)\b"
    r".{0,100}\b(?:out of scope|not required|not needed|no change|unsupported)\b",
    re.I,
)
_OPERATION_PATTERNS: dict[MediaProcessingOperation, tuple[re.Pattern[str], ...]] = {
    "image_processing": (
        re.compile(
            r"\b(?:image processing|process images?|resize images?|crop images?|rotate images?|image preview|photo processing|imagemagick|sharp|pillow)\b",
            re.I,
        ),
    ),
    "video_processing": (
        re.compile(
            r"\b(?:video processing|process videos?|video preview|poster frame|extract frames?|video duration|ffmpeg)\b",
            re.I,
        ),
    ),
    "audio_processing": (
        re.compile(
            r"\b(?:audio processing|process audio|audio preview|waveform|audio duration|normalize audio|speech file)\b",
            re.I,
        ),
    ),
    "thumbnail_generation": (
        re.compile(
            r"\b(?:thumbnail|thumbs?|preview image|poster frame|generate previews?|rend(?:er|ers|ered|ering) preview)\b",
            re.I,
        ),
    ),
    "transcoding": (
        re.compile(
            r"\b(?:transcod(?:e|es|ed|ing)|encode|encoding|decode|decoding|convert videos?|convert audio|codec|ffmpeg)\b",
            re.I,
        ),
    ),
    "metadata_extraction": (
        re.compile(
            r"\b(?:metadata extraction|extract metadata|read metadata|parse metadata|exif read|exif extraction|duration probe|mediainfo)\b",
            re.I,
        ),
    ),
    "metadata_stripping": (
        re.compile(
            r"\b(?:metadata stripping|strip metadata|remove metadata|exif stripping|strip exif|remove exif|privacy scrub)\b",
            re.I,
        ),
    ),
    "malware_scanning": (
        re.compile(
            r"\b(?:virus scan|virus scanning|malware scan|malware scanning|clamav|av scan|quarantine infected)\b",
            re.I,
        ),
    ),
    "async_media_pipeline": (
        re.compile(
            r"\b(?:media pipeline|processing pipeline|async processing|background processing|worker queue|queue worker|job retry|dead[- ]?letter|dlq|webhook callback)\b",
            re.I,
        ),
    ),
}
_CONTROL_PATTERNS: dict[MediaProcessingControl, tuple[re.Pattern[str], ...]] = {
    "sandboxing": (
        re.compile(
            r"\b(?:sandbox|sandboxed|isolate|isolated|container|seccomp|chroot|unprivileged|no network|read[- ]?only filesystem|jail)\b",
            re.I,
        ),
    ),
    "size_duration_limits": (
        re.compile(
            r"\b(?:file size limit|max file size|size cap|mb limit|gb limit|duration limit|max duration|timeout|time limit|frame limit|pixel limit|resolution limit)\b",
            re.I,
        ),
    ),
    "codec_allowlist": (
        re.compile(
            r"\b(?:codec allowlist|codec whitelist|allowed codecs?|supported codecs?|mime allowlist|format allowlist|reject unsupported|magic byte|content type validation)\b",
            re.I,
        ),
    ),
    "metadata_privacy": (
        re.compile(
            r"\b(?:strip metadata|remove metadata|exif stripping|strip exif|remove exif|metadata privacy|privacy scrub|drop gps|gps metadata|sanitize metadata)\b",
            re.I,
        ),
    ),
    "queue_failure_handling": (
        re.compile(
            r"\b(?:retry|retries|dead[- ]?letter|dlq|poison message|idempotent|backoff|queue timeout|job timeout|worker timeout|failed jobs?|quarantine failed|queue isolation)\b",
            re.I,
        ),
    ),
    "fallback_ux": (
        re.compile(
            r"\b(?:fallback|placeholder|processing state|failed state|user[- ]?visible error|user visible error|retry button|manual retry|status message|graceful degradation)\b",
            re.I,
        ),
    ),
}
_REQUIRED_BY_OPERATION: dict[MediaProcessingOperation, tuple[MediaProcessingControl, ...]] = {
    "image_processing": (
        "sandboxing",
        "size_duration_limits",
        "codec_allowlist",
        "metadata_privacy",
        "fallback_ux",
    ),
    "video_processing": (
        "sandboxing",
        "size_duration_limits",
        "codec_allowlist",
        "metadata_privacy",
        "fallback_ux",
    ),
    "audio_processing": (
        "sandboxing",
        "size_duration_limits",
        "codec_allowlist",
        "metadata_privacy",
        "fallback_ux",
    ),
    "thumbnail_generation": (
        "sandboxing",
        "size_duration_limits",
        "codec_allowlist",
        "metadata_privacy",
        "fallback_ux",
    ),
    "transcoding": (
        "sandboxing",
        "size_duration_limits",
        "codec_allowlist",
        "queue_failure_handling",
        "fallback_ux",
    ),
    "metadata_extraction": (
        "sandboxing",
        "size_duration_limits",
        "codec_allowlist",
        "metadata_privacy",
        "fallback_ux",
    ),
    "metadata_stripping": (
        "sandboxing",
        "size_duration_limits",
        "codec_allowlist",
        "metadata_privacy",
        "fallback_ux",
    ),
    "malware_scanning": (
        "sandboxing",
        "size_duration_limits",
        "queue_failure_handling",
        "fallback_ux",
    ),
    "async_media_pipeline": (
        "sandboxing",
        "size_duration_limits",
        "queue_failure_handling",
        "fallback_ux",
    ),
}


@dataclass(frozen=True, slots=True)
class TaskMediaProcessingSafetyReadinessRecord:
    """Media-processing safety assessment for one execution task."""

    task_id: str
    title: str
    media_operations: tuple[MediaProcessingOperation, ...] = field(default_factory=tuple)
    required_controls: tuple[MediaProcessingControl, ...] = field(default_factory=tuple)
    present_controls: tuple[MediaProcessingControl, ...] = field(default_factory=tuple)
    missing_controls: tuple[MediaProcessingControl, ...] = field(default_factory=tuple)
    readiness_level: MediaProcessingReadinessLevel = "needs_controls"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_actions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "media_operations": list(self.media_operations),
            "required_controls": list(self.required_controls),
            "present_controls": list(self.present_controls),
            "missing_controls": list(self.missing_controls),
            "readiness_level": self.readiness_level,
            "evidence": list(self.evidence),
            "recommended_actions": list(self.recommended_actions),
        }


@dataclass(frozen=True, slots=True)
class TaskMediaProcessingSafetyReadinessPlan:
    """Plan-level task media processing safety readiness assessment."""

    plan_id: str | None = None
    records: tuple[TaskMediaProcessingSafetyReadinessRecord, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)

    @property
    def findings(self) -> tuple[TaskMediaProcessingSafetyReadinessRecord, ...]:
        """Compatibility view matching reports that expose records as findings."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "ignored_task_ids": list(self.ignored_task_ids),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return media processing safety records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the assessment as deterministic Markdown."""
        title = "# Task Media Processing Safety Readiness Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Total tasks: {self.summary.get('total_task_count', 0)}",
            f"- Media processing tasks: {self.summary.get('media_processing_task_count', 0)}",
            f"- Ignored tasks: {self.summary.get('ignored_task_count', 0)}",
            f"- Ready tasks: {self.summary.get('ready_task_count', 0)}",
            f"- Tasks needing controls: {self.summary.get('needs_controls_task_count', 0)}",
            f"- Blocked tasks: {self.summary.get('blocked_task_count', 0)}",
            f"- Missing controls: {self.summary.get('missing_control_count', 0)}",
        ]
        if not self.records:
            lines.extend(["", "No media processing safety readiness records were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Records",
                "",
                (
                    "| Task | Title | Operations | Required Controls | Present Controls | "
                    "Missing Controls | Readiness | Recommended Actions | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{_markdown_cell(record.title)} | "
                f"{_markdown_cell(', '.join(record.media_operations))} | "
                f"{_markdown_cell(', '.join(record.required_controls))} | "
                f"{_markdown_cell(', '.join(record.present_controls) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_controls) or 'none')} | "
                f"{_markdown_cell(record.readiness_level)} | "
                f"{_markdown_cell('; '.join(record.recommended_actions) or 'Ready to implement with stated controls.')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_media_processing_safety_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskMediaProcessingSafetyReadinessPlan:
    """Assess safety readiness for media-processing execution tasks."""
    plan_id, tasks = _source_payload(source)
    records: list[TaskMediaProcessingSafetyReadinessRecord] = []
    ignored_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        record = _record_for_task(task, index)
        if record is None:
            ignored_task_ids.append(task_id)
        else:
            records.append(record)
    ordered_records = tuple(
        sorted(
            records,
            key=lambda record: (
                _readiness_rank(record.readiness_level),
                len(record.missing_controls),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    ignored = tuple(sorted(_dedupe(ignored_task_ids), key=lambda value: value.casefold()))
    return TaskMediaProcessingSafetyReadinessPlan(
        plan_id=plan_id,
        records=ordered_records,
        summary=_summary(
            ordered_records, total_task_count=len(tasks), ignored_task_count=len(ignored)
        ),
        ignored_task_ids=ignored,
    )


def generate_task_media_processing_safety_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[TaskMediaProcessingSafetyReadinessRecord, ...]:
    """Return media processing safety readiness records for relevant tasks."""
    return build_task_media_processing_safety_readiness(source).records


def derive_task_media_processing_safety_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskMediaProcessingSafetyReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskMediaProcessingSafetyReadinessPlan:
    """Return an existing media processing plan or build one from a plan-shaped source."""
    if isinstance(source, TaskMediaProcessingSafetyReadinessPlan):
        return source
    return build_task_media_processing_safety_readiness(source)


def summarize_task_media_processing_safety_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskMediaProcessingSafetyReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskMediaProcessingSafetyReadinessPlan:
    """Compatibility alias for task media processing safety readiness summaries."""
    return derive_task_media_processing_safety_readiness(source)


def task_media_processing_safety_readiness_to_dict(
    plan: TaskMediaProcessingSafetyReadinessPlan,
) -> dict[str, Any]:
    """Serialize a task media processing safety readiness plan to a plain dictionary."""
    return plan.to_dict()


task_media_processing_safety_readiness_to_dict.__test__ = False


def task_media_processing_safety_readiness_to_dicts(
    records: (
        TaskMediaProcessingSafetyReadinessPlan
        | tuple[TaskMediaProcessingSafetyReadinessRecord, ...]
        | list[TaskMediaProcessingSafetyReadinessRecord]
    ),
) -> list[dict[str, Any]]:
    """Serialize task media processing safety readiness records to dictionaries."""
    if isinstance(records, TaskMediaProcessingSafetyReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


task_media_processing_safety_readiness_to_dicts.__test__ = False


def task_media_processing_safety_readiness_to_markdown(
    plan: TaskMediaProcessingSafetyReadinessPlan,
) -> str:
    """Render a task media processing safety readiness plan as Markdown."""
    return plan.to_markdown()


task_media_processing_safety_readiness_to_markdown.__test__ = False


def _record_for_task(
    task: Mapping[str, Any], index: int
) -> TaskMediaProcessingSafetyReadinessRecord | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    operations: dict[MediaProcessingOperation, list[str]] = {}
    controls: dict[MediaProcessingControl, list[str]] = {}
    evidence: list[str] = []
    has_processing_signal = False

    for source_field, text in _candidate_texts(task):
        if _NEGATED_SCOPE_RE.search(text) and not _explicit_structured_media_field(source_field):
            continue
        field_processing_signal = bool(
            _MEDIA_PROCESSING_RE.search(text) or _path_media_processing_signal(source_field, text)
        )
        if field_processing_signal:
            has_processing_signal = True
            evidence.append(_evidence_snippet(source_field, text))
        for operation, patterns in _OPERATION_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns) or _path_operation_match(
                operation, source_field, text
            ):
                snippet = _evidence_snippet(source_field, text)
                operations.setdefault(operation, []).append(snippet)
                evidence.append(snippet)
        for control, patterns in _CONTROL_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                snippet = _evidence_snippet(source_field, text)
                controls.setdefault(control, []).append(snippet)
                evidence.append(snippet)

    if not operations or not has_processing_signal:
        return None

    media_operations = tuple(operation for operation in _OPERATION_ORDER if operation in operations)
    required = _required_controls(media_operations)
    present = tuple(control for control in _CONTROL_ORDER if control in controls)
    missing = tuple(control for control in required if control not in present)
    readiness = _readiness_level(media_operations, missing)
    return TaskMediaProcessingSafetyReadinessRecord(
        task_id=task_id,
        title=title,
        media_operations=media_operations,
        required_controls=required,
        present_controls=present,
        missing_controls=missing,
        readiness_level=readiness,
        evidence=tuple(_dedupe(evidence)),
        recommended_actions=_recommended_actions(missing, readiness),
    )


def _required_controls(
    operations: tuple[MediaProcessingOperation, ...],
) -> tuple[MediaProcessingControl, ...]:
    required: list[MediaProcessingControl] = []
    for operation in operations:
        required.extend(_REQUIRED_BY_OPERATION[operation])
    return tuple(control for control in _CONTROL_ORDER if control in set(required))


def _readiness_level(
    operations: tuple[MediaProcessingOperation, ...],
    missing: tuple[MediaProcessingControl, ...],
) -> MediaProcessingReadinessLevel:
    if not missing:
        return "ready"
    if "sandboxing" in missing or "size_duration_limits" in missing:
        return "blocked"
    if "transcoding" in operations and "codec_allowlist" in missing:
        return "blocked"
    if "async_media_pipeline" in operations and "queue_failure_handling" in missing:
        return "blocked"
    if len(missing) >= 3:
        return "blocked"
    return "needs_controls"


def _recommended_actions(
    missing: tuple[MediaProcessingControl, ...],
    readiness: MediaProcessingReadinessLevel,
) -> tuple[str, ...]:
    if not missing:
        return ("Ready to implement after preserving the documented media processing controls.",)
    actions = {
        "sandboxing": "Run media parsers, codecs, scanners, and converters inside a sandboxed or isolated worker.",
        "size_duration_limits": "Enforce file size, pixel, frame, duration, and processing timeout limits before processing.",
        "codec_allowlist": "Validate MIME, magic bytes, formats, and codecs against an allowlist before decoding.",
        "metadata_privacy": "Strip or sanitize EXIF, GPS, and other embedded metadata before user-visible output.",
        "queue_failure_handling": "Define queue isolation, retries, idempotency, timeouts, and dead-letter handling.",
        "fallback_ux": "Show user-visible processing, failure, retry, or placeholder states when media work cannot complete.",
    }
    prefix = "Block implementation until" if readiness == "blocked" else "Before implementation"
    return tuple(f"{prefix}: {actions[control]}" for control in missing)


def _summary(
    records: tuple[TaskMediaProcessingSafetyReadinessRecord, ...],
    *,
    total_task_count: int,
    ignored_task_count: int,
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "media_processing_task_count": len(records),
        "ignored_task_count": ignored_task_count,
        "ready_task_count": sum(1 for record in records if record.readiness_level == "ready"),
        "needs_controls_task_count": sum(
            1 for record in records if record.readiness_level == "needs_controls"
        ),
        "blocked_task_count": sum(1 for record in records if record.readiness_level == "blocked"),
        "missing_control_count": sum(len(record.missing_controls) for record in records),
        "operation_counts": {
            operation: sum(1 for record in records if operation in record.media_operations)
            for operation in _OPERATION_ORDER
        },
        "missing_control_counts": {
            control: sum(1 for record in records if control in record.missing_controls)
            for control in _CONTROL_ORDER
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "definition_of_done",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "modules",
        "tags",
        "labels",
        "notes",
        "risks",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        texts.extend(_metadata_texts(metadata))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            key_text = str(key).replace("_", " ")
            if _MEDIA_PROCESSING_RE.search(key_text) or any(
                pattern.search(key_text)
                for patterns in (*_OPERATION_PATTERNS.values(), *_CONTROL_PATTERNS.values())
                for pattern in patterns
            ):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _explicit_structured_media_field(source_field: str) -> bool:
    normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
    return any(
        token in normalized
        for token in (
            "media",
            "image",
            "video",
            "audio",
            "thumbnail",
            "transcoding",
            "metadata",
            "exif",
            "scanner",
            "scan",
            "pipeline",
        )
    )


def _path_media_processing_signal(source_field: str, text: str) -> bool:
    if not source_field.startswith(("files_or_modules", "files", "modules")):
        return False
    path = text.casefold().replace("-", "_")
    if _attachment_only_path(path):
        return False
    return any(
        token in path
        for token in (
            "media",
            "image",
            "video",
            "audio",
            "thumbnail",
            "transcode",
            "codec",
            "ffmpeg",
            "exif",
            "metadata",
            "scanner",
            "virus",
            "malware",
            "preview",
            "waveform",
            "pipeline",
        )
    )


def _attachment_only_path(path: str) -> bool:
    return any(
        token in path
        for token in (
            "attachment",
            "upload",
            "download",
            "storage",
            "file_picker",
            "filepicker",
            "presigned",
        )
    ) and not any(
        token in path
        for token in (
            "media",
            "thumbnail",
            "transcode",
            "codec",
            "ffmpeg",
            "exif",
            "metadata",
            "scanner",
            "virus",
            "malware",
            "preview",
            "waveform",
            "pipeline",
        )
    )


def _path_operation_match(
    operation: MediaProcessingOperation, source_field: str, text: str
) -> bool:
    if not _path_media_processing_signal(source_field, text):
        return False
    path = text.casefold().replace("-", "_")
    tokens = {
        "image_processing": ("image", "photo", "picture", "sharp", "pillow", "imagemagick"),
        "video_processing": ("video", "poster_frame"),
        "audio_processing": ("audio", "waveform"),
        "thumbnail_generation": ("thumbnail", "thumb", "preview", "poster_frame"),
        "transcoding": ("transcode", "codec", "ffmpeg", "encode", "decode"),
        "metadata_extraction": ("metadata_extract", "extract_metadata", "mediainfo"),
        "metadata_stripping": ("metadata_strip", "strip_metadata", "exif", "privacy_scrub"),
        "malware_scanning": ("virus", "malware", "scanner", "clamav"),
        "async_media_pipeline": ("pipeline", "queue", "worker"),
    }
    return any(token in path for token in tokens[operation])


def _readiness_rank(readiness: MediaProcessingReadinessLevel) -> int:
    return {"blocked": 0, "needs_controls": 1, "ready": 2}[readiness]


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


__all__ = [
    "MediaProcessingControl",
    "MediaProcessingOperation",
    "MediaProcessingReadinessLevel",
    "TaskMediaProcessingSafetyReadinessPlan",
    "TaskMediaProcessingSafetyReadinessRecord",
    "build_task_media_processing_safety_readiness",
    "derive_task_media_processing_safety_readiness",
    "generate_task_media_processing_safety_readiness",
    "summarize_task_media_processing_safety_readiness",
    "task_media_processing_safety_readiness_to_dict",
    "task_media_processing_safety_readiness_to_dicts",
    "task_media_processing_safety_readiness_to_markdown",
]
