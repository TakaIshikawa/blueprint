"""Plan file upload safety readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


UploadVector = Literal[
    "file_upload",
    "attachment",
    "document_upload",
    "csv_ingestion",
    "bulk_import",
    "media_upload",
    "user_supplied_file",
]
UploadSafetySafeguard = Literal[
    "file_type_validation",
    "size_limit",
    "malware_scanning",
    "storage_isolation",
    "storage_cleanup",
    "failure_handling",
]
UploadSafetyReadinessLevel = Literal["missing", "partial", "ready", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[UploadSafetyReadinessLevel, int] = {
    "missing": 0,
    "partial": 1,
    "ready": 2,
    "strong": 3,
}
_VECTOR_ORDER: dict[UploadVector, int] = {
    "file_upload": 0,
    "attachment": 1,
    "document_upload": 2,
    "csv_ingestion": 3,
    "bulk_import": 4,
    "media_upload": 5,
    "user_supplied_file": 6,
}
_SAFEGUARD_ORDER: tuple[UploadSafetySafeguard, ...] = (
    "file_type_validation",
    "size_limit",
    "malware_scanning",
    "storage_isolation",
    "storage_cleanup",
    "failure_handling",
)
_PATH_VECTOR_PATTERNS: tuple[tuple[UploadVector, re.Pattern[str]], ...] = (
    ("file_upload", re.compile(r"(?:^|/)(?:uploads?|uploaders?|dropzone|file[-_]?picker)(?:/|\.|_|-|$)", re.I)),
    ("attachment", re.compile(r"(?:^|/)(?:attachments?|attached[-_]?files?)(?:/|\.|_|-|$)", re.I)),
    ("document_upload", re.compile(r"(?:^|/)(?:documents?|docs?|pdfs?|claims?)(?:/|\.|_|-|$)", re.I)),
    ("csv_ingestion", re.compile(r"(?:^|/)(?:csv|spreadsheets?|sheets?|ingest(?:ion)?)(?:/|\.|_|-|$)", re.I)),
    ("bulk_import", re.compile(r"(?:^|/)(?:imports?|bulk[-_]?imports?|importers?)(?:/|\.|_|-|$)", re.I)),
    ("media_upload", re.compile(r"(?:^|/)(?:media|images?|videos?|audio|photos?|gallery)(?:/|\.|_|-|$)", re.I)),
    ("user_supplied_file", re.compile(r"(?:^|/)(?:user[-_]?files?|user[-_]?uploads?|ugc|files?)(?:/|\.|_|-|$)", re.I)),
)
_TEXT_VECTOR_PATTERNS: dict[UploadVector, re.Pattern[str]] = {
    "file_upload": re.compile(
        r"\b(?:file upload|upload files?|uploaded files?|upload flow|uploader|dropzone|file picker|"
        r"multipart upload|direct upload|presigned upload)\b",
        re.I,
    ),
    "attachment": re.compile(
        r"\b(?:attachments?|attached files?|file attachments?|attach files?|attach documents?)\b",
        re.I,
    ),
    "document_upload": re.compile(
        r"\b(?:document uploads?|upload documents?|supporting documents?|invoice pdfs?|claim documents?|pdf upload)\b",
        re.I,
    ),
    "csv_ingestion": re.compile(
        r"\b(?:csv ingestion|csv import|ingest csv|spreadsheet import|parse csv|uploaded csv|csv files?)\b",
        re.I,
    ),
    "bulk_import": re.compile(
        r"\b(?:bulk import|batch import|data import|file import|import files?|bulk upload|import uploaded files?)\b",
        re.I,
    ),
    "media_upload": re.compile(
        r"\b(?:media upload|image upload|video upload|audio upload|photo upload|upload images?|upload videos?|gallery upload)\b",
        re.I,
    ),
    "user_supplied_file": re.compile(
        r"\b(?:user[- ]supplied (?:files?|content|media|images?|videos?)|"
        r"user[- ]provided (?:files?|content|media|images?|videos?)|user[- ]generated files?|user uploads?|"
        r"customer files?|external files?|untrusted files?)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[UploadSafetySafeguard, re.Pattern[str]] = {
    "file_type_validation": re.compile(
        r"\b(?:file type validation|validate file types?|file type allowlist|allowed file types?|"
        r"mime validation|mime allowlist|content type validation|extension allowlist|reject unsupported types?)\b",
        re.I,
    ),
    "size_limit": re.compile(
        r"\b(?:size limits?|file size limit|max(?:imum)? upload size|max(?:imum)? file size|payload limit|"
        r"\d+\s*(?:kb|mb|gb)\s*(?:limit|max|maximum)|up to \d+\s*(?:kb|mb|gb))\b",
        re.I,
    ),
    "malware_scanning": re.compile(
        r"\b(?:malware scan(?:ning)?|virus scan(?:ning)?|antivirus|anti-virus|clamav|quarantine|"
        r"infected files?|scan before processing)\b",
        re.I,
    ),
    "storage_isolation": re.compile(
        r"\b(?:storage isolation|isolated storage|private bucket|tenant isolation|object isolation|"
        r"storage acl|bucket permissions?|least privilege|quarantine bucket|separate bucket)\b",
        re.I,
    ),
    "storage_cleanup": re.compile(
        r"\b(?:storage cleanup|cleanup uploaded files?|delete uploaded files?|remove temporary files?|"
        r"orphan cleanup|orphaned files?|ttl|time[- ]to[- ]live|retention cleanup|purge uploads?|lifecycle rule)\b",
        re.I,
    ),
    "failure_handling": re.compile(
        r"\b(?:failure handling|scan failure|upload failure|parser failure|partial upload|failed uploads?|"
        r"retry|dead[- ]letter|dlq|error handling|rollback|cleanup on failure)\b",
        re.I,
    ),
}
_RECOMMENDED_ACTIONS: dict[UploadSafetySafeguard, str] = {
    "file_type_validation": "Add server-side MIME, content-type, and extension validation with explicit unsupported-type rejection.",
    "size_limit": "Set upload size limits at client, API, parser, and storage boundaries.",
    "malware_scanning": "Add antivirus or malware scanning before files are trusted, parsed, or exposed.",
    "storage_isolation": "Store uploaded files in isolated private storage with tenant-aware access controls.",
    "storage_cleanup": "Define cleanup for temporary, rejected, orphaned, expired, and partially processed files.",
    "failure_handling": "Specify retry, quarantine, user feedback, and cleanup behavior for upload, scan, parser, and storage failures.",
}


@dataclass(frozen=True, slots=True)
class TaskFileUploadSafetyRecord:
    """Safety readiness guidance for one task touching upload or file-ingestion flows."""

    task_id: str
    title: str
    upload_vectors: tuple[UploadVector, ...]
    required_safeguards: tuple[UploadSafetySafeguard, ...] = field(default_factory=tuple)
    present_safeguards: tuple[UploadSafetySafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[UploadSafetySafeguard, ...] = field(default_factory=tuple)
    readiness_level: UploadSafetyReadinessLevel = "missing"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_follow_up_actions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "upload_vectors": list(self.upload_vectors),
            "required_safeguards": list(self.required_safeguards),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness_level": self.readiness_level,
            "evidence": list(self.evidence),
            "recommended_follow_up_actions": list(self.recommended_follow_up_actions),
        }


@dataclass(frozen=True, slots=True)
class TaskFileUploadSafetyPlan:
    """Plan-level file upload safety readiness review."""

    plan_id: str | None = None
    records: tuple[TaskFileUploadSafetyRecord, ...] = field(default_factory=tuple)
    upload_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "upload_task_ids": list(self.upload_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render file upload safety readiness as deterministic Markdown."""
        title = "# Task File Upload Safety"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Upload task count: {self.summary.get('upload_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No file upload safety records were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Upload Vectors | Missing Safeguards | Recommended Follow-up Actions | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell(', '.join(record.upload_vectors) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_follow_up_actions) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_file_upload_safety_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSafetyPlan:
    """Build safety readiness records for tasks that touch file upload flows."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_READINESS_ORDER[record.readiness_level], record.task_id, record.title.casefold()),
        )
    )
    ignored_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskFileUploadSafetyPlan(
        plan_id=plan_id,
        records=records,
        upload_task_ids=tuple(record.task_id for record in records),
        ignored_task_ids=ignored_task_ids,
        summary=_summary(records, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def analyze_task_file_upload_safety(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSafetyPlan:
    """Compatibility alias for building file upload safety plans."""
    return build_task_file_upload_safety_plan(source)


def summarize_task_file_upload_safety(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSafetyPlan:
    """Compatibility alias for building file upload safety plans."""
    return build_task_file_upload_safety_plan(source)


def extract_task_file_upload_safety(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSafetyPlan:
    """Compatibility alias for building file upload safety plans."""
    return build_task_file_upload_safety_plan(source)


def generate_task_file_upload_safety(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSafetyPlan:
    """Compatibility alias for generating file upload safety plans."""
    return build_task_file_upload_safety_plan(source)


def recommend_task_file_upload_safety(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSafetyPlan:
    """Compatibility alias for recommending file upload safety plans."""
    return build_task_file_upload_safety_plan(source)


def task_file_upload_safety_plan_to_dict(result: TaskFileUploadSafetyPlan) -> dict[str, Any]:
    """Serialize a file upload safety plan to a plain dictionary."""
    return result.to_dict()


task_file_upload_safety_plan_to_dict.__test__ = False


def task_file_upload_safety_plan_to_markdown(result: TaskFileUploadSafetyPlan) -> str:
    """Render a file upload safety plan as Markdown."""
    return result.to_markdown()


task_file_upload_safety_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    vectors: tuple[UploadVector, ...] = field(default_factory=tuple)
    vector_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[UploadSafetySafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskFileUploadSafetyRecord | None:
    signals = _signals(task)
    if not signals.vectors:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskFileUploadSafetyRecord(
        task_id=task_id,
        title=title,
        upload_vectors=signals.vectors,
        required_safeguards=_SAFEGUARD_ORDER,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        readiness_level=_readiness_level(signals.present_safeguards, missing),
        evidence=tuple(_dedupe([*signals.vector_evidence, *signals.safeguard_evidence])),
        recommended_follow_up_actions=tuple(_RECOMMENDED_ACTIONS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    vector_hits: set[UploadVector] = set()
    safeguard_hits: set[UploadSafetySafeguard] = set()
    vector_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_vectors = _path_vectors(normalized)
        if path_vectors:
            vector_hits.update(path_vectors)
            vector_evidence.append(f"files_or_modules: {path}")
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(searchable) or pattern.search(normalized):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_vector = False
        for vector, pattern in _TEXT_VECTOR_PATTERNS.items():
            if pattern.search(text):
                vector_hits.add(vector)
                matched_vector = True
        if matched_vector:
            vector_evidence.append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    return _Signals(
        vectors=tuple(vector for vector in _VECTOR_ORDER if vector in vector_hits),
        vector_evidence=tuple(_dedupe(vector_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_vectors(path: str) -> set[UploadVector]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    vectors: set[UploadVector] = set()
    for vector, pattern in _PATH_VECTOR_PATTERNS:
        if pattern.search(normalized) or pattern.search(text):
            vectors.add(vector)
    name = PurePosixPath(normalized).name
    if re.search(r"\bupload(?:er|s|ing)?\b", text) or name in {"upload.py", "uploads.py", "uploader.ts", "uploader.tsx"}:
        vectors.add("file_upload")
    if re.search(r"\bcsv\b|\bspreadsheet\b", text):
        vectors.add("csv_ingestion")
    if re.search(r"\buser supplied\b|\buser provided\b|\buntrusted\b|\bugc\b", text):
        vectors.add("user_supplied_file")
    return vectors


def _readiness_level(
    present: tuple[UploadSafetySafeguard, ...],
    missing: tuple[UploadSafetySafeguard, ...],
) -> UploadSafetyReadinessLevel:
    if not missing:
        if "storage_cleanup" in present and "failure_handling" in present:
            return "strong"
        return "ready"
    if present:
        return "partial"
    return "missing"


def _summary(
    records: tuple[TaskFileUploadSafetyRecord, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "upload_task_count": len(records),
        "ignored_task_ids": list(ignored_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            level: sum(1 for record in records if record.readiness_level == level)
            for level in _READINESS_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "vector_counts": {
            vector: sum(1 for record in records if vector in record.upload_vectors)
            for vector in sorted({vector for record in records for vector in record.upload_vectors})
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
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
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
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


def _metadata_key_is_signal(value: str) -> bool:
    return any(pattern.search(value) for pattern in [*_TEXT_VECTOR_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


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


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


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


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


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


__all__ = [
    "TaskFileUploadSafetyPlan",
    "TaskFileUploadSafetyRecord",
    "UploadSafetyReadinessLevel",
    "UploadSafetySafeguard",
    "UploadVector",
    "analyze_task_file_upload_safety",
    "build_task_file_upload_safety_plan",
    "extract_task_file_upload_safety",
    "generate_task_file_upload_safety",
    "recommend_task_file_upload_safety",
    "summarize_task_file_upload_safety",
    "task_file_upload_safety_plan_to_dict",
    "task_file_upload_safety_plan_to_markdown",
]
