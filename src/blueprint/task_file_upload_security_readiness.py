"""Plan file upload security readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


UploadSurface = Literal[
    "avatar_upload",
    "document_attachment",
    "bulk_import",
    "media_upload",
    "signed_upload_url",
    "user_generated_files",
]
UploadSafeguard = Literal[
    "file_type_allowlist",
    "size_limit",
    "malware_scan",
    "storage_acl",
    "signed_url_expiry",
    "content_moderation",
    "audit_logging",
]
UploadSecurityRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: dict[UploadSurface, int] = {
    "avatar_upload": 0,
    "document_attachment": 1,
    "bulk_import": 2,
    "media_upload": 3,
    "signed_upload_url": 4,
    "user_generated_files": 5,
}
_SAFEGUARD_ORDER: tuple[UploadSafeguard, ...] = (
    "file_type_allowlist",
    "size_limit",
    "malware_scan",
    "storage_acl",
    "signed_url_expiry",
    "content_moderation",
    "audit_logging",
)
_RISK_ORDER: dict[UploadSecurityRisk, int] = {"high": 0, "medium": 1, "low": 2}
_PATH_SURFACE_PATTERNS: tuple[tuple[UploadSurface, re.Pattern[str]], ...] = (
    ("avatar_upload", re.compile(r"(?:^|/)(?:avatars?|profile[-_]?photos?|profile[-_]?images?)(?:/|$)", re.I)),
    (
        "document_attachment",
        re.compile(r"(?:^|/)(?:attachments?|documents?|docs?|supporting[-_]?documents?)(?:/|$)", re.I),
    ),
    ("bulk_import", re.compile(r"(?:^|/)(?:imports?|bulk[-_]?imports?|csv|spreadsheets?)(?:/|$)", re.I)),
    ("media_upload", re.compile(r"(?:^|/)(?:media|images?|videos?|audio|photos?|gallery)(?:/|$)", re.I)),
    (
        "signed_upload_url",
        re.compile(r"(?:^|/)(?:signed[-_]?uploads?|presigned|pre[-_]?signed|direct[-_]?uploads?)(?:/|$)", re.I),
    ),
    ("user_generated_files", re.compile(r"(?:^|/)(?:uploads?|user[-_]?files?|ugc|files?)(?:/|$)", re.I)),
)
_TEXT_SURFACE_PATTERNS: dict[UploadSurface, re.Pattern[str]] = {
    "avatar_upload": re.compile(
        r"\b(?:avatar|profile photo|profile image|user photo|headshot)\b[^.\n;]*\b(?:upload|attach|image|file)\b|"
        r"\b(?:upload|change|update)\b[^.\n;]*\b(?:avatar|profile photo|profile image|user photo|headshot)\b",
        re.I,
    ),
    "document_attachment": re.compile(
        r"\b(?:document attachments?|file attachments?|attached files?|supporting documents?|"
        r"attach documents?|attach files?|claim documents?|invoice pdfs?)\b",
        re.I,
    ),
    "bulk_import": re.compile(
        r"\b(?:bulk import|csv import|spreadsheet import|data import|file import|import files?|"
        r"ingest files?|batch upload|bulk upload)\b",
        re.I,
    ),
    "media_upload": re.compile(
        r"\b(?:media upload|image upload|video upload|audio upload|photo upload|upload images?|"
        r"upload videos?|upload media|gallery upload|multipart image)\b",
        re.I,
    ),
    "signed_upload_url": re.compile(
        r"\b(?:signed upload urls?|pre[- ]signed upload urls?|presigned upload urls?|signed urls? for uploads?|"
        r"direct upload urls?|temporary upload urls?|time[- ]limited upload links?)\b",
        re.I,
    ),
    "user_generated_files": re.compile(
        r"\b(?:user[- ]generated files?|user uploads?|uploaded files?|upload flow|file upload|uploader|"
        r"dropzone|file picker|object storage uploads?|store uploaded files?)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[UploadSafeguard, re.Pattern[str]] = {
    "file_type_allowlist": re.compile(
        r"\b(?:file type allowlist|allowlisted file types?|allowed file types?|mime allowlist|mime types?|"
        r"content type validation|extension allowlist|allowed extensions?|reject unsupported types?)\b",
        re.I,
    ),
    "size_limit": re.compile(
        r"\b(?:size limits?|file size limit|max(?:imum)? upload size|max(?:imum)? file size|payload limit|"
        r"\d+\s*(?:kb|mb|gb)\s*(?:limit|max|maximum)|up to \d+\s*(?:kb|mb|gb))\b",
        re.I,
    ),
    "malware_scan": re.compile(
        r"\b(?:malware scan(?:ning)?|virus scan(?:ning)?|antivirus|anti-virus|clamav|quarantine|"
        r"infected files?|safe browsing scan)\b",
        re.I,
    ),
    "storage_acl": re.compile(
        r"\b(?:storage acl|file acl|bucket acl|bucket permissions?|private bucket|storage permissions?|"
        r"least privilege|tenant isolation|object access control|authorized users? only|private object storage)\b",
        re.I,
    ),
    "signed_url_expiry": re.compile(
        r"\b(?:signed url expir(?:y|ation)|url expir(?:y|ation)|expires? after|expiry window|ttl|time[- ]to[- ]live|"
        r"time[- ]limited urls?|short[- ]lived signed urls?|presigned url expir(?:y|ation))\b",
        re.I,
    ),
    "content_moderation": re.compile(
        r"\b(?:content moderation|moderate content|moderation queue|unsafe image|nsfw|abuse review|"
        r"policy review|user content review|media moderation)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit logging|audit log|audit trail|upload logs?|log upload events?|access logs?|"
        r"security logging|trace upload activity)\b",
        re.I,
    ),
}
_RECOMMENDED_STEPS: dict[UploadSafeguard, str] = {
    "file_type_allowlist": "Define server-side MIME and extension allowlists with unsafe type rejection.",
    "size_limit": "Set upload size limits across client, API, parser, and storage boundaries.",
    "malware_scan": "Add malware scanning, quarantine behavior, and scan-failure handling before files are trusted.",
    "storage_acl": "Confirm storage ACLs, tenant isolation, and least-privilege access for uploaded objects.",
    "signed_url_expiry": "Constrain signed upload URLs with short expiry, scoped permissions, and revocation expectations.",
    "content_moderation": "Plan moderation or abuse review for user-visible generated media and files.",
    "audit_logging": "Log upload, access, rejection, scan, and deletion events for investigation and compliance.",
}
_BASE_REQUIRED_SAFEGUARDS: tuple[UploadSafeguard, ...] = (
    "file_type_allowlist",
    "size_limit",
    "malware_scan",
    "storage_acl",
    "audit_logging",
)
_SURFACE_REQUIRED_SAFEGUARDS: dict[UploadSurface, tuple[UploadSafeguard, ...]] = {
    "avatar_upload": ("content_moderation",),
    "document_attachment": (),
    "bulk_import": (),
    "media_upload": ("content_moderation",),
    "signed_upload_url": ("signed_url_expiry",),
    "user_generated_files": ("content_moderation",),
}


@dataclass(frozen=True, slots=True)
class TaskFileUploadSecurityReadinessRecord:
    """Security readiness guidance for one task touching upload flows."""

    task_id: str
    title: str
    upload_surfaces: tuple[UploadSurface, ...]
    present_safeguards: tuple[UploadSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[UploadSafeguard, ...] = field(default_factory=tuple)
    risk_level: UploadSecurityRisk = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_readiness_steps: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "upload_surfaces": list(self.upload_surfaces),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_readiness_steps": list(self.recommended_readiness_steps),
        }


@dataclass(frozen=True, slots=True)
class TaskFileUploadSecurityReadinessPlan:
    """Plan-level file upload security readiness review."""

    plan_id: str | None = None
    records: tuple[TaskFileUploadSecurityReadinessRecord, ...] = field(default_factory=tuple)
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
        """Render file upload security readiness as deterministic Markdown."""
        title = "# Task File Upload Security Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Upload task count: {self.summary.get('upload_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No file upload security readiness records were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Surfaces | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.upload_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_file_upload_security_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSecurityReadinessPlan:
    """Build security readiness records for tasks that touch file upload flows."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    upload_task_ids = tuple(record.task_id for record in records)
    ignored_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskFileUploadSecurityReadinessPlan(
        plan_id=plan_id,
        records=records,
        upload_task_ids=upload_task_ids,
        ignored_task_ids=ignored_task_ids,
        summary=_summary(records, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def analyze_task_file_upload_security_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSecurityReadinessPlan:
    """Compatibility alias for building file upload security readiness plans."""
    return build_task_file_upload_security_readiness_plan(source)


def summarize_task_file_upload_security_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSecurityReadinessPlan:
    """Compatibility alias for building file upload security readiness plans."""
    return build_task_file_upload_security_readiness_plan(source)


def extract_task_file_upload_security_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSecurityReadinessPlan:
    """Compatibility alias for building file upload security readiness plans."""
    return build_task_file_upload_security_readiness_plan(source)


def generate_task_file_upload_security_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSecurityReadinessPlan:
    """Compatibility alias for generating file upload security readiness plans."""
    return build_task_file_upload_security_readiness_plan(source)


def recommend_task_file_upload_security_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskFileUploadSecurityReadinessPlan:
    """Compatibility alias for recommending file upload security readiness plans."""
    return build_task_file_upload_security_readiness_plan(source)


def task_file_upload_security_readiness_plan_to_dict(
    result: TaskFileUploadSecurityReadinessPlan,
) -> dict[str, Any]:
    """Serialize a file upload security readiness plan to a plain dictionary."""
    return result.to_dict()


task_file_upload_security_readiness_plan_to_dict.__test__ = False


def task_file_upload_security_readiness_plan_to_markdown(
    result: TaskFileUploadSecurityReadinessPlan,
) -> str:
    """Render a file upload security readiness plan as Markdown."""
    return result.to_markdown()


task_file_upload_security_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[UploadSurface, ...] = field(default_factory=tuple)
    surface_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[UploadSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskFileUploadSecurityReadinessRecord | None:
    signals = _signals(task)
    if not signals.surfaces:
        return None

    required = _required_safeguards(signals.surfaces)
    missing = tuple(safeguard for safeguard in required if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskFileUploadSecurityReadinessRecord(
        task_id=task_id,
        title=title,
        upload_surfaces=signals.surfaces,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals.surfaces, signals.present_safeguards, missing),
        evidence=tuple(_dedupe([*signals.surface_evidence, *signals.safeguard_evidence])),
        recommended_readiness_steps=tuple(_RECOMMENDED_STEPS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surface_hits: set[UploadSurface] = set()
    safeguard_hits: set[UploadSafeguard] = set()
    surface_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_surfaces = _path_surfaces(normalized)
        if path_surfaces:
            surface_hits.update(path_surfaces)
            surface_evidence.append(f"files_or_modules: {path}")
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(searchable) or pattern.search(normalized):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_surface = False
        for surface, pattern in _TEXT_SURFACE_PATTERNS.items():
            if pattern.search(text):
                surface_hits.add(surface)
                matched_surface = True
        if matched_surface:
            surface_evidence.append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    surfaces = tuple(surface for surface in _SURFACE_ORDER if surface in surface_hits)
    return _Signals(
        surfaces=surfaces,
        surface_evidence=tuple(_dedupe(surface_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_surfaces(path: str) -> set[UploadSurface]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    surfaces: set[UploadSurface] = set()
    for surface, pattern in _PATH_SURFACE_PATTERNS:
        if pattern.search(normalized) or pattern.search(text):
            surfaces.add(surface)
    name = PurePosixPath(normalized).name
    if re.search(r"\bupload(?:er|s|ing)?\b", text) or name in {"upload.py", "uploads.py", "uploader.ts", "uploader.tsx"}:
        surfaces.add("user_generated_files")
    if re.search(r"\bpresigned\b|\bsigned upload\b|\bdirect upload\b", text):
        surfaces.add("signed_upload_url")
    if re.search(r"\bavatar\b|\bprofile photo\b|\bprofile image\b", text):
        surfaces.add("avatar_upload")
    return surfaces


def _required_safeguards(surfaces: tuple[UploadSurface, ...]) -> tuple[UploadSafeguard, ...]:
    required: list[UploadSafeguard] = list(_BASE_REQUIRED_SAFEGUARDS)
    for surface in surfaces:
        required.extend(_SURFACE_REQUIRED_SAFEGUARDS[surface])
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in set(required))


def _risk_level(
    surfaces: tuple[UploadSurface, ...],
    present: tuple[UploadSafeguard, ...],
    missing: tuple[UploadSafeguard, ...],
) -> UploadSecurityRisk:
    if not missing:
        return "low"
    present_set = set(present)
    missing_set = set(missing)
    surface_set = set(surfaces)
    high_impact_surface = bool(surface_set & {"bulk_import", "media_upload", "signed_upload_url", "user_generated_files"})
    if {"file_type_allowlist", "size_limit", "malware_scan", "storage_acl"} & missing_set and high_impact_surface:
        return "high"
    if "signed_upload_url" in surface_set and "signed_url_expiry" in missing_set:
        return "high"
    if "media_upload" in surface_set and "content_moderation" in missing_set:
        return "high"
    if len(missing) >= 5:
        return "high"
    if {"file_type_allowlist", "size_limit", "storage_acl"} <= present_set:
        return "medium"
    return "medium"


def _summary(
    records: tuple[TaskFileUploadSecurityReadinessRecord, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "upload_task_count": len(records),
        "ignored_task_ids": list(ignored_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "surface_counts": {
            surface: sum(1 for record in records if surface in record.upload_surfaces)
            for surface in sorted({surface for record in records for surface in record.upload_surfaces})
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
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
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
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


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
    return any(pattern.search(value) for pattern in [*_TEXT_SURFACE_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


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
    "TaskFileUploadSecurityReadinessPlan",
    "TaskFileUploadSecurityReadinessRecord",
    "UploadSafeguard",
    "UploadSecurityRisk",
    "UploadSurface",
    "analyze_task_file_upload_security_readiness",
    "build_task_file_upload_security_readiness_plan",
    "extract_task_file_upload_security_readiness",
    "generate_task_file_upload_security_readiness",
    "recommend_task_file_upload_security_readiness",
    "summarize_task_file_upload_security_readiness",
    "task_file_upload_security_readiness_plan_to_dict",
    "task_file_upload_security_readiness_plan_to_markdown",
]
