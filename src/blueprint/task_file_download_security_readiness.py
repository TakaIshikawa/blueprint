"""Plan secure file download readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DownloadSurface = Literal[
    "signed_download_url",
    "document_download",
    "export_download",
    "media_delivery",
    "private_attachment",
    "bulk_archive_download",
]
DownloadSafeguard = Literal[
    "authorization_check",
    "short_lived_signed_url",
    "storage_acl",
    "cache_control",
    "audit_logging",
    "content_disposition_safety",
    "malware_quarantine_check",
    "revocation_behavior",
]
DownloadReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: tuple[DownloadSurface, ...] = (
    "signed_download_url",
    "document_download",
    "export_download",
    "media_delivery",
    "private_attachment",
    "bulk_archive_download",
)
_SAFEGUARD_ORDER: tuple[DownloadSafeguard, ...] = (
    "authorization_check",
    "short_lived_signed_url",
    "storage_acl",
    "cache_control",
    "audit_logging",
    "content_disposition_safety",
    "malware_quarantine_check",
    "revocation_behavior",
)
_READINESS_ORDER: dict[DownloadReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_PATH_SURFACE_PATTERNS: tuple[tuple[DownloadSurface, re.Pattern[str]], ...] = (
    (
        "signed_download_url",
        re.compile(r"(?:signed[-_]?downloads?|presigned|pre[-_]?signed|temporary[-_]?links?)(?:/|$|[-_])", re.I),
    ),
    ("document_download", re.compile(r"(?:^|/)(?:documents?|docs?|pdfs?|statements?|invoices?)(?:/|$)", re.I)),
    ("export_download", re.compile(r"(?:^|/)(?:exports?|reports?|csv|spreadsheets?)(?:/|$)", re.I)),
    ("media_delivery", re.compile(r"(?:^|/)(?:media|images?|videos?|audio|photos?|thumbnails?|cdn)(?:/|$)", re.I)),
    ("private_attachment", re.compile(r"(?:^|/)(?:attachments?|private[-_]?files?|user[-_]?files?)(?:/|$)", re.I)),
    (
        "bulk_archive_download",
        re.compile(r"(?:^|/)(?:archives?|bulk[-_]?downloads?|zip|zips|bundles?|tarballs?)(?:/|$)", re.I),
    ),
)
_TEXT_SURFACE_PATTERNS: dict[DownloadSurface, re.Pattern[str]] = {
    "signed_download_url": re.compile(
        r"\b(?:signed download urls?|pre[- ]signed download urls?|presigned download urls?|"
        r"signed urls? for downloads?|temporary download links?|time[- ]limited download links?)\b",
        re.I,
    ),
    "document_download": re.compile(
        r"\b(?:document downloads?|download documents?|download pdfs?|download invoices?|"
        r"statement downloads?|claim documents?|customer documents?)\b",
        re.I,
    ),
    "export_download": re.compile(
        r"\b(?:export downloads?|download exports?|csv export|spreadsheet export|report export|"
        r"download reports?|data export|exported files?)\b",
        re.I,
    ),
    "media_delivery": re.compile(
        r"\b(?:media delivery|download images?|download videos?|stream videos?|serve media|"
        r"deliver media|cdn delivery|private media|image download|video download)\b",
        re.I,
    ),
    "private_attachment": re.compile(
        r"\b(?:private attachments?|download attachments?|attached files?|private files?|"
        r"secure attachments?|user file downloads?|file downloads?|download files?)\b",
        re.I,
    ),
    "bulk_archive_download": re.compile(
        r"\b(?:bulk archive downloads?|bulk downloads?|zip downloads?|archive downloads?|"
        r"download zip|download archive|download all files?|bundle downloads?)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[DownloadSafeguard, re.Pattern[str]] = {
    "authorization_check": re.compile(
        r"\b(?:authorization checks?|authori[sz]e downloads?|permission checks?|access checks?|"
        r"entitlement checks?|owner checks?|tenant access|rbac|acl check|deny unauthorized)\b",
        re.I,
    ),
    "short_lived_signed_url": re.compile(
        r"\b(?:short[- ]lived signed urls?|signed url expir(?:y|ation)|expires? after|expiry window|"
        r"ttl|time[- ]to[- ]live|time[- ]limited urls?|presigned url expir(?:y|ation))\b",
        re.I,
    ),
    "storage_acl": re.compile(
        r"\b(?:storage acl|file acl|bucket acl|bucket permissions?|private bucket|storage permissions?|"
        r"least privilege|tenant isolation|object access control|private object storage)\b",
        re.I,
    ),
    "cache_control": re.compile(
        r"\b(?:cache[- ]control|no[- ]store|private cache|no[- ]cache|cdn cache policy|"
        r"browser cache|cache headers?|surrogate control)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit logging|audit log|audit trail|download logs?|access logs?|log download events?|"
        r"security logging|trace download activity)\b",
        re.I,
    ),
    "content_disposition_safety": re.compile(
        r"\b(?:content[- ]disposition|safe filenames?|filename sanit(?:y|ization)|sanitize filenames?|"
        r"attachment filename|download filename|header injection|mime sniffing|x-content-type-options)\b",
        re.I,
    ),
    "malware_quarantine_check": re.compile(
        r"\b(?:malware scan(?:ning)?|virus scan(?:ning)?|antivirus|anti-virus|quarantine|"
        r"quarantine state|infected files?|scan status|only clean files?)\b",
        re.I,
    ),
    "revocation_behavior": re.compile(
        r"\b(?:revocation behavior|revoke download|revoked downloads?|invalidate links?|link revocation|"
        r"revoke signed urls?|delete invalidates?|permission changes? invalidate|access revoked)\b",
        re.I,
    ),
}
_RECOMMENDED_STEPS: dict[DownloadSafeguard, str] = {
    "authorization_check": "Verify every download request enforces user, tenant, owner, and entitlement authorization.",
    "short_lived_signed_url": "Constrain signed download URLs with short expiry, scoped object permissions, and one-purpose links.",
    "storage_acl": "Confirm storage ACLs keep source objects private and isolated from direct public access.",
    "cache_control": "Set Cache-Control and CDN policies that prevent private downloads from being stored in shared caches.",
    "audit_logging": "Log download creation, access, denial, revocation, and storage lookup events for investigation.",
    "content_disposition_safety": "Sanitize filenames and Content-Disposition headers to prevent injection, spoofing, and unsafe rendering.",
    "malware_quarantine_check": "Block downloads for files that are unscanned, quarantined, infected, or pending security review.",
    "revocation_behavior": "Define how deleted files, permission changes, and compromised links revoke existing download access.",
}
_BASE_REQUIRED_SAFEGUARDS: tuple[DownloadSafeguard, ...] = (
    "authorization_check",
    "storage_acl",
    "cache_control",
    "audit_logging",
    "content_disposition_safety",
)
_SURFACE_REQUIRED_SAFEGUARDS: dict[DownloadSurface, tuple[DownloadSafeguard, ...]] = {
    "signed_download_url": ("short_lived_signed_url", "revocation_behavior"),
    "document_download": ("malware_quarantine_check",),
    "export_download": (),
    "media_delivery": (),
    "private_attachment": ("malware_quarantine_check", "revocation_behavior"),
    "bulk_archive_download": ("malware_quarantine_check", "revocation_behavior"),
}


@dataclass(frozen=True, slots=True)
class TaskFileDownloadSecurityReadinessRecord:
    """Security readiness guidance for one task touching download flows."""

    task_id: str
    title: str
    download_surfaces: tuple[DownloadSurface, ...]
    present_safeguards: tuple[DownloadSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[DownloadSafeguard, ...] = field(default_factory=tuple)
    readiness: DownloadReadiness = "weak"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_readiness_steps: tuple[str, ...] = field(default_factory=tuple)

    @property
    def detected_signals(self) -> tuple[DownloadSurface, ...]:
        """Compatibility view for planners that name surfaces detected signals."""
        return self.download_surfaces

    @property
    def matched_signals(self) -> tuple[DownloadSurface, ...]:
        """Compatibility view for planners that name surfaces matched signals."""
        return self.download_surfaces

    @property
    def recommendations(self) -> tuple[str, ...]:
        """Compatibility view for planners that name readiness steps recommendations."""
        return self.recommended_readiness_steps

    @property
    def recommended_actions(self) -> tuple[str, ...]:
        """Compatibility view for planners that name readiness steps recommended actions."""
        return self.recommended_readiness_steps

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "download_surfaces": list(self.download_surfaces),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "recommended_readiness_steps": list(self.recommended_readiness_steps),
        }


@dataclass(frozen=True, slots=True)
class TaskFileDownloadSecurityReadinessPlan:
    """Plan-level file download security readiness review."""

    plan_id: str | None = None
    records: tuple[TaskFileDownloadSecurityReadinessRecord, ...] = field(default_factory=tuple)
    download_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskFileDownloadSecurityReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskFileDownloadSecurityReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    @property
    def impacted_task_ids(self) -> tuple[str, ...]:
        """Compatibility view matching planners that expose impacted task ids."""
        return self.download_task_ids

    @property
    def ignored_task_ids(self) -> tuple[str, ...]:
        """Compatibility view matching planners that expose ignored task ids."""
        return self.no_signal_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "download_task_ids": list(self.download_task_ids),
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render file download security readiness as deterministic Markdown."""
        title = "# Task File Download Security Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Download task count: {self.summary.get('download_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task file download security readiness records were inferred."])
            if self.no_signal_task_ids:
                lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Surfaces | Present Safeguards | Missing Safeguards | Recommendations | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.download_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_readiness_steps) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_signal_task_ids:
            lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
        return "\n".join(lines)


def build_task_file_download_security_readiness_plan(source: Any) -> TaskFileDownloadSecurityReadinessPlan:
    """Build security readiness records for tasks that touch file download flows."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                -len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    download_task_ids = tuple(record.task_id for record in records)
    download_task_id_set = set(download_task_ids)
    no_signal_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in download_task_id_set
    )
    return TaskFileDownloadSecurityReadinessPlan(
        plan_id=plan_id,
        records=records,
        download_task_ids=download_task_ids,
        no_signal_task_ids=no_signal_task_ids,
        summary=_summary(records, task_count=len(tasks), no_signal_task_ids=no_signal_task_ids),
    )


def analyze_task_file_download_security_readiness(source: Any) -> TaskFileDownloadSecurityReadinessPlan:
    """Compatibility alias for building file download security readiness plans."""
    return build_task_file_download_security_readiness_plan(source)


def summarize_task_file_download_security_readiness(source: Any) -> TaskFileDownloadSecurityReadinessPlan:
    """Compatibility alias for building file download security readiness plans."""
    return build_task_file_download_security_readiness_plan(source)


def extract_task_file_download_security_readiness(source: Any) -> TaskFileDownloadSecurityReadinessPlan:
    """Compatibility alias for building file download security readiness plans."""
    return build_task_file_download_security_readiness_plan(source)


def generate_task_file_download_security_readiness(source: Any) -> TaskFileDownloadSecurityReadinessPlan:
    """Compatibility alias for generating file download security readiness plans."""
    return build_task_file_download_security_readiness_plan(source)


def recommend_task_file_download_security_readiness(source: Any) -> TaskFileDownloadSecurityReadinessPlan:
    """Compatibility alias for recommending file download security readiness plans."""
    return build_task_file_download_security_readiness_plan(source)


def derive_task_file_download_security_readiness(source: Any) -> TaskFileDownloadSecurityReadinessPlan:
    """Compatibility alias for deriving file download security readiness plans."""
    return build_task_file_download_security_readiness_plan(source)


def task_file_download_security_readiness_plan_to_dict(
    result: TaskFileDownloadSecurityReadinessPlan,
) -> dict[str, Any]:
    """Serialize a file download security readiness plan to a plain dictionary."""
    return result.to_dict()


task_file_download_security_readiness_plan_to_dict.__test__ = False


def task_file_download_security_readiness_plan_to_dicts(
    result: TaskFileDownloadSecurityReadinessPlan | Iterable[TaskFileDownloadSecurityReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize file download security readiness records to plain dictionaries."""
    if isinstance(result, TaskFileDownloadSecurityReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_file_download_security_readiness_plan_to_dicts.__test__ = False
task_file_download_security_readiness_to_dicts = task_file_download_security_readiness_plan_to_dicts
task_file_download_security_readiness_to_dicts.__test__ = False


def task_file_download_security_readiness_plan_to_markdown(
    result: TaskFileDownloadSecurityReadinessPlan,
) -> str:
    """Render a file download security readiness plan as Markdown."""
    return result.to_markdown()


task_file_download_security_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[DownloadSurface, ...] = field(default_factory=tuple)
    present_safeguards: tuple[DownloadSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskFileDownloadSecurityReadinessRecord | None:
    signals = _signals(task)
    if not signals.surfaces:
        return None

    required = _required_safeguards(signals.surfaces)
    missing = tuple(safeguard for safeguard in required if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    return TaskFileDownloadSecurityReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        download_surfaces=signals.surfaces,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        readiness=_readiness(signals.present_safeguards, missing),
        evidence=signals.evidence,
        recommended_readiness_steps=tuple(_RECOMMENDED_STEPS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surface_hits: set[DownloadSurface] = set()
    safeguard_hits: set[DownloadSafeguard] = set()
    evidence: list[str] = []

    for path in _strings(
        task.get("files_or_modules")
        or task.get("files")
        or task.get("expected_file_paths")
        or task.get("expected_files")
        or task.get("paths")
    ):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        path_surfaces = _path_surfaces(normalized)
        if path_surfaces:
            surface_hits.update(path_surfaces)
            matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for surface, pattern in _TEXT_SURFACE_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                surface_hits.add(surface)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        surfaces=tuple(surface for surface in _SURFACE_ORDER if surface in surface_hits),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        evidence=tuple(_dedupe(evidence)),
    )


def _path_surfaces(path: str) -> set[DownloadSurface]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    surfaces: set[DownloadSurface] = set()
    for surface, pattern in _PATH_SURFACE_PATTERNS:
        if pattern.search(normalized) or pattern.search(text):
            surfaces.add(surface)
    name = PurePosixPath(normalized).name
    if re.search(r"\bdownload(?:er|s|ing)?\b", text) or name in {"download.py", "downloads.py", "download.ts"}:
        if re.search(r"\b(?:document|doc|pdf|invoice|statement)\b", text):
            surfaces.add("document_download")
        elif re.search(r"\b(?:export|report|csv|spreadsheet)\b", text):
            surfaces.add("export_download")
        elif re.search(r"\b(?:media|image|video|audio|cdn)\b", text):
            surfaces.add("media_delivery")
        elif re.search(r"\b(?:attachment|private|user file)\b", text):
            surfaces.add("private_attachment")
        elif re.search(r"\bfiles?\b", text):
            surfaces.add("private_attachment")
    if re.search(r"\bpresigned\b|\bsigned download\b|\btemporary link\b", text):
        surfaces.add("signed_download_url")
    if re.search(r"\b(?:archive|bulk download|zip|bundle)\b", text):
        surfaces.add("bulk_archive_download")
    return surfaces


def _required_safeguards(surfaces: tuple[DownloadSurface, ...]) -> tuple[DownloadSafeguard, ...]:
    required: list[DownloadSafeguard] = list(_BASE_REQUIRED_SAFEGUARDS)
    for surface in surfaces:
        required.extend(_SURFACE_REQUIRED_SAFEGUARDS[surface])
    required_set = set(required)
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required_set)


def _readiness(
    present: tuple[DownloadSafeguard, ...],
    missing: tuple[DownloadSafeguard, ...],
) -> DownloadReadiness:
    if not missing:
        return "strong"
    if len(present) >= 3:
        return "partial"
    return "weak"


def _summary(
    records: tuple[TaskFileDownloadSecurityReadinessRecord, ...],
    *,
    task_count: int,
    no_signal_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "download_task_count": len(records),
        "download_task_ids": [record.task_id for record in records],
        "impacted_task_ids": [record.task_id for record in records],
        "no_signal_task_ids": list(no_signal_task_ids),
        "surface_count": sum(len(record.download_surfaces) for record in records),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "surface_counts": {
            surface: sum(1 for record in records if surface in record.download_surfaces)
            for surface in _SURFACE_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
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
        iterator = iter(source)
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
        "expected_file_paths",
        "expected_files",
        "paths",
        "acceptance_criteria",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
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
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
        "validation_command",
        "validation_commands",
        "test_commands",
    ):
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
    path = value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
    return str(PurePosixPath(path)) if path else ""


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
    "DownloadReadiness",
    "DownloadSafeguard",
    "DownloadSurface",
    "TaskFileDownloadSecurityReadinessPlan",
    "TaskFileDownloadSecurityReadinessRecord",
    "analyze_task_file_download_security_readiness",
    "build_task_file_download_security_readiness_plan",
    "derive_task_file_download_security_readiness",
    "extract_task_file_download_security_readiness",
    "generate_task_file_download_security_readiness",
    "recommend_task_file_download_security_readiness",
    "summarize_task_file_download_security_readiness",
    "task_file_download_security_readiness_plan_to_dict",
    "task_file_download_security_readiness_plan_to_dicts",
    "task_file_download_security_readiness_plan_to_markdown",
    "task_file_download_security_readiness_to_dicts",
]
