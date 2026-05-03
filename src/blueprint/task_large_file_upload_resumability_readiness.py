"""Plan large file upload resumability readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


LargeUploadResumabilitySignal = Literal[
    "large_file_upload",
    "resumable_upload",
    "chunked_upload",
    "multipart_upload",
    "direct_object_upload",
]
LargeUploadResumabilityRequirement = Literal[
    "chunk_protocol",
    "session_lifecycle",
    "integrity_validation",
    "retry_behavior",
    "partial_cleanup",
    "quota_enforcement",
    "security_scanning_handoff",
    "progress_telemetry",
    "user_recovery_states",
]
LargeUploadResumabilityRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[LargeUploadResumabilitySignal, ...] = (
    "large_file_upload",
    "resumable_upload",
    "chunked_upload",
    "multipart_upload",
    "direct_object_upload",
)
_REQUIREMENT_ORDER: tuple[LargeUploadResumabilityRequirement, ...] = (
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
_RISK_ORDER: dict[LargeUploadResumabilityRisk, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_PATTERNS: dict[LargeUploadResumabilitySignal, re.Pattern[str]] = {
    "large_file_upload": re.compile(
        r"\b(?:large|huge|big|multi[- ]?gb|gigabyte|video|archive|dataset|bulk).{0,80}"
        r"(?:file )?uploads?\b|\buploads?[^.\n;]{0,80}(?:large files?|multi[- ]?gb|gigabyte|huge files?)\b",
        re.I,
    ),
    "resumable_upload": re.compile(
        r"\b(?:resumable uploads?|resume uploads?|resume interrupted uploads?|upload resumption|"
        r"continue interrupted uploads?|tus protocol)\b",
        re.I,
    ),
    "chunked_upload": re.compile(
        r"\b(?:chunked uploads?|upload chunks?|chunk uploads?|chunking|chunk size|parts? upload|"
        r"split files? into chunks?)\b",
        re.I,
    ),
    "multipart_upload": re.compile(r"\b(?:multipart uploads?|s3 multipart|complete multipart|upload parts?)\b", re.I),
    "direct_object_upload": re.compile(
        r"\b(?:direct uploads?|browser direct uploads?|object storage uploads?|presigned uploads?|"
        r"pre[- ]signed upload urls?|signed upload urls?|blob storage uploads?)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[LargeUploadResumabilitySignal, re.Pattern[str]] = {
    "large_file_upload": re.compile(r"large[_-]?file|big[_-]?file|video[_-]?upload|bulk[_-]?upload", re.I),
    "resumable_upload": re.compile(r"resum(?:able|e)[_-]?uploads?|tus", re.I),
    "chunked_upload": re.compile(r"chunk(?:ed|s|ing)?[_-]?uploads?|upload[_-]?chunks?", re.I),
    "multipart_upload": re.compile(r"multipart[_-]?uploads?|s3[_-]?multipart", re.I),
    "direct_object_upload": re.compile(r"presigned|signed[_-]?uploads?|direct[_-]?uploads?|object[_-]?uploads?", re.I),
}
_REQUIREMENT_PATTERNS: dict[LargeUploadResumabilityRequirement, re.Pattern[str]] = {
    "chunk_protocol": re.compile(
        r"\b(?:chunk protocol|chunk contract|chunk size|part size|chunk order|part order|byte ranges?|"
        r"content-range|upload offset|tus offset|multipart part numbers?|complete multipart)\b",
        re.I,
    ),
    "session_lifecycle": re.compile(
        r"\b(?:upload sessions?|session lifecycle|initiate upload|create upload session|complete upload|"
        r"commit upload|abort upload|expire upload sessions?|session ttl|upload id|idempotent session)\b",
        re.I,
    ),
    "integrity_validation": re.compile(
        r"\b(?:checksum|sha-?256|sha-?1|md5|etag|crc32c|digest|hash validation|integrity validation|"
        r"verify parts?|validate chunks?|mismatched chunks?)\b",
        re.I,
    ),
    "retry_behavior": re.compile(
        r"\b(?:retry(?:ing|ies)?|retry behavior|retry policy|resume after failure|network interruption|"
        r"connection drop|backoff|idempotent retry|reupload failed chunks?|recover failed parts?)\b",
        re.I,
    ),
    "partial_cleanup": re.compile(
        r"\b(?:partial cleanup|cleanup partial|orphaned chunks?|orphaned parts?|abandoned uploads?|"
        r"incomplete uploads?|stale upload sessions?|garbage collect|gc partial|abort multipart)\b",
        re.I,
    ),
    "quota_enforcement": re.compile(
        r"\b(?:quota|storage quota|upload quota|tenant quota|user quota|size limit|file size limit|"
        r"remaining storage|max(?:imum)? upload size|enforce limits?)\b",
        re.I,
    ),
    "security_scanning_handoff": re.compile(
        r"\b(?:malware scan(?:ning)?|virus scan(?:ning)?|security scan(?:ning)?|antivirus|quarantine|"
        r"scan handoff|handoff to scanning|scan after assembly|scan completed upload)\b",
        re.I,
    ),
    "progress_telemetry": re.compile(
        r"\b(?:progress telemetry|upload progress|progress events?|progress bar|bytes uploaded|percent complete|"
        r"upload metrics?|upload logs?|upload traces?|progress websocket|progress polling)\b",
        re.I,
    ),
    "user_recovery_states": re.compile(
        r"\b(?:user recovery|recovery states?|resume button|retry button|paused state|failed state|"
        r"recover interrupted|continue upload|cancel upload|user-facing error|restart upload)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:large file|resumable|chunked|multipart|upload)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_ACTIONABLE_GAPS: dict[LargeUploadResumabilityRequirement, str] = {
    "chunk_protocol": "Define chunk or multipart protocol details: part size, ordering, offsets, completion, and idempotency.",
    "session_lifecycle": "Specify upload session creation, persistence, expiry, completion, abort, and ownership rules.",
    "integrity_validation": "Add checksum, digest, ETag, or per-part integrity validation before assembled files are trusted.",
    "retry_behavior": "Describe bounded retry and resume behavior for failed chunks, network drops, and duplicate submissions.",
    "partial_cleanup": "Plan cleanup for orphaned chunks, stale sessions, abandoned multipart uploads, and failed assemblies.",
    "quota_enforcement": "Enforce file, user, tenant, and storage quotas before and during resumable uploads.",
    "security_scanning_handoff": "Define handoff from completed upload assembly to malware/security scanning and quarantine states.",
    "progress_telemetry": "Emit progress events, metrics, logs, and traces for upload progress, failures, and completion.",
    "user_recovery_states": "Define user-facing paused, failed, resumable, retryable, cancelled, and completed recovery states.",
}


@dataclass(frozen=True, slots=True)
class TaskLargeFileUploadResumabilityReadinessFinding:
    """Large file upload resumability readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[LargeUploadResumabilitySignal, ...] = field(default_factory=tuple)
    present_requirements: tuple[LargeUploadResumabilityRequirement, ...] = field(default_factory=tuple)
    missing_requirements: tuple[LargeUploadResumabilityRequirement, ...] = field(default_factory=tuple)
    risk_level: LargeUploadResumabilityRisk = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actionable_gaps: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_requirements": list(self.present_requirements),
            "missing_requirements": list(self.missing_requirements),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "actionable_gaps": list(self.actionable_gaps),
        }


@dataclass(frozen=True, slots=True)
class TaskLargeFileUploadResumabilityReadinessPlan:
    """Plan-level large file upload resumability readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskLargeFileUploadResumabilityReadinessFinding, ...] = field(default_factory=tuple)
    upload_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskLargeFileUploadResumabilityReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [record.to_dict() for record in self.findings],
            "upload_task_ids": list(self.upload_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness findings as plain dictionaries."""
        return [record.to_dict() for record in self.findings]


def build_task_large_file_upload_resumability_readiness_plan(
    source: Any,
) -> TaskLargeFileUploadResumabilityReadinessPlan:
    """Build large file upload resumability readiness findings for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_finding_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    findings = tuple(
        sorted(
            (finding for finding in candidates if finding is not None),
            key=lambda finding: (_RISK_ORDER[finding.risk_level], finding.task_id, finding.title.casefold()),
        )
    )
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskLargeFileUploadResumabilityReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        upload_task_ids=tuple(finding.task_id for finding in findings),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_large_file_upload_resumability_readiness(source: Any) -> TaskLargeFileUploadResumabilityReadinessPlan:
    """Compatibility alias for building large file upload resumability readiness plans."""
    return build_task_large_file_upload_resumability_readiness_plan(source)


def summarize_task_large_file_upload_resumability_readiness(source: Any) -> TaskLargeFileUploadResumabilityReadinessPlan:
    """Compatibility alias for building large file upload resumability readiness plans."""
    return build_task_large_file_upload_resumability_readiness_plan(source)


def extract_task_large_file_upload_resumability_readiness(source: Any) -> TaskLargeFileUploadResumabilityReadinessPlan:
    """Compatibility alias for extracting large file upload resumability readiness plans."""
    return build_task_large_file_upload_resumability_readiness_plan(source)


def generate_task_large_file_upload_resumability_readiness(source: Any) -> TaskLargeFileUploadResumabilityReadinessPlan:
    """Compatibility alias for generating large file upload resumability readiness plans."""
    return build_task_large_file_upload_resumability_readiness_plan(source)


def task_large_file_upload_resumability_readiness_plan_to_dict(
    result: TaskLargeFileUploadResumabilityReadinessPlan,
) -> dict[str, Any]:
    """Serialize a large file upload resumability readiness plan to a plain dictionary."""
    return result.to_dict()


task_large_file_upload_resumability_readiness_plan_to_dict.__test__ = False


def task_large_file_upload_resumability_readiness_plan_to_dicts(
    result: TaskLargeFileUploadResumabilityReadinessPlan | Iterable[TaskLargeFileUploadResumabilityReadinessFinding],
) -> list[dict[str, Any]]:
    """Serialize large file upload resumability readiness findings to plain dictionaries."""
    if isinstance(result, TaskLargeFileUploadResumabilityReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_large_file_upload_resumability_readiness_plan_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[LargeUploadResumabilitySignal, ...] = field(default_factory=tuple)
    requirements: tuple[LargeUploadResumabilityRequirement, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskLargeFileUploadResumabilityReadinessFinding | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not _is_large_upload_task(signals.signals):
        return None

    missing = tuple(requirement for requirement in _REQUIREMENT_ORDER if requirement not in signals.requirements)
    return TaskLargeFileUploadResumabilityReadinessFinding(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        detected_signals=signals.signals,
        present_requirements=signals.requirements,
        missing_requirements=missing,
        risk_level=_risk_level(signals.signals, missing),
        evidence=signals.evidence,
        actionable_gaps=tuple(_ACTIONABLE_GAPS[requirement] for requirement in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[LargeUploadResumabilitySignal] = set()
    requirement_hits: set[LargeUploadResumabilityRequirement] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        matched = False
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        matched = False
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for requirement, pattern in _REQUIREMENT_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                requirement_hits.add(requirement)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        requirements=tuple(requirement for requirement in _REQUIREMENT_ORDER if requirement in requirement_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _is_large_upload_task(signals: tuple[LargeUploadResumabilitySignal, ...]) -> bool:
    signal_set = set(signals)
    return "resumable_upload" in signal_set or "chunked_upload" in signal_set or "multipart_upload" in signal_set or (
        "large_file_upload" in signal_set and bool(signal_set & {"direct_object_upload", "chunked_upload"})
    )


def _risk_level(
    signals: tuple[LargeUploadResumabilitySignal, ...],
    missing: tuple[LargeUploadResumabilityRequirement, ...],
) -> LargeUploadResumabilityRisk:
    if not missing:
        return "low"
    missing_set = set(missing)
    if len(missing) >= 6:
        return "high"
    if {"chunk_protocol", "session_lifecycle", "integrity_validation", "retry_behavior"} & missing_set and len(missing) >= 4:
        return "high"
    if ("multipart_upload" in signals or "resumable_upload" in signals) and {"session_lifecycle", "partial_cleanup"} <= missing_set:
        return "high"
    return "medium"


def _summary(
    findings: tuple[TaskLargeFileUploadResumabilityReadinessFinding, ...],
    *,
    total_task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "upload_task_count": len(findings),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_requirement_count": sum(len(finding.missing_requirements) for finding in findings),
        "risk_counts": {risk: sum(1 for finding in findings if finding.risk_level == risk) for risk in _RISK_ORDER},
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_requirement_counts": {
            requirement: sum(1 for finding in findings if requirement in finding.present_requirements)
            for requirement in _REQUIREMENT_ORDER
        },
        "missing_requirement_counts": {
            requirement: sum(1 for finding in findings if requirement in finding.missing_requirements)
            for requirement in _REQUIREMENT_ORDER
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


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
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
        "validation_command",
        "validation_commands",
        "test_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))
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
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_REQUIREMENT_PATTERNS.values()])


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
    return str(PurePosixPath(value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")))


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


__all__ = [
    "LargeUploadResumabilityRequirement",
    "LargeUploadResumabilityRisk",
    "LargeUploadResumabilitySignal",
    "TaskLargeFileUploadResumabilityReadinessFinding",
    "TaskLargeFileUploadResumabilityReadinessPlan",
    "analyze_task_large_file_upload_resumability_readiness",
    "build_task_large_file_upload_resumability_readiness_plan",
    "extract_task_large_file_upload_resumability_readiness",
    "generate_task_large_file_upload_resumability_readiness",
    "summarize_task_large_file_upload_resumability_readiness",
    "task_large_file_upload_resumability_readiness_plan_to_dict",
    "task_large_file_upload_resumability_readiness_plan_to_dicts",
]
