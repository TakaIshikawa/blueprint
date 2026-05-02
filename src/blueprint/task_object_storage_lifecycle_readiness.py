"""Plan object storage lifecycle readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ObjectStorageSignal = Literal[
    "object_storage",
    "bucket",
    "blob_store",
    "upload_storage",
    "generated_files",
    "media_assets",
    "archive_path",
    "retention_sensitive",
    "destructive_lifecycle",
]
ObjectStorageLifecycleSafeguard = Literal[
    "retention_policy",
    "lifecycle_expiration",
    "access_tier_archive_behavior",
    "deletion_recovery",
    "encryption_owner_evidence",
    "cost_quota_monitoring",
]
ObjectStorageLifecycleRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: dict[ObjectStorageSignal, int] = {
    "object_storage": 0,
    "bucket": 1,
    "blob_store": 2,
    "upload_storage": 3,
    "generated_files": 4,
    "media_assets": 5,
    "archive_path": 6,
    "retention_sensitive": 7,
    "destructive_lifecycle": 8,
}
_SAFEGUARD_ORDER: tuple[ObjectStorageLifecycleSafeguard, ...] = (
    "retention_policy",
    "lifecycle_expiration",
    "access_tier_archive_behavior",
    "deletion_recovery",
    "encryption_owner_evidence",
    "cost_quota_monitoring",
)
_RISK_ORDER: dict[ObjectStorageLifecycleRisk, int] = {"high": 0, "medium": 1, "low": 2}
_PATH_SIGNAL_PATTERNS: tuple[tuple[ObjectStorageSignal, re.Pattern[str]], ...] = (
    ("bucket", re.compile(r"(?:^|/)(?:buckets?|s3|gcs|cloud[-_]?storage)(?:/|$)", re.I)),
    ("blob_store", re.compile(r"(?:^|/)(?:blobs?|blob[-_]?store|azure[-_]?blob)(?:/|$)", re.I)),
    ("upload_storage", re.compile(r"(?:^|/)(?:uploads?|uploaded[-_]?files?|user[-_]?files?|files?)(?:/|$)", re.I)),
    ("generated_files", re.compile(r"(?:^|/)(?:generated|exports?|reports?|invoices?|pdfs?|artifacts?)(?:/|$)", re.I)),
    ("media_assets", re.compile(r"(?:^|/)(?:media|images?|videos?|photos?|assets?)(?:/|$)", re.I)),
    ("archive_path", re.compile(r"(?:^|/)(?:archives?|cold[-_]?storage|glacier|nearline|coldline)(?:/|$)", re.I)),
)
_TEXT_SIGNAL_PATTERNS: dict[ObjectStorageSignal, re.Pattern[str]] = {
    "object_storage": re.compile(
        r"\b(?:object storage|file storage|storage objects?|stored objects?|storage layer|s3|gcs|"
        r"google cloud storage|cloud storage|azure blob|blob storage|minio)\b",
        re.I,
    ),
    "bucket": re.compile(r"\b(?:bucket|buckets|s3 bucket|gcs bucket|storage bucket)\b", re.I),
    "blob_store": re.compile(r"\b(?:blob store|blob storage|azure blobs?|stored blobs?)\b", re.I),
    "upload_storage": re.compile(r"\b(?:uploaded files?|user uploads?|upload storage|file uploads?|stored uploads?)\b", re.I),
    "generated_files": re.compile(
        r"\b(?:generated files?|generated reports?|export files?|report exports?|invoice pdfs?|"
        r"generated pdfs?|artifacts?)\b",
        re.I,
    ),
    "media_assets": re.compile(r"\b(?:media assets?|image assets?|video assets?|photos?|gallery assets?)\b", re.I),
    "archive_path": re.compile(r"\b(?:archive path|archive paths|archive bucket|cold storage|glacier|nearline|coldline|archive prefix)\b", re.I),
    "retention_sensitive": re.compile(
        r"\b(?:retention|retain|kept for|keep for|legal hold|compliance hold|records? policy|ttl|"
        r"time[- ]to[- ]live|expire after|expiration)\b",
        re.I,
    ),
    "destructive_lifecycle": re.compile(
        r"\b(?:delete|deletion|purge|expire|hard delete|destroy|remove old|cleanup|clean up|"
        r"overwrite|prune|lifecycle rule)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[ObjectStorageLifecycleSafeguard, re.Pattern[str]] = {
    "retention_policy": re.compile(
        r"\b(?:retention policy|retention period|retention rules?|retain for|kept for|keep for|legal hold|"
        r"compliance hold|records? retention)\b",
        re.I,
    ),
    "lifecycle_expiration": re.compile(
        r"\b(?:lifecycle expiration|lifecycle rules?|expiration rules?|expire after|ttl|time[- ]to[- ]live|"
        r"auto[- ]?expire|object expiration|delete marker cleanup)\b",
        re.I,
    ),
    "access_tier_archive_behavior": re.compile(
        r"\b(?:access tier|archive tier|archive behavior|cold storage|glacier|nearline|coldline|"
        r"intelligent tiering|storage class|rehydrat(?:e|ion)|restore from archive)\b",
        re.I,
    ),
    "deletion_recovery": re.compile(
        r"\b(?:deletion recovery|soft delete|versioning|object lock|undelete|restore deleted|recovery window|"
        r"delete protection|trash window|rollback deleted)\b",
        re.I,
    ),
    "encryption_owner_evidence": re.compile(
        r"\b(?:encryption|kms|customer[- ]managed key|cmk|bucket owner|object owner|service owner|data owner|"
        r"owner approval|owner sign[- ]?off|ownership evidence|approved by)\b",
        re.I,
    ),
    "cost_quota_monitoring": re.compile(
        r"\b(?:cost monitoring|quota monitoring|storage quota|bucket quota|budget alert|cost alert|usage alert|"
        r"size monitoring|storage metrics|object count metric|lifecycle cost)\b",
        re.I,
    ),
}
_PROVIDER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("s3", re.compile(r"\b(?:s3|aws bucket|s3 bucket|glacier)\b", re.I)),
    ("gcs", re.compile(r"\b(?:gcs|google cloud storage|gcs bucket|nearline|coldline)\b", re.I)),
    ("azure_blob", re.compile(r"\b(?:azure blob|blob storage|azure storage)\b", re.I)),
    ("minio", re.compile(r"\bminio\b", re.I)),
)
_RECOMMENDED_STEPS: dict[ObjectStorageLifecycleSafeguard, str] = {
    "retention_policy": "Define retention periods, legal-hold expectations, and owner-approved exceptions for stored objects.",
    "lifecycle_expiration": "Document lifecycle expiration rules, prefixes, delete markers, and rollout validation.",
    "access_tier_archive_behavior": "Plan storage class or archive-tier transitions, rehydration behavior, and restore expectations.",
    "deletion_recovery": "Confirm versioning, soft delete, object lock, or another recovery path before object deletion or expiration.",
    "encryption_owner_evidence": "Attach encryption, key ownership, bucket ownership, and service-owner approval evidence.",
    "cost_quota_monitoring": "Add storage usage, object count, quota, and budget monitoring for lifecycle changes.",
}


@dataclass(frozen=True, slots=True)
class TaskObjectStorageLifecycleReadinessRecord:
    """Readiness guidance for one task touching object storage lifecycle behavior."""

    task_id: str
    title: str
    storage_providers: tuple[str, ...]
    detected_signals: tuple[ObjectStorageSignal, ...]
    present_safeguards: tuple[ObjectStorageLifecycleSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[ObjectStorageLifecycleSafeguard, ...] = field(default_factory=tuple)
    risk_level: ObjectStorageLifecycleRisk = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_readiness_steps: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "storage_providers": list(self.storage_providers),
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_readiness_steps": list(self.recommended_readiness_steps),
        }


@dataclass(frozen=True, slots=True)
class TaskObjectStorageLifecycleReadinessPlan:
    """Plan-level object storage lifecycle readiness review."""

    plan_id: str | None = None
    records: tuple[TaskObjectStorageLifecycleReadinessRecord, ...] = field(default_factory=tuple)
    object_storage_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "object_storage_task_ids": list(self.object_storage_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render object storage lifecycle readiness as deterministic Markdown."""
        title = "# Task Object Storage Lifecycle Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Object storage task count: {self.summary.get('object_storage_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No object storage lifecycle readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Providers | Signals | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.storage_providers) or 'generic_object_storage')} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_object_storage_lifecycle_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskObjectStorageLifecycleReadinessPlan:
    """Build lifecycle readiness records for tasks that touch object storage."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    object_storage_task_ids = tuple(record.task_id for record in records)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskObjectStorageLifecycleReadinessPlan(
        plan_id=plan_id,
        records=records,
        object_storage_task_ids=object_storage_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_object_storage_lifecycle_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskObjectStorageLifecycleReadinessPlan:
    """Compatibility alias for building object storage lifecycle readiness plans."""
    return build_task_object_storage_lifecycle_readiness_plan(source)


def summarize_task_object_storage_lifecycle_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskObjectStorageLifecycleReadinessPlan:
    """Compatibility alias for building object storage lifecycle readiness plans."""
    return build_task_object_storage_lifecycle_readiness_plan(source)


def extract_task_object_storage_lifecycle_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskObjectStorageLifecycleReadinessPlan:
    """Compatibility alias for building object storage lifecycle readiness plans."""
    return build_task_object_storage_lifecycle_readiness_plan(source)


def generate_task_object_storage_lifecycle_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskObjectStorageLifecycleReadinessPlan:
    """Compatibility alias for generating object storage lifecycle readiness plans."""
    return build_task_object_storage_lifecycle_readiness_plan(source)


def recommend_task_object_storage_lifecycle_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskObjectStorageLifecycleReadinessPlan:
    """Compatibility alias for recommending object storage lifecycle readiness plans."""
    return build_task_object_storage_lifecycle_readiness_plan(source)


def task_object_storage_lifecycle_readiness_plan_to_dict(
    result: TaskObjectStorageLifecycleReadinessPlan,
) -> dict[str, Any]:
    """Serialize an object storage lifecycle readiness plan to a plain dictionary."""
    return result.to_dict()


task_object_storage_lifecycle_readiness_plan_to_dict.__test__ = False


def task_object_storage_lifecycle_readiness_plan_to_markdown(
    result: TaskObjectStorageLifecycleReadinessPlan,
) -> str:
    """Render an object storage lifecycle readiness plan as Markdown."""
    return result.to_markdown()


task_object_storage_lifecycle_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[ObjectStorageSignal, ...] = field(default_factory=tuple)
    storage_providers: tuple[str, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[ObjectStorageLifecycleSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskObjectStorageLifecycleReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskObjectStorageLifecycleReadinessRecord(
        task_id=task_id,
        title=title,
        storage_providers=signals.storage_providers,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals.signals, signals.present_safeguards, missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
        recommended_readiness_steps=tuple(_RECOMMENDED_STEPS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[ObjectStorageSignal] = set()
    safeguard_hits: set[ObjectStorageLifecycleSafeguard] = set()
    provider_hits: list[str] = []
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_signals = _path_signals(normalized)
        if path_signals:
            signal_hits.update(path_signals)
            signal_evidence.append(f"files_or_modules: {path}")
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(searchable) or pattern.search(normalized):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")
        provider_hits.extend(_providers(f"{normalized} {searchable}"))

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_signal = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signal_hits.add(signal)
                matched_signal = True
        if matched_signal:
            signal_evidence.append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)
        provider_hits.extend(_providers(text))

    if signal_hits & {"bucket", "blob_store", "upload_storage", "generated_files", "media_assets", "archive_path"}:
        signal_hits.add("object_storage")

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        storage_providers=tuple(_ordered_providers(provider_hits)),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_signals(path: str) -> set[ObjectStorageSignal]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals: set[ObjectStorageSignal] = set()
    for signal, pattern in _PATH_SIGNAL_PATTERNS:
        if pattern.search(normalized) or pattern.search(text):
            signals.add(signal)
    if re.search(r"\b(?:storage|object storage|bucket|blob|s3|gcs|minio)\b", text):
        signals.add("object_storage")
    if re.search(r"\b(?:retention|ttl|expire|expiration|lifecycle)\b", text):
        signals.add("retention_sensitive")
    if re.search(r"\b(?:delete|purge|expire|cleanup|prune|destroy|overwrite)\b", text):
        signals.add("destructive_lifecycle")
    name = PurePosixPath(normalized).name
    if name in {"bucket.py", "buckets.py", "storage.py", "object_storage.py", "blob_store.py"}:
        signals.add("object_storage")
    return signals


def _risk_level(
    signals: tuple[ObjectStorageSignal, ...],
    present: tuple[ObjectStorageLifecycleSafeguard, ...],
    missing: tuple[ObjectStorageLifecycleSafeguard, ...],
) -> ObjectStorageLifecycleRisk:
    if not missing:
        return "low"
    signal_set = set(signals)
    missing_set = set(missing)
    if "destructive_lifecycle" in signal_set and missing_set & {"retention_policy", "lifecycle_expiration", "deletion_recovery"}:
        return "high"
    if "retention_sensitive" in signal_set and missing_set & {"retention_policy", "lifecycle_expiration"}:
        return "high"
    if "archive_path" in signal_set and "access_tier_archive_behavior" in missing_set:
        return "high"
    if len(missing) >= 5:
        return "high"
    if {"retention_policy", "lifecycle_expiration"} <= set(present):
        return "medium"
    return "medium"


def _summary(
    records: tuple[TaskObjectStorageLifecycleReadinessRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "object_storage_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in sorted({signal for record in records for signal in record.detected_signals})
        },
        "storage_provider_counts": {
            provider: sum(1 for record in records if provider in record.storage_providers)
            for provider in sorted({provider for record in records for provider in record.storage_providers})
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
    return any(pattern.search(value) for pattern in [*_TEXT_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


def _providers(text: str) -> list[str]:
    providers = [provider for provider, pattern in _PROVIDER_PATTERNS if pattern.search(text)]
    if re.search(r"\b(?:object storage|bucket|blob store|storage objects?|stored objects?)\b", text, re.I):
        providers.append("generic_object_storage")
    return providers


def _ordered_providers(values: Iterable[str]) -> list[str]:
    priority = {"s3": 0, "gcs": 1, "azure_blob": 2, "minio": 3, "generic_object_storage": 4}
    return sorted(_dedupe(values), key=lambda provider: (priority.get(provider, 99), provider))


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
    "ObjectStorageLifecycleRisk",
    "ObjectStorageLifecycleSafeguard",
    "ObjectStorageSignal",
    "TaskObjectStorageLifecycleReadinessPlan",
    "TaskObjectStorageLifecycleReadinessRecord",
    "analyze_task_object_storage_lifecycle_readiness",
    "build_task_object_storage_lifecycle_readiness_plan",
    "extract_task_object_storage_lifecycle_readiness",
    "generate_task_object_storage_lifecycle_readiness",
    "recommend_task_object_storage_lifecycle_readiness",
    "summarize_task_object_storage_lifecycle_readiness",
    "task_object_storage_lifecycle_readiness_plan_to_dict",
    "task_object_storage_lifecycle_readiness_plan_to_markdown",
]
