"""Plan PII anonymization readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


PiiAnonymizationTechnique = Literal[
    "anonymization",
    "masking",
    "tokenization",
    "hashing",
    "pseudonymization",
    "synthetic_data",
    "redaction",
    "deidentification",
]
PiiAnonymizationControl = Literal[
    "reversibility_policy",
    "reidentification_risk_check",
    "test_fixture_coverage",
    "downstream_consumer_validation",
    "retention_alignment",
    "audit_evidence",
]
PiiAnonymizationReadinessLevel = Literal["missing", "partial", "ready"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[PiiAnonymizationReadinessLevel, int] = {"missing": 0, "partial": 1, "ready": 2}
_TECHNIQUE_ORDER: dict[PiiAnonymizationTechnique, int] = {
    "anonymization": 0,
    "masking": 1,
    "tokenization": 2,
    "hashing": 3,
    "pseudonymization": 4,
    "synthetic_data": 5,
    "redaction": 6,
    "deidentification": 7,
}
_CONTROL_ORDER: tuple[PiiAnonymizationControl, ...] = (
    "reversibility_policy",
    "reidentification_risk_check",
    "test_fixture_coverage",
    "downstream_consumer_validation",
    "retention_alignment",
    "audit_evidence",
)
_TECHNIQUE_PATTERNS: dict[PiiAnonymizationTechnique, re.Pattern[str]] = {
    "anonymization": re.compile(r"\b(?:anonymi[sz]e|anonymi[sz]ation|anonymous data)\b", re.I),
    "masking": re.compile(r"\b(?:mask|masked|masking|data masking|pii masking|field masking)\b", re.I),
    "tokenization": re.compile(r"\b(?:tokeni[sz]e|tokeni[sz]ation|tokenized identifiers?|pii tokens?)\b", re.I),
    "hashing": re.compile(r"\b(?:hash|hashed|hashing|salted hash|hmac|sha[- ]?256|one[- ]way digest)\b", re.I),
    "pseudonymization": re.compile(r"\b(?:pseudonymi[sz]e|pseudonymi[sz]ation|pseudonymous|surrogate keys?)\b", re.I),
    "synthetic_data": re.compile(r"\b(?:synthetic data|synthetic pii|fake pii|generated test data)\b", re.I),
    "redaction": re.compile(r"\b(?:redact|redacted|redaction|scrub|scrubbed|scrubbing)\b", re.I),
    "deidentification": re.compile(r"\b(?:de[- ]?identify|de[- ]?identified|de[- ]?identification|deid|de-id)\b", re.I),
}
_PATH_TECHNIQUE_PATTERNS: dict[PiiAnonymizationTechnique, re.Pattern[str]] = {
    "anonymization": re.compile(r"anonymi[sz]|anonymous", re.I),
    "masking": re.compile(r"mask(?:ing|ed)?|pii[_-]?mask", re.I),
    "tokenization": re.compile(r"tokeni[sz]|token(?:s|izer)?", re.I),
    "hashing": re.compile(r"hash(?:ing|ed)?|hmac|sha[_-]?256|digest", re.I),
    "pseudonymization": re.compile(r"pseudonymi[sz]|pseudonym|surrogate[_-]?key", re.I),
    "synthetic_data": re.compile(r"synthetic|fake[_-]?pii|fixtures?", re.I),
    "redaction": re.compile(r"redact|redaction|scrub", re.I),
    "deidentification": re.compile(r"de[_-]?ident|deid|de[_-]?id", re.I),
}
_PII_CONTEXT_RE = re.compile(
    r"\b(?:pii|personal data|personally identifiable|sensitive data|customer data|user data|"
    r"email addresses?|phone numbers?|ssn|social security|names?|addresses?|birth dates?)\b",
    re.I,
)
_CONTROL_PATTERNS: dict[PiiAnonymizationControl, re.Pattern[str]] = {
    "reversibility_policy": re.compile(
        r"\b(?:reversibility policy|reversible|irreversible|one[- ]way|detokeni[sz]ation|reversal|"
        r"token vault|key rotation|mapping table|salt management|secret key)\b",
        re.I,
    ),
    "reidentification_risk_check": re.compile(
        r"\b(?:re[- ]?identification risk|reidentify|re-identify|k[- ]?anonymity|l[- ]?diversity|"
        r"linkage attack|singling out|uniqueness check|privacy risk review)\b",
        re.I,
    ),
    "test_fixture_coverage": re.compile(
        r"\b(?:test fixtures?|fixture coverage|unit tests?|integration tests?|golden tests?|"
        r"sample pii|synthetic test data|redaction tests?|masking tests?)\b",
        re.I,
    ),
    "downstream_consumer_validation": re.compile(
        r"\b(?:downstream consumer|consumer validation|analytics validation|reporting validation|"
        r"data contract|schema contract|pipeline compatibility|warehouse validation|export validation)\b",
        re.I,
    ),
    "retention_alignment": re.compile(
        r"\b(?:retention alignment|retention policy|retention window|data retention|ttl|time[- ]to[- ]live|"
        r"purge|delete source pii|deletion schedule|data lifecycle)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit evidence|audit trail|audit log|audit logging|approval record|compliance evidence|"
        r"review evidence|decision log|privacy review)\b",
        re.I,
    ),
}
_RECOMMENDED_STEPS: dict[PiiAnonymizationControl, str] = {
    "reversibility_policy": "Define whether the transform is reversible, who can reverse it, and how keys or token maps are governed.",
    "reidentification_risk_check": "Add a reidentification risk check for linkage, uniqueness, and singling-out risk before release.",
    "test_fixture_coverage": "Cover representative PII fixtures, edge cases, and non-PII false positives in tests.",
    "downstream_consumer_validation": "Validate downstream analytics, exports, schemas, and consumers against anonymized values.",
    "retention_alignment": "Align source PII, derived data, token maps, and audit records with retention and deletion policy.",
    "audit_evidence": "Record privacy review, control decisions, and verification evidence for auditability.",
}


@dataclass(frozen=True, slots=True)
class TaskPiiAnonymizationReadinessRecord:
    """Readiness guidance for one task touching PII anonymization flows."""

    task_id: str
    title: str
    techniques: tuple[PiiAnonymizationTechnique, ...]
    present_controls: tuple[PiiAnonymizationControl, ...] = field(default_factory=tuple)
    missing_controls: tuple[PiiAnonymizationControl, ...] = field(default_factory=tuple)
    readiness_level: PiiAnonymizationReadinessLevel = "missing"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_readiness_steps: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "techniques": list(self.techniques),
            "present_controls": list(self.present_controls),
            "missing_controls": list(self.missing_controls),
            "readiness_level": self.readiness_level,
            "evidence": list(self.evidence),
            "recommended_readiness_steps": list(self.recommended_readiness_steps),
        }


@dataclass(frozen=True, slots=True)
class TaskPiiAnonymizationReadinessPlan:
    """Plan-level PII anonymization readiness review."""

    plan_id: str | None = None
    records: tuple[TaskPiiAnonymizationReadinessRecord, ...] = field(default_factory=tuple)
    pii_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "pii_task_ids": list(self.pii_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render PII anonymization readiness as deterministic Markdown."""
        title = "# Task PII Anonymization Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Affected task count: {self.summary.get('affected_task_count', 0)}",
            f"- Missing control count: {self.summary.get('missing_control_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No PII anonymization readiness records were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Techniques | Missing Controls | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell(', '.join(record.techniques) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_controls) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_pii_anonymization_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPiiAnonymizationReadinessPlan:
    """Build readiness records for tasks that touch PII anonymization work."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_READINESS_ORDER[record.readiness_level], record.task_id, record.title.casefold()),
        )
    )
    pii_task_ids = tuple(record.task_id for record in records)
    ignored_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskPiiAnonymizationReadinessPlan(
        plan_id=plan_id,
        records=records,
        pii_task_ids=pii_task_ids,
        ignored_task_ids=ignored_task_ids,
        summary=_summary(records, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def analyze_task_pii_anonymization_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPiiAnonymizationReadinessPlan:
    """Compatibility alias for building PII anonymization readiness plans."""
    return build_task_pii_anonymization_readiness_plan(source)


def summarize_task_pii_anonymization_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPiiAnonymizationReadinessPlan:
    """Compatibility alias for building PII anonymization readiness plans."""
    return build_task_pii_anonymization_readiness_plan(source)


def extract_task_pii_anonymization_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPiiAnonymizationReadinessPlan:
    """Compatibility alias for building PII anonymization readiness plans."""
    return build_task_pii_anonymization_readiness_plan(source)


def generate_task_pii_anonymization_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPiiAnonymizationReadinessPlan:
    """Compatibility alias for generating PII anonymization readiness plans."""
    return build_task_pii_anonymization_readiness_plan(source)


def recommend_task_pii_anonymization_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPiiAnonymizationReadinessPlan:
    """Compatibility alias for recommending PII anonymization readiness plans."""
    return build_task_pii_anonymization_readiness_plan(source)


def task_pii_anonymization_readiness_plan_to_dict(
    result: TaskPiiAnonymizationReadinessPlan,
) -> dict[str, Any]:
    """Serialize a PII anonymization readiness plan to a plain dictionary."""
    return result.to_dict()


task_pii_anonymization_readiness_plan_to_dict.__test__ = False


def task_pii_anonymization_readiness_plan_to_markdown(
    result: TaskPiiAnonymizationReadinessPlan,
) -> str:
    """Render a PII anonymization readiness plan as Markdown."""
    return result.to_markdown()


task_pii_anonymization_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    techniques: tuple[PiiAnonymizationTechnique, ...] = field(default_factory=tuple)
    technique_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_controls: tuple[PiiAnonymizationControl, ...] = field(default_factory=tuple)
    control_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskPiiAnonymizationReadinessRecord | None:
    signals = _signals(task)
    if not signals.techniques:
        return None

    missing = tuple(control for control in _CONTROL_ORDER if control not in signals.present_controls)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskPiiAnonymizationReadinessRecord(
        task_id=task_id,
        title=title,
        techniques=signals.techniques,
        present_controls=signals.present_controls,
        missing_controls=missing,
        readiness_level=_readiness_level(signals.present_controls, missing),
        evidence=tuple(_dedupe([*signals.technique_evidence, *signals.control_evidence])),
        recommended_readiness_steps=tuple(_RECOMMENDED_STEPS[control] for control in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    technique_hits: set[PiiAnonymizationTechnique] = set()
    control_hits: set[PiiAnonymizationControl] = set()
    technique_evidence: list[str] = []
    control_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_techniques = _path_techniques(normalized)
        if path_techniques:
            technique_hits.update(path_techniques)
            technique_evidence.append(f"files_or_modules: {path}")
        for control, pattern in _CONTROL_PATTERNS.items():
            if pattern.search(searchable) or pattern.search(normalized):
                control_hits.add(control)
                control_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_technique = False
        for technique, pattern in _TECHNIQUE_PATTERNS.items():
            if pattern.search(text):
                technique_hits.add(technique)
                matched_technique = True
        if matched_technique or (_PII_CONTEXT_RE.search(text) and re.search(r"\b(?:privacy|protect|remove|hide|sanitize)\b", text, re.I)):
            technique_evidence.append(snippet)
        for control, pattern in _CONTROL_PATTERNS.items():
            if pattern.search(text):
                control_hits.add(control)
                control_evidence.append(snippet)

    return _Signals(
        techniques=tuple(technique for technique in _TECHNIQUE_ORDER if technique in technique_hits),
        technique_evidence=tuple(_dedupe(technique_evidence)),
        present_controls=tuple(control for control in _CONTROL_ORDER if control in control_hits),
        control_evidence=tuple(_dedupe(control_evidence)),
    )


def _path_techniques(path: str) -> set[PiiAnonymizationTechnique]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    techniques: set[PiiAnonymizationTechnique] = set()
    for technique, pattern in _PATH_TECHNIQUE_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(text):
            techniques.add(technique)
    if _PII_CONTEXT_RE.search(text) and re.search(r"\b(?:privacy|sanitize|safe|clean)\b", text, re.I):
        techniques.add("anonymization")
    return techniques


def _readiness_level(
    present: tuple[PiiAnonymizationControl, ...],
    missing: tuple[PiiAnonymizationControl, ...],
) -> PiiAnonymizationReadinessLevel:
    if not missing:
        return "ready"
    if present:
        return "partial"
    return "missing"


def _summary(
    records: tuple[TaskPiiAnonymizationReadinessRecord, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "affected_task_count": len(records),
        "pii_task_count": len(records),
        "ignored_task_ids": list(ignored_task_ids),
        "missing_control_count": sum(len(record.missing_controls) for record in records),
        "readiness_counts": {
            level: sum(1 for record in records if record.readiness_level == level)
            for level in _READINESS_ORDER
        },
        "missing_control_counts": {
            control: sum(1 for record in records if control in record.missing_controls)
            for control in _CONTROL_ORDER
        },
        "technique_counts": {
            technique: sum(1 for record in records if technique in record.techniques)
            for technique in sorted({technique for record in records for technique in record.techniques})
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
        patterns = (*_TECHNIQUE_PATTERNS.values(), *_CONTROL_PATTERNS.values(), _PII_CONTEXT_RE)
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            key_is_signal = any(pattern.search(key_text) for pattern in patterns)
            if key_is_signal:
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if key_is_signal:
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
    "PiiAnonymizationControl",
    "PiiAnonymizationReadinessLevel",
    "PiiAnonymizationTechnique",
    "TaskPiiAnonymizationReadinessPlan",
    "TaskPiiAnonymizationReadinessRecord",
    "analyze_task_pii_anonymization_readiness",
    "build_task_pii_anonymization_readiness_plan",
    "extract_task_pii_anonymization_readiness",
    "generate_task_pii_anonymization_readiness",
    "recommend_task_pii_anonymization_readiness",
    "summarize_task_pii_anonymization_readiness",
    "task_pii_anonymization_readiness_plan_to_dict",
    "task_pii_anonymization_readiness_plan_to_markdown",
]
