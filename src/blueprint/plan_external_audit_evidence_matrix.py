"""Build external audit evidence matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ExternalAuditEvidenceField = Literal[
    "audit_framework",
    "evidence_artifact",
    "collection_owner",
    "retention_or_storage_signal",
    "privacy_or_redaction_signal",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_REQUIRED_FIELDS: tuple[ExternalAuditEvidenceField, ...] = (
    "audit_framework",
    "evidence_artifact",
    "collection_owner",
    "retention_or_storage_signal",
    "privacy_or_redaction_signal",
)
_SURFACE_ORDER: dict[str, int] = {
    "SOC 2": 0,
    "ISO 27001": 1,
    "PCI": 2,
    "HIPAA": 3,
    "security questionnaire": 4,
    "vendor evidence": 5,
    "external audit": 6,
    "compliance review": 7,
    "audit evidence": 8,
}
_AUDIT_RE = re.compile(
    r"\b(?:external auditors?|auditors?|audit evidence|audit package|compliance review|"
    r"soc\s*2|soc2|iso\s*27001|pci(?:[- ]?dss)?|hipaa|security questionnaires?|"
    r"vendor evidence|customer evidence|evidence retention|control owners?|"
    r"control evidence|screenshots?|exports?|attestations?)\b",
    re.I,
)
_SURFACE_PATTERNS: dict[str, re.Pattern[str]] = {
    "SOC 2": re.compile(r"\b(?:soc\s*2|soc2|trust services criteria|type\s*[12])\b", re.I),
    "ISO 27001": re.compile(r"\b(?:iso\s*27001|isms|annex\s*a)\b", re.I),
    "PCI": re.compile(r"\b(?:pci(?:[- ]?dss)?|cardholder|payment card)\b", re.I),
    "HIPAA": re.compile(r"\b(?:hipaa|phi|health information)\b", re.I),
    "security questionnaire": re.compile(r"\bsecurity questionnaires?\b", re.I),
    "vendor evidence": re.compile(r"\b(?:vendor evidence|customer evidence|vendor review)\b", re.I),
    "external audit": re.compile(r"\bexternal auditors?\b|\baudit package\b", re.I),
    "compliance review": re.compile(r"\bcompliance reviews?\b|\bregulatory reviews?\b", re.I),
    "audit evidence": re.compile(r"\b(?:audit evidence|control evidence|evidence collection)\b", re.I),
}
_FIELD_KEY_ALIASES: dict[ExternalAuditEvidenceField, tuple[str, ...]] = {
    "audit_framework": (
        "audit_framework",
        "framework",
        "frameworks",
        "control_framework",
        "compliance_framework",
        "audit_surface",
        "surface",
    ),
    "evidence_artifact": (
        "artifact",
        "artifacts",
        "evidence_artifact",
        "evidence_artifacts",
        "evidence",
        "deliverable",
        "deliverables",
        "screenshot",
        "screenshots",
        "export",
        "exports",
    ),
    "collection_owner": (
        "owner",
        "owners",
        "collection_owner",
        "evidence_owner",
        "control_owner",
        "dri",
        "assignee",
        "team",
    ),
    "retention_or_storage_signal": (
        "retention",
        "retention_period",
        "storage",
        "storage_location",
        "evidence_store",
        "repository",
        "archive",
    ),
    "privacy_or_redaction_signal": (
        "redaction",
        "redactions",
        "privacy",
        "privacy_review",
        "data_handling",
        "masking",
        "sanitization",
    ),
}
_FIELD_PATTERNS: dict[ExternalAuditEvidenceField, tuple[re.Pattern[str], ...]] = {
    "audit_framework": (
        re.compile(
            r"\b(?:frameworks?|audit framework|control framework|compliance framework|surface)"
            r"\s*[:=-]\s*([^.;\n]+)",
            re.I,
        ),
    ),
    "evidence_artifact": (
        re.compile(
            r"\b(?:evidence artifacts?|artifacts?|deliverables?|proof|screenshots?|exports?|"
            r"reports?|attestations?)\s*[:=-]\s*([^.;\n]+)",
            re.I,
        ),
        re.compile(
            r"\b((?:screenshots?|exports?|csv exports?|pdf exports?|reports?|attestations?|"
            r"audit logs?|access reviews?|change tickets?|test results?|questionnaire responses?)"
            r"[^.;\n]*)",
            re.I,
        ),
    ),
    "collection_owner": (
        re.compile(
            r"\b(?:collection owner|evidence owner|control owner|audit owner|owner|dri|assignee)"
            r"\s*[:=-]?\s*([^.;\n]+)",
            re.I,
        ),
    ),
    "retention_or_storage_signal": (
        re.compile(
            r"\b(?:retention|retain(?:ed)? for|store(?:d)? in|storage|archive|evidence store|"
            r"repository|grc|vanta|drata)\s*[:=-]?\s*([^.;\n]+)",
            re.I,
        ),
    ),
    "privacy_or_redaction_signal": (
        re.compile(
            r"\b(?:redact(?:ion|ed)?|mask(?:ed|ing)?|sanitize(?:d|ation)?|privacy review|"
            r"remove pii|remove phi|exclude secrets?|pii|phi|personal data|secrets?)\b([^.;\n]*)",
            re.I,
        ),
    ),
}
_CADENCE_RE = re.compile(
    r"\b((?:daily|weekly|monthly|quarterly|annual(?:ly)?|yearly|before each audit|"
    r"by \d{4}-\d{2}-\d{2}|by [A-Z][a-z]+ \d{1,2}(?:, \d{4})?|"
    r"deadline[: ]+[^.;\n]+|due[: ]+[^.;\n]+|before (?:the )?(?:audit|review)[^.;\n]*|"
    r"every \d+\s+(?:days?|weeks?|months?|quarters?|years?))\b[^.;\n]*)",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PlanExternalAuditEvidenceMatrixRow:
    """Audit evidence planning details for one audit-related task."""

    task_id: str
    title: str
    audit_framework: str = ""
    evidence_artifact: str = ""
    collection_owner: str = ""
    retention_or_storage_signal: str = ""
    review_cadence_or_deadline: str = ""
    privacy_or_redaction_signal: str = ""
    missing_fields: tuple[ExternalAuditEvidenceField, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommendation: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "audit_framework": self.audit_framework,
            "evidence_artifact": self.evidence_artifact,
            "collection_owner": self.collection_owner,
            "retention_or_storage_signal": self.retention_or_storage_signal,
            "review_cadence_or_deadline": self.review_cadence_or_deadline,
            "privacy_or_redaction_signal": self.privacy_or_redaction_signal,
            "missing_fields": list(self.missing_fields),
            "evidence": list(self.evidence),
            "recommendation": self.recommendation,
        }


@dataclass(frozen=True, slots=True)
class PlanExternalAuditEvidenceMatrix:
    """Plan-level external audit evidence matrix."""

    plan_id: str | None = None
    rows: tuple[PlanExternalAuditEvidenceMatrixRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanExternalAuditEvidenceMatrixRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "summary": dict(self.summary),
            "rows": [row.to_dict() for row in self.rows],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return external audit evidence rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan External Audit Evidence Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Total tasks: {self.summary.get('total_task_count', 0)}",
            f"- Audit-related tasks: {self.summary.get('audit_task_count', 0)}",
            f"- Tasks missing artifacts: {self.summary.get('tasks_missing_artifact', 0)}",
            f"- Tasks missing owners: {self.summary.get('tasks_missing_owner', 0)}",
            f"- Tasks missing retention: {self.summary.get('tasks_missing_retention', 0)}",
            f"- Tasks missing redaction: {self.summary.get('tasks_missing_redaction', 0)}",
        ]
        if not self.rows:
            lines.extend(["", "No external audit evidence rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                (
                    "| Task | Title | Framework/Surface | Artifact | Owner | Retention/Storage | "
                    "Cadence/Deadline | Redaction | Missing Fields | Recommendation | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.task_id)} | "
                f"{_markdown_cell(row.title)} | "
                f"{_markdown_cell(row.audit_framework or 'unspecified')} | "
                f"{_markdown_cell(row.evidence_artifact or 'unspecified')} | "
                f"{_markdown_cell(row.collection_owner or 'unspecified')} | "
                f"{_markdown_cell(row.retention_or_storage_signal or 'unspecified')} | "
                f"{_markdown_cell(row.review_cadence_or_deadline or 'unspecified')} | "
                f"{_markdown_cell(row.privacy_or_redaction_signal or 'unspecified')} | "
                f"{_markdown_cell(', '.join(row.missing_fields) or 'none')} | "
                f"{_markdown_cell(row.recommendation)} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_external_audit_evidence_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanExternalAuditEvidenceMatrix:
    """Build external audit evidence rows for audit-related execution tasks."""
    plan_id, tasks = _source_payload(source)
    rows = tuple(
        sorted(
            (
                row
                for index, task in enumerate(tasks, start=1)
                if (row := _row_for_task(task, index)) is not None
            ),
            key=lambda row: (len(row.missing_fields), row.task_id, row.title.casefold()),
        )
    )
    return PlanExternalAuditEvidenceMatrix(
        plan_id=plan_id,
        rows=rows,
        summary=_summary(rows, total_task_count=len(tasks)),
    )


def generate_plan_external_audit_evidence_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[PlanExternalAuditEvidenceMatrixRow, ...]:
    """Return external audit evidence rows for relevant execution tasks."""
    return build_plan_external_audit_evidence_matrix(source).rows


def derive_plan_external_audit_evidence_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanExternalAuditEvidenceMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanExternalAuditEvidenceMatrix:
    """Return an existing matrix or generate one from a plan-shaped source."""
    if isinstance(source, PlanExternalAuditEvidenceMatrix):
        return source
    return build_plan_external_audit_evidence_matrix(source)


def summarize_plan_external_audit_evidence_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanExternalAuditEvidenceMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanExternalAuditEvidenceMatrix:
    """Compatibility alias for external audit evidence summaries."""
    return derive_plan_external_audit_evidence_matrix(source)


def plan_external_audit_evidence_matrix_to_dict(
    matrix: PlanExternalAuditEvidenceMatrix,
) -> dict[str, Any]:
    """Serialize an external audit evidence matrix to a plain dictionary."""
    return matrix.to_dict()


plan_external_audit_evidence_matrix_to_dict.__test__ = False


def plan_external_audit_evidence_matrix_to_dicts(
    rows: (
        PlanExternalAuditEvidenceMatrix
        | tuple[PlanExternalAuditEvidenceMatrixRow, ...]
        | list[PlanExternalAuditEvidenceMatrixRow]
    ),
) -> list[dict[str, Any]]:
    """Serialize external audit evidence rows to dictionaries."""
    if isinstance(rows, PlanExternalAuditEvidenceMatrix):
        return rows.to_dicts()
    return [row.to_dict() for row in rows]


plan_external_audit_evidence_matrix_to_dicts.__test__ = False


def plan_external_audit_evidence_matrix_to_markdown(
    matrix: PlanExternalAuditEvidenceMatrix,
) -> str:
    """Render an external audit evidence matrix as Markdown."""
    return matrix.to_markdown()


plan_external_audit_evidence_matrix_to_markdown.__test__ = False


def _row_for_task(
    task: Mapping[str, Any],
    index: int,
) -> PlanExternalAuditEvidenceMatrixRow | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    surfaces: dict[str, list[str]] = {}
    fields: dict[ExternalAuditEvidenceField, list[str]] = {key: [] for key in _REQUIRED_FIELDS}
    cadence: list[str] = []
    evidence: list[str] = []

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        if _AUDIT_RE.search(text):
            evidence.append(snippet)
            for surface, pattern in _SURFACE_PATTERNS.items():
                if pattern.search(text):
                    surfaces.setdefault(surface, []).append(snippet)
        for field_name, patterns in _FIELD_PATTERNS.items():
            for pattern in patterns:
                for match in pattern.finditer(text):
                    value = next((group for group in match.groups() if group), match.group(0))
                    fields[field_name].append(_clean(value))
                    evidence.append(snippet)
        for match in _CADENCE_RE.finditer(text):
            cadence.append(_clean(match.group(1)))
            evidence.append(snippet)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for field_name, aliases in _FIELD_KEY_ALIASES.items():
            for source_field, value in _metadata_values(metadata, aliases):
                for item in _strings(value):
                    fields[field_name].append(_clean(item))
                    evidence.append(_evidence_snippet(source_field, item))
        for source_field, text in _metadata_texts(metadata):
            key_text = source_field.replace("_", " ")
            if _AUDIT_RE.search(text) or _AUDIT_RE.search(key_text):
                evidence.append(_evidence_snippet(source_field, text or key_text))
                for surface, pattern in _SURFACE_PATTERNS.items():
                    if pattern.search(text) or pattern.search(key_text):
                        surfaces.setdefault(surface, []).append(
                            _evidence_snippet(source_field, text or key_text)
                        )
        for source_field, value in _metadata_values(
            metadata,
            ("cadence", "review_cadence", "deadline", "due_date", "review_deadline"),
        ):
            for item in _strings(value):
                cadence.append(_clean(item))
                evidence.append(_evidence_snippet(source_field, item))

    for key in ("owner", "assignee", "dri", "team", "owner_type"):
        if owner := _optional_text(task.get(key)):
            fields["collection_owner"].append(owner)
            evidence.append(_evidence_snippet(key, owner))

    if not surfaces and not any(fields.values()):
        return None
    if not surfaces:
        surfaces["audit evidence"] = evidence[:1] or [_evidence_snippet("title", title)]

    audit_framework = _first(fields["audit_framework"]) or sorted(
        surfaces, key=lambda item: _SURFACE_ORDER[item]
    )[0]
    evidence_artifact = _most_specific(fields["evidence_artifact"])
    collection_owner = _first(fields["collection_owner"])
    retention = _first(fields["retention_or_storage_signal"])
    redaction = _first(fields["privacy_or_redaction_signal"])
    missing_fields = tuple(
        field_name
        for field_name in _REQUIRED_FIELDS
        if (
            (field_name == "audit_framework" and not audit_framework)
            or (field_name == "evidence_artifact" and not evidence_artifact)
            or (field_name == "collection_owner" and not collection_owner)
            or (field_name == "retention_or_storage_signal" and not retention)
            or (field_name == "privacy_or_redaction_signal" and not redaction)
        )
    )
    return PlanExternalAuditEvidenceMatrixRow(
        task_id=task_id,
        title=title,
        audit_framework=audit_framework,
        evidence_artifact=evidence_artifact,
        collection_owner=collection_owner,
        retention_or_storage_signal=retention,
        review_cadence_or_deadline=_first(cadence),
        privacy_or_redaction_signal=redaction,
        missing_fields=missing_fields,
        evidence=tuple(_dedupe(evidence)),
        recommendation=_recommendation(missing_fields),
    )


def _recommendation(missing_fields: tuple[ExternalAuditEvidenceField, ...]) -> str:
    if not missing_fields:
        return "Ready: audit evidence plan names the framework, artifact, owner, retention, and redaction handling."
    actions = {
        "audit_framework": "name the audit framework or review surface",
        "evidence_artifact": "specify the evidence artifact reviewers will inspect",
        "collection_owner": "assign the control or evidence collection owner",
        "retention_or_storage_signal": "state the retention period or evidence storage location",
        "privacy_or_redaction_signal": "define redaction or privacy handling before sharing",
    }
    return "Before audit review, " + "; ".join(actions[field_name] for field_name in missing_fields) + "."


def _summary(
    rows: tuple[PlanExternalAuditEvidenceMatrixRow, ...],
    *,
    total_task_count: int,
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "audit_task_count": len(rows),
        "non_audit_task_count": max(total_task_count - len(rows), 0),
        "tasks_missing_owner": sum(1 for row in rows if "collection_owner" in row.missing_fields),
        "tasks_missing_artifact": sum(1 for row in rows if "evidence_artifact" in row.missing_fields),
        "tasks_missing_retention": sum(
            1 for row in rows if "retention_or_storage_signal" in row.missing_fields
        ),
        "tasks_missing_redaction": sum(
            1 for row in rows if "privacy_or_redaction_signal" in row.missing_fields
        ),
        "missing_field_count": sum(len(row.missing_fields) for row in rows),
        "missing_field_counts": {
            field_name: sum(1 for row in rows if field_name in row.missing_fields)
            for field_name in _REQUIRED_FIELDS
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
        "tags",
        "labels",
        "notes",
        "risks",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    return texts


def _metadata_values(
    value: Any,
    aliases: tuple[str, ...],
    prefix: str = "metadata",
) -> list[tuple[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    wanted = {alias.casefold() for alias in aliases}
    matches: list[tuple[str, Any]] = []
    for key in sorted(value, key=lambda item: str(item)):
        child = value[key]
        field = f"{prefix}.{key}"
        normalized = str(key).casefold().replace("-", "_").replace(" ", "_")
        if normalized in wanted:
            matches.append((field, child))
        if isinstance(child, Mapping):
            matches.extend(_metadata_values(child, aliases, field))
    return matches


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            key_text = str(key).replace("_", " ")
            if _AUDIT_RE.search(key_text):
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


def _first(values: Iterable[str]) -> str:
    values = _dedupe(_clean(value) for value in values)
    return values[0] if values else ""


def _most_specific(values: Iterable[str]) -> str:
    values = _dedupe(_clean(value) for value in values)
    if not values:
        return ""
    return sorted(values, key=lambda value: (-len(value), values.index(value)))[0]


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _clean(value: str) -> str:
    return _SPACE_RE.sub(" ", str(value)).strip().strip("`'\",;:()[]{}").rstrip(".")


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
    "ExternalAuditEvidenceField",
    "PlanExternalAuditEvidenceMatrix",
    "PlanExternalAuditEvidenceMatrixRow",
    "build_plan_external_audit_evidence_matrix",
    "derive_plan_external_audit_evidence_matrix",
    "generate_plan_external_audit_evidence_matrix",
    "plan_external_audit_evidence_matrix_to_dict",
    "plan_external_audit_evidence_matrix_to_dicts",
    "plan_external_audit_evidence_matrix_to_markdown",
    "summarize_plan_external_audit_evidence_matrix",
]
