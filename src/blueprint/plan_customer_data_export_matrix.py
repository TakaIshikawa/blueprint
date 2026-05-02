"""Build plan-level customer data export planning matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CustomerDataExportSurface = Literal[
    "self_service_export",
    "admin_export",
    "audit_export",
    "reporting_export",
    "file_download",
    "bulk_export",
    "retention_bound_export",
    "redacted_export",
]
CustomerDataExportScope = Literal[
    "personal_data",
    "customer_data",
    "tenant_data",
    "audit_records",
    "reporting_data",
    "unknown_customer_data",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_EXPORT_ORDER: dict[CustomerDataExportSurface, int] = {
    "self_service_export": 0,
    "admin_export": 1,
    "audit_export": 2,
    "reporting_export": 3,
    "file_download": 4,
    "bulk_export": 5,
    "retention_bound_export": 6,
    "redacted_export": 7,
}
_SCOPE_ORDER: dict[CustomerDataExportScope, int] = {
    "personal_data": 0,
    "customer_data": 1,
    "tenant_data": 2,
    "audit_records": 3,
    "reporting_data": 4,
    "unknown_customer_data": 5,
}
_MISSING_FIELD_ORDER: tuple[str, ...] = (
    "data_scope",
    "actor_or_owner",
    "format_or_destination",
    "privacy_safeguards",
)
_EXPORT_PATTERNS: dict[CustomerDataExportSurface, re.Pattern[str]] = {
    "self_service_export": re.compile(
        r"\b(?:self[- ]service|user[- ]initiated|customer[- ]initiated|download my data|"
        r"data portability|dsr export|subject access request)\b",
        re.I,
    ),
    "admin_export": re.compile(
        r"\b(?:admin export|administrator export|backoffice export|support export|operator export|"
        r"staff download|tenant admin)\b",
        re.I,
    ),
    "audit_export": re.compile(
        r"\b(?:audit export|audit log export|audit trail export|export audit|compliance export|"
        r"audit report|audit logs?|audit trail)\b",
        re.I,
    ),
    "reporting_export": re.compile(
        r"\b(?:report export|export report|reporting export|scheduled report|generated report|"
        r"pdf report|csv report|business intelligence|dashboard export|analytics report)\b",
        re.I,
    ),
    "file_download": re.compile(
        r"\b(?:download|downloads|downloadable|csv|pdf|xlsx|spreadsheet|attachment|file export|"
        r"generate (?:a )?(?:csv|pdf)|csv generation|pdf generation)\b",
        re.I,
    ),
    "bulk_export": re.compile(
        r"\b(?:bulk\W+(?:\w+\W+){0,3}export|bulk download|mass export|full export|data dump|archive export|"
        r"batch export|export all|tenant dump)\b",
        re.I,
    ),
    "retention_bound_export": re.compile(
        r"\b(?:retention[- ]bound export|retention bounds?|export retention|retained export|"
        r"expire exports?|export expiry|export expiration|ttl for exports?|delete exported files)\b",
        re.I,
    ),
    "redacted_export": re.compile(
        r"\b(?:redacted export|redact(?:ed|ion)?|mask(?:ed|ing)?|anonymi[sz]e|pseudonymi[sz]e|"
        r"remove pii|exclude pii)\b",
        re.I,
    ),
}
_DATA_SCOPE_PATTERNS: dict[CustomerDataExportScope, re.Pattern[str]] = {
    "personal_data": re.compile(
        r"\b(?:pii|personal data|personal information|personally identifiable|email addresses?|"
        r"phone numbers?|addresses?|date of birth|birthdate|ssn|passport)\b",
        re.I,
    ),
    "customer_data": re.compile(
        r"\b(?:customer data|customer records?|account data|profile data|user data|users?|"
        r"customers?|accounts?)\b",
        re.I,
    ),
    "tenant_data": re.compile(
        r"\b(?:tenant data|tenant records?|tenant export|workspace data)\b", re.I
    ),
    "audit_records": re.compile(
        r"\b(?:audit records?|audit logs?|audit trail|compliance events?|access logs?)\b", re.I
    ),
    "reporting_data": re.compile(
        r"\b(?:reporting data|report data|analytics data|metrics|dashboard data|bi data)\b", re.I
    ),
}
_FORMAT_RE = re.compile(
    r"\b(?:csv|pdf|xlsx|json|jsonl|parquet|zip|spreadsheet|s3|gcs|bucket|email|attachment|"
    r"download|webhook|warehouse|data lake|object storage)\b",
    re.I,
)
_SAFEGUARD_RE = re.compile(
    r"\b(?:redact|redacted|redaction|mask|masked|masking|encrypt|encrypted|encryption|"
    r"access control|permission|rbac|audit|approval|retention|expire|ttl|delete exported|"
    r"watermark|rate limit|least privilege|privacy review)\b",
    re.I,
)
_OWNER_KEY_RE = re.compile(
    r"\b(?:owner|actor|admin|customer|tenant admin|support|privacy|data steward)\b", re.I
)
_EXPORT_SIGNAL_RE = re.compile(
    r"\b(?:export|exports|download|downloads|csv|pdf|report|bulk|data dump|spreadsheet)\b", re.I
)
_NEGATED_EXPORT_RE = re.compile(
    r"\b(?:no|not|never|without|exclude|excluding|do not|don't|does not|should not|non[- ]export|"
    r"docs? only|documentation only)\b.{0,40}\b(?:export|download|csv|pdf|report|bulk)\b|"
    r"\b(?:export|download|csv|pdf|report|bulk)\b.{0,30}\b(?:not required|out of scope|disabled|removed)\b",
    re.I,
)
_LOW_RISK_DOC_RE = re.compile(
    r"\b(?:docs?|documentation|copy|runbook|readme|help text|labels?)\b", re.I
)


@dataclass(frozen=True, slots=True)
class PlanCustomerDataExportMatrixRow:
    """One task-level customer data export planning row."""

    task_id: str
    title: str
    export_surface: CustomerDataExportSurface
    data_scope: CustomerDataExportScope
    actor_or_owner: str | None
    format_or_destination: str | None
    privacy_safeguards: tuple[str, ...] = field(default_factory=tuple)
    missing_fields: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommendation: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "export_surface": self.export_surface,
            "data_scope": self.data_scope,
            "actor_or_owner": self.actor_or_owner,
            "format_or_destination": self.format_or_destination,
            "privacy_safeguards": list(self.privacy_safeguards),
            "missing_fields": list(self.missing_fields),
            "evidence": list(self.evidence),
            "recommendation": self.recommendation,
        }


@dataclass(frozen=True, slots=True)
class PlanCustomerDataExportMatrix:
    """Plan-level customer data export planning matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanCustomerDataExportMatrixRow, ...] = field(default_factory=tuple)
    export_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanCustomerDataExportMatrixRow, ...]:
        """Compatibility alias for matrix rows."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "export_task_ids": list(self.export_task_ids),
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return customer data export planning rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Customer Data Export Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('export_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks require customer data export planning "
                f"({self.summary.get('missing_field_total', 0)} missing planning fields)."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No customer data export planning rows were inferred."])
            if self.no_signal_task_ids:
                lines.extend(
                    ["", f"No export signals: {_markdown_cell(', '.join(self.no_signal_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Task | Title | Export Surface | Data Scope | Actor/Owner | "
                    "Format/Destination | Safeguards | Missing Fields | Evidence | Recommendation |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.title)} | "
                f"{row.export_surface} | "
                f"{row.data_scope} | "
                f"{_markdown_cell(row.actor_or_owner or 'Unassigned')} | "
                f"{_markdown_cell(row.format_or_destination or 'Unspecified')} | "
                f"{_markdown_cell(', '.join(row.privacy_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(row.missing_fields) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence))} | "
                f"{_markdown_cell(row.recommendation)} |"
            )
        if self.no_signal_task_ids:
            lines.extend(
                ["", f"No export signals: {_markdown_cell(', '.join(self.no_signal_task_ids))}"]
            )
        return "\n".join(lines)


def build_plan_customer_data_export_matrix(
    source: (
        PlanCustomerDataExportMatrix
        | Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanCustomerDataExportMatrix:
    """Build task-level customer data export planning rows for an execution plan."""
    prebuilt = _prebuilt_matrix(source)
    if prebuilt is not None:
        return prebuilt

    plan_id, tasks = _source_payload(source)
    candidates = [_task_rows(task, index) for index, task in enumerate(tasks, start=1)]
    rows = tuple(
        sorted(
            (row for task_rows in candidates for row in task_rows),
            key=lambda row: (
                _EXPORT_ORDER[row.export_surface],
                _SCOPE_ORDER[row.data_scope],
                row.task_id,
                row.title.casefold(),
            ),
        )
    )
    export_task_ids = tuple(_dedupe(row.task_id for row in rows))
    no_signal_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if not candidates[index - 1]
    )
    export_surface_counts = {
        surface: sum(1 for row in rows if row.export_surface == surface)
        for surface in _EXPORT_ORDER
    }
    missing_field_counts = {
        field_name: sum(1 for row in rows if field_name in row.missing_fields)
        for field_name in _MISSING_FIELD_ORDER
    }
    return PlanCustomerDataExportMatrix(
        plan_id=plan_id,
        rows=rows,
        export_task_ids=export_task_ids,
        no_signal_task_ids=no_signal_task_ids,
        summary={
            "task_count": len(tasks),
            "row_count": len(rows),
            "export_task_count": len(export_task_ids),
            "no_signal_task_count": len(no_signal_task_ids),
            "export_surface_counts": export_surface_counts,
            "missing_field_counts": missing_field_counts,
            "missing_field_total": sum(len(row.missing_fields) for row in rows),
        },
    )


def generate_plan_customer_data_export_matrix(
    source: (
        PlanCustomerDataExportMatrix
        | Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanCustomerDataExportMatrix:
    """Compatibility alias for building customer data export matrices."""
    return build_plan_customer_data_export_matrix(source)


def derive_plan_customer_data_export_matrix(
    source: (
        PlanCustomerDataExportMatrix
        | Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanCustomerDataExportMatrix:
    """Compatibility alias for deriving customer data export matrices."""
    return build_plan_customer_data_export_matrix(source)


def summarize_plan_customer_data_export_matrix(
    source: (
        PlanCustomerDataExportMatrix
        | Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanCustomerDataExportMatrix:
    """Compatibility alias for summarizing customer data export matrices."""
    return build_plan_customer_data_export_matrix(source)


def plan_customer_data_export_matrix_to_dict(
    matrix: PlanCustomerDataExportMatrix,
) -> dict[str, Any]:
    """Serialize a customer data export matrix to a plain dictionary."""
    return matrix.to_dict()


plan_customer_data_export_matrix_to_dict.__test__ = False


def plan_customer_data_export_matrix_to_markdown(
    matrix: PlanCustomerDataExportMatrix,
) -> str:
    """Render a customer data export matrix as Markdown."""
    return matrix.to_markdown()


plan_customer_data_export_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    export_surfaces: tuple[CustomerDataExportSurface, ...] = field(default_factory=tuple)
    data_scopes: tuple[CustomerDataExportScope, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actor_or_owner: str | None = None
    format_or_destination: str | None = None
    privacy_safeguards: tuple[str, ...] = field(default_factory=tuple)


def _task_rows(task: Mapping[str, Any], index: int) -> tuple[PlanCustomerDataExportMatrixRow, ...]:
    signals = _signals(task)
    if not signals.export_surfaces:
        return ()

    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    rows: list[PlanCustomerDataExportMatrixRow] = []
    for export_surface in signals.export_surfaces:
        data_scope = _data_scope_for_surface(export_surface, signals.data_scopes)
        missing_fields = _missing_fields(data_scope, signals)
        rows.append(
            PlanCustomerDataExportMatrixRow(
                task_id=task_id,
                title=title,
                export_surface=export_surface,
                data_scope=data_scope,
                actor_or_owner=signals.actor_or_owner,
                format_or_destination=signals.format_or_destination,
                privacy_safeguards=signals.privacy_safeguards,
                missing_fields=missing_fields,
                evidence=signals.evidence,
                recommendation=_recommendation(export_surface, data_scope, missing_fields),
            )
        )
    return tuple(rows)


def _signals(task: Mapping[str, Any]) -> _Signals:
    export_surfaces: set[CustomerDataExportSurface] = set()
    data_scopes: set[CustomerDataExportScope] = set()
    evidence: list[str] = []
    formats: list[str] = []
    safeguards: list[str] = []

    for source_field, text in _candidate_texts(task):
        if _negated_export_text(text):
            continue
        snippet = _evidence_snippet(source_field, text)
        matched_surface = False
        for surface, pattern in _EXPORT_PATTERNS.items():
            if pattern.search(text):
                export_surfaces.add(surface)
                matched_surface = True
        if matched_surface or _EXPORT_SIGNAL_RE.search(text):
            evidence.append(snippet)
        for scope, pattern in _DATA_SCOPE_PATTERNS.items():
            if pattern.search(text):
                data_scopes.add(scope)
                if matched_surface or _EXPORT_SIGNAL_RE.search(text):
                    evidence.append(snippet)
        formats.extend(_formats_in_text(text))
        safeguards.extend(_safeguards_in_text(text))

    if not export_surfaces or _docs_only_without_export_intent(task, evidence):
        return _Signals()

    if "file_download" in export_surfaces and not (_has_customer_scope(data_scopes) or evidence):
        return _Signals()

    return _Signals(
        export_surfaces=tuple(surface for surface in _EXPORT_ORDER if surface in export_surfaces),
        data_scopes=tuple(scope for scope in _SCOPE_ORDER if scope in data_scopes),
        evidence=tuple(_dedupe(evidence)),
        actor_or_owner=_actor_or_owner(task),
        format_or_destination=_format_or_destination(formats),
        privacy_safeguards=tuple(_dedupe(safeguards)),
    )


def _missing_fields(data_scope: CustomerDataExportScope, signals: _Signals) -> tuple[str, ...]:
    missing: list[str] = []
    if data_scope == "unknown_customer_data":
        missing.append("data_scope")
    if not signals.actor_or_owner:
        missing.append("actor_or_owner")
    if not signals.format_or_destination:
        missing.append("format_or_destination")
    if not signals.privacy_safeguards:
        missing.append("privacy_safeguards")
    return tuple(field_name for field_name in _MISSING_FIELD_ORDER if field_name in missing)


def _data_scope_for_surface(
    export_surface: CustomerDataExportSurface,
    data_scopes: tuple[CustomerDataExportScope, ...],
) -> CustomerDataExportScope:
    if data_scopes:
        return data_scopes[0]
    if export_surface == "audit_export":
        return "audit_records"
    if export_surface == "reporting_export":
        return "reporting_data"
    return "unknown_customer_data"


def _recommendation(
    export_surface: CustomerDataExportSurface,
    data_scope: CustomerDataExportScope,
    missing_fields: tuple[str, ...],
) -> str:
    parts = [
        f"Document {export_surface.replace('_', ' ')} planning for {data_scope.replace('_', ' ')}."
    ]
    if missing_fields:
        parts.append(f"Add missing fields: {', '.join(missing_fields)}.")
    if export_surface in {"bulk_export", "admin_export", "audit_export"}:
        parts.append("Include approval, access logging, and least-privilege ownership.")
    if export_surface in {"redacted_export", "reporting_export", "file_download"}:
        parts.append("Specify redaction rules, generated format, and retention for produced files.")
    if export_surface == "retention_bound_export":
        parts.append("Tie exported artifact expiry to the source data retention policy.")
    return " ".join(parts)


def _actor_or_owner(task: Mapping[str, Any]) -> str | None:
    for field_name in ("owner_type", "actor", "owner"):
        if text := _optional_text(task.get(field_name)):
            return text
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "owner",
            "owner_hint",
            "team",
            "service_owner",
            "data_owner",
            "privacy_owner",
            "actor",
        ):
            if text := _optional_text(metadata.get(key)):
                return text
    for source_field, text in _metadata_texts(task.get("metadata")):
        if _OWNER_KEY_RE.search(source_field):
            if match := re.search(
                r"\b(?:owner|actor|admin|customer|tenant admin|support|privacy|data steward)\b(?:\s*[:=-]\s*)?([\w .@/-]{2,50})?",
                text,
                re.I,
            ):
                value = _optional_text(match.group(1))
                return value or match.group(0)
    return None


def _formats_in_text(text: str) -> list[str]:
    return [match.group(0).lower() for match in _FORMAT_RE.finditer(text)]


def _safeguards_in_text(text: str) -> list[str]:
    safeguards: list[str] = []
    for match in _SAFEGUARD_RE.finditer(text):
        value = match.group(0).lower()
        if value.startswith("redact"):
            safeguards.append("redaction")
        elif value.startswith("mask"):
            safeguards.append("masking")
        elif value.startswith("encrypt"):
            safeguards.append("encryption")
        elif value in {"expire", "ttl", "retention"} or "delete exported" in value:
            safeguards.append("retention bounds")
        elif value in {"approval", "privacy review"}:
            safeguards.append("approval")
        elif value == "audit":
            safeguards.append("audit logging")
        else:
            safeguards.append(value)
    return safeguards


def _format_or_destination(formats: Iterable[str]) -> str | None:
    ordered = _dedupe(formats)
    return ", ".join(ordered) if ordered else None


def _has_customer_scope(data_scopes: set[CustomerDataExportScope]) -> bool:
    return bool(data_scopes & {"personal_data", "customer_data", "tenant_data", "audit_records"})


def _negated_export_text(text: str) -> bool:
    return (
        _EXPORT_SIGNAL_RE.search(text) is not None and _NEGATED_EXPORT_RE.search(text) is not None
    )


def _docs_only_without_export_intent(task: Mapping[str, Any], evidence: list[str]) -> bool:
    joined = " ".join(text for _, text in _candidate_texts(task))
    if not _LOW_RISK_DOC_RE.search(joined):
        return False
    risk = (_optional_text(task.get("risk_level")) or "").casefold()
    title = (_optional_text(task.get("title")) or "").casefold()
    if risk == "low" or re.search(r"\b(?:docs?|documentation|copy) only\b", joined, re.I):
        return True
    if title.startswith(
        ("document ", "refresh ", "update docs", "update documentation", "readme", "runbook")
    ):
        return not re.search(
            r"\b(?:build|add|implement|create|generate|enable|ship)\b", joined, re.I
        )
    return not evidence


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
    for field_name in (
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if _any_signal(key_text):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _any_signal(key_text):
                texts.append((field, str(key)))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _any_signal(text: str) -> bool:
    return (
        _EXPORT_SIGNAL_RE.search(text) is not None
        or any(pattern.search(text) for pattern in _EXPORT_PATTERNS.values())
        or any(pattern.search(text) for pattern in _DATA_SCOPE_PATTERNS.values())
    )


def _prebuilt_matrix(source: object) -> PlanCustomerDataExportMatrix | None:
    if isinstance(source, PlanCustomerDataExportMatrix):
        return source
    if not isinstance(source, Mapping) or "rows" not in source:
        return None
    rows_value = source.get("rows")
    if not isinstance(rows_value, (list, tuple)):
        return None
    rows = tuple(_row_from_mapping(row) for row in rows_value if isinstance(row, Mapping))
    export_task_ids = source.get("export_task_ids")
    no_signal_task_ids = source.get("no_signal_task_ids")
    summary = source.get("summary")
    return PlanCustomerDataExportMatrix(
        plan_id=_optional_text(source.get("plan_id")) or _optional_text(source.get("id")),
        rows=rows,
        export_task_ids=(
            tuple(_strings(export_task_ids))
            if export_task_ids is not None
            else tuple(_dedupe(row.task_id for row in rows))
        ),
        no_signal_task_ids=tuple(_strings(no_signal_task_ids)),
        summary=dict(summary) if isinstance(summary, Mapping) else _summary_for_rows(rows, ()),
    )


def _row_from_mapping(row: Mapping[str, Any]) -> PlanCustomerDataExportMatrixRow:
    task_id = _optional_text(row.get("task_id")) or ""
    title = _optional_text(row.get("title")) or task_id
    export_surface = _literal_or_default(row.get("export_surface"), _EXPORT_ORDER, "file_download")
    data_scope = _literal_or_default(row.get("data_scope"), _SCOPE_ORDER, "unknown_customer_data")
    return PlanCustomerDataExportMatrixRow(
        task_id=task_id,
        title=title,
        export_surface=export_surface,
        data_scope=data_scope,
        actor_or_owner=_optional_text(row.get("actor_or_owner")),
        format_or_destination=_optional_text(row.get("format_or_destination")),
        privacy_safeguards=tuple(_strings(row.get("privacy_safeguards"))),
        missing_fields=tuple(_strings(row.get("missing_fields"))),
        evidence=tuple(_strings(row.get("evidence"))),
        recommendation=_optional_text(row.get("recommendation")) or "",
    )


def _literal_or_default(value: Any, allowed: Mapping[_T, int], default: _T) -> _T:
    text = _optional_text(value)
    return text if text in allowed else default  # type: ignore[return-value]


def _summary_for_rows(
    rows: tuple[PlanCustomerDataExportMatrixRow, ...],
    no_signal_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    export_task_ids = tuple(_dedupe(row.task_id for row in rows))
    return {
        "task_count": len(export_task_ids) + len(no_signal_task_ids),
        "row_count": len(rows),
        "export_task_count": len(export_task_ids),
        "no_signal_task_count": len(no_signal_task_ids),
        "export_surface_counts": {
            surface: sum(1 for row in rows if row.export_surface == surface)
            for surface in _EXPORT_ORDER
        },
        "missing_field_counts": {
            field_name: sum(1 for row in rows if field_name in row.missing_fields)
            for field_name in _MISSING_FIELD_ORDER
        },
        "missing_field_total": sum(len(row.missing_fields) for row in rows),
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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
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
        "tags",
        "labels",
        "notes",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "CustomerDataExportScope",
    "CustomerDataExportSurface",
    "PlanCustomerDataExportMatrix",
    "PlanCustomerDataExportMatrixRow",
    "build_plan_customer_data_export_matrix",
    "derive_plan_customer_data_export_matrix",
    "generate_plan_customer_data_export_matrix",
    "plan_customer_data_export_matrix_to_dict",
    "plan_customer_data_export_matrix_to_markdown",
    "summarize_plan_customer_data_export_matrix",
]
