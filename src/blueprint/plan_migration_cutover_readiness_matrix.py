"""Build plan-level migration cutover readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


MigrationCutoverReadiness = Literal["ready", "partial", "blocked"]
MigrationCutoverSeverity = Literal["high", "medium", "low"]

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[MigrationCutoverReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_SEVERITY_ORDER: dict[MigrationCutoverSeverity, int] = {"high": 0, "medium": 1, "low": 2}
_CUTOVER_RE = re.compile(
    r"\b(?:cutover|cut over|switchover|switch over|dual[- ]write|dual[- ]read|shadow read|"
    r"traffic shift|traffic shifting|migration validation|post[- ]cutover|new system|old system|"
    r"read switch|write switch|promotion window|dependency freeze)\b",
    re.I,
)
_WINDOW_RE = re.compile(r"\b(?:window|maintenance window|switchover window|cutover time|scheduled|freeze window|t\+|by \d+)\b", re.I)
_FREEZE_RE = re.compile(r"\b(?:dependency freeze|code freeze|schema freeze|write freeze|change freeze|freeze dependencies|lock dependencies)\b", re.I)
_VALIDATION_RE = re.compile(
    r"\b(?:validation|validate|verification|verify|gate|gates|smoke test|synthetic|checksum|"
    r"row count|parity|reconciliation|shadow read|compare|acceptance gate)\b",
    re.I,
)
_ROLLBACK_RE = re.compile(r"\b(?:rollback|roll back|revert|fallback|abort|restore|rollback trigger|backout|undo)\b", re.I)
_OWNER_RE = re.compile(
    r"\b(?:owner|owners|dri|responsible|assignee|comms owner|communication owner|lead|"
    r"incident commander|on[- ]?call|stakeholder|support lead|team)\b",
    re.I,
)
_MONITORING_RE = re.compile(r"\b(?:monitor(?:ing)?|metric|metrics|alert|dashboard|slo|health check|watch|post[- ]cutover monitoring|telemetry)\b", re.I)
_CLEANUP_RE = re.compile(r"\b(?:cleanup|clean up|post[- ]cutover cleanup|remove old|decommission|retire|delete old|follow[- ]up|drain)\b", re.I)
_SURFACE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:cutover|switchover|migrat(?:e|ion)|traffic shift)\s+(?:for|of|to)?\s*[`'\"]?([a-z0-9][\w./:-]{2,})", re.I),
    re.compile(r"\b[`'\"]?([a-z0-9][\w./:-]{2,})[`'\"]?\s+(?:cutover|switchover|migration|post[- ]cutover monitoring)", re.I),
)


@dataclass(frozen=True, slots=True)
class PlanMigrationCutoverReadinessRow:
    """One grouped operational migration cutover readiness row."""

    cutover_surface: str
    task_ids: tuple[str, ...]
    titles: tuple[str, ...]
    cutover_window: str = "missing"
    dependency_freeze: str = "missing"
    validation_gates: str = "missing"
    rollback_trigger: str = "missing"
    communication_owner: str = "missing"
    monitoring: str = "missing"
    post_cutover_cleanup: str = "missing"
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: MigrationCutoverReadiness = "partial"
    severity: MigrationCutoverSeverity = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "cutover_surface": self.cutover_surface,
            "task_ids": list(self.task_ids),
            "titles": list(self.titles),
            "cutover_window": self.cutover_window,
            "dependency_freeze": self.dependency_freeze,
            "validation_gates": self.validation_gates,
            "rollback_trigger": self.rollback_trigger,
            "communication_owner": self.communication_owner,
            "monitoring": self.monitoring,
            "post_cutover_cleanup": self.post_cutover_cleanup,
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "severity": self.severity,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanMigrationCutoverReadinessMatrix:
    """Plan-level migration cutover readiness matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanMigrationCutoverReadinessRow, ...] = field(default_factory=tuple)
    cutover_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_cutover_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanMigrationCutoverReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "cutover_task_ids": list(self.cutover_task_ids),
            "no_cutover_task_ids": list(self.no_cutover_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the migration cutover readiness matrix as deterministic Markdown."""
        title = "# Plan Migration Cutover Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('cutover_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks require cutover readiness "
                f"(blocked: {readiness_counts.get('blocked', 0)}, "
                f"partial: {readiness_counts.get('partial', 0)}, "
                f"ready: {readiness_counts.get('ready', 0)}; "
                f"high: {severity_counts.get('high', 0)}, "
                f"medium: {severity_counts.get('medium', 0)}, "
                f"low: {severity_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No migration cutover readiness rows were inferred."])
            if self.no_cutover_task_ids:
                lines.extend(["", f"No cutover signals: {_markdown_cell(', '.join(self.no_cutover_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Cutover Surface | Tasks | Titles | Window | Freeze | Validation | Rollback | "
                    "Communication Owner | Monitoring | Cleanup | Readiness | Severity | Gaps | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.cutover_surface)} | "
                f"{_markdown_cell(', '.join(row.task_ids))} | "
                f"{_markdown_cell('; '.join(row.titles))} | "
                f"{row.cutover_window} | {row.dependency_freeze} | {row.validation_gates} | "
                f"{row.rollback_trigger} | {row.communication_owner} | {row.monitoring} | "
                f"{row.post_cutover_cleanup} | {row.readiness} | {row.severity} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.no_cutover_task_ids:
            lines.extend(["", f"No cutover signals: {_markdown_cell(', '.join(self.no_cutover_task_ids))}"])
        return "\n".join(lines)


def build_plan_migration_cutover_readiness_matrix(source: Any) -> PlanMigrationCutoverReadinessMatrix:
    """Build grouped operational migration cutover readiness for an execution plan."""
    plan_id, tasks = _source_payload(source)
    grouped: dict[str, list[_TaskCutoverSignals]] = {}
    no_cutover_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        signals = _task_signals(task, index)
        if not signals.has_cutover:
            no_cutover_task_ids.append(signals.task_id)
            continue
        grouped.setdefault(signals.cutover_surface, []).append(signals)

    rows = tuple(sorted((_row_from_group(surface, values) for surface, values in grouped.items()), key=_row_sort_key))
    cutover_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.task_ids))
    return PlanMigrationCutoverReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        cutover_task_ids=cutover_task_ids,
        no_cutover_task_ids=tuple(no_cutover_task_ids),
        summary=_summary(len(tasks), rows, no_cutover_task_ids),
    )


def generate_plan_migration_cutover_readiness_matrix(source: Any) -> PlanMigrationCutoverReadinessMatrix:
    """Generate a migration cutover readiness matrix from a plan-like source."""
    return build_plan_migration_cutover_readiness_matrix(source)


def analyze_plan_migration_cutover_readiness_matrix(source: Any) -> PlanMigrationCutoverReadinessMatrix:
    """Analyze an execution plan for migration cutover readiness."""
    if isinstance(source, PlanMigrationCutoverReadinessMatrix):
        return source
    return build_plan_migration_cutover_readiness_matrix(source)


def derive_plan_migration_cutover_readiness_matrix(source: Any) -> PlanMigrationCutoverReadinessMatrix:
    """Derive a migration cutover readiness matrix from a plan-like source."""
    return analyze_plan_migration_cutover_readiness_matrix(source)


def extract_plan_migration_cutover_readiness_matrix(source: Any) -> PlanMigrationCutoverReadinessMatrix:
    """Extract a migration cutover readiness matrix from a plan-like source."""
    return derive_plan_migration_cutover_readiness_matrix(source)


def summarize_plan_migration_cutover_readiness_matrix(
    source: PlanMigrationCutoverReadinessMatrix | Iterable[PlanMigrationCutoverReadinessRow] | Any,
) -> dict[str, Any] | PlanMigrationCutoverReadinessMatrix:
    """Return deterministic summary counts for a matrix, row iterable, or source."""
    if isinstance(source, PlanMigrationCutoverReadinessMatrix):
        return dict(source.summary)
    if _looks_like_plan(source) or _looks_like_task(source) or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_migration_cutover_readiness_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows, ())


def plan_migration_cutover_readiness_matrix_to_dict(
    matrix: PlanMigrationCutoverReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a migration cutover readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_migration_cutover_readiness_matrix_to_dict.__test__ = False


def plan_migration_cutover_readiness_matrix_to_dicts(
    matrix: PlanMigrationCutoverReadinessMatrix | Iterable[PlanMigrationCutoverReadinessRow],
) -> list[dict[str, Any]]:
    """Serialize migration cutover rows to plain dictionaries."""
    if isinstance(matrix, PlanMigrationCutoverReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_migration_cutover_readiness_matrix_to_dicts.__test__ = False


def plan_migration_cutover_readiness_matrix_to_markdown(
    matrix: PlanMigrationCutoverReadinessMatrix,
) -> str:
    """Render a migration cutover readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_migration_cutover_readiness_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskCutoverSignals:
    task_id: str
    title: str
    cutover_surface: str
    statuses: dict[str, str]
    gaps: tuple[str, ...]
    evidence: tuple[str, ...]
    has_cutover: bool


def _task_signals(task: Mapping[str, Any], index: int) -> _TaskCutoverSignals:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    has_cutover = bool(_CUTOVER_RE.search(context) or _path_cutover_signal(texts))
    statuses = {
        "cutover_window": _status(_WINDOW_RE, texts),
        "dependency_freeze": _status(_FREEZE_RE, texts),
        "validation_gates": _status(_VALIDATION_RE, texts, skip_fields=("id",)),
        "rollback_trigger": _status(_ROLLBACK_RE, texts),
        "communication_owner": _status(_OWNER_RE, texts, skip_fields=("id",)),
        "monitoring": _status(_MONITORING_RE, texts, skip_fields=("id",)),
        "post_cutover_cleanup": _status(_CLEANUP_RE, texts),
    }
    gaps = [
        f"Missing {label}."
        for field_name, label in (
            ("cutover_window", "cutover window"),
            ("dependency_freeze", "dependency freeze"),
            ("validation_gates", "validation gates"),
            ("rollback_trigger", "rollback trigger"),
            ("communication_owner", "communication owner"),
            ("monitoring", "monitoring criteria"),
            ("post_cutover_cleanup", "post-cutover cleanup"),
        )
        if statuses[field_name] == "missing"
    ]
    return _TaskCutoverSignals(
        task_id=task_id,
        title=title,
        cutover_surface=_cutover_surface(texts) or "unspecified_cutover_surface",
        statuses=statuses,
        gaps=tuple(_dedupe(gaps)),
        evidence=tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _cutover_evidence_match(text))),
        has_cutover=has_cutover,
    )


def _row_from_group(surface: str, signals: list[_TaskCutoverSignals]) -> PlanMigrationCutoverReadinessRow:
    statuses = {
        field_name: "present" if any(signal.statuses[field_name] == "present" for signal in signals) else "missing"
        for field_name in (
            "cutover_window",
            "dependency_freeze",
            "validation_gates",
            "rollback_trigger",
            "communication_owner",
            "monitoring",
            "post_cutover_cleanup",
        )
    }
    gaps = tuple(
        _dedupe(
            gap
            for signal in signals
            for gap in signal.gaps
            if statuses[_gap_field(gap)] == "missing"
        )
    )
    readiness = _readiness(statuses)
    return PlanMigrationCutoverReadinessRow(
        cutover_surface=surface,
        task_ids=tuple(_dedupe(signal.task_id for signal in signals)),
        titles=tuple(_dedupe(signal.title for signal in signals)),
        gaps=gaps,
        readiness=readiness,
        severity=_severity(readiness),
        evidence=tuple(_dedupe(item for signal in signals for item in signal.evidence)),
        **statuses,
    )


def _gap_field(gap: str) -> str:
    if "window" in gap:
        return "cutover_window"
    if "freeze" in gap:
        return "dependency_freeze"
    if "validation" in gap:
        return "validation_gates"
    if "rollback" in gap:
        return "rollback_trigger"
    if "owner" in gap:
        return "communication_owner"
    if "monitoring" in gap:
        return "monitoring"
    if "cleanup" in gap:
        return "post_cutover_cleanup"
    return "validation_gates"


def _readiness(statuses: Mapping[str, str]) -> MigrationCutoverReadiness:
    if any(statuses[field_name] == "missing" for field_name in ("validation_gates", "rollback_trigger", "communication_owner", "monitoring")):
        return "blocked"
    if any(value == "missing" for value in statuses.values()):
        return "partial"
    return "ready"


def _severity(readiness: MigrationCutoverReadiness) -> MigrationCutoverSeverity:
    return {"blocked": "high", "partial": "medium", "ready": "low"}[readiness]


def _summary(
    task_count: int,
    rows: Iterable[PlanMigrationCutoverReadinessRow],
    no_cutover_task_ids: Iterable[str],
) -> dict[str, Any]:
    row_list = list(rows)
    no_cutover_ids = tuple(no_cutover_task_ids)
    cutover_task_ids = tuple(_dedupe(task_id for row in row_list for task_id in row.task_ids))
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "cutover_task_count": len(cutover_task_ids),
        "no_cutover_task_count": len(no_cutover_ids),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "severity_counts": {
            severity: sum(1 for row in row_list if row.severity == severity)
            for severity in _SEVERITY_ORDER
        },
        "gap_counts": {
            gap: sum(1 for row in row_list if gap in row.gaps)
            for gap in sorted({gap for row in row_list for gap in row.gaps})
        },
        "surface_counts": {
            surface: sum(1 for row in row_list if row.cutover_surface == surface)
            for surface in sorted({row.cutover_surface for row in row_list})
        },
    }


def _row_sort_key(row: PlanMigrationCutoverReadinessRow) -> tuple[int, int, str, str]:
    return (
        _SEVERITY_ORDER[row.severity],
        _READINESS_ORDER[row.readiness],
        row.cutover_surface,
        ",".join(row.task_ids),
    )


def _status(
    pattern: re.Pattern[str],
    texts: Iterable[tuple[str, str]],
    *,
    skip_fields: tuple[str, ...] = (),
) -> str:
    return "present" if any(field not in skip_fields and pattern.search(text) for field, text in texts) else "missing"


def _path_cutover_signal(texts: Iterable[tuple[str, str]]) -> bool:
    return any(field.startswith("files") and re.search(r"(?:^|/)(?:migrations?|cutovers?|traffic|runbooks?)(?:/|$)", text, re.I) for field, text in texts)


def _cutover_surface(texts: Iterable[tuple[str, str]]) -> str | None:
    for field, text in texts:
        for pattern in _SURFACE_PATTERNS:
            match = pattern.search(text)
            if match:
                candidate = _normalise_surface(match.group(1))
                if candidate and candidate not in {
                    "for",
                    "the",
                    "cutover",
                    "migration",
                    "switchover",
                    "monitoring",
                    "window",
                    "validation",
                    "traffic",
                }:
                    return candidate
        if field.startswith("files"):
            parts = [part for part in re.split(r"[/\\]", text) if part]
            for part in reversed(parts):
                if not re.search(r"\.(?:py|ts|tsx|js|md|sql|yaml|yml)$", part, re.I):
                    return _normalise_surface(part)
    return None


def _normalise_surface(value: str) -> str:
    text = _text(value).strip("`'\".,;:()[]{}")
    text = re.sub(r"[^a-zA-Z0-9./:-]+", "_", text)
    return text.strip("_").lower()


def _cutover_evidence_match(text: str) -> bool:
    return any(
        pattern.search(text)
        for pattern in (
            _CUTOVER_RE,
            _WINDOW_RE,
            _FREEZE_RE,
            _VALIDATION_RE,
            _ROLLBACK_RE,
            _OWNER_RE,
            _MONITORING_RE,
            _CLEANUP_RE,
        )
    )


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
    return None, _task_payloads(iterator)


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    if value is None:
        return tasks
    for item in value:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            payload = item.model_dump(mode="python")
            if isinstance(payload, Mapping):
                tasks.append(dict(payload))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _candidate_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for key in ("id", "title", "description", "milestone", "owner_type", "risk_level", "test_command", "blocked_reason"):
        value = _optional_text(task.get(key))
        if value:
            texts.append((key, value))
    for key in ("depends_on", "dependencies", "files_or_modules", "acceptance_criteria", "tags", "validation_commands"):
        for idx, value in enumerate(_strings(task.get(key))):
            texts.append((f"{key}[{idx}]", value))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key, value in sorted(metadata.items()):
            for idx, item in enumerate(_strings(value)):
                texts.append((f"metadata.{key}" if idx == 0 else f"metadata.{key}[{idx}]", item))
    return tuple(texts)


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (_text(value),) if _text(value) else ()
    if isinstance(value, Mapping):
        return tuple(_text(f"{key}: {item}") for key, item in value.items() if _text(item))
    if isinstance(value, Iterable):
        return tuple(_text(item) for item in value if _text(item))
    text = _text(value)
    return (text,) if text else ()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value).strip())


def _evidence_snippet(field: str, text: str) -> str:
    return f"{field}: {_text(text)[:220]}"


def _dedupe(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = _text(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return tuple(result)


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


def _object_payload(value: object) -> dict[str, Any]:
    return {
        key: getattr(value, key)
        for key in dir(value)
        if not key.startswith("_") and not callable(getattr(value, key))
    }


def _looks_like_plan(value: Any) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: Any) -> bool:
    return not isinstance(value, (str, bytes)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )
