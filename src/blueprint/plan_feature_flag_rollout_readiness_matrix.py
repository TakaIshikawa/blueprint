"""Build plan-level feature flag rollout readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FeatureFlagRolloutReadiness = Literal["ready", "partial", "blocked"]
FeatureFlagRolloutSeverity = Literal["high", "medium", "low"]

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[FeatureFlagRolloutReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_SEVERITY_ORDER: dict[FeatureFlagRolloutSeverity, int] = {"high": 0, "medium": 1, "low": 2}
_ROLLOUT_RE = re.compile(
    r"\b(?:feature flag|feature[- ]flag|flagged|flag gate|gradual rollout|rollout|roll out|"
    r"experiment|a/b|ab test|canary|beta|kill[- ]switch|dark launch|percentage rollout|"
    r"traffic ramp|ramp[- ]up|cohort|variant)\b",
    re.I,
)
_OWNER_RE = re.compile(r"\b(?:owner|owners|dri|responsible|assignee|team|lead|pm|product|sre|on[- ]?call)\b", re.I)
_TARGETING_RE = re.compile(
    r"\b(?:target(?:ing)?|cohort|segment|audience|allowlist|denylist|tenant|customer|"
    r"percentage|percent|traffic|ramp|variant|beta users?|internal users?|region)\b",
    re.I,
)
_MONITORING_RE = re.compile(
    r"\b(?:monitor(?:ing)?|metric|metrics|alert|dashboard|slo|health check|synthetic|"
    r"telemetry|observability|error rate|latency|conversion|watch)\b",
    re.I,
)
_ROLLBACK_RE = re.compile(r"\b(?:rollback|roll back|revert|fallback|abort|undo|restore)\b", re.I)
_KILL_SWITCH_RE = re.compile(r"\b(?:kill[- ]switch|emergency off|circuit breaker|instant disable|global off|shutoff)\b", re.I)
_CLEANUP_RE = re.compile(
    r"\b(?:cleanup|clean up|remove flag|delete flag|flag removal|decommission|sunset|"
    r"retire|remove experiment|follow[- ]up)\b",
    re.I,
)
_EXPLICIT_GAP_RE = re.compile(r"\b(?:gap|missing|unknown|unresolved|tbd|todo|not documented|not defined)\b", re.I)
_SURFACE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:feature[- ]flag|feature flag|flag|kill[- ]switch|experiment|canary|beta)\s+[`'\"]?([a-z0-9][\w./:-]{2,})", re.I),
    re.compile(r"\b[`'\"]([a-z0-9][\w./:-]{2,})[`'\"]\s+(?:feature[- ]flag|feature flag|flag|experiment|canary|beta)", re.I),
    re.compile(r"\b(?:roll(?:out| out)|ramp|launch)\s+(?:the\s+)?[`'\"]?([a-z0-9][\w./:-]{2,})", re.I),
)


@dataclass(frozen=True, slots=True)
class PlanFeatureFlagRolloutReadinessRow:
    """One grouped rollout surface readiness row."""

    rollout_surface: str
    task_ids: tuple[str, ...]
    titles: tuple[str, ...]
    owner: str = "missing"
    targeting: str = "missing"
    monitoring: str = "missing"
    rollback: str = "missing"
    kill_switch: str = "missing"
    cleanup: str = "missing"
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: FeatureFlagRolloutReadiness = "partial"
    severity: FeatureFlagRolloutSeverity = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "rollout_surface": self.rollout_surface,
            "task_ids": list(self.task_ids),
            "titles": list(self.titles),
            "owner": self.owner,
            "targeting": self.targeting,
            "monitoring": self.monitoring,
            "rollback": self.rollback,
            "kill_switch": self.kill_switch,
            "cleanup": self.cleanup,
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "severity": self.severity,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanFeatureFlagRolloutReadinessMatrix:
    """Plan-level feature flag rollout readiness matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanFeatureFlagRolloutReadinessRow, ...] = field(default_factory=tuple)
    rollout_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_rollout_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanFeatureFlagRolloutReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "rollout_task_ids": list(self.rollout_task_ids),
            "no_rollout_task_ids": list(self.no_rollout_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the feature flag rollout readiness matrix as deterministic Markdown."""
        title = "# Plan Feature Flag Rollout Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('rollout_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks require rollout readiness "
                f"(blocked: {readiness_counts.get('blocked', 0)}, "
                f"partial: {readiness_counts.get('partial', 0)}, "
                f"ready: {readiness_counts.get('ready', 0)}; "
                f"high: {severity_counts.get('high', 0)}, "
                f"medium: {severity_counts.get('medium', 0)}, "
                f"low: {severity_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No feature flag rollout readiness rows were inferred."])
            if self.no_rollout_task_ids:
                lines.extend(["", f"No rollout signals: {_markdown_cell(', '.join(self.no_rollout_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Rollout Surface | Tasks | Titles | Owner | Targeting | Monitoring | Rollback | "
                    "Kill Switch | Cleanup | Readiness | Severity | Gaps | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.rollout_surface)} | "
                f"{_markdown_cell(', '.join(row.task_ids))} | "
                f"{_markdown_cell('; '.join(row.titles))} | "
                f"{row.owner} | {row.targeting} | {row.monitoring} | {row.rollback} | "
                f"{row.kill_switch} | {row.cleanup} | {row.readiness} | {row.severity} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.no_rollout_task_ids:
            lines.extend(["", f"No rollout signals: {_markdown_cell(', '.join(self.no_rollout_task_ids))}"])
        return "\n".join(lines)


def build_plan_feature_flag_rollout_readiness_matrix(source: Any) -> PlanFeatureFlagRolloutReadinessMatrix:
    """Build grouped feature flag rollout readiness for an execution plan."""
    plan_id, tasks = _source_payload(source)
    grouped: dict[str, list[_TaskRolloutSignals]] = {}
    no_rollout_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        signals = _task_signals(task, index)
        if not signals.has_rollout:
            no_rollout_task_ids.append(signals.task_id)
            continue
        grouped.setdefault(signals.rollout_surface, []).append(signals)

    rows = tuple(sorted((_row_from_group(surface, values) for surface, values in grouped.items()), key=_row_sort_key))
    rollout_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.task_ids))
    return PlanFeatureFlagRolloutReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        rollout_task_ids=rollout_task_ids,
        no_rollout_task_ids=tuple(no_rollout_task_ids),
        summary=_summary(len(tasks), rows, no_rollout_task_ids),
    )


def generate_plan_feature_flag_rollout_readiness_matrix(source: Any) -> PlanFeatureFlagRolloutReadinessMatrix:
    """Generate a feature flag rollout readiness matrix from a plan-like source."""
    return build_plan_feature_flag_rollout_readiness_matrix(source)


def analyze_plan_feature_flag_rollout_readiness_matrix(source: Any) -> PlanFeatureFlagRolloutReadinessMatrix:
    """Analyze an execution plan for feature flag rollout readiness."""
    if isinstance(source, PlanFeatureFlagRolloutReadinessMatrix):
        return source
    return build_plan_feature_flag_rollout_readiness_matrix(source)


def derive_plan_feature_flag_rollout_readiness_matrix(source: Any) -> PlanFeatureFlagRolloutReadinessMatrix:
    """Derive a feature flag rollout readiness matrix from a plan-like source."""
    return analyze_plan_feature_flag_rollout_readiness_matrix(source)


def extract_plan_feature_flag_rollout_readiness_matrix(source: Any) -> PlanFeatureFlagRolloutReadinessMatrix:
    """Extract a feature flag rollout readiness matrix from a plan-like source."""
    return derive_plan_feature_flag_rollout_readiness_matrix(source)


def summarize_plan_feature_flag_rollout_readiness_matrix(
    source: PlanFeatureFlagRolloutReadinessMatrix | Iterable[PlanFeatureFlagRolloutReadinessRow] | Any,
) -> dict[str, Any] | PlanFeatureFlagRolloutReadinessMatrix:
    """Return deterministic summary counts for a matrix, row iterable, or source."""
    if isinstance(source, PlanFeatureFlagRolloutReadinessMatrix):
        return dict(source.summary)
    if _looks_like_plan(source) or _looks_like_task(source) or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_feature_flag_rollout_readiness_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows, ())


def plan_feature_flag_rollout_readiness_matrix_to_dict(
    matrix: PlanFeatureFlagRolloutReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a feature flag rollout readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_feature_flag_rollout_readiness_matrix_to_dict.__test__ = False


def plan_feature_flag_rollout_readiness_matrix_to_dicts(
    matrix: PlanFeatureFlagRolloutReadinessMatrix | Iterable[PlanFeatureFlagRolloutReadinessRow],
) -> list[dict[str, Any]]:
    """Serialize feature flag rollout rows to plain dictionaries."""
    if isinstance(matrix, PlanFeatureFlagRolloutReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_feature_flag_rollout_readiness_matrix_to_dicts.__test__ = False


def plan_feature_flag_rollout_readiness_matrix_to_markdown(
    matrix: PlanFeatureFlagRolloutReadinessMatrix,
) -> str:
    """Render a feature flag rollout readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_feature_flag_rollout_readiness_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRolloutSignals:
    task_id: str
    title: str
    rollout_surface: str
    statuses: dict[str, str]
    gaps: tuple[str, ...]
    evidence: tuple[str, ...]
    has_rollout: bool


def _task_signals(task: Mapping[str, Any], index: int) -> _TaskRolloutSignals:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    has_rollout = bool(_ROLLOUT_RE.search(context))
    statuses = {
        "owner": _status(_OWNER_RE, texts, skip_fields=("id",)),
        "targeting": _status(_TARGETING_RE, texts),
        "monitoring": _status(_MONITORING_RE, texts, skip_fields=("id",)),
        "rollback": _status(_ROLLBACK_RE, texts),
        "kill_switch": _status(_KILL_SWITCH_RE, texts),
        "cleanup": _status(_CLEANUP_RE, texts),
    }
    gaps = [
        f"Missing {label}."
        for field_name, label in (
            ("owner", "rollout owner"),
            ("targeting", "targeting or ramp criteria"),
            ("monitoring", "monitoring or alert criteria"),
            ("rollback", "rollback criteria"),
            ("kill_switch", "kill switch criteria"),
            ("cleanup", "flag cleanup criteria"),
        )
        if statuses[field_name] == "missing"
    ]
    gaps.extend(_evidence_snippet(field, text) for field, text in texts if field != "id" and _EXPLICIT_GAP_RE.search(text))
    return _TaskRolloutSignals(
        task_id=task_id,
        title=title,
        rollout_surface=_rollout_surface(texts) or "unspecified_rollout_surface",
        statuses=statuses,
        gaps=tuple(_dedupe(gaps)),
        evidence=tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _rollout_evidence_match(text))),
        has_rollout=has_rollout,
    )


def _row_from_group(surface: str, signals: list[_TaskRolloutSignals]) -> PlanFeatureFlagRolloutReadinessRow:
    statuses = {
        field_name: "present" if any(signal.statuses[field_name] == "present" for signal in signals) else "missing"
        for field_name in ("owner", "targeting", "monitoring", "rollback", "kill_switch", "cleanup")
    }
    gaps = tuple(
        _dedupe(
            gap
            for signal in signals
            for gap in signal.gaps
            if not gap.startswith("Missing ")
            or statuses[_gap_field(gap)] == "missing"
        )
    )
    readiness = _readiness(statuses)
    return PlanFeatureFlagRolloutReadinessRow(
        rollout_surface=surface,
        task_ids=tuple(_dedupe(signal.task_id for signal in signals)),
        titles=tuple(_dedupe(signal.title for signal in signals)),
        gaps=gaps,
        readiness=readiness,
        severity=_severity(readiness),
        evidence=tuple(_dedupe(item for signal in signals for item in signal.evidence)),
        **statuses,
    )


def _gap_field(gap: str) -> str:
    if "owner" in gap:
        return "owner"
    if "targeting" in gap or "ramp" in gap:
        return "targeting"
    if "monitoring" in gap or "alert" in gap:
        return "monitoring"
    if "rollback" in gap:
        return "rollback"
    if "kill switch" in gap:
        return "kill_switch"
    if "cleanup" in gap or "flag cleanup" in gap:
        return "cleanup"
    return "owner"


def _readiness(statuses: Mapping[str, str]) -> FeatureFlagRolloutReadiness:
    if any(statuses[field_name] == "missing" for field_name in ("owner", "monitoring", "rollback", "kill_switch")):
        return "blocked"
    if any(value == "missing" for value in statuses.values()):
        return "partial"
    return "ready"


def _severity(readiness: FeatureFlagRolloutReadiness) -> FeatureFlagRolloutSeverity:
    return {"blocked": "high", "partial": "medium", "ready": "low"}[readiness]


def _summary(
    task_count: int,
    rows: Iterable[PlanFeatureFlagRolloutReadinessRow],
    no_rollout_task_ids: Iterable[str],
) -> dict[str, Any]:
    row_list = list(rows)
    no_rollout_ids = tuple(no_rollout_task_ids)
    rollout_task_ids = tuple(_dedupe(task_id for row in row_list for task_id in row.task_ids))
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "rollout_task_count": len(rollout_task_ids),
        "no_rollout_task_count": len(no_rollout_ids),
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
            surface: sum(1 for row in row_list if row.rollout_surface == surface)
            for surface in sorted({row.rollout_surface for row in row_list})
        },
    }


def _row_sort_key(row: PlanFeatureFlagRolloutReadinessRow) -> tuple[int, int, str, str]:
    return (
        _SEVERITY_ORDER[row.severity],
        _READINESS_ORDER[row.readiness],
        row.rollout_surface,
        ",".join(row.task_ids),
    )


def _status(
    pattern: re.Pattern[str],
    texts: Iterable[tuple[str, str]],
    *,
    skip_fields: tuple[str, ...] = (),
) -> str:
    return "present" if any(field not in skip_fields and pattern.search(text) for field, text in texts) else "missing"


def _rollout_surface(texts: Iterable[tuple[str, str]]) -> str | None:
    for field, text in texts:
        for pattern in _SURFACE_PATTERNS:
            match = pattern.search(text)
            if match:
                candidate = _normalise_surface(match.group(1))
                if candidate and candidate not in {
                    "feature",
                    "flag",
                    "rollout",
                    "experiment",
                    "canary",
                    "beta",
                    "and",
                    "with",
                    "gets",
                    "for",
                    "the",
                }:
                    return candidate
        if field.startswith("files"):
            for part in re.split(r"[/\\]", text):
                if _ROLLOUT_RE.search(part):
                    return _normalise_surface(part)
    return None


def _normalise_surface(value: str) -> str:
    text = _text(value).strip("`'\".,;:()[]{}")
    text = re.sub(r"[^a-zA-Z0-9./:-]+", "_", text)
    return text.strip("_").lower()


def _rollout_evidence_match(text: str) -> bool:
    return any(
        pattern.search(text)
        for pattern in (
            _ROLLOUT_RE,
            _OWNER_RE,
            _TARGETING_RE,
            _MONITORING_RE,
            _ROLLBACK_RE,
            _KILL_SWITCH_RE,
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
