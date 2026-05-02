"""Build shadow-mode comparison readiness matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ShadowModeComparisonField = Literal[
    "baseline_system",
    "candidate_system",
    "traffic_sample_signal",
    "mismatch_tolerance",
    "escalation_owner",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_REQUIRED_FIELDS: tuple[ShadowModeComparisonField, ...] = (
    "baseline_system",
    "candidate_system",
    "traffic_sample_signal",
    "mismatch_tolerance",
    "escalation_owner",
)
_SURFACE_ORDER = {
    "shadow mode": 0,
    "parallel run": 1,
    "dual-run": 2,
    "dual-write comparison": 3,
    "dark reads": 4,
    "mirrored traffic": 5,
    "old-vs-new comparison": 6,
}
_SURFACE_PATTERNS: dict[str, re.Pattern[str]] = {
    "shadow mode": re.compile(r"\bshadow(?:[- ]?mode|ing)?\b|\bshadow traffic\b", re.I),
    "parallel run": re.compile(r"\bparallel[- ]?runs?\b|\brun(?:ning)? in parallel\b", re.I),
    "dual-run": re.compile(r"\bdual[- ]?runs?\b|\bdual run(?:ning)?\b", re.I),
    "dual-write comparison": re.compile(
        r"\bdual[- ]?writes?\b|\bdual[- ]?write comparison\b", re.I
    ),
    "dark reads": re.compile(r"\bdark[- ]?reads?\b|\bdark read comparison\b", re.I),
    "mirrored traffic": re.compile(
        r"\bmirror(?:ed|ing)? traffic\b|\btraffic mirror(?:ing)?\b", re.I
    ),
    "old-vs-new comparison": re.compile(
        r"\bold\s*(?:vs\.?|versus|-)\s*new\b|\bnew\s*(?:vs\.?|versus|-)\s*old\b|"
        r"\bcompare\s+(?:old|legacy|current)\s+(?:and|to|vs\.?)\s+(?:new|candidate)\b|"
        r"\bresult comparison\b",
        re.I,
    ),
}
_SHADOW_RE = re.compile(
    r"\b(?:shadow(?:[- ]?mode|ing)?|shadow traffic|parallel[- ]?run|running in parallel|"
    r"dual[- ]?run|dual[- ]?write|dark[- ]?read|mirror(?:ed|ing)? traffic|"
    r"traffic mirror(?:ing)?|old\s*(?:vs\.?|versus|-)\s*new|"
    r"new\s*(?:vs\.?|versus|-)\s*old|result comparison)\b",
    re.I,
)
_FIELD_KEY_ALIASES: dict[ShadowModeComparisonField, tuple[str, ...]] = {
    "baseline_system": (
        "baseline_system",
        "baseline",
        "control_system",
        "current_system",
        "old_system",
        "legacy_system",
        "source_system",
    ),
    "candidate_system": (
        "candidate_system",
        "candidate",
        "treatment_system",
        "new_system",
        "replacement_system",
        "shadow_system",
        "target_system",
    ),
    "traffic_sample_signal": (
        "traffic_sample_signal",
        "traffic_sample",
        "sample",
        "sample_window",
        "replay_window",
        "traffic_split",
        "mirror_window",
        "read_sample",
    ),
    "mismatch_tolerance": (
        "mismatch_tolerance",
        "tolerance",
        "mismatch_threshold",
        "diff_threshold",
        "comparison_threshold",
        "acceptable_delta",
        "error_budget",
    ),
    "escalation_owner": (
        "escalation_owner",
        "owner",
        "comparison_owner",
        "analysis_owner",
        "dri",
        "team",
        "assignee",
    ),
}
_FIELD_PATTERNS: dict[ShadowModeComparisonField, tuple[re.Pattern[str], ...]] = {
    "baseline_system": (
        re.compile(
            r"\b(?:baseline system|baseline|control system|current system|old system|"
            r"legacy system|source system)\s*[:=-]\s*([^.;\n]+)",
            re.I,
        ),
        re.compile(r"\bcompare\s+([^.;\n]+?)\s+(?:against|to|with|vs\.?)\s+", re.I),
    ),
    "candidate_system": (
        re.compile(
            r"\b(?:candidate system|candidate|treatment system|new system|replacement system|"
            r"shadow system|target system)\s*[:=-]\s*([^.;\n]+)",
            re.I,
        ),
        re.compile(r"\b(?:against|to|with|vs\.?)\s+([^.;\n]+?)\s+(?:for|over|using|on)\b", re.I),
    ),
    "traffic_sample_signal": (
        re.compile(
            r"\b(?:traffic sample|sample window|sample|replay window|traffic split|"
            r"mirror window|read sample|replay)\s*[:=-]\s*([^.;\n]+)",
            re.I,
        ),
        re.compile(
            r"\b((?:\d{1,3}\s*%\s+(?:of\s+)?(?:traffic|requests|reads|writes|users|accounts))|"
            r"(?:for|over)\s+\d+\s+(?:hours?|days?|weeks?)|"
            r"\d+\s+(?:hours?|days?|weeks?)\s+(?:of\s+)?(?:replay|traffic|requests|reads))\b",
            re.I,
        ),
    ),
    "mismatch_tolerance": (
        re.compile(
            r"\b(?:mismatch tolerance|mismatch threshold|diff threshold|comparison threshold|"
            r"acceptable delta|tolerance|error budget|allowable variance)"
            r"\s*[:=-]?\s*((?:\d+\.\d+|[^.;\n])+)",
            re.I,
        ),
    ),
    "escalation_owner": (
        re.compile(
            r"\b(?:escalation owner|comparison owner|analysis owner|owner|dri)"
            r"\s*[:=-]?\s*([^.;\n]+)",
            re.I,
        ),
    ),
}


@dataclass(frozen=True, slots=True)
class PlanShadowModeComparisonMatrixRow:
    """Comparison readiness details for one shadow-mode task."""

    task_id: str
    title: str
    comparison_surface: str
    baseline_system: str = ""
    candidate_system: str = ""
    traffic_sample_signal: str = ""
    mismatch_tolerance: str = ""
    escalation_owner: str = ""
    missing_fields: tuple[ShadowModeComparisonField, ...] = field(default_factory=tuple)
    recommendation: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "comparison_surface": self.comparison_surface,
            "baseline_system": self.baseline_system,
            "candidate_system": self.candidate_system,
            "traffic_sample_signal": self.traffic_sample_signal,
            "mismatch_tolerance": self.mismatch_tolerance,
            "escalation_owner": self.escalation_owner,
            "missing_fields": list(self.missing_fields),
            "recommendation": self.recommendation,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanShadowModeComparisonMatrix:
    """Plan-level shadow-mode comparison matrix."""

    plan_id: str | None = None
    rows: tuple[PlanShadowModeComparisonMatrixRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanShadowModeComparisonMatrixRow, ...]:
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
        """Return shadow comparison rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the shadow comparison matrix as deterministic Markdown."""
        title = "# Plan Shadow Mode Comparison Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Total tasks: {self.summary.get('total_task_count', 0)}",
            f"- Shadow comparison tasks: {self.summary.get('shadow_task_count', 0)}",
            f"- Tasks missing tolerances: {self.summary.get('tasks_missing_tolerances', 0)}",
            f"- Tasks missing owners: {self.summary.get('tasks_missing_owners', 0)}",
            f"- Missing comparison fields: {self.summary.get('missing_field_count', 0)}",
        ]
        if not self.rows:
            lines.extend(["", "No shadow-mode comparison rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                (
                    "| Task | Title | Surface | Baseline | Candidate | Traffic/Sample | "
                    "Mismatch Tolerance | Escalation Owner | Missing Fields | "
                    "Recommendation | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.task_id)} | "
                f"{_markdown_cell(row.title)} | "
                f"{_markdown_cell(row.comparison_surface)} | "
                f"{_markdown_cell(row.baseline_system or 'unspecified')} | "
                f"{_markdown_cell(row.candidate_system or 'unspecified')} | "
                f"{_markdown_cell(row.traffic_sample_signal or 'unspecified')} | "
                f"{_markdown_cell(row.mismatch_tolerance or 'unspecified')} | "
                f"{_markdown_cell(row.escalation_owner or 'unspecified')} | "
                f"{_markdown_cell(', '.join(row.missing_fields) or 'none')} | "
                f"{_markdown_cell(row.recommendation)} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_shadow_mode_comparison_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanShadowModeComparisonMatrix:
    """Build comparison readiness rows for shadow-mode execution tasks."""
    plan_id, tasks = _source_payload(source)
    rows = tuple(
        sorted(
            (
                row
                for index, task in enumerate(tasks, start=1)
                if (row := _row_for_task(task, index)) is not None
            ),
            key=lambda row: (
                len(row.missing_fields),
                _SURFACE_ORDER.get(row.comparison_surface, 99),
                row.task_id,
                row.title.casefold(),
            ),
        )
    )
    return PlanShadowModeComparisonMatrix(
        plan_id=plan_id,
        rows=rows,
        summary=_summary(rows, total_task_count=len(tasks)),
    )


def generate_plan_shadow_mode_comparison_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[PlanShadowModeComparisonMatrixRow, ...]:
    """Return shadow comparison rows for relevant execution tasks."""
    return build_plan_shadow_mode_comparison_matrix(source).rows


def derive_plan_shadow_mode_comparison_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanShadowModeComparisonMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanShadowModeComparisonMatrix:
    """Return an existing matrix or generate one from a plan-shaped source."""
    if isinstance(source, PlanShadowModeComparisonMatrix):
        return source
    return build_plan_shadow_mode_comparison_matrix(source)


def summarize_plan_shadow_mode_comparison_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanShadowModeComparisonMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanShadowModeComparisonMatrix:
    """Compatibility alias for shadow-mode comparison summaries."""
    return derive_plan_shadow_mode_comparison_matrix(source)


def plan_shadow_mode_comparison_matrix_to_dict(
    matrix: PlanShadowModeComparisonMatrix,
) -> dict[str, Any]:
    """Serialize a shadow comparison matrix to a plain dictionary."""
    return matrix.to_dict()


plan_shadow_mode_comparison_matrix_to_dict.__test__ = False


def plan_shadow_mode_comparison_matrix_to_dicts(
    rows: (
        PlanShadowModeComparisonMatrix
        | tuple[PlanShadowModeComparisonMatrixRow, ...]
        | list[PlanShadowModeComparisonMatrixRow]
    ),
) -> list[dict[str, Any]]:
    """Serialize shadow comparison rows to dictionaries."""
    if isinstance(rows, PlanShadowModeComparisonMatrix):
        return rows.to_dicts()
    return [row.to_dict() for row in rows]


plan_shadow_mode_comparison_matrix_to_dicts.__test__ = False


def plan_shadow_mode_comparison_matrix_to_markdown(
    matrix: PlanShadowModeComparisonMatrix,
) -> str:
    """Render a shadow comparison matrix as Markdown."""
    return matrix.to_markdown()


plan_shadow_mode_comparison_matrix_to_markdown.__test__ = False


def _row_for_task(
    task: Mapping[str, Any],
    index: int,
) -> PlanShadowModeComparisonMatrixRow | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    surfaces: dict[str, list[str]] = {}
    fields: dict[ShadowModeComparisonField, list[str]] = {key: [] for key in _REQUIRED_FIELDS}
    evidence: list[str] = []

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        if _SHADOW_RE.search(text):
            for surface, pattern in _SURFACE_PATTERNS.items():
                if pattern.search(text):
                    surfaces.setdefault(surface, []).append(snippet)
        for field_name, patterns in _FIELD_PATTERNS.items():
            for pattern in patterns:
                for match in pattern.finditer(text):
                    value = next((group for group in match.groups() if group), match.group(0))
                    fields[field_name].append(_clean(value))
                    evidence.append(snippet)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for field_name, aliases in _FIELD_KEY_ALIASES.items():
            for source_field, value in _metadata_values(metadata, aliases):
                for item in _strings(value):
                    fields[field_name].insert(0, _clean(item))
                    evidence.append(_evidence_snippet(source_field, item))
        for source_field, text in _metadata_texts(metadata):
            key_text = source_field.replace("_", " ")
            if _SHADOW_RE.search(text) or _SHADOW_RE.search(key_text):
                for surface, pattern in _SURFACE_PATTERNS.items():
                    if pattern.search(text) or pattern.search(key_text):
                        surfaces.setdefault(surface, []).append(
                            _evidence_snippet(source_field, text or key_text)
                        )

    for key in ("owner", "assignee", "dri", "team", "owner_type"):
        if owner := _optional_text(task.get(key)):
            fields["escalation_owner"].append(owner)
            evidence.append(_evidence_snippet(key, owner))

    if not surfaces:
        return None

    comparison_surface = sorted(surfaces, key=lambda item: _SURFACE_ORDER[item])[0]
    baseline_system = _first(fields["baseline_system"])
    candidate_system = _first(fields["candidate_system"])
    traffic_sample_signal = _first(fields["traffic_sample_signal"])
    mismatch_tolerance = _first(fields["mismatch_tolerance"])
    escalation_owner = _first(fields["escalation_owner"])
    missing_fields = tuple(
        field_name
        for field_name in _REQUIRED_FIELDS
        if (
            (field_name == "baseline_system" and not baseline_system)
            or (field_name == "candidate_system" and not candidate_system)
            or (field_name == "traffic_sample_signal" and not traffic_sample_signal)
            or (field_name == "mismatch_tolerance" and not mismatch_tolerance)
            or (field_name == "escalation_owner" and not escalation_owner)
        )
    )
    return PlanShadowModeComparisonMatrixRow(
        task_id=task_id,
        title=title,
        comparison_surface=comparison_surface,
        baseline_system=baseline_system,
        candidate_system=candidate_system,
        traffic_sample_signal=traffic_sample_signal,
        mismatch_tolerance=mismatch_tolerance,
        escalation_owner=escalation_owner,
        missing_fields=missing_fields,
        recommendation=_recommendation(missing_fields),
        evidence=tuple(_dedupe([*surfaces[comparison_surface], *evidence])),
    )


def _recommendation(missing_fields: tuple[ShadowModeComparisonField, ...]) -> str:
    if not missing_fields:
        return (
            "Ready: comparison plan identifies baseline, candidate, replay window, "
            "mismatch tolerance, escalation owner, and rollback or disable action."
        )
    actions = {
        "baseline_system": "name the baseline production system or legacy path",
        "candidate_system": "name the candidate implementation being shadowed",
        "traffic_sample_signal": "add the mirrored traffic sample or replay window",
        "mismatch_tolerance": "define mismatch tolerances and comparison thresholds",
        "escalation_owner": "assign an escalation owner for mismatch triage",
    }
    parts = [actions[field_name] for field_name in missing_fields]
    if "traffic_sample_signal" not in missing_fields:
        parts.append("confirm the replay window is long enough for representative traffic")
    parts.append("document rollback or disable actions before enabling autonomous rollout work")
    return "Before rollout, " + "; ".join(parts) + "."


def _summary(
    rows: tuple[PlanShadowModeComparisonMatrixRow, ...],
    *,
    total_task_count: int,
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "shadow_task_count": len(rows),
        "unrelated_task_count": max(total_task_count - len(rows), 0),
        "tasks_missing_tolerances": sum(
            1 for row in rows if "mismatch_tolerance" in row.missing_fields
        ),
        "tasks_missing_owners": sum(1 for row in rows if "escalation_owner" in row.missing_fields),
        "missing_field_count": sum(len(row.missing_fields) for row in rows),
        "missing_field_counts": {
            field_name: sum(1 for row in rows if field_name in row.missing_fields)
            for field_name in _REQUIRED_FIELDS
        },
        "comparison_surface_counts": {
            surface: sum(1 for row in rows if row.comparison_surface == surface)
            for surface in _SURFACE_ORDER
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
        "depends_on",
        "dependencies",
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
            if _SHADOW_RE.search(key_text):
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
    "PlanShadowModeComparisonMatrix",
    "PlanShadowModeComparisonMatrixRow",
    "ShadowModeComparisonField",
    "build_plan_shadow_mode_comparison_matrix",
    "derive_plan_shadow_mode_comparison_matrix",
    "generate_plan_shadow_mode_comparison_matrix",
    "plan_shadow_mode_comparison_matrix_to_dict",
    "plan_shadow_mode_comparison_matrix_to_dicts",
    "plan_shadow_mode_comparison_matrix_to_markdown",
    "summarize_plan_shadow_mode_comparison_matrix",
]
