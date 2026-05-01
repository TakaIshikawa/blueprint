"""Build cutover readiness matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CutoverType = Literal[
    "migration",
    "switchover",
    "cutover",
    "backfill",
    "dual_write",
    "dual_read",
    "dns",
    "traffic_shift",
    "queue_drain",
    "feature_flag_activation",
    "production_rollout",
]
ReadinessStatus = Literal["ready", "needs_prerequisites", "needs_validation", "needs_rollback"]
_T = TypeVar("_T")

_CUTOVER_ORDER: dict[CutoverType, int] = {
    "traffic_shift": 0,
    "dns": 1,
    "dual_write": 2,
    "dual_read": 3,
    "feature_flag_activation": 4,
    "queue_drain": 5,
    "migration": 6,
    "backfill": 7,
    "production_rollout": 8,
    "switchover": 9,
    "cutover": 10,
}
_STATUS_ORDER: dict[ReadinessStatus, int] = {
    "needs_rollback": 0,
    "needs_validation": 1,
    "needs_prerequisites": 2,
    "ready": 3,
}
_CUTOVER_PATTERNS: tuple[tuple[CutoverType, re.Pattern[str]], ...] = (
    ("migration", re.compile(r"\b(?:migration|migrations|migrate|schema change|ddl)\b", re.I)),
    ("switchover", re.compile(r"\b(?:switchover|switch over|switch-over)\b", re.I)),
    ("cutover", re.compile(r"\b(?:cutover|cut over|cut-over|go[- ]?live)\b", re.I)),
    ("backfill", re.compile(r"\b(?:backfill|backfills|backfilled|historical fill)\b", re.I)),
    ("dual_write", re.compile(r"\b(?:dual[- ]?write|dual writes|write[- ]?both)\b", re.I)),
    ("dual_read", re.compile(r"\b(?:dual[- ]?read|dual reads|read[- ]?both)\b", re.I)),
    ("dns", re.compile(r"\b(?:dns|cname|a record|aaaa record|route53|cloudflare)\b", re.I)),
    (
        "traffic_shift",
        re.compile(
            r"\b(?:traffic shift|shift traffic|canary|weighted routing|ramp(?: up)?)\b", re.I
        ),
    ),
    (
        "queue_drain",
        re.compile(r"\b(?:queue drain|drain queue|draining queue|drain backlog)\b", re.I),
    ),
    (
        "feature_flag_activation",
        re.compile(
            r"\b(?:feature flag|flag activation|enable flag|activate flag|launchdarkly)\b", re.I
        ),
    ),
    (
        "production_rollout",
        re.compile(
            r"\b(?:production rollout|prod rollout|rollout|production deploy|prod deploy)\b", re.I
        ),
    ),
)
_PATH_PATTERNS: tuple[tuple[CutoverType, re.Pattern[str]], ...] = (
    ("migration", re.compile(r"(?:^|/)(?:migrations?|alembic|schema|ddl)(?:/|$)|\.sql$", re.I)),
    ("backfill", re.compile(r"(?:^|/)(?:backfills?|data_migrations?)(?:/|$)|backfill", re.I)),
    ("dual_write", re.compile(r"dual[_-]?write", re.I)),
    ("dual_read", re.compile(r"dual[_-]?read", re.I)),
    ("dns", re.compile(r"(?:^|/)(?:dns|route53|cloudflare)(?:/|$)", re.I)),
    ("traffic_shift", re.compile(r"(?:^|/)(?:canary|traffic|routing)(?:/|$)", re.I)),
    ("queue_drain", re.compile(r"(?:^|/)(?:queues?|consumers?)(?:/|$)|drain", re.I)),
    (
        "feature_flag_activation",
        re.compile(r"(?:^|/)(?:flags?|feature_flags?|launchdarkly)(?:/|$)", re.I),
    ),
    (
        "production_rollout",
        re.compile(r"(?:^|/)(?:deploy|deployments?|release|rollout)(?:/|$)", re.I),
    ),
)
_VALIDATION_RE = re.compile(
    r"\b(?:validate|validation|verify|verified|smoke|test|monitor|metric|dashboard|"
    r"alert|health check|synthetic|success criteria)\b",
    re.I,
)
_ROLLBACK_RE = re.compile(r"\b(?:rollback|roll back|revert|restore|disable|failback)\b", re.I)
_PREREQ_RE = re.compile(
    r"\b(?:prereq|prerequisite|approval|freeze|backup|snapshot|runbook|owner|"
    r"maintenance window|ready|before)\b",
    re.I,
)
_OWNER_KEYS = (
    "owner",
    "owners",
    "assignee",
    "assignees",
    "owner_hint",
    "owner_hints",
    "owner_type",
    "suggested_owner",
    "reviewer",
    "reviewers",
)


@dataclass(frozen=True, slots=True)
class PlanCutoverReadinessRow:
    """Readiness guidance for one cutover-sensitive execution task."""

    task_id: str
    title: str
    cutover_type: CutoverType
    prerequisites: tuple[str, ...] = field(default_factory=tuple)
    validation_checkpoints: tuple[str, ...] = field(default_factory=tuple)
    rollback_checkpoint: str = ""
    owner_hints: tuple[str, ...] = field(default_factory=tuple)
    readiness_status: ReadinessStatus = "needs_prerequisites"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "cutover_type": self.cutover_type,
            "prerequisites": list(self.prerequisites),
            "validation_checkpoints": list(self.validation_checkpoints),
            "rollback_checkpoint": self.rollback_checkpoint,
            "owner_hints": list(self.owner_hints),
            "readiness_status": self.readiness_status,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanCutoverReadinessMatrix:
    """Plan-level cutover readiness matrix."""

    plan_id: str | None = None
    rows: tuple[PlanCutoverReadinessRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return cutover readiness rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the cutover readiness matrix as deterministic Markdown."""
        title = "# Plan Cutover Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title, "", "## Summary", ""]
        lines.extend(
            [
                f"- Task count: {self.summary.get('task_count', 0)}",
                f"- Cutover task count: {self.summary.get('cutover_task_count', 0)}",
                f"- Ready count: {self.summary.get('ready_count', 0)}",
                f"- Not ready count: {self.summary.get('not_ready_count', 0)}",
            ]
        )
        if not self.rows:
            lines.extend(["", "No cutover-sensitive tasks were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Type | Status | Prerequisites | Validation | Rollback | Owners |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` {_markdown_cell(row.title)} | "
                f"{row.cutover_type} | "
                f"{row.readiness_status} | "
                f"{_markdown_cell('; '.join(row.prerequisites) or 'none')} | "
                f"{_markdown_cell('; '.join(row.validation_checkpoints) or 'none')} | "
                f"{_markdown_cell(row.rollback_checkpoint or 'none')} | "
                f"{_markdown_cell(', '.join(row.owner_hints) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_cutover_readiness_matrix(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanCutoverReadinessMatrix:
    """Derive cutover readiness guidance from an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    rows = tuple(
        sorted(
            (
                row
                for index, task in enumerate(tasks, start=1)
                if (row := _readiness_row(task, index)) is not None
            ),
            key=lambda row: (
                _STATUS_ORDER[row.readiness_status],
                _CUTOVER_ORDER[row.cutover_type],
                row.task_id,
                row.title.casefold(),
            ),
        )
    )
    status_counts = {
        status: sum(1 for row in rows if row.readiness_status == status) for status in _STATUS_ORDER
    }
    type_counts = {
        cutover_type: sum(1 for row in rows if row.cutover_type == cutover_type)
        for cutover_type in _CUTOVER_ORDER
    }
    return PlanCutoverReadinessMatrix(
        plan_id=_optional_text(plan.get("id")),
        rows=rows,
        summary={
            "task_count": len(tasks),
            "cutover_task_count": len(rows),
            "ready_count": status_counts["ready"],
            "not_ready_count": len(rows) - status_counts["ready"],
            "status_counts": status_counts,
            "type_counts": type_counts,
        },
    )


def summarize_plan_cutover_readiness(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanCutoverReadinessMatrix:
    """Compatibility alias for building cutover readiness matrices."""
    return build_plan_cutover_readiness_matrix(source)


def plan_cutover_readiness_matrix_to_dict(
    matrix: PlanCutoverReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a cutover readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_cutover_readiness_matrix_to_dict.__test__ = False


def plan_cutover_readiness_matrix_to_markdown(
    matrix: PlanCutoverReadinessMatrix,
) -> str:
    """Render a cutover readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_cutover_readiness_matrix_to_markdown.__test__ = False


def _readiness_row(task: Mapping[str, Any], index: int) -> PlanCutoverReadinessRow | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _cutover_signals(task)
    if not signals:
        return None

    cutover_type = _primary_cutover_type(signals)
    prerequisites = tuple(
        _dedupe(
            [
                *_metadata_values(task, "prerequisites", "prerequisite"),
                *_default_prerequisites(cutover_type),
            ]
        )
    )
    validations = tuple(
        _dedupe(
            [
                *_metadata_values(
                    task,
                    "validation_checkpoints",
                    "validation_checkpoint",
                    "validations",
                    "validation",
                    "success_criteria",
                ),
                *_default_validation_checkpoints(cutover_type),
            ]
        )
    )
    rollback = _first_metadata_value(
        task,
        "rollback_checkpoint",
        "rollback",
        "rollback_plan",
        "revert_plan",
    ) or _default_rollback_checkpoint(cutover_type)
    owner_hints = tuple(
        _dedupe([*_explicit_owner_hints(task), *_default_owner_hints(cutover_type)])
    )
    status = _explicit_status(task) or _readiness_status(task, prerequisites, validations, rollback)
    evidence = tuple(_dedupe(evidence for values in signals.values() for evidence in values))
    return PlanCutoverReadinessRow(
        task_id=task_id,
        title=title,
        cutover_type=cutover_type,
        prerequisites=prerequisites,
        validation_checkpoints=validations,
        rollback_checkpoint=rollback,
        owner_hints=owner_hints,
        readiness_status=status,
        evidence=evidence,
    )


def _cutover_signals(task: Mapping[str, Any]) -> dict[CutoverType, tuple[str, ...]]:
    signals: dict[CutoverType, list[str]] = {}
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        for cutover_type, pattern in _PATH_PATTERNS:
            if pattern.search(normalized):
                _append(signals, cutover_type, f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        for cutover_type, pattern in _CUTOVER_PATTERNS:
            if pattern.search(text):
                _append(signals, cutover_type, _evidence_snippet(source_field, text))

    return {cutover_type: tuple(_dedupe(values)) for cutover_type, values in signals.items()}


def _primary_cutover_type(signals: Mapping[CutoverType, tuple[str, ...]]) -> CutoverType:
    return min(signals, key=lambda cutover_type: _CUTOVER_ORDER[cutover_type])


def _readiness_status(
    task: Mapping[str, Any],
    prerequisites: tuple[str, ...],
    validations: tuple[str, ...],
    rollback: str,
) -> ReadinessStatus:
    context = " ".join(text for _, text in _candidate_texts(task))
    missing_prereq = not prerequisites or not _PREREQ_RE.search(context)
    missing_validation = not validations or not _VALIDATION_RE.search(context)
    missing_rollback = not rollback or not _ROLLBACK_RE.search(context)
    if missing_rollback:
        return "needs_rollback"
    if missing_validation:
        return "needs_validation"
    if missing_prereq:
        return "needs_prerequisites"
    return "ready"


def _explicit_status(task: Mapping[str, Any]) -> ReadinessStatus | None:
    value = _first_metadata_value(task, "readiness_status", "cutover_readiness_status")
    normalized = (value or "").casefold().replace("-", "_").replace(" ", "_")
    return normalized if normalized in _STATUS_ORDER else None  # type: ignore[return-value]


def _default_prerequisites(cutover_type: CutoverType) -> tuple[str, ...]:
    defaults = {
        "migration": (
            "Schema backup or restore point captured",
            "Migration owner and window assigned",
        ),
        "switchover": (
            "Source and target systems are in sync",
            "Stakeholder approval for switch window captured",
        ),
        "cutover": (
            "Cutover window and decision owner assigned",
            "Runbook is reviewed before start",
        ),
        "backfill": (
            "Backfill input scope and idempotency guard confirmed",
            "Expected row or record counts captured",
        ),
        "dual_write": (
            "Dual-write guardrail and conflict policy confirmed",
            "Downstream consumers can tolerate duplicate writes",
        ),
        "dual_read": (
            "Read comparison criteria and sampling window confirmed",
            "Fallback read path remains available",
        ),
        "dns": (
            "DNS TTL lowered before change window",
            "Current records and target records captured",
        ),
        "traffic_shift": (
            "Traffic ramp schedule and stop criteria approved",
            "Capacity headroom confirmed for target path",
        ),
        "queue_drain": (
            "Queue depth baseline and producer pause plan confirmed",
            "Consumer lag owner assigned",
        ),
        "feature_flag_activation": (
            "Flag owner and cohort scope assigned",
            "Default-off behavior confirmed",
        ),
        "production_rollout": (
            "Release owner and rollout window assigned",
            "Production readiness checklist reviewed",
        ),
    }
    return defaults[cutover_type]


def _default_validation_checkpoints(cutover_type: CutoverType) -> tuple[str, ...]:
    defaults = {
        "migration": (
            "Validate migration completion and row counts",
            "Run application smoke tests against migrated schema",
        ),
        "switchover": (
            "Validate target system health after switch",
            "Compare source and target success metrics",
        ),
        "cutover": (
            "Validate primary production path immediately after cutover",
            "Monitor error rate and latency during the window",
        ),
        "backfill": (
            "Validate processed, skipped, and failed record counts",
            "Spot-check representative backfilled records",
        ),
        "dual_write": (
            "Compare writes in old and new stores",
            "Monitor divergence and write error metrics",
        ),
        "dual_read": (
            "Compare read results across old and new paths",
            "Monitor fallback read rate",
        ),
        "dns": (
            "Validate DNS resolution from internal and external networks",
            "Confirm traffic reaches the intended target",
        ),
        "traffic_shift": (
            "Validate canary metrics at each traffic step",
            "Monitor error budget and saturation before continuing",
        ),
        "queue_drain": (
            "Validate queue depth reaches the target threshold",
            "Confirm no dead-letter spike during drain",
        ),
        "feature_flag_activation": (
            "Validate enabled cohort behavior",
            "Monitor flag-specific errors and conversion guardrails",
        ),
        "production_rollout": (
            "Run production smoke checks after rollout",
            "Monitor dashboards and alerts through the rollout window",
        ),
    }
    return defaults[cutover_type]


def _default_rollback_checkpoint(cutover_type: CutoverType) -> str:
    defaults = {
        "migration": "Rollback checkpoint: restore from backup or run the documented down migration before new writes depend on the schema.",
        "switchover": "Rollback checkpoint: switch traffic back to the source system if target validation fails.",
        "cutover": "Rollback checkpoint: stop the cutover and revert to the previous production path at the first failed validation gate.",
        "backfill": "Rollback checkpoint: stop the job and revert or quarantine affected records from the last known-good batch.",
        "dual_write": "Rollback checkpoint: disable dual-write and continue on the primary write path if divergence exceeds the threshold.",
        "dual_read": "Rollback checkpoint: disable dual-read and route reads to the known-good path if mismatches exceed the threshold.",
        "dns": "Rollback checkpoint: restore previous DNS records if resolution or target health checks fail.",
        "traffic_shift": "Rollback checkpoint: return traffic to the prior weight if canary metrics breach stop criteria.",
        "queue_drain": "Rollback checkpoint: resume producers or revert consumers if queue depth, lag, or DLQ metrics regress.",
        "feature_flag_activation": "Rollback checkpoint: disable the feature flag for the affected cohort if validation fails.",
        "production_rollout": "Rollback checkpoint: halt rollout and redeploy or re-enable the previous production version.",
    }
    return defaults[cutover_type]


def _default_owner_hints(cutover_type: CutoverType) -> tuple[str, ...]:
    defaults = {
        "migration": ("database owner", "service owner"),
        "switchover": ("service owner", "incident commander"),
        "cutover": ("release owner", "incident commander"),
        "backfill": ("data owner", "service owner"),
        "dual_write": ("service owner", "data owner"),
        "dual_read": ("service owner", "quality owner"),
        "dns": ("infrastructure owner", "service owner"),
        "traffic_shift": ("release owner", "SRE owner"),
        "queue_drain": ("queue owner", "SRE owner"),
        "feature_flag_activation": ("feature owner", "release owner"),
        "production_rollout": ("release owner", "SRE owner"),
    }
    return defaults[cutover_type]


def _explicit_owner_hints(task: Mapping[str, Any]) -> list[str]:
    hints: list[str] = []
    for key in _OWNER_KEYS:
        hints.extend(_strings(task.get(key)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in _OWNER_KEYS:
            hints.extend(_strings(metadata.get(key)))
    return hints


def _metadata_values(task: Mapping[str, Any], *keys: str) -> list[str]:
    metadata = task.get("metadata")
    if not isinstance(metadata, Mapping):
        return []
    values: list[str] = []
    for key in keys:
        values.extend(_strings(metadata.get(key)))
    return values


def _first_metadata_value(task: Mapping[str, Any], *keys: str) -> str | None:
    return next(iter(_metadata_values(task, *keys)), None)


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
    ):
        text = _optional_text(task.get(field_name))
        if text:
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "depends_on", "dependencies"):
        for text in _strings(task.get(field_name)):
            texts.append((field_name, text))
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        texts.append(("files_or_modules", path))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in sorted(metadata, key=lambda item: str(item)):
            for text in _strings(metadata[key]):
                texts.append((f"metadata.{key}", text))
    return texts


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


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _normalized_path(path: str) -> str:
    return str(PurePosixPath(path.strip().replace("\\", "/").lower().strip("/")))


def _evidence_snippet(source_field: str, text: str) -> str:
    normalized = _text(text)
    if len(normalized) > 160:
        normalized = f"{normalized[:157].rstrip()}..."
    return f"{source_field}: {normalized}"


def _append(values: dict[CutoverType, list[str]], key: CutoverType, value: str) -> None:
    values.setdefault(key, []).append(value)


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "CutoverType",
    "PlanCutoverReadinessMatrix",
    "PlanCutoverReadinessRow",
    "ReadinessStatus",
    "build_plan_cutover_readiness_matrix",
    "plan_cutover_readiness_matrix_to_dict",
    "plan_cutover_readiness_matrix_to_markdown",
    "summarize_plan_cutover_readiness",
]
