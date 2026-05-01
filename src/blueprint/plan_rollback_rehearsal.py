"""Build rollback rehearsal checklists for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


RollbackRehearsalType = Literal[
    "deployment",
    "migration",
    "data_movement",
    "feature_flag",
    "queue",
    "infrastructure",
    "external_service",
]
_T = TypeVar("_T")

_REHEARSAL_ORDER: dict[RollbackRehearsalType, int] = {
    "migration": 0,
    "data_movement": 1,
    "deployment": 2,
    "feature_flag": 3,
    "queue": 4,
    "infrastructure": 5,
    "external_service": 6,
}
_TEXT_PATTERNS: tuple[tuple[RollbackRehearsalType, re.Pattern[str]], ...] = (
    (
        "deployment",
        re.compile(
            r"\b(?:deploy|deployment|release|rollout|production rollout|prod deploy|canary|"
            r"traffic shift|blue[- ]?green)\b",
            re.I,
        ),
    ),
    (
        "migration",
        re.compile(r"\b(?:migration|migrations|migrate|schema change|ddl|alembic)\b", re.I),
    ),
    (
        "data_movement",
        re.compile(
            r"\b(?:backfill|data movement|data migration|copy data|move data|replicate|"
            r"replication|bulk import|bulk export|etl)\b",
            re.I,
        ),
    ),
    (
        "feature_flag",
        re.compile(
            r"\b(?:feature flag|flag rollback|flag activation|enable flag|disable flag|"
            r"launchdarkly|split\.io)\b",
            re.I,
        ),
    ),
    (
        "queue",
        re.compile(
            r"\b(?:queue|queues|consumer|worker|drain queue|queue drain|backlog|dead[- ]?letter|dlq)\b",
            re.I,
        ),
    ),
    (
        "infrastructure",
        re.compile(
            r"\b(?:infrastructure|infra|terraform|pulumi|cloudformation|kubernetes|k8s|"
            r"helm|dns|route53|cloudflare|load balancer|vpc|iam)\b",
            re.I,
        ),
    ),
    (
        "external_service",
        re.compile(
            r"\b(?:external service|third[- ]?party|vendor|integration|webhook|api client|"
            r"stripe|salesforce|pagerduty|sendgrid)\b",
            re.I,
        ),
    ),
)
_PATH_PATTERNS: tuple[tuple[RollbackRehearsalType, re.Pattern[str]], ...] = (
    (
        "deployment",
        re.compile(r"(?:^|/)(?:deploy|deployments?|release|rollout|canary|traffic)(?:/|$)", re.I),
    ),
    ("migration", re.compile(r"(?:^|/)(?:migrations?|alembic|schema|ddl)(?:/|$)|\.sql$", re.I)),
    (
        "data_movement",
        re.compile(r"(?:^|/)(?:backfills?|data_migrations?|etl|exports?|imports?)(?:/|$)", re.I),
    ),
    (
        "feature_flag",
        re.compile(r"(?:^|/)(?:flags?|feature_flags?|launchdarkly)(?:/|$)", re.I),
    ),
    ("queue", re.compile(r"(?:^|/)(?:queues?|consumers?|workers?)(?:/|$)|drain|dlq", re.I)),
    (
        "infrastructure",
        re.compile(
            r"(?:^|/)(?:infra|infrastructure|terraform|pulumi|k8s|kubernetes|helm|dns|"
            r"route53|cloudflare)(?:/|$)|\.(?:tf|tfvars)$",
            re.I,
        ),
    ),
    (
        "external_service",
        re.compile(r"(?:^|/)(?:integrations?|vendors?|webhooks?|clients?)(?:/|$)", re.I),
    ),
)
_PRODUCTION_RE = re.compile(
    r"\b(?:prod|production|live|customer[- ]?facing|release|rollout|deploy|cutover)\b", re.I
)
_HIGH_RISK_RE = re.compile(r"\b(?:high|critical|severe|sev(?:erity)?[- ]?[012]?|p[012])\b", re.I)
_DRY_RUN_RE = re.compile(
    r"\b(?:dry[- ]?run|rehears(?:al|e)|sandbox|staging|pre[- ]?prod|test run)\b", re.I
)
_ROLLBACK_VALIDATION_RE = re.compile(
    r"\b(?:(?:rollback|roll back|revert|restore|disable|failback).{0,48}"
    r"(?:validat\w*|verify|test|prove|confirm)|(?:validat\w*|verify|test|prove|confirm).{0,48}"
    r"(?:rollback|roll back|revert|restore|disable|failback))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class RollbackRehearsalChecklistItem:
    """Plan-scoped rehearsal guidance for one rollback-sensitive surface."""

    rehearsal_type: RollbackRehearsalType
    rehearsal_scope: str
    linked_task_ids: tuple[str, ...] = field(default_factory=tuple)
    preconditions: tuple[str, ...] = field(default_factory=tuple)
    dry_run_command_hints: tuple[str, ...] = field(default_factory=tuple)
    validation_evidence: tuple[str, ...] = field(default_factory=tuple)
    abort_criteria: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "rehearsal_type": self.rehearsal_type,
            "rehearsal_scope": self.rehearsal_scope,
            "linked_task_ids": list(self.linked_task_ids),
            "preconditions": list(self.preconditions),
            "dry_run_command_hints": list(self.dry_run_command_hints),
            "validation_evidence": list(self.validation_evidence),
            "abort_criteria": list(self.abort_criteria),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class RollbackRehearsalChecklist:
    """Plan-level rollback rehearsal checklist."""

    plan_id: str | None = None
    items: tuple[RollbackRehearsalChecklistItem, ...] = field(default_factory=tuple)
    rehearsal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "items": [item.to_dict() for item in self.items],
            "rehearsal_task_ids": list(self.rehearsal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return rehearsal checklist items as plain dictionaries."""
        return [item.to_dict() for item in self.items]

    def to_markdown(self) -> str:
        """Render the rollback rehearsal checklist as deterministic Markdown."""
        title = "# Rollback Rehearsal Checklist"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title, "", "## Summary", ""]
        lines.extend(
            [
                f"- Task count: {self.summary.get('task_count', 0)}",
                f"- Rehearsal task count: {self.summary.get('rehearsal_task_count', 0)}",
                f"- Checklist item count: {self.summary.get('checklist_item_count', 0)}",
            ]
        )
        if not self.items:
            lines.extend(["", "No rollback rehearsal candidates were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Type | Scope | Tasks | Preconditions | Dry Run Hints | Validation Evidence | Abort Criteria |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for item in self.items:
            lines.append(
                "| "
                f"{item.rehearsal_type} | "
                f"{_markdown_cell(item.rehearsal_scope)} | "
                f"{_markdown_cell(', '.join(item.linked_task_ids))} | "
                f"{_markdown_cell('; '.join(item.preconditions) or 'none')} | "
                f"{_markdown_cell('; '.join(item.dry_run_command_hints) or 'none')} | "
                f"{_markdown_cell('; '.join(item.validation_evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(item.abort_criteria) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_rollback_rehearsal_checklist(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> RollbackRehearsalChecklist:
    """Derive plan-level rollback rehearsal guidance from execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_candidate(task, index) for index, task in enumerate(tasks, start=1)]
    candidates = [candidate for candidate in candidates if candidate is not None]
    grouped = _group_candidates(candidates)
    items = tuple(
        _item_from_group(rehearsal_type, grouped[rehearsal_type])
        for rehearsal_type in sorted(grouped, key=lambda item: _REHEARSAL_ORDER[item])
    )
    type_counts = {
        rehearsal_type: sum(1 for candidate in candidates if candidate["type"] == rehearsal_type)
        for rehearsal_type in _REHEARSAL_ORDER
    }
    rehearsal_task_ids = tuple(
        _dedupe(task_id for item in items for task_id in item.linked_task_ids)
    )
    return RollbackRehearsalChecklist(
        plan_id=plan_id,
        items=items,
        rehearsal_task_ids=rehearsal_task_ids,
        summary={
            "task_count": len(tasks),
            "rehearsal_task_count": len(rehearsal_task_ids),
            "checklist_item_count": len(items),
            "type_counts": type_counts,
        },
    )


def summarize_plan_rollback_rehearsal(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> RollbackRehearsalChecklist:
    """Compatibility alias for building rollback rehearsal checklists."""
    return build_plan_rollback_rehearsal_checklist(source)


def plan_rollback_rehearsal_checklist_to_dict(
    checklist: RollbackRehearsalChecklist,
) -> dict[str, Any]:
    """Serialize a rollback rehearsal checklist to a plain dictionary."""
    return checklist.to_dict()


plan_rollback_rehearsal_checklist_to_dict.__test__ = False


def plan_rollback_rehearsal_checklist_to_markdown(
    checklist: RollbackRehearsalChecklist,
) -> str:
    """Render a rollback rehearsal checklist as Markdown."""
    return checklist.to_markdown()


plan_rollback_rehearsal_checklist_to_markdown.__test__ = False


def _candidate(task: Mapping[str, Any], index: int) -> dict[str, Any] | None:
    signals = _rehearsal_signals(task)
    if not signals:
        return None
    if not (_is_high_risk(task) or _is_production_task(task) or _has_rehearsal_evidence(task)):
        return None
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    rehearsal_type = _primary_rehearsal_type(signals)
    return {
        "task_id": task_id,
        "title": title,
        "type": rehearsal_type,
        "preconditions": tuple(
            _dedupe(
                [
                    *_metadata_values(task, "preconditions", "prerequisites"),
                    *_default_preconditions(rehearsal_type),
                ]
            )
        ),
        "dry_run_command_hints": tuple(_dry_run_command_hints(task)),
        "validation_evidence": tuple(
            _dedupe(
                [
                    *_metadata_values(
                        task,
                        "validation_evidence",
                        "rollback_validation",
                        "rollback_validation_evidence",
                        "rehearsal_evidence",
                    ),
                    *_rollback_validation_evidence(task),
                    *_default_validation_evidence(rehearsal_type),
                ]
            )
        ),
        "abort_criteria": tuple(
            _dedupe(
                [
                    *_metadata_values(task, "abort_criteria", "abort_criterion", "stop_criteria"),
                    *_default_abort_criteria(rehearsal_type),
                ]
            )
        ),
        "evidence": tuple(_dedupe(evidence for values in signals.values() for evidence in values)),
    }


def _group_candidates(
    candidates: Iterable[dict[str, Any]],
) -> dict[RollbackRehearsalType, list[dict[str, Any]]]:
    grouped: dict[RollbackRehearsalType, list[dict[str, Any]]] = {}
    for candidate in sorted(
        candidates, key=lambda item: (item["task_id"], item["title"].casefold())
    ):
        grouped.setdefault(candidate["type"], []).append(candidate)
    return grouped


def _item_from_group(
    rehearsal_type: RollbackRehearsalType,
    candidates: list[dict[str, Any]],
) -> RollbackRehearsalChecklistItem:
    linked_task_ids = tuple(_dedupe(candidate["task_id"] for candidate in candidates))
    titles = tuple(_dedupe(candidate["title"] for candidate in candidates))
    return RollbackRehearsalChecklistItem(
        rehearsal_type=rehearsal_type,
        rehearsal_scope=_rehearsal_scope(rehearsal_type, titles),
        linked_task_ids=linked_task_ids,
        preconditions=tuple(
            _dedupe(value for candidate in candidates for value in candidate["preconditions"])
        ),
        dry_run_command_hints=tuple(
            _dedupe(
                value for candidate in candidates for value in candidate["dry_run_command_hints"]
            )
        ),
        validation_evidence=tuple(
            _dedupe(value for candidate in candidates for value in candidate["validation_evidence"])
        ),
        abort_criteria=tuple(
            _dedupe(value for candidate in candidates for value in candidate["abort_criteria"])
        ),
        evidence=tuple(
            _dedupe(value for candidate in candidates for value in candidate["evidence"])
        ),
    )


def _rehearsal_signals(task: Mapping[str, Any]) -> dict[RollbackRehearsalType, tuple[str, ...]]:
    signals: dict[RollbackRehearsalType, list[str]] = {}
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        for rehearsal_type, pattern in _PATH_PATTERNS:
            if pattern.search(normalized):
                _append(signals, rehearsal_type, f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        for rehearsal_type, pattern in _TEXT_PATTERNS:
            if pattern.search(text):
                _append(signals, rehearsal_type, _evidence_snippet(source_field, text))

    return {rehearsal_type: tuple(_dedupe(values)) for rehearsal_type, values in signals.items()}


def _primary_rehearsal_type(
    signals: Mapping[RollbackRehearsalType, tuple[str, ...]]
) -> RollbackRehearsalType:
    return min(
        signals,
        key=lambda rehearsal_type: (
            -len(signals[rehearsal_type]),
            _REHEARSAL_ORDER[rehearsal_type],
        ),
    )


def _is_high_risk(task: Mapping[str, Any]) -> bool:
    text = " ".join(_strings(task.get("risk_level")) + _metadata_values(task, "risk", "risk_level"))
    return bool(_HIGH_RISK_RE.search(text))


def _is_production_task(task: Mapping[str, Any]) -> bool:
    return any(_PRODUCTION_RE.search(text) for _, text in _candidate_texts(task))


def _has_rehearsal_evidence(task: Mapping[str, Any]) -> bool:
    return any(
        _DRY_RUN_RE.search(text) or _ROLLBACK_VALIDATION_RE.search(text)
        for _, text in _candidate_texts(task)
    )


def _dry_run_command_hints(task: Mapping[str, Any]) -> list[str]:
    hints = _metadata_values(
        task,
        "dry_run_command",
        "dry_run_commands",
        "dry_run_command_hint",
        "dry_run_command_hints",
        "rehearsal_command",
        "rehearsal_commands",
    )
    for source_field, text in _candidate_texts(task):
        if (
            source_field == "acceptance_criteria"
            or source_field.startswith("metadata.dry_run")
            or source_field.startswith("metadata.rehearsal_command")
        ) and _DRY_RUN_RE.search(text):
            hints.append(_evidence_snippet(source_field, text))
    return _dedupe(hints)


def _rollback_validation_evidence(task: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    for source_field, text in _candidate_texts(task):
        if _DRY_RUN_RE.search(text) or _ROLLBACK_VALIDATION_RE.search(text):
            evidence.append(_evidence_snippet(source_field, text))
    return _dedupe(evidence)


def _rehearsal_scope(rehearsal_type: RollbackRehearsalType, titles: tuple[str, ...]) -> str:
    if len(titles) == 1:
        return f"Rehearse rollback for {titles[0]}"
    return f"Rehearse rollback for {len(titles)} {rehearsal_type.replace('_', ' ')} tasks"


def _default_preconditions(rehearsal_type: RollbackRehearsalType) -> tuple[str, ...]:
    defaults = {
        "deployment": (
            "Production-like environment and previous release artifact are available",
            "Rollback owner and communication channel are assigned",
        ),
        "migration": (
            "Database backup or restore point is captured before the rehearsal",
            "Down migration or restore procedure is reviewed by the database owner",
        ),
        "data_movement": (
            "Representative source data and idempotency guard are available",
            "Expected record counts and reconciliation query are captured",
        ),
        "feature_flag": (
            "Flag defaults, cohorts, and owner are confirmed",
            "Control path remains available when the flag is disabled",
        ),
        "queue": (
            "Queue depth, retry, and dead-letter baselines are captured",
            "Producer pause and consumer resume owners are assigned",
        ),
        "infrastructure": (
            "Current infrastructure state and rollback artifact are captured",
            "Change window and approval owner are assigned",
        ),
        "external_service": (
            "Vendor sandbox or test endpoint is available",
            "Fallback route and credential owner are confirmed",
        ),
    }
    return defaults[rehearsal_type]


def _default_validation_evidence(rehearsal_type: RollbackRehearsalType) -> tuple[str, ...]:
    defaults = {
        "deployment": (
            "Rehearsal records redeploy or rollback artifact version and smoke-test result",
        ),
        "migration": (
            "Rehearsal records rollback validation for schema, data shape, and application smoke tests",
        ),
        "data_movement": ("Rehearsal records reconciliation counts before and after rollback",),
        "feature_flag": (
            "Rehearsal records disabled-flag behavior and cohort rollback validation",
        ),
        "queue": ("Rehearsal records queue depth, lag, retry, and dead-letter validation",),
        "infrastructure": (
            "Rehearsal records state diff, health checks, and restored infrastructure version",
        ),
        "external_service": (
            "Rehearsal records sandbox response, fallback behavior, and integration health checks",
        ),
    }
    return defaults[rehearsal_type]


def _default_abort_criteria(rehearsal_type: RollbackRehearsalType) -> tuple[str, ...]:
    defaults = {
        "deployment": (
            "Abort production execution if rollback artifact cannot be deployed cleanly during rehearsal",
        ),
        "migration": (
            "Abort production execution if down migration, restore, or post-rollback smoke checks fail",
        ),
        "data_movement": (
            "Abort production execution if rehearsal reconciliation leaves unexplained record drift",
        ),
        "feature_flag": (
            "Abort production execution if disabling the flag does not restore the control path",
        ),
        "queue": (
            "Abort production execution if queue drain or resume rehearsal increases lag or dead-letter volume",
        ),
        "infrastructure": (
            "Abort production execution if state rollback is incomplete or health checks fail",
        ),
        "external_service": (
            "Abort production execution if vendor fallback or credential recovery fails in rehearsal",
        ),
    }
    return defaults[rehearsal_type]


def _metadata_values(task: Mapping[str, Any], *keys: str) -> list[str]:
    metadata = task.get("metadata")
    if not isinstance(metadata, Mapping):
        return []
    values: list[str] = []
    for key in keys:
        values.extend(_strings(metadata.get(key)))
    return values


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


def _append(
    values: dict[RollbackRehearsalType, list[str]], key: RollbackRehearsalType, value: str
) -> None:
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
    "RollbackRehearsalChecklist",
    "RollbackRehearsalChecklistItem",
    "RollbackRehearsalType",
    "build_plan_rollback_rehearsal_checklist",
    "plan_rollback_rehearsal_checklist_to_dict",
    "plan_rollback_rehearsal_checklist_to_markdown",
    "summarize_plan_rollback_rehearsal",
]
