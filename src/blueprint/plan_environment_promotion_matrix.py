"""Build environment promotion gate matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


PromotionEnvironment = Literal["development", "staging", "production", "rollback"]
PromotionGateType = Literal[
    "development_validation",
    "staging_validation",
    "production_approval",
    "smoke_test",
    "canary",
    "feature_flag",
    "migration_check",
    "backfill_reconciliation",
    "integration_check",
    "config_review",
    "rollback_plan",
]
PromotionPriority = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_ENV_ORDER: dict[PromotionEnvironment, int] = {
    "development": 0,
    "staging": 1,
    "production": 2,
    "rollback": 3,
}
_GATE_ORDER: dict[PromotionGateType, int] = {
    "development_validation": 0,
    "staging_validation": 1,
    "production_approval": 2,
    "smoke_test": 3,
    "canary": 4,
    "feature_flag": 5,
    "migration_check": 6,
    "backfill_reconciliation": 7,
    "integration_check": 8,
    "config_review": 9,
    "rollback_plan": 10,
}
_PRIORITY_ORDER: dict[PromotionPriority, int] = {"high": 0, "medium": 1, "low": 2}
_PRIORITY_RANK: dict[PromotionPriority, int] = {"low": 0, "medium": 1, "high": 2}

_ENV_PATTERNS: dict[PromotionEnvironment, re.Pattern[str]] = {
    "development": re.compile(r"\b(?:dev|development|local)\b", re.I),
    "staging": re.compile(r"\b(?:staging|stage|preprod|pre-prod|uat|qa)\b", re.I),
    "production": re.compile(r"\b(?:prod|production|live|launch|release)\b", re.I),
    "rollback": re.compile(r"\b(?:rollback|roll back|revert|restore|undo)\b", re.I),
}
_GATE_PATTERNS: dict[PromotionGateType, re.Pattern[str]] = {
    "development_validation": re.compile(
        r"\b(?:dev(?:elopment)? validation|local validation|unit test|developer test)\b",
        re.I,
    ),
    "staging_validation": re.compile(
        r"\b(?:staging validation|stage validation|uat|qa sign[- ]?off|preprod|pre-prod|"
        r"environment-specific validation|environment specific validation)\b",
        re.I,
    ),
    "production_approval": re.compile(
        r"\b(?:approval|approve|sign[- ]?off|release gate|go/no-go|change advisory|cab|"
        r"production gate|prod gate)\b",
        re.I,
    ),
    "smoke_test": re.compile(r"\b(?:smoke test|smoke tests|smoke validation|post[- ]?deploy)\b", re.I),
    "canary": re.compile(r"\b(?:canary|phased rollout|gradual rollout|progressive rollout)\b", re.I),
    "feature_flag": re.compile(r"\b(?:feature flag|feature toggle|flagged|kill switch)\b", re.I),
    "migration_check": re.compile(
        r"\b(?:migration|migrations|schema change|schema migration|alembic|ddl|"
        r"database change|db change)\b",
        re.I,
    ),
    "backfill_reconciliation": re.compile(
        r"\b(?:backfill|backfills|reconciliation|reconcile|row counts?|data repair|data fix)\b",
        re.I,
    ),
    "integration_check": re.compile(
        r"\b(?:integration|third[- ]?party|external service|vendor|provider|webhook|"
        r"callback|sync|api client)\b",
        re.I,
    ),
    "config_review": re.compile(
        r"\b(?:config|configuration|env var|environment variable|secret|settings|"
        r"runtime flag|helm values|terraform)\b",
        re.I,
    ),
    "rollback_plan": re.compile(
        r"\b(?:rollback|roll back|revert|restore|undo|recovery plan|rollback plan)\b",
        re.I,
    ),
}
_DEPLOY_RE = re.compile(
    r"\b(?:deploy|deployment|release|rollout|ship|launch|promote|promotion)\b",
    re.I,
)
_HIGH_RISK_GATES: set[PromotionGateType] = {
    "migration_check",
    "backfill_reconciliation",
    "canary",
    "rollback_plan",
}


@dataclass(frozen=True, slots=True)
class PlanEnvironmentPromotionRow:
    """One task-level promotion gate row."""

    task_id: str
    task_title: str
    affected_environments: tuple[PromotionEnvironment, ...]
    required_gates: tuple[PromotionGateType, ...]
    validation_evidence: tuple[str, ...] = field(default_factory=tuple)
    rollback_requirement: str = ""
    priority: PromotionPriority = "low"
    owner_hint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "affected_environments": list(self.affected_environments),
            "required_gates": list(self.required_gates),
            "validation_evidence": list(self.validation_evidence),
            "rollback_requirement": self.rollback_requirement,
            "priority": self.priority,
            "owner_hint": self.owner_hint,
        }


@dataclass(frozen=True, slots=True)
class PlanEnvironmentPromotionMatrix:
    """Plan-level environment promotion gate matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanEnvironmentPromotionRow, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return promotion matrix rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Environment Promotion Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        priority_counts = self.summary.get("priority_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('promoted_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks require promotion gates "
                f"(high: {priority_counts.get('high', 0)}, "
                f"medium: {priority_counts.get('medium', 0)}, "
                f"low: {priority_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No environment promotion gates were inferred."])
            if self.no_signal_task_ids:
                lines.extend(
                    [
                        "",
                        f"No promotion signals: {_markdown_cell(', '.join(self.no_signal_task_ids))}",
                    ]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Environments | Gates | Rollback | Priority | Owner | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.task_title)} | "
                f"{_markdown_cell(', '.join(row.affected_environments))} | "
                f"{_markdown_cell(', '.join(row.required_gates))} | "
                f"{_markdown_cell(row.rollback_requirement)} | "
                f"{row.priority} | "
                f"{_markdown_cell(row.owner_hint or 'Unassigned')} | "
                f"{_markdown_cell('; '.join(row.validation_evidence))} |"
            )
        if self.no_signal_task_ids:
            lines.extend(
                [
                    "",
                    f"No promotion signals: {_markdown_cell(', '.join(self.no_signal_task_ids))}",
                ]
            )
        return "\n".join(lines)


def build_plan_environment_promotion_matrix(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> PlanEnvironmentPromotionMatrix:
    """Build task-level promotion gates for deployment-sensitive execution plans."""
    plan_id, tasks = _source_payload(source)
    rows: list[PlanEnvironmentPromotionRow] = []
    no_signal_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index)
        if row:
            rows.append(row)
        else:
            no_signal_task_ids.append(_task_id(task, index))

    gate_counts = {
        gate: sum(1 for row in rows if gate in row.required_gates) for gate in _GATE_ORDER
    }
    priority_counts = {
        priority: sum(1 for row in rows if row.priority == priority) for priority in _PRIORITY_ORDER
    }
    return PlanEnvironmentPromotionMatrix(
        plan_id=plan_id,
        rows=tuple(rows),
        no_signal_task_ids=tuple(no_signal_task_ids),
        summary={
            "task_count": len(tasks),
            "promoted_task_count": len(rows),
            "gate_counts": gate_counts,
            "priority_counts": priority_counts,
        },
    )


def plan_environment_promotion_matrix_to_dict(
    matrix: PlanEnvironmentPromotionMatrix,
) -> dict[str, Any]:
    """Serialize an environment promotion matrix to a plain dictionary."""
    return matrix.to_dict()


plan_environment_promotion_matrix_to_dict.__test__ = False


def plan_environment_promotion_matrix_to_markdown(
    matrix: PlanEnvironmentPromotionMatrix,
) -> str:
    """Render an environment promotion matrix as Markdown."""
    return matrix.to_markdown()


plan_environment_promotion_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanEnvironmentPromotionRow | None:
    task_id = _task_id(task, index)
    signals = _signals(task)
    if not signals.gates and not signals.deploy_signal:
        return None

    gates = set(signals.gates)
    environments = set(signals.environments)
    if signals.deploy_signal:
        gates.update(("development_validation", "staging_validation", "production_approval"))
        environments.update(("development", "staging", "production"))
    if "development_validation" in gates:
        environments.add("development")
    if "staging_validation" in gates:
        environments.add("staging")
    if "production_approval" in gates:
        environments.add("production")
    if gates & {"migration_check", "backfill_reconciliation", "integration_check", "config_review"}:
        gates.update(("development_validation", "staging_validation"))
        environments.update(("development", "staging"))
    if gates & {"feature_flag", "smoke_test"}:
        environments.update(("development", "staging"))
    if gates & {"production_approval", "canary", "feature_flag", "smoke_test"}:
        environments.add("production")
    if "rollback_plan" in gates or _rollback_required(gates):
        gates.add("rollback_plan")
        environments.add("rollback")

    ordered_gates = tuple(gate for gate in _GATE_ORDER if gate in gates)
    return PlanEnvironmentPromotionRow(
        task_id=task_id,
        task_title=_optional_text(task.get("title")) or task_id,
        affected_environments=tuple(env for env in _ENV_ORDER if env in environments),
        required_gates=ordered_gates,
        validation_evidence=tuple(_dedupe(signals.evidence)),
        rollback_requirement=_rollback_requirement(ordered_gates),
        priority=_priority(task, ordered_gates, signals.deploy_signal),
        owner_hint=_owner_hint(task, ordered_gates),
    )


@dataclass(frozen=True, slots=True)
class _Signals:
    gates: tuple[PromotionGateType, ...] = field(default_factory=tuple)
    environments: tuple[PromotionEnvironment, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    deploy_signal: bool = False


def _signals(task: Mapping[str, Any]) -> _Signals:
    gates: set[PromotionGateType] = set()
    environments: set[PromotionEnvironment] = set()
    evidence: list[str] = []
    deploy_signal = False

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        if _DEPLOY_RE.search(text):
            deploy_signal = True
            evidence.append(snippet)
        for environment, pattern in _ENV_PATTERNS.items():
            if pattern.search(text):
                environments.add(environment)
                evidence.append(snippet)
        for gate, pattern in _GATE_PATTERNS.items():
            if pattern.search(text):
                gates.add(gate)
                evidence.append(snippet)

    return _Signals(
        gates=tuple(gate for gate in _GATE_ORDER if gate in gates),
        environments=tuple(environment for environment in _ENV_ORDER if environment in environments),
        evidence=tuple(_dedupe(evidence)),
        deploy_signal=deploy_signal,
    )


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
    return _DEPLOY_RE.search(text) is not None or any(
        pattern.search(text) for pattern in (*_ENV_PATTERNS.values(), *_GATE_PATTERNS.values())
    )


def _rollback_required(gates: Iterable[PromotionGateType]) -> bool:
    return bool(set(gates) & {"migration_check", "backfill_reconciliation", "canary"})


def _rollback_requirement(gates: Iterable[PromotionGateType]) -> str:
    gate_set = set(gates)
    if "migration_check" in gate_set:
        return "Document forward-fix or restore path before production promotion."
    if "backfill_reconciliation" in gate_set:
        return "Define pause, retry, and data reconciliation procedure before launch."
    if "canary" in gate_set:
        return "Define canary abort triggers and traffic rollback steps."
    if "rollback_plan" in gate_set:
        return "Rollback plan required before promotion."
    return "Confirm rollback is not required for this promotion."


def _priority(
    task: Mapping[str, Any],
    gates: Iterable[PromotionGateType],
    deploy_signal: bool,
) -> PromotionPriority:
    risk = (_optional_text(task.get("risk_level")) or "").casefold()
    if risk in {"critical", "blocker", "high"}:
        return "high"
    gate_set = set(gates)
    if gate_set & _HIGH_RISK_GATES:
        return "high"
    if deploy_signal or gate_set & {
        "production_approval",
        "feature_flag",
        "integration_check",
        "config_review",
        "smoke_test",
    }:
        return "medium"
    return "low"


def _owner_hint(task: Mapping[str, Any], gates: Iterable[PromotionGateType]) -> str | None:
    explicit = _optional_text(task.get("owner_type"))
    if explicit:
        return explicit
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("owner", "owner_hint", "team", "service_owner"):
            if text := _optional_text(metadata.get(key)):
                return text
    gate_set = set(gates)
    if "migration_check" in gate_set or "backfill_reconciliation" in gate_set:
        return "data_owner"
    if "integration_check" in gate_set:
        return "integration_owner"
    if "config_review" in gate_set:
        return "platform_owner"
    if "production_approval" in gate_set:
        return "release_owner"
    return None


def _source_payload(source: Mapping[str, Any] | ExecutionPlan | object) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    return None, []


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
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
    "PlanEnvironmentPromotionMatrix",
    "PlanEnvironmentPromotionRow",
    "PromotionEnvironment",
    "PromotionGateType",
    "PromotionPriority",
    "build_plan_environment_promotion_matrix",
    "plan_environment_promotion_matrix_to_dict",
    "plan_environment_promotion_matrix_to_markdown",
]
