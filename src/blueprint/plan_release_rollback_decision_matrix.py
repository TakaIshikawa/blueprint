"""Build release rollback decision matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ReleaseRollbackRiskCategory = Literal[
    "schema_change",
    "data_migration",
    "external_integration",
    "feature_flag_rollout",
    "customer_visible_change",
    "billing_change",
    "irreversible_operation",
]
ReleaseRollbackPriority = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: tuple[ReleaseRollbackRiskCategory, ...] = (
    "schema_change",
    "data_migration",
    "external_integration",
    "feature_flag_rollout",
    "customer_visible_change",
    "billing_change",
    "irreversible_operation",
)
_PRIORITY_ORDER: dict[ReleaseRollbackPriority, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_TEXT_PATTERNS: tuple[tuple[ReleaseRollbackRiskCategory, re.Pattern[str]], ...] = (
    (
        "schema_change",
        re.compile(
            r"\b(?:schema|schemas|database|db|ddl|migration|migrations|migrate|"
            r"alembic|liquibase|table|column|index|constraint)\b",
            re.I,
        ),
    ),
    (
        "data_migration",
        re.compile(
            r"\b(?:data[- ]migration|backfill|backfills|bulk import|bulk export|etl|"
            r"copy data|move data|reprocess|reconcile|reconciliation|existing records?)\b",
            re.I,
        ),
    ),
    (
        "external_integration",
        re.compile(
            r"\b(?:external|third[- ]?party|vendor|partner|integration|webhook|"
            r"oauth|api provider|stripe|salesforce|slack|twilio|sendgrid|provider)\b",
            re.I,
        ),
    ),
    (
        "feature_flag_rollout",
        re.compile(
            r"\b(?:feature flag|flag rollout|flag activation|kill switch|toggle|"
            r"cohort|gradual rollout|canary|launchdarkly|split\.io)\b",
            re.I,
        ),
    ),
    (
        "customer_visible_change",
        re.compile(
            r"\b(?:customer[- ]?visible|customer[- ]?facing|user[- ]?visible|"
            r"user[- ]?facing|end users?|admins?|ui|frontend|page|screen|checkout|"
            r"notification|email|dashboard|release notes?|support)\b",
            re.I,
        ),
    ),
    (
        "billing_change",
        re.compile(
            r"\b(?:billing|payment|payments|invoice|invoices|subscription|subscriptions|"
            r"checkout|charge|charges|refund|tax|vat|metering|usage meter|stripe)\b",
            re.I,
        ),
    ),
    (
        "irreversible_operation",
        re.compile(
            r"\b(?:irreversible|non[- ]?reversible|destructive|delete|deletion|purge|"
            r"truncate|drop table|drop column|hard delete|erase|cannot roll back|"
            r"cannot rollback|one[- ]?way|permanent)\b",
            re.I,
        ),
    ),
)
_PATH_PATTERNS: tuple[tuple[ReleaseRollbackRiskCategory, re.Pattern[str]], ...] = (
    ("schema_change", re.compile(r"(?:^|/)(?:db|database|schemas?|models?|migrations?|alembic)(?:/|$)|\.(?:sql|ddl)$", re.I)),
    ("data_migration", re.compile(r"(?:^|/)(?:backfills?|data_migrations?|etl|imports?|exports?|reconcile)(?:/|$)", re.I)),
    ("external_integration", re.compile(r"(?:^|/)(?:integrations?|webhooks?|vendors?|providers?|clients?|oauth)(?:/|$)", re.I)),
    ("feature_flag_rollout", re.compile(r"(?:^|/)(?:flags?|feature_flags?|rollouts?|launchdarkly)(?:/|$)", re.I)),
    ("customer_visible_change", re.compile(r"(?:^|/)(?:app|web|ui|frontend|pages?|routes?|components?|emails?|notifications?)(?:/|$)", re.I)),
    ("billing_change", re.compile(r"(?:^|/)(?:billing|payments?|checkout|invoices?|subscriptions?|metering|tax)(?:/|$)", re.I)),
    ("irreversible_operation", re.compile(r"(?:^|/)(?:purge|delete|destructive|retention)(?:/|$)|drop[_-]", re.I)),
)
_ROLLBACK_CRITERIA_RE = re.compile(
    r"\b(?:(?:rollback|roll back|revert|disable|restore|abort|go/no[- ]?go|go no go)"
    r".{0,80}(?:criteria|criterion|threshold|trigger|decision|guardrail|abort|stop)|"
    r"(?:criteria|criterion|threshold|trigger|decision|guardrail|abort|stop).{0,80}"
    r"(?:rollback|roll back|revert|disable|restore|go/no[- ]?go|go no go))\b",
    re.I,
)
_ROLLBACK_COMMAND_RE = re.compile(
    r"\b(?:rollback|roll back|revert|restore|disable flag|failback|down migration|"
    r"migration down|terraform apply|helm rollback)\b",
    re.I,
)
_VALIDATION_COMMAND_RE = re.compile(
    r"\b(?:pytest|test|smoke|validate|verify|check|reconcile|dry[- ]?run|staging|"
    r"pre[- ]?prod|canary)\b",
    re.I,
)
_PRODUCTION_RE = re.compile(r"\b(?:prod|production|release|launch|rollout|cutover|go live|live)\b", re.I)
_HIGH_RISK_RE = re.compile(r"\b(?:high|critical|severe|sev(?:erity)?[- ]?[012]?|p[012])\b", re.I)
_OWNER_KEYS = (
    "owner",
    "owners",
    "owner_hint",
    "owner_hints",
    "assignee",
    "assignees",
    "dri",
    "oncall",
    "on_call",
    "team",
)


@dataclass(frozen=True, slots=True)
class PlanReleaseRollbackDecisionRow:
    """Release rollback decision guidance for one rollback-sensitive task."""

    task_id: str
    title: str
    risk_categories: tuple[ReleaseRollbackRiskCategory, ...] = field(default_factory=tuple)
    rollback_decision_signals: tuple[str, ...] = field(default_factory=tuple)
    required_decision_points: tuple[str, ...] = field(default_factory=tuple)
    owner_suggestions: tuple[str, ...] = field(default_factory=tuple)
    missing_inputs: tuple[str, ...] = field(default_factory=tuple)
    priority: ReleaseRollbackPriority = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_categories": list(self.risk_categories),
            "rollback_decision_signals": list(self.rollback_decision_signals),
            "required_decision_points": list(self.required_decision_points),
            "owner_suggestions": list(self.owner_suggestions),
            "missing_inputs": list(self.missing_inputs),
            "priority": self.priority,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanReleaseRollbackDecisionMatrix:
    """Plan-level release rollback decision matrix."""

    plan_id: str | None = None
    rows: tuple[PlanReleaseRollbackDecisionRow, ...] = field(default_factory=tuple)
    rollback_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanReleaseRollbackDecisionRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "rollback_task_ids": list(self.rollback_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return rollback decision rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the rollback decision matrix as deterministic Markdown."""
        title = "# Plan Release Rollback Decision Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        priority_counts = self.summary.get("priority_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Rollback task count: {self.summary.get('rollback_task_count', 0)}",
            f"- Decision row count: {self.summary.get('decision_row_count', 0)}",
            "- Priority counts: "
            + ", ".join(
                f"{priority} {priority_counts.get(priority, 0)}" for priority in _PRIORITY_ORDER
            ),
        ]
        if not self.rows:
            lines.extend(["", "No release rollback decision rows were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Priority | Risks | Signals | Decision Points | Owners | Missing Inputs | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.title)} | "
                f"{row.priority} | "
                f"{_markdown_cell('; '.join(row.risk_categories) or 'none')} | "
                f"{_markdown_cell('; '.join(row.rollback_decision_signals) or 'none')} | "
                f"{_markdown_cell('; '.join(row.required_decision_points) or 'none')} | "
                f"{_markdown_cell('; '.join(row.owner_suggestions) or 'none')} | "
                f"{_markdown_cell('; '.join(row.missing_inputs) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_release_rollback_decision_matrix(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanReleaseRollbackDecisionMatrix:
    """Identify tasks that need explicit release rollback decision criteria."""
    plan_id, tasks = _source_payload(source)
    rows = tuple(
        row
        for index, task in enumerate(tasks, start=1)
        if (row := _task_row(task, index)) is not None
    )
    rows = tuple(
        sorted(
            rows,
            key=lambda row: (
                _PRIORITY_ORDER[row.priority],
                row.task_id,
                tuple(_RISK_ORDER.index(risk) for risk in row.risk_categories),
            ),
        )
    )
    rollback_task_ids = tuple(_dedupe(row.task_id for row in rows))
    return PlanReleaseRollbackDecisionMatrix(
        plan_id=plan_id,
        rows=rows,
        rollback_task_ids=rollback_task_ids,
        summary={
            "task_count": len(tasks),
            "rollback_task_count": len(rollback_task_ids),
            "decision_row_count": len(rows),
            "priority_counts": {
                priority: sum(1 for row in rows if row.priority == priority)
                for priority in _PRIORITY_ORDER
            },
            "risk_counts": {
                risk: sum(1 for row in rows if risk in row.risk_categories)
                for risk in _RISK_ORDER
            },
            "missing_input_count": sum(len(row.missing_inputs) for row in rows),
        },
    )


def analyze_plan_release_rollback_decisions(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | PlanReleaseRollbackDecisionMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanReleaseRollbackDecisionMatrix:
    """Return an existing matrix or build one from a plan-shaped source."""
    if isinstance(source, PlanReleaseRollbackDecisionMatrix):
        return source
    return build_plan_release_rollback_decision_matrix(source)


def generate_plan_release_rollback_decision_matrix(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanReleaseRollbackDecisionMatrix:
    """Compatibility alias for building release rollback decision matrices."""
    return build_plan_release_rollback_decision_matrix(source)


def plan_release_rollback_decision_matrix_to_dict(
    matrix: PlanReleaseRollbackDecisionMatrix,
) -> dict[str, Any]:
    """Serialize a release rollback decision matrix to a plain dictionary."""
    return matrix.to_dict()


plan_release_rollback_decision_matrix_to_dict.__test__ = False


def plan_release_rollback_decision_matrix_to_markdown(
    matrix: PlanReleaseRollbackDecisionMatrix,
) -> str:
    """Render a release rollback decision matrix as Markdown."""
    return matrix.to_markdown()


plan_release_rollback_decision_matrix_to_markdown.__test__ = False


def plan_release_rollback_decision_matrix_to_dicts(
    matrix: PlanReleaseRollbackDecisionMatrix,
) -> list[dict[str, Any]]:
    """Serialize release rollback decision rows to plain dictionaries."""
    return matrix.to_dicts()


plan_release_rollback_decision_matrix_to_dicts.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanReleaseRollbackDecisionRow | None:
    risks, risk_evidence = _task_risks(task)
    signals, signal_evidence = _decision_signals(task, risks)
    if not risks and not signals:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    decision_points = _required_decision_points(risks)
    owners = tuple(_dedupe([*_owner_hints(task), *_default_owners(risks)]))
    missing_inputs = _missing_inputs(task, risks, signals)
    priority = _priority(task, risks, missing_inputs)
    return PlanReleaseRollbackDecisionRow(
        task_id=task_id,
        title=title,
        risk_categories=risks,
        rollback_decision_signals=signals,
        required_decision_points=decision_points,
        owner_suggestions=owners,
        missing_inputs=missing_inputs,
        priority=priority,
        evidence=tuple(_dedupe([*risk_evidence, *signal_evidence])),
    )


def _task_risks(
    task: Mapping[str, Any],
) -> tuple[tuple[ReleaseRollbackRiskCategory, ...], tuple[str, ...]]:
    risks: list[ReleaseRollbackRiskCategory] = []
    evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        path_text = _path_text(normalized)
        matched = False
        for risk, pattern in _PATH_PATTERNS:
            if pattern.search(normalized) or pattern.search(path_text):
                risks.append(risk)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        before = len(risks)
        for risk, pattern in _TEXT_PATTERNS:
            if pattern.search(text):
                risks.append(risk)
        if len(risks) > before:
            evidence.append(_evidence_snippet(source_field, text))

    risk_set = set(risks)
    return tuple(risk for risk in _RISK_ORDER if risk in risk_set), tuple(_dedupe(evidence))


def _decision_signals(
    task: Mapping[str, Any],
    risks: tuple[ReleaseRollbackRiskCategory, ...],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    signals: list[str] = []
    evidence: list[str] = []
    for source_field, text in _candidate_texts(task):
        if _ROLLBACK_CRITERIA_RE.search(text):
            signals.append("explicit rollback criteria")
            evidence.append(_evidence_snippet(source_field, text))
        if _ROLLBACK_COMMAND_RE.search(text):
            signals.append("rollback command or procedure")
            evidence.append(_evidence_snippet(source_field, text))
        if source_field in {"test_command", "validation_command"} or source_field.startswith(
            ("validation_commands", "metadata.validation", "metadata.test", "metadata.rollback")
        ):
            if _VALIDATION_COMMAND_RE.search(text):
                signals.append("validation command")
                evidence.append(_evidence_snippet(source_field, text))
    if risks and any(_PRODUCTION_RE.search(text) for _, text in _candidate_texts(task)):
        signals.append("production release surface")
    return tuple(_dedupe(signals)), tuple(_dedupe(evidence))


def _required_decision_points(
    risks: tuple[ReleaseRollbackRiskCategory, ...],
) -> tuple[str, ...]:
    points: list[str] = [
        "Named go/no-go owner and backup approver",
        "Objective rollback trigger thresholds before release",
        "Post-rollback validation command or evidence",
    ]
    if "schema_change" in risks:
        points.append("Schema rollback, restore, or forward-fix decision path")
    if "data_migration" in risks:
        points.append("Record-count reconciliation and data drift threshold")
    if "external_integration" in risks:
        points.append("Provider fallback or escalation threshold")
    if "feature_flag_rollout" in risks:
        points.append("Flag disable criteria and control-path validation")
    if "customer_visible_change" in risks:
        points.append("Customer-impact threshold and communication handoff")
    if "billing_change" in risks:
        points.append("Charge, invoice, tax, or metering correction threshold")
    if "irreversible_operation" in risks:
        points.append("Irreversibility approval and recovery alternative")
    return tuple(_dedupe(points))


def _missing_inputs(
    task: Mapping[str, Any],
    risks: tuple[ReleaseRollbackRiskCategory, ...],
    signals: tuple[str, ...],
) -> tuple[str, ...]:
    text = " ".join(value for _, value in _candidate_texts(task))
    missing: list[str] = []
    if not _owner_hints(task):
        missing.append("Assign rollback decision owner.")
    if "explicit rollback criteria" not in signals and not _ROLLBACK_CRITERIA_RE.search(text):
        missing.append("Define objective go/no-go and rollback trigger criteria.")
    if "validation command" not in signals:
        missing.append("Add post-rollback validation command or evidence.")
    if ("schema_change" in risks or "data_migration" in risks) and not re.search(
        r"\b(?:backup|restore|snapshot|down migration|forward[- ]?fix|reconcile|record count)\b",
        text,
        re.I,
    ):
        missing.append("Document backup, restore, forward-fix, or reconciliation input.")
    if "external_integration" in risks and not re.search(
        r"\b(?:fallback|vendor escalation|provider escalation|sandbox|retry|disable integration)\b",
        text,
        re.I,
    ):
        missing.append("Document provider fallback or escalation input.")
    if "customer_visible_change" in risks and not re.search(
        r"\b(?:customer impact|support|status page|communication|release notes|customer success)\b",
        text,
        re.I,
    ):
        missing.append("Document customer-impact and communication input.")
    if "billing_change" in risks and not re.search(
        r"\b(?:refund|credit|invoice correction|charge reversal|tax correction|metering correction)\b",
        text,
        re.I,
    ):
        missing.append("Document billing correction or reversal input.")
    if "irreversible_operation" in risks and not re.search(
        r"\b(?:approval|backup|restore|export|retention exception|manual recovery)\b",
        text,
        re.I,
    ):
        missing.append("Document approval and recovery alternative for irreversible work.")
    return tuple(_dedupe(missing))


def _priority(
    task: Mapping[str, Any],
    risks: tuple[ReleaseRollbackRiskCategory, ...],
    missing_inputs: tuple[str, ...],
) -> ReleaseRollbackPriority:
    text = " ".join(value for _, value in _candidate_texts(task))
    if _HIGH_RISK_RE.search(text) or _optional_text(task.get("risk_level")) == "high":
        return "high"
    if any(risk in risks for risk in ("schema_change", "billing_change", "irreversible_operation")):
        return "high"
    if len(risks) >= 3 or len(missing_inputs) >= 4:
        return "high"
    if any(risk in risks for risk in ("data_migration", "external_integration", "customer_visible_change")):
        return "medium"
    if risks:
        return "medium"
    return "low"


def _default_owners(
    risks: tuple[ReleaseRollbackRiskCategory, ...],
) -> tuple[str, ...]:
    owners: list[str] = ["release owner", "engineering owner"]
    if "schema_change" in risks or "data_migration" in risks:
        owners.append("database owner")
    if "external_integration" in risks:
        owners.extend(["integration owner", "vendor escalation owner"])
    if "feature_flag_rollout" in risks:
        owners.append("feature flag owner")
    if "customer_visible_change" in risks:
        owners.extend(["support lead", "customer success owner"])
    if "billing_change" in risks:
        owners.append("billing owner")
    if "irreversible_operation" in risks:
        owners.append("approving manager")
    return tuple(_dedupe(owners))


def _owner_hints(task: Mapping[str, Any]) -> list[str]:
    hints: list[str] = []
    for key in _OWNER_KEYS:
        hints.extend(_strings(task.get(key)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key, value in _walk_metadata(metadata):
            normalized = key.casefold().replace("-", "_").replace(" ", "_")
            if normalized in _OWNER_KEYS:
                hints.extend(_strings(value))
    return _dedupe(hints)


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "id",
        "title",
        "description",
        "milestone",
        "owner",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "validation_command",
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
        "validation_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        texts.append(("files_or_modules", path))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key, value in _walk_metadata(metadata):
            for index, text in enumerate(_strings(value)):
                suffix = f"[{index}]" if len(_strings(value)) > 1 else ""
                texts.append((f"metadata.{key}{suffix}", text))
    return texts


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))
        return None, [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))

    tasks: list[dict[str, Any]] = []
    try:
        iterator = iter(source)
    except TypeError:
        return None, []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
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
    return _object_payload(value)


def _object_payload(value: Any) -> dict[str, Any]:
    if value is None or isinstance(value, (str, bytes)):
        return {}
    data: dict[str, Any] = {}
    for name in dir(value):
        if name.startswith("_"):
            continue
        try:
            item = getattr(value, name)
        except Exception:
            continue
        if not callable(item):
            data[name] = item
    return data


def _walk_metadata(value: Mapping[str, Any]) -> list[tuple[str, Any]]:
    pairs: list[tuple[str, Any]] = []
    for key in sorted(value, key=lambda item: str(item)):
        key_text = str(key)
        child = value[key]
        pairs.append((key_text, child))
        if isinstance(child, Mapping):
            pairs.extend(_walk_metadata(child))
    return pairs


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
    return value.replace("\\", "/").casefold()


def _path_text(path: str) -> str:
    return " ".join(part.replace("_", " ").replace("-", " ") for part in PurePosixPath(path).parts)


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


def _dedupe(values: Iterable[_T | None]) -> list[_T]:
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
    "PlanReleaseRollbackDecisionMatrix",
    "PlanReleaseRollbackDecisionRow",
    "ReleaseRollbackPriority",
    "ReleaseRollbackRiskCategory",
    "analyze_plan_release_rollback_decisions",
    "build_plan_release_rollback_decision_matrix",
    "generate_plan_release_rollback_decision_matrix",
    "plan_release_rollback_decision_matrix_to_dict",
    "plan_release_rollback_decision_matrix_to_dicts",
    "plan_release_rollback_decision_matrix_to_markdown",
]
