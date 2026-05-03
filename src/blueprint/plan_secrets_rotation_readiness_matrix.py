"""Build plan-level secrets rotation readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SecretsRotationReadiness = Literal["ready", "partial", "blocked"]

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[SecretsRotationReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_ROTATION_RE = re.compile(r"\b(?:rotat(?:e|es|ing|ion)|roll(?:ing)? key|rekey|renew|regenerat(?:e|es|ing|ion))\b", re.I)
_SECRET_RE = re.compile(r"\b(?:secret|secrets|token|tokens|credential|credentials|api key|api keys|key|keys|oauth|jwt|certificate|cert)\b", re.I)
_INVENTORY_RE = re.compile(r"\b(?:inventory|catalog|list|enumerate|identify|map|registry|vault|secret store|key store|kms)\b", re.I)
_OWNER_RE = re.compile(r"\b(?:owner|owners|dri|responsible|assignee|team|lead|security|secops|platform|sre)\b", re.I)
_DEPENDENCY_RE = re.compile(r"\b(?:depend(?:s|ency|encies)|consumer|service|client|integration|downstream|upstream|impact|blast radius|webhook|provider)\b", re.I)
_OVERLAP_RE = re.compile(r"\b(?:dual[- ]read|dual[- ]write|overlap|grace period|compatibility window|parallel|old and new|both keys|fallback key)\b", re.I)
_ROLLOUT_RE = re.compile(r"\b(?:rollout|roll out|deploy|deployment|order|sequence|phase|canary|staged|wave|cutover|promotion)\b", re.I)
_VERIFICATION_RE = re.compile(r"\b(?:verify|verification|validate|validation|test|pytest|smoke|synthetic|health check|monitor|alert|check)\b", re.I)
_ROLLBACK_RE = re.compile(r"\b(?:rollback|roll back|revert|restore|fallback|old secret|old token|previous key|disable|abort)\b", re.I)
_AUDIT_RE = re.compile(r"\b(?:audit|evidence|log|signoff|sign-off|record|ticket|change record|control|soc 2|compliance)\b", re.I)


@dataclass(frozen=True, slots=True)
class PlanSecretsRotationReadinessRow:
    """Readiness signals for one secrets rotation task."""

    task_id: str
    title: str
    secret_inventory: str = "missing"
    rotation_owner: str = "missing"
    dependency_impact: str = "missing"
    overlap_window: str = "missing"
    rollout_order: str = "missing"
    verification: str = "missing"
    rollback: str = "missing"
    audit_evidence: str = "missing"
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: SecretsRotationReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "secret_inventory": self.secret_inventory,
            "rotation_owner": self.rotation_owner,
            "dependency_impact": self.dependency_impact,
            "overlap_window": self.overlap_window,
            "rollout_order": self.rollout_order,
            "verification": self.verification,
            "rollback": self.rollback,
            "audit_evidence": self.audit_evidence,
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanSecretsRotationReadinessMatrix:
    """Plan-level secrets rotation readiness matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanSecretsRotationReadinessRow, ...] = field(default_factory=tuple)
    rotation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_rotation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanSecretsRotationReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "rotation_task_ids": list(self.rotation_task_ids),
            "no_rotation_task_ids": list(self.no_rotation_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the secrets rotation readiness matrix as deterministic Markdown."""
        title = "# Plan Secrets Rotation Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('rotation_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks require secrets rotation readiness "
                f"(blocked: {readiness_counts.get('blocked', 0)}, "
                f"partial: {readiness_counts.get('partial', 0)}, "
                f"ready: {readiness_counts.get('ready', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No secrets rotation readiness rows were inferred."])
            return "\n".join(lines)
        lines.extend(
            [
                "",
                (
                    "| Task | Title | Inventory | Owner | Dependencies | Overlap | Rollout | "
                    "Verification | Rollback | Audit | Readiness | Gaps | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.title)} | "
                f"{row.secret_inventory} | {row.rotation_owner} | {row.dependency_impact} | "
                f"{row.overlap_window} | {row.rollout_order} | {row.verification} | "
                f"{row.rollback} | {row.audit_evidence} | {row.readiness} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.no_rotation_task_ids:
            lines.extend(["", f"No rotation signals: {_markdown_cell(', '.join(self.no_rotation_task_ids))}"])
        return "\n".join(lines)


def build_plan_secrets_rotation_readiness_matrix(source: Any) -> PlanSecretsRotationReadinessMatrix:
    """Build task-level secrets rotation readiness for an execution plan."""
    plan_id, tasks = _source_payload(source)
    rows: list[PlanSecretsRotationReadinessRow] = []
    no_rotation_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index)
        if row:
            rows.append(row)
        else:
            no_rotation_task_ids.append(_task_id(task, index))
    rows.sort(key=lambda row: (_READINESS_ORDER[row.readiness], -len(row.gaps), row.task_id))
    result = tuple(rows)
    return PlanSecretsRotationReadinessMatrix(
        plan_id=plan_id,
        rows=result,
        rotation_task_ids=tuple(row.task_id for row in result),
        no_rotation_task_ids=tuple(no_rotation_task_ids),
        summary=_summary(len(tasks), result),
    )


def generate_plan_secrets_rotation_readiness_matrix(source: Any) -> PlanSecretsRotationReadinessMatrix:
    """Generate a secrets rotation readiness matrix from a plan-like source."""
    return build_plan_secrets_rotation_readiness_matrix(source)


def analyze_plan_secrets_rotation_readiness_matrix(source: Any) -> PlanSecretsRotationReadinessMatrix:
    """Analyze an execution plan for secrets rotation readiness."""
    if isinstance(source, PlanSecretsRotationReadinessMatrix):
        return source
    return build_plan_secrets_rotation_readiness_matrix(source)


def derive_plan_secrets_rotation_readiness_matrix(source: Any) -> PlanSecretsRotationReadinessMatrix:
    """Derive a secrets rotation readiness matrix from a plan-like source."""
    return analyze_plan_secrets_rotation_readiness_matrix(source)


def extract_plan_secrets_rotation_readiness_matrix(source: Any) -> PlanSecretsRotationReadinessMatrix:
    """Extract a secrets rotation readiness matrix from a plan-like source."""
    return derive_plan_secrets_rotation_readiness_matrix(source)


def summarize_plan_secrets_rotation_readiness_matrix(
    source: PlanSecretsRotationReadinessMatrix | Iterable[PlanSecretsRotationReadinessRow] | Any,
) -> dict[str, Any] | PlanSecretsRotationReadinessMatrix:
    """Return deterministic summary counts for a matrix, row iterable, or source."""
    if isinstance(source, PlanSecretsRotationReadinessMatrix):
        return dict(source.summary)
    if _looks_like_plan(source) or _looks_like_task(source) or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_secrets_rotation_readiness_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows)


def plan_secrets_rotation_readiness_matrix_to_dict(matrix: PlanSecretsRotationReadinessMatrix) -> dict[str, Any]:
    """Serialize a secrets rotation readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_secrets_rotation_readiness_matrix_to_dict.__test__ = False


def plan_secrets_rotation_readiness_matrix_to_dicts(
    matrix: PlanSecretsRotationReadinessMatrix | Iterable[PlanSecretsRotationReadinessRow],
) -> list[dict[str, Any]]:
    """Serialize secrets rotation rows to plain dictionaries."""
    if isinstance(matrix, PlanSecretsRotationReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_secrets_rotation_readiness_matrix_to_dicts.__test__ = False


def plan_secrets_rotation_readiness_matrix_to_markdown(matrix: PlanSecretsRotationReadinessMatrix) -> str:
    """Render a secrets rotation readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_secrets_rotation_readiness_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanSecretsRotationReadinessRow | None:
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    if not (_ROTATION_RE.search(context) and _SECRET_RE.search(context)):
        return None
    statuses = {
        "secret_inventory": _status(_INVENTORY_RE, texts),
        "rotation_owner": _status(_OWNER_RE, texts, skip_fields=("id",)),
        "dependency_impact": _status(_DEPENDENCY_RE, texts),
        "overlap_window": _status(_OVERLAP_RE, texts),
        "rollout_order": _status(_ROLLOUT_RE, texts),
        "verification": _status(_VERIFICATION_RE, texts, skip_fields=("id",)),
        "rollback": _status(_ROLLBACK_RE, texts),
        "audit_evidence": _status(_AUDIT_RE, texts),
    }
    gaps = tuple(
        f"Missing {label}."
        for field, label in (
            ("secret_inventory", "secret inventory"),
            ("rotation_owner", "rotation owner"),
            ("dependency_impact", "dependency impact"),
            ("overlap_window", "dual-read or overlap window"),
            ("rollout_order", "rollout order"),
            ("verification", "verification"),
            ("rollback", "rollback"),
            ("audit_evidence", "audit evidence"),
        )
        if statuses[field] == "missing"
    )
    readiness: SecretsRotationReadiness
    if statuses["rotation_owner"] == "missing" or statuses["verification"] == "missing":
        readiness = "blocked"
    elif gaps:
        readiness = "partial"
    else:
        readiness = "ready"
    return PlanSecretsRotationReadinessRow(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        gaps=gaps,
        readiness=readiness,
        evidence=tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _SECRET_RE.search(text) or _ROTATION_RE.search(text))),
        **statuses,
    )


def _status(
    pattern: re.Pattern[str],
    texts: Iterable[tuple[str, str]],
    *,
    skip_fields: tuple[str, ...] = (),
) -> str:
    return "present" if any(field not in skip_fields and pattern.search(text) for field, text in texts) else "missing"


def _summary(task_count: int, rows: Iterable[PlanSecretsRotationReadinessRow]) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "rotation_task_count": len(row_list),
        "no_rotation_task_count": task_count - len(row_list),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "gap_counts": {
            gap: sum(1 for row in row_list if gap in row.gaps)
            for gap in sorted({gap for row in row_list for gap in row.gaps})
        },
    }


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
    return hasattr(value, "tasks")


def _looks_like_task(value: Any) -> bool:
    return hasattr(value, "title") and hasattr(value, "description")
