"""Build plan-level dependency owner escalation matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_EXTERNAL_SIGNAL_RE = re.compile(
    r"\b(?:external|cross[- ]team|dependency|depends on|blocked by|blocker|waiting on|"
    r"awaiting|vendor|partner|legal|security|privacy|compliance|data|infra|platform|"
    r"support|customer success|design|product|ops|sre|finance|approval|decision)\b",
    re.I,
)
_BLOCKED_RE = re.compile(r"\b(?:blocked|blocker|blocks|waiting on|awaiting|depends on|dependency|pending)\b", re.I)
_DECISION_RE = re.compile(
    r"\b(?:decision|decide|approval|approve|sign[- ]?off|choose|confirm|resolve|go/no[- ]go)\b",
    re.I,
)
_MISSING_VALUES = {"", "none", "null", "n/a", "na", "tbd", "todo", "unknown", "unassigned", "missing"}


@dataclass(frozen=True, slots=True)
class PlanDependencyOwnerEscalationRow:
    """Owner and escalation status for one external or cross-team dependency."""

    dependency_name: str
    blocked_task_ids: tuple[str, ...] = field(default_factory=tuple)
    current_owner: str | None = None
    missing_owner_fields: tuple[str, ...] = field(default_factory=tuple)
    escalation_recommendation: str = ""
    decision_needed: str = ""
    escalation_path: str | None = None
    target_response_date: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "dependency_name": self.dependency_name,
            "blocked_task_ids": list(self.blocked_task_ids),
            "current_owner": self.current_owner,
            "missing_owner_fields": list(self.missing_owner_fields),
            "escalation_recommendation": self.escalation_recommendation,
            "decision_needed": self.decision_needed,
            "escalation_path": self.escalation_path,
            "target_response_date": self.target_response_date,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanDependencyOwnerEscalationMatrix:
    """Plan-level dependency owner escalation matrix and rollup counts."""

    plan_id: str | None = None
    rows: tuple[PlanDependencyOwnerEscalationRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return escalation rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the escalation matrix as deterministic Markdown."""
        title = "# Plan Dependency Owner Escalation Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Dependency count: {self.summary.get('dependency_count', 0)}",
            f"- Dependencies missing owners: {self.summary.get('dependencies_missing_owners', 0)}",
            f"- Dependencies missing escalation paths: {self.summary.get('dependencies_missing_escalation_paths', 0)}",
            f"- Blocked task count: {self.summary.get('blocked_task_count', 0)}",
        ]
        if not self.rows:
            lines.extend(["", "No dependency-owner escalation signals were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Dependency | Blocked Tasks | Current Owner | Missing Fields | Escalation Recommendation | Decision Needed | Target Response Date |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.dependency_name)} | "
                f"{_markdown_cell(', '.join(row.blocked_task_ids))} | "
                f"{_markdown_cell(row.current_owner or 'missing')} | "
                f"{_markdown_cell('; '.join(row.missing_owner_fields) or 'none')} | "
                f"{_markdown_cell(row.escalation_recommendation)} | "
                f"{_markdown_cell(row.decision_needed)} | "
                f"{_markdown_cell(row.target_response_date or 'missing')} |"
            )
        return "\n".join(lines)


def build_plan_dependency_owner_escalation_matrix(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanDependencyOwnerEscalationMatrix:
    """Identify external or cross-team dependencies that need owner escalation."""
    plan_id, tasks = _source_payload(source)
    known_task_ids = {
        task_id
        for index, task in enumerate(tasks, start=1)
        if (task_id := _optional_text(task.get("id")) or f"task-{index}")
    }
    buckets: dict[str, _DependencyBucket] = {}
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        for signal in _dependency_signals(task, task_id, known_task_ids):
            bucket = buckets.setdefault(signal.key, _DependencyBucket(name=signal.name))
            bucket.blocked_task_ids.append(task_id)
            bucket.evidence.extend(signal.evidence)
            bucket.owners.extend(signal.owners)
            bucket.escalation_paths.extend(signal.escalation_paths)
            bucket.response_dates.extend(signal.response_dates)
            bucket.decisions.extend(signal.decisions)

    rows = tuple(_row_from_bucket(bucket) for _, bucket in sorted(buckets.items()))
    blocked_task_ids = {task_id for row in rows for task_id in row.blocked_task_ids}
    return PlanDependencyOwnerEscalationMatrix(
        plan_id=plan_id,
        rows=rows,
        summary={
            "dependency_count": len(rows),
            "dependencies_missing_owners": sum(1 for row in rows if "current_owner" in row.missing_owner_fields),
            "dependencies_missing_escalation_paths": sum(
                1 for row in rows if "escalation_path" in row.missing_owner_fields
            ),
            "blocked_task_count": len(blocked_task_ids),
        },
    )


def summarize_plan_dependency_owner_escalation(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanDependencyOwnerEscalationMatrix:
    """Compatibility alias for building dependency owner escalation matrices."""
    return build_plan_dependency_owner_escalation_matrix(source)


def plan_dependency_owner_escalation_matrix_to_dict(
    matrix: PlanDependencyOwnerEscalationMatrix,
) -> dict[str, Any]:
    """Serialize a dependency owner escalation matrix to a plain dictionary."""
    return matrix.to_dict()


plan_dependency_owner_escalation_matrix_to_dict.__test__ = False


def plan_dependency_owner_escalation_matrix_to_markdown(
    matrix: PlanDependencyOwnerEscalationMatrix,
) -> str:
    """Render a dependency owner escalation matrix as Markdown."""
    return matrix.to_markdown()


plan_dependency_owner_escalation_matrix_to_markdown.__test__ = False


@dataclass(slots=True)
class _DependencySignal:
    key: str
    name: str
    evidence: list[str] = field(default_factory=list)
    owners: list[str] = field(default_factory=list)
    escalation_paths: list[str] = field(default_factory=list)
    response_dates: list[str] = field(default_factory=list)
    decisions: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _DependencyBucket:
    name: str
    blocked_task_ids: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    owners: list[str] = field(default_factory=list)
    escalation_paths: list[str] = field(default_factory=list)
    response_dates: list[str] = field(default_factory=list)
    decisions: list[str] = field(default_factory=list)


def _dependency_signals(
    task: Mapping[str, Any],
    task_id: str,
    known_task_ids: set[str],
) -> list[_DependencySignal]:
    signals: list[_DependencySignal] = []
    metadata = task.get("metadata")
    for index, item in enumerate(_list_items(task.get("depends_on") or task.get("dependencies"))):
        signal = _signal_from_dependency_item(item, f"depends_on[{index}]", known_task_ids)
        if signal:
            signals.append(signal)

    for source_field in ("blockers", "blocked_by", "external_dependencies", "cross_team_dependencies"):
        for index, item in enumerate(_list_items(task.get(source_field) or _metadata_value(metadata, source_field))):
            signal = _signal_from_dependency_item(item, f"{source_field}[{index}]", known_task_ids)
            if signal:
                signals.append(signal)

    for source_field, text in _candidate_texts(task):
        if _EXTERNAL_SIGNAL_RE.search(text) and _BLOCKED_RE.search(text):
            name = _dependency_name_from_text(text)
            signals.append(
                _DependencySignal(
                    key=_dependency_key(name),
                    name=name,
                    evidence=[_evidence_snippet(source_field, text)],
                    owners=_field_values(task, ("owner", "dependency_owner", "current_owner")),
                    escalation_paths=_field_values(task, ("escalation_path", "escalation_owner", "escalate_to")),
                    response_dates=_field_values(task, ("target_response_date", "response_by", "due_date")),
                    decisions=_decision_values(task, text),
                )
            )
    return _merge_task_signals(signals)


def _signal_from_dependency_item(
    item: Any,
    source_field: str,
    known_task_ids: set[str],
) -> _DependencySignal | None:
    if isinstance(item, Mapping):
        name = (
            _optional_text(item.get("dependency"))
            or _optional_text(item.get("name"))
            or _optional_text(item.get("title"))
            or _optional_text(item.get("id"))
        )
        if not name:
            return None
        if _dependency_key(name) in {_dependency_key(task_id) for task_id in known_task_ids}:
            return None
        text = " ".join(_strings(item))
        return _DependencySignal(
            key=_dependency_key(name),
            name=name,
            evidence=[_evidence_snippet(source_field, text or name)],
            owners=_values_from_mapping(item, ("owner", "dependency_owner", "current_owner", "team", "assignee")),
            escalation_paths=_values_from_mapping(item, ("escalation_path", "escalation_owner", "escalate_to", "backup_owner")),
            response_dates=_values_from_mapping(item, ("target_response_date", "response_by", "respond_by", "due_date")),
            decisions=_values_from_mapping(item, ("decision_needed", "blocked_decision", "decision", "approval_needed")),
        )
    name = _optional_text(item)
    if not name or name in known_task_ids:
        return None
    if not (_EXTERNAL_SIGNAL_RE.search(name) or name not in known_task_ids):
        return None
    return _DependencySignal(
        key=_dependency_key(name),
        name=name,
        evidence=[_evidence_snippet(source_field, name)],
        decisions=[f"Resolve dependency before unblocking tasks: {name}"],
    )


def _row_from_bucket(bucket: _DependencyBucket) -> PlanDependencyOwnerEscalationRow:
    owner = _first_present(bucket.owners)
    escalation_path = _first_present(bucket.escalation_paths)
    response_date = _first_present(bucket.response_dates)
    missing = tuple(
        field_name
        for field_name, value in (
            ("current_owner", owner),
            ("escalation_path", escalation_path),
            ("target_response_date", response_date),
        )
        if not value
    )
    return PlanDependencyOwnerEscalationRow(
        dependency_name=bucket.name,
        blocked_task_ids=tuple(_dedupe(bucket.blocked_task_ids)),
        current_owner=owner,
        missing_owner_fields=missing,
        escalation_recommendation=_recommendation(bucket.name, owner, escalation_path, response_date),
        decision_needed=_first_present(bucket.decisions) or f"Confirm the decision needed to unblock {bucket.name}.",
        escalation_path=escalation_path,
        target_response_date=response_date,
        evidence=tuple(_dedupe(bucket.evidence)),
    )


def _recommendation(
    dependency_name: str,
    owner: str | None,
    escalation_path: str | None,
    response_date: str | None,
) -> str:
    if owner and escalation_path and response_date:
        return f"Track {dependency_name} with {owner}; escalate through {escalation_path} if no response by {response_date}."
    missing = []
    if not owner:
        missing.append("owner")
    if not escalation_path:
        missing.append("escalation path")
    if not response_date:
        missing.append("target response date")
    return f"Assign {', '.join(missing)} for {dependency_name} before dependent work continues."


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "blocked_reason", "notes"):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("risks", "acceptance_criteria", "criteria", "blockers", "tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _dependency_name_from_text(text: str) -> str:
    for pattern in (
        r"(?:blocked by|waiting on|awaiting|depends on|dependency on)\s+(?P<name>[^.;,\n]+)",
        r"(?P<name>[A-Z][A-Za-z0-9&/ ._-]{1,60})\s+(?:approval|decision|sign[- ]?off)",
    ):
        match = re.search(pattern, text, re.I)
        if match:
            return _clean_dependency_name(match.group("name"))
    return _clean_dependency_name(text)


def _clean_dependency_name(value: str) -> str:
    text = _text(value).strip("`'\" ")
    text = re.sub(r"\s+(?:before|to unblock|for|on|with)\b.*$", "", text, flags=re.I).strip()
    return text[:80].rstrip(".:;") or "External dependency"


def _merge_task_signals(signals: list[_DependencySignal]) -> list[_DependencySignal]:
    merged: dict[str, _DependencySignal] = {}
    for signal in signals:
        current = merged.setdefault(signal.key, _DependencySignal(key=signal.key, name=signal.name))
        current.evidence.extend(signal.evidence)
        current.owners.extend(signal.owners)
        current.escalation_paths.extend(signal.escalation_paths)
        current.response_dates.extend(signal.response_dates)
        current.decisions.extend(signal.decisions)
    for signal in merged.values():
        signal.evidence = _dedupe(signal.evidence)
        signal.owners = _dedupe(signal.owners)
        signal.escalation_paths = _dedupe(signal.escalation_paths)
        signal.response_dates = _dedupe(signal.response_dates)
        signal.decisions = _dedupe(signal.decisions)
    return list(merged.values())


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
    for item in source:
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


def _field_values(task: Mapping[str, Any], keys: tuple[str, ...]) -> list[str]:
    values: list[str] = []
    metadata = task.get("metadata")
    for key in keys:
        values.extend(_strings(task.get(key)))
        values.extend(_strings(_metadata_value(metadata, key)))
    return [value for value in values if not _is_missing(value)]


def _decision_values(task: Mapping[str, Any], text: str) -> list[str]:
    explicit = _field_values(task, ("decision_needed", "blocked_decision", "decision", "approval_needed"))
    if explicit:
        return explicit
    if _DECISION_RE.search(text):
        return [_evidence_snippet("decision", text).removeprefix("decision: ")]
    return []


def _values_from_mapping(mapping: Mapping[str, Any], keys: tuple[str, ...]) -> list[str]:
    values: list[str] = []
    for key in keys:
        values.extend(_strings(mapping.get(key)))
    return [value for value in values if not _is_missing(value)]


def _metadata_value(value: Any, key: str) -> Any:
    if not isinstance(value, Mapping):
        return None
    if key in value:
        return value[key]
    hyphen_key = key.replace("_", "-")
    if hyphen_key in value:
        return value[hyphen_key]
    return None


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                key_text = str(key).replace("_", " ").replace("-", " ")
                texts.append((field, f"{key_text}: {text}"))
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


def _list_items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(sorted(value, key=lambda item: str(item))) if isinstance(value, set) else list(value)
    return [value]


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


def _dependency_key(value: str) -> str:
    text = _clean_dependency_name(value).casefold()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return _SPACE_RE.sub(" ", text).strip()


def _first_present(values: Iterable[str]) -> str | None:
    for value in values:
        if not _is_missing(value):
            return value
    return None


def _is_missing(value: Any) -> bool:
    text = _text(value).casefold()
    return text in _MISSING_VALUES


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
    "PlanDependencyOwnerEscalationMatrix",
    "PlanDependencyOwnerEscalationRow",
    "build_plan_dependency_owner_escalation_matrix",
    "plan_dependency_owner_escalation_matrix_to_dict",
    "plan_dependency_owner_escalation_matrix_to_markdown",
    "summarize_plan_dependency_owner_escalation",
]
