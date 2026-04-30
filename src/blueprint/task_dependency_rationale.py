"""Annotate execution-plan task dependencies with likely rationale."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


_TOKEN_RE = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True, slots=True)
class TaskDependencyRationale:
    """Explanation for one declared ``depends_on`` relationship."""

    task_id: str
    dependency_id: str
    rationale: str
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "dependency_id": self.dependency_id,
            "rationale": self.rationale,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


def annotate_dependency_rationales(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> tuple[TaskDependencyRationale, ...]:
    """Explain every declared task dependency in an execution plan.

    The input is validated and copied into plain Python data before analysis so
    callers can pass dictionaries or domain models without the original objects
    being mutated.
    """
    plan_payload = _plan_payload(plan)
    tasks = _task_payloads(plan_payload.get("tasks"))
    records = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records_by_id = {record["task_id"]: record for record in records}
    milestone_order = _milestone_order(plan_payload, records)

    annotations: list[TaskDependencyRationale] = []
    for record in records:
        for dependency_id in _strings(record["task"].get("depends_on")):
            dependency = records_by_id.get(dependency_id)
            if dependency is None:
                annotations.append(_missing_dependency_annotation(record, dependency_id))
                continue
            annotations.append(
                _dependency_annotation(
                    task=record,
                    dependency=dependency,
                    milestone_order=milestone_order,
                )
            )

    return tuple(annotations)


def task_dependency_rationales_to_dict(
    annotations: tuple[TaskDependencyRationale, ...] | list[TaskDependencyRationale],
) -> list[dict[str, Any]]:
    """Serialize dependency rationale annotations to dictionaries."""
    return [annotation.to_dict() for annotation in annotations]


task_dependency_rationales_to_dict.__test__ = False


def _dependency_annotation(
    *,
    task: dict[str, Any],
    dependency: dict[str, Any],
    milestone_order: dict[str, int],
) -> TaskDependencyRationale:
    sources: list[tuple[str, float, str, str]] = []
    task_id = task["task_id"]
    dependency_id = dependency["task_id"]
    dependency_title = dependency["title"]

    metadata_reason = _metadata_dependency_reason(task["task"], dependency_id)
    if metadata_reason:
        sources.append(
            (
                "metadata",
                0.92,
                f"metadata states this dependency: {metadata_reason}",
                f"metadata dependency rationale for {dependency_id}: {metadata_reason}",
            )
        )

    mention_evidence = _explicit_mention_evidence(task, dependency)
    if mention_evidence:
        sources.append(
            (
                "explicit mention",
                0.88,
                f"task text explicitly references {dependency_title} before this work",
                mention_evidence,
            )
        )

    shared_paths = _shared_files(task["task"], dependency["task"])
    if shared_paths:
        sources.append(
            (
                "file overlap",
                0.82,
                "both tasks touch the same files or modules",
                "shared files_or_modules: " + ", ".join(shared_paths),
            )
        )

    milestone_evidence = _milestone_evidence(task, dependency, milestone_order)
    if milestone_evidence:
        sources.append(
            (
                "milestone order",
                0.72,
                milestone_evidence[0],
                milestone_evidence[1],
            )
        )

    if not sources:
        return TaskDependencyRationale(
            task_id=task_id,
            dependency_id=dependency_id,
            rationale=f"{task_id} declares {dependency_id} as a prerequisite.",
            confidence=0.5,
            evidence=(f"depends_on includes {dependency_id}",),
        )

    confidence = _confidence(sources)
    rationale = _rationale(sources)
    evidence = tuple(_dedupe(source[3] for source in sources))
    return TaskDependencyRationale(
        task_id=task_id,
        dependency_id=dependency_id,
        rationale=rationale,
        confidence=confidence,
        evidence=evidence,
    )


def _missing_dependency_annotation(
    task: dict[str, Any],
    dependency_id: str,
) -> TaskDependencyRationale:
    task_id = task["task_id"]
    return TaskDependencyRationale(
        task_id=task_id,
        dependency_id=dependency_id,
        rationale=f"{task_id} references missing dependency {dependency_id}.",
        confidence=0.2,
        evidence=(f"depends_on includes {dependency_id}, but no task with that id exists.",),
    )


def _confidence(sources: list[tuple[str, float, str, str]]) -> float:
    base = max(source[1] for source in sources)
    bonus = min((len(sources) - 1) * 0.03, 0.09)
    return round(min(base + bonus, 0.95), 2)


def _rationale(sources: list[tuple[str, float, str, str]]) -> str:
    ordered = sorted(sources, key=lambda source: (-source[1], source[0]))
    return "; ".join(_dedupe(source[2] for source in ordered))


def _explicit_mention_evidence(task: dict[str, Any], dependency: dict[str, Any]) -> str | None:
    dependency_id = dependency["task_id"]
    dependency_title = dependency["title"]
    needles = _mention_needles(dependency_id, dependency_title)
    for source, text in _task_text_fields(task["task"]):
        normalized = _normalized_text(text)
        for needle in needles:
            if needle and needle in normalized:
                return f"{source} mentions {dependency_title}: {text}"
    return None


def _mention_needles(dependency_id: str, dependency_title: str) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            needle
            for needle in (
                _normalized_text(dependency_id),
                _normalized_text(dependency_title),
                _normalized_text(dependency_id.replace("-", " ")),
            )
            if needle
        )
    )


def _shared_files(task: Mapping[str, Any], dependency: Mapping[str, Any]) -> tuple[str, ...]:
    task_paths = {_path_key(path): path for path in _strings(task.get("files_or_modules"))}
    dependency_paths = {
        _path_key(path): path for path in _strings(dependency.get("files_or_modules"))
    }
    shared_keys = sorted(key for key in task_paths if key and key in dependency_paths)
    return tuple(task_paths[key] for key in shared_keys)


def _path_key(path: str) -> str:
    text = path.strip().replace("\\", "/")
    if not text:
        return ""
    try:
        return PurePosixPath(text).as_posix().casefold()
    except ValueError:
        return text.casefold()


def _milestone_evidence(
    task: dict[str, Any],
    dependency: dict[str, Any],
    milestone_order: dict[str, int],
) -> tuple[str, str] | None:
    task_milestone = _optional_text(task["task"].get("milestone"))
    dependency_milestone = _optional_text(dependency["task"].get("milestone"))
    if not task_milestone or not dependency_milestone or task_milestone == dependency_milestone:
        return None

    task_rank = milestone_order.get(task_milestone)
    dependency_rank = milestone_order.get(dependency_milestone)
    if task_rank is None or dependency_rank is None or dependency_rank >= task_rank:
        return None

    reason = (
        f"{dependency['task_id']} belongs to earlier milestone "
        f"{dependency_milestone!r} before {task_milestone!r}"
    )
    evidence = (
        f"milestone order: {dependency_milestone} precedes {task_milestone}"
    )
    return (reason, evidence)


def _metadata_dependency_reason(task: Mapping[str, Any], dependency_id: str) -> str | None:
    metadata = task.get("metadata")
    if not isinstance(metadata, Mapping):
        return None

    for key in ("dependency_rationales", "dependency_reasons", "depends_on_rationales"):
        value = metadata.get(key)
        if isinstance(value, Mapping):
            reason = _optional_text(value.get(dependency_id))
            if reason:
                return reason

    for key in ("dependency_rationale", "dependency_reason", "depends_on_rationale"):
        reason = _optional_text(metadata.get(key))
        if reason and dependency_id.casefold() in reason.casefold():
            return reason
    return None


def _milestone_order(
    plan_payload: Mapping[str, Any],
    records: list[dict[str, Any]],
) -> dict[str, int]:
    order: dict[str, int] = {}
    milestones = plan_payload.get("milestones")
    if isinstance(milestones, list):
        for index, milestone in enumerate(milestones, start=1):
            if isinstance(milestone, Mapping):
                milestone_text = (
                    _optional_text(milestone.get("id"))
                    or _optional_text(milestone.get("name"))
                    or _optional_text(milestone.get("title"))
                )
            else:
                milestone_text = _optional_text(milestone)
            if milestone_text and milestone_text not in order:
                order[milestone_text] = index

    next_rank = len(order) + 1
    for record in records:
        milestone = _optional_text(record["task"].get("milestone"))
        if milestone and milestone not in order:
            order[milestone] = next_rank
            next_rank += 1
    return order


def _task_record(task: dict[str, Any], index: int) -> dict[str, Any]:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    return {
        "index": index,
        "task_id": task_id,
        "title": _optional_text(task.get("title")) or task_id,
        "task": task,
    }


def _task_text_fields(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    for key in ("title", "description", "milestone", "acceptance_criteria"):
        _append_text_fields(fields, key, task.get(key))
    _append_metadata_text(fields, task.get("metadata"))
    return fields


def _append_metadata_text(fields: list[tuple[str, str]], value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=str):
            _append_text_fields(fields, f"metadata.{key}", value[key])


def _append_text_fields(fields: list[tuple[str, str]], source: str, value: Any) -> None:
    if isinstance(value, str):
        text = _optional_text(value)
        if text:
            fields.append((source, text))
        return
    if isinstance(value, list):
        for index, item in enumerate(value, start=1):
            _append_text_fields(fields, f"{source}.{index}", item)
        return
    if isinstance(value, Mapping):
        for key in sorted(value, key=str):
            _append_text_fields(fields, f"{source}.{key}", value[key])


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _strings(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple, set)):
        return []
    strings: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = _optional_text(item)
        if text and text not in seen:
            strings.append(text)
            seen.add(text)
    return strings


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    text = " ".join(value.split())
    return text or None


def _normalized_text(value: str) -> str:
    return " ".join(_TOKEN_RE.findall(value.casefold()))


def _dedupe(values: Any) -> list[Any]:
    deduped: list[Any] = []
    seen: set[Any] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "TaskDependencyRationale",
    "annotate_dependency_rationales",
    "task_dependency_rationales_to_dict",
]
