"""Summarize handoff contracts between dependent execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


_TOKEN_RE = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True, slots=True)
class DependencyContract:
    """Contract expected by one task from one declared dependency."""

    task_id: str
    dependency_id: str
    contract_points: tuple[str, ...] = field(default_factory=tuple)
    missing_contract_points: tuple[str, ...] = field(default_factory=tuple)
    review_required: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "dependency_id": self.dependency_id,
            "contract_points": list(self.contract_points),
            "missing_contract_points": list(self.missing_contract_points),
            "review_required": self.review_required,
        }


def build_dependency_contracts(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> tuple[DependencyContract, ...]:
    """Build dependency handoff contracts for every declared task dependency."""
    payload = _plan_payload(plan)
    task_records = _task_records(_task_payloads(payload.get("tasks")))
    tasks_by_id = {record["task_id"]: record for record in task_records}

    contracts: list[DependencyContract] = []
    for record in task_records:
        for dependency_id in _string_list(record["task"].get("depends_on")):
            dependency = tasks_by_id.get(dependency_id)
            if dependency is None:
                contracts.append(_missing_dependency_contract(record, dependency_id))
                continue
            contracts.append(_dependency_contract(record, dependency))
    return tuple(contracts)


def dependency_contracts_to_dicts(
    contracts: tuple[DependencyContract, ...] | list[DependencyContract],
) -> list[dict[str, Any]]:
    """Serialize dependency contracts to dictionaries."""
    return [contract.to_dict() for contract in contracts]


dependency_contracts_to_dicts.__test__ = False


def _dependency_contract(
    task: dict[str, Any],
    dependency: dict[str, Any],
) -> DependencyContract:
    task_payload = task["task"]
    dependency_payload = dependency["task"]
    dependency_id = dependency["task_id"]

    contract_points = _dedupe(
        [
            f"prerequisite title: {dependency['title']}",
            *(
                f"acceptance criteria: {criterion}"
                for criterion in _string_list(dependency_payload.get("acceptance_criteria"))
            ),
            *(
                f"artifact: {artifact}"
                for artifact in _metadata_artifacts(dependency_payload.get("metadata"))
            ),
            *(
                f"blocked reason: {reason}"
                for reason in _blocked_reasons(task_payload, dependency_payload)
            ),
            *(
                f"dependent assumption: {assumption}"
                for assumption in _dependent_assumptions(task_payload, dependency)
            ),
        ]
    )
    missing_contract_points = _missing_contract_points(dependency_payload)

    return DependencyContract(
        task_id=task["task_id"],
        dependency_id=dependency_id,
        contract_points=tuple(contract_points),
        missing_contract_points=tuple(missing_contract_points),
        review_required=bool(missing_contract_points),
    )


def _missing_dependency_contract(
    task: dict[str, Any],
    dependency_id: str,
) -> DependencyContract:
    return DependencyContract(
        task_id=task["task_id"],
        dependency_id=dependency_id,
        contract_points=(
            f"missing dependency: {dependency_id} is declared but no task record exists",
        ),
        missing_contract_points=("dependency task record",),
        review_required=True,
    )


def _missing_contract_points(dependency: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    if not _string_list(dependency.get("acceptance_criteria")):
        missing.append("prerequisite acceptance criteria")
    if not _metadata_artifacts(dependency.get("metadata")):
        missing.append("prerequisite metadata artifacts")
    return missing


def _blocked_reasons(
    task: Mapping[str, Any], dependency: Mapping[str, Any]
) -> tuple[str, ...]:
    reasons = [
        reason
        for reason in (
            _optional_text(dependency.get("blocked_reason")),
            _optional_text(task.get("blocked_reason")),
        )
        if reason
    ]
    return tuple(_dedupe(reasons))


def _dependent_assumptions(
    task: Mapping[str, Any],
    dependency: dict[str, Any],
) -> tuple[str, ...]:
    dependency_id = dependency["task_id"]
    metadata = task.get("metadata")
    if not isinstance(metadata, Mapping):
        return ()

    assumptions: list[str] = []
    for key in (
        "dependency_assumptions",
        "depends_on_assumptions",
        "prerequisite_assumptions",
    ):
        value = metadata.get(key)
        if isinstance(value, Mapping):
            assumptions.extend(_strings_from_value(value.get(dependency_id)))
        else:
            assumptions.extend(_strings_from_value(value))

    assumptions.extend(_strings_from_value(metadata.get("assumptions")))
    return tuple(_dedupe(_matching_assumptions(assumptions, dependency)))


def _matching_assumptions(
    assumptions: list[str], dependency: dict[str, Any]
) -> list[str]:
    needles = {
        _normalized_text(dependency["task_id"]),
        _normalized_text(dependency["title"]),
        _normalized_text(dependency["task_id"].replace("-", " ")),
    }
    needles.discard("")

    matches: list[str] = []
    for assumption in assumptions:
        normalized = _normalized_text(assumption)
        if any(needle and needle in normalized for needle in needles):
            matches.append(assumption)
    return matches


def _metadata_artifacts(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Mapping):
        return ()

    artifacts: list[str] = []
    for key in (
        "artifacts",
        "expected_artifacts",
        "output_artifacts",
        "required_artifacts",
        "deliverables",
    ):
        artifacts.extend(_artifact_strings(value.get(key), prefix=None))
    return tuple(_dedupe(artifacts))


def _artifact_strings(value: Any, *, prefix: str | None) -> list[str]:
    text = _optional_text(value)
    if text:
        return [f"{prefix}: {text}" if prefix else text]

    if isinstance(value, Mapping):
        artifacts: list[str] = []
        for key in sorted(value, key=str):
            text_key = _optional_text(key)
            nested_prefix = text_key if prefix is None else f"{prefix}.{text_key}"
            artifacts.extend(_artifact_strings(value[key], prefix=nested_prefix))
        return artifacts

    if isinstance(value, (list, tuple, set)):
        artifacts = []
        items = sorted(value, key=str) if isinstance(value, set) else value
        for item in items:
            artifacts.extend(_artifact_strings(item, prefix=prefix))
        return artifacts

    return []


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


def _task_records(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        records.append(
            {
                "index": index,
                "task_id": task_id,
                "title": _optional_text(task.get("title")) or task_id,
                "task": task,
            }
        )
    return records


def _strings_from_value(value: Any) -> list[str]:
    text = _optional_text(value)
    if text:
        return [text]
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=str):
            strings.extend(_strings_from_value(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        return [text for item in items if (text := _optional_text(item))]
    return []


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=str) if isinstance(value, set) else value
    return [text for item in items if (text := _optional_text(item))]


def _optional_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = " ".join(value.split())
    return text or None


def _normalized_text(value: str) -> str:
    return " ".join(_TOKEN_RE.findall(value.casefold()))


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "DependencyContract",
    "build_dependency_contracts",
    "dependency_contracts_to_dicts",
]
