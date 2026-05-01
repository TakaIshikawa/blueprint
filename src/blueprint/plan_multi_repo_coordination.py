"""Map execution-plan coordination boundaries across repos and components."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CoordinationBoundaryType = Literal["repository", "component"]
CoordinationRiskCode = Literal[
    "cross_repo_dependency",
    "shared_release_order",
    "contract_boundary",
    "ownership_split",
    "validation_gap",
]
_T = TypeVar("_T")

_METADATA_REPO_KEYS = {"repo", "repository", "target_repo"}
_METADATA_COMPONENT_KEYS = {"package", "service", "component"}
_DEPENDENCY_KEYS = ("depends_on", "dependencies", "blocked_by", "blockers")
_COMPONENT_ROOTS = {
    "app",
    "apps",
    "client",
    "clients",
    "component",
    "components",
    "lib",
    "libs",
    "module",
    "modules",
    "package",
    "packages",
    "service",
    "services",
}
_NAMED_COMPONENT_ROOTS = {
    "api",
    "backend",
    "cli",
    "docs",
    "frontend",
    "infra",
    "mobile",
    "server",
    "shared",
    "ui",
    "web",
    "worker",
    "workers",
}
_CONTRACT_RE = re.compile(
    r"\b(?:api|contract|openapi|graphql|grpc|schema|sdk|client|webhook|event|payload)\b",
    re.IGNORECASE,
)
_VALIDATION_RE = re.compile(
    r"\b(?:test|tests|validation|verify|smoke|integration|e2e|contract test|ci)\b",
    re.IGNORECASE,
)
_RELEASE_RE = re.compile(
    r"\b(?:release|rollout|deploy|deployment|migration|backfill|feature flag|launch)\b",
    re.IGNORECASE,
)
_REPO_SLUG_RE = re.compile(r"\b([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)\b")


@dataclass(frozen=True, slots=True)
class PlanMultiRepoCoordinationRecord:
    """One coordination boundary or cross-boundary dependency in an execution plan."""

    coordination_id: str
    coordination_type: Literal["boundary_group", "dependency_chain"]
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    repositories: tuple[str, ...] = field(default_factory=tuple)
    components: tuple[str, ...] = field(default_factory=tuple)
    risk_codes: tuple[CoordinationRiskCode, ...] = field(default_factory=tuple)
    recommended_sequence: str = ""
    recommended_actions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "coordination_id": self.coordination_id,
            "coordination_type": self.coordination_type,
            "task_ids": list(self.task_ids),
            "repositories": list(self.repositories),
            "components": list(self.components),
            "risk_codes": list(self.risk_codes),
            "recommended_sequence": self.recommended_sequence,
            "recommended_actions": list(self.recommended_actions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanMultiRepoCoordinationMap:
    """Coordination guidance for multi-repo or cross-component execution plans."""

    plan_id: str | None = None
    records: tuple[PlanMultiRepoCoordinationRecord, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return coordination records as JSON-compatible dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render multi-repo coordination guidance as deterministic Markdown."""
        title = "# Plan Multi-Repo Coordination Map"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No multi-repo or cross-component coordination boundaries detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Coordination | Tasks | Repos | Components | Risks | Sequence | Actions | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.coordination_id)} | "
                f"{_markdown_cell(', '.join(record.task_ids))} | "
                f"{_markdown_cell(', '.join(record.repositories))} | "
                f"{_markdown_cell(', '.join(record.components))} | "
                f"{_markdown_cell(', '.join(record.risk_codes))} | "
                f"{_markdown_cell(record.recommended_sequence)} | "
                f"{_markdown_cell('; '.join(record.recommended_actions))} | "
                f"{_markdown_cell('; '.join(record.evidence))} |"
            )
        return "\n".join(lines)


def build_plan_multi_repo_coordination_map(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanMultiRepoCoordinationMap:
    """Build a deterministic coordination map without mutating the input plan."""
    plan_id, plan_repo, tasks = _source_payload(source)
    task_records = tuple(
        _task_record(task, index, plan_repo) for index, task in enumerate(tasks, 1)
    )
    boundary_names = tuple(
        _dedupe(boundary.name for record in task_records for boundary in record.boundaries)
    )
    if len(boundary_names) <= 1:
        return PlanMultiRepoCoordinationMap(
            plan_id=plan_id,
            records=(),
            summary=_summary((), task_records, boundary_count=0),
        )

    records = [*_boundary_records(task_records), *_dependency_records(task_records)]
    records.sort(
        key=lambda record: (
            0 if record.coordination_type == "dependency_chain" else 1,
            record.coordination_id,
        )
    )
    result = tuple(records)
    return PlanMultiRepoCoordinationMap(
        plan_id=plan_id,
        records=result,
        summary=_summary(result, task_records, boundary_count=len(boundary_names)),
    )


def derive_plan_multi_repo_coordination_map(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanMultiRepoCoordinationMap:
    """Compatibility alias for building a multi-repo coordination map."""
    return build_plan_multi_repo_coordination_map(source)


def plan_multi_repo_coordination_map_to_dict(
    coordination_map: PlanMultiRepoCoordinationMap,
) -> dict[str, Any]:
    """Serialize a multi-repo coordination map to a plain dictionary."""
    return coordination_map.to_dict()


plan_multi_repo_coordination_map_to_dict.__test__ = False


def plan_multi_repo_coordination_map_to_dicts(
    coordination_map: PlanMultiRepoCoordinationMap,
) -> list[dict[str, Any]]:
    """Serialize multi-repo coordination records to plain dictionaries."""
    return coordination_map.to_dicts()


plan_multi_repo_coordination_map_to_dicts.__test__ = False


def plan_multi_repo_coordination_map_to_markdown(
    coordination_map: PlanMultiRepoCoordinationMap,
) -> str:
    """Render a multi-repo coordination map as Markdown."""
    return coordination_map.to_markdown()


plan_multi_repo_coordination_map_to_markdown.__test__ = False


def summarize_plan_multi_repo_coordination(
    coordination_map: PlanMultiRepoCoordinationMap,
) -> dict[str, Any]:
    """Return summary counts for a multi-repo coordination map."""
    return dict(coordination_map.summary)


summarize_plan_multi_repo_coordination.__test__ = False


plan_multi_repo_coordination_to_dict = plan_multi_repo_coordination_map_to_dict
plan_multi_repo_coordination_to_dict.__test__ = False
plan_multi_repo_coordination_to_dicts = plan_multi_repo_coordination_map_to_dicts
plan_multi_repo_coordination_to_dicts.__test__ = False
plan_multi_repo_coordination_to_markdown = plan_multi_repo_coordination_map_to_markdown
plan_multi_repo_coordination_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Boundary:
    kind: CoordinationBoundaryType
    name: str
    evidence: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    description: str
    depends_on: tuple[str, ...]
    boundaries: tuple[_Boundary, ...]
    owner: str | None
    validation_text: str
    release_text: str

    @property
    def repositories(self) -> tuple[str, ...]:
        return tuple(
            _dedupe(boundary.name for boundary in self.boundaries if boundary.kind == "repository")
        )

    @property
    def components(self) -> tuple[str, ...]:
        return tuple(
            _dedupe(boundary.name for boundary in self.boundaries if boundary.kind == "component")
        )

    @property
    def primary_boundary(self) -> str | None:
        return (
            self.components[0]
            if self.components
            else (self.repositories[0] if self.repositories else None)
        )


def _boundary_records(
    records: tuple[_TaskRecord, ...],
) -> list[PlanMultiRepoCoordinationRecord]:
    by_boundary: dict[str, list[_TaskRecord]] = {}
    boundary_by_name: dict[str, _Boundary] = {}
    for record in records:
        for boundary in record.boundaries:
            by_boundary.setdefault(boundary.name, []).append(record)
            boundary_by_name.setdefault(boundary.name, boundary)

    result: list[PlanMultiRepoCoordinationRecord] = []
    ordered_boundaries = sorted(
        by_boundary,
        key=lambda name: (0 if boundary_by_name[name].kind == "component" else 1, name),
    )
    for index, boundary_name in enumerate(ordered_boundaries, start=1):
        grouped = by_boundary[boundary_name]
        boundary = boundary_by_name[boundary_name]
        repositories = (
            (boundary.name,)
            if boundary.kind == "repository"
            else tuple(_dedupe(repo for record in grouped for repo in record.repositories))
        )
        components = (boundary.name,) if boundary.kind == "component" else ()
        result.append(
            PlanMultiRepoCoordinationRecord(
                coordination_id=f"boundary-{index}",
                coordination_type="boundary_group",
                task_ids=tuple(record.task_id for record in grouped),
                repositories=repositories,
                components=components,
                risk_codes=_boundary_risks(grouped),
                recommended_sequence=(
                    f"Keep {boundary.name} changes on an owned branch or PR; merge after "
                    "declared prerequisites and before dependent boundaries."
                ),
                recommended_actions=_boundary_actions(boundary, grouped),
                evidence=tuple(
                    _dedupe(evidence for record in grouped for evidence in _task_evidence(record))
                ),
            )
        )
    return result


def _dependency_records(
    records: tuple[_TaskRecord, ...],
) -> list[PlanMultiRepoCoordinationRecord]:
    by_id = {record.task_id: record for record in records}
    result: list[PlanMultiRepoCoordinationRecord] = []
    for record in records:
        for dependency_id in record.depends_on:
            dependency = by_id.get(dependency_id)
            if not dependency or dependency.primary_boundary == record.primary_boundary:
                continue
            risks = _dependency_risks(dependency, record)
            result.append(
                PlanMultiRepoCoordinationRecord(
                    coordination_id=f"dependency-{dependency.task_id}-to-{record.task_id}",
                    coordination_type="dependency_chain",
                    task_ids=(dependency.task_id, record.task_id),
                    repositories=tuple(_dedupe([*dependency.repositories, *record.repositories])),
                    components=tuple(_dedupe([*dependency.components, *record.components])),
                    risk_codes=risks,
                    recommended_sequence=(
                        f"Finish {dependency.task_id} in {dependency.primary_boundary or 'its boundary'} "
                        f"before starting {record.task_id} in {record.primary_boundary or 'its boundary'}."
                    ),
                    recommended_actions=_dependency_actions(risks),
                    evidence=tuple(
                        _dedupe(
                            [
                                f"depends_on: {record.task_id} -> {dependency.task_id}",
                                *_task_evidence(dependency),
                                *_task_evidence(record),
                            ]
                        )
                    ),
                )
            )
    return result


def _boundary_risks(records: list[_TaskRecord]) -> tuple[CoordinationRiskCode, ...]:
    risks: list[CoordinationRiskCode] = []
    owners = _dedupe(record.owner for record in records if record.owner)
    if len(owners) > 1:
        risks.append("ownership_split")
    if any(_CONTRACT_RE.search(_task_text(record)) for record in records):
        risks.append("contract_boundary")
    if any(_RELEASE_RE.search(record.release_text) for record in records):
        risks.append("shared_release_order")
    if any(not _VALIDATION_RE.search(record.validation_text) for record in records):
        risks.append("validation_gap")
    return tuple(_dedupe(risks))


def _dependency_risks(left: _TaskRecord, right: _TaskRecord) -> tuple[CoordinationRiskCode, ...]:
    risks: list[CoordinationRiskCode] = ["cross_repo_dependency", "shared_release_order"]
    text = f"{_task_text(left)} {_task_text(right)}"
    if _CONTRACT_RE.search(text):
        risks.append("contract_boundary")
    if left.owner and right.owner and left.owner != right.owner:
        risks.append("ownership_split")
    if not (
        _VALIDATION_RE.search(left.validation_text) and _VALIDATION_RE.search(right.validation_text)
    ):
        risks.append("validation_gap")
    return tuple(_dedupe(risks))


def _boundary_actions(boundary: _Boundary, records: list[_TaskRecord]) -> tuple[str, ...]:
    actions = [
        f"Assign one coordinator for {boundary.kind} {boundary.name}.",
        "Confirm branch ownership and merge window before dispatching parallel agents.",
    ]
    if any(_CONTRACT_RE.search(_task_text(record)) for record in records):
        actions.append(
            "Freeze the interface contract or schema before dependent implementation starts."
        )
    if any(not _VALIDATION_RE.search(record.validation_text) for record in records):
        actions.append("Add explicit validation evidence for this boundary before release.")
    return tuple(actions)


def _dependency_actions(risks: tuple[CoordinationRiskCode, ...]) -> tuple[str, ...]:
    actions = [
        "Serialize dependent work across the boundary instead of launching both branches blindly.",
        "Publish the prerequisite branch, artifact, or contract before starting the dependent task.",
    ]
    if "contract_boundary" in risks:
        actions.append("Add or run contract tests at the handoff point.")
    if "validation_gap" in risks:
        actions.append("Define validation ownership for both sides of the dependency.")
    return tuple(actions)


def _summary(
    records: tuple[PlanMultiRepoCoordinationRecord, ...],
    task_records: tuple[_TaskRecord, ...],
    *,
    boundary_count: int,
) -> dict[str, Any]:
    risk_counts = {
        risk: sum(1 for record in records if risk in record.risk_codes)
        for risk in (
            "cross_repo_dependency",
            "shared_release_order",
            "contract_boundary",
            "ownership_split",
            "validation_gap",
        )
    }
    return {
        "task_count": len(task_records),
        "record_count": len(records),
        "boundary_count": boundary_count,
        "repository_count": (
            len(_dedupe(repo for record in task_records for repo in record.repositories))
            if records
            else 0
        ),
        "component_count": (
            len(_dedupe(component for record in task_records for component in record.components))
            if records
            else 0
        ),
        "cross_boundary_dependency_count": sum(
            1 for record in records if record.coordination_type == "dependency_chain"
        ),
        "risk_counts": risk_counts,
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, str | None, list[dict[str, Any]]]:
    if source is None:
        return None, None, []
    if isinstance(source, ExecutionTask):
        return None, None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        plan = source.model_dump(mode="python")
        return (
            _optional_text(plan.get("id")),
            _optional_text(plan.get("target_repo")),
            [task.model_dump(mode="python") for task in source.tasks],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return (
                _optional_text(plan.get("id")),
                _optional_text(plan.get("target_repo")),
                _task_payloads(plan.get("tasks")),
            )
        return None, None, [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return (
            _optional_text(plan.get("id")),
            _optional_text(plan.get("target_repo")),
            _task_payloads(plan.get("tasks")),
        )

    tasks: list[dict[str, Any]] = []
    for item in source:
        if task := _task_payload(item):
            tasks.append(task)
    return None, None, tasks


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
    fields = (
        "id",
        "tasks",
        "title",
        "description",
        "milestone",
        "owner",
        "assignee",
        "owner_type",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "test_command",
        "validation_command",
        "validation_plan",
        "metadata",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _task_record(task: Mapping[str, Any], index: int, plan_repo: str | None) -> _TaskRecord:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    metadata = task.get("metadata")
    boundaries = _task_boundaries(task, plan_repo)
    return _TaskRecord(
        task=dict(task),
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        description=_optional_text(task.get("description")) or "",
        depends_on=tuple(
            _dedupe(_strings(task.get("depends_on")) + _strings(task.get("dependencies")))
        ),
        boundaries=boundaries,
        owner=(
            _optional_text(task.get("owner"))
            or _optional_text(task.get("assignee"))
            or _optional_text(_metadata_value(metadata, "owner"))
            or _optional_text(_metadata_value(metadata, "assignee"))
        ),
        validation_text=" ".join(
            [
                *_strings(task.get("acceptance_criteria")),
                *_strings(task.get("test_command")),
                *_strings(task.get("validation_command")),
                *_strings(task.get("validation_plan")),
                *_metadata_strings(metadata, {"test", "tests", "validation", "validation_plan"}),
            ]
        ),
        release_text=" ".join(
            [
                _optional_text(task.get("milestone")) or "",
                *_strings(task.get("acceptance_criteria")),
                *_metadata_strings(metadata, {"release", "rollout", "deploy", "deployment"}),
            ]
        ),
    )


def _task_boundaries(task: Mapping[str, Any], plan_repo: str | None) -> tuple[_Boundary, ...]:
    metadata = task.get("metadata")
    boundaries: list[_Boundary] = []

    for field_name in sorted(_METADATA_REPO_KEYS):
        for value in _strings(_metadata_value(metadata, field_name)):
            boundaries.append(_boundary("repository", value, f"metadata.{field_name}: {value}"))
    for field_name in sorted(_METADATA_COMPONENT_KEYS):
        for value in _strings(_metadata_value(metadata, field_name)):
            boundaries.append(_boundary("component", value, f"metadata.{field_name}: {value}"))

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if component := _component_from_path(path):
            boundaries.append(_boundary("component", component, f"files_or_modules: {path}"))

    for field_path, text in _task_texts(task):
        for match in _REPO_SLUG_RE.finditer(text):
            boundaries.append(_boundary("repository", match.group(1), f"{field_path}: {text}"))

    if not any(boundary.kind == "repository" for boundary in boundaries) and plan_repo:
        boundaries.append(_boundary("repository", plan_repo, f"target_repo: {plan_repo}"))

    merged: dict[tuple[str, str], list[str]] = {}
    for boundary in boundaries:
        key = (boundary.kind, boundary.name)
        merged.setdefault(key, []).extend(boundary.evidence)
    return tuple(
        _Boundary(kind=kind, name=name, evidence=tuple(_dedupe(evidence)))
        for (kind, name), evidence in sorted(
            merged.items(), key=lambda item: (item[0][0], item[0][1])
        )
    )


def _boundary(kind: CoordinationBoundaryType, value: str, evidence: str) -> _Boundary:
    name = _boundary_name(value)
    return _Boundary(kind=kind, name=name, evidence=(evidence,))


def _component_from_path(value: str) -> str | None:
    normalized = _normalized_path(value)
    if not normalized:
        return None
    parts = PurePosixPath(normalized).parts
    if len(parts) >= 2 and parts[0].casefold() in _COMPONENT_ROOTS:
        return f"{parts[0]}/{parts[1]}"
    if parts and parts[0].casefold() in _NAMED_COMPONENT_ROOTS:
        return parts[0]
    return None


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "test_command",
        "validation_command",
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    for field_path, text in _metadata_texts(task.get("metadata")):
        texts.append((field_path, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
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
    if text := _optional_text(value):
        return [(prefix, text)]
    return []


def _metadata_strings(metadata: Any, keys: set[str]) -> list[str]:
    if not isinstance(metadata, Mapping):
        return []
    values: list[str] = []
    for key, value in metadata.items():
        if str(key).casefold() in keys:
            values.extend(_strings(value))
    return values


def _task_evidence(record: _TaskRecord) -> tuple[str, ...]:
    return tuple(
        _dedupe(evidence for boundary in record.boundaries for evidence in boundary.evidence)
    )


def _task_text(record: _TaskRecord) -> str:
    return " ".join(
        [
            record.title,
            record.description,
            record.validation_text,
            record.release_text,
            " ".join(text for _, text in _metadata_texts(record.task.get("metadata"))),
        ]
    )


def _metadata_value(metadata: Any, key: str) -> Any:
    if not isinstance(metadata, Mapping):
        return None
    return metadata.get(key)


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
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _boundary_name(value: str) -> str:
    return _normalized_path(_text(value)).casefold()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


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
    "CoordinationBoundaryType",
    "CoordinationRiskCode",
    "PlanMultiRepoCoordinationMap",
    "PlanMultiRepoCoordinationRecord",
    "build_plan_multi_repo_coordination_map",
    "derive_plan_multi_repo_coordination_map",
    "plan_multi_repo_coordination_map_to_dict",
    "plan_multi_repo_coordination_map_to_dicts",
    "plan_multi_repo_coordination_map_to_markdown",
    "plan_multi_repo_coordination_to_dict",
    "plan_multi_repo_coordination_to_dicts",
    "plan_multi_repo_coordination_to_markdown",
    "summarize_plan_multi_repo_coordination",
]
