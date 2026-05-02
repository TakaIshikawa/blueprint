"""Recommend input validation readiness checks for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


InputValidationSurface = Literal[
    "form",
    "api_payload",
    "import_parser",
    "cli_argument",
    "config_file",
    "user_generated_input",
]
InputValidationAcceptanceCriterion = Literal[
    "schema_validation",
    "boundary_values",
    "malformed_payloads",
    "required_fields",
    "user_visible_errors",
    "backward_compatible_rollout",
]
InputValidationRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[InputValidationRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER: tuple[InputValidationSurface, ...] = (
    "form",
    "api_payload",
    "import_parser",
    "cli_argument",
    "config_file",
    "user_generated_input",
)
_ACCEPTANCE_ORDER: tuple[InputValidationAcceptanceCriterion, ...] = (
    "schema_validation",
    "boundary_values",
    "malformed_payloads",
    "required_fields",
    "user_visible_errors",
    "backward_compatible_rollout",
)
_TEXT_SURFACE_PATTERNS: dict[InputValidationSurface, re.Pattern[str]] = {
    "form": re.compile(
        r"\b(?:forms?|form fields?|signup|sign[- ]up|registration|checkout form|settings form|"
        r"profile form|wizard|input fields?|client input)\b",
        re.I,
    ),
    "api_payload": re.compile(
        r"\b(?:api payloads?|request bodies?|json payloads?|graphql inputs?|rest endpoints?|"
        r"endpoint payloads?|webhook payloads?|post body|put body|patch body)\b",
        re.I,
    ),
    "import_parser": re.compile(
        r"\b(?:imports?|import parsers?|csv parsers?|csv import|spreadsheet import|bulk import|"
        r"file ingest(?:ion)?|parse uploaded|parser validation|external feed)\b",
        re.I,
    ),
    "cli_argument": re.compile(
        r"\b(?:cli arguments?|command[- ]line arguments?|command line flags?|argv|argparse|click command|"
        r"terminal command|flags? and options?)\b",
        re.I,
    ),
    "config_file": re.compile(
        r"\b(?:config files?|configuration files?|yaml config|json config|toml config|env vars?|"
        r"environment variables?|settings file|feature config)\b",
        re.I,
    ),
    "user_generated_input": re.compile(
        r"\b(?:user[- ]generated input|user[- ]generated content|ugc|user[- ]supplied input|"
        r"user[- ]provided input|free[- ]text|comments?|messages?|untrusted input|customer input)\b",
        re.I,
    ),
}
_PATH_SURFACE_PATTERNS: dict[InputValidationSurface, re.Pattern[str]] = {
    "form": re.compile(r"(?:^|/)(?:forms?|fields?|signup|registration|checkout)(?:/|\.|_|-|$)", re.I),
    "api_payload": re.compile(
        r"(?:^|/)(?:api|routes?|endpoints?|controllers?|requests?|payloads?|schemas?)(?:/|\.|_|-|$)",
        re.I,
    ),
    "import_parser": re.compile(
        r"(?:^|/)(?:imports?|importers?|parsers?|csv|ingest(?:ion)?|feeds?)(?:/|\.|_|-|$)",
        re.I,
    ),
    "cli_argument": re.compile(r"(?:^|/)(?:cli|commands?|argparse|click|argv)(?:/|\.|_|-|$)", re.I),
    "config_file": re.compile(
        r"(?:^|/)(?:configs?|configuration|settings|env|yaml|json|toml)(?:/|\.|_|-|$)",
        re.I,
    ),
    "user_generated_input": re.compile(
        r"(?:^|/)(?:ugc|comments?|messages?|content|user[-_]?input)(?:/|\.|_|-|$)",
        re.I,
    ),
}
_ACCEPTANCE_PATTERNS: dict[InputValidationAcceptanceCriterion, re.Pattern[str]] = {
    "schema_validation": re.compile(
        r"\b(?:schema validation|validate schema|json schema|openapi schema|pydantic|zod|yup|"
        r"type validation|contract validation|validator)\b",
        re.I,
    ),
    "boundary_values": re.compile(
        r"\b(?:boundary values?|min(?:imum)?|max(?:imum)?|range checks?|length limits?|size limits?|"
        r"empty string|too long|negative|zero|overflow|underflow|edge cases?)\b",
        re.I,
    ),
    "malformed_payloads": re.compile(
        r"\b(?:malformed payloads?|invalid json|bad request|invalid input|parse failures?|"
        r"corrupt(?:ed)? files?|reject malformed|invalid types?|wrong types?)\b",
        re.I,
    ),
    "required_fields": re.compile(
        r"\b(?:required fields?|mandatory fields?|missing fields?|presence validation|must be provided|"
        r"cannot be blank|required inputs?)\b",
        re.I,
    ),
    "user_visible_errors": re.compile(
        r"\b(?:user[- ]visible errors?|inline errors?|field errors?|error messages?|validation messages?|"
        r"helpful errors?|error handling|show errors?|422 response|400 response)\b",
        re.I,
    ),
    "backward_compatible_rollout": re.compile(
        r"\b(?:backward compatible|backwards compatible|backward-compatible|legacy clients?|old clients?|"
        r"versioned validation|soft validation|warn-only|compatibility rollout|non-breaking)\b",
        re.I,
    ),
}
_SUGGESTED_CHECKS: dict[InputValidationAcceptanceCriterion, str] = {
    "schema_validation": "Define schema or type validation at the first trusted boundary.",
    "boundary_values": "Add boundary value coverage for length, range, empty, and extreme inputs.",
    "malformed_payloads": "Reject malformed payloads and parser failures with deterministic status and errors.",
    "required_fields": "Specify required fields and missing-field behavior for each input surface.",
    "user_visible_errors": "Require user-visible validation errors that are actionable and stable.",
    "backward_compatible_rollout": "Plan backward-compatible validation rollout for legacy clients and existing data.",
}


@dataclass(frozen=True, slots=True)
class TaskInputValidationReadinessRecord:
    """Input validation readiness guidance for one execution task."""

    task_id: str
    title: str
    validation_surfaces: tuple[InputValidationSurface, ...]
    risk_level: InputValidationRiskLevel
    present_acceptance_criteria: tuple[InputValidationAcceptanceCriterion, ...] = field(
        default_factory=tuple
    )
    missing_acceptance_criteria: tuple[InputValidationAcceptanceCriterion, ...] = field(
        default_factory=tuple
    )
    suggested_validation_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "validation_surfaces": list(self.validation_surfaces),
            "risk_level": self.risk_level,
            "present_acceptance_criteria": list(self.present_acceptance_criteria),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "suggested_validation_checks": list(self.suggested_validation_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskInputValidationReadinessPlan:
    """Task-level input validation readiness recommendations."""

    plan_id: str | None = None
    records: tuple[TaskInputValidationReadinessRecord, ...] = field(default_factory=tuple)
    validation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskInputValidationReadinessRecord, ...]:
        """Compatibility view matching planners that call extracted items recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "validation_task_ids": list(self.validation_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render input validation readiness as deterministic Markdown."""
        title = "# Task Input Validation Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Validation task count: {self.summary.get('validation_task_count', 0)}",
            f"- Missing acceptance criteria count: {self.summary.get('missing_acceptance_criteria_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No input validation readiness records were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Validation Surfaces | Missing Acceptance Criteria | Suggested Validation Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.validation_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(record.suggested_validation_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_input_validation_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskInputValidationReadinessPlan:
    """Build input validation readiness recommendations for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    ignored_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskInputValidationReadinessPlan(
        plan_id=plan_id,
        records=records,
        validation_task_ids=tuple(record.task_id for record in records),
        ignored_task_ids=ignored_task_ids,
        summary=_summary(records, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def analyze_task_input_validation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskInputValidationReadinessPlan:
    """Compatibility alias for building input validation readiness plans."""
    return build_task_input_validation_readiness_plan(source)


def summarize_task_input_validation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskInputValidationReadinessPlan:
    """Compatibility alias for building input validation readiness plans."""
    return build_task_input_validation_readiness_plan(source)


def extract_task_input_validation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskInputValidationReadinessPlan:
    """Compatibility alias for building input validation readiness plans."""
    return build_task_input_validation_readiness_plan(source)


def generate_task_input_validation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskInputValidationReadinessPlan:
    """Compatibility alias for generating input validation readiness plans."""
    return build_task_input_validation_readiness_plan(source)


def recommend_task_input_validation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskInputValidationReadinessPlan:
    """Compatibility alias for recommending input validation readiness plans."""
    return build_task_input_validation_readiness_plan(source)


def task_input_validation_readiness_plan_to_dict(
    result: TaskInputValidationReadinessPlan,
) -> dict[str, Any]:
    """Serialize an input validation readiness plan to a plain dictionary."""
    return result.to_dict()


task_input_validation_readiness_plan_to_dict.__test__ = False


def task_input_validation_readiness_plan_to_dicts(
    result: TaskInputValidationReadinessPlan
    | Iterable[TaskInputValidationReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize input validation readiness records to plain dictionaries."""
    if isinstance(result, TaskInputValidationReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_input_validation_readiness_plan_to_dicts.__test__ = False


def task_input_validation_readiness_plan_to_markdown(
    result: TaskInputValidationReadinessPlan,
) -> str:
    """Render an input validation readiness plan as Markdown."""
    return result.to_markdown()


task_input_validation_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[InputValidationSurface, ...] = field(default_factory=tuple)
    surface_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_criteria: tuple[InputValidationAcceptanceCriterion, ...] = field(default_factory=tuple)
    criteria_evidence: tuple[str, ...] = field(default_factory=tuple)


def _record(task: Mapping[str, Any], index: int) -> TaskInputValidationReadinessRecord | None:
    signals = _signals(task)
    if not signals.surfaces:
        return None

    missing = tuple(
        criterion for criterion in _ACCEPTANCE_ORDER if criterion not in signals.present_criteria
    )
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskInputValidationReadinessRecord(
        task_id=task_id,
        title=title,
        validation_surfaces=signals.surfaces,
        risk_level=_risk_level(task, signals.surfaces, missing),
        present_acceptance_criteria=signals.present_criteria,
        missing_acceptance_criteria=missing,
        suggested_validation_checks=tuple(_SUGGESTED_CHECKS[criterion] for criterion in missing),
        evidence=tuple(_dedupe([*signals.surface_evidence, *signals.criteria_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surface_hits: set[InputValidationSurface] = set()
    criteria_hits: set[InputValidationAcceptanceCriterion] = set()
    surface_evidence: list[str] = []
    criteria_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_surfaces = _path_surfaces(normalized)
        if path_surfaces:
            surface_hits.update(path_surfaces)
            surface_evidence.append(f"files_or_modules: {path}")
        for criterion, pattern in _ACCEPTANCE_PATTERNS.items():
            if pattern.search(searchable) or pattern.search(normalized):
                criteria_hits.add(criterion)
                criteria_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_surface = False
        for surface, pattern in _TEXT_SURFACE_PATTERNS.items():
            if pattern.search(text):
                surface_hits.add(surface)
                matched_surface = True
        if matched_surface:
            surface_evidence.append(snippet)
        for criterion, pattern in _ACCEPTANCE_PATTERNS.items():
            if pattern.search(text):
                criteria_hits.add(criterion)
                criteria_evidence.append(snippet)

    return _Signals(
        surfaces=tuple(surface for surface in _SURFACE_ORDER if surface in surface_hits),
        surface_evidence=tuple(_dedupe(surface_evidence)),
        present_criteria=tuple(criterion for criterion in _ACCEPTANCE_ORDER if criterion in criteria_hits),
        criteria_evidence=tuple(_dedupe(criteria_evidence)),
    )


def _path_surfaces(path: str) -> set[InputValidationSurface]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    surfaces: set[InputValidationSurface] = set()
    for surface, pattern in _PATH_SURFACE_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(text):
            surfaces.add(surface)
    name = PurePosixPath(normalized).name
    if re.search(r"\b(?:schema|payload|request|endpoint)\b", text):
        surfaces.add("api_payload")
    if re.search(r"\b(?:csv|import|parser|ingest)\b", text):
        surfaces.add("import_parser")
    if name in {"cli.py", "commands.py", "command.py"}:
        surfaces.add("cli_argument")
    if re.search(r"\b(?:config|settings|yaml|json|toml|env)\b", text):
        surfaces.add("config_file")
    return surfaces


def _risk_level(
    task: Mapping[str, Any],
    surfaces: tuple[InputValidationSurface, ...],
    missing: tuple[InputValidationAcceptanceCriterion, ...],
) -> InputValidationRiskLevel:
    risk_text = " ".join(_strings(task.get("risk_level"))).casefold()
    if "high" in risk_text:
        return "high"
    if {"api_payload", "import_parser", "config_file", "user_generated_input"} & set(surfaces):
        return "high" if len(missing) >= 3 else "medium"
    if {"form", "cli_argument"} & set(surfaces):
        return "medium" if len(missing) >= 3 else "low"
    return "low"


def _summary(
    records: tuple[TaskInputValidationReadinessRecord, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "validation_task_count": len(records),
        "ignored_task_ids": list(ignored_task_ids),
        "missing_acceptance_criteria_count": sum(
            len(record.missing_acceptance_criteria) for record in records
        ),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk) for risk in _RISK_ORDER
        },
        "missing_acceptance_criteria_counts": {
            criterion: sum(1 for record in records if criterion in record.missing_acceptance_criteria)
            for criterion in _ACCEPTANCE_ORDER
        },
        "surface_counts": {
            surface: sum(1 for record in records if surface in record.validation_surfaces)
            for surface in sorted({surface for record in records for surface in record.validation_surfaces})
        },
        "validation_task_ids": [record.task_id for record in records],
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
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
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


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
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
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
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
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
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
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


def _metadata_key_is_signal(value: str) -> bool:
    return any(
        pattern.search(value) for pattern in [*_TEXT_SURFACE_PATTERNS.values(), *_ACCEPTANCE_PATTERNS.values()]
    )


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


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


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


__all__ = [
    "InputValidationAcceptanceCriterion",
    "InputValidationRiskLevel",
    "InputValidationSurface",
    "TaskInputValidationReadinessPlan",
    "TaskInputValidationReadinessRecord",
    "analyze_task_input_validation_readiness",
    "build_task_input_validation_readiness_plan",
    "extract_task_input_validation_readiness",
    "generate_task_input_validation_readiness",
    "recommend_task_input_validation_readiness",
    "summarize_task_input_validation_readiness",
    "task_input_validation_readiness_plan_to_dict",
    "task_input_validation_readiness_plan_to_dicts",
    "task_input_validation_readiness_plan_to_markdown",
]
