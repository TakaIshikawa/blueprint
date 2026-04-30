"""Recommend integration test scenarios from briefs and execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


_T = TypeVar("_T")

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SPLIT_RE = re.compile(r"(?:\r?\n|;)+")
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "by",
    "for",
    "from",
    "in",
    "into",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}
_GENERIC_SCENARIO_WORDS = {
    "boundary",
    "check",
    "flow",
    "integration",
    "point",
    "scenario",
    "test",
    "tests",
    "validate",
    "validation",
}
_AUTOMATED_VALIDATION_TERMS = {
    "assert",
    "automated",
    "contract",
    "e2e",
    "integration",
    "pytest",
    "regression",
    "smoke",
    "test",
    "tests",
}
_BOUNDARY_LABELS = {
    "importer": {"import", "importer", "ingest", "ingestion", "reader", "source"},
    "exporter": {"export", "exporter", "writer", "sink", "publish", "publisher"},
    "api": {"api", "endpoint", "openapi", "rest", "graphql"},
    "client": {"client", "consumer", "sdk"},
    "schema": {"schema", "database", "db", "migration", "model"},
    "event": {"event", "queue", "topic", "message", "subscriber"},
    "config": {"config", "configuration", "flag", "settings", "env"},
    "auth": {"auth", "oauth", "permission", "security", "token"},
    "storage": {"file", "files", "storage", "bucket", "blob"},
}
_BOUNDARY_PRIORITY = (
    "importer",
    "exporter",
    "api",
    "client",
    "schema",
    "event",
    "config",
    "auth",
    "storage",
)
_BOUNDARY_PAIRS = (
    ("importer", "exporter"),
    ("api", "client"),
    ("schema", "client"),
    ("schema", "api"),
    ("event", "client"),
    ("config", "api"),
    ("auth", "api"),
    ("storage", "exporter"),
)


@dataclass(frozen=True, slots=True)
class IntegrationTestScenario:
    """One recommended cross-component validation scenario."""

    name: str
    integration_point: str
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    setup_notes: tuple[str, ...] = field(default_factory=tuple)
    assertion: str = ""
    validation_type: str = "manual"
    manual_validation_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "name": self.name,
            "integration_point": self.integration_point,
            "impacted_task_ids": list(self.impacted_task_ids),
            "setup_notes": list(self.setup_notes),
            "assertion": self.assertion,
            "validation_type": self.validation_type,
            "manual_validation_notes": list(self.manual_validation_notes),
        }


@dataclass(frozen=True, slots=True)
class IntegrationTestScenarioPlan:
    """Complete integration scenario recommendation result."""

    brief_id: str
    plan_id: str
    scenarios: tuple[IntegrationTestScenario, ...] = field(default_factory=tuple)
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "plan_id": self.plan_id,
            "scenarios": [scenario.to_dict() for scenario in self.scenarios],
            "summary": self.summary,
        }


def build_integration_test_scenarios(
    implementation_brief: Mapping[str, Any] | ImplementationBrief,
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> IntegrationTestScenarioPlan:
    """Build deterministic integration test scenario recommendations."""
    brief = _brief_payload(implementation_brief)
    plan = _plan_payload(execution_plan)
    records = [
        _task_record(task, index)
        for index, task in enumerate(_task_payloads(plan.get("tasks")), start=1)
    ]

    scenarios: list[IntegrationTestScenario] = []
    for integration_point in _strings(brief.get("integration_points")):
        scenarios.append(_brief_scenario(integration_point, brief, plan, records))

    scenarios.extend(_boundary_scenarios(brief, plan, records))
    scenarios = _collapse_similar_scenarios(scenarios, records)

    return IntegrationTestScenarioPlan(
        brief_id=_text(brief.get("id")),
        plan_id=_text(plan.get("id")),
        scenarios=tuple(scenarios),
        summary=_summary(scenarios),
    )


def integration_test_scenarios_to_dict(
    result: IntegrationTestScenarioPlan,
) -> dict[str, Any]:
    """Serialize an integration scenario plan to a plain dictionary."""
    return result.to_dict()


integration_test_scenarios_to_dict.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    fields: tuple[tuple[str, str], ...]
    tokens: frozenset[str]
    boundary_labels: tuple[str, ...]


def _brief_scenario(
    integration_point: str,
    brief: Mapping[str, Any],
    plan: Mapping[str, Any],
    records: list[_TaskRecord],
) -> IntegrationTestScenario:
    matching_records = [
        record for record in records if _matches_integration_point(integration_point, record)
    ]
    impacted_ids = _task_ids(matching_records)
    return IntegrationTestScenario(
        name=f"Validate {integration_point}",
        integration_point=integration_point,
        impacted_task_ids=tuple(impacted_ids),
        setup_notes=tuple(_setup_notes(brief, matching_records)),
        assertion=_assertion(integration_point, brief, plan, matching_records),
        validation_type=_validation_type(brief, plan, matching_records),
        manual_validation_notes=tuple(_manual_validation_notes(integration_point, brief, plan)),
    )


def _boundary_scenarios(
    brief: Mapping[str, Any],
    plan: Mapping[str, Any],
    records: list[_TaskRecord],
) -> list[IntegrationTestScenario]:
    scenarios: list[IntegrationTestScenario] = []
    for left_label, right_label in _BOUNDARY_PAIRS:
        matching_records = [
            record
            for record in records
            if left_label in record.boundary_labels or right_label in record.boundary_labels
        ]
        if len(matching_records) < 2:
            continue
        present_labels = {
            label
            for record in matching_records
            for label in record.boundary_labels
            if label in {left_label, right_label}
        }
        if {left_label, right_label} - present_labels:
            continue

        integration_point = f"{left_label.title()}/{right_label} boundary"
        scenarios.append(
            IntegrationTestScenario(
                name=f"Validate {integration_point}",
                integration_point=integration_point,
                impacted_task_ids=tuple(_task_ids(matching_records)),
                setup_notes=tuple(_setup_notes(brief, matching_records)),
                assertion=_assertion(integration_point, brief, plan, matching_records),
                validation_type=_validation_type(brief, plan, matching_records),
                manual_validation_notes=tuple(
                    _manual_validation_notes(integration_point, brief, plan)
                ),
            )
        )
    return scenarios


def _collapse_similar_scenarios(
    scenarios: list[IntegrationTestScenario],
    records: list[_TaskRecord],
) -> list[IntegrationTestScenario]:
    collapsed: list[IntegrationTestScenario] = []
    order_by_task_id = {record.task_id: index for index, record in enumerate(records)}

    for scenario in scenarios:
        match_index = _matching_scenario_index(scenario, collapsed)
        if match_index is None:
            collapsed.append(scenario)
            continue
        existing = collapsed[match_index]
        collapsed[match_index] = IntegrationTestScenario(
            name=existing.name,
            integration_point=existing.integration_point,
            impacted_task_ids=tuple(
                sorted(
                    _dedupe((*existing.impacted_task_ids, *scenario.impacted_task_ids)),
                    key=lambda task_id: order_by_task_id.get(task_id, len(order_by_task_id)),
                )
            ),
            setup_notes=tuple(_dedupe((*existing.setup_notes, *scenario.setup_notes))),
            assertion=existing.assertion or scenario.assertion,
            validation_type=_stronger_validation_type(
                existing.validation_type,
                scenario.validation_type,
            ),
            manual_validation_notes=tuple(
                _dedupe((*existing.manual_validation_notes, *scenario.manual_validation_notes))
            ),
        )

    return collapsed


def _matching_scenario_index(
    scenario: IntegrationTestScenario,
    existing_scenarios: list[IntegrationTestScenario],
) -> int | None:
    scenario_tokens = _scenario_tokens(scenario.integration_point)
    for index, existing in enumerate(existing_scenarios):
        existing_tokens = _scenario_tokens(existing.integration_point)
        if _near_identical_tokens(scenario_tokens, existing_tokens):
            return index
    return None


def _near_identical_tokens(left: set[str], right: set[str]) -> bool:
    if not left or not right:
        return False
    if left == right:
        return True
    smaller, larger = (left, right) if len(left) <= len(right) else (right, left)
    return len(smaller) >= 2 and smaller <= larger


def _task_record(task: Mapping[str, Any], index: int) -> _TaskRecord:
    fields = tuple(_task_text_fields(task))
    tokens = frozenset(_token_key(token) for _, text in fields for token in _tokens(text))
    boundary_labels = tuple(
        label
        for label in _BOUNDARY_PRIORITY
        if tokens & _BOUNDARY_LABELS[label]
        or any(
            _path_has_boundary(text, label) for field, text in fields if field == "files_or_modules"
        )
    )
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    return _TaskRecord(
        task=dict(task),
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        fields=fields,
        tokens=tokens,
        boundary_labels=boundary_labels,
    )


def _task_text_fields(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    for key in ("title", "description"):
        if text := _optional_text(task.get(key)):
            fields.append((key, text))
    for path in _strings(task.get("files_or_modules")):
        fields.append(("files_or_modules", path))
    for criterion in _strings(task.get("acceptance_criteria")):
        fields.append(("acceptance_criteria", criterion))
    for key, value in _metadata_texts(task.get("metadata")):
        fields.append((f"metadata.{key}", value))
    if test_command := _optional_text(task.get("test_command")):
        fields.append(("test_command", test_command))
    return fields


def _matches_integration_point(integration_point: str, record: _TaskRecord) -> bool:
    point_tokens = _significant_tokens(integration_point)
    if not point_tokens:
        return False
    if point_tokens <= record.tokens:
        return True
    overlap = point_tokens & record.tokens
    return len(point_tokens) >= 3 and len(overlap) >= len(point_tokens) - 1


def _setup_notes(
    brief: Mapping[str, Any],
    records: list[_TaskRecord],
) -> list[str]:
    notes: list[str] = []
    data_requirements = _optional_text(brief.get("data_requirements"))
    if data_requirements:
        notes.append(f"Prepare data requirements: {data_requirements}")
    files = _dedupe(
        path for record in records for path in _strings(record.task.get("files_or_modules"))
    )
    if files:
        notes.append("Exercise files/modules: " + ", ".join(files[:5]))
    if records:
        notes.append("Coordinate task outputs: " + ", ".join(record.task_id for record in records))
    return notes


def _assertion(
    integration_point: str,
    brief: Mapping[str, Any],
    plan: Mapping[str, Any],
    records: list[_TaskRecord],
) -> str:
    for record in records:
        for criterion in _strings(record.task.get("acceptance_criteria")):
            return f"{integration_point}: {criterion}"
    validation_plan = _optional_text(brief.get("validation_plan")) or _optional_text(
        plan.get("test_strategy")
    )
    if validation_plan:
        return f"{integration_point}: {validation_plan}"
    return f"{integration_point} behaves correctly across the impacted components."


def _validation_type(
    brief: Mapping[str, Any],
    plan: Mapping[str, Any],
    records: list[_TaskRecord],
) -> str:
    validation_text = " ".join(
        _strings(brief.get("validation_plan"))
        + _strings(plan.get("test_strategy"))
        + [
            text
            for record in records
            for value in (
                record.task.get("acceptance_criteria"),
                record.task.get("test_command"),
            )
            for text in _strings(value)
        ]
    )
    tokens = set(_tokens(validation_text))
    if tokens & _AUTOMATED_VALIDATION_TERMS or any(
        "test_" in text for text in _strings(plan.get("test_strategy"))
    ):
        return "automated"
    return "manual"


def _stronger_validation_type(left: str, right: str) -> str:
    if "automated" in {left, right}:
        return "automated"
    return left or right or "manual"


def _manual_validation_notes(
    integration_point: str,
    brief: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> list[str]:
    notes = []
    validation_plan = _optional_text(brief.get("validation_plan")) or _optional_text(
        plan.get("test_strategy")
    )
    if validation_plan and _significant_tokens(validation_plan) - {"manual", "validation"}:
        notes.append(f"Fallback validation: {validation_plan}")
    else:
        notes.append(f"Fallback validation: manually verify {integration_point}.")
    return notes


def _summary(scenarios: list[IntegrationTestScenario]) -> str:
    if not scenarios:
        return (
            "No integration test scenarios recommended because no brief-level "
            "integration points or task-level integration signals were found."
        )
    scenario_word = "scenario" if len(scenarios) == 1 else "scenarios"
    impacted_task_ids = _dedupe(
        task_id for scenario in scenarios for task_id in scenario.impacted_task_ids
    )
    return (
        f"Recommended {len(scenarios)} integration test {scenario_word} "
        f"covering {len(impacted_task_ids)} impacted task(s)."
    )


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        value = brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ImplementationBrief.model_validate(brief).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(brief, Mapping):
            return dict(brief)
    return {}


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _metadata_texts(value: Any, prefix: str = "") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _optional_text(key)
            if not key_text:
                continue
            path = f"{prefix}.{key_text}" if prefix else key_text
            texts.extend(_metadata_texts(value[key], path))
        return texts
    if isinstance(value, (list, tuple, set)):
        texts: list[tuple[str, str]] = []
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items, start=1):
            path = f"{prefix}.{index}" if prefix else str(index)
            texts.extend(_metadata_texts(item, path))
        return texts
    text = _optional_text(value)
    return [(prefix or "value", text)] if text else []


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for part in _SPLIT_RE.split(value) if (item := _optional_text(part))]
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return [text for item in items for text in _strings(item)]
    text = _optional_text(value)
    return [text] if text else []


def _task_ids(records: list[_TaskRecord]) -> list[str]:
    return _dedupe(record.task_id for record in records)


def _path_has_boundary(value: str, label: str) -> bool:
    path = _normalized_path(value)
    if not path:
        return False
    parts = {_token_key(token) for part in PurePosixPath(path).parts for token in _tokens(part)}
    return bool(parts & _BOUNDARY_LABELS[label])


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _scenario_tokens(value: str) -> set[str]:
    return _significant_tokens(value) - _GENERIC_SCENARIO_WORDS


def _significant_tokens(value: str) -> set[str]:
    return {_token_key(token) for token in _tokens(value) if _token_key(token) not in _STOP_WORDS}


def _tokens(value: Any) -> list[str]:
    return _TOKEN_RE.findall(str(value).lower())


def _token_key(token: str) -> str:
    if len(token) > 4 and token.endswith("ies"):
        return f"{token[:-3]}y"
    if len(token) > 4 and token.endswith("es"):
        return token[:-2]
    if len(token) > 3 and token.endswith("s"):
        return token[:-1]
    return token


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


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
    "IntegrationTestScenario",
    "IntegrationTestScenarioPlan",
    "build_integration_test_scenarios",
    "integration_test_scenarios_to_dict",
]
