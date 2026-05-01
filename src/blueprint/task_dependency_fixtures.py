"""Infer fixture and seed-data needs for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


FixtureType = Literal[
    "api_contract",
    "database_seed",
    "filesystem_sample",
    "configuration",
    "mock_service",
]
_T = TypeVar("_T")

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_PATH_STRIP_RE = re.compile(r"^[`'\",;:(){}\[\]\s]+|[`'\",;:(){}\[\]\s.]+$")
_EXTERNAL_SERVICE_RE = re.compile(
    r"\b(?:auth0|aws|azure|github|google|hubspot|mailgun|oauth|paypal|salesforce|s3|"
    r"sendgrid|slack|stripe|twilio|webhook|webhooks)\b"
)
_SERVICE_WORD_RE = re.compile(r"\b(?:external|third[- ]party|mock|stub|fake|sandbox)\b")


@dataclass(frozen=True, slots=True)
class TaskDependencyFixtureRequirement:
    """One fixture or seed-data recommendation consumed by execution tasks."""

    fixture_type: FixtureType
    likely_location: str
    consuming_task_ids: tuple[str, ...] = field(default_factory=tuple)
    confidence: float = 0.0
    rationale: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "fixture_type": self.fixture_type,
            "likely_location": self.likely_location,
            "consuming_task_ids": list(self.consuming_task_ids),
            "confidence": self.confidence,
            "rationale": self.rationale,
        }


@dataclass(frozen=True, slots=True)
class TaskDependencyFixtureAdvice:
    """Complete fixture recommendation set for an execution plan."""

    plan_id: str
    requirements: tuple[TaskDependencyFixtureRequirement, ...] = field(default_factory=tuple)

    @property
    def fixture_types(self) -> tuple[FixtureType, ...]:
        """Fixture types represented by the advice in stable order."""
        return tuple(_dedupe(requirement.fixture_type for requirement in self.requirements))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "fixture_types": list(self.fixture_types),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
        }


def infer_task_dependency_fixtures(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> TaskDependencyFixtureAdvice:
    """Infer fixture and seed-data requirements without inspecting the worktree."""
    payload = _plan_payload(plan)
    plan_id = _text(payload.get("id")) or "plan"
    records = _task_records(_task_payloads(payload.get("tasks")))
    candidates: list[_FixtureCandidate] = []
    for record in records:
        candidates.extend(_fixture_candidates(record))

    return TaskDependencyFixtureAdvice(
        plan_id=plan_id,
        requirements=_merged_requirements(candidates),
    )


def task_dependency_fixtures_to_dict(
    advice: TaskDependencyFixtureAdvice,
) -> dict[str, Any]:
    """Serialize fixture advice to a plain dictionary."""
    return advice.to_dict()


task_dependency_fixtures_to_dict.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    description: str
    acceptance_criteria: tuple[str, ...]
    validation_commands: tuple[str, ...]
    files_or_modules: tuple[str, ...]
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _FixtureCandidate:
    fixture_type: FixtureType
    likely_location: str
    task_id: str
    confidence: float
    rationale: str
    sequence: int


def _fixture_candidates(record: _TaskRecord) -> list[_FixtureCandidate]:
    text = _task_text(record)
    candidates: list[_FixtureCandidate] = []
    for fixture_type in (
        "api_contract",
        "database_seed",
        "filesystem_sample",
        "configuration",
        "mock_service",
    ):
        signals = _signals_for(fixture_type, record, text)
        if not signals:
            continue
        candidates.append(
            _FixtureCandidate(
                fixture_type=fixture_type,
                likely_location=_likely_location(fixture_type, record, signals),
                task_id=record.task_id,
                confidence=_confidence(signals),
                rationale=_rationale(record.task_id, fixture_type, signals),
                sequence=len(candidates),
            )
        )
    return candidates


def _signals_for(
    fixture_type: FixtureType,
    record: _TaskRecord,
    text: str,
) -> tuple[str, ...]:
    signals: list[str] = []
    metadata = record.metadata

    if fixture_type == "api_contract":
        signals.extend(_location_signals(metadata, fixture_type))
        signals.extend(_metadata_signals(metadata, ("api_contract", "api_contracts", "openapi")))
        if _has_any(text, ("api contract", "openapi", "swagger", "request schema", "response schema")):
            signals.append("task text references API contracts or schemas")
        if _has_path(record.files_or_modules, ("openapi", "swagger", "api/", "routes", "endpoints")):
            signals.append("expected files include API surface paths")

    if fixture_type == "database_seed":
        signals.extend(_location_signals(metadata, fixture_type))
        signals.extend(
            _metadata_signals(
                metadata,
                ("database_seed", "database_seeds", "seed_data", "seed_records", "tables"),
            )
        )
        if _has_any(text, ("database", "db seed", "seed data", "seed record", "migration", "sql")):
            signals.append("task text references database seed records")
        if _has_path(record.files_or_modules, ("db/", "database", "migration", "models", ".sql")):
            signals.append("expected files include database-related paths")

    if fixture_type == "filesystem_sample":
        signals.extend(_location_signals(metadata, fixture_type))
        signals.extend(
            _metadata_signals(
                metadata,
                ("sample_file", "sample_files", "file_fixtures", "filesystem_samples"),
            )
        )
        if _has_any(
            text,
            (
                "sample file",
                "fixture file",
                "upload",
                "csv",
                "jsonl",
                "import file",
                "export file",
                "filesystem",
            ),
        ):
            signals.append("task text references file-system sample data")
        if _has_path(record.files_or_modules, ("import", "export", "uploads", "fixtures/files")):
            signals.append("expected files include import/export or upload paths")

    if fixture_type == "configuration":
        signals.extend(_location_signals(metadata, fixture_type))
        signals.extend(
            _metadata_signals(
                metadata,
                ("config_fixture", "config_fixtures", "env", "environment", "feature_flags"),
            )
        )
        if _has_any(
            text,
            ("config", "configuration", "environment variable", ".env", "feature flag", "settings"),
        ):
            signals.append("task text references configuration inputs")
        if _has_path(
            record.files_or_modules,
            (".env", "config", "settings", "config/", "configs/"),
        ):
            signals.append("expected files include configuration paths")

    if fixture_type == "mock_service":
        signals.extend(_location_signals(metadata, fixture_type))
        signals.extend(
            _metadata_signals(
                metadata,
                ("mock_service", "mock_services", "external_services", "integrations"),
            )
        )
        if _EXTERNAL_SERVICE_RE.search(text) or (
            _SERVICE_WORD_RE.search(text) and _has_any(text, ("service", "api", "integration"))
        ):
            signals.append("task text references mocked external service behavior")
        if _has_path(record.files_or_modules, ("integrations", "webhooks", "clients", "services")):
            signals.append("expected files include integration paths")

    return tuple(_dedupe(signals))


def _metadata_signals(metadata: Mapping[str, Any], keys: tuple[str, ...]) -> list[str]:
    signals: list[str] = []
    for key in keys:
        value = metadata.get(key)
        if value in (None, False, "", [], {}):
            continue
        if value is True:
            signals.append(f"metadata sets {key}")
            continue
        strings = _strings(value)
        if strings:
            signals.append(f"metadata {key}: " + ", ".join(strings[:3]))
    return signals


def _location_signals(metadata: Mapping[str, Any], fixture_type: FixtureType) -> list[str]:
    for key in (f"{fixture_type}_fixture", f"{fixture_type}_fixture_path"):
        text = _optional_text(metadata.get(key))
        if text:
            return [f"metadata {key}: {text}"]
    fixtures = metadata.get("fixtures")
    if isinstance(fixtures, Mapping):
        text = _optional_text(fixtures.get(fixture_type)) or _optional_text(
            fixtures.get(f"{fixture_type}_path")
        )
        if text:
            return [f"metadata fixtures.{fixture_type}: {text}"]
    return []


def _likely_location(
    fixture_type: FixtureType,
    record: _TaskRecord,
    signals: tuple[str, ...],
) -> str:
    explicit = _explicit_location(record.metadata, fixture_type)
    if explicit:
        return explicit

    subject = _fixture_subject(record, signals)
    if fixture_type == "api_contract":
        return f"tests/fixtures/api/{subject}.json"
    if fixture_type == "database_seed":
        return f"tests/fixtures/db/{subject}.json"
    if fixture_type == "filesystem_sample":
        return f"tests/fixtures/files/{subject}"
    if fixture_type == "configuration":
        return f"tests/fixtures/config/{subject}.env"
    return f"tests/fixtures/mocks/{subject}.json"


def _explicit_location(metadata: Mapping[str, Any], fixture_type: FixtureType) -> str | None:
    for key in (
        f"{fixture_type}_fixture",
        f"{fixture_type}_fixture_path",
        "fixture_path",
        "fixture_location",
    ):
        text = _optional_text(metadata.get(key))
        if text:
            return _normalized_path(text)
    fixtures = metadata.get("fixtures")
    if isinstance(fixtures, Mapping):
        text = _optional_text(fixtures.get(fixture_type)) or _optional_text(
            fixtures.get(f"{fixture_type}_path")
        )
        if text:
            return _normalized_path(text)
    return None


def _fixture_subject(record: _TaskRecord, signals: tuple[str, ...]) -> str:
    for signal in signals:
        if ":" not in signal:
            continue
        value = signal.split(":", 1)[1]
        words = _TOKEN_RE.findall(value.lower())
        useful = [word for word in words if word not in {"true", "fixture", "fixtures"}]
        if useful:
            return "-".join(useful[:4])[:60].strip("-")
    return _slug(record.task_id) or "task"


def _confidence(signals: tuple[str, ...]) -> float:
    confidence = 0.55
    if any(signal.startswith("metadata ") or signal.startswith("metadata sets") for signal in signals):
        confidence += 0.25
    if any(signal.startswith("task text") for signal in signals):
        confidence += 0.15
    if any(signal.startswith("expected files") for signal in signals):
        confidence += 0.10
    confidence += min(max(len(signals) - 1, 0), 2) * 0.03
    return round(min(confidence, 0.98), 2)


def _rationale(
    task_id: str,
    fixture_type: FixtureType,
    signals: tuple[str, ...],
) -> str:
    label = fixture_type.replace("_", " ")
    return f"Task {task_id} likely needs {label} fixtures because " + "; ".join(signals) + "."


def _merged_requirements(
    candidates: Iterable[_FixtureCandidate],
) -> tuple[TaskDependencyFixtureRequirement, ...]:
    grouped: dict[tuple[FixtureType, str], list[_FixtureCandidate]] = {}
    order: dict[tuple[FixtureType, str], int] = {}
    for index, candidate in enumerate(candidates):
        key = (candidate.fixture_type, candidate.likely_location)
        grouped.setdefault(key, []).append(candidate)
        order.setdefault(key, index)

    requirements: list[TaskDependencyFixtureRequirement] = []
    for key, group in grouped.items():
        group.sort(key=lambda candidate: candidate.sequence)
        task_ids = tuple(_dedupe(candidate.task_id for candidate in group))
        confidence = round(max(candidate.confidence for candidate in group), 2)
        rationale = " ".join(_dedupe(candidate.rationale for candidate in group))
        requirements.append(
            TaskDependencyFixtureRequirement(
                fixture_type=key[0],
                likely_location=key[1],
                consuming_task_ids=task_ids,
                confidence=confidence,
                rationale=rationale,
            )
        )

    requirements.sort(
        key=lambda requirement: (
            order[(requirement.fixture_type, requirement.likely_location)],
            requirement.fixture_type,
            requirement.likely_location,
        )
    )
    return tuple(requirements)


def _task_text(record: _TaskRecord) -> str:
    values = [
        record.title,
        record.description,
        *record.acceptance_criteria,
        *record.validation_commands,
        *_strings(record.metadata.get("fixture_notes")),
        *_strings(record.metadata.get("validation_commands")),
    ]
    return " ".join(values).lower()


def _task_records(tasks: list[dict[str, Any]]) -> tuple[_TaskRecord, ...]:
    records: list[_TaskRecord] = []
    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        if task_id in seen_ids:
            continue
        seen_ids.add(task_id)
        metadata = task.get("metadata")
        records.append(
            _TaskRecord(
                task=task,
                task_id=task_id,
                title=_optional_text(task.get("title")) or task_id,
                description=_optional_text(task.get("description")) or "",
                acceptance_criteria=tuple(_strings(task.get("acceptance_criteria"))),
                validation_commands=tuple(
                    _dedupe(
                        [
                            *_strings(task.get("test_command")),
                            *_strings(task.get("validation_command")),
                            *_strings(task.get("validation_commands")),
                        ]
                    )
                ),
                files_or_modules=tuple(
                    _dedupe(
                        _normalized_path(path) for path in _strings(task.get("files_or_modules"))
                    )
                ),
                metadata=metadata if isinstance(metadata, Mapping) else {},
            )
        )
    return tuple(records)


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


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _has_any(text: str, needles: tuple[str, ...]) -> bool:
    return any(needle in text for needle in needles)


def _has_path(paths: Iterable[str], needles: tuple[str, ...]) -> bool:
    return any(any(needle in path.lower() for needle in needles) for path in paths)


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
    return _PATH_STRIP_RE.sub("", value).replace("\\", "/").strip("/")


def _slug(value: str) -> str:
    return "-".join(_TOKEN_RE.findall(_text(value).lower()))[:60].strip("-")


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
    "FixtureType",
    "TaskDependencyFixtureAdvice",
    "TaskDependencyFixtureRequirement",
    "infer_task_dependency_fixtures",
    "task_dependency_fixtures_to_dict",
]
