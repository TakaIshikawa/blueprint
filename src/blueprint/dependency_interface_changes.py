"""Detect dependency edges that may carry public interface changes."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class DependencyInterfaceChangeFinding:
    """Interface-change risk inferred for one task dependency edge."""

    upstream_task_id: str
    downstream_task_id: str
    impact_type: str
    confidence: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_coordination: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "upstream_task_id": self.upstream_task_id,
            "downstream_task_id": self.downstream_task_id,
            "impact_type": self.impact_type,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "recommended_coordination": self.recommended_coordination,
        }


@dataclass(frozen=True, slots=True)
class _Signal:
    signal_type: str
    value: str
    field: str


def detect_dependency_interface_changes(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> tuple[DependencyInterfaceChangeFinding, ...]:
    """Infer public-interface coordination needs for each dependency edge."""
    payload = _plan_payload(plan)
    records = _task_records(_task_payloads(payload.get("tasks")))
    task_by_id = {record.task_id: record.task for record in records}
    signals_by_task_id = {
        record.task_id: _public_interface_signals(record.task) for record in records
    }

    findings: list[DependencyInterfaceChangeFinding] = []
    for record in records:
        for dependency_id in _strings(record.task.get("depends_on")):
            upstream = task_by_id.get(dependency_id)
            if upstream is None:
                findings.append(_missing_dependency_finding(dependency_id, record.task_id))
                continue
            findings.append(
                _edge_finding(
                    upstream_task_id=dependency_id,
                    downstream_task_id=record.task_id,
                    upstream_signals=signals_by_task_id.get(dependency_id, ()),
                    downstream_signals=signals_by_task_id.get(record.task_id, ()),
                    downstream_task=record.task,
                )
            )

    return tuple(findings)


def dependency_interface_changes_to_dict(
    findings: tuple[DependencyInterfaceChangeFinding, ...]
    | list[DependencyInterfaceChangeFinding],
) -> list[dict[str, Any]]:
    """Serialize dependency interface change findings to plain dictionaries."""
    return [finding.to_dict() for finding in findings]


dependency_interface_changes_to_dict.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str


def _missing_dependency_finding(
    upstream_task_id: str,
    downstream_task_id: str,
) -> DependencyInterfaceChangeFinding:
    return DependencyInterfaceChangeFinding(
        upstream_task_id=upstream_task_id,
        downstream_task_id=downstream_task_id,
        impact_type="missing_dependency",
        confidence="low",
        evidence=(f"depends_on references unknown task '{upstream_task_id}'",),
        recommended_coordination=(
            "Resolve the dependency id before using it to coordinate interface work."
        ),
    )


def _edge_finding(
    *,
    upstream_task_id: str,
    downstream_task_id: str,
    upstream_signals: tuple[_Signal, ...],
    downstream_signals: tuple[_Signal, ...],
    downstream_task: Mapping[str, Any],
) -> DependencyInterfaceChangeFinding:
    if not upstream_signals:
        return DependencyInterfaceChangeFinding(
            upstream_task_id=upstream_task_id,
            downstream_task_id=downstream_task_id,
            impact_type="unrelated",
            confidence="low",
            evidence=("No public interface signal found on upstream dependency.",),
            recommended_coordination="No interface-specific coordination detected.",
        )

    impact_type = _impact_type(upstream_signals)
    confidence = _confidence(upstream_signals, downstream_signals, downstream_task)
    evidence = _evidence(upstream_signals, downstream_signals, confidence)
    return DependencyInterfaceChangeFinding(
        upstream_task_id=upstream_task_id,
        downstream_task_id=downstream_task_id,
        impact_type=impact_type,
        confidence=confidence,
        evidence=tuple(evidence),
        recommended_coordination=_recommended_coordination(impact_type, confidence),
    )


def _public_interface_signals(task: Mapping[str, Any]) -> tuple[_Signal, ...]:
    signals: list[_Signal] = []
    for field_name in ("title", "description"):
        for text in _strings(task.get(field_name)):
            signals.extend(_signals_from_text(text, field_name))

    for field_name in ("files_or_modules", "acceptance_criteria"):
        for text in _strings(task.get(field_name)):
            signals.extend(_signals_from_text(text, field_name))
            signals.extend(_signals_from_path(text, field_name))

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        signals.extend(_signals_from_metadata(metadata, "metadata"))

    return tuple(
        sorted(
            _dedupe(signals),
            key=lambda signal: (signal.signal_type, signal.value, signal.field),
        )
    )


def _signals_from_metadata(value: Mapping[Any, Any], field_path: str) -> list[_Signal]:
    signals: list[_Signal] = []
    for raw_key, raw_value in sorted(value.items(), key=lambda item: str(item[0])):
        key = _optional_text(raw_key)
        if not key:
            continue
        nested_path = f"{field_path}.{key}"
        normalized_key = _normalized_key(key)
        if normalized_key in _METADATA_CONTRACT_KEYS:
            for text in _strings(raw_value) or [key]:
                signals.append(
                    _Signal(
                        _METADATA_CONTRACT_KEYS[normalized_key],
                        _signal_value(text),
                        nested_path,
                    )
                )

        signals.extend(_signals_from_text(key, nested_path))
        for text in _strings(raw_value):
            signals.extend(_signals_from_text(text, nested_path))
            signals.extend(_signals_from_path(text, nested_path))
        if isinstance(raw_value, Mapping):
            signals.extend(_signals_from_metadata(raw_value, nested_path))
    return signals


def _signals_from_text(text: str, field_name: str) -> list[_Signal]:
    lowered = text.lower()
    signals: list[_Signal] = []
    for signal_type, pattern in _TEXT_PATTERNS:
        if pattern.search(lowered):
            signals.append(_Signal(signal_type, _signal_value(text), field_name))
    return signals


def _signals_from_path(value: str, field_name: str) -> list[_Signal]:
    path = _normalized_path(value)
    if not path or not _looks_like_path(path):
        return []
    parts = [part.lower() for part in PurePosixPath(path).parts]
    name = PurePosixPath(path).name.lower()
    suffix = PurePosixPath(path).suffix.lower()
    joined = "/".join(parts)

    signals: list[_Signal] = []
    for signal_type, tokens in _PATH_PART_PATTERNS.items():
        if any(
            token in parts or token in name or f"/{token}/" in f"/{joined}/"
            for token in tokens
        ):
            signals.append(_Signal(signal_type, path, field_name))

    if re.match(r"^[0-9]{4}[_-].*\.(sql|py|rb|js|ts)$", name) or "migration" in parts:
        signals.append(_Signal("schema", path, field_name))
    if suffix in {".proto", ".graphql", ".gql", ".openapi", ".avsc"}:
        signals.append(_Signal("api", path, field_name))
    if suffix in {".sql"}:
        signals.append(_Signal("schema", path, field_name))
    if name in _CONFIG_FILENAMES or suffix in _CONFIG_SUFFIXES:
        signals.append(_Signal("config", path, field_name))
    return signals


def _impact_type(signals: tuple[_Signal, ...]) -> str:
    signal_types = {signal.signal_type for signal in signals}
    for signal_type in _IMPACT_PRIORITY:
        if signal_type in signal_types:
            return _IMPACT_BY_SIGNAL[signal_type]
    return "public_contract"


def _confidence(
    upstream_signals: tuple[_Signal, ...],
    downstream_signals: tuple[_Signal, ...],
    downstream_task: Mapping[str, Any],
) -> str:
    upstream_types = {signal.signal_type for signal in upstream_signals}
    downstream_types = {signal.signal_type for signal in downstream_signals}
    if upstream_types & downstream_types:
        return "high"

    upstream_paths = {
        signal.value for signal in upstream_signals if "/" in signal.value or "." in signal.value
    }
    downstream_text = " ".join(_task_strings(downstream_task)).lower()
    if any(_path_stem(path) and _path_stem(path) in downstream_text for path in upstream_paths):
        return "medium"
    if downstream_signals:
        return "medium"
    return "low"


def _evidence(
    upstream_signals: tuple[_Signal, ...],
    downstream_signals: tuple[_Signal, ...],
    confidence: str,
) -> list[str]:
    selected_upstream = _top_signals(upstream_signals, limit=3)
    evidence = [
        f"upstream {signal.signal_type} signal from {signal.field}: {signal.value}"
        for signal in selected_upstream
    ]

    if confidence in {"high", "medium"} and downstream_signals:
        selected_downstream = _top_signals(downstream_signals, limit=2)
        evidence.extend(
            f"downstream {signal.signal_type} signal from {signal.field}: {signal.value}"
            for signal in selected_downstream
        )
    return _dedupe(evidence)


def _top_signals(signals: tuple[_Signal, ...], *, limit: int) -> list[_Signal]:
    return sorted(
        signals,
        key=lambda signal: (
            _IMPACT_PRIORITY.index(signal.signal_type)
            if signal.signal_type in _IMPACT_PRIORITY
            else len(_IMPACT_PRIORITY),
            signal.field,
            signal.value,
        ),
    )[:limit]


def _recommended_coordination(impact_type: str, confidence: str) -> str:
    if confidence == "high":
        return (
            f"Coordinate the {impact_type} contract before downstream implementation starts."
        )
    if confidence == "medium":
        return (
            f"Confirm whether the {impact_type} affects downstream assumptions during handoff."
        )
    return "Track the dependency normally; no interface-specific coordination is evident."


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


def _task_records(tasks: list[dict[str, Any]]) -> list[_TaskRecord]:
    return [
        _TaskRecord(task=task, task_id=_task_id(task, index))
        for index, task in enumerate(tasks, start=1)
    ]


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


def _task_strings(task: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for field_name in ("title", "description", "files_or_modules", "acceptance_criteria"):
        values.extend(_strings(task.get(field_name)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        values.extend(_strings(metadata))
    return values


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for item in value.values():
            strings.extend(_strings(item))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    return []


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if isinstance(value, str):
        return " ".join(value.split())
    return ""


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _normalized_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _signal_value(value: str) -> str:
    return value[:140]


def _path_stem(path: str) -> str:
    return PurePosixPath(path).stem.lower()


def _looks_like_path(value: str) -> bool:
    if re.search(r"\s", value):
        return False
    path = PurePosixPath(value)
    return "/" in value or bool(path.suffix) or path.name in _CONFIG_FILENAMES


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


_TEXT_PATTERNS = (
    ("api", re.compile(r"\b(api|endpoint|route|rest|graphql|openapi|swagger|webhook)\b")),
    ("schema", re.compile(r"\b(schema|migration|database contract|ddl|column|table)\b")),
    (
        "config",
        re.compile(
            r"\b(config|configuration|settings|feature flag|env var|environment variable)\b"
        ),
    ),
    ("cli", re.compile(r"\b(cli|command line|subcommand|flag|argument|option)\b")),
    ("event", re.compile(r"\b(event|topic|queue|message|payload|publisher|subscriber)\b")),
    (
        "contract",
        re.compile(r"\b(contract|public interface|backward compatible|breaking change)\b"),
    ),
    ("data_model", re.compile(r"\b(data model|model|serializer|dto|entity|domain object)\b")),
    (
        "import_export",
        re.compile(r"\b(importer|exporter|import format|export format|csv|json output)\b"),
    ),
)

_PATH_PART_PATTERNS = {
    "api": {"api", "apis", "routes", "controllers", "openapi", "swagger", "graphql"},
    "schema": {"schema", "schemas", "migrations", "migration", "ddl"},
    "config": {"config", "configs", "settings"},
    "cli": {"cli", "commands", "command"},
    "event": {"events", "event", "messages", "message", "topics", "queues"},
    "contract": {"contracts", "contract", "interfaces", "interface"},
    "data_model": {"models", "model", "entities", "entity", "serializers", "dto", "dtos"},
    "import_export": {"exporters", "exporter", "importers", "importer"},
}

_METADATA_CONTRACT_KEYS = {
    "api": "api",
    "apis": "api",
    "api_contract": "api",
    "api_contracts": "api",
    "contract": "contract",
    "contracts": "contract",
    "public_contract": "contract",
    "schema": "schema",
    "schemas": "schema",
    "migration": "schema",
    "migrations": "schema",
    "config": "config",
    "configs": "config",
    "configuration": "config",
    "cli": "cli",
    "commands": "cli",
    "events": "event",
    "event_contracts": "event",
    "data_model": "data_model",
    "data_models": "data_model",
    "models": "data_model",
    "exporters": "import_export",
    "importers": "import_export",
}

_CONFIG_FILENAMES = {
    ".env",
    "config.yml",
    "config.yaml",
    "settings.yml",
    "settings.yaml",
    "settings.toml",
    "pyproject.toml",
}
_CONFIG_SUFFIXES = {".cfg", ".conf", ".env", ".ini", ".toml", ".yaml", ".yml"}
_IMPACT_PRIORITY = (
    "api",
    "schema",
    "config",
    "cli",
    "event",
    "data_model",
    "import_export",
    "contract",
)
_IMPACT_BY_SIGNAL = {
    "api": "api_contract",
    "schema": "schema_contract",
    "config": "config_contract",
    "cli": "cli_contract",
    "event": "event_contract",
    "data_model": "data_model_contract",
    "import_export": "import_export_contract",
    "contract": "public_contract",
}


__all__ = [
    "DependencyInterfaceChangeFinding",
    "dependency_interface_changes_to_dict",
    "detect_dependency_interface_changes",
]
