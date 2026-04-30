"""Identify execution-plan tasks that need backwards compatibility handling."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CompatibilityTriggerCategory = Literal[
    "api",
    "cli",
    "schema",
    "database",
    "importer",
    "exporter",
    "config",
    "persisted_metadata",
]
CompatibilitySeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_API_RE = re.compile(
    r"\b(?:api|apis|endpoint|endpoints|route|routes|rest|http|graphql|grpc|openapi|"
    r"swagger|webhook|request|response|client contract|public interface)\b",
    re.IGNORECASE,
)
_CLI_RE = re.compile(
    r"\b(?:cli|command line|terminal|argparse|click|typer|console|subcommand|"
    r"command option|flag|positional argument)\b",
    re.IGNORECASE,
)
_SCHEMA_RE = re.compile(
    r"\b(?:schema|schemas|json schema|protobuf|proto|avro|graphql schema|openapi|"
    r"swagger|field|fields|payload shape|response shape)\b",
    re.IGNORECASE,
)
_DATABASE_RE = re.compile(
    r"\b(?:database|db|sql|table|tables|column|columns|migration|migrations|alembic|"
    r"index|indexes|backfill|existing rows|data model|orm model)\b",
    re.IGNORECASE,
)
_IMPORTER_RE = re.compile(
    r"\b(?:importer|importers|import job|data import|csv import|sync inbound|ingest|"
    r"ingestion|loader|etl)\b",
    re.IGNORECASE,
)
_EXPORTER_RE = re.compile(
    r"\b(?:exporter|exporters|export job|data export|csv export|report export|"
    r"download format|serialize|serialization|feed output)\b",
    re.IGNORECASE,
)
_CONFIG_RE = re.compile(
    r"\b(?:config|configuration|setting|settings|env var|environment variable|"
    r"feature flag|feature flags|toggle|yaml|toml|ini)\b",
    re.IGNORECASE,
)
_PERSISTED_METADATA_RE = re.compile(
    r"\b(?:persisted metadata|stored metadata|metadata field|metadata fields|"
    r"saved preferences|saved state|serialized state|cache key|cookie|session data|"
    r"audit metadata)\b",
    re.IGNORECASE,
)
_COMPAT_RE = re.compile(
    r"\b(?:backward compatible|backwards compatible|compatibility|non[- ]breaking|"
    r"migration path|deprecation|deprecated|rollback|contract fixture|golden fixture|"
    r"existing clients?|existing consumers?|old format|legacy)\b",
    re.IGNORECASE,
)
_BREAKING_RE = re.compile(
    r"\b(?:remove|rename|replace|drop|delete|change|alter|required|breaking|"
    r"no longer|instead of|new format|new default)\b",
    re.IGNORECASE,
)
_RISK_ORDER: dict[CompatibilitySeverity, int] = {"high": 0, "medium": 1, "low": 2}
_CATEGORY_ORDER: dict[CompatibilityTriggerCategory, int] = {
    "api": 0,
    "cli": 1,
    "schema": 2,
    "database": 3,
    "importer": 4,
    "exporter": 5,
    "config": 6,
    "persisted_metadata": 7,
}


@dataclass(frozen=True, slots=True)
class TaskBackwardsCompatibilityRecommendation:
    """Compatibility guidance for one execution task."""

    task_id: str
    title: str
    trigger_categories: tuple[CompatibilityTriggerCategory, ...]
    severity: CompatibilitySeverity
    rationale: str
    suggested_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "trigger_categories": list(self.trigger_categories),
            "severity": self.severity,
            "rationale": self.rationale,
            "suggested_checks": list(self.suggested_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskBackwardsCompatibilityPlan:
    """Backwards compatibility recommendations for a plan or task collection."""

    plan_id: str | None = None
    recommendations: tuple[TaskBackwardsCompatibilityRecommendation, ...] = field(
        default_factory=tuple
    )
    compatible_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [
                recommendation.to_dict() for recommendation in self.recommendations
            ],
            "compatible_task_ids": list(self.compatible_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return recommendation records as plain dictionaries."""
        return [recommendation.to_dict() for recommendation in self.recommendations]

    def to_markdown(self) -> str:
        """Render compatibility recommendations as deterministic Markdown."""
        title = "# Task Backwards Compatibility Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.recommendations:
            lines.extend(["", "No backwards compatibility recommendations were derived."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Severity | Trigger Categories | Rationale | Suggested Checks |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for recommendation in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(recommendation.task_id)}` | "
                f"{recommendation.severity} | "
                f"{_markdown_cell(', '.join(recommendation.trigger_categories))} | "
                f"{_markdown_cell(recommendation.rationale)} | "
                f"{_markdown_cell('; '.join(recommendation.suggested_checks))} |"
            )
        return "\n".join(lines)


def build_task_backwards_compatibility_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskBackwardsCompatibilityPlan:
    """Recommend backwards compatibility checks for interface- or data-facing tasks."""
    plan_id, tasks = _source_payload(source)
    recommendations = [
        recommendation
        for index, task in enumerate(tasks, start=1)
        if (recommendation := _recommendation(task, index)) is not None
    ]
    recommendations.sort(
        key=lambda item: (_RISK_ORDER[item.severity], item.task_id, item.title.casefold())
    )
    result = tuple(recommendations)
    severity_counts = {
        severity: sum(1 for item in result if item.severity == severity)
        for severity in _RISK_ORDER
    }

    return TaskBackwardsCompatibilityPlan(
        plan_id=plan_id,
        recommendations=result,
        compatible_task_ids=tuple(item.task_id for item in result),
        summary={
            "task_count": len(tasks),
            "recommendation_count": len(result),
            "high_severity_count": severity_counts["high"],
            "medium_severity_count": severity_counts["medium"],
            "low_severity_count": severity_counts["low"],
            "trigger_category_count": sum(len(item.trigger_categories) for item in result),
        },
    )


def task_backwards_compatibility_plan_to_dict(
    result: TaskBackwardsCompatibilityPlan,
) -> dict[str, Any]:
    """Serialize a backwards compatibility plan to a plain dictionary."""
    return result.to_dict()


task_backwards_compatibility_plan_to_dict.__test__ = False


def task_backwards_compatibility_plan_to_markdown(
    result: TaskBackwardsCompatibilityPlan,
) -> str:
    """Render a backwards compatibility plan as Markdown."""
    return result.to_markdown()


task_backwards_compatibility_plan_to_markdown.__test__ = False


def recommend_task_backwards_compatibility(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskBackwardsCompatibilityPlan:
    """Compatibility alias for building backwards compatibility recommendations."""
    return build_task_backwards_compatibility_plan(source)


def _recommendation(
    task: Mapping[str, Any],
    index: int,
) -> TaskBackwardsCompatibilityRecommendation | None:
    signals = _signals(task)
    if not signals:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    categories = tuple(
        sorted(signals, key=lambda category: _CATEGORY_ORDER[category])
    )
    context = _task_context(task)
    severity = _severity(categories, context)
    checks = _suggested_checks(categories, task)
    rationale = _rationale(categories, severity, context)
    evidence = tuple(_dedupe(item for category in categories for item in signals[category]))

    return TaskBackwardsCompatibilityRecommendation(
        task_id=task_id,
        title=title,
        trigger_categories=categories,
        severity=severity,
        rationale=rationale,
        suggested_checks=tuple(checks),
        evidence=evidence,
    )


def _signals(task: Mapping[str, Any]) -> dict[CompatibilityTriggerCategory, tuple[str, ...]]:
    signals: dict[CompatibilityTriggerCategory, list[str]] = {}

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _add_path_signals(signals, path)

    for source_field, text in _task_texts(task):
        _add_text_signals(signals, source_field, text)

    for source_field, text in _metadata_texts(task.get("metadata")):
        _add_text_signals(signals, source_field, text)

    return {
        category: tuple(_dedupe(evidence))
        for category, evidence in signals.items()
        if evidence
    }


def _add_path_signals(
    signals: dict[CompatibilityTriggerCategory, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    suffix = path.suffix
    evidence = f"files_or_modules: {original}"

    if bool({"api", "apis", "controllers", "handlers", "routes", "webhooks"} & parts) or name in {
        "openapi.yaml",
        "openapi.yml",
        "swagger.yaml",
        "swagger.yml",
    }:
        _append(signals, "api", evidence)
    if bool({"cli", "commands", "console", "bin"} & parts) or name in {
        "cli.py",
        "commands.py",
    } or name.endswith("_cli.py"):
        _append(signals, "cli", evidence)
    if bool({"schema", "schemas", "contracts", "proto", "protobuf"} & parts) or suffix in {
        ".proto",
        ".avsc",
    } or name in {"schema.py", "schemas.py"}:
        _append(signals, "schema", evidence)
    if suffix == ".sql" or bool({"alembic", "database", "db", "migrations", "models"} & parts):
        _append(signals, "database", evidence)
    if bool({"importer", "importers", "imports", "ingest", "ingestion", "etl"} & parts) or (
        "import" in name and suffix in {".py", ".ts", ".js"}
    ):
        _append(signals, "importer", evidence)
    if bool({"exporter", "exporters", "exports", "reports"} & parts) or (
        "export" in name and suffix in {".py", ".ts", ".js"}
    ):
        _append(signals, "exporter", evidence)
    if bool({"config", "configs", "settings", "environments"} & parts) or name in {
        ".env",
        ".env.example",
        "pyproject.toml",
    } or suffix in {".toml", ".ini"}:
        _append(signals, "config", evidence)
    if "metadata" in name or bool({"metadata", "preferences", "sessions", "cache"} & parts):
        _append(signals, "persisted_metadata", evidence)


def _add_text_signals(
    signals: dict[CompatibilityTriggerCategory, list[str]],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    if _API_RE.search(text):
        _append(signals, "api", evidence)
    if _CLI_RE.search(text):
        _append(signals, "cli", evidence)
    if _SCHEMA_RE.search(text):
        _append(signals, "schema", evidence)
    if _DATABASE_RE.search(text):
        _append(signals, "database", evidence)
    if _IMPORTER_RE.search(text):
        _append(signals, "importer", evidence)
    if _EXPORTER_RE.search(text):
        _append(signals, "exporter", evidence)
    if _CONFIG_RE.search(text):
        _append(signals, "config", evidence)
    if _PERSISTED_METADATA_RE.search(text):
        _append(signals, "persisted_metadata", evidence)


def _severity(
    categories: tuple[CompatibilityTriggerCategory, ...],
    context: str,
) -> CompatibilitySeverity:
    if _BREAKING_RE.search(context) and categories:
        return "high"
    if "database" in categories or "persisted_metadata" in categories:
        return "high"
    if "api" in categories and ("importer" in categories or "exporter" in categories):
        return "high"
    if "api" in categories or "cli" in categories or "config" in categories:
        return "medium"
    return "low"


def _suggested_checks(
    categories: tuple[CompatibilityTriggerCategory, ...],
    task: Mapping[str, Any],
) -> list[str]:
    checks: list[str] = []
    for category in categories:
        checks.extend(_checks_for_category(category))
    if not _COMPAT_RE.search(" ".join(_strings(task.get("acceptance_criteria")))):
        checks.append("Add acceptance criteria covering backwards compatibility expectations.")
    return _dedupe(checks)


def _checks_for_category(category: CompatibilityTriggerCategory) -> tuple[str, ...]:
    return {
        "api": (
            "Contract fixture proves existing request and response consumers still work.",
            "Deprecation note or versioning plan documents any changed public API behavior.",
        ),
        "cli": (
            "CLI compatibility fixture covers existing arguments, flags, output, and exit codes.",
            "Deprecation note documents renamed or removed commands and options.",
        ),
        "schema": (
            "Schema contract fixture covers old and new payload shapes.",
            "Migration path describes how existing producers and consumers handle changed fields.",
        ),
        "database": (
            "Migration path preserves existing data and supports mixed-version rollout.",
            "Rollback condition defines when to stop or reverse the database change.",
        ),
        "importer": (
            "Importer fixture covers legacy input files or payloads.",
            "Migration path documents format detection or conversion for existing imports.",
        ),
        "exporter": (
            "Exporter contract fixture covers legacy output columns, fields, and ordering.",
            "Deprecation note documents any changed export format or default.",
        ),
        "config": (
            "Config fallback preserves existing defaults and environment variable names.",
            "Rollback condition documents how to restore the previous configuration behavior.",
        ),
        "persisted_metadata": (
            "Migration path upgrades existing persisted metadata without losing unknown fields.",
            "Rollback condition covers old metadata readers and mixed-version deployments.",
        ),
    }[category]


def _rationale(
    categories: tuple[CompatibilityTriggerCategory, ...],
    severity: CompatibilitySeverity,
    context: str,
) -> str:
    rendered = ", ".join(categories)
    if severity == "high":
        return (
            f"Task touches {rendered} compatibility surface(s) where existing data, "
            "consumers, or mixed-version rollout may break."
        )
    if _COMPAT_RE.search(context):
        return f"Task touches {rendered} surface(s) and already mentions compatibility handling."
    return f"Task touches {rendered} compatibility surface(s) used by existing consumers."


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
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

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
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


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    for index, text in enumerate(_strings(task.get("tags"))):
        texts.append((f"tags[{index}]", text))
    for index, text in enumerate(_strings(task.get("labels"))):
        texts.append((f"labels[{index}]", text))
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
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _task_context(task: Mapping[str, Any]) -> str:
    values = [text for _, text in _task_texts(task)]
    values.extend(_strings(task.get("files_or_modules") or task.get("files")))
    values.extend(text for _, text in _metadata_texts(task.get("metadata")))
    return " ".join(values)


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


def _append(
    signals: dict[CompatibilityTriggerCategory, list[str]],
    category: CompatibilityTriggerCategory,
    evidence: str,
) -> None:
    signals.setdefault(category, []).append(evidence)


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
    "CompatibilitySeverity",
    "CompatibilityTriggerCategory",
    "TaskBackwardsCompatibilityPlan",
    "TaskBackwardsCompatibilityRecommendation",
    "build_task_backwards_compatibility_plan",
    "recommend_task_backwards_compatibility",
    "task_backwards_compatibility_plan_to_dict",
    "task_backwards_compatibility_plan_to_markdown",
]
