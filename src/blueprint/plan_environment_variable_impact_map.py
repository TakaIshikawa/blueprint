"""Map environment variable and configuration-key impact across execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


EnvironmentConfigScope = Literal[
    "local",
    "ci",
    "preview",
    "staging",
    "production",
    "deployment",
    "runtime",
]
EnvironmentConfigSensitivity = Literal["sensitive", "non_sensitive"]
_T = TypeVar("_T")

_SCOPE_ORDER: dict[EnvironmentConfigScope, int] = {
    "production": 0,
    "staging": 1,
    "preview": 2,
    "ci": 3,
    "deployment": 4,
    "runtime": 5,
    "local": 6,
}
_SENSITIVITY_ORDER: dict[EnvironmentConfigSensitivity, int] = {
    "sensitive": 0,
    "non_sensitive": 1,
}
_SUMMARY_SCOPE_ORDER: tuple[EnvironmentConfigScope, ...] = (
    "local",
    "ci",
    "preview",
    "staging",
    "production",
    "deployment",
    "runtime",
)
_SUMMARY_SENSITIVITY_ORDER: tuple[EnvironmentConfigSensitivity, ...] = (
    "sensitive",
    "non_sensitive",
)

_UPPER_KEY_RE = re.compile(r"(?<![A-Za-z0-9_])\$?\{?([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)\}?")
_ASSIGNMENT_KEY_RE = re.compile(
    r"(?<![A-Za-z0-9_.-])([A-Za-z][A-Za-z0-9]*(?:[_.-][A-Za-z0-9]+)+)\s*="
)
_DOTTED_KEY_RE = re.compile(
    r"\b([a-z][a-z0-9]*(?:[._-][a-z0-9]+){1,5})\b"
)
_SETTING_PHRASE_RE = re.compile(
    r"\b(?:env(?:ironment)? variable|config(?:uration)? key|setting|secret name|"
    r"feature flag|feature toggle|toggle|deploy(?:ment)? setting)\s+"
    r"`?([A-Za-z][A-Za-z0-9_.-]{2,})`?",
    re.I,
)
_SENSITIVE_RE = re.compile(
    r"(?:SECRET|TOKEN|PASSWORD|PASSWD|PRIVATE[_-]?KEY|CLIENT[_-]?SECRET|"
    r"API[_-]?KEY|ACCESS[_-]?KEY|AUTH[_-]?KEY|WEBHOOK[_-]?SECRET|SIGNING[_-]?KEY|"
    r"CREDENTIAL|CERT|DSN)",
    re.I,
)
_NON_KEY_PATH_PARTS = {
    ".github",
    "app",
    "apps",
    "ci",
    "config",
    "configs",
    "deploy",
    "deployment",
    "deployments",
    "docs",
    "env",
    "environments",
    "feature_flags",
    "feature-flags",
    "flags",
    "helm",
    "infra",
    "infrastructure",
    "k8s",
    "kubernetes",
    "local",
    "prod",
    "production",
    "preview",
    "review",
    "secrets",
    "settings",
    "staging",
    "terraform",
    "uat",
}
_PATH_CONFIG_RE = re.compile(
    r"(?:^|/)(?:\.env(?:[./-]|$)|env(?:ironments)?/|configs?/|settings/|"
    r"secrets?/|feature[-_]?flags?/|deploy(?:ments)?/|helm/|k8s/|kubernetes/|terraform/)",
    re.I,
)
_PRODUCTION_RE = re.compile(r"\b(?:prod|production|live|customer traffic|go[- ]live)\b", re.I)
_STAGING_RE = re.compile(r"\b(?:staging|stage|preprod|pre-prod|uat)\b", re.I)
_PREVIEW_RE = re.compile(r"\b(?:preview|review app|review-app|vercel|netlify|ephemeral)\b", re.I)
_CI_RE = re.compile(r"\b(?:ci|github actions?|workflow|pipeline|pytest|lint|typecheck|build)\b", re.I)
_LOCAL_RE = re.compile(r"\b(?:local|developer|dev env|\.env\.local)\b", re.I)
_DEPLOYMENT_RE = re.compile(
    r"\b(?:deploy|deployment|helm|kubernetes|k8s|terraform|replica|pod|container|"
    r"rollout|release)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class EnvironmentVariableImpactRecord:
    """Impact record for one environment variable or configuration key."""

    key_name: str
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    likely_environment_scope: EnvironmentConfigScope = "runtime"
    sensitivity: EnvironmentConfigSensitivity = "non_sensitive"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_coordination_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "key_name": self.key_name,
            "task_ids": list(self.task_ids),
            "likely_environment_scope": self.likely_environment_scope,
            "sensitivity": self.sensitivity,
            "evidence": list(self.evidence),
            "recommended_coordination_notes": list(self.recommended_coordination_notes),
        }


@dataclass(frozen=True, slots=True)
class PlanEnvironmentVariableImpactMap:
    """Plan-level impact map grouped by environment/config key."""

    plan_id: str | None = None
    records: tuple[EnvironmentVariableImpactRecord, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return impact records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    @property
    def findings(self) -> tuple[EnvironmentVariableImpactRecord, ...]:
        """Compatibility view matching planners that name task records findings."""
        return self.records

    def to_markdown(self) -> str:
        """Render the impact map as deterministic Markdown."""
        title = "# Plan Environment Variable Impact Map"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Key count: {self.summary.get('key_count', 0)}",
            "- Scope counts: " + _format_counts(self.summary.get("scope_counts", {})),
            "- Sensitivity counts: "
            + _format_counts(self.summary.get("sensitivity_counts", {})),
        ]
        if not self.records:
            lines.extend(["", "No environment variable or configuration key impacts were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Key | Tasks | Scope | Sensitivity | Evidence | Coordination Notes |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.key_name)} | "
                f"{_markdown_cell(', '.join(record.task_ids) or 'none')} | "
                f"{record.likely_environment_scope} | "
                f"{record.sensitivity} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_coordination_notes) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_environment_variable_impact_map(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | PlanEnvironmentVariableImpactMap
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanEnvironmentVariableImpactMap:
    """Build a deterministic impact map for env vars and config keys in a plan."""
    if isinstance(source, PlanEnvironmentVariableImpactMap):
        return source

    plan_id, tasks = _source_payload(source)
    task_ids = [
        _optional_text(task.get("id")) or f"task-{index}"
        for index, task in enumerate(tasks, start=1)
    ]
    normalized_tasks = [
        {**task, "id": task_id} for task, task_id in zip(tasks, task_ids, strict=True)
    ]
    impacts: dict[str, dict[str, Any]] = {}

    for task in normalized_tasks:
        task_id = _optional_text(task.get("id")) or "task"
        for hit in _task_key_hits(task):
            impact = impacts.setdefault(
                hit.key_name,
                {
                    "task_ids": [],
                    "scopes": [],
                    "sensitivity": hit.sensitivity,
                    "evidence": [],
                },
            )
            impact["task_ids"].append(task_id)
            impact["scopes"].append(hit.scope)
            if hit.sensitivity == "sensitive":
                impact["sensitivity"] = "sensitive"
            impact["evidence"].append(f"{task_id}: {hit.evidence}")

    records = tuple(
        EnvironmentVariableImpactRecord(
            key_name=key_name,
            task_ids=tuple(_dedupe(values["task_ids"])),
            likely_environment_scope=_highest_scope(values["scopes"]),
            sensitivity=values["sensitivity"],
            evidence=tuple(_dedupe(values["evidence"])),
            recommended_coordination_notes=tuple(
                _coordination_notes(
                    key_name,
                    values["sensitivity"],
                    _highest_scope(values["scopes"]),
                    _dedupe(values["task_ids"]),
                )
            ),
        )
        for key_name, values in sorted(
            impacts.items(),
            key=lambda item: (
                _SENSITIVITY_ORDER[item[1]["sensitivity"]],
                _SCOPE_ORDER[_highest_scope(item[1]["scopes"])],
                item[0].casefold(),
            ),
        )
    )
    return PlanEnvironmentVariableImpactMap(
        plan_id=plan_id,
        records=records,
        summary=_summary(len(normalized_tasks), records),
    )


def derive_plan_environment_variable_impact_map(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | PlanEnvironmentVariableImpactMap
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanEnvironmentVariableImpactMap:
    """Compatibility alias for building an environment variable impact map."""
    return build_plan_environment_variable_impact_map(source)


def summarize_plan_environment_variable_impact_map(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | PlanEnvironmentVariableImpactMap
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanEnvironmentVariableImpactMap:
    """Compatibility alias for callers that use summarize_* helper names."""
    return build_plan_environment_variable_impact_map(source)


def plan_environment_variable_impact_map_to_dict(
    impact_map: PlanEnvironmentVariableImpactMap,
) -> dict[str, Any]:
    """Serialize an environment variable impact map to a plain dictionary."""
    return impact_map.to_dict()


plan_environment_variable_impact_map_to_dict.__test__ = False


def plan_environment_variable_impact_map_to_markdown(
    impact_map: PlanEnvironmentVariableImpactMap,
) -> str:
    """Render an environment variable impact map as Markdown."""
    return impact_map.to_markdown()


plan_environment_variable_impact_map_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _KeyHit:
    key_name: str
    scope: EnvironmentConfigScope
    sensitivity: EnvironmentConfigSensitivity
    evidence: str


def _task_key_hits(task: Mapping[str, Any]) -> list[_KeyHit]:
    hits: list[_KeyHit] = []
    for label, value in _task_evidence_sources(task):
        for key_name in _extract_keys(value, is_path=label == "files_or_modules"):
            scope = _scope_for_evidence(value, label)
            hits.append(
                _KeyHit(
                    key_name=key_name,
                    scope=scope,
                    sensitivity=_classify_sensitivity(key_name),
                    evidence=f"{label}: {_short_text(value)}",
                )
            )
    return _dedupe_hits(hits)


def _task_evidence_sources(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    sources: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "status",
    ):
        if text := _optional_text(task.get(field_name)):
            sources.append((field_name, text))
    for value in _strings(task.get("acceptance_criteria")):
        sources.append(("acceptance_criteria", value))
    for value in _strings(task.get("files_or_modules") or task.get("files")):
        sources.append(("files_or_modules", value))
    for value in _task_validation_commands(task):
        sources.append(("validation_commands", value))
    for value in _metadata_entries(task.get("metadata")):
        sources.append(("metadata", value))
    return sources


def _extract_keys(value: str, *, is_path: bool) -> list[str]:
    keys: list[str] = []
    keys.extend(match.group(1) for match in _SETTING_PHRASE_RE.finditer(value))
    keys.extend(match.group(1) for match in _ASSIGNMENT_KEY_RE.finditer(value))
    keys.extend(match.group(1) for match in _UPPER_KEY_RE.finditer(value))
    if is_path:
        keys.extend(_path_keys(value))
    else:
        keys.extend(_contextual_dotted_keys(value))
    return _dedupe(_normalize_key(key) for key in keys if _looks_like_key(key))


def _contextual_dotted_keys(value: str) -> list[str]:
    if not re.search(r"\b(?:config|setting|secret|flag|toggle|env|deploy|feature)\b", value, re.I):
        return []
    return [
        match.group(1)
        for match in _DOTTED_KEY_RE.finditer(value)
        if _looks_like_key(match.group(1))
    ]


def _path_keys(path: str) -> list[str]:
    normalized = path.replace("\\", "/").strip()
    if not _PATH_CONFIG_RE.search(normalized):
        return []
    pure = PurePosixPath(normalized)
    name = pure.name
    stem = name
    for suffix in pure.suffixes:
        stem = stem[: -len(suffix)]
    candidates = [part for part in pure.parts if part != name] + [stem]
    keys: list[str] = []
    for candidate in candidates:
        cleaned = candidate.strip(".")
        if cleaned.casefold() in _NON_KEY_PATH_PARTS:
            continue
        if _looks_like_key(cleaned):
            keys.append(cleaned)
    return keys


def _looks_like_key(value: str) -> bool:
    text = value.strip("`'\" ")
    if len(text) < 3:
        return False
    if "/" in text or text.startswith(("http.", "https.")):
        return False
    if text.casefold() in _NON_KEY_PATH_PARTS:
        return False
    if re.fullmatch(r"\d+", text):
        return False
    return bool(
        "_" in text
        or "." in text
        or "-" in text
        or text.upper() == text and any(char.isalpha() for char in text)
    )


def _normalize_key(value: str) -> str:
    text = value.strip("`'\" ${}.,;:")
    text = text.replace("-", "_") if text.upper() == text else text
    return text


def _classify_sensitivity(key_name: str) -> EnvironmentConfigSensitivity:
    return "sensitive" if _SENSITIVE_RE.search(key_name) else "non_sensitive"


def _scope_for_evidence(value: str, label: str) -> EnvironmentConfigScope:
    text = value.replace("\\", "/")
    if _PRODUCTION_RE.search(text):
        return "production"
    if _STAGING_RE.search(text):
        return "staging"
    if _PREVIEW_RE.search(text):
        return "preview"
    if label == "validation_commands" or _CI_RE.search(text):
        return "ci"
    if _LOCAL_RE.search(text):
        return "local"
    if label == "files_or_modules" and _DEPLOYMENT_RE.search(text):
        return "deployment"
    if _DEPLOYMENT_RE.search(text):
        return "deployment"
    return "runtime"


def _highest_scope(scopes: Iterable[EnvironmentConfigScope]) -> EnvironmentConfigScope:
    values = list(scopes)
    if not values:
        return "runtime"
    return min(values, key=lambda scope: _SCOPE_ORDER[scope])


def _coordination_notes(
    key_name: str,
    sensitivity: EnvironmentConfigSensitivity,
    scope: EnvironmentConfigScope,
    task_ids: list[str],
) -> list[str]:
    notes: list[str] = []
    if sensitivity == "sensitive":
        notes.append(f"Coordinate {key_name} through secret storage, access review, and rotation.")
    else:
        notes.append(f"Coordinate {key_name} value ownership and defaults before merge.")
    if scope in {"production", "staging", "preview"}:
        notes.append(f"Confirm {scope} value is provisioned before deployment.")
    elif scope == "ci":
        notes.append("Confirm CI variables are configured before validation runs.")
    elif scope == "deployment":
        notes.append("Align deployment manifests and runtime configuration in the same release.")
    if len(task_ids) > 1:
        notes.append("Multiple tasks reference this key; assign one owner for rollout sequencing.")
    return notes


def _summary(
    task_count: int,
    records: tuple[EnvironmentVariableImpactRecord, ...],
) -> dict[str, Any]:
    scope_counts = {scope: 0 for scope in _SUMMARY_SCOPE_ORDER}
    sensitivity_counts = {sensitivity: 0 for sensitivity in _SUMMARY_SENSITIVITY_ORDER}
    for record in records:
        scope_counts[record.likely_environment_scope] += 1
        sensitivity_counts[record.sensitivity] += 1
    return {
        "task_count": task_count,
        "key_count": len(records),
        "scope_counts": scope_counts,
        "sensitivity_counts": sensitivity_counts,
        "sensitive_key_count": sensitivity_counts["sensitive"],
        "non_sensitive_key_count": sensitivity_counts["non_sensitive"],
    }


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
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
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "status",
        "tags",
        "labels",
        "metadata",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        if text := _optional_text(task.get(key)):
            commands.append(text)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        commands.extend(_commands_from_value(metadata.get("validation_commands")))
        commands.extend(_commands_from_value(metadata.get("validation_command")))
        commands.extend(_commands_from_value(metadata.get("test_commands")))
        commands.extend(_commands_from_value(metadata.get("test_command")))
    return _dedupe(commands)


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    return _strings(value)


def _metadata_entries(value: Any, prefix: str = "") -> list[str]:
    if isinstance(value, Mapping):
        texts: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            key_text = str(key)
            child_prefix = f"{prefix}.{key_text}" if prefix else key_text
            if _looks_like_key(key_text):
                texts.append(child_prefix)
            texts.extend(_metadata_entries(value[key], child_prefix))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[str] = []
        for item in items:
            texts.extend(_metadata_entries(item, prefix))
        return texts
    if text := _optional_text(value):
        return [f"{prefix}: {text}" if prefix else text]
    return []


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dedupe(values: Iterable[_T]) -> list[_T]:
    seen: set[_T] = set()
    deduped: list[_T] = []
    for value in values:
        if value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped


def _dedupe_hits(values: Iterable[_KeyHit]) -> list[_KeyHit]:
    seen: set[tuple[str, EnvironmentConfigScope, EnvironmentConfigSensitivity, str]] = set()
    deduped: list[_KeyHit] = []
    for value in values:
        key = (value.key_name, value.scope, value.sensitivity, value.evidence)
        if key not in seen:
            deduped.append(value)
            seen.add(key)
    return deduped


def _short_text(value: str) -> str:
    text = re.sub(r"\s+", " ", value).strip()
    if len(text) <= 120:
        return text
    return f"{text[:117]}..."


def _format_counts(counts: Mapping[str, Any]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key} {counts.get(key, 0)}" for key in counts)


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")
