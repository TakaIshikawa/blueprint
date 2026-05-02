"""Plan post-rollout feature-flag cleanup readiness for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FeatureFlagCleanupSignal = Literal[
    "stale_flag",
    "experiment_cleanup",
    "toggle_removal",
    "dead_branch_cleanup",
]
FeatureFlagCleanupRisk = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_FLAG_RE = re.compile(
    r"\b(?:feature[- ]?flags?|release[- ]?flags?|feature[- ]?toggles?|toggles?|"
    r"rollout flags?|flags?|launchdarkly|split\.io|flipper|unleash)\b",
    re.IGNORECASE,
)
_ORDINARY_FLAG_RE = re.compile(
    r"\b(?:cli|command line|argparse|click|typer|option|argument|parser|"
    r"compiler|country|status|fraud|red flag|warning flag)\b",
    re.IGNORECASE,
)
_INITIAL_ROLLOUT_RE = re.compile(
    r"\b(?:add|introduce|create|implement|gate|guard|enable|roll out|rollout|"
    r"gradual|canary|beta|default off|kill switch)\b",
    re.IGNORECASE,
)
_CLEANUP_RE = re.compile(
    r"\b(?:cleanup|clean up|remove|delete|deleting|retire|retirement|sunset|"
    r"decommission|consolidate|drop|prune|rip out)\b",
    re.IGNORECASE,
)
_STALE_RE = re.compile(
    r"\b(?:stale|expired|unused|orphaned|long[- ]?lived|temporary|legacy)\b",
    re.IGNORECASE,
)
_EXPERIMENT_RE = re.compile(
    r"\b(?:experiment|a/b|ab test|split test|variant|winner|holdout|cohort)\b",
    re.IGNORECASE,
)
_DEAD_BRANCH_RE = re.compile(
    r"\b(?:dead branch|dead code|guarded branch|old branch|disabled branch|"
    r"enabled branch|alternate path|old code path|flagged code path|conditional path)\b",
    re.IGNORECASE,
)
_ROLLOUT_CONFIRMATION_RE = re.compile(
    r"\b(?:100\s*%|fully rolled out|rollout complete|all users|all traffic|"
    r"enabled for all|general availability|ga|winner selected|experiment concluded)\b",
    re.IGNORECASE,
)
_CONFIG_DELETION_RE = re.compile(
    r"\b(?:delete|remove|drop|clean(?:\s+up)?)\b[^.\n;]*(?:config|configuration|"
    r"flag entry|launchdarkly|split\.io|flipper|unleash|env var|environment variable)",
    re.IGNORECASE,
)
_CODE_PATH_RE = re.compile(
    r"\b(?:remove|delete|drop|prune|clean(?:\s+up)?)\b[^.\n;]*(?:dead branch|"
    r"dead code|guarded branch|code path|old path|conditional|fallback branch|"
    r"enabled path|disabled path)",
    re.IGNORECASE,
)
_OBSERVABILITY_RE = re.compile(
    r"\b(?:dashboards?|alerts?|monitors?|metrics?|telemetry|observability|logs?|logging|slo)\b",
    re.IGNORECASE,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|fallback|backout|revert|restore|alternative)\b",
    re.IGNORECASE,
)
_DOCS_RE = re.compile(r"\b(?:docs?|documentation|runbook|playbook)\b", re.IGNORECASE)
_VALIDATION_RE = re.compile(
    r"\b(?:test|tests|pytest|regression|validate|validation|verify|verified|qa)\b",
    re.IGNORECASE,
)
_CONFIG_PATH_PARTS = {
    "config",
    "configs",
    "configuration",
    "settings",
    "feature_flags",
    "feature-flags",
    "flags",
    "toggles",
    "experiments",
}
_CONFIG_PATH_NAMES = {
    "feature_flags.yaml",
    "feature_flags.yml",
    "feature-flags.yaml",
    "feature-flags.yml",
    "flags.yaml",
    "flags.yml",
    "flags.json",
    "flags.toml",
}
_STOP_WORDS = {
    "a",
    "and",
    "cleanup",
    "clean",
    "consolidate",
    "delete",
    "drop",
    "feature",
    "flag",
    "for",
    "from",
    "old",
    "remove",
    "retire",
    "retirement",
    "stale",
    "sunset",
    "task",
    "the",
    "toggle",
}
_SIGNAL_ORDER: dict[FeatureFlagCleanupSignal, int] = {
    "stale_flag": 0,
    "experiment_cleanup": 1,
    "toggle_removal": 2,
    "dead_branch_cleanup": 3,
}
_MISSING_ORDER = (
    "rollout_confirmation",
    "config_deletion",
    "code_path_removal",
    "observability_cleanup",
    "rollback_alternative",
)


@dataclass(frozen=True, slots=True)
class TaskFeatureFlagCleanupReadinessRecord:
    """Cleanup readiness guidance for one post-rollout feature-flag task."""

    task_id: str
    title: str
    flag_name: str
    cleanup_signals: tuple[FeatureFlagCleanupSignal, ...]
    cleanup_surfaces: tuple[str, ...]
    removal_risk: FeatureFlagCleanupRisk
    required_verification_steps: tuple[str, ...]
    missing_acceptance_criteria: tuple[str, ...]
    owner_assumptions: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "flag_name": self.flag_name,
            "cleanup_signals": list(self.cleanup_signals),
            "cleanup_surfaces": list(self.cleanup_surfaces),
            "removal_risk": self.removal_risk,
            "required_verification_steps": list(self.required_verification_steps),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "owner_assumptions": list(self.owner_assumptions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskFeatureFlagCleanupReadinessPlan:
    """Plan-level feature-flag cleanup readiness records."""

    plan_id: str | None = None
    records: tuple[TaskFeatureFlagCleanupReadinessRecord, ...] = field(default_factory=tuple)
    flagged_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "flagged_task_ids": list(self.flagged_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return cleanup readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render cleanup readiness as deterministic Markdown."""
        title = "# Task Feature Flag Cleanup Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No feature-flag cleanup readiness records were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Flag | Signals | Surfaces | Risk | Missing Criteria |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"`{_markdown_cell(record.flag_name)}` | "
                f"{_markdown_cell(', '.join(record.cleanup_signals))} | "
                f"{_markdown_cell(', '.join(record.cleanup_surfaces))} | "
                f"{record.removal_risk} | "
                f"{_markdown_cell(', '.join(record.missing_acceptance_criteria) or 'None')} |"
            )
        return "\n".join(lines)


def build_task_feature_flag_cleanup_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskFeatureFlagCleanupReadinessPlan:
    """Build post-rollout feature-flag cleanup readiness records."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _task_record(task, index)) is not None
            ),
            key=lambda record: (record.task_id, record.title.casefold()),
        )
    )
    flagged_task_ids = tuple(record.task_id for record in records)
    all_task_ids = tuple(
        _optional_text(task.get("id")) or f"task-{index}"
        for index, task in enumerate(tasks, start=1)
    )
    return TaskFeatureFlagCleanupReadinessPlan(
        plan_id=plan_id,
        records=records,
        flagged_task_ids=flagged_task_ids,
        no_impact_task_ids=tuple(
            task_id for task_id in all_task_ids if task_id not in flagged_task_ids
        ),
        summary=_summary(records),
    )


def task_feature_flag_cleanup_readiness_plan_to_dict(
    result: TaskFeatureFlagCleanupReadinessPlan,
) -> dict[str, Any]:
    """Serialize a feature-flag cleanup readiness plan to a plain dictionary."""
    return result.to_dict()


task_feature_flag_cleanup_readiness_plan_to_dict.__test__ = False


def task_feature_flag_cleanup_readiness_plan_to_markdown(
    result: TaskFeatureFlagCleanupReadinessPlan,
) -> str:
    """Render a feature-flag cleanup readiness plan as Markdown."""
    return result.to_markdown()


task_feature_flag_cleanup_readiness_plan_to_markdown.__test__ = False


def summarize_task_feature_flag_cleanup_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskFeatureFlagCleanupReadinessPlan:
    """Compatibility alias for building feature-flag cleanup readiness plans."""
    return build_task_feature_flag_cleanup_readiness_plan(source)


def extract_task_feature_flag_cleanup_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskFeatureFlagCleanupReadinessPlan:
    """Compatibility alias for building feature-flag cleanup readiness plans."""
    return build_task_feature_flag_cleanup_readiness_plan(source)


def _task_record(
    task: Mapping[str, Any],
    index: int,
) -> TaskFeatureFlagCleanupReadinessRecord | None:
    signals = _cleanup_signals(task)
    if not signals:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    context = _task_context(task)
    evidence = tuple(
        _dedupe(item for signal in signals.values() for item in signal)
    )
    surfaces = _cleanup_surfaces(task, context)
    missing = _missing_acceptance_criteria(task, context)
    return TaskFeatureFlagCleanupReadinessRecord(
        task_id=task_id,
        title=title,
        flag_name=_flag_name(task, index),
        cleanup_signals=tuple(sorted(signals, key=lambda signal: _SIGNAL_ORDER[signal])),
        cleanup_surfaces=surfaces,
        removal_risk=_removal_risk(signals=tuple(signals), missing=missing),
        required_verification_steps=_required_verification_steps(surfaces),
        missing_acceptance_criteria=missing,
        owner_assumptions=_owner_assumptions(task, signals=tuple(signals)),
        evidence=evidence,
    )


def _cleanup_signals(
    task: Mapping[str, Any],
) -> dict[FeatureFlagCleanupSignal, tuple[str, ...]]:
    detected: dict[FeatureFlagCleanupSignal, list[str]] = {}
    context = _task_context(task)
    has_cleanup = bool(_CLEANUP_RE.search(context))
    has_flag = _has_flag_context(task, context)

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _is_flag_cleanup_path(path):
            detected.setdefault("toggle_removal", []).append(f"files_or_modules: {path}")
        if _EXPERIMENT_RE.search(path):
            detected.setdefault("experiment_cleanup", []).append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        if _is_initial_rollout_only(text, context):
            continue
        if has_cleanup and has_flag and _STALE_RE.search(text):
            detected.setdefault("stale_flag", []).append(_evidence_snippet(source_field, text))
        if has_cleanup and _EXPERIMENT_RE.search(text):
            detected.setdefault("experiment_cleanup", []).append(
                _evidence_snippet(source_field, text)
            )
        if has_cleanup and has_flag and _FLAG_RE.search(text) and not _ORDINARY_FLAG_RE.search(text):
            detected.setdefault("toggle_removal", []).append(
                _evidence_snippet(source_field, text)
            )
        if has_cleanup and _DEAD_BRANCH_RE.search(text):
            detected.setdefault("dead_branch_cleanup", []).append(
                _evidence_snippet(source_field, text)
            )

    if not has_cleanup:
        return {}
    return {
        signal: tuple(_dedupe(evidence))
        for signal, evidence in detected.items()
        if evidence
    }


def _has_flag_context(task: Mapping[str, Any], context: str) -> bool:
    if _FLAG_RE.search(context) and not _ORDINARY_FLAG_RE.search(context):
        return True
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in metadata:
            normalized = str(key).replace("_", " ").casefold()
            if "flag" in normalized or "toggle" in normalized:
                return True
    return any(_is_flag_cleanup_path(path) for path in _strings(task.get("files_or_modules") or task.get("files")))


def _is_initial_rollout_only(text: str, context: str) -> bool:
    if _CLEANUP_RE.search(text):
        return False
    return bool(_INITIAL_ROLLOUT_RE.search(text) and not _CLEANUP_RE.search(context))


def _cleanup_surfaces(task: Mapping[str, Any], context: str) -> tuple[str, ...]:
    surfaces: list[str] = []
    paths = _strings(task.get("files_or_modules") or task.get("files"))
    if _CONFIG_DELETION_RE.search(context) or any(_is_config_path(path) for path in paths):
        surfaces.append("flag_configuration")
    if _CODE_PATH_RE.search(context) or _DEAD_BRANCH_RE.search(context) or any(
        _is_code_path(path) for path in paths
    ):
        surfaces.append("guarded_code_paths")
    if _EXPERIMENT_RE.search(context) or any("experiment" in path.casefold() for path in paths):
        surfaces.append("experiment_artifacts")
    if _OBSERVABILITY_RE.search(context):
        surfaces.append("observability_assets")
    if _DOCS_RE.search(context):
        surfaces.append("documentation")
    if _VALIDATION_RE.search(context):
        surfaces.append("regression_validation")
    if not surfaces:
        surfaces.extend(["flag_configuration", "guarded_code_paths"])
    return tuple(_dedupe(surfaces))


def _missing_acceptance_criteria(
    task: Mapping[str, Any],
    context: str,
) -> tuple[str, ...]:
    acceptance_context = " ".join(_strings(task.get("acceptance_criteria")))
    if not acceptance_context:
        acceptance_context = context
    checks = {
        "rollout_confirmation": _ROLLOUT_CONFIRMATION_RE.search(acceptance_context),
        "config_deletion": _CONFIG_DELETION_RE.search(acceptance_context),
        "code_path_removal": _CODE_PATH_RE.search(acceptance_context),
        "observability_cleanup": (
            _OBSERVABILITY_RE.search(acceptance_context)
            and _CLEANUP_RE.search(acceptance_context)
        ),
        "rollback_alternative": _ROLLBACK_RE.search(acceptance_context),
    }
    return tuple(key for key in _MISSING_ORDER if not checks[key])


def _removal_risk(
    *,
    signals: tuple[FeatureFlagCleanupSignal, ...],
    missing: tuple[str, ...],
) -> FeatureFlagCleanupRisk:
    if len(missing) <= 1:
        return "low"
    if len(missing) <= 3 and "rollout_confirmation" not in missing:
        return "medium"
    if "dead_branch_cleanup" in signals and len(missing) <= 2:
        return "medium"
    return "high"


def _required_verification_steps(surfaces: tuple[str, ...]) -> tuple[str, ...]:
    steps = [
        "Confirm the flag is at 100% rollout or the experiment winner is final.",
        "Remove stale flag configuration from every environment.",
        "Remove guarded dead branches and validate the surviving code path.",
        "Validate rollback alternatives after the toggle is removed.",
    ]
    if "observability_assets" in surfaces:
        steps.append("Delete or update dashboards, alerts, and metrics tied only to the flag.")
    else:
        steps.append("Check dashboards, alerts, and metrics for stale flag references.")
    if "documentation" in surfaces:
        steps.append("Update docs and runbooks that mention the retired flag.")
    if "experiment_artifacts" in surfaces:
        steps.append("Archive experiment results and remove obsolete variants or cohorts.")
    steps.append("Run regression tests covering the post-cleanup behavior.")
    return tuple(_dedupe(steps))


def _owner_assumptions(
    task: Mapping[str, Any],
    *,
    signals: tuple[FeatureFlagCleanupSignal, ...],
) -> tuple[str, ...]:
    metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}
    explicit_owner = _first_metadata_value(
        metadata,
        "cleanup_owner",
        "cleanup_owner_role",
        "flag_owner",
        "owner",
        "owner_team",
    )
    if explicit_owner:
        return (f"{explicit_owner} owns cleanup sign-off.",)
    if "experiment_cleanup" in signals:
        return ("Product analytics owner confirms experiment retirement.",)
    return ("Release owner confirms post-rollout flag removal.",)


def _flag_name(task: Mapping[str, Any], index: int) -> str:
    metadata = task.get("metadata")
    for value in (
        task.get("flag_name"),
        task.get("feature_flag"),
        task.get("toggle"),
        metadata.get("flag_name") if isinstance(metadata, Mapping) else None,
        metadata.get("feature_flag") if isinstance(metadata, Mapping) else None,
        metadata.get("feature_flags") if isinstance(metadata, Mapping) else None,
        metadata.get("toggle") if isinstance(metadata, Mapping) else None,
        metadata.get("experiment") if isinstance(metadata, Mapping) else None,
    ):
        strings = _strings(value)
        if strings:
            return _slug(strings[0], drop_generic=False)

    title = _optional_text(task.get("title")) or _optional_text(task.get("id")) or f"task-{index}"
    return _slug(title)


def _summary(records: tuple[TaskFeatureFlagCleanupReadinessRecord, ...]) -> dict[str, Any]:
    return {
        "flagged_task_count": len(records),
        "risk_counts": {
            risk: sum(1 for record in records if record.removal_risk == risk)
            for risk in ("low", "medium", "high")
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.cleanup_signals)
            for signal in _SIGNAL_ORDER
        },
    }


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
        task = _task_payload(item)
        if task:
            tasks.append(task)
    return None, tasks


def _plan_payload(source: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(source).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(source) if isinstance(source, Mapping) else {}


def _task_payload(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    return dict(value) if isinstance(value, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    return [task for item in items if (task := _task_payload(item))]


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for index, text in enumerate(_strings(task.get("files_or_modules") or task.get("files"))):
        texts.append((f"files_or_modules[{index}]", text))
    texts.extend(_metadata_texts(task.get("metadata")))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _FLAG_RE.search(key_text) or _CLEANUP_RE.search(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _FLAG_RE.search(key_text) or _CLEANUP_RE.search(key_text):
                texts.append((field, key_text))
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
    return " ".join(text for _, text in _candidate_texts(task))


def _is_flag_cleanup_path(value: str) -> bool:
    normalized = _normalized_path(value).casefold()
    path = PurePosixPath(normalized)
    return _is_config_path(value) or "flag" in path.name or "toggle" in path.name


def _is_config_path(value: str) -> bool:
    path = PurePosixPath(_normalized_path(value).casefold())
    parts = set(path.parts)
    return (
        path.name in _CONFIG_PATH_NAMES
        or bool(parts & _CONFIG_PATH_PARTS)
        and (path.suffix in {".env", ".yaml", ".yml", ".json", ".toml", ".py", ".ts", ".tsx"})
    )


def _is_code_path(value: str) -> bool:
    path = PurePosixPath(_normalized_path(value).casefold())
    return path.suffix in {".py", ".js", ".jsx", ".ts", ".tsx", ".rb", ".go", ".java"}


def _first_metadata_value(metadata: Mapping[str, Any], *keys: str) -> str | None:
    wanted = {key.casefold() for key in keys}
    for key in sorted(metadata, key=lambda item: str(item)):
        value = metadata[key]
        if str(key).casefold() in wanted:
            return next(iter(_strings(value)), None)
        if isinstance(value, Mapping):
            nested = _first_metadata_value(value, *keys)
            if nested:
                return nested
    return None


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


def _slug(value: str, *, drop_generic: bool = True) -> str:
    words = _TOKEN_RE.findall(value.casefold())
    if drop_generic:
        words = [word for word in words if word not in _STOP_WORDS]
    return "_".join(words[:5]) or "feature_flag"


def _evidence_snippet(source_field: str, text: str) -> str:
    return f"{source_field}: {_text(text)}"


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/").strip().strip("`'\",;:(){}[] ").strip("/")


def _markdown_cell(value: str) -> str:
    return _SPACE_RE.sub(" ", value.replace("|", "\\|").replace("\n", "<br>")).strip()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


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
    "FeatureFlagCleanupRisk",
    "FeatureFlagCleanupSignal",
    "TaskFeatureFlagCleanupReadinessPlan",
    "TaskFeatureFlagCleanupReadinessRecord",
    "build_task_feature_flag_cleanup_readiness_plan",
    "extract_task_feature_flag_cleanup_readiness",
    "summarize_task_feature_flag_cleanup_readiness",
    "task_feature_flag_cleanup_readiness_plan_to_dict",
    "task_feature_flag_cleanup_readiness_plan_to_markdown",
]
