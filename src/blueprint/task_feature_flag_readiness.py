"""Plan feature flag readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


FeatureFlagReadinessLevel = Literal["ready", "needs_safeguards", "needs_owner"]
FeatureFlagSignal = Literal[
    "feature_flag",
    "toggle",
    "kill_switch",
    "cohort",
    "percentage_rollout",
    "experiment",
    "beta_access",
    "staged_enablement",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_PATTERNS: dict[FeatureFlagSignal, re.Pattern[str]] = {
    "feature_flag": re.compile(
        r"\b(?:feature[- ]?flags?|flagged|flag gate|flag key|flag config|"
        r"launchdarkly|split\.io|flipper|unleash)\b",
        re.IGNORECASE,
    ),
    "toggle": re.compile(
        r"\b(?:feature[- ]?toggles?|toggles?|toggleable|enable flag|disable flag)\b",
        re.IGNORECASE,
    ),
    "kill_switch": re.compile(
        r"\b(?:kill switch|killswitch|emergency off|circuit breaker|disable quickly|"
        r"instant(?:ly)? disable|backout switch)\b",
        re.IGNORECASE,
    ),
    "cohort": re.compile(
        r"\b(?:cohorts?|segments?|audiences?|target users?|control group|treatment)\b",
        re.IGNORECASE,
    ),
    "percentage_rollout": re.compile(
        r"\b(?:\d{1,3}\s*%\s*(?:of\s+)?(?:users|traffic|accounts|requests)?|"
        r"percentage rollout|percent rollout|ramp(?:ing)?|gradual(?:ly)? rollout|"
        r"canary|progressive delivery)\b",
        re.IGNORECASE,
    ),
    "experiment": re.compile(
        r"\b(?:experiments?|a/b test|ab test|split test|variant|holdout)\b",
        re.IGNORECASE,
    ),
    "beta_access": re.compile(
        r"\b(?:beta access|private beta|public beta|beta users?|early access|"
        r"preview users?|allowlist|whitelist)\b",
        re.IGNORECASE,
    ),
    "staged_enablement": re.compile(
        r"\b(?:staged enablement|staged rollout|phased rollout|phased launch|"
        r"gradual enablement|internal first|staff first|dogfood)\b",
        re.IGNORECASE,
    ),
}
_SIGNAL_ORDER: dict[FeatureFlagSignal, int] = {
    "feature_flag": 0,
    "toggle": 1,
    "kill_switch": 2,
    "cohort": 3,
    "percentage_rollout": 4,
    "experiment": 5,
    "beta_access": 6,
    "staged_enablement": 7,
}


@dataclass(frozen=True, slots=True)
class TaskFeatureFlagReadinessRecord:
    """Feature flag readiness guidance for one execution task."""

    task_id: str
    title: str
    readiness_level: FeatureFlagReadinessLevel
    detected_flag_signals: tuple[FeatureFlagSignal, ...]
    required_steps: tuple[str, ...]
    rollout_safeguards: dict[str, str] = field(default_factory=dict)
    suggested_validation_commands: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "readiness_level": self.readiness_level,
            "detected_flag_signals": list(self.detected_flag_signals),
            "required_steps": list(self.required_steps),
            "rollout_safeguards": dict(self.rollout_safeguards),
            "suggested_validation_commands": list(self.suggested_validation_commands),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskFeatureFlagReadinessPlan:
    """Plan-level feature flag readiness checklist."""

    plan_id: str | None = None
    records: tuple[TaskFeatureFlagReadinessRecord, ...] = field(default_factory=tuple)
    flagged_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "flagged_task_ids": list(self.flagged_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the readiness checklist as deterministic Markdown."""
        title = "# Task Feature Flag Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No feature-flag readiness checklist entries were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Readiness | Signals | Required Steps | Safeguards | Validation |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            safeguards = "; ".join(
                f"{key}: {value}" for key, value in record.rollout_safeguards.items()
            )
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell(', '.join(record.detected_flag_signals))} | "
                f"{_markdown_cell('; '.join(record.required_steps))} | "
                f"{_markdown_cell(safeguards)} | "
                f"{_markdown_cell('; '.join(record.suggested_validation_commands))} |"
            )
        return "\n".join(lines)


def build_task_feature_flag_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskFeatureFlagReadinessPlan:
    """Build feature flag readiness safeguards for flag-related execution tasks."""
    plan_id, tasks, plan_commands = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _task_record(task, index, plan_commands=plan_commands)) is not None
            ),
            key=lambda record: (record.task_id, record.title.casefold()),
        )
    )
    flagged_task_ids = tuple(record.task_id for record in records)
    readiness_counts = {
        level: sum(1 for record in records if record.readiness_level == level)
        for level in ("ready", "needs_safeguards", "needs_owner")
    }
    signal_counts = {
        signal: sum(1 for record in records if signal in record.detected_flag_signals)
        for signal in _SIGNAL_ORDER
    }
    return TaskFeatureFlagReadinessPlan(
        plan_id=plan_id,
        records=records,
        flagged_task_ids=flagged_task_ids,
        summary={
            "flagged_task_count": len(records),
            "readiness_counts": readiness_counts,
            "signal_counts": signal_counts,
        },
    )


def task_feature_flag_readiness_plan_to_dict(
    result: TaskFeatureFlagReadinessPlan,
) -> dict[str, Any]:
    """Serialize a feature flag readiness plan to a plain dictionary."""
    return result.to_dict()


task_feature_flag_readiness_plan_to_dict.__test__ = False


def task_feature_flag_readiness_plan_to_markdown(
    result: TaskFeatureFlagReadinessPlan,
) -> str:
    """Render a feature flag readiness plan as Markdown."""
    return result.to_markdown()


task_feature_flag_readiness_plan_to_markdown.__test__ = False


def summarize_task_feature_flag_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskFeatureFlagReadinessPlan:
    """Compatibility alias for building feature flag readiness plans."""
    return build_task_feature_flag_readiness_plan(source)


def _task_record(
    task: Mapping[str, Any],
    index: int,
    *,
    plan_commands: tuple[str, ...],
) -> TaskFeatureFlagReadinessRecord | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    detected = tuple(sorted(signals, key=lambda signal: _SIGNAL_ORDER[signal]))
    if not detected:
        return None

    evidence = tuple(_dedupe(item for signal in detected for item in signals[signal]))
    commands = tuple(_dedupe([*_task_validation_commands(task), *plan_commands]))
    safeguards = _rollout_safeguards(task, title=title)
    missing = _missing_safeguards(task, safeguards, commands)
    readiness = _readiness_level(safeguards, missing)

    return TaskFeatureFlagReadinessRecord(
        task_id=task_id,
        title=title,
        readiness_level=readiness,
        detected_flag_signals=detected,
        required_steps=tuple(_required_steps(detected, missing, has_commands=bool(commands))),
        rollout_safeguards=safeguards,
        suggested_validation_commands=commands,
        evidence=evidence,
    )


def _signals(task: Mapping[str, Any]) -> dict[FeatureFlagSignal, tuple[str, ...]]:
    signals: dict[FeatureFlagSignal, list[str]] = {}

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _add_path_signals(signals, path)

    for source_field, text in _candidate_texts(task):
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signals.setdefault(signal, []).append(_evidence_snippet(source_field, text))

    return {
        signal: tuple(_dedupe(evidence))
        for signal, evidence in signals.items()
        if evidence
    }


def _add_path_signals(signals: dict[FeatureFlagSignal, list[str]], original: str) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    evidence = f"files_or_modules: {original}"

    if (
        bool({"flags", "feature_flags", "feature-flags", "toggles", "experiments"} & parts)
        or any(token in normalized for token in ("feature_flag", "feature-flag", "launchdarkly"))
        or "flag" in name
    ):
        signals.setdefault("feature_flag", []).append(evidence)
    if bool({"toggles", "toggle"} & parts) or "toggle" in name:
        signals.setdefault("toggle", []).append(evidence)
    if "kill" in name or "killswitch" in normalized:
        signals.setdefault("kill_switch", []).append(evidence)
    if bool({"cohorts", "segments", "audiences"} & parts):
        signals.setdefault("cohort", []).append(evidence)
    if bool({"experiments", "experiment", "ab_tests", "ab-tests"} & parts):
        signals.setdefault("experiment", []).append(evidence)
    if "beta" in normalized or "early_access" in normalized or "early-access" in normalized:
        signals.setdefault("beta_access", []).append(evidence)
    if any(token in normalized for token in ("rollout", "canary", "progressive_delivery")):
        signals.setdefault("percentage_rollout", []).append(evidence)


def _rollout_safeguards(task: Mapping[str, Any], *, title: str) -> dict[str, str]:
    metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}
    owner = (
        _first_metadata_value(metadata, "flag_owner", "owner", "owner_team", "rollout_owner")
        or _optional_text(task.get("owner_type"))
        or "Assign a directly accountable owner before enabling the flag."
    )
    default_state = (
        _first_metadata_value(metadata, "default_state", "flag_default", "default")
        or _default_state_from_text(_task_context(task))
        or "off until validation passes"
    )
    kill_switch = (
        _first_metadata_value(metadata, "kill_switch", "killswitch", "disable_path")
        or _kill_switch_from_text(_task_context(task))
        or "Document the operator path to disable the flag without a deploy."
    )
    cohort = (
        _first_metadata_value(metadata, "cohort", "cohorts", "target_cohort", "audience", "segment")
        or _cohort_from_text(_task_context(task))
        or "Define the initial cohort and expansion criteria."
    )
    rollback = (
        _first_metadata_value(metadata, "rollback", "rollback_behavior", "rollback_plan")
        or _rollback_from_text(_task_context(task))
        or "Disable the flag, verify baseline behavior, and revert code only if disabling is insufficient."
    )
    return {
        "owner": _clean_sentence(owner),
        "default_state": _clean_sentence(default_state),
        "kill_switch": _clean_sentence(kill_switch),
        "cohort_definition": _clean_sentence(cohort),
        "rollback_behavior": _clean_sentence(rollback),
    }


def _missing_safeguards(
    task: Mapping[str, Any],
    safeguards: Mapping[str, str],
    commands: tuple[str, ...],
) -> tuple[str, ...]:
    missing: list[str] = []
    if safeguards["owner"].startswith("Assign "):
        missing.append("owner")
    if safeguards["default_state"] == "off until validation passes":
        missing.append("default_state")
    if safeguards["kill_switch"].startswith("Document "):
        missing.append("kill_switch")
    if safeguards["cohort_definition"].startswith("Define "):
        missing.append("cohort_definition")
    if safeguards["rollback_behavior"].startswith("Disable the flag"):
        missing.append("rollback_behavior")
    if not commands and _has_command_hint_words(task):
        missing.append("validation_commands")
    return tuple(missing)


def _readiness_level(
    safeguards: Mapping[str, str],
    missing: tuple[str, ...],
) -> FeatureFlagReadinessLevel:
    if "owner" in missing:
        return "needs_owner"
    if missing:
        return "needs_safeguards"
    if any(value.endswith(".") for value in safeguards.values()):
        return "needs_safeguards"
    return "ready"


def _required_steps(
    detected: tuple[FeatureFlagSignal, ...],
    missing: tuple[str, ...],
    *,
    has_commands: bool,
) -> list[str]:
    steps = [
        "Confirm flag owner before implementation starts.",
        "Keep the default state disabled until validation passes.",
        "Verify a kill switch or immediate disable path exists.",
        "Define the first cohort, expansion criteria, and exclusion rules.",
        "Document rollback behavior for flag disablement and code revert fallback.",
    ]
    if "percentage_rollout" in detected or "staged_enablement" in detected:
        steps.append("Record the staged rollout percentages and promotion gates.")
    if "experiment" in detected:
        steps.append("Confirm control, treatment, and guardrail metrics before exposure.")
    if "beta_access" in detected:
        steps.append("Confirm beta allowlist ownership and removal criteria.")
    if has_commands:
        steps.append("Run the suggested validation commands before each exposure increase.")
    elif "validation_commands" in missing:
        steps.append("Add validation commands before increasing exposure.")
    return _dedupe(steps)


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in (
        "test_command",
        "suggested_test_command",
        "validation_command",
        "validation_commands",
    ):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value))
        else:
            commands.extend(_strings(value))

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "validation_commands",
            "validation_command",
            "test_commands",
            "test_command",
        ):
            value = metadata.get(key)
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


def _plan_validation_commands(plan: Mapping[str, Any]) -> tuple[str, ...]:
    metadata = plan.get("metadata")
    if not isinstance(metadata, Mapping):
        return ()
    value = metadata.get("validation_commands") or metadata.get("validation_command")
    if isinstance(value, Mapping):
        return tuple(flatten_validation_commands(value))
    return tuple(_strings(value))


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, list[dict[str, Any]], tuple[str, ...]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")], ()
    if isinstance(source, ExecutionPlan):
        return (
            _optional_text(source.id),
            [task.model_dump(mode="python") for task in source.tasks],
            _plan_validation_commands(source.model_dump(mode="python")),
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return (
                _optional_text(payload.get("id")),
                _task_payloads(payload.get("tasks")),
                _plan_validation_commands(payload),
            )
        return None, [dict(source)], ()

    try:
        iterator = iter(source)
    except TypeError:
        return None, [], ()

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
    return None, tasks, ()


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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes"):
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
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                key_text = str(key).replace("_", " ")
                if any(pattern.search(key_text) for pattern in _SIGNAL_PATTERNS.values()):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(
                pattern.search(str(key).replace("_", " "))
                for pattern in _SIGNAL_PATTERNS.values()
            ):
                texts.append((field, str(key)))
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


def _default_state_from_text(context: str) -> str | None:
    if re.search(r"\b(?:default(?:s)?\s+(?:off|disabled)|off by default|disabled by default)\b", context, re.IGNORECASE):
        return "off by default"
    if re.search(r"\b(?:default(?:s)?\s+(?:on|enabled)|on by default|enabled by default)\b", context, re.IGNORECASE):
        return "on by default"
    return None


def _kill_switch_from_text(context: str) -> str | None:
    if _SIGNAL_PATTERNS["kill_switch"].search(context):
        return "kill switch documented in task context"
    return None


def _cohort_from_text(context: str) -> str | None:
    match = re.search(
        r"\b(?:cohort|audience|segment|target users?|beta users?)\s*[:=-]\s*([^.;\n]+)",
        context,
        re.IGNORECASE,
    )
    if match:
        return match.group(1)
    if re.search(r"\binternal (?:users|staff|employees)\b", context, re.IGNORECASE):
        return "internal users"
    if re.search(r"\bbeta (?:users|customers|accounts)\b", context, re.IGNORECASE):
        return "beta users"
    return None


def _rollback_from_text(context: str) -> str | None:
    match = re.search(r"\brollback\s*[:=-]\s*([^.;\n]+)", context, re.IGNORECASE)
    if match:
        return match.group(1)
    if re.search(r"\b(?:rollback|backout|revert)\b", context, re.IGNORECASE):
        return "disable the flag and follow the documented rollback path"
    return None


def _has_command_hint_words(task: Mapping[str, Any]) -> bool:
    context = _task_context(task)
    return bool(re.search(r"\b(?:validation|test command|pytest|lint|smoke test)\b", context, re.IGNORECASE))


def _task_context(task: Mapping[str, Any]) -> str:
    values = [text for _, text in _candidate_texts(task)]
    values.extend(_strings(task.get("files_or_modules") or task.get("files")))
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


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _clean_sentence(value: str) -> str:
    return _SPACE_RE.sub(" ", str(value)).strip().strip("`'\",;:()[]{}").rstrip(".")


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
    "FeatureFlagReadinessLevel",
    "FeatureFlagSignal",
    "TaskFeatureFlagReadinessPlan",
    "TaskFeatureFlagReadinessRecord",
    "build_task_feature_flag_readiness_plan",
    "summarize_task_feature_flag_readiness",
    "task_feature_flag_readiness_plan_to_dict",
    "task_feature_flag_readiness_plan_to_markdown",
]
