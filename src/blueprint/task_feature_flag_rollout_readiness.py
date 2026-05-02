"""Plan feature flag and staged rollout readiness tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


RolloutControlSignal = Literal[
    "feature_flag",
    "default_state",
    "targeting",
    "beta_cohort",
    "percentage_rollout",
    "monitoring_gate",
    "rollback",
    "kill_switch",
    "cleanup",
    "qa_coverage",
]
RolloutReadinessLevel = Literal["ready", "partial", "needs_rollout_tasks"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[RolloutControlSignal, ...] = (
    "feature_flag",
    "default_state",
    "targeting",
    "beta_cohort",
    "percentage_rollout",
    "monitoring_gate",
    "rollback",
    "kill_switch",
    "cleanup",
    "qa_coverage",
)
_READINESS_ORDER: dict[RolloutReadinessLevel, int] = {
    "needs_rollout_tasks": 0,
    "partial": 1,
    "ready": 2,
}
_PATH_SIGNAL_PATTERNS: tuple[tuple[RolloutControlSignal, re.Pattern[str]], ...] = (
    ("feature_flag", re.compile(r"(?:^|/)(?:flags?|feature[-_]?flags?|toggles?|launchdarkly|unleash)(?:/|$)|flag", re.I)),
    ("targeting", re.compile(r"(?:^|/)(?:cohorts?|segments?|audiences?|targeting)(?:/|$)", re.I)),
    ("beta_cohort", re.compile(r"(?:^|/)(?:beta|early[-_]?access|preview)(?:/|$)|beta", re.I)),
    ("percentage_rollout", re.compile(r"(?:^|/)(?:rollouts?|ramps?|canary|progressive[-_]?delivery)(?:/|$)", re.I)),
    ("kill_switch", re.compile(r"kill[-_]?switch|killswitch|emergency[-_]?off", re.I)),
    ("cleanup", re.compile(r"(?:^|/)(?:cleanup|cleanups|flag[-_]?removal|retire[-_]?flags?)(?:/|$)|remove[-_]?flag", re.I)),
)
_TEXT_SIGNAL_PATTERNS: dict[RolloutControlSignal, re.Pattern[str]] = {
    "feature_flag": re.compile(
        r"\b(?:feature[- ]?flags?|release[- ]?flags?|flag key|flag config|flagged rollout|"
        r"feature[- ]?toggles?|toggles?|launchdarkly|unleash|split\.io|flipper)\b",
        re.I,
    ),
    "default_state": re.compile(
        r"\b(?:default(?:s)? (?:off|on|disabled|enabled)|off by default|on by default|"
        r"disabled by default|enabled by default|initial state|default state)\b",
        re.I,
    ),
    "targeting": re.compile(
        r"\b(?:cohorts?|segments?|audiences?|target(?:ing|ed)?(?: users?| accounts?| tenants?| merchants?)?|tenant allowlist|"
        r"allowlist|whitelist|denylist|eligibility|exclusion rules?)\b",
        re.I,
    ),
    "beta_cohort": re.compile(
        r"\b(?:private beta|public beta|beta users?|beta customers?|beta accounts?|"
        r"early access|preview cohort|limited availability)\b",
        re.I,
    ),
    "percentage_rollout": re.compile(
        r"\b(?:\d{1,3}\s*%\s*(?:of\s+)?(?:users|traffic|accounts|tenants|requests)?|"
        r"percentage rollout|percent rollout|gradual rollout|progressive rollout|"
        r"progressive delivery|ramp(?:ing)?|canary|staged rollout|phased rollout)\b",
        re.I,
    ),
    "monitoring_gate": re.compile(
        r"\b(?:monitoring gates?|promotion gates?|rollout gates?|guardrails?|go/no-go|"
        r"go no go|metrics? threshold|error rate|latency|dashboard|alerts?|slo)\b",
        re.I,
    ),
    "rollback": re.compile(
        r"\b(?:rollback|roll back|backout|back out|revert|disable flag|turn off|abort rollout|"
        r"pause rollout|fallback)\b",
        re.I,
    ),
    "kill_switch": re.compile(
        r"\b(?:kill switch|killswitch|emergency off|instant(?:ly)? disable|disable quickly|"
        r"operator off switch|circuit breaker)\b",
        re.I,
    ),
    "cleanup": re.compile(
        r"\b(?:cleanup|clean up|remove flag|delete flag|retire flag|decommission flag|"
        r"stale flag|flag removal|sunset flag|100% rollout)\b",
        re.I,
    ),
    "qa_coverage": re.compile(
        r"\b(?:qa|test coverage|validation criteria|enabled state|disabled state|flag on|"
        r"flag off|both states|on and off states|regression tests?)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class TaskFeatureFlagRolloutReadinessRecord:
    """Readiness tasks for one feature flag or staged rollout task."""

    task_id: str
    title: str
    detected_signals: tuple[RolloutControlSignal, ...]
    readiness_level: RolloutReadinessLevel
    rollout_tasks: tuple[str, ...] = field(default_factory=tuple)
    validation_criteria: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def recommended_readiness_steps(self) -> tuple[str, ...]:
        """Compatibility view for planners that name rollout tasks readiness steps."""
        return self.rollout_tasks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "readiness_level": self.readiness_level,
            "rollout_tasks": list(self.rollout_tasks),
            "validation_criteria": list(self.validation_criteria),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskFeatureFlagRolloutReadinessPlan:
    """Plan-level feature flag rollout readiness tasks."""

    plan_id: str | None = None
    records: tuple[TaskFeatureFlagRolloutReadinessRecord, ...] = field(default_factory=tuple)
    rollout_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def flagged_task_ids(self) -> tuple[str, ...]:
        """Compatibility view for feature flag planners."""
        return self.rollout_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "rollout_task_ids": list(self.rollout_task_ids),
            "flagged_task_ids": list(self.flagged_task_ids),
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render feature flag rollout readiness as deterministic Markdown."""
        title = "# Task Feature Flag Rollout Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Rollout-control task count: {self.summary.get('rollout_task_count', 0)}",
            f"- Generated rollout task count: {self.summary.get('generated_rollout_task_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No feature flag rollout readiness tasks were inferred."])
            if self.no_signal_task_ids:
                lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Rollout Tasks | Validation Criteria | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell('; '.join(record.rollout_tasks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.validation_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_signal_task_ids:
            lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
        return "\n".join(lines)


def build_task_feature_flag_rollout_readiness_plan(source: Any) -> TaskFeatureFlagRolloutReadinessPlan:
    """Build readiness tasks for feature flag and staged rollout work."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_READINESS_ORDER[record.readiness_level], record.task_id, record.title.casefold()),
        )
    )
    rollout_task_ids = tuple(record.task_id for record in records)
    no_signal_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskFeatureFlagRolloutReadinessPlan(
        plan_id=plan_id,
        records=records,
        rollout_task_ids=rollout_task_ids,
        no_signal_task_ids=no_signal_task_ids,
        summary=_summary(records, task_count=len(tasks), no_signal_task_ids=no_signal_task_ids),
    )


def analyze_task_feature_flag_rollout_readiness(source: Any) -> TaskFeatureFlagRolloutReadinessPlan:
    """Compatibility alias for building rollout readiness plans."""
    return build_task_feature_flag_rollout_readiness_plan(source)


def summarize_task_feature_flag_rollout_readiness(source: Any) -> TaskFeatureFlagRolloutReadinessPlan:
    """Build a rollout readiness plan, accepting an existing plan unchanged."""
    if isinstance(source, TaskFeatureFlagRolloutReadinessPlan):
        return source
    return build_task_feature_flag_rollout_readiness_plan(source)


def extract_task_feature_flag_rollout_readiness(source: Any) -> TaskFeatureFlagRolloutReadinessPlan:
    """Compatibility alias for extracting rollout readiness plans."""
    return build_task_feature_flag_rollout_readiness_plan(source)


def generate_task_feature_flag_rollout_readiness(source: Any) -> TaskFeatureFlagRolloutReadinessPlan:
    """Compatibility alias for generating rollout readiness plans."""
    return build_task_feature_flag_rollout_readiness_plan(source)


def task_feature_flag_rollout_readiness_plan_to_dict(
    result: TaskFeatureFlagRolloutReadinessPlan,
) -> dict[str, Any]:
    """Serialize a rollout readiness plan to a plain dictionary."""
    return result.to_dict()


task_feature_flag_rollout_readiness_plan_to_dict.__test__ = False


def task_feature_flag_rollout_readiness_plan_to_dicts(
    source: TaskFeatureFlagRolloutReadinessPlan | Iterable[TaskFeatureFlagRolloutReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize rollout readiness records to plain dictionaries."""
    if isinstance(source, TaskFeatureFlagRolloutReadinessPlan):
        return source.to_dicts()
    return [record.to_dict() for record in source]


task_feature_flag_rollout_readiness_plan_to_dicts.__test__ = False


def task_feature_flag_rollout_readiness_plan_to_markdown(
    result: TaskFeatureFlagRolloutReadinessPlan,
) -> str:
    """Render a rollout readiness plan as Markdown."""
    return result.to_markdown()


task_feature_flag_rollout_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[RolloutControlSignal, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskFeatureFlagRolloutReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    rollout_tasks = _rollout_tasks(signals.signals)
    validation_criteria = _validation_criteria(signals.signals)
    return TaskFeatureFlagRolloutReadinessRecord(
        task_id=task_id,
        title=title,
        detected_signals=signals.signals,
        readiness_level=_readiness(signals.signals),
        rollout_tasks=rollout_tasks,
        validation_criteria=validation_criteria,
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    hits: set[RolloutControlSignal] = set()
    evidence: list[str] = []

    for path in _strings(
        task.get("files_or_modules")
        or task.get("files")
        or task.get("expected_files")
        or task.get("expected_file_paths")
    ):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_hits = _path_signals(normalized)
        if path_hits:
            hits.update(path_hits)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        text_hits = _text_signals(text)
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        text_hits = tuple(_ordered_dedupe([*text_hits, *_text_signals(searchable)], _SIGNAL_ORDER))
        if text_hits:
            hits.update(text_hits)
            evidence.append(_evidence_snippet(source_field, text))

    if hits and "feature_flag" not in hits and any(signal in hits for signal in ("kill_switch", "percentage_rollout", "beta_cohort")):
        hits.add("feature_flag")

    return _Signals(
        signals=tuple(_ordered_dedupe(hits, _SIGNAL_ORDER)),
        evidence=tuple(_dedupe(evidence)),
    )


def _path_signals(path: str) -> tuple[RolloutControlSignal, ...]:
    normalized = path.casefold()
    searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    hits: list[RolloutControlSignal] = []
    for signal, pattern in _PATH_SIGNAL_PATTERNS:
        if pattern.search(normalized) or pattern.search(searchable):
            hits.append(signal)
    name = PurePosixPath(normalized).name
    if name in {"feature_flags.yml", "feature_flags.yaml", "flags.yml", "flags.yaml"}:
        hits.append("feature_flag")
    hits.extend(_text_signals(searchable))
    return tuple(_ordered_dedupe(hits, _SIGNAL_ORDER))


def _text_signals(text: str) -> tuple[RolloutControlSignal, ...]:
    return tuple(signal for signal in _SIGNAL_ORDER if _TEXT_SIGNAL_PATTERNS[signal].search(text))


def _rollout_tasks(signals: tuple[RolloutControlSignal, ...]) -> tuple[str, ...]:
    signal_set = set(signals)
    tasks = [
        "Create or update the feature flag with a named owner, stable flag key, and environment-specific configuration.",
        "Set the default state explicitly and keep production disabled until rollout gates pass.",
        "Define cohort targeting, eligibility, exclusions, and auditability for every exposure decision.",
        "Define monitoring gates with metrics, thresholds, owners, and go/no-go decisions before expansion.",
        "Document rollback behavior, including how to disable the flag and verify baseline behavior.",
        "Add cleanup criteria for removing the flag, stale branches, configuration, and documentation after rollout completion.",
        "Add QA coverage for enabled and disabled flag states before production exposure.",
    ]
    if "beta_cohort" in signal_set:
        tasks.insert(3, "Configure beta cohort targeting with allowlist ownership, entry criteria, exit criteria, and support visibility.")
    if "percentage_rollout" in signal_set:
        tasks.insert(3, "Define progressive rollout percentages, hold times, promotion gates, and pause conditions.")
    if "kill_switch" in signal_set:
        tasks.insert(5, "Implement and rehearse kill switch behavior that can disable the feature without a code deploy.")
    if "cleanup" in signal_set:
        tasks.append("Schedule the cleanup task only after the flag reaches 100% rollout or the beta/experiment decision is final.")
    return tuple(_dedupe(tasks))


def _validation_criteria(signals: tuple[RolloutControlSignal, ...]) -> tuple[str, ...]:
    criteria = [
        "Disabled state validation confirms existing users see baseline behavior and no new side effects run.",
        "Enabled state validation confirms targeted users see the new behavior and flag exposure is recorded.",
        "Targeting validation confirms ineligible users, tenants, and excluded cohorts remain unexposed.",
        "Rollback validation confirms disabling the flag or kill switch restores baseline behavior without a deploy.",
        "Cleanup validation confirms no stale flag references, dead branches, or obsolete configuration remain.",
    ]
    signal_set = set(signals)
    if "beta_cohort" in signal_set:
        criteria.insert(3, "Beta cohort validation confirms only allowlisted beta users receive access and support can identify them.")
    if "percentage_rollout" in signal_set:
        criteria.insert(3, "Percentage rollout validation confirms each ramp level exposes the intended traffic share and stops on gate failure.")
    if "kill_switch" in signal_set:
        criteria.insert(4, "Kill switch validation confirms operators can turn the feature off immediately and observe the disabled state.")
    return tuple(_dedupe(criteria))


def _readiness(signals: tuple[RolloutControlSignal, ...]) -> RolloutReadinessLevel:
    coverage = len(set(signals))
    if {"feature_flag", "default_state", "targeting", "monitoring_gate", "rollback", "qa_coverage"} <= set(signals):
        return "ready"
    if coverage >= 3:
        return "partial"
    return "needs_rollout_tasks"


def _summary(
    records: tuple[TaskFeatureFlagRolloutReadinessRecord, ...],
    *,
    task_count: int,
    no_signal_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "rollout_task_count": len(records),
        "rollout_task_ids": [record.task_id for record in records],
        "flagged_task_ids": [record.task_id for record in records],
        "no_signal_task_count": len(no_signal_task_ids),
        "no_signal_task_ids": list(no_signal_task_ids),
        "generated_rollout_task_count": sum(len(record.rollout_tasks) for record in records),
        "validation_criteria_count": sum(len(record.validation_criteria) for record in records),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness_level == readiness)
            for readiness in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "status": (
            "no_rollout_control_signals"
            if not records
            else "ready"
            if all(record.readiness_level == "ready" for record in records)
            else "rollout_tasks_needed"
        ),
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
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
        iterator = iter(source)
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
        "expected_files",
        "expected_file_paths",
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "risk",
        "risk_level",
        "test_command",
        "validation_commands",
        "status",
        "metadata",
        "tags",
        "labels",
        "notes",
        "blocked_reason",
        "tasks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "depends_on",
        "tags",
        "labels",
        "notes",
        "validation_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return tuple(texts)


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
    return any(pattern.search(value) for pattern in _TEXT_SIGNAL_PATTERNS.values())


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
    path = value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
    return str(PurePosixPath(path)) if path else ""


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


def _ordered_dedupe(items: Iterable[_T], order: tuple[_T, ...]) -> list[_T]:
    seen = set(items)
    return [item for item in order if item in seen]


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
    "RolloutControlSignal",
    "RolloutReadinessLevel",
    "TaskFeatureFlagRolloutReadinessPlan",
    "TaskFeatureFlagRolloutReadinessRecord",
    "analyze_task_feature_flag_rollout_readiness",
    "build_task_feature_flag_rollout_readiness_plan",
    "extract_task_feature_flag_rollout_readiness",
    "generate_task_feature_flag_rollout_readiness",
    "summarize_task_feature_flag_rollout_readiness",
    "task_feature_flag_rollout_readiness_plan_to_dict",
    "task_feature_flag_rollout_readiness_plan_to_dicts",
    "task_feature_flag_rollout_readiness_plan_to_markdown",
]
