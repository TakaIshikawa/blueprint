"""Plan notification preference implementation readiness work for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


NotificationPreferenceSignal = Literal[
    "notification_preference",
    "subscription",
    "unsubscribe",
    "mute_setting",
    "digest_cadence",
    "channel_preference",
    "preference_center",
]
NotificationPreferenceReadinessCategory = Literal[
    "unsubscribe_flow",
    "preference_persistence",
    "default_state",
    "channel_settings",
    "audit_trail",
    "migration_handling",
    "denied_channel_tests",
]
NotificationPreferenceReadinessLevel = Literal["needs_planning", "partial", "ready"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[NotificationPreferenceReadinessLevel, int] = {
    "needs_planning": 0,
    "partial": 1,
    "ready": 2,
}
_SIGNAL_ORDER: tuple[NotificationPreferenceSignal, ...] = (
    "notification_preference",
    "subscription",
    "unsubscribe",
    "mute_setting",
    "digest_cadence",
    "channel_preference",
    "preference_center",
)
_CATEGORY_ORDER: tuple[NotificationPreferenceReadinessCategory, ...] = (
    "unsubscribe_flow",
    "preference_persistence",
    "default_state",
    "channel_settings",
    "audit_trail",
    "migration_handling",
    "denied_channel_tests",
)
_SIGNAL_PATTERNS: dict[NotificationPreferenceSignal, re.Pattern[str]] = {
    "notification_preference": re.compile(
        r"\b(?:notification preference|notification setting|notification option|"
        r"notification control|notification management|notification config|"
        r"preference (?:model|persist|storage|manage|center|setting))\b",
        re.I,
    ),
    "subscription": re.compile(
        r"\b(?:subscribe|subscription|unsubscribe|opt[- ]?in|opt[- ]?out|"
        r"email subscription|notification subscription|subscribe to|subscription manage|subscription (?:model|persist|storage))\b",
        re.I,
    ),
    "unsubscribe": re.compile(
        r"\b(?:unsubscribe|opt[- ]?out|disable notification|turn off|"
        r"stop notification|cancel subscription|unsubscribe link|one[- ]click unsubscribe)\b",
        re.I,
    ),
    "mute_setting": re.compile(
        r"\b(?:mute|unmute|snooze|silence|quiet|do not disturb|dnd|"
        r"mute notification|mute channel|mute conversation|temporary silence)\b",
        re.I,
    ),
    "digest_cadence": re.compile(
        r"\b(?:digest|cadence|frequency|daily digest|weekly digest|"
        r"digest setting|notification frequency|delivery frequency|batch notification)\b",
        re.I,
    ),
    "channel_preference": re.compile(
        r"\b(?:channel preference|channel[- ]?specific preference|email preference|sms preference|push preference|"
        r"slack preference|delivery channel|notification channel|channel setting|"
        r"preferred channel|multi[- ]?channel|per[- ]?channel)\b",
        re.I,
    ),
    "preference_center": re.compile(
        r"\b(?:preference center|notification center|notification preferences|"
        r"subscription center|manage preferences|notification settings page|"
        r"preference dashboard|subscription management)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[NotificationPreferenceSignal, re.Pattern[str]] = {
    "notification_preference": re.compile(r"(?:notification[_-]?preference|notification[_-]?setting)", re.I),
    "subscription": re.compile(r"(?:subscri(?:be|ption)|opt[_-]?in|opt[_-]?out)", re.I),
    "unsubscribe": re.compile(r"(?:unsubscribe|opt[_-]?out)", re.I),
    "mute_setting": re.compile(r"(?:mute|snooze|silence|quiet|dnd)", re.I),
    "digest_cadence": re.compile(r"(?:digest|cadence|frequency|batch)", re.I),
    "channel_preference": re.compile(r"(?:channel[_-]?preference|email[_-]?preference|sms[_-]?preference|push[_-]?preference)", re.I),
    "preference_center": re.compile(r"(?:preference[_-]?center|notification[_-]?center|subscription[_-]?center)", re.I),
}
_NO_PREFERENCE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:notification preference|subscription|unsubscribe|preference change)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|requirements?)\b",
    re.I,
)
_GENERIC_NOTIFICATION_RE = re.compile(
    r"\b(?:send notification|push notification|email notification|notification service|notification system)\b",
    re.I,
)
_ACTIONABLE_PREFERENCE_RE = re.compile(
    r"\b(?:subscribe|unsubscribe|opt[- ]?in|opt[- ]?out|mute|unmute|"
    r"preference center|notification setting|channel preference|digest|frequency|"
    r"preference persist|preference model|preference storage|preference audit|"
    r"preference migration|default preference)\b",
    re.I,
)
_CATEGORY_GUIDANCE: dict[NotificationPreferenceReadinessCategory, tuple[str, tuple[str, ...]]] = {
    "unsubscribe_flow": (
        "Implement unsubscribe and re-subscribe flow for notification preferences.",
        (
            "Users can unsubscribe from notification types without contacting support.",
            "Unsubscribe takes effect immediately and persists across sessions.",
            "Tests cover unsubscribe, re-subscribe, and preference enforcement.",
        ),
    ),
    "preference_persistence": (
        "Persist notification preference state in a durable model.",
        (
            "Preferences are stored in a persistent model with user, channel, notification type, and state.",
            "Preference reads use the persisted source of truth, not session or cache-only state.",
            "Tests verify preference persistence, retrieval, and update correctness.",
        ),
    ),
    "default_state": (
        "Define default notification preference state for new users and new notification types.",
        (
            "New users receive a documented default preference state (opt-in or opt-out).",
            "New notification types added later respect user preferences or default to a safe state.",
            "Tests cover first-time user defaults and new notification type defaults.",
        ),
    ),
    "channel_settings": (
        "Support channel-specific notification preferences (email, SMS, push, Slack).",
        (
            "Users can configure preferences per channel independently.",
            "Channel settings are stored and retrieved separately for each delivery method.",
            "Tests cover multi-channel preference updates and channel-specific delivery enforcement.",
        ),
    ),
    "audit_trail": (
        "Record auditable preference change events for notification settings.",
        (
            "Preference changes log actor, timestamp, previous value, new value, and source surface.",
            "Audit trail is append-only or tamper-evident for compliance review.",
            "Tests verify audit records for subscribe, unsubscribe, mute, and preference center changes.",
        ),
    ),
    "migration_handling": (
        "Handle migration or backfill for existing users when preference schema changes.",
        (
            "Existing users receive a documented migration strategy when preferences change.",
            "Migration preserves existing user intent or provides a safe fallback.",
            "Tests cover pre-migration state, migration execution, and post-migration verification.",
        ),
    ),
    "denied_channel_tests": (
        "Test notification delivery enforcement for denied channels and muted settings.",
        (
            "Tests verify that notifications respect unsubscribe, mute, and channel denial state.",
            "Denied channels do not receive notifications even when upstream triggers fire.",
            "Tests cover edge cases: unsubscribe before delivery, concurrent preference changes, and stale cache states.",
        ),
    ),
}


@dataclass(frozen=True, slots=True)
class NotificationPreferenceReadinessTask:
    """One generated implementation task for notification preference readiness."""

    category: NotificationPreferenceReadinessCategory
    title: str
    description: str
    acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "title": self.title,
            "description": self.description,
            "acceptance_criteria": list(self.acceptance_criteria),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskNotificationPreferenceReadinessRecord:
    """Notification preference readiness guidance for one execution task or requirement text."""

    task_id: str
    title: str
    detected_signals: tuple[NotificationPreferenceSignal, ...]
    present_safeguards: tuple[NotificationPreferenceReadinessCategory, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[NotificationPreferenceReadinessCategory, ...] = field(default_factory=tuple)
    risk_level: NotificationPreferenceReadinessLevel = "needs_planning"
    generated_tasks: tuple[NotificationPreferenceReadinessTask, ...] = field(default_factory=tuple)
    suggested_validation_commands: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[NotificationPreferenceSignal, ...]:
        """Compatibility view for planners that name detected signals matched signals."""
        return self.detected_signals

    @property
    def recommended_tasks(self) -> tuple[NotificationPreferenceReadinessTask, ...]:
        """Compatibility view for generated readiness tasks."""
        return self.generated_tasks

    @property
    def readiness(self) -> NotificationPreferenceReadinessLevel:
        """Compatibility view for planners that expose readiness as risk_level."""
        return self.risk_level

    @property
    def acceptance_criteria(self) -> tuple[str, ...]:
        """Flatten generated task acceptance criteria for simple consumers."""
        return tuple(criteria for task in self.generated_tasks for criteria in task.acceptance_criteria)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "generated_tasks": [task.to_dict() for task in self.generated_tasks],
            "suggested_validation_commands": list(self.suggested_validation_commands),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskNotificationPreferenceReadinessPlan:
    """Plan-level notification preference readiness tasks."""

    plan_id: str | None = None
    records: tuple[TaskNotificationPreferenceReadinessRecord, ...] = field(default_factory=tuple)
    preference_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskNotificationPreferenceReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskNotificationPreferenceReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    @property
    def impacted_task_ids(self) -> tuple[str, ...]:
        """Compatibility view matching planners that expose impacted task ids."""
        return self.preference_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "preference_task_ids": list(self.preference_task_ids),
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return notification preference readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render notification preference readiness guidance as deterministic Markdown."""
        title = "# Task Notification Preference Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        category_counts = self.summary.get("generated_task_category_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Preference task count: {self.summary.get('preference_task_count', 0)}",
            f"- Generated readiness task count: {self.summary.get('generated_task_count', 0)}",
            "- Risk level counts: "
            + ", ".join(f"{level} {risk_counts.get(level, 0)}" for level in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
            "- Generated task counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task notification preference readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk Level | Signals | Present Safeguards | Missing Safeguards | Generated Tasks | Validation | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            generated = "; ".join(f"{task.category}: {task.title}" for task in record.generated_tasks)
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell(generated or 'none')} | "
                f"{_markdown_cell('; '.join(record.suggested_validation_commands) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_notification_preference_readiness_plan(source: Any) -> TaskNotificationPreferenceReadinessPlan:
    """Build notification preference readiness records for task-shaped or requirement-text input."""
    plan_id, tasks, plan_commands = _source_payload(source)
    candidates = [
        _task_record(task, index, plan_commands=plan_commands)
        for index, task in enumerate(tasks, start=1)
    ]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.risk_level],
                -len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    preference_task_ids = tuple(record.task_id for record in records)
    impacted = set(preference_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted
    )
    return TaskNotificationPreferenceReadinessPlan(
        plan_id=plan_id,
        records=records,
        preference_task_ids=preference_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_notification_preference_readiness(source: Any) -> TaskNotificationPreferenceReadinessPlan:
    """Compatibility alias for building notification preference readiness plans."""
    return build_task_notification_preference_readiness_plan(source)


def recommend_task_notification_preference_readiness(source: Any) -> TaskNotificationPreferenceReadinessPlan:
    """Compatibility alias for recommending notification preference readiness tasks."""
    return build_task_notification_preference_readiness_plan(source)


def summarize_task_notification_preference_readiness(source: Any) -> TaskNotificationPreferenceReadinessPlan:
    """Compatibility alias for summarizing notification preference readiness plans."""
    if isinstance(source, TaskNotificationPreferenceReadinessPlan):
        return source
    return build_task_notification_preference_readiness_plan(source)


def generate_task_notification_preference_readiness(source: Any) -> TaskNotificationPreferenceReadinessPlan:
    """Compatibility alias for generating notification preference readiness plans."""
    return build_task_notification_preference_readiness_plan(source)


def extract_task_notification_preference_readiness(source: Any) -> TaskNotificationPreferenceReadinessPlan:
    """Compatibility alias for extracting notification preference readiness plans."""
    return build_task_notification_preference_readiness_plan(source)


def derive_task_notification_preference_readiness(source: Any) -> TaskNotificationPreferenceReadinessPlan:
    """Compatibility alias for deriving notification preference readiness plans."""
    return build_task_notification_preference_readiness_plan(source)


def task_notification_preference_readiness_plan_to_dict(
    result: TaskNotificationPreferenceReadinessPlan,
) -> dict[str, Any]:
    """Serialize a notification preference readiness plan to a plain dictionary."""
    return result.to_dict()


task_notification_preference_readiness_plan_to_dict.__test__ = False


def task_notification_preference_readiness_plan_to_dicts(
    result: TaskNotificationPreferenceReadinessPlan | Iterable[TaskNotificationPreferenceReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize notification preference readiness records to plain dictionaries."""
    if isinstance(result, TaskNotificationPreferenceReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_notification_preference_readiness_plan_to_dicts.__test__ = False
task_notification_preference_readiness_to_dicts = task_notification_preference_readiness_plan_to_dicts
task_notification_preference_readiness_to_dicts.__test__ = False


def task_notification_preference_readiness_plan_to_markdown(
    result: TaskNotificationPreferenceReadinessPlan,
) -> str:
    """Render a notification preference readiness plan as Markdown."""
    return result.to_markdown()


task_notification_preference_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[NotificationPreferenceSignal, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _task_record(
    task: Mapping[str, Any],
    index: int,
    *,
    plan_commands: tuple[str, ...],
) -> TaskNotificationPreferenceReadinessRecord | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None

    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    present_safeguards = _detect_present_safeguards(task, signals.signals)
    missing_safeguards = _detect_missing_safeguards(signals.signals, present_safeguards)
    generated_tasks = _generated_tasks(title, signals, missing_safeguards)
    commands = tuple(_dedupe([*_validation_commands(task), *plan_commands]))
    return TaskNotificationPreferenceReadinessRecord(
        task_id=task_id,
        title=title,
        detected_signals=signals.signals,
        present_safeguards=present_safeguards,
        missing_safeguards=missing_safeguards,
        risk_level=_risk_level(present_safeguards, missing_safeguards),
        generated_tasks=generated_tasks,
        suggested_validation_commands=commands,
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[NotificationPreferenceSignal] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(
        task.get("files_or_modules")
        or task.get("files")
        or task.get("expected_file_paths")
        or task.get("expected_files")
        or task.get("paths")
    ):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or _SIGNAL_PATTERNS[signal].search(searchable):
                signal_hits.add(signal)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        if _NO_PREFERENCE_RE.search(text):
            explicitly_no_impact = True
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    if signal_hits and signal_hits != {"notification_preference"}:
        signal_hits.add("notification_preference")
    if signal_hits == {"notification_preference"} and not _has_actionable_preference_context(task):
        signal_hits.clear()
        evidence.clear()
    if _has_only_generic_notification(task) and not _has_actionable_preference_context(task):
        signal_hits.clear()
        evidence.clear()

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _detect_present_safeguards(
    task: Mapping[str, Any],
    signals: tuple[NotificationPreferenceSignal, ...],
) -> tuple[NotificationPreferenceReadinessCategory, ...]:
    present: set[NotificationPreferenceReadinessCategory] = set()
    texts = [text for _, text in _candidate_texts(task)]
    context = " ".join(texts)

    if re.search(r"\b(?:unsubscribe|re[- ]?subscribe|opt[- ]?out|opt[- ]?in)\b", context, re.I):
        present.add("unsubscribe_flow")

    if re.search(
        r"\b(?:persist|save|store|database|model|table|column)\b.{0,100}"
        r"\b(?:preference|subscription|setting|notification)\b|"
        r"\b(?:preference|subscription|setting|notification)\b.{0,100}"
        r"\b(?:persist|save|store|database|model|table|column)\b|"
        r"\bpreference[_ ](?:model|storage|persist|table)\b|"
        r"\bsubscription[_ ](?:model|storage|persist|table)\b",
        context,
        re.I,
    ):
        present.add("preference_persistence")

    if re.search(r"\b(?:default|initial|new user|first[- ]time)\b.{0,100}\b(?:preference|subscription|state)\b", context, re.I):
        present.add("default_state")

    if re.search(r"\b(?:channel|email|sms|push|slack)\b.{0,100}\b(?:preference|setting|specific)\b", context, re.I):
        present.add("channel_settings")

    if re.search(r"\b(?:audit|history|log|track|event|timestamp)\b.{0,100}\b(?:preference|change|update)\b", context, re.I):
        present.add("audit_trail")

    if re.search(r"\b(?:migration|backfill|migrate|existing user)\b.{0,100}\b(?:preference|subscription)\b", context, re.I):
        present.add("migration_handling")

    if re.search(
        r"\b(?:test|verify|check|validate)\b.{0,100}"
        r"\b(?:denied|unsubscribe|mute|opt[- ]?out|blocked channel|no notification)\b",
        context,
        re.I,
    ):
        present.add("denied_channel_tests")

    return tuple(category for category in _CATEGORY_ORDER if category in present)


def _detect_missing_safeguards(
    signals: tuple[NotificationPreferenceSignal, ...],
    present_safeguards: tuple[NotificationPreferenceReadinessCategory, ...],
) -> tuple[NotificationPreferenceReadinessCategory, ...]:
    present = set(present_safeguards)
    required: set[NotificationPreferenceReadinessCategory] = set()

    if "unsubscribe" in signals or "subscription" in signals:
        required.update(
            {
                "unsubscribe_flow",
                "preference_persistence",
                "default_state",
                "denied_channel_tests",
            }
        )

    if "channel_preference" in signals or "digest_cadence" in signals:
        required.add("channel_settings")

    if "preference_center" in signals:
        required.update(
            {
                "unsubscribe_flow",
                "preference_persistence",
                "channel_settings",
                "audit_trail",
            }
        )

    if "mute_setting" in signals:
        required.update(
            {
                "preference_persistence",
                "denied_channel_tests",
            }
        )

    return tuple(category for category in _CATEGORY_ORDER if category in required - present)


def _generated_tasks(
    source_title: str,
    signals: _Signals,
    missing_safeguards: tuple[NotificationPreferenceReadinessCategory, ...],
) -> tuple[NotificationPreferenceReadinessTask, ...]:
    evidence = tuple(sorted(signals.evidence, key=_evidence_priority))[:3]
    rationale = "; ".join(evidence) if evidence else "Notification preference task context was detected."
    tasks: list[NotificationPreferenceReadinessTask] = []
    for category in _CATEGORY_ORDER:
        if category not in missing_safeguards:
            continue
        guidance, acceptance = _CATEGORY_GUIDANCE[category]
        tasks.append(
            NotificationPreferenceReadinessTask(
                category=category,
                title=f"{_category_title(category)} for {source_title}",
                description=f"{guidance} Rationale: {rationale}",
                acceptance_criteria=acceptance,
                evidence=evidence,
            )
        )
    return tuple(tasks)


def _risk_level(
    present_safeguards: tuple[NotificationPreferenceReadinessCategory, ...],
    missing_safeguards: tuple[NotificationPreferenceReadinessCategory, ...],
) -> NotificationPreferenceReadinessLevel:
    critical_missing = {
        "unsubscribe_flow",
        "preference_persistence",
        "denied_channel_tests",
    }
    if critical_missing & set(missing_safeguards):
        return "needs_planning"
    if missing_safeguards:
        return "partial"
    if {
        "unsubscribe_flow",
        "preference_persistence",
        "default_state",
        "denied_channel_tests",
    } <= set(present_safeguards):
        return "ready"
    return "partial"


def _summary(
    records: tuple[TaskNotificationPreferenceReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    generated_tasks = [task for record in records for task in record.generated_tasks]
    missing = [
        category for record in records for category in record.missing_safeguards
    ]
    return {
        "task_count": task_count,
        "preference_task_count": len(records),
        "preference_task_ids": [record.task_id for record in records],
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
        "generated_task_count": len(generated_tasks),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "generated_task_category_counts": {
            category: sum(1 for task in generated_tasks if task.category == category)
            for category in _CATEGORY_ORDER
        },
        "missing_safeguard_counts": {
            category: sum(1 for item in missing if item == category)
            for category in _CATEGORY_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]], tuple[str, ...]]:
    if isinstance(source, str):
        text = _optional_text(source)
        if not text:
            return None, [], ()
        return None, [{"id": "requirement-text", "title": "Notification preference requirements", "description": text}], ()
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")], ()
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks], _plan_validation_commands(payload)
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return (
                _optional_text(payload.get("id")),
                _task_payloads(payload.get("tasks")),
                _plan_validation_commands(payload),
            )
        return None, [dict(source)], ()
    if _looks_like_task(source):
        return None, [_object_payload(source)], ()
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks")), _plan_validation_commands(payload)

    try:
        iterator = iter(source)
    except TypeError:
        return None, [], ()

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks, ()


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
    if isinstance(value, str):
        text = _optional_text(value)
        return {"id": "requirement-text", "title": "Notification preference requirements", "description": text} if text else {}
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
        "expected_file_paths",
        "expected_files",
        "paths",
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
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
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "tags",
        "labels",
        "notes",
        "risks",
    ):
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
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in _SIGNAL_PATTERNS.values()):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in _SIGNAL_PATTERNS.values()):
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


def _validation_command_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    return [
        (f"validation_command[{index}]", command)
        for index, command in enumerate(_validation_commands(task))
        if _optional_text(command)
    ]


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in (
        "test_command",
        "test_commands",
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


def _has_actionable_preference_context(task: Mapping[str, Any]) -> bool:
    return any(_ACTIONABLE_PREFERENCE_RE.search(text) for _, text in _candidate_texts(task))


def _has_only_generic_notification(task: Mapping[str, Any]) -> bool:
    texts = [text for _, text in _candidate_texts(task)]
    if not texts:
        return False
    context = " ".join(texts)
    return bool(_GENERIC_NOTIFICATION_RE.search(context)) and not bool(_ACTIONABLE_PREFERENCE_RE.search(context))


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or _optional_text(task.get("task_id")) or f"task-{index}"


def _category_title(category: str) -> str:
    return category.replace("_", " ").title()


def _evidence_priority(value: str) -> tuple[int, str]:
    priority = 1
    if value.startswith(("title:", "description:", "acceptance_criteria")):
        priority = 0
    return priority, value.casefold()


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
    "NotificationPreferenceReadinessCategory",
    "NotificationPreferenceReadinessLevel",
    "NotificationPreferenceReadinessTask",
    "NotificationPreferenceSignal",
    "TaskNotificationPreferenceReadinessPlan",
    "TaskNotificationPreferenceReadinessRecord",
    "analyze_task_notification_preference_readiness",
    "build_task_notification_preference_readiness_plan",
    "derive_task_notification_preference_readiness",
    "extract_task_notification_preference_readiness",
    "generate_task_notification_preference_readiness",
    "recommend_task_notification_preference_readiness",
    "summarize_task_notification_preference_readiness",
    "task_notification_preference_readiness_plan_to_dict",
    "task_notification_preference_readiness_plan_to_dicts",
    "task_notification_preference_readiness_plan_to_markdown",
    "task_notification_preference_readiness_to_dicts",
]
