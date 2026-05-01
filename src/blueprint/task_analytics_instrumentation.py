"""Plan product analytics instrumentation readiness for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


AnalyticsInstrumentationSignal = Literal[
    "event_tracking",
    "funnel",
    "metrics",
    "dashboard",
    "experiment",
    "conversion",
    "activation",
    "retention",
    "attribution",
    "analytics_integration",
    "telemetry_schema",
]
AnalyticsInstrumentationReadiness = Literal["ready", "partial", "missing", "not_applicable"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: dict[AnalyticsInstrumentationSignal, int] = {
    "event_tracking": 0,
    "funnel": 1,
    "metrics": 2,
    "dashboard": 3,
    "experiment": 4,
    "conversion": 5,
    "activation": 6,
    "retention": 7,
    "attribution": 8,
    "analytics_integration": 9,
    "telemetry_schema": 10,
}
_READINESS_ORDER: dict[AnalyticsInstrumentationReadiness, int] = {
    "missing": 0,
    "partial": 1,
    "ready": 2,
    "not_applicable": 3,
}
_TEXT_SIGNAL_PATTERNS: dict[AnalyticsInstrumentationSignal, re.Pattern[str]] = {
    "event_tracking": re.compile(
        r"\b(?:analytics event|event tracking|track event|tracking event|instrument event|"
        r"product event|user event|clickstream|page view|screen view|track\(|identify\(|"
        r"group\(|alias\()\b",
        re.I,
    ),
    "funnel": re.compile(r"\b(?:funnel|checkout funnel|onboarding funnel|drop[- ]?off|step conversion)\b", re.I),
    "metrics": re.compile(
        r"\b(?:metric|metrics|kpi|north star|success metric|product metric|usage metric|"
        r"engagement rate|adoption rate)\b",
        re.I,
    ),
    "dashboard": re.compile(r"\b(?:dashboard|reporting view|analytics report|looker|mode report|metabase)\b", re.I),
    "experiment": re.compile(
        r"\b(?:experiment|a/b test|ab test|split test|variant|control group|treatment|"
        r"experiment assignment|feature flag experiment)\b",
        re.I,
    ),
    "conversion": re.compile(r"\b(?:conversion|convert|converted|signup rate|purchase rate|trial start)\b", re.I),
    "activation": re.compile(r"\b(?:activation|activated|aha moment|onboarding completion|first value)\b", re.I),
    "retention": re.compile(r"\b(?:retention|retained|cohort|churn|repeat usage|returning user)\b", re.I),
    "attribution": re.compile(
        r"\b(?:attribution|utm|campaign|referrer|referral|source medium|marketing source|"
        r"ad click|channel attribution)\b",
        re.I,
    ),
    "analytics_integration": re.compile(
        r"\b(?:segment|amplitude|google analytics|ga4|gtag|mixpanel|heap|rudderstack|"
        r"snowplow|posthog|pendo|analytics sdk|data layer|gtm|google tag manager)\b",
        re.I,
    ),
    "telemetry_schema": re.compile(
        r"\b(?:event schema|tracking plan|telemetry schema|analytics schema|event contract|"
        r"schema registry|property schema|event taxonomy)\b",
        re.I,
    ),
}
_DEFINITION_PATTERNS: dict[str, re.Pattern[str]] = {
    "event_name": re.compile(r"\b(?:event name|event names|naming convention|taxonomy|tracking plan|event contract)\b", re.I),
    "properties": re.compile(r"\b(?:properties|props|payload|attributes|parameters|dimensions|traits)\b", re.I),
    "identity": re.compile(r"\b(?:user id|anonymous id|account id|workspace id|identify|identity|group id)\b", re.I),
    "schema": re.compile(r"\b(?:schema|contract|registry|typed event|validation fixture|schema test)\b", re.I),
    "dashboard": re.compile(r"\b(?:dashboard|report|chart|looker|mode|metabase|explore)\b", re.I),
    "experiment": re.compile(r"\b(?:hypothesis|variant|control|treatment|assignment|exposure|sample size)\b", re.I),
    "privacy": re.compile(r"\b(?:privacy|pii|personal data|consent|gdpr|ccpa|hipaa|redact|hash|retention)\b", re.I),
    "backfill": re.compile(r"\b(?:backfill|historical|replay|warehouse migration|retroactive|reprocess)\b", re.I),
}


@dataclass(frozen=True, slots=True)
class TaskAnalyticsInstrumentationFinding:
    """Analytics instrumentation readiness guidance for one execution task."""

    task_id: str
    title: str
    readiness: AnalyticsInstrumentationReadiness
    instrumentation_signals: tuple[AnalyticsInstrumentationSignal, ...]
    required_event_definitions: tuple[str, ...]
    validation_recommendations: tuple[str, ...]
    privacy_cues: tuple[str, ...]
    dashboard_updates: tuple[str, ...]
    backfill_considerations: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "readiness": self.readiness,
            "instrumentation_signals": list(self.instrumentation_signals),
            "required_event_definitions": list(self.required_event_definitions),
            "validation_recommendations": list(self.validation_recommendations),
            "privacy_cues": list(self.privacy_cues),
            "dashboard_updates": list(self.dashboard_updates),
            "backfill_considerations": list(self.backfill_considerations),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskAnalyticsInstrumentationPlan:
    """Plan-level product analytics instrumentation review."""

    plan_id: str | None = None
    findings: tuple[TaskAnalyticsInstrumentationFinding, ...] = field(default_factory=tuple)
    instrumented_task_ids: tuple[str, ...] = field(default_factory=tuple)
    non_instrumentation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "instrumented_task_ids": list(self.instrumented_task_ids),
            "non_instrumentation_task_ids": list(self.non_instrumentation_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return analytics instrumentation findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    @property
    def records(self) -> tuple[TaskAnalyticsInstrumentationFinding, ...]:
        """Compatibility view matching other task-level planners."""
        return self.findings

    def to_markdown(self) -> str:
        """Render the analytics instrumentation plan as deterministic Markdown."""
        title = "# Task Analytics Instrumentation Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('instrumented_task_count', 0)} instrumentation tasks "
                f"(ready: {counts.get('ready', 0)}, partial: {counts.get('partial', 0)}, "
                f"missing: {counts.get('missing', 0)}, not_applicable: {counts.get('not_applicable', 0)})."
            ),
        ]
        if not self.findings:
            lines.extend(["", "No analytics instrumentation findings were inferred."])
            if self.non_instrumentation_task_ids:
                lines.extend(
                    ["", f"Non-instrumentation tasks: {_markdown_cell(', '.join(self.non_instrumentation_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Readiness | Signals | Required Event Definitions | Validation | Privacy | Dashboards | Backfill | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for finding in self.findings:
            lines.append(
                "| "
                f"`{_markdown_cell(finding.task_id)}` {_markdown_cell(finding.title)} | "
                f"{finding.readiness} | "
                f"{_markdown_cell(', '.join(finding.instrumentation_signals))} | "
                f"{_markdown_cell('; '.join(finding.required_event_definitions) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.validation_recommendations) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.privacy_cues) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.dashboard_updates) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.backfill_considerations) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.evidence) or 'none')} |"
            )
        if self.non_instrumentation_task_ids:
            lines.extend(["", f"Non-instrumentation tasks: {_markdown_cell(', '.join(self.non_instrumentation_task_ids))}"])
        return "\n".join(lines)


def build_task_analytics_instrumentation_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAnalyticsInstrumentationPlan:
    """Build product analytics instrumentation readiness guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_finding(task, index) for index, task in enumerate(tasks, start=1)]
    findings = tuple(
        sorted(
            (finding for finding in candidates if finding is not None),
            key=lambda finding: (
                _READINESS_ORDER[finding.readiness],
                finding.task_id,
                finding.title.casefold(),
            ),
        )
    )
    non_instrumentation_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    readiness_counts = {
        readiness: (
            len(non_instrumentation_task_ids)
            if readiness == "not_applicable"
            else sum(1 for finding in findings if finding.readiness == readiness)
        )
        for readiness in _READINESS_ORDER
    }
    signal_counts = {
        signal: sum(1 for finding in findings if signal in finding.instrumentation_signals)
        for signal in _SIGNAL_ORDER
    }
    return TaskAnalyticsInstrumentationPlan(
        plan_id=plan_id,
        findings=findings,
        instrumented_task_ids=tuple(finding.task_id for finding in findings),
        non_instrumentation_task_ids=non_instrumentation_task_ids,
        summary={
            "task_count": len(tasks),
            "instrumented_task_count": len(findings),
            "non_instrumentation_task_count": len(non_instrumentation_task_ids),
            "readiness_counts": readiness_counts,
            "signal_counts": signal_counts,
        },
    )


def summarize_task_analytics_instrumentation(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAnalyticsInstrumentationPlan:
    """Compatibility alias for building analytics instrumentation plans."""
    return build_task_analytics_instrumentation_plan(source)


def task_analytics_instrumentation_plan_to_dict(
    result: TaskAnalyticsInstrumentationPlan,
) -> dict[str, Any]:
    """Serialize an analytics instrumentation plan to a plain dictionary."""
    return result.to_dict()


task_analytics_instrumentation_plan_to_dict.__test__ = False


def task_analytics_instrumentation_plan_to_markdown(
    result: TaskAnalyticsInstrumentationPlan,
) -> str:
    """Render an analytics instrumentation plan as Markdown."""
    return result.to_markdown()


task_analytics_instrumentation_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    instrumentation: tuple[AnalyticsInstrumentationSignal, ...] = field(default_factory=tuple)
    definitions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    validation_commands: tuple[str, ...] = field(default_factory=tuple)


def _task_finding(task: Mapping[str, Any], index: int) -> TaskAnalyticsInstrumentationFinding | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals.instrumentation:
        return None
    readiness = _readiness(signals)
    return TaskAnalyticsInstrumentationFinding(
        task_id=task_id,
        title=title,
        readiness=readiness,
        instrumentation_signals=signals.instrumentation,
        required_event_definitions=_required_event_definitions(signals),
        validation_recommendations=_validation_recommendations(signals),
        privacy_cues=_privacy_cues(signals),
        dashboard_updates=_dashboard_updates(signals),
        backfill_considerations=_backfill_considerations(signals),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    instrumentation: set[AnalyticsInstrumentationSignal] = set()
    definitions: set[str] = set()
    evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_signals, path_definitions = _path_signals(normalized)
        if path_signals or path_definitions:
            instrumentation.update(path_signals)
            definitions.update(path_definitions)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text):
                instrumentation.add(signal)
                matched = True
        for signal, pattern in _DEFINITION_PATTERNS.items():
            if pattern.search(text):
                definitions.add(signal)
                matched = True
        if matched:
            evidence.append(snippet)

    validation_commands = tuple(_validation_commands(task))
    for command in validation_commands:
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(command) or pattern.search(command_text):
                instrumentation.add(signal)
                matched = True
        for signal, pattern in _DEFINITION_PATTERNS.items():
            if pattern.search(command) or pattern.search(command_text):
                definitions.add(signal)
                matched = True
        if matched:
            evidence.append(snippet)

    return _Signals(
        instrumentation=tuple(signal for signal in _SIGNAL_ORDER if signal in instrumentation),
        definitions=tuple(key for key in _DEFINITION_PATTERNS if key in definitions),
        evidence=tuple(_dedupe(evidence)),
        validation_commands=validation_commands,
    )


def _path_signals(path: str) -> tuple[set[AnalyticsInstrumentationSignal], set[str]]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    instrumentation: set[AnalyticsInstrumentationSignal] = set()
    definitions: set[str] = set()
    if bool({"tracking", "events", "eventing", "instrumentation", "telemetry"} & parts):
        instrumentation.add("event_tracking")
    if "funnel" in text:
        instrumentation.add("funnel")
    if bool({"metrics", "kpis", "measurements"} & parts) or "metric" in name:
        instrumentation.add("metrics")
    if bool({"dashboard", "dashboards", "reports", "reporting"} & parts):
        instrumentation.add("dashboard")
        definitions.add("dashboard")
    if any(token in text for token in ("experiment", "ab test", "a b test", "split test", "variant")):
        instrumentation.add("experiment")
        definitions.add("experiment")
    if "conversion" in text:
        instrumentation.add("conversion")
    if "activation" in text:
        instrumentation.add("activation")
    if "retention" in text or "cohort" in text:
        instrumentation.add("retention")
    if "attribution" in text or "utm" in text or "campaign" in text:
        instrumentation.add("attribution")
    if any(token in text for token in ("segment", "amplitude", "ga4", "gtag", "mixpanel", "posthog", "rudderstack")):
        instrumentation.add("analytics_integration")
    if any(token in text for token in ("schema", "tracking plan", "taxonomy", "contract")):
        instrumentation.add("telemetry_schema")
        definitions.add("schema")
    return instrumentation, definitions


def _readiness(signals: _Signals) -> AnalyticsInstrumentationReadiness:
    definition_set = set(signals.definitions)
    if not signals.instrumentation:
        return "not_applicable"
    has_event_contract = {"event_name", "properties", "schema"} <= definition_set
    has_validation = bool(signals.validation_commands) or "schema" in definition_set
    has_operational_view = bool({"dashboard", "experiment"} & definition_set)
    if has_event_contract and has_validation and ("privacy" in definition_set or has_operational_view):
        return "ready"
    if definition_set or signals.validation_commands:
        return "partial"
    return "missing"


def _required_event_definitions(signals: _Signals) -> tuple[str, ...]:
    signal_set = set(signals.instrumentation)
    definition_set = set(signals.definitions)
    definitions: list[str] = []
    if "event_name" not in definition_set:
        definitions.append("Define canonical event names using a stable product analytics taxonomy.")
    if "properties" not in definition_set:
        definitions.append("Define required, optional, and nullable event properties with types and examples.")
    if "identity" not in definition_set:
        definitions.append("Define user, anonymous, account, and workspace identifiers used for joins.")
    if "telemetry_schema" in signal_set and "schema" not in definition_set:
        definitions.append("Publish telemetry schema contracts and owner-approved schema change rules.")
    if "experiment" in signal_set:
        definitions.append("Define exposure, assignment, variant, and conversion events for experiment analysis.")
    if "funnel" in signal_set:
        definitions.append("Define ordered funnel step events and step completion criteria.")
    if {"conversion", "activation", "retention"} & signal_set:
        definitions.append("Define success, activation, retention, and cohort membership events.")
    if "attribution" in signal_set:
        definitions.append("Define attribution source, campaign, referrer, and UTM property handling.")
    return tuple(_dedupe(definitions))


def _validation_recommendations(signals: _Signals) -> tuple[str, ...]:
    signal_set = set(signals.instrumentation)
    recommendations = [
        "Add fixtures or contract tests that assert emitted event names, required properties, and property types.",
        "Validate no event is emitted before consent, identity resolution, and required context are available.",
    ]
    if "analytics_integration" in signal_set:
        recommendations.append("Run provider-specific validation in Segment, Amplitude, GA4, or the configured analytics SDK.")
    if "experiment" in signal_set:
        recommendations.append("Validate exposure and conversion fixtures cover control and treatment variants.")
    if "funnel" in signal_set:
        recommendations.append("Validate funnel fixtures cover entry, completion, abandonment, and retry paths.")
    if signals.validation_commands:
        recommendations.append("Run the detected validation commands against analytics fixture and schema scenarios.")
    return tuple(_dedupe(recommendations))


def _privacy_cues(signals: _Signals) -> tuple[str, ...]:
    signal_set = set(signals.instrumentation)
    cues = [
        "Review event properties for PII, sensitive attributes, consent requirements, and retention policy fit.",
        "Prefer stable opaque identifiers over emails, names, raw addresses, or free-form user content.",
    ]
    if "attribution" in signal_set:
        cues.append("Confirm campaign, referrer, and UTM capture respects consent and regional privacy rules.")
    if "analytics_integration" in signal_set:
        cues.append("Confirm third-party analytics destinations, data residency, and deletion propagation requirements.")
    return tuple(_dedupe(cues))


def _dashboard_updates(signals: _Signals) -> tuple[str, ...]:
    signal_set = set(signals.instrumentation)
    updates: list[str] = []
    if {"dashboard", "metrics"} & signal_set:
        updates.append("Update product dashboards with the new metric definition, denominator, filters, and owner.")
    if "funnel" in signal_set:
        updates.append("Add or update funnel dashboard panels for each ordered step and drop-off rate.")
    if "experiment" in signal_set:
        updates.append("Add experiment readout panels for exposure counts, guardrails, and primary conversion.")
    if {"activation", "retention", "conversion"} & signal_set:
        updates.append("Add activation, retention, or conversion trend panels with cohort filters.")
    if not updates:
        updates.append("Document whether existing dashboards consume the new events or no dashboard update is required.")
    return tuple(_dedupe(updates))


def _backfill_considerations(signals: _Signals) -> tuple[str, ...]:
    signal_set = set(signals.instrumentation)
    considerations: list[str] = []
    if {"dashboard", "metrics", "funnel", "conversion", "activation", "retention"} & signal_set:
        considerations.append("Decide whether historical events need warehouse backfill or a launch-date annotation.")
    if "telemetry_schema" in signal_set:
        considerations.append("Document schema migration, compatibility, and replay behavior for historical analytics data.")
    if "attribution" in signal_set:
        considerations.append("Define how historical sessions without attribution properties appear in reports.")
    if not considerations:
        considerations.append("Document why no analytics backfill or historical comparison is needed.")
    return tuple(_dedupe(considerations))


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
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
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
        "test_commands",
        "validation_command",
        "validation_commands",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
    for field_name in ("depends_on", "files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes"):
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if _any_signal(key_text):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _any_signal(key_text):
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


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in (*_TEXT_SIGNAL_PATTERNS.values(), *_DEFINITION_PATTERNS.values()))


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


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
    "AnalyticsInstrumentationReadiness",
    "AnalyticsInstrumentationSignal",
    "TaskAnalyticsInstrumentationFinding",
    "TaskAnalyticsInstrumentationPlan",
    "build_task_analytics_instrumentation_plan",
    "summarize_task_analytics_instrumentation",
    "task_analytics_instrumentation_plan_to_dict",
    "task_analytics_instrumentation_plan_to_markdown",
]
