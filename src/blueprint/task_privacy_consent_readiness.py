"""Plan privacy consent implementation readiness work for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


PrivacyConsentSignal = Literal[
    "consent",
    "consent_capture",
    "consent_withdrawal",
    "consent_versioning",
    "preference_center",
    "jurisdiction_copy",
    "auditability",
    "downstream_propagation",
]
PrivacyConsentReadinessCategory = Literal[
    "consent_capture",
    "consent_withdrawal",
    "consent_versioning",
    "preference_center",
    "jurisdiction_copy",
    "auditability",
    "downstream_propagation",
]
PrivacyConsentReadinessLevel = Literal["needs_planning", "partial", "ready"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[PrivacyConsentReadinessLevel, int] = {
    "needs_planning": 0,
    "partial": 1,
    "ready": 2,
}
_SIGNAL_ORDER: tuple[PrivacyConsentSignal, ...] = (
    "consent",
    "consent_capture",
    "consent_withdrawal",
    "consent_versioning",
    "preference_center",
    "jurisdiction_copy",
    "auditability",
    "downstream_propagation",
)
_CATEGORY_ORDER: tuple[PrivacyConsentReadinessCategory, ...] = (
    "consent_capture",
    "consent_withdrawal",
    "consent_versioning",
    "preference_center",
    "jurisdiction_copy",
    "auditability",
    "downstream_propagation",
)
_SIGNAL_PATTERNS: dict[PrivacyConsentSignal, re.Pattern[str]] = {
    "consent": re.compile(
        r"\b(?:privacy consent|user consent|consent workflow|consent management|"
        r"marketing consent|tracking consent|cookie consent|gdpr consent|ccpa consent)\b",
        re.I,
    ),
    "consent_capture": re.compile(
        r"\b(?:capture|collect|record|obtain|request|prompt for|opt[- ]?in|checkbox|"
        r"consent banner|cookie banner|consent modal|consent form)\b.{0,100}\bconsent\b|"
        r"\bconsent\b.{0,100}\b(?:capture|collect|record|obtain|request|prompt|opt[- ]?in|checkbox|"
        r"banner|modal|form)\b",
        re.I,
    ),
    "consent_withdrawal": re.compile(
        r"\b(?:withdraw|revoke|opt[- ]?out|unsubscribe|remove|disable|turn off)\b.{0,100}\bconsent\b|"
        r"\bconsent\b.{0,100}\b(?:withdrawal|withdraw|revoke|revocation|opt[- ]?out|unsubscribe|remove|disable)\b",
        re.I,
    ),
    "consent_versioning": re.compile(
        r"\b(?:consent|privacy notice|policy|terms|copy)\b.{0,100}\b(?:version|versioning|revision|"
        r"effective date|policy version|notice version|copy version|snapshot)\b|"
        r"\b(?:version|versioning|revision|effective date|policy version|notice version|copy version|snapshot)\b"
        r".{0,100}\b(?:consent|privacy notice|policy|terms|copy)\b",
        re.I,
    ),
    "preference_center": re.compile(
        r"\b(?:preference center|privacy preferences|privacy settings|consent settings|"
        r"communication preferences|marketing preferences|cookie settings|manage preferences)\b",
        re.I,
    ),
    "jurisdiction_copy": re.compile(
        r"\b(?:jurisdiction|jurisdiction[- ]specific|region[- ]specific|locale[- ]specific|"
        r"gdpr|ccpa|cpra|lgpd|uk gdpr|eprivacy|eea|eu|california|privacy copy|legal copy)\b",
        re.I,
    ),
    "auditability": re.compile(
        r"\b(?:audit log|audit trail|auditability|auditable|consent event|consent history|"
        r"proof of consent|evidence of consent|who changed|timestamp|ip address|user agent)\b",
        re.I,
    ),
    "downstream_propagation": re.compile(
        r"\b(?:downstream|propagat(?:e|ion)|sync|webhook|event|processor|subprocessor|vendor|"
        r"third[- ]party|analytics|crm|email provider|data warehouse)\b.{0,100}\b(?:consent|preference|opt[- ]?out)\b|"
        r"\b(?:consent|preference|opt[- ]?out)\b.{0,100}\b(?:downstream|propagat(?:e|ion)|sync|webhook|event|"
        r"processor|subprocessor|vendor|third[- ]party|analytics|crm|email provider|data warehouse)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[PrivacyConsentSignal, re.Pattern[str]] = {
    "consent": re.compile(r"(?:consent|privacy[_-]?preferences|cookie[_-]?banner)", re.I),
    "consent_capture": re.compile(r"(?:consent[_-]?capture|opt[_-]?in|cookie[_-]?banner)", re.I),
    "consent_withdrawal": re.compile(r"(?:withdraw|revoke|opt[_-]?out|unsubscribe)", re.I),
    "consent_versioning": re.compile(r"(?:consent[_-]?version|policy[_-]?version|notice[_-]?version)", re.I),
    "preference_center": re.compile(r"(?:preference[_-]?center|privacy[_-]?settings|consent[_-]?settings)", re.I),
    "jurisdiction_copy": re.compile(r"(?:jurisdiction|gdpr|ccpa|legal[_-]?copy|privacy[_-]?copy)", re.I),
    "auditability": re.compile(r"(?:audit|history|event[_-]?log|consent[_-]?event)", re.I),
    "downstream_propagation": re.compile(r"(?:downstream|propagation|processor|vendor|webhook|sync)", re.I),
}
_NO_CONSENT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:consent|preference center|privacy preferences|cookie banner)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|requirements?)\b",
    re.I,
)
_GENERIC_PRIVACY_COPY_RE = re.compile(
    r"\b(?:privacy policy|privacy notice|privacy copy|legal copy|data protection)\b",
    re.I,
)
_ACTIONABLE_CONSENT_RE = re.compile(
    r"\b(?:capture|collect|record|obtain|withdraw|revoke|opt[- ]?in|opt[- ]?out|preference center|"
    r"privacy preferences|consent settings|version|audit|history|propagat|downstream|processor|"
    r"jurisdiction|gdpr|ccpa|cookie banner|marketing consent|tracking consent)\b",
    re.I,
)
_CATEGORY_GUIDANCE: dict[PrivacyConsentReadinessCategory, tuple[str, tuple[str, ...]]] = {
    "consent_capture": (
        "Implement consent capture at the point where the affected workflow asks for permission.",
        (
            "The UI records affirmative, granular consent with purpose, channel, and source surface.",
            "Consent capture rejects ambiguous defaults and preserves the pre-submit state for validation.",
            "Tests cover first-time capture, unchanged choices, and validation errors.",
        ),
    ),
    "consent_withdrawal": (
        "Implement withdrawal behavior for previously granted consent.",
        (
            "Users can revoke each consent purpose without contacting support.",
            "Withdrawal takes effect in the product state used by the affected workflow.",
            "Tests cover revoked consent blocking future use while preserving allowed historical records.",
        ),
    ),
    "consent_versioning": (
        "Version the consent text and policy basis used for each recorded choice.",
        (
            "Stored consent records include policy or notice version, effective date, purpose, and locale where available.",
            "A new policy version can require re-consent without overwriting prior evidence.",
            "Tests cover migration or display behavior for records captured under an older version.",
        ),
    ),
    "preference_center": (
        "Connect consent state to preference center behavior.",
        (
            "The preference center displays current consent state per purpose or channel.",
            "Saving preferences updates the same source of truth used by capture and withdrawal flows.",
            "Tests cover loading, saving, and conflicting preference changes.",
        ),
    ),
    "jurisdiction_copy": (
        "Add hooks for jurisdiction-specific consent copy and legal variants.",
        (
            "Consent copy can vary by jurisdiction, locale, or policy regime without code forks.",
            "The selected copy variant is recorded with the consent decision.",
            "Tests cover at least one default region and one regulated-region variant.",
        ),
    ),
    "auditability": (
        "Record auditable consent events for capture, updates, and withdrawal.",
        (
            "Audit events include actor, timestamp, purpose, previous value, new value, source surface, and version.",
            "Audit history is append-only or otherwise tamper-evident for compliance review.",
            "Tests verify audit records for capture, update, and withdrawal paths.",
        ),
    ),
    "downstream_propagation": (
        "Propagate consent changes to downstream systems that consume consent state.",
        (
            "Consent changes emit an event, webhook, or sync job with idempotency and retry behavior.",
            "Downstream processors receive withdrawal before any future disallowed processing.",
            "Tests cover successful propagation, retryable failure, and stale downstream state handling.",
        ),
    ),
}


@dataclass(frozen=True, slots=True)
class PrivacyConsentReadinessTask:
    """One generated implementation task for privacy consent readiness."""

    category: PrivacyConsentReadinessCategory
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
class TaskPrivacyConsentReadinessRecord:
    """Privacy consent readiness guidance for one execution task or requirement text."""

    task_id: str
    title: str
    detected_signals: tuple[PrivacyConsentSignal, ...]
    generated_tasks: tuple[PrivacyConsentReadinessTask, ...] = field(default_factory=tuple)
    missing_recommended_categories: tuple[PrivacyConsentReadinessCategory, ...] = field(default_factory=tuple)
    readiness: PrivacyConsentReadinessLevel = "needs_planning"
    suggested_validation_commands: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[PrivacyConsentSignal, ...]:
        """Compatibility view for planners that name detected signals matched signals."""
        return self.detected_signals

    @property
    def recommended_tasks(self) -> tuple[PrivacyConsentReadinessTask, ...]:
        """Compatibility view for generated readiness tasks."""
        return self.generated_tasks

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
            "generated_tasks": [task.to_dict() for task in self.generated_tasks],
            "missing_recommended_categories": list(self.missing_recommended_categories),
            "readiness": self.readiness,
            "suggested_validation_commands": list(self.suggested_validation_commands),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskPrivacyConsentReadinessPlan:
    """Plan-level privacy consent readiness tasks."""

    plan_id: str | None = None
    records: tuple[TaskPrivacyConsentReadinessRecord, ...] = field(default_factory=tuple)
    consent_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskPrivacyConsentReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskPrivacyConsentReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    @property
    def impacted_task_ids(self) -> tuple[str, ...]:
        """Compatibility view matching planners that expose impacted task ids."""
        return self.consent_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "consent_task_ids": list(self.consent_task_ids),
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return privacy consent readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render privacy consent readiness guidance as deterministic Markdown."""
        title = "# Task Privacy Consent Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        category_counts = self.summary.get("generated_task_category_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Consent task count: {self.summary.get('consent_task_count', 0)}",
            f"- Generated readiness task count: {self.summary.get('generated_task_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
            "- Generated task counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task privacy consent readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Generated Tasks | Missing Recommended Categories | Validation | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            generated = "; ".join(f"{task.category}: {task.title}" for task in record.generated_tasks)
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(generated or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_recommended_categories) or 'none')} | "
                f"{_markdown_cell('; '.join(record.suggested_validation_commands) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_privacy_consent_readiness_plan(source: Any) -> TaskPrivacyConsentReadinessPlan:
    """Build privacy consent readiness records for task-shaped or requirement-text input."""
    plan_id, tasks, plan_commands = _source_payload(source)
    candidates = [
        _task_record(task, index, plan_commands=plan_commands)
        for index, task in enumerate(tasks, start=1)
    ]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                -len(record.generated_tasks),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    consent_task_ids = tuple(record.task_id for record in records)
    impacted = set(consent_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted
    )
    return TaskPrivacyConsentReadinessPlan(
        plan_id=plan_id,
        records=records,
        consent_task_ids=consent_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_privacy_consent_readiness(source: Any) -> TaskPrivacyConsentReadinessPlan:
    """Compatibility alias for building privacy consent readiness plans."""
    return build_task_privacy_consent_readiness_plan(source)


def recommend_task_privacy_consent_readiness(source: Any) -> TaskPrivacyConsentReadinessPlan:
    """Compatibility alias for recommending privacy consent readiness tasks."""
    return build_task_privacy_consent_readiness_plan(source)


def summarize_task_privacy_consent_readiness(source: Any) -> TaskPrivacyConsentReadinessPlan:
    """Compatibility alias for summarizing privacy consent readiness plans."""
    if isinstance(source, TaskPrivacyConsentReadinessPlan):
        return source
    return build_task_privacy_consent_readiness_plan(source)


def generate_task_privacy_consent_readiness(source: Any) -> TaskPrivacyConsentReadinessPlan:
    """Compatibility alias for generating privacy consent readiness plans."""
    return build_task_privacy_consent_readiness_plan(source)


def extract_task_privacy_consent_readiness(source: Any) -> TaskPrivacyConsentReadinessPlan:
    """Compatibility alias for extracting privacy consent readiness plans."""
    return build_task_privacy_consent_readiness_plan(source)


def derive_task_privacy_consent_readiness(source: Any) -> TaskPrivacyConsentReadinessPlan:
    """Compatibility alias for deriving privacy consent readiness plans."""
    return build_task_privacy_consent_readiness_plan(source)


def task_privacy_consent_readiness_plan_to_dict(
    result: TaskPrivacyConsentReadinessPlan,
) -> dict[str, Any]:
    """Serialize a privacy consent readiness plan to a plain dictionary."""
    return result.to_dict()


task_privacy_consent_readiness_plan_to_dict.__test__ = False


def task_privacy_consent_readiness_plan_to_dicts(
    result: TaskPrivacyConsentReadinessPlan | Iterable[TaskPrivacyConsentReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize privacy consent readiness records to plain dictionaries."""
    if isinstance(result, TaskPrivacyConsentReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_privacy_consent_readiness_plan_to_dicts.__test__ = False
task_privacy_consent_readiness_to_dicts = task_privacy_consent_readiness_plan_to_dicts
task_privacy_consent_readiness_to_dicts.__test__ = False


def task_privacy_consent_readiness_plan_to_markdown(
    result: TaskPrivacyConsentReadinessPlan,
) -> str:
    """Render a privacy consent readiness plan as Markdown."""
    return result.to_markdown()


task_privacy_consent_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[PrivacyConsentSignal, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _task_record(
    task: Mapping[str, Any],
    index: int,
    *,
    plan_commands: tuple[str, ...],
) -> TaskPrivacyConsentReadinessRecord | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None

    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    categories = _categories(signals.signals)
    generated_tasks = _generated_tasks(title, signals, categories)
    missing = _missing_recommended_categories(signals.signals, categories)
    commands = tuple(_dedupe([*_validation_commands(task), *plan_commands]))
    return TaskPrivacyConsentReadinessRecord(
        task_id=task_id,
        title=title,
        detected_signals=signals.signals,
        generated_tasks=generated_tasks,
        missing_recommended_categories=missing,
        readiness=_readiness(categories, missing),
        suggested_validation_commands=commands,
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[PrivacyConsentSignal] = set()
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
        if _NO_CONSENT_RE.search(text):
            explicitly_no_impact = True
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    if signal_hits & set(_SIGNAL_ORDER[1:]):
        signal_hits.add("consent")
    if signal_hits == {"consent"} and not _has_actionable_consent_context(task):
        signal_hits.clear()
        evidence.clear()
    if _has_only_generic_privacy_copy(task) and signal_hits <= {"consent", "jurisdiction_copy"}:
        signal_hits.clear()
        evidence.clear()

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _categories(
    signals: tuple[PrivacyConsentSignal, ...],
) -> tuple[PrivacyConsentReadinessCategory, ...]:
    categories: set[PrivacyConsentReadinessCategory] = {
        signal for signal in signals if signal != "consent"
    }
    if "consent_capture" in signals:
        categories.update({"consent_versioning", "auditability"})
    if "consent_withdrawal" in signals:
        categories.add("auditability")
    if "preference_center" in signals:
        categories.update({"consent_withdrawal", "auditability"})
    return tuple(category for category in _CATEGORY_ORDER if category in categories)


def _missing_recommended_categories(
    signals: tuple[PrivacyConsentSignal, ...],
    categories: tuple[PrivacyConsentReadinessCategory, ...],
) -> tuple[PrivacyConsentReadinessCategory, ...]:
    category_set = set(categories)
    expected: set[PrivacyConsentReadinessCategory] = set()
    if {"consent_capture", "downstream_propagation"} & set(signals):
        expected.update(
            {
                "consent_capture",
                "consent_withdrawal",
                "consent_versioning",
                "preference_center",
                "auditability",
                "downstream_propagation",
            }
        )
    if "jurisdiction_copy" in signals:
        expected.add("jurisdiction_copy")
    return tuple(category for category in _CATEGORY_ORDER if category in expected - category_set)


def _generated_tasks(
    source_title: str,
    signals: _Signals,
    categories: tuple[PrivacyConsentReadinessCategory, ...],
) -> tuple[PrivacyConsentReadinessTask, ...]:
    evidence = tuple(sorted(signals.evidence, key=_evidence_priority))[:3]
    rationale = "; ".join(evidence) if evidence else "Privacy consent task context was detected."
    tasks: list[PrivacyConsentReadinessTask] = []
    for category in _CATEGORY_ORDER:
        if category not in categories:
            continue
        guidance, acceptance = _CATEGORY_GUIDANCE[category]
        tasks.append(
            PrivacyConsentReadinessTask(
                category=category,
                title=f"{_category_title(category)} for {source_title}",
                description=f"{guidance} Rationale: {rationale}",
                acceptance_criteria=acceptance,
                evidence=evidence,
            )
        )
    return tuple(tasks)


def _readiness(
    categories: tuple[PrivacyConsentReadinessCategory, ...],
    missing: tuple[PrivacyConsentReadinessCategory, ...],
) -> PrivacyConsentReadinessLevel:
    if missing:
        return "partial"
    if {"auditability", "consent_versioning"} <= set(categories):
        return "ready"
    return "needs_planning"


def _summary(
    records: tuple[TaskPrivacyConsentReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    generated_tasks = [task for record in records for task in record.generated_tasks]
    missing = [
        category for record in records for category in record.missing_recommended_categories
    ]
    return {
        "task_count": task_count,
        "consent_task_count": len(records),
        "consent_task_ids": [record.task_id for record in records],
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
        "generated_task_count": len(generated_tasks),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "generated_task_category_counts": {
            category: sum(1 for task in generated_tasks if task.category == category)
            for category in _CATEGORY_ORDER
        },
        "missing_recommended_category_counts": {
            category: sum(1 for item in missing if item == category)
            for category in _CATEGORY_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]], tuple[str, ...]]:
    if isinstance(source, str):
        text = _optional_text(source)
        if not text:
            return None, [], ()
        return None, [{"id": "requirement-text", "title": "Privacy consent requirements", "description": text}], ()
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
        return {"id": "requirement-text", "title": "Privacy consent requirements", "description": text} if text else {}
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


def _has_actionable_consent_context(task: Mapping[str, Any]) -> bool:
    return any(_ACTIONABLE_CONSENT_RE.search(text) for _, text in _candidate_texts(task))


def _has_only_generic_privacy_copy(task: Mapping[str, Any]) -> bool:
    texts = [text for _, text in _candidate_texts(task)]
    if not texts:
        return False
    context = " ".join(texts)
    return bool(_GENERIC_PRIVACY_COPY_RE.search(context)) and not bool(_ACTIONABLE_CONSENT_RE.search(context))


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
    "PrivacyConsentReadinessCategory",
    "PrivacyConsentReadinessLevel",
    "PrivacyConsentReadinessSignal",
    "PrivacyConsentReadinessTask",
    "PrivacyConsentSignal",
    "TaskPrivacyConsentReadinessPlan",
    "TaskPrivacyConsentReadinessRecord",
    "analyze_task_privacy_consent_readiness",
    "build_task_privacy_consent_readiness_plan",
    "derive_task_privacy_consent_readiness",
    "extract_task_privacy_consent_readiness",
    "generate_task_privacy_consent_readiness",
    "recommend_task_privacy_consent_readiness",
    "summarize_task_privacy_consent_readiness",
    "task_privacy_consent_readiness_plan_to_dict",
    "task_privacy_consent_readiness_plan_to_dicts",
    "task_privacy_consent_readiness_plan_to_markdown",
    "task_privacy_consent_readiness_to_dicts",
]

PrivacyConsentReadinessSignal = PrivacyConsentSignal
