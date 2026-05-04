"""Assess execution-plan tasks for data privacy impact."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


PrivacyDataCategory = Literal[
    "pii",
    "phi",
    "credentials",
    "logs",
    "analytics",
    "user_profiles",
    "exports",
    "retention",
    "deletion",
    "consent",
    "third_party_sharing",
]
PrivacyImpactLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_PII_RE = re.compile(
    r"\b(?:pii|personal data|personally identifiable|email addresses?|phone numbers?|"
    r"mailing addresses?|ssn|social security|date of birth|birthdate|full name|"
    r"customer data|user data)\b",
    re.IGNORECASE,
)
_PHI_RE = re.compile(
    r"\b(?:phi|protected health|hipaa|health data|medical records?|patient|diagnosis|"
    r"clinical|prescription)\b",
    re.IGNORECASE,
)
_CREDENTIALS_RE = re.compile(
    r"\b(?:credential|credentials|password|passcode|token|api key|apikey|secret|"
    r"oauth|session cookie|refresh token|access token|private key)\b",
    re.IGNORECASE,
)
_LOGS_RE = re.compile(
    r"\b(?:log|logs|logging|audit trail|audit event|trace|traces|telemetry|"
    r"observability|error report|crash report)\b",
    re.IGNORECASE,
)
_ANALYTICS_RE = re.compile(
    r"\b(?:analytics|tracking|track event|tracked event|funnel|attribution|"
    r"behavioral data|usage metrics|product metrics)\b",
    re.IGNORECASE,
)
_USER_PROFILES_RE = re.compile(
    r"\b(?:user profile|user profiles|profile field|profile fields|preferences|"
    r"account settings|demographic|identity profile)\b",
    re.IGNORECASE,
)
_EXPORTS_RE = re.compile(
    r"\b(?:export|exports|data export|csv export|download personal data|"
    r"download user data|report download|portable data|data portability)\b",
    re.IGNORECASE,
)
_RETENTION_RE = re.compile(
    r"\b(?:retention|retain|ttl|time to live|archive|archival|purge schedule|"
    r"data lifecycle|expiration|expire)\b",
    re.IGNORECASE,
)
_DELETION_RE = re.compile(
    r"\b(?:delete|deletion|erase|erasure|right to be forgotten|account removal|"
    r"data removal|purge|tombstone|hard delete|soft delete)\b",
    re.IGNORECASE,
)
_CONSENT_RE = re.compile(
    r"\b(?:consent|opt in|opt-in|opt out|opt-out|unsubscribe|preference center|"
    r"privacy choice|marketing permission)\b",
    re.IGNORECASE,
)
_THIRD_PARTY_RE = re.compile(
    r"\b(?:third party|third-party|vendor|processor|subprocessor|external api|"
    r"webhook|data sharing|share with|partner integration|send to partner)\b",
    re.IGNORECASE,
)
_PRIVACY_ACCEPTANCE_RE = re.compile(
    r"\b(?:privacy|data protection|gdpr|ccpa|hipaa|consent|retention|deletion|"
    r"redaction|encryption|data minimization|privacy review)\b",
    re.IGNORECASE,
)
_CATEGORY_ORDER: dict[PrivacyDataCategory, int] = {
    "pii": 0,
    "phi": 1,
    "credentials": 2,
    "logs": 3,
    "analytics": 4,
    "user_profiles": 5,
    "exports": 6,
    "retention": 7,
    "deletion": 8,
    "consent": 9,
    "third_party_sharing": 10,
}
_IMPACT_ORDER: dict[PrivacyImpactLevel, int] = {"high": 0, "medium": 1, "low": 2}


@dataclass(frozen=True, slots=True)
class TaskDataPrivacyImpact:
    """Privacy impact assessment for one execution task."""

    task_id: str
    title: str
    impact_level: PrivacyImpactLevel
    data_categories: tuple[PrivacyDataCategory, ...] = field(default_factory=tuple)
    required_safeguards: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)
    rationale: str = "No privacy-sensitive data handling signals detected."
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "impact_level": self.impact_level,
            "data_categories": list(self.data_categories),
            "required_safeguards": list(self.required_safeguards),
            "follow_up_questions": list(self.follow_up_questions),
            "rationale": self.rationale,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDataPrivacyImpactPlan:
    """Privacy impact assessments for a plan or task collection."""

    plan_id: str | None = None
    task_impacts: tuple[TaskDataPrivacyImpact, ...] = field(default_factory=tuple)
    privacy_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "task_impacts": [impact.to_dict() for impact in self.task_impacts],
            "privacy_task_ids": list(self.privacy_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return task impact records as plain dictionaries."""
        return [impact.to_dict() for impact in self.task_impacts]

    def to_markdown(self) -> str:
        """Render privacy impact assessments as deterministic Markdown."""
        title = "# Task Data Privacy Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.task_impacts:
            lines.extend(["", "No tasks were available for privacy impact assessment."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Impact | Data Categories | Required Safeguards | Follow-up Questions |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for impact in self.task_impacts:
            lines.append(
                "| "
                f"`{_markdown_cell(impact.task_id)}` | "
                f"{impact.impact_level} | "
                f"{_markdown_cell(', '.join(impact.data_categories) or 'none')} | "
                f"{_markdown_cell('; '.join(impact.required_safeguards) or impact.rationale)} | "
                f"{_markdown_cell('; '.join(impact.follow_up_questions) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_data_privacy_impact_plan(source: Any) -> TaskDataPrivacyImpactPlan:
    """Assess tasks for privacy-sensitive data handling and required safeguards."""
    plan_id, tasks = _source_payload(source)
    impacts = tuple(
        _impact(task, index)
        for index, task in enumerate(tasks, start=1)
    )
    privacy_task_ids = tuple(
        impact.task_id for impact in impacts if impact.impact_level != "low"
    )
    no_impact_task_ids = tuple(
        impact.task_id for impact in impacts if impact.impact_level == "low"
    )
    level_counts = {
        level: sum(1 for impact in impacts if impact.impact_level == level)
        for level in _IMPACT_ORDER
    }
    category_counts = {
        category: sum(1 for impact in impacts if category in impact.data_categories)
        for category in _CATEGORY_ORDER
    }

    return TaskDataPrivacyImpactPlan(
        plan_id=plan_id,
        task_impacts=impacts,
        privacy_task_ids=privacy_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary={
            "task_count": len(tasks),
            "privacy_task_count": len(privacy_task_ids),
            "no_impact_task_count": len(no_impact_task_ids),
            "high_impact_count": level_counts["high"],
            "medium_impact_count": level_counts["medium"],
            "low_impact_count": level_counts["low"],
            "category_counts": category_counts,
        },
    )


def task_data_privacy_impact_plan_to_dict(
    result: TaskDataPrivacyImpactPlan,
) -> dict[str, Any]:
    """Serialize a data privacy impact plan to a plain dictionary."""
    return result.to_dict()


task_data_privacy_impact_plan_to_dict.__test__ = False


def task_data_privacy_impact_plan_to_markdown(
    result: TaskDataPrivacyImpactPlan,
) -> str:
    """Render a data privacy impact plan as Markdown."""
    return result.to_markdown()


task_data_privacy_impact_plan_to_markdown.__test__ = False


def recommend_task_data_privacy_impacts(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
) -> TaskDataPrivacyImpactPlan:
    """Compatibility alias for building task data privacy impact assessments."""
    return build_task_data_privacy_impact_plan(source)


def _impact(task: Mapping[str, Any], index: int) -> TaskDataPrivacyImpact:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals:
        return TaskDataPrivacyImpact(
            task_id=task_id,
            title=title,
            impact_level="low",
        )

    categories = tuple(sorted(signals, key=lambda category: _CATEGORY_ORDER[category]))
    impact_level = _impact_level(categories)
    evidence = tuple(_dedupe(item for category in categories for item in signals[category]))
    safeguards = tuple(_required_safeguards(categories, task))
    questions = tuple(_follow_up_questions(categories, task))

    return TaskDataPrivacyImpact(
        task_id=task_id,
        title=title,
        impact_level=impact_level,
        data_categories=categories,
        required_safeguards=safeguards,
        follow_up_questions=questions,
        rationale=_rationale(categories, impact_level),
        evidence=evidence,
    )


def _signals(task: Mapping[str, Any]) -> dict[PrivacyDataCategory, tuple[str, ...]]:
    signals: dict[PrivacyDataCategory, list[str]] = {}

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
    signals: dict[PrivacyDataCategory, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    evidence = f"files_or_modules: {original}"

    if bool({"users", "customers", "contacts", "identity", "pii"} & parts):
        _append(signals, "pii", evidence)
    if bool({"health", "medical", "patients", "hipaa", "phi"} & parts):
        _append(signals, "phi", evidence)
    if bool({"auth", "oauth", "secrets", "credentials", "sessions"} & parts) or any(
        token in name for token in ("password", "token", "secret", "credential")
    ):
        _append(signals, "credentials", evidence)
    if bool({"logs", "logging", "audit", "telemetry", "traces"} & parts) or "log" in name:
        _append(signals, "logs", evidence)
    if bool({"analytics", "metrics", "tracking", "events"} & parts):
        _append(signals, "analytics", evidence)
    if bool({"profiles", "profile", "preferences", "accounts"} & parts):
        _append(signals, "user_profiles", evidence)
    if bool({"exports", "exporters", "reports", "downloads"} & parts) or "export" in name:
        _append(signals, "exports", evidence)
    if bool({"retention", "archive", "archives", "lifecycle"} & parts):
        _append(signals, "retention", evidence)
    if bool({"deletion", "delete", "purge", "erasure"} & parts) or any(
        token in name for token in ("delete", "deletion", "purge", "erasure")
    ):
        _append(signals, "deletion", evidence)
    if bool({"consent", "preferences"} & parts):
        _append(signals, "consent", evidence)
    if bool({"vendors", "partners", "webhooks", "integrations", "processors"} & parts):
        _append(signals, "third_party_sharing", evidence)


def _add_text_signals(
    signals: dict[PrivacyDataCategory, list[str]],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    if _PII_RE.search(text):
        _append(signals, "pii", evidence)
    if _PHI_RE.search(text):
        _append(signals, "phi", evidence)
    if _CREDENTIALS_RE.search(text):
        _append(signals, "credentials", evidence)
    if _LOGS_RE.search(text):
        _append(signals, "logs", evidence)
    if _ANALYTICS_RE.search(text):
        _append(signals, "analytics", evidence)
    if _USER_PROFILES_RE.search(text):
        _append(signals, "user_profiles", evidence)
    if _EXPORTS_RE.search(text):
        _append(signals, "exports", evidence)
    if _RETENTION_RE.search(text):
        _append(signals, "retention", evidence)
    if _DELETION_RE.search(text):
        _append(signals, "deletion", evidence)
    if _CONSENT_RE.search(text):
        _append(signals, "consent", evidence)
    if _THIRD_PARTY_RE.search(text):
        _append(signals, "third_party_sharing", evidence)


def _impact_level(categories: tuple[PrivacyDataCategory, ...]) -> PrivacyImpactLevel:
    if "phi" in categories or "credentials" in categories:
        return "high"
    if "third_party_sharing" in categories and bool({"pii", "user_profiles"} & set(categories)):
        return "high"
    if "exports" in categories and bool({"pii", "phi", "user_profiles"} & set(categories)):
        return "high"
    if "deletion" in categories and bool({"pii", "phi", "user_profiles"} & set(categories)):
        return "high"
    return "medium"


def _required_safeguards(
    categories: tuple[PrivacyDataCategory, ...],
    task: Mapping[str, Any],
) -> list[str]:
    safeguards: list[str] = []
    for category in categories:
        safeguards.extend(_safeguards_for_category(category))
    if not _PRIVACY_ACCEPTANCE_RE.search(" ".join(_strings(task.get("acceptance_criteria")))):
        safeguards.append("Add acceptance criteria covering privacy review outcomes.")
    return _dedupe(safeguards)


def _safeguards_for_category(category: PrivacyDataCategory) -> tuple[str, ...]:
    return {
        "pii": (
            "Classify PII fields and apply data minimization before implementation.",
            "Mask or redact PII in logs, fixtures, and support-visible surfaces.",
        ),
        "phi": (
            "Confirm HIPAA or health-data handling requirements with the privacy owner.",
            "Restrict PHI access paths and document audit logging expectations.",
        ),
        "credentials": (
            "Store credentials only in approved secret storage with rotation support.",
            "Prevent credentials from appearing in logs, analytics, exports, or error payloads.",
        ),
        "logs": (
            "Redact sensitive fields before writing logs, traces, or audit events.",
            "Define log retention and access controls for privacy-sensitive entries.",
        ),
        "analytics": (
            "Limit analytics payloads to necessary event fields and avoid direct identifiers.",
            "Confirm tracking consent requirements before emitting user-level events.",
        ),
        "user_profiles": (
            "Document profile fields, purpose, and update visibility for users or operators.",
            "Ensure profile changes respect existing access controls and consent choices.",
        ),
        "exports": (
            "Gate exports with authorization checks and include only intended data fields.",
            "Record export access in an audit trail and avoid leaking deleted or hidden data.",
        ),
        "retention": (
            "Define retention period, purge schedule, and archival behavior.",
            "Verify retention changes are reversible or have an approved migration plan.",
        ),
        "deletion": (
            "Define deletion semantics across primary storage, caches, logs, and exports.",
            "Add verification that deleted data is not restored by background syncs.",
        ),
        "consent": (
            "Honor consent state before collection, processing, tracking, or sharing.",
            "Document fallback behavior when consent is missing, withdrawn, or stale.",
        ),
        "third_party_sharing": (
            "Confirm the third party is approved for the data categories being shared.",
            "Document outbound fields, purpose, retention, and failure handling.",
        ),
    }[category]


def _follow_up_questions(
    categories: tuple[PrivacyDataCategory, ...],
    task: Mapping[str, Any],
) -> list[str]:
    questions: list[str] = []
    for category in categories:
        questions.extend(_questions_for_category(category))
    context = _task_context(task)
    if not _PRIVACY_ACCEPTANCE_RE.search(context):
        questions.append("Who is responsible for signing off the privacy impact before assignment?")
    return _dedupe(questions)


def _questions_for_category(category: PrivacyDataCategory) -> tuple[str, ...]:
    return {
        "pii": ("Which exact personal data fields are collected, stored, displayed, or transformed?",),
        "phi": ("Does this task require HIPAA controls or other health-data compliance review?",),
        "credentials": ("How are secrets rotated, revoked, and excluded from observable output?",),
        "logs": ("Which sensitive fields must be redacted before logging or tracing?",),
        "analytics": ("Is user consent required before these analytics events are emitted?",),
        "user_profiles": ("Can users view, correct, export, or delete the affected profile fields?",),
        "exports": ("Who can run the export and what fields are included in the exported artifact?",),
        "retention": ("What retention period and purge trigger apply to each affected data store?",),
        "deletion": ("Which stores, caches, queues, logs, and third parties must receive deletion handling?",),
        "consent": ("What should happen when consent is absent, withdrawn, or changed?",),
        "third_party_sharing": ("What third party receives the data and under which processing agreement?",),
    }[category]


def _rationale(
    categories: tuple[PrivacyDataCategory, ...],
    impact_level: PrivacyImpactLevel,
) -> str:
    rendered = ", ".join(categories)
    if impact_level == "high":
        return f"Task touches high-risk privacy category or cross-boundary handling: {rendered}."
    return f"Task touches privacy-sensitive data handling category or workflow: {rendered}."


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
    if hasattr(source, "tasks"):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)
    except TypeError:
        task = _task_like_payload(source)
        return (None, [task]) if task else (None, [])

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_like_payload(item):
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
        if task := _task_like_payload(item):
            tasks.append(task)
    return tasks


def _task_like_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if hasattr(value, "dict"):
        task = value.dict()
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
        "risk_level",
        "test_command",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "tags",
        "labels",
        "metadata",
        "tasks",
    )
    payload = {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }
    return payload


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "test_command",
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
    signals: dict[PrivacyDataCategory, list[str]],
    category: PrivacyDataCategory,
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
    "PrivacyDataCategory",
    "PrivacyImpactLevel",
    "TaskDataPrivacyImpact",
    "TaskDataPrivacyImpactPlan",
    "build_task_data_privacy_impact_plan",
    "recommend_task_data_privacy_impacts",
    "task_data_privacy_impact_plan_to_dict",
    "task_data_privacy_impact_plan_to_markdown",
]
