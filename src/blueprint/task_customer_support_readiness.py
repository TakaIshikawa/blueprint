"""Plan customer support readiness for launch-impacting execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SupportReadinessCategory = Literal[
    "user_visible_behavior",
    "billing_account_flow",
    "data_migration",
    "rollout_risk",
    "documentation_change",
    "support_macro",
    "faq_help_center",
    "escalation_path",
    "known_customer_communication",
]
SupportReadinessSeverity = Literal["critical", "high", "medium"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CATEGORY_ORDER: dict[SupportReadinessCategory, int] = {
    "billing_account_flow": 0,
    "data_migration": 1,
    "rollout_risk": 2,
    "user_visible_behavior": 3,
    "documentation_change": 4,
    "support_macro": 5,
    "faq_help_center": 6,
    "escalation_path": 7,
    "known_customer_communication": 8,
}
_SEVERITY_ORDER: dict[SupportReadinessSeverity, int] = {
    "critical": 0,
    "high": 1,
    "medium": 2,
}

_USER_VISIBLE_RE = re.compile(
    r"\b(?:user[- ]facing|customer[- ]facing|customer[- ]visible|user[- ]visible|"
    r"ui|ux|frontend|screen|page|dashboard|form|copy|message|empty state|"
    r"workflow|checkout|signup|sign up|login|onboarding|notification|email)\b",
    re.IGNORECASE,
)
_BILLING_RE = re.compile(
    r"\b(?:billing|invoice|invoices|payment|payments|subscription|subscriptions|"
    r"plan change|account flow|account flows|seat|seats|trial|renewal|refund|charge|"
    r"checkout|stripe|tax|credits?)\b",
    re.IGNORECASE,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrations|migrate|backfill|data migration|database migration|"
    r"schema change|existing customers?|existing accounts?|cutover|import|export)\b",
    re.IGNORECASE,
)
_ROLLOUT_RE = re.compile(
    r"\b(?:rollout|roll out|launch|release|deploy|deployment|production|canary|"
    r"feature flag|flagged|gradual|rollback|roll back|kill switch|incident|"
    r"launch watch|watch window|known issue)\b",
    re.IGNORECASE,
)
_DOCS_RE = re.compile(
    r"\b(?:docs?|documentation|readme|release notes?|changelog|runbook|"
    r"support guide|operator guide|migration guide)\b",
    re.IGNORECASE,
)
_MACRO_RE = re.compile(
    r"\b(?:support macro|macro|canned response|support snippet|support script|"
    r"agent script|zendesk macro|intercom macro)\b",
    re.IGNORECASE,
)
_FAQ_RE = re.compile(
    r"\b(?:faq|frequently asked|help center|help article|knowledge base|kb article|"
    r"customer help|self[- ]serve help|troubleshooting article)\b",
    re.IGNORECASE,
)
_ESCALATION_RE = re.compile(
    r"\b(?:escalation|escalate|tier 2|tier two|tier 3|tier three|on-call|"
    r"pager|incident commander|support owner|engineering owner|triage owner)\b",
    re.IGNORECASE,
)
_CUSTOMER_COMM_RE = re.compile(
    r"\b(?:known customer|named customer|customer communication|customer comms|"
    r"announce|announcement|notify customers?|customer email|customer list|"
    r"account manager|csm|success manager|beta customer|early access)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class TaskCustomerSupportReadinessRecord:
    """Support enablement recommendation for one execution task and category."""

    task_id: str
    title: str
    readiness_category: SupportReadinessCategory
    severity: SupportReadinessSeverity
    suggested_artifacts: tuple[str, ...] = field(default_factory=tuple)
    rationale: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "readiness_category": self.readiness_category,
            "severity": self.severity,
            "suggested_artifacts": list(self.suggested_artifacts),
            "rationale": self.rationale,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskCustomerSupportReadinessPlan:
    """Task-level customer support readiness plan."""

    plan_id: str | None = None
    records: tuple[TaskCustomerSupportReadinessRecord, ...] = field(default_factory=tuple)
    support_ready_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_support_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "support_ready_task_ids": list(self.support_ready_task_ids),
            "no_support_task_ids": list(self.no_support_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return support readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render support readiness guidance as deterministic Markdown."""
        title = "# Task Customer Support Readiness Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No customer support readiness signals detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Category | Severity | Suggested Artifacts | Rationale | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{record.readiness_category} | "
                f"{record.severity} | "
                f"{_markdown_cell('; '.join(record.suggested_artifacts))} | "
                f"{_markdown_cell(record.rationale)} | "
                f"{_markdown_cell('; '.join(record.evidence))} |"
            )
        return "\n".join(lines)


def build_task_customer_support_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskCustomerSupportReadinessPlan:
    """Detect execution tasks that need customer support enablement."""
    plan_id, tasks = _source_payload(source)
    records = [
        record for index, task in enumerate(tasks, start=1) for record in _task_records(task, index)
    ]
    records.sort(
        key=lambda record: (
            _SEVERITY_ORDER[record.severity],
            _CATEGORY_ORDER[record.readiness_category],
            record.task_id,
            record.title.casefold(),
        )
    )
    result = tuple(records)
    support_ready_task_ids = tuple(_dedupe(record.task_id for record in result))
    all_task_ids = tuple(
        _optional_text(task.get("id")) or f"task-{index}"
        for index, task in enumerate(tasks, start=1)
    )
    category_counts = {
        category: sum(1 for record in result if record.readiness_category == category)
        for category in _CATEGORY_ORDER
    }
    severity_counts = {
        severity: sum(1 for record in result if record.severity == severity)
        for severity in _SEVERITY_ORDER
    }

    return TaskCustomerSupportReadinessPlan(
        plan_id=plan_id,
        records=result,
        support_ready_task_ids=support_ready_task_ids,
        no_support_task_ids=tuple(
            task_id for task_id in all_task_ids if task_id not in support_ready_task_ids
        ),
        summary={
            "task_count": len(tasks),
            "support_ready_task_count": len(support_ready_task_ids),
            "no_support_task_count": len(all_task_ids) - len(support_ready_task_ids),
            "record_count": len(result),
            "category_counts": category_counts,
            "severity_counts": severity_counts,
        },
    )


def derive_task_customer_support_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskCustomerSupportReadinessPlan:
    """Compatibility alias for building customer support readiness guidance."""
    return build_task_customer_support_readiness_plan(source)


def build_task_customer_support_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskCustomerSupportReadinessPlan:
    """Compatibility alias for building customer support readiness guidance."""
    return build_task_customer_support_readiness_plan(source)


def task_customer_support_readiness_plan_to_dict(
    plan: TaskCustomerSupportReadinessPlan,
) -> dict[str, Any]:
    """Serialize a customer support readiness plan to a plain dictionary."""
    return plan.to_dict()


task_customer_support_readiness_plan_to_dict.__test__ = False


def task_customer_support_readiness_plan_to_markdown(
    plan: TaskCustomerSupportReadinessPlan,
) -> str:
    """Render a customer support readiness plan as Markdown."""
    return plan.to_markdown()


task_customer_support_readiness_plan_to_markdown.__test__ = False

task_customer_support_readiness_to_dict = task_customer_support_readiness_plan_to_dict
task_customer_support_readiness_to_dict.__test__ = False
task_customer_support_readiness_to_markdown = task_customer_support_readiness_plan_to_markdown
task_customer_support_readiness_to_markdown.__test__ = False


def _task_records(
    task: Mapping[str, Any],
    index: int,
) -> tuple[TaskCustomerSupportReadinessRecord, ...]:
    signals = _signals(task)
    if not signals:
        return ()

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    categories = sorted(signals, key=lambda category: _CATEGORY_ORDER[category])

    return tuple(
        TaskCustomerSupportReadinessRecord(
            task_id=task_id,
            title=title,
            readiness_category=category,
            severity=_severity(category, signals[category], task),
            suggested_artifacts=_suggested_artifacts(category, task),
            rationale=_rationale(category),
            evidence=tuple(_dedupe(signals[category])),
        )
        for category in categories
    )


def _signals(
    task: Mapping[str, Any],
) -> dict[SupportReadinessCategory, tuple[str, ...]]:
    signals: dict[SupportReadinessCategory, list[str]] = {}
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _add_path_signals(signals, path)
    for source_field, text in _task_texts(task):
        _add_text_signals(signals, source_field, text)
    for source_field, text in _metadata_texts(task.get("metadata")):
        field = source_field.casefold()
        if any(
            token in field
            for token in ("support", "macro", "faq", "help", "escalation", "customer")
        ):
            (
                _append(signals, "support_macro", f"{source_field}: {text}")
                if "macro" in field
                else None
            )
            (
                _append(signals, "faq_help_center", f"{source_field}: {text}")
                if "faq" in field or "help" in field
                else None
            )
            (
                _append(signals, "escalation_path", f"{source_field}: {text}")
                if "escalation" in field
                else None
            )
            (
                _append(signals, "known_customer_communication", f"{source_field}: {text}")
                if "customer" in field
                else None
            )
        _add_text_signals(signals, source_field, text)
    return {
        category: tuple(_dedupe(evidence)) for category, evidence in signals.items() if evidence
    }


def _add_path_signals(
    signals: dict[SupportReadinessCategory, list[str]],
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

    if suffix in {".tsx", ".jsx", ".vue", ".svelte"} or bool(
        {"ui", "frontend", "components", "pages", "screens", "views", "templates"} & parts
    ):
        _append(signals, "user_visible_behavior", evidence)
    if bool({"billing", "payments", "checkout", "subscriptions", "accounts"} & parts) or any(
        token in name for token in ("billing", "invoice", "payment", "subscription", "account")
    ):
        _append(signals, "billing_account_flow", evidence)
    if suffix == ".sql" or bool({"migrations", "migration", "backfills", "alembic"} & parts):
        _append(signals, "data_migration", evidence)
    if bool(
        {"rollout", "deploy", "deployment", "flags", "feature_flags", "runbooks"} & parts
    ) or any(token in name for token in ("rollback", "rollout", "flag", "launch")):
        _append(signals, "rollout_risk", evidence)
    if bool({"docs", "documentation", "runbooks"} & parts) or name in {
        "readme.md",
        "changelog.md",
    }:
        _append(signals, "documentation_change", evidence)
    if bool({"macros", "support_macros", "zendesk"} & parts) or "macro" in name:
        _append(signals, "support_macro", evidence)
    if bool({"faq", "help", "help_center", "knowledge_base", "kb"} & parts) or any(
        name.startswith(token) for token in ("faq", "help", "troubleshooting")
    ):
        _append(signals, "faq_help_center", evidence)
    if bool({"escalations", "oncall", "incidents"} & parts) or any(
        token in name for token in ("escalation", "oncall", "pager")
    ):
        _append(signals, "escalation_path", evidence)
    if bool({"communications", "comms", "customers", "announcements"} & parts) or any(
        token in name for token in ("announcement", "customer_email", "customer_notice")
    ):
        _append(signals, "known_customer_communication", evidence)


def _add_text_signals(
    signals: dict[SupportReadinessCategory, list[str]],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    if _USER_VISIBLE_RE.search(text):
        _append(signals, "user_visible_behavior", evidence)
    if _BILLING_RE.search(text):
        _append(signals, "billing_account_flow", evidence)
    if _MIGRATION_RE.search(text):
        _append(signals, "data_migration", evidence)
    if _ROLLOUT_RE.search(text):
        _append(signals, "rollout_risk", evidence)
    if _DOCS_RE.search(text):
        _append(signals, "documentation_change", evidence)
    if _MACRO_RE.search(text):
        _append(signals, "support_macro", evidence)
    if _FAQ_RE.search(text):
        _append(signals, "faq_help_center", evidence)
    if _ESCALATION_RE.search(text):
        _append(signals, "escalation_path", evidence)
    if _CUSTOMER_COMM_RE.search(text):
        _append(signals, "known_customer_communication", evidence)


def _severity(
    category: SupportReadinessCategory,
    evidence: tuple[str, ...],
    task: Mapping[str, Any],
) -> SupportReadinessSeverity:
    context = f"{' '.join(evidence)} {_optional_text(task.get('risk_level')) or ''}".casefold()
    if category in {"billing_account_flow", "data_migration"}:
        return "critical"
    if category == "rollout_risk" and any(
        token in context
        for token in ("production", "rollback", "incident", "known customer", "launch")
    ):
        return "critical"
    if category in {"rollout_risk", "escalation_path", "known_customer_communication"}:
        return "high"
    if "high" in context or "blocker" in context or "critical" in context:
        return "high"
    return "medium"


def _suggested_artifacts(
    category: SupportReadinessCategory,
    task: Mapping[str, Any],
) -> tuple[str, ...]:
    artifacts = list(_ARTIFACTS[category])
    context = _task_context(task)
    if _ROLLOUT_RE.search(context):
        artifacts.extend(
            (
                "Rollback wording for support replies.",
                "Launch watch items and owner handoff.",
            )
        )
    if _ESCALATION_RE.search(context):
        artifacts.append("Escalation owner and severity routing.")
    return tuple(_dedupe(artifacts))


_ARTIFACTS: dict[SupportReadinessCategory, tuple[str, ...]] = {
    "user_visible_behavior": (
        "Support notes explaining the changed customer behavior.",
        "Troubleshooting steps for the affected workflow.",
    ),
    "billing_account_flow": (
        "Customer messaging for billing or account-impacting changes.",
        "Troubleshooting steps for payment, subscription, or account failures.",
        "Escalation owner and severity routing.",
    ),
    "data_migration": (
        "Support notes covering expected customer-visible data changes.",
        "Troubleshooting steps for missing, delayed, or unexpected migrated data.",
        "Rollback wording for support replies.",
    ),
    "rollout_risk": (
        "Launch watch items and owner handoff.",
        "Rollback wording for support replies.",
        "Escalation owner and severity routing.",
    ),
    "documentation_change": (
        "Support notes linking the updated documentation.",
        "Help center or release note handoff for support agents.",
    ),
    "support_macro": (
        "Support macro or canned response update.",
        "Agent-facing support notes with usage guidance.",
    ),
    "faq_help_center": (
        "FAQ or help center article update.",
        "Troubleshooting steps for common customer questions.",
    ),
    "escalation_path": (
        "Escalation owner and severity routing.",
        "Support notes describing when to escalate.",
    ),
    "known_customer_communication": (
        "Known-customer communication plan.",
        "Customer messaging approved by product or success.",
    ),
}


def _rationale(category: SupportReadinessCategory) -> str:
    return {
        "user_visible_behavior": "Customer-visible behavior changes require support context before launch.",
        "billing_account_flow": "Billing or account changes can create urgent customer-impacting support cases.",
        "data_migration": "Migration work can produce visible data discrepancies that support must explain or escalate.",
        "rollout_risk": "Rollout-sensitive work needs launch-watch, rollback, and escalation language ready.",
        "documentation_change": "Documentation changes need a support handoff so agents can point customers to current guidance.",
        "support_macro": "Support macro changes should be ready before agents receive related contacts.",
        "faq_help_center": "FAQ or help center updates reduce avoidable support contacts after launch.",
        "escalation_path": "Explicit escalation paths prevent support from stalling on launch-impacting customer issues.",
        "known_customer_communication": "Known-customer communication should be coordinated before customer-impacting rollout.",
    }[category]


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if source is None:
        return None, []
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
        task = _task_payload(source)
        return (None, [task]) if task else (None, [])

    tasks: list[dict[str, Any]] = []
    for item in iterator:
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
        "tasks",
        "title",
        "description",
        "milestone",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "test_strategy",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "validation_plan",
        "metadata",
        "tags",
        "labels",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "validation_plan",
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
    return " ".join(
        text for _, text in [*_task_texts(task), *_metadata_texts(task.get("metadata"))]
    )


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
    signals: dict[SupportReadinessCategory, list[str]],
    category: SupportReadinessCategory,
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
    "SupportReadinessCategory",
    "SupportReadinessSeverity",
    "TaskCustomerSupportReadinessPlan",
    "TaskCustomerSupportReadinessRecord",
    "build_task_customer_support_readiness",
    "build_task_customer_support_readiness_plan",
    "derive_task_customer_support_readiness_plan",
    "task_customer_support_readiness_plan_to_dict",
    "task_customer_support_readiness_plan_to_markdown",
    "task_customer_support_readiness_to_dict",
    "task_customer_support_readiness_to_markdown",
]
