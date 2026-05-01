"""Classify execution tasks by release-note communication impact."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ReleaseNoteImpactCategory = Literal[
    "user_facing_feature",
    "behavior_change",
    "breaking_change",
    "bug_fix",
    "operational_internal",
    "documentation_only",
    "migration_required",
]
ReleaseNoteImpactSeverity = Literal["critical", "high", "medium", "low"]
ReleaseNoteAudience = Literal["customers", "admins", "developers", "support", "internal", "docs"]

_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_BREAKING_RE = re.compile(
    r"\b(?:breaking(?:\s+change)?|backwards?\s+incompatib|incompatib|remove\s+support|"
    r"drop\s+support|deprecated?\s+api|deprecation|rename\s+(?:api|field|endpoint)|"
    r"delete\s+(?:api|field|endpoint)|contract\s+change)\b",
    re.IGNORECASE,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|required\s+migration|migrate|schema\s+change|schema\s+migration|"
    r"data\s+migration|backfill|reindex|manual\s+upgrade|upgrade\s+path)\b",
    re.IGNORECASE,
)
_FEATURE_RE = re.compile(
    r"\b(?:add|launch|enable|introduce|new|feature|self[- ]serve|user(?:s)?\s+can|"
    r"customers?\s+can|admins?\s+can|dashboard|ui|workflow)\b",
    re.IGNORECASE,
)
_BEHAVIOR_RE = re.compile(
    r"\b(?:change|adjust|update|alter|default|now\s+(?:uses|shows|requires|returns)|"
    r"behavior|validation|permission|limit|rate\s+limit|notification|sorting|ranking)\b",
    re.IGNORECASE,
)
_BUG_RE = re.compile(
    r"\b(?:fix|bug|defect|regression|incorrect|broken|failure|error|crash|exception|"
    r"resolve|repair|retry)\b",
    re.IGNORECASE,
)
_DOCS_RE = re.compile(r"\b(?:docs?|documentation|readme|guide|tutorial|changelog|help)\b", re.IGNORECASE)
_INTERNAL_RE = re.compile(
    r"\b(?:internal[- ]only|internal|refactor|cleanup|ci|pipeline|build|test harness|"
    r"developer tooling|observability|telemetry|runbook|logging|monitoring|infra|"
    r"infrastructure|deployment|admin\s+tooling)\b",
    re.IGNORECASE,
)
_CUSTOMER_RE = re.compile(
    r"\b(?:customer|user|merchant|tenant|account|admin|developer|api|sdk|public|external|"
    r"beta|dashboard|checkout|onboarding|notification|integration)\b",
    re.IGNORECASE,
)
_ADMIN_RE = re.compile(r"\b(?:admin|administrator|owner|operator|tenant)\b", re.IGNORECASE)
_DEVELOPER_RE = re.compile(r"\b(?:api|sdk|webhook|endpoint|schema|developer|integration)\b", re.IGNORECASE)
_SUPPORT_RE = re.compile(r"\b(?:support|ticket|runbook|incident|escalation)\b", re.IGNORECASE)
_DOC_PATH_RE = re.compile(r"(?:^|/)(?:docs?|guides?|readme|changelog)(?:/|\.|$)", re.IGNORECASE)
_INTERNAL_PATH_RE = re.compile(
    r"(?:^|/)(?:tests?|ci|\.github|scripts?|infra|ops|deploy|internal|tools?)(?:/|$)",
    re.IGNORECASE,
)
_CUSTOMER_PATH_RE = re.compile(
    r"(?:^|/)(?:app|web|ui|frontend|api|sdk|public|clients?|routes?|pages?)(?:/|$)",
    re.IGNORECASE,
)
_CATEGORY_ORDER: dict[ReleaseNoteImpactCategory, int] = {
    "breaking_change": 0,
    "migration_required": 1,
    "user_facing_feature": 2,
    "behavior_change": 3,
    "bug_fix": 4,
    "documentation_only": 5,
    "operational_internal": 6,
}
_SEVERITY_ORDER: dict[ReleaseNoteImpactSeverity, int] = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}


@dataclass(frozen=True, slots=True)
class PlanReleaseNoteImpactRecord:
    """Release-note impact classification for one execution task."""

    task_id: str
    title: str
    category: ReleaseNoteImpactCategory
    severity: ReleaseNoteImpactSeverity
    audience: ReleaseNoteAudience
    customer_facing_communication_recommended: bool
    evidence: tuple[str, ...] = field(default_factory=tuple)
    dependencies: tuple[str, ...] = field(default_factory=tuple)
    files_or_modules: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "category": self.category,
            "severity": self.severity,
            "audience": self.audience,
            "customer_facing_communication_recommended": (
                self.customer_facing_communication_recommended
            ),
            "evidence": list(self.evidence),
            "dependencies": list(self.dependencies),
            "files_or_modules": list(self.files_or_modules),
        }


@dataclass(frozen=True, slots=True)
class PlanReleaseNoteImpactReport:
    """Release-note impact report for an execution plan."""

    plan_id: str | None = None
    records: tuple[PlanReleaseNoteImpactRecord, ...] = field(default_factory=tuple)

    @property
    def summary_counts(self) -> dict[str, int]:
        """Return counts by impact category in stable key order."""
        return {
            "user_facing_feature": sum(
                1 for record in self.records if record.category == "user_facing_feature"
            ),
            "behavior_change": sum(
                1 for record in self.records if record.category == "behavior_change"
            ),
            "breaking_change": sum(
                1 for record in self.records if record.category == "breaking_change"
            ),
            "bug_fix": sum(1 for record in self.records if record.category == "bug_fix"),
            "operational_internal": sum(
                1 for record in self.records if record.category == "operational_internal"
            ),
            "documentation_only": sum(
                1 for record in self.records if record.category == "documentation_only"
            ),
            "migration_required": sum(
                1 for record in self.records if record.category == "migration_required"
            ),
        }

    @property
    def summary(self) -> dict[str, Any]:
        """Return compact rollup counts in stable key order."""
        customer_facing_count = sum(
            1 for record in self.records if record.customer_facing_communication_recommended
        )
        return {
            "task_count": len(self.records),
            "customer_facing_count": customer_facing_count,
            "internal_only_count": sum(
                1 for record in self.records if record.audience == "internal"
            ),
            "high_or_critical_count": sum(
                1 for record in self.records if record.severity in {"critical", "high"}
            ),
            "summary_counts": self.summary_counts,
        }

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "summary": self.summary,
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return impact records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Plan Release Note Impact"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No execution tasks were classified."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Severity | Category | Audience | Customer-facing | Task | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{record.severity} | "
                f"{record.category} | "
                f"{record.audience} | "
                f"{'yes' if record.customer_facing_communication_recommended else 'no'} | "
                f"{_markdown_cell(f'{record.task_id}: {record.title}')} | "
                f"{_markdown_cell('; '.join(record.evidence))} |"
            )
        return "\n".join(lines)


def build_plan_release_note_impact_report(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanReleaseNoteImpactReport:
    """Classify execution tasks by release-note communication impact."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    records = tuple(
        sorted(
            (
                _record(
                    task,
                    fallback_id=f"task-{index}",
                    plan_metadata=(
                        plan.get("metadata") if isinstance(plan.get("metadata"), Mapping) else {}
                    ),
                )
                for index, task in enumerate(tasks, start=1)
            ),
            key=_record_sort_key,
        )
    )
    return PlanReleaseNoteImpactReport(plan_id=_optional_text(plan.get("id")), records=records)


def plan_release_note_impact_report_to_dict(
    report: PlanReleaseNoteImpactReport,
) -> dict[str, Any]:
    """Serialize a release-note impact report to a plain dictionary."""
    return report.to_dict()


plan_release_note_impact_report_to_dict.__test__ = False


def plan_release_note_impact_report_to_markdown(report: PlanReleaseNoteImpactReport) -> str:
    """Render a release-note impact report as Markdown."""
    return report.to_markdown()


plan_release_note_impact_report_to_markdown.__test__ = False


def summarize_plan_release_note_impact(
    source: Mapping[str, Any] | ExecutionPlan | PlanReleaseNoteImpactReport,
) -> dict[str, Any]:
    """Return release-note impact summary counts for a plan or report."""
    if isinstance(source, PlanReleaseNoteImpactReport):
        return source.summary
    return build_plan_release_note_impact_report(source).summary


summarize_plan_release_note_impact.__test__ = False


def _record(
    task: Mapping[str, Any],
    *,
    fallback_id: str,
    plan_metadata: Mapping[str, Any],
) -> PlanReleaseNoteImpactRecord:
    task_id = _optional_text(task.get("id")) or fallback_id
    title = _optional_text(task.get("title")) or task_id
    metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}
    context = _task_context(task)
    files = tuple(_dedupe(_clean_sentence(value) for value in _strings(task.get("files_or_modules"))))
    dependencies = tuple(_dedupe(_clean_sentence(value) for value in _strings(task.get("depends_on"))))
    internal_only = _internal_only(metadata, plan_metadata, context, files)
    docs_only = _docs_only(task, context, files)
    category = _category(context, metadata, files, internal_only=internal_only, docs_only=docs_only)
    severity = _severity(task, context, metadata, category, internal_only=internal_only)
    audience = _audience(context, metadata, files, category, internal_only=internal_only)
    customer_facing = _customer_facing_recommended(
        context,
        category,
        severity,
        audience,
        internal_only=internal_only,
    )
    return PlanReleaseNoteImpactRecord(
        task_id=task_id,
        title=title,
        category=category,
        severity=severity,
        audience=audience,
        customer_facing_communication_recommended=customer_facing,
        evidence=tuple(_evidence(task, metadata, context, files, category, severity, internal_only)),
        dependencies=dependencies,
        files_or_modules=files,
    )


def _category(
    context: str,
    metadata: Mapping[str, Any],
    files: tuple[str, ...],
    *,
    internal_only: bool,
    docs_only: bool,
) -> ReleaseNoteImpactCategory:
    explicit = _explicit_category(metadata)
    if explicit:
        return explicit
    if internal_only:
        return "operational_internal"
    if _BREAKING_RE.search(context):
        return "breaking_change"
    if _MIGRATION_RE.search(context):
        return "migration_required"
    if docs_only:
        return "documentation_only"
    if _BUG_RE.search(context):
        return "bug_fix"
    if _BEHAVIOR_RE.search(context):
        return "behavior_change"
    if _FEATURE_RE.search(context) or any(_CUSTOMER_PATH_RE.search(path) for path in files):
        return "user_facing_feature"
    return "operational_internal"


def _severity(
    task: Mapping[str, Any],
    context: str,
    metadata: Mapping[str, Any],
    category: ReleaseNoteImpactCategory,
    *,
    internal_only: bool,
) -> ReleaseNoteImpactSeverity:
    explicit = _metadata_value(metadata, "release_note_severity", "severity", "impact_severity")
    if explicit and explicit.lower() in _SEVERITY_ORDER:
        return explicit.lower()  # type: ignore[return-value]

    risk = (_optional_text(task.get("risk_level")) or "").lower()
    if category == "breaking_change":
        return "critical" if risk in {"critical", "high", "blocker"} else "high"
    if category == "migration_required":
        return "high" if risk in {"critical", "high", "blocker"} or _CUSTOMER_RE.search(context) else "medium"
    if internal_only:
        return "medium" if risk in {"critical", "high", "blocker"} else "low"
    if category in {"user_facing_feature", "behavior_change"}:
        return "high" if risk in {"critical", "high", "blocker"} else "medium"
    if category == "bug_fix":
        return "medium" if risk in {"critical", "high", "blocker"} or _CUSTOMER_RE.search(context) else "low"
    return "low"


def _audience(
    context: str,
    metadata: Mapping[str, Any],
    files: tuple[str, ...],
    category: ReleaseNoteImpactCategory,
    *,
    internal_only: bool,
) -> ReleaseNoteAudience:
    explicit = _metadata_value(metadata, "release_note_audience", "audience", "target_audience")
    if explicit and explicit.lower() in {"customers", "admins", "developers", "support", "internal", "docs"}:
        return explicit.lower()  # type: ignore[return-value]
    if internal_only or category == "operational_internal":
        return "internal"
    if category == "documentation_only":
        return "docs"
    if _DEVELOPER_RE.search(context) or any("api" in path.lower() or "sdk" in path.lower() for path in files):
        return "developers"
    if _ADMIN_RE.search(context):
        return "admins"
    if _SUPPORT_RE.search(context):
        return "support"
    return "customers"


def _customer_facing_recommended(
    context: str,
    category: ReleaseNoteImpactCategory,
    severity: ReleaseNoteImpactSeverity,
    audience: ReleaseNoteAudience,
    *,
    internal_only: bool,
) -> bool:
    if internal_only or audience == "internal":
        return False
    if category in {"breaking_change", "migration_required", "user_facing_feature", "behavior_change"}:
        return True
    if category == "bug_fix":
        return severity in {"high", "medium"} and bool(_CUSTOMER_RE.search(context))
    return False


def _evidence(
    task: Mapping[str, Any],
    metadata: Mapping[str, Any],
    context: str,
    files: tuple[str, ...],
    category: ReleaseNoteImpactCategory,
    severity: ReleaseNoteImpactSeverity,
    internal_only: bool,
) -> list[str]:
    evidence: list[str] = [f"Classified as {category}."]
    if _BREAKING_RE.search(context):
        evidence.append("Breaking-change signal found.")
    if _MIGRATION_RE.search(context):
        evidence.append("Migration-required signal found.")
    if _BUG_RE.search(context):
        evidence.append("Bug-fix signal found.")
    if _CUSTOMER_RE.search(context) or any(_CUSTOMER_PATH_RE.search(path) for path in files):
        evidence.append("Customer-facing surface signal found.")
    if internal_only:
        evidence.append("Internal-only signal found.")
    if files and all(_DOC_PATH_RE.search(path) for path in files):
        evidence.append("Documentation-only file paths.")
    risk = _optional_text(task.get("risk_level"))
    if risk:
        evidence.append(f"Risk level: {risk}.")
    explicit = _metadata_value(metadata, "release_note_category", "release_note_impact", "impact_category")
    if explicit:
        evidence.append(f"Metadata category: {explicit}.")
    evidence.append(f"Severity: {severity}.")
    return _dedupe(evidence)


def _explicit_category(metadata: Mapping[str, Any]) -> ReleaseNoteImpactCategory | None:
    value = _metadata_value(metadata, "release_note_category", "release_note_impact", "impact_category")
    if not value:
        return None
    normalized = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    aliases: dict[str, ReleaseNoteImpactCategory] = {
        "feature": "user_facing_feature",
        "user_facing": "user_facing_feature",
        "user_facing_feature": "user_facing_feature",
        "behavior": "behavior_change",
        "behavior_change": "behavior_change",
        "breaking": "breaking_change",
        "breaking_change": "breaking_change",
        "bug": "bug_fix",
        "bug_fix": "bug_fix",
        "fix": "bug_fix",
        "internal": "operational_internal",
        "operational": "operational_internal",
        "operational_internal": "operational_internal",
        "docs": "documentation_only",
        "documentation": "documentation_only",
        "documentation_only": "documentation_only",
        "migration": "migration_required",
        "migration_required": "migration_required",
    }
    return aliases.get(normalized)


def _internal_only(
    metadata: Mapping[str, Any],
    plan_metadata: Mapping[str, Any],
    context: str,
    files: tuple[str, ...],
) -> bool:
    for source in (metadata, plan_metadata):
        for key in ("internal_only", "release_note_internal_only", "customer_facing"):
            if key not in source:
                continue
            value = source[key]
            if key == "customer_facing":
                return value is False or str(value).lower() in {"false", "no", "0"}
            return value is True or str(value).lower() in {"true", "yes", "1", "internal"}
    if "internal-only" in context.lower():
        return True
    if _INTERNAL_RE.search(context) and not _CUSTOMER_RE.search(context):
        return True
    return bool(files) and all(_INTERNAL_PATH_RE.search(path) for path in files)


def _docs_only(task: Mapping[str, Any], context: str, files: tuple[str, ...]) -> bool:
    if files and all(_DOC_PATH_RE.search(path) for path in files):
        return True
    title = _optional_text(task.get("title")) or ""
    return bool(_DOCS_RE.search(context)) and not any(
        pattern.search(title) or pattern.search(context)
        for pattern in (_BREAKING_RE, _MIGRATION_RE, _FEATURE_RE, _BUG_RE)
    )


def _record_sort_key(
    record: PlanReleaseNoteImpactRecord,
) -> tuple[int, int, str, str]:
    return (
        _SEVERITY_ORDER[record.severity],
        _CATEGORY_ORDER[record.category],
        record.task_id,
        record.title.lower(),
    )


def _task_context(task: Mapping[str, Any]) -> str:
    metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}
    values = [
        _text(task.get("title")),
        _text(task.get("description")),
        *_strings(task.get("acceptance_criteria")),
        *_strings(task.get("files_or_modules")),
        *_strings(task.get("depends_on")),
        *_strings(task.get("tags")),
        *_strings(metadata),
    ]
    return " ".join(value for value in values if value)


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            dumped = item.model_dump(mode="python")
            if isinstance(dumped, Mapping):
                tasks.append(dict(dumped))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _metadata_value(metadata: Mapping[str, Any], *keys: str) -> str | None:
    values = _metadata_values(metadata, *keys)
    return values[0] if values else None


def _metadata_values(metadata: Mapping[str, Any], *keys: str) -> list[str]:
    values: list[str] = []
    wanted = {key.lower() for key in keys}
    for key, value in metadata.items():
        normalized = str(key).lower()
        if normalized in wanted:
            values.extend(_strings(value))
        elif isinstance(value, Mapping):
            values.extend(_metadata_values(value, *keys))
    return values


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


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _clean_sentence(value: str) -> str:
    return _SPACE_RE.sub(" ", str(value)).strip().strip("`'\",;:()[]{}").rstrip(".")


def _dedupe(values: Iterable[_T]) -> tuple[_T, ...]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return tuple(deduped)


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "PlanReleaseNoteImpactRecord",
    "PlanReleaseNoteImpactReport",
    "ReleaseNoteAudience",
    "ReleaseNoteImpactCategory",
    "ReleaseNoteImpactSeverity",
    "build_plan_release_note_impact_report",
    "plan_release_note_impact_report_to_dict",
    "plan_release_note_impact_report_to_markdown",
    "summarize_plan_release_note_impact",
]
