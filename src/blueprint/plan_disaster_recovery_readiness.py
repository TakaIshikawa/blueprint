"""Build plan-level disaster recovery readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


RecoveryCapability = Literal[
    "backup",
    "restore",
    "failover",
    "replica",
    "rpo_rto",
    "regional_outage",
    "incident_response",
    "data_recovery",
    "manual_recovery",
]
RecoveryReadinessSeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CAPABILITY_ORDER: dict[RecoveryCapability, int] = {
    "backup": 0,
    "restore": 1,
    "failover": 2,
    "replica": 3,
    "rpo_rto": 4,
    "regional_outage": 5,
    "incident_response": 6,
    "data_recovery": 7,
    "manual_recovery": 8,
}
_SEVERITY_ORDER: dict[RecoveryReadinessSeverity, int] = {"high": 0, "medium": 1, "low": 2}
_CAPABILITY_PATTERNS: tuple[tuple[RecoveryCapability, re.Pattern[str]], ...] = (
    (
        "backup",
        re.compile(
            r"\b(?:backup|backups|snapshot|snapshots|restore point|pitr|point[- ]in[- ]time)\b",
            re.I,
        ),
    ),
    (
        "restore",
        re.compile(
            r"\b(?:restore|restores|restoration|restore test|restore validation|rollback data)\b",
            re.I,
        ),
    ),
    (
        "failover",
        re.compile(
            r"\b(?:failover|fail over|fail-over|failback|fail back|standby promotion|promote standby)\b",
            re.I,
        ),
    ),
    (
        "replica",
        re.compile(
            r"\b(?:replica|replicas|replication|read replica|standby|secondary region|hot standby)\b",
            re.I,
        ),
    ),
    (
        "rpo_rto",
        re.compile(r"\b(?:rpo|recovery point objective|rto|recovery time objective)\b", re.I),
    ),
    (
        "regional_outage",
        re.compile(
            r"\b(?:regional outage|region outage|region failure|az outage|availability zone|multi[- ]region|cross[- ]region|disaster recovery|dr)\b",
            re.I,
        ),
    ),
    (
        "incident_response",
        re.compile(
            r"\b(?:incident|incident commander|war room|sev[ -]?[0-9]|on[- ]call|pager|escalation|postmortem)\b",
            re.I,
        ),
    ),
    (
        "data_recovery",
        re.compile(
            r"\b(?:data recovery|recover data|recover records?|lost data|corrupt(?:ed|ion)?|data loss|replay events?)\b",
            re.I,
        ),
    ),
    (
        "manual_recovery",
        re.compile(
            r"\b(?:manual recovery|manual restore|manual failover|runbook|operator action|break glass|manual step)\b",
            re.I,
        ),
    ),
)
_PATH_PATTERNS: tuple[tuple[RecoveryCapability, re.Pattern[str]], ...] = (
    ("backup", re.compile(r"(?:^|/)(?:backups?|snapshots?|pitr)(?:/|$)|backup|snapshot", re.I)),
    ("restore", re.compile(r"(?:^|/)(?:restore|restores|restoration)(?:/|$)|restore", re.I)),
    ("failover", re.compile(r"(?:^|/)(?:failover|failback|standby)(?:/|$)|failover", re.I)),
    ("replica", re.compile(r"(?:^|/)(?:replicas?|replication|standby)(?:/|$)|replica", re.I)),
    (
        "regional_outage",
        re.compile(r"(?:^|/)(?:regions?|multi-region|dr|disaster-recovery)(?:/|$)|regional", re.I),
    ),
    (
        "incident_response",
        re.compile(r"(?:^|/)(?:incident|incidents|oncall|runbooks?)(?:/|$)", re.I),
    ),
    (
        "data_recovery",
        re.compile(r"(?:^|/)(?:recovery|replay|repair)(?:/|$)|data[_-]?recovery", re.I),
    ),
    (
        "manual_recovery",
        re.compile(r"(?:^|/)(?:runbooks?|manual|break-glass)(?:/|$)|manual[_-]?recovery", re.I),
    ),
)
_RPO_RE = re.compile(r"\b(?:rpo|recovery point objective)\b", re.I)
_RTO_RE = re.compile(r"\b(?:rto|recovery time objective)\b", re.I)
_RESTORE_VALIDATION_RE = re.compile(
    r"\b(?:restore validation|restore test|test restore|validated restore|rehears(?:e|al)|"
    r"drill|game day|tabletop|smoke test|checksum|row count|verify restore|validate restore)\b",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:owner|owners|assignee|assignees|on[- ]call|sre|dri|incident commander|operator)\b", re.I
)
_OWNER_KEYS = (
    "owner",
    "owners",
    "assignee",
    "assignees",
    "owner_hint",
    "owner_hints",
    "owner_type",
    "suggested_owner",
    "reviewer",
    "reviewers",
    "dri",
    "oncall",
    "on_call",
)


@dataclass(frozen=True, slots=True)
class PlanDisasterRecoveryReadinessRow:
    """Readiness guidance for one disaster recovery capability."""

    recovery_capability: RecoveryCapability
    covered_task_ids: tuple[str, ...] = field(default_factory=tuple)
    readiness_gaps: tuple[str, ...] = field(default_factory=tuple)
    required_rehearsal_artifacts: tuple[str, ...] = field(default_factory=tuple)
    owner_hints: tuple[str, ...] = field(default_factory=tuple)
    severity: RecoveryReadinessSeverity = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "recovery_capability": self.recovery_capability,
            "covered_task_ids": list(self.covered_task_ids),
            "readiness_gaps": list(self.readiness_gaps),
            "required_rehearsal_artifacts": list(self.required_rehearsal_artifacts),
            "owner_hints": list(self.owner_hints),
            "severity": self.severity,
            "evidence": list(self.evidence),
            "follow_up_questions": list(self.follow_up_questions),
        }


@dataclass(frozen=True, slots=True)
class PlanDisasterRecoveryReadinessMatrix:
    """Plan-level disaster recovery readiness matrix and rollup counts."""

    plan_id: str | None = None
    rows: tuple[PlanDisasterRecoveryReadinessRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return matrix rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the disaster recovery readiness matrix as deterministic Markdown."""
        title = "# Plan Disaster Recovery Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Recovery capability count: {self.summary.get('recovery_capability_count', 0)}",
            f"- Covered task count: {self.summary.get('covered_task_count', 0)}",
            f"- Follow-up question count: {self.summary.get('follow_up_question_count', 0)}",
            (
                "- Severity counts: "
                f"high {severity_counts.get('high', 0)}, "
                f"medium {severity_counts.get('medium', 0)}, "
                f"low {severity_counts.get('low', 0)}"
            ),
        ]
        if not self.rows:
            lines.extend(["", "No disaster recovery readiness signals were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Capability | Severity | Tasks | Gaps | Rehearsal Artifacts | Owners | Follow-up Questions | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.recovery_capability} | "
                f"{row.severity} | "
                f"{_markdown_cell(', '.join(row.covered_task_ids) or 'plan metadata')} | "
                f"{_markdown_cell('; '.join(row.readiness_gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.required_rehearsal_artifacts) or 'none')} | "
                f"{_markdown_cell(', '.join(row.owner_hints) or 'none')} | "
                f"{_markdown_cell('; '.join(row.follow_up_questions) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_disaster_recovery_readiness_matrix(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanDisasterRecoveryReadinessMatrix:
    """Derive disaster recovery readiness guidance from an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    grouped: dict[RecoveryCapability, dict[str, Any]] = {
        capability: {"task_ids": [], "evidence": [], "texts": [], "owners": []}
        for capability in _CAPABILITY_ORDER
    }

    plan_metadata = plan.get("metadata")
    for source_field, text in _plan_candidate_texts(plan):
        for capability, pattern in _CAPABILITY_PATTERNS:
            if pattern.search(text):
                grouped[capability]["evidence"].append(_evidence_snippet(source_field, text))
                grouped[capability]["texts"].append(text)
        if isinstance(plan_metadata, Mapping):
            grouped_text = _evidence_snippet(source_field, text)
            for capability in _metadata_capabilities(plan_metadata):
                grouped[capability]["evidence"].append(grouped_text)
                grouped[capability]["texts"].append(text)

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        signals = _task_signals(task)
        for capability, evidence in signals.items():
            grouped[capability]["task_ids"].append(task_id)
            grouped[capability]["evidence"].extend(evidence)
            grouped[capability]["texts"].extend(text for _, text in _candidate_texts(task))
            grouped[capability]["owners"].extend(_explicit_owner_hints(task))

    rows = tuple(
        sorted(
            (
                _row(capability, values)
                for capability, values in grouped.items()
                if values["task_ids"] or values["evidence"]
            ),
            key=lambda row: (
                _SEVERITY_ORDER[row.severity],
                _CAPABILITY_ORDER[row.recovery_capability],
                row.covered_task_ids,
            ),
        )
    )
    covered_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.covered_task_ids))
    severity_counts = {
        severity: sum(1 for row in rows if row.severity == severity) for severity in _SEVERITY_ORDER
    }
    capability_counts = {
        capability: sum(1 for row in rows if row.recovery_capability == capability)
        for capability in _CAPABILITY_ORDER
    }
    return PlanDisasterRecoveryReadinessMatrix(
        plan_id=_optional_text(plan.get("id")),
        rows=rows,
        summary={
            "task_count": len(tasks),
            "recovery_capability_count": len(rows),
            "covered_task_count": len(covered_task_ids),
            "follow_up_question_count": sum(len(row.follow_up_questions) for row in rows),
            "gap_count": sum(len(row.readiness_gaps) for row in rows),
            "severity_counts": severity_counts,
            "capability_counts": capability_counts,
        },
    )


def summarize_plan_disaster_recovery_readiness(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanDisasterRecoveryReadinessMatrix:
    """Compatibility alias for building disaster recovery readiness matrices."""
    return build_plan_disaster_recovery_readiness_matrix(source)


def plan_disaster_recovery_readiness_matrix_to_dict(
    matrix: PlanDisasterRecoveryReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a disaster recovery readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_disaster_recovery_readiness_matrix_to_dict.__test__ = False


def plan_disaster_recovery_readiness_matrix_to_markdown(
    matrix: PlanDisasterRecoveryReadinessMatrix,
) -> str:
    """Render a disaster recovery readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_disaster_recovery_readiness_matrix_to_markdown.__test__ = False


def _row(
    capability: RecoveryCapability,
    values: Mapping[str, list[str]],
) -> PlanDisasterRecoveryReadinessRow:
    texts = tuple(values.get("texts", ()))
    explicit_owner_hints = tuple(_dedupe(values.get("owners", ())))
    owner_hints = tuple(_dedupe([*explicit_owner_hints, *_default_owner_hints(capability)]))
    gaps = tuple(_readiness_gaps(texts, explicit_owner_hints))
    return PlanDisasterRecoveryReadinessRow(
        recovery_capability=capability,
        covered_task_ids=tuple(sorted(_dedupe(values.get("task_ids", ())))),
        readiness_gaps=gaps,
        required_rehearsal_artifacts=_required_rehearsal_artifacts(capability),
        owner_hints=owner_hints,
        severity=_severity(capability, gaps),
        evidence=tuple(_dedupe(values.get("evidence", ()))),
        follow_up_questions=tuple(_follow_up_questions(gaps, capability)),
    )


def _task_signals(task: Mapping[str, Any]) -> dict[RecoveryCapability, tuple[str, ...]]:
    signals: dict[RecoveryCapability, list[str]] = {}
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for capability in _metadata_capabilities(metadata):
            _append(signals, capability, f"metadata.recovery_capabilities: {capability}")

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        for capability, pattern in _PATH_PATTERNS:
            if pattern.search(normalized):
                _append(signals, capability, f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        for capability, pattern in _CAPABILITY_PATTERNS:
            if pattern.search(text):
                _append(signals, capability, _evidence_snippet(source_field, text))

    return {capability: tuple(_dedupe(values)) for capability, values in signals.items()}


def _metadata_capabilities(metadata: Mapping[str, Any]) -> tuple[RecoveryCapability, ...]:
    capabilities: list[RecoveryCapability] = []
    for key in (
        "recovery_capability",
        "recovery_capabilities",
        "dr_capability",
        "dr_capabilities",
        "disaster_recovery",
        "disaster_recovery_capabilities",
    ):
        for value in _strings(metadata.get(key)):
            normalized = value.casefold().replace("-", "_").replace(" ", "_").replace("/", "_")
            if normalized in _CAPABILITY_ORDER:
                capabilities.append(normalized)  # type: ignore[arg-type]
    return tuple(_dedupe(capabilities))


def _readiness_gaps(texts: tuple[str, ...], owner_hints: tuple[str, ...]) -> list[str]:
    context = " ".join(texts)
    gaps: list[str] = []
    if not _RPO_RE.search(context):
        gaps.append("Missing RPO target")
    if not _RTO_RE.search(context):
        gaps.append("Missing RTO target")
    if not _RESTORE_VALIDATION_RE.search(context):
        gaps.append("Missing restore validation evidence")
    if not owner_hints and not _OWNER_RE.search(context):
        gaps.append("Missing recovery owner")
    return gaps


def _follow_up_questions(
    gaps: tuple[str, ...],
    capability: RecoveryCapability,
) -> list[str]:
    questions: list[str] = []
    if "Missing RPO target" in gaps:
        questions.append(f"What RPO target applies to {capability}?")
    if "Missing RTO target" in gaps:
        questions.append(f"What RTO target applies to {capability}?")
    if "Missing restore validation evidence" in gaps:
        questions.append(
            f"Which restore validation or rehearsal artifact proves {capability} works?"
        )
    if "Missing recovery owner" in gaps:
        questions.append(f"Who owns recovery decisions for {capability}?")
    return questions


def _required_rehearsal_artifacts(capability: RecoveryCapability) -> tuple[str, ...]:
    defaults = {
        "backup": (
            "Backup inventory with retention window",
            "Sample restore proof from the backup set",
        ),
        "restore": (
            "Restore runbook",
            "Restore validation log with checksums or row counts",
        ),
        "failover": (
            "Failover runbook",
            "Failover drill results with failback criteria",
        ),
        "replica": (
            "Replica lag dashboard",
            "Replica promotion rehearsal notes",
        ),
        "rpo_rto": (
            "Approved RPO/RTO targets",
            "Measurement evidence from the latest recovery drill",
        ),
        "regional_outage": (
            "Regional outage tabletop notes",
            "Cross-region dependency checklist",
        ),
        "incident_response": (
            "Incident command roster",
            "Escalation and communications checklist",
        ),
        "data_recovery": (
            "Data recovery runbook",
            "Recovered record validation sample",
        ),
        "manual_recovery": (
            "Manual recovery runbook",
            "Operator rehearsal sign-off",
        ),
    }
    return defaults[capability]


def _default_owner_hints(capability: RecoveryCapability) -> tuple[str, ...]:
    defaults = {
        "backup": ("database owner", "SRE owner"),
        "restore": ("database owner", "service owner"),
        "failover": ("SRE owner", "incident commander"),
        "replica": ("database owner", "infrastructure owner"),
        "rpo_rto": ("product owner", "SRE owner"),
        "regional_outage": ("incident commander", "infrastructure owner"),
        "incident_response": ("incident commander", "on-call owner"),
        "data_recovery": ("data owner", "service owner"),
        "manual_recovery": ("operations owner", "service owner"),
    }
    return defaults[capability]


def _severity(
    capability: RecoveryCapability,
    gaps: tuple[str, ...],
) -> RecoveryReadinessSeverity:
    if capability in {"failover", "regional_outage", "data_recovery", "manual_recovery"}:
        return "high"
    if len(gaps) >= 3:
        return "high"
    if capability in {"restore", "rpo_rto", "incident_response"} or gaps:
        return "medium"
    return "low"


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        text = _optional_text(task.get(field_name))
        if text:
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "depends_on", "dependencies"):
        for text in _strings(task.get(field_name)):
            texts.append((field_name, text))
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        texts.append(("files_or_modules", path))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in sorted(metadata, key=lambda item: str(item)):
            for text in _strings(metadata[key]):
                texts.append((f"metadata.{key}", text))
    return texts


def _plan_candidate_texts(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "target_engine",
        "target_repo",
        "project_type",
        "test_strategy",
        "handoff_prompt",
        "status",
    ):
        text = _optional_text(plan.get(field_name))
        if text:
            texts.append((field_name, text))
    for field_name in ("milestones",):
        for text in _strings(plan.get(field_name)):
            texts.append((field_name, text))
    metadata = plan.get("metadata")
    if isinstance(metadata, Mapping):
        for key in sorted(metadata, key=lambda item: str(item)):
            for text in _strings(metadata[key]):
                texts.append((f"metadata.{key}", text))
    return texts


def _explicit_owner_hints(task: Mapping[str, Any]) -> list[str]:
    hints: list[str] = []
    for key in _OWNER_KEYS:
        hints.extend(_strings(task.get(key)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in _OWNER_KEYS:
            hints.extend(_strings(metadata.get(key)))
    return hints


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


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _normalized_path(path: str) -> str:
    return str(PurePosixPath(path.strip().replace("\\", "/").lower().strip("/")))


def _evidence_snippet(source_field: str, text: str) -> str:
    normalized = _text(text)
    if len(normalized) > 160:
        normalized = f"{normalized[:157].rstrip()}..."
    return f"{source_field}: {normalized}"


def _append(
    values: dict[RecoveryCapability, list[str]], key: RecoveryCapability, value: str
) -> None:
    values.setdefault(key, []).append(value)


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "PlanDisasterRecoveryReadinessMatrix",
    "PlanDisasterRecoveryReadinessRow",
    "RecoveryCapability",
    "RecoveryReadinessSeverity",
    "build_plan_disaster_recovery_readiness_matrix",
    "plan_disaster_recovery_readiness_matrix_to_dict",
    "plan_disaster_recovery_readiness_matrix_to_markdown",
    "summarize_plan_disaster_recovery_readiness",
]
