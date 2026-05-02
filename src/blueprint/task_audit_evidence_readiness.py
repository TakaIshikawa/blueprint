"""Plan task evidence readiness for audit and compliance review."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


AuditEvidenceSignal = Literal[
    "audit_review",
    "compliance_review",
    "security_review",
    "migration_change",
    "rollout_change",
    "approval_gate",
]
AuditEvidenceArtifact = Literal[
    "compliance_evidence",
    "screenshot_evidence",
    "log_evidence",
    "test_report",
    "approval_record",
    "migration_proof",
    "rollout_proof",
    "security_review_artifact",
]
AuditEvidenceRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[AuditEvidenceRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: tuple[AuditEvidenceSignal, ...] = (
    "audit_review",
    "compliance_review",
    "security_review",
    "migration_change",
    "rollout_change",
    "approval_gate",
)
_ARTIFACT_ORDER: tuple[AuditEvidenceArtifact, ...] = (
    "compliance_evidence",
    "screenshot_evidence",
    "log_evidence",
    "test_report",
    "approval_record",
    "migration_proof",
    "rollout_proof",
    "security_review_artifact",
)
_SENSITIVE_SIGNALS = {
    "audit_review",
    "compliance_review",
    "security_review",
    "migration_change",
    "rollout_change",
    "approval_gate",
}
_SIGNAL_PATTERNS: dict[AuditEvidenceSignal, re.Pattern[str]] = {
    "audit_review": re.compile(
        r"\b(?:audit(?:or)? review|audit evidence|audit readiness|audit package|audit trail evidence|"
        r"evidence for audit|soc ?2|sox|hipaa audit|gdpr audit)\b",
        re.I,
    ),
    "compliance_review": re.compile(
        r"\b(?:compliance review|compliance evidence|regulatory review|policy attestation|"
        r"control evidence|control owner|gdpr|hipaa|pci|sox|soc ?2|iso ?27001)\b",
        re.I,
    ),
    "security_review": re.compile(
        r"\b(?:security review|security approval|threat model|risk assessment|vulnerability|"
        r"penetration test|pentest|secret rotation|access control review|privacy review)\b",
        re.I,
    ),
    "migration_change": re.compile(
        r"\b(?:migration|migrate|schema change|backfill|data backfill|cutover|rollback proof|"
        r"data reconciliation|row count|pre[- ]migration|post[- ]migration)\b",
        re.I,
    ),
    "rollout_change": re.compile(
        r"\b(?:rollout|roll back|rollback|canary|dark launch|feature flag|flag ramp|gradual release|"
        r"launch gate|deployment gate|release gate|post[- ]deploy|post deploy)\b",
        re.I,
    ),
    "approval_gate": re.compile(
        r"\b(?:approval|approved by|sign[- ]?off|signoff|change advisory|cab|go/no[- ]go|"
        r"go no[- ]go|release approval|owner approval|compliance approval)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[AuditEvidenceSignal, re.Pattern[str]] = {
    "audit_review": re.compile(r"(?:^|/)(?:audit|audits|evidence|controls?)(?:/|\.|_|-|$)", re.I),
    "compliance_review": re.compile(r"(?:compliance|regulatory|soc2|sox|hipaa|gdpr|pci|controls?)", re.I),
    "security_review": re.compile(r"(?:security|threat[_-]?model|vulnerabilit|privacy|access[_-]?control)", re.I),
    "migration_change": re.compile(r"(?:^|/)(?:migrations?|backfills?|cutovers?|schema)(?:/|\.|_|-|$)", re.I),
    "rollout_change": re.compile(r"(?:rollout|release|deploy|canary|feature[_-]?flags?|dark[_-]?launch)", re.I),
    "approval_gate": re.compile(r"(?:approvals?|signoff|sign[_-]?off|cab|go[_-]?no[_-]?go)", re.I),
}
_ARTIFACT_PATTERNS: dict[AuditEvidenceArtifact, re.Pattern[str]] = {
    "compliance_evidence": re.compile(
        r"\b(?:compliance evidence|control evidence|attestation|audit evidence package|evidence package|"
        r"policy evidence|regulatory evidence)\b",
        re.I,
    ),
    "screenshot_evidence": re.compile(
        r"\b(?:screenshots?|screen captures?|before/after screenshot|visual evidence|ui proof)\b",
        re.I,
    ),
    "log_evidence": re.compile(
        r"\b(?:log excerpts?|logs? attached|deployment logs?|audit logs? export|system logs?|"
        r"event logs?|trace logs?|observability logs?)\b",
        re.I,
    ),
    "test_report": re.compile(
        r"\b(?:test reports?|pytest output|junit|coverage report|qa report|validation report|"
        r"automated test results?|test evidence|test artifact)\b",
        re.I,
    ),
    "approval_record": re.compile(
        r"\b(?:approval record|approval evidence|approved by|approval .{0,40}record(?:ed|s)?|"
        r"sign[- ]?off record|signoff record|record(?:ed|s)? .{0,40}sign[- ]?off|cab record|change approval record|"
        r"go/no[- ]go record|owner approval record|release approval record)\b",
        re.I,
    ),
    "migration_proof": re.compile(
        r"\b(?:migration proof|migration dry[- ]run|migration verification|rollback proof|"
        r"row count reconciliation|data reconciliation|backfill report|schema diff|post[- ]migration check)\b",
        re.I,
    ),
    "rollout_proof": re.compile(
        r"\b(?:rollout proof|canary metrics?|flag ramp evidence|deployment evidence|post[- ]deploy metrics?|"
        r"release evidence|rollout metrics?|rollback evidence|launch verification)\b",
        re.I,
    ),
    "security_review_artifact": re.compile(
        r"\b(?:security review artifact|security review notes?|threat model|risk assessment|pentest report|"
        r"vulnerability scan|security checklist|privacy review artifact|access review evidence)\b",
        re.I,
    ),
}
_RECOMMENDATIONS: dict[AuditEvidenceArtifact, str] = {
    "compliance_evidence": "Attach compliance or control evidence that shows the acceptance criteria were met.",
    "screenshot_evidence": "Capture screenshots or screen recordings for user-visible regulated workflow changes.",
    "log_evidence": "Attach relevant logs or event excerpts proving the changed behavior ran in the target environment.",
    "test_report": "Attach test reports or validation output for the required automated and manual checks.",
    "approval_record": "Record owner, compliance, security, or release approval with approver and timestamp.",
    "migration_proof": "Attach migration dry-run, reconciliation, rollback, or post-migration verification proof.",
    "rollout_proof": "Attach rollout, canary, feature-flag ramp, deployment, or rollback verification proof.",
    "security_review_artifact": "Attach security review, threat model, vulnerability scan, or access review artifacts.",
}
_NO_EVIDENCE_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:audit|compliance|security|migration|rollout|approval|evidence)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskAuditEvidenceReadinessRecord:
    """Evidence readiness guidance for one execution task."""

    task_id: str
    title: str
    evidence_signals: tuple[AuditEvidenceSignal, ...]
    risk_level: AuditEvidenceRiskLevel
    present_evidence_artifacts: tuple[AuditEvidenceArtifact, ...] = field(default_factory=tuple)
    missing_recommended_artifacts: tuple[AuditEvidenceArtifact, ...] = field(default_factory=tuple)
    recommended_evidence_artifacts: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "evidence_signals": list(self.evidence_signals),
            "risk_level": self.risk_level,
            "present_evidence_artifacts": list(self.present_evidence_artifacts),
            "missing_recommended_artifacts": list(self.missing_recommended_artifacts),
            "recommended_evidence_artifacts": list(self.recommended_evidence_artifacts),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskAuditEvidenceReadinessPlan:
    """Plan-level evidence readiness recommendations."""

    plan_id: str | None = None
    records: tuple[TaskAuditEvidenceReadinessRecord, ...] = field(default_factory=tuple)
    evidence_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskAuditEvidenceReadinessRecord, ...]:
        """Compatibility view matching planners that call records recommendations."""
        return self.records

    @property
    def findings(self) -> tuple[TaskAuditEvidenceReadinessRecord, ...]:
        """Compatibility view matching planners that call records findings."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "evidence_task_ids": list(self.evidence_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return evidence readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render evidence readiness as deterministic Markdown."""
        title = "# Task Audit Evidence Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Evidence-relevant task count: {self.summary.get('evidence_task_count', 0)}",
            f"- Missing recommended artifact count: {self.summary.get('missing_recommended_artifact_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No audit evidence readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(
                    ["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Evidence Signals | Present Artifacts | Missing Artifacts | Recommended Evidence | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.evidence_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_evidence_artifacts) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_recommended_artifacts) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_evidence_artifacts) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(
                ["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"]
            )
        return "\n".join(lines)


def build_task_audit_evidence_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditEvidenceReadinessPlan:
    """Build audit evidence readiness records for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _RISK_ORDER[record.risk_level],
                -len(record.missing_recommended_artifacts),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    evidence_task_ids = tuple(record.task_id for record in records)
    evidence_task_id_set = set(evidence_task_ids)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in evidence_task_id_set
    )
    return TaskAuditEvidenceReadinessPlan(
        plan_id=plan_id,
        records=records,
        evidence_task_ids=evidence_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_audit_evidence_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditEvidenceReadinessPlan:
    """Compatibility alias for building audit evidence readiness plans."""
    return build_task_audit_evidence_readiness_plan(source)


def derive_task_audit_evidence_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditEvidenceReadinessPlan:
    """Compatibility alias for deriving audit evidence readiness plans."""
    return build_task_audit_evidence_readiness_plan(source)


def extract_task_audit_evidence_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditEvidenceReadinessPlan:
    """Compatibility alias for extracting audit evidence readiness plans."""
    return build_task_audit_evidence_readiness_plan(source)


def generate_task_audit_evidence_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditEvidenceReadinessPlan:
    """Compatibility alias for generating audit evidence readiness plans."""
    return build_task_audit_evidence_readiness_plan(source)


def recommend_task_audit_evidence_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditEvidenceReadinessPlan:
    """Compatibility alias for recommending audit evidence readiness plans."""
    return build_task_audit_evidence_readiness_plan(source)


def summarize_task_audit_evidence_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditEvidenceReadinessPlan:
    """Compatibility alias for summarizing audit evidence readiness plans."""
    return build_task_audit_evidence_readiness_plan(source)


def task_audit_evidence_readiness_plan_to_dict(
    result: TaskAuditEvidenceReadinessPlan,
) -> dict[str, Any]:
    """Serialize an audit evidence readiness plan to a plain dictionary."""
    return result.to_dict()


task_audit_evidence_readiness_plan_to_dict.__test__ = False


def task_audit_evidence_readiness_plan_to_dicts(
    result: TaskAuditEvidenceReadinessPlan
    | Iterable[TaskAuditEvidenceReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize audit evidence readiness records to plain dictionaries."""
    if isinstance(result, TaskAuditEvidenceReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_audit_evidence_readiness_plan_to_dicts.__test__ = False
task_audit_evidence_readiness_to_dicts = task_audit_evidence_readiness_plan_to_dicts
task_audit_evidence_readiness_to_dicts.__test__ = False


def task_audit_evidence_readiness_plan_to_markdown(
    result: TaskAuditEvidenceReadinessPlan,
) -> str:
    """Render an audit evidence readiness plan as Markdown."""
    return result.to_markdown()


task_audit_evidence_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    evidence_signals: tuple[AuditEvidenceSignal, ...] = field(default_factory=tuple)
    present_artifacts: tuple[AuditEvidenceArtifact, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _record(task: Mapping[str, Any], index: int) -> TaskAuditEvidenceReadinessRecord | None:
    signals = _signals(task)
    signal_set = set(signals.evidence_signals)
    present_set = set(signals.present_artifacts)
    if signals.explicitly_no_impact:
        return None
    if not signal_set and not present_set:
        return None

    missing = _missing_artifacts(signal_set, present_set)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskAuditEvidenceReadinessRecord(
        task_id=task_id,
        title=title,
        evidence_signals=signals.evidence_signals,
        risk_level=_risk_level(signal_set, present_set, missing),
        present_evidence_artifacts=signals.present_artifacts,
        missing_recommended_artifacts=missing,
        recommended_evidence_artifacts=tuple(_RECOMMENDATIONS[artifact] for artifact in missing),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[AuditEvidenceSignal] = set()
    artifact_hits: set[AuditEvidenceArtifact] = set()
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
        for artifact, pattern in _ARTIFACT_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                artifact_hits.add(artifact)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        if _NO_EVIDENCE_IMPACT_RE.search(text):
            explicitly_no_impact = True
        matched = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signal_hits.add(signal)
                matched = True
        for artifact, pattern in _ARTIFACT_PATTERNS.items():
            if pattern.search(text):
                artifact_hits.add(artifact)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        evidence_signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        present_artifacts=tuple(artifact for artifact in _ARTIFACT_ORDER if artifact in artifact_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _missing_artifacts(
    signal_set: set[AuditEvidenceSignal],
    present_set: set[AuditEvidenceArtifact],
) -> tuple[AuditEvidenceArtifact, ...]:
    recommended: set[AuditEvidenceArtifact] = set()
    if "audit_review" in signal_set:
        recommended.update({"compliance_evidence", "log_evidence", "test_report", "approval_record"})
    if "compliance_review" in signal_set:
        recommended.update({"compliance_evidence", "test_report", "approval_record"})
    if "security_review" in signal_set:
        recommended.update({"security_review_artifact", "test_report", "approval_record"})
    if "migration_change" in signal_set:
        recommended.update({"migration_proof", "log_evidence", "test_report", "approval_record"})
    if "rollout_change" in signal_set:
        recommended.update({"rollout_proof", "log_evidence", "test_report"})
    if "approval_gate" in signal_set:
        recommended.add("approval_record")
    if present_set and not recommended:
        recommended.update(present_set)
    return tuple(artifact for artifact in _ARTIFACT_ORDER if artifact in recommended and artifact not in present_set)


def _risk_level(
    signal_set: set[AuditEvidenceSignal],
    present_set: set[AuditEvidenceArtifact],
    missing: tuple[AuditEvidenceArtifact, ...],
) -> AuditEvidenceRiskLevel:
    if signal_set & _SENSITIVE_SIGNALS and not present_set:
        return "high"
    if "migration_change" in signal_set and "migration_proof" in missing:
        return "high"
    if "security_review" in signal_set and "security_review_artifact" in missing:
        return "high"
    if "compliance_review" in signal_set and "compliance_evidence" in missing:
        return "high"
    if missing:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskAuditEvidenceReadinessRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "evidence_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_recommended_artifact_count": sum(
            len(record.missing_recommended_artifacts) for record in records
        ),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk) for risk in _RISK_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.evidence_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_artifact_counts": {
            artifact: sum(1 for record in records if artifact in record.present_evidence_artifacts)
            for artifact in _ARTIFACT_ORDER
        },
        "missing_artifact_counts": {
            artifact: sum(1 for record in records if artifact in record.missing_recommended_artifacts)
            for artifact in _ARTIFACT_ORDER
        },
        "evidence_task_ids": [record.task_id for record in records],
    }


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
        "dependencies",
        "files_or_modules",
        "files",
        "expected_file_paths",
        "expected_files",
        "paths",
        "acceptance_criteria",
        "validation_plan",
        "validation_plans",
        "validation_commands",
        "definition_of_done",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
        "category",
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
        "test_strategy",
        "category",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "validation_plans",
        "validation_commands",
        "definition_of_done",
        "tags",
        "labels",
        "notes",
        "risks",
        "dependencies",
        "depends_on",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        patterns = (*_SIGNAL_PATTERNS.values(), *_ARTIFACT_PATTERNS.values())
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, key_text))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in patterns):
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
    return str(
        PurePosixPath(
            value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
        )
    )


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
    "AuditEvidenceArtifact",
    "AuditEvidenceRiskLevel",
    "AuditEvidenceSignal",
    "TaskAuditEvidenceReadinessPlan",
    "TaskAuditEvidenceReadinessRecord",
    "analyze_task_audit_evidence_readiness",
    "build_task_audit_evidence_readiness_plan",
    "derive_task_audit_evidence_readiness",
    "extract_task_audit_evidence_readiness",
    "generate_task_audit_evidence_readiness",
    "recommend_task_audit_evidence_readiness",
    "summarize_task_audit_evidence_readiness",
    "task_audit_evidence_readiness_plan_to_dict",
    "task_audit_evidence_readiness_plan_to_dicts",
    "task_audit_evidence_readiness_plan_to_markdown",
    "task_audit_evidence_readiness_to_dicts",
]
