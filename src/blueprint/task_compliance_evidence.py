"""Plan compliance evidence requirements for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ComplianceEvidenceDomain = Literal[
    "audit_logs",
    "data_processing",
    "access_control",
    "financial_workflows",
    "healthcare_pii",
    "retention",
    "encryption",
    "consent",
    "policy_changes",
]
ComplianceEvidenceSeverity = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_VALIDATION_RE = re.compile(
    r"\b(?:test|tests|tested|verify|verifies|verified|validate|validates|"
    r"validation|validated|assert|acceptance|qa|check|checks|review|"
    r"reviewed|screenshot|pytest|unit|integration|e2e)\b",
    re.IGNORECASE,
)
_AUDIT_PROOF_RE = re.compile(
    r"\b(?:audit|log|logged|logging|trace|evidence|record|records|review|"
    r"approv(?:e|al|ed)|sign[- ]?off)\b",
    re.IGNORECASE,
)
_SIGNAL_PATTERNS: dict[ComplianceEvidenceDomain, re.Pattern[str]] = {
    "audit_logs": re.compile(
        r"\b(?:audit logs?|audit trail|audit event|security event|compliance event|"
        r"activity log|immutable log|tamper[- ]?evident)\b",
        re.IGNORECASE,
    ),
    "data_processing": re.compile(
        r"\b(?:data processing|process(?:es|ing)? data|etl|pipeline|imports?|exports?|"
        r"transform|sync|replication|analytics|tracking|reporting data)\b",
        re.IGNORECASE,
    ),
    "access_control": re.compile(
        r"\b(?:auth|authentication|authorization|permission|permissions|rbac|"
        r"role[- ]?based|access control|login|session|sso|oauth|jwt|acl)\b",
        re.IGNORECASE,
    ),
    "financial_workflows": re.compile(
        r"\b(?:billing|invoice|invoices|payment|payments|refund|chargeback|"
        r"subscription|tax|revenue|ledger|payout|checkout|credit card)\b",
        re.IGNORECASE,
    ),
    "healthcare_pii": re.compile(
        r"\b(?:pii|personal data|personally identifiable|phi|hipaa|healthcare|"
        r"medical|patient|email addresses?|phone numbers?|ssn|social security|"
        r"date of birth|birthdate|customer data|user data|profile data)\b",
        re.IGNORECASE,
    ),
    "retention": re.compile(
        r"\b(?:retention|retain|retained|purge|delete|deletion|ttl|expiration|"
        r"expire|archive|backup|restore|data lifecycle)\b",
        re.IGNORECASE,
    ),
    "encryption": re.compile(
        r"\b(?:encryption|encrypt|encrypted|decrypt|kms|key rotation|secret|"
        r"secrets|certificate|tls|ssl|hash|hashed|tokenization)\b",
        re.IGNORECASE,
    ),
    "consent": re.compile(
        r"\b(?:consent|opt[- ]?in|opt[- ]?out|unsubscribe|preference center|"
        r"privacy choice|cookie banner|marketing preference)\b",
        re.IGNORECASE,
    ),
    "policy_changes": re.compile(
        r"\b(?:policy|policies|compliance rule|governance|terms of service|"
        r"privacy notice|privacy policy|procedure|standard|control)\b",
        re.IGNORECASE,
    ),
}
_DOMAIN_ORDER: dict[ComplianceEvidenceDomain, int] = {
    "healthcare_pii": 0,
    "access_control": 1,
    "encryption": 2,
    "financial_workflows": 3,
    "audit_logs": 4,
    "retention": 5,
    "consent": 6,
    "data_processing": 7,
    "policy_changes": 8,
}


@dataclass(frozen=True, slots=True)
class TaskComplianceEvidenceRecord:
    """Evidence planning guidance for one execution task."""

    task_id: str
    task_title: str
    severity: ComplianceEvidenceSeverity
    compliance_domains: tuple[ComplianceEvidenceDomain, ...]
    evidence_requirements: tuple[str, ...]
    missing_acceptance_criteria: tuple[str, ...]
    validation_hints: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "severity": self.severity,
            "compliance_domains": list(self.compliance_domains),
            "evidence_requirements": list(self.evidence_requirements),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "validation_hints": list(self.validation_hints),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskComplianceEvidencePlan:
    """Plan-level compliance evidence guidance and rollup counts."""

    plan_id: str | None = None
    records: tuple[TaskComplianceEvidenceRecord, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return evidence records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the evidence plan as deterministic Markdown."""
        title = "# Task Compliance Evidence"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No execution tasks were found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Severity | Domains | Evidence Requirements | Missing Acceptance Criteria | Validation Hints |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{record.severity} | "
                f"{_markdown_cell(', '.join(record.compliance_domains))} | "
                f"{_markdown_cell('; '.join(record.evidence_requirements))} | "
                f"{_markdown_cell('; '.join(record.missing_acceptance_criteria))} | "
                f"{_markdown_cell('; '.join(record.validation_hints))} |"
            )
        return "\n".join(lines)


def build_task_compliance_evidence_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskComplianceEvidencePlan:
    """Build compliance evidence guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (_task_record(task, index) for index, task in enumerate(tasks, start=1)),
            key=lambda record: (record.task_id, record.task_title),
        )
    )
    severity_counts = {
        severity: sum(1 for record in records if record.severity == severity)
        for severity in ("high", "medium", "low")
    }
    domain_counts = {
        domain: sum(1 for record in records if domain in record.compliance_domains)
        for domain in _DOMAIN_ORDER
    }
    return TaskComplianceEvidencePlan(
        plan_id=plan_id,
        records=records,
        summary={
            "record_count": len(records),
            "sensitive_task_count": sum(
                1
                for record in records
                if record.compliance_domains != ("policy_changes",) or record.severity != "low"
            ),
            "missing_acceptance_criteria_count": sum(
                len(record.missing_acceptance_criteria) for record in records
            ),
            "severity_counts": severity_counts,
            "domain_counts": domain_counts,
        },
    )


def task_compliance_evidence_plan_to_dict(
    result: TaskComplianceEvidencePlan,
) -> dict[str, Any]:
    """Serialize a compliance evidence plan to a plain dictionary."""
    return result.to_dict()


task_compliance_evidence_plan_to_dict.__test__ = False


def task_compliance_evidence_plan_to_markdown(
    result: TaskComplianceEvidencePlan,
) -> str:
    """Render a compliance evidence plan as Markdown."""
    return result.to_markdown()


task_compliance_evidence_plan_to_markdown.__test__ = False


def summarize_task_compliance_evidence(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskComplianceEvidencePlan:
    """Compatibility alias for building task compliance evidence plans."""
    return build_task_compliance_evidence_plan(source)


def _task_record(task: Mapping[str, Any], index: int) -> TaskComplianceEvidenceRecord:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    domains = tuple(sorted(signals, key=lambda domain: _DOMAIN_ORDER[domain]))
    if not domains:
        domains = ("policy_changes",)

    severity = _severity(domains, bool(signals))
    requirements = tuple(_dedupe(_evidence_requirements(domains, bool(signals))))
    missing = tuple(_missing_acceptance_criteria(task, domains, bool(signals)))
    hints = tuple(_dedupe(_validation_hints(domains, bool(signals))))
    evidence = tuple(_dedupe(item for domain in domains for item in signals.get(domain, ())))

    return TaskComplianceEvidenceRecord(
        task_id=task_id,
        task_title=title,
        severity=severity,
        compliance_domains=domains,
        evidence_requirements=requirements,
        missing_acceptance_criteria=missing,
        validation_hints=hints,
        evidence=evidence,
    )


def _signals(task: Mapping[str, Any]) -> dict[ComplianceEvidenceDomain, tuple[str, ...]]:
    signals: dict[ComplianceEvidenceDomain, list[str]] = {}

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _add_path_signals(signals, path)

    for source_field, text in _candidate_texts(task):
        for domain, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signals.setdefault(domain, []).append(f"{source_field}: {text}")

    return {domain: tuple(_dedupe(evidence)) for domain, evidence in signals.items() if evidence}


def _add_path_signals(
    signals: dict[ComplianceEvidenceDomain, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    evidence = f"files_or_modules: {original}"

    if bool({"audit", "audits", "logs", "logging", "events"} & parts) or "audit" in name:
        signals.setdefault("audit_logs", []).append(evidence)
    if bool({"data", "etl", "pipelines", "analytics", "exports", "imports"} & parts):
        signals.setdefault("data_processing", []).append(evidence)
    if "export" in name or "import" in name:
        signals.setdefault("data_processing", []).append(evidence)
    if bool({"auth", "authentication", "authorization", "permissions", "rbac"} & parts):
        signals.setdefault("access_control", []).append(evidence)
    if bool({"billing", "payments", "invoices", "ledger", "checkout"} & parts):
        signals.setdefault("financial_workflows", []).append(evidence)
    if bool({"pii", "phi", "patients", "healthcare", "customers", "users"} & parts):
        signals.setdefault("healthcare_pii", []).append(evidence)
    if bool({"retention", "backups", "archive", "ttl", "deletion"} & parts):
        signals.setdefault("retention", []).append(evidence)
    if bool({"encryption", "kms", "secrets", "certificates", "tls"} & parts):
        signals.setdefault("encryption", []).append(evidence)
    if bool({"consent", "privacy", "preferences"} & parts) or "consent" in name:
        signals.setdefault("consent", []).append(evidence)
    if bool({"policies", "policy", "compliance", "governance"} & parts):
        signals.setdefault("policy_changes", []).append(evidence)


def _severity(
    domains: tuple[ComplianceEvidenceDomain, ...],
    has_sensitive_signal: bool,
) -> ComplianceEvidenceSeverity:
    if not has_sensitive_signal:
        return "low"
    if any(
        domain in domains
        for domain in (
            "healthcare_pii",
            "access_control",
            "encryption",
            "financial_workflows",
        )
    ):
        return "high"
    return "medium"


def _evidence_requirements(
    domains: tuple[ComplianceEvidenceDomain, ...],
    has_sensitive_signal: bool,
) -> list[str]:
    if not has_sensitive_signal:
        return [
            "Leave validation output showing the intended behavior still works.",
            "Document why no regulated data, access, billing, retention, or policy evidence is required.",
        ]

    requirements: list[str] = []
    for domain in domains:
        requirements.extend(_DOMAIN_REQUIREMENTS[domain])
    requirements.append("Link the evidence to the task id and changed files or modules.")
    return requirements


_DOMAIN_REQUIREMENTS: dict[ComplianceEvidenceDomain, tuple[str, ...]] = {
    "audit_logs": (
        "Capture before and after audit event examples with actor, action, target, and timestamp fields.",
        "Show audit records are reviewable and protected from unauthorized edits.",
    ),
    "data_processing": (
        "Record source, transformation, destination, and reconciliation evidence for processed data.",
        "Provide sample validation proving derived data matches documented rules.",
    ),
    "access_control": (
        "Capture authorization matrix or role coverage for allowed and denied paths.",
        "Provide negative access tests for unauthorized users or roles.",
    ),
    "financial_workflows": (
        "Capture ledger, invoice, payment, refund, or reconciliation evidence for the changed workflow.",
        "Show totals, currency, idempotency, and rollback behavior are validated.",
    ),
    "healthcare_pii": (
        "Document touched personal or protected data fields and the minimum necessary exposure.",
        "Capture privacy review evidence for masking, redaction, access, and deletion behavior.",
    ),
    "retention": (
        "Document retention period, purge trigger, owner, and deletion verification evidence.",
        "Show retained artifacts, backups, caches, and exports are handled consistently.",
    ),
    "encryption": (
        "Record encryption boundaries, key ownership, rotation impact, and secret handling evidence.",
        "Show plaintext is not logged, stored, exported, or exposed outside approved boundaries.",
    ),
    "consent": (
        "Capture consent state transitions and proof that opt-in or opt-out choices are honored.",
        "Show downstream processing stops or changes when consent is withdrawn.",
    ),
    "policy_changes": (
        "Attach policy, control, procedure, or governance review evidence for the changed rule.",
        "Document rollout, exception handling, and approval owner for the policy change.",
    ),
}


def _missing_acceptance_criteria(
    task: Mapping[str, Any],
    domains: tuple[ComplianceEvidenceDomain, ...],
    has_sensitive_signal: bool,
) -> list[str]:
    criteria_text = " ".join(_strings(task.get("acceptance_criteria")))
    missing: list[str] = []
    if not has_sensitive_signal:
        if not _VALIDATION_RE.search(criteria_text):
            missing.append("Add acceptance criteria that names the focused validation to run.")
        return missing

    if not _VALIDATION_RE.search(criteria_text):
        missing.append(
            "Add acceptance criteria requiring validation evidence for the compliance-sensitive path."
        )
    if any(
        domain in domains
        for domain in (
            "audit_logs",
            "access_control",
            "financial_workflows",
            "healthcare_pii",
            "retention",
        )
    ) and not _AUDIT_PROOF_RE.search(criteria_text):
        missing.append(
            "Add acceptance criteria requiring audit or reviewable evidence for the changed compliance control."
        )
    if "encryption" in domains and not re.search(
        r"\b(?:plaintext|secret|key|encrypt|decrypt|rotation)\b", criteria_text, re.IGNORECASE
    ):
        missing.append(
            "Add acceptance criteria proving secrets, keys, and plaintext exposure are controlled."
        )
    if "consent" in domains and not re.search(
        r"\b(?:consent|opt[- ]?in|opt[- ]?out|withdraw)\b", criteria_text, re.IGNORECASE
    ):
        missing.append(
            "Add acceptance criteria proving consent choices are honored and reversible."
        )
    return missing


def _validation_hints(
    domains: tuple[ComplianceEvidenceDomain, ...],
    has_sensitive_signal: bool,
) -> list[str]:
    if not has_sensitive_signal:
        return [
            "Run the smallest relevant automated or manual check and keep the command or checklist result.",
        ]

    hints = ["Keep command output, screenshots, fixtures, or review links with stable identifiers."]
    if "access_control" in domains:
        hints.append("Exercise at least one allowed role and one denied role.")
    if "audit_logs" in domains:
        hints.append("Verify audit records contain actor, action, target, result, and timestamp.")
    if "financial_workflows" in domains:
        hints.append("Validate rounding, idempotency, reconciliation, and failure handling.")
    if "healthcare_pii" in domains:
        hints.append("Verify masking, minimization, deletion, and unauthorized access behavior.")
    if "retention" in domains:
        hints.append(
            "Validate purge, TTL, export, cache, and backup expectations where applicable."
        )
    if "encryption" in domains:
        hints.append(
            "Check encrypted-at-rest or in-transit paths and confirm plaintext is not emitted."
        )
    if "consent" in domains:
        hints.append("Test opt-in, opt-out, withdrawal, and downstream propagation behavior.")
    if "policy_changes" in domains:
        hints.append("Capture approval owner and effective-date evidence for the policy change.")
    if "data_processing" in domains:
        hints.append("Compare input, output, and exception samples for processing correctness.")
    return hints


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
    for field_name in ("acceptance_criteria", "tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
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

    try:
        iterator = iter(source)
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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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
    "ComplianceEvidenceDomain",
    "ComplianceEvidenceSeverity",
    "TaskComplianceEvidencePlan",
    "TaskComplianceEvidenceRecord",
    "build_task_compliance_evidence_plan",
    "summarize_task_compliance_evidence",
    "task_compliance_evidence_plan_to_dict",
    "task_compliance_evidence_plan_to_markdown",
]
