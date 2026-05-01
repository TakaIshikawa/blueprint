"""Build compliance evidence matrices from implementation briefs and plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask, ImplementationBrief


ComplianceEvidenceDomain = Literal[
    "soc2",
    "iso27001",
    "gdpr",
    "hipaa",
    "pci",
    "sox",
    "audit_logging",
    "approval_records",
    "data_processing",
    "access_review",
    "change_management",
]
ComplianceEvidenceRiskLevel = Literal["high", "medium", "low"]

_SPACE_RE = re.compile(r"\s+")
_DOMAIN_ORDER: dict[ComplianceEvidenceDomain, int] = {
    "soc2": 0,
    "iso27001": 1,
    "gdpr": 2,
    "hipaa": 3,
    "pci": 4,
    "sox": 5,
    "audit_logging": 6,
    "approval_records": 7,
    "data_processing": 8,
    "access_review": 9,
    "change_management": 10,
}
_DOMAIN_PATTERNS: dict[ComplianceEvidenceDomain, re.Pattern[str]] = {
    "soc2": re.compile(r"\b(?:soc ?2|soc ii|service organization control)\b", re.I),
    "iso27001": re.compile(r"\b(?:iso ?27001|iso/iec 27001|isms)\b", re.I),
    "gdpr": re.compile(
        r"\b(?:gdpr|general data protection regulation|data subject|dsr|data protection impact|dpia)\b",
        re.I,
    ),
    "hipaa": re.compile(
        r"\b(?:hipaa|phi|protected health|patient data|medical records?|healthcare data)\b",
        re.I,
    ),
    "pci": re.compile(
        r"\b(?:pci|pci[- ]?dss|cardholder data|credit card|payment card|pan|checkout)\b",
        re.I,
    ),
    "sox": re.compile(
        r"\b(?:sox|sarbanes[- ]?oxley|financial control|revenue recognition|ledger close|segregation of duties)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit logs?|audit trail|audit event|audit records?|immutable logs?|security event|activity log|compliance log)\b",
        re.I,
    ),
    "approval_records": re.compile(
        r"\b(?:approval records?|approvals?|sign[- ]?off|authorized approver|review approval|attestation)\b",
        re.I,
    ),
    "data_processing": re.compile(
        r"\b(?:data processing|process(?:es|ing)? data|personal data|customer data|etl|pipeline|import|export|processor|subprocessor|analytics data)\b",
        re.I,
    ),
    "access_review": re.compile(
        r"\b(?:access review|user access review|permission review|rbac|role[- ]?based|least privilege|privileged access|access control|authorization)\b",
        re.I,
    ),
    "change_management": re.compile(
        r"\b(?:change management|change control|change request|cab|release approval|deployment approval|change ticket|rollback plan|implementation plan)\b",
        re.I,
    ),
}
_EVIDENCE_KEYWORDS: dict[str, re.Pattern[str]] = {
    "control mapping": re.compile(r"\b(?:control mapping|control id|control objective|soc ?2|iso ?27001|isms)\b", re.I),
    "validation output": re.compile(r"\b(?:test|tests|tested|pytest|validation|validated|verify|verified|qa|checklist|e2e|integration)\b", re.I),
    "audit log sample": re.compile(r"\b(?:audit log sample|audit event|audit record|log excerpt|activity log|security event)\b", re.I),
    "approval record": re.compile(r"\b(?:approval|approved|sign[- ]?off|attestation|review record|approver)\b", re.I),
    "data inventory": re.compile(r"\b(?:data inventory|data map|data flow|processing purpose|lawful basis|dpia|dpa|subprocessor)\b", re.I),
    "access review": re.compile(r"\b(?:access review|permission review|rbac matrix|role matrix|least privilege)\b", re.I),
    "change record": re.compile(r"\b(?:change record|change ticket|change request|cab|release approval|rollback plan)\b", re.I),
    "pci scan": re.compile(r"\b(?:pci scan|asv|cardholder|pan masking|payment card|pci[- ]?dss)\b", re.I),
    "hipaa review": re.compile(r"\b(?:hipaa review|risk analysis|minimum necessary|phi disclosure|business associate)\b", re.I),
    "sox control": re.compile(r"\b(?:sox control|financial control|segregation of duties|reconciliation|ledger approval)\b", re.I),
}


@dataclass(frozen=True, slots=True)
class PlanComplianceEvidenceMatrixRow:
    """One compliance evidence row grouped by domain."""

    domain: ComplianceEvidenceDomain
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    required_evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_evidence: tuple[str, ...] = field(default_factory=tuple)
    evidence_sources: tuple[str, ...] = field(default_factory=tuple)
    risk_level: ComplianceEvidenceRiskLevel = "medium"
    review_owner: str = "compliance"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "domain": self.domain,
            "affected_task_ids": list(self.affected_task_ids),
            "required_evidence": list(self.required_evidence),
            "missing_evidence": list(self.missing_evidence),
            "evidence_sources": list(self.evidence_sources),
            "risk_level": self.risk_level,
            "review_owner": self.review_owner,
        }


@dataclass(frozen=True, slots=True)
class PlanComplianceEvidenceMatrix:
    """Compliance evidence matrix for a brief, plan, or brief-plus-plan pair."""

    brief_id: str | None = None
    plan_id: str | None = None
    rows: tuple[PlanComplianceEvidenceMatrixRow, ...] = field(default_factory=tuple)

    @property
    def summary(self) -> dict[str, Any]:
        """Return stable rollup counts for serializers."""
        risk_counts = {
            risk: sum(1 for row in self.rows if row.risk_level == risk)
            for risk in ("high", "medium", "low")
        }
        domain_counts = {
            domain: sum(1 for row in self.rows if row.domain == domain)
            for domain in _DOMAIN_ORDER
        }
        return {
            "domain_count": len(self.rows),
            "affected_task_count": len(
                {task_id for row in self.rows for task_id in row.affected_task_ids}
            ),
            "required_evidence_count": sum(len(row.required_evidence) for row in self.rows),
            "missing_evidence_count": sum(len(row.missing_evidence) for row in self.rows),
            "risk_counts": risk_counts,
            "domain_counts": domain_counts,
        }

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "plan_id": self.plan_id,
            "summary": self.summary,
            "rows": [row.to_dict() for row in self.rows],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return compliance evidence rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Compliance Evidence Matrix"
        identifiers = ", ".join(
            value
            for value in (
                f"brief {self.brief_id}" if self.brief_id else "",
                f"plan {self.plan_id}" if self.plan_id else "",
            )
            if value
        )
        if identifiers:
            title = f"{title}: {identifiers}"
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary['domain_count']} domains, "
                f"{self.summary['affected_task_count']} affected tasks, "
                f"{self.summary['missing_evidence_count']} missing evidence artifacts."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No compliance evidence rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Domain | Risk | Affected Tasks | Required Evidence | Missing Evidence | Evidence Sources | Review Owner |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.domain} | "
                f"{row.risk_level} | "
                f"{_markdown_cell(', '.join(row.affected_task_ids) or 'brief')} | "
                f"{_markdown_cell('; '.join(row.required_evidence))} | "
                f"{_markdown_cell('; '.join(row.missing_evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence_sources))} | "
                f"{_markdown_cell(row.review_owner)} |"
            )
        return "\n".join(lines)


def build_plan_compliance_evidence_matrix(
    brief: Mapping[str, Any] | ImplementationBrief | ExecutionPlan | None = None,
    plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> PlanComplianceEvidenceMatrix:
    """Build compliance evidence rows from an implementation brief and/or execution plan."""
    brief_payload: dict[str, Any] = {}
    plan_payload: dict[str, Any] = {}

    if plan is None and _looks_like_plan(brief):
        plan_payload = _plan_payload(brief)
    else:
        brief_payload = _brief_payload(brief)
        plan_payload = _plan_payload(plan)

    builders = {domain: _RowBuilder(domain) for domain in _DOMAIN_ORDER}

    for source_field, text in _brief_texts(brief_payload):
        _add_signals(builders, source_field, text, task_id=None)
    for source_field, text in _plan_texts(plan_payload):
        _add_signals(builders, source_field, text, task_id=None)
    for index, task in enumerate(_task_payloads(plan_payload.get("tasks")), start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        for source_field, text in _task_texts(task):
            _add_signals(builders, source_field, text, task_id=task_id)

    rows = tuple(
        _row(builder)
        for builder in sorted(builders.values(), key=lambda item: _DOMAIN_ORDER[item.domain])
        if builder.sources
    )
    return PlanComplianceEvidenceMatrix(
        brief_id=_optional_text(brief_payload.get("id")),
        plan_id=_optional_text(plan_payload.get("id")),
        rows=rows,
    )


def summarize_plan_compliance_evidence(
    brief: Mapping[str, Any] | ImplementationBrief | ExecutionPlan | None = None,
    plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> PlanComplianceEvidenceMatrix:
    """Compatibility alias for building compliance evidence matrices."""
    return build_plan_compliance_evidence_matrix(brief, plan)


def plan_compliance_evidence_matrix_to_dict(
    matrix: PlanComplianceEvidenceMatrix,
) -> dict[str, Any]:
    """Serialize a compliance evidence matrix to a plain dictionary."""
    return matrix.to_dict()


plan_compliance_evidence_matrix_to_dict.__test__ = False


def plan_compliance_evidence_matrix_to_dicts(
    matrix: PlanComplianceEvidenceMatrix,
) -> list[dict[str, Any]]:
    """Serialize compliance evidence matrix rows to plain dictionaries."""
    return matrix.to_dicts()


plan_compliance_evidence_matrix_to_dicts.__test__ = False


def plan_compliance_evidence_matrix_to_markdown(
    matrix: PlanComplianceEvidenceMatrix,
) -> str:
    """Render a compliance evidence matrix as Markdown."""
    return matrix.to_markdown()


plan_compliance_evidence_matrix_to_markdown.__test__ = False


@dataclass(slots=True)
class _RowBuilder:
    domain: ComplianceEvidenceDomain
    task_ids: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    source_texts: list[str] = field(default_factory=list)


def _row(builder: _RowBuilder) -> PlanComplianceEvidenceMatrixRow:
    required = _required_evidence(builder.domain)
    source_text = " ".join(builder.source_texts)
    missing = tuple(item for item in required if not _evidence_present(item, source_text))
    affected_task_ids = tuple(_dedupe(builder.task_ids))
    return PlanComplianceEvidenceMatrixRow(
        domain=builder.domain,
        affected_task_ids=affected_task_ids,
        required_evidence=required,
        missing_evidence=missing,
        evidence_sources=tuple(_dedupe(builder.sources)),
        risk_level=_risk_level(builder.domain, missing, affected_task_ids),
        review_owner=_review_owner(builder.domain),
    )


def _add_signals(
    builders: dict[ComplianceEvidenceDomain, _RowBuilder],
    source_field: str,
    text: str,
    *,
    task_id: str | None,
) -> None:
    snippet = _evidence(source_field, text)
    for domain, pattern in _DOMAIN_PATTERNS.items():
        if not pattern.search(text):
            continue
        builder = builders[domain]
        if task_id:
            builder.task_ids.append(task_id)
        builder.sources.append(snippet)
        builder.source_texts.append(text)


def _required_evidence(domain: ComplianceEvidenceDomain) -> tuple[str, ...]:
    requirements: dict[ComplianceEvidenceDomain, tuple[str, ...]] = {
        "soc2": (
            "control mapping",
            "validation output",
            "approval record",
        ),
        "iso27001": (
            "control mapping",
            "risk treatment or ISMS control review",
            "approval record",
        ),
        "gdpr": (
            "data inventory",
            "lawful basis or DPIA review",
            "validation output",
        ),
        "hipaa": (
            "hipaa review",
            "audit log sample",
            "validation output",
        ),
        "pci": (
            "pci scan",
            "data inventory",
            "validation output",
        ),
        "sox": (
            "sox control",
            "approval record",
            "validation output",
        ),
        "audit_logging": (
            "audit log sample",
            "validation output",
        ),
        "approval_records": (
            "approval record",
            "change record",
        ),
        "data_processing": (
            "data inventory",
            "validation output",
        ),
        "access_review": (
            "access review",
            "validation output",
            "approval record",
        ),
        "change_management": (
            "change record",
            "validation output",
            "approval record",
        ),
    }
    return requirements[domain]


def _evidence_present(requirement: str, text: str) -> bool:
    if requirement == "risk treatment or ISMS control review":
        return bool(re.search(r"\b(?:risk treatment|isms|iso ?27001|control review)\b", text, re.I))
    if requirement == "lawful basis or DPIA review":
        return bool(re.search(r"\b(?:lawful basis|dpia|data protection impact|dpo review)\b", text, re.I))
    pattern = _EVIDENCE_KEYWORDS.get(requirement)
    return bool(pattern and pattern.search(text))


def _risk_level(
    domain: ComplianceEvidenceDomain,
    missing: tuple[str, ...],
    affected_task_ids: tuple[str, ...],
) -> ComplianceEvidenceRiskLevel:
    if not missing:
        return "low"
    if domain in {"gdpr", "hipaa", "pci", "sox"} or len(missing) >= 2:
        return "high"
    if affected_task_ids:
        return "medium"
    return "low"


def _review_owner(domain: ComplianceEvidenceDomain) -> str:
    owners: dict[ComplianceEvidenceDomain, str] = {
        "soc2": "security compliance",
        "iso27001": "security compliance",
        "gdpr": "privacy legal",
        "hipaa": "privacy security",
        "pci": "payments security",
        "sox": "finance controls",
        "audit_logging": "security engineering",
        "approval_records": "program owner",
        "data_processing": "data governance",
        "access_review": "identity security",
        "change_management": "release management",
    }
    return owners[domain]


def _brief_texts(brief: Mapping[str, Any]) -> list[tuple[str, str]]:
    if not brief:
        return []
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "domain",
        "target_user",
        "buyer",
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "product_surface",
        "architecture_notes",
        "data_requirements",
        "validation_plan",
        "generation_prompt",
    ):
        if text := _optional_text(brief.get(field_name)):
            texts.append((f"brief.{field_name}", text))
    for field_name in (
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "risks",
        "definition_of_done",
    ):
        for index, text in enumerate(_strings(brief.get(field_name))):
            texts.append((f"brief.{field_name}[{index}]", text))
    return texts


def _plan_texts(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    if not plan:
        return []
    texts: list[tuple[str, str]] = []
    for field_name in ("target_repo", "project_type", "test_strategy", "handoff_prompt"):
        if text := _optional_text(plan.get(field_name)):
            texts.append((f"plan.{field_name}", text))
    for source_field, text in _metadata_texts(plan.get("metadata"), prefix="plan.metadata"):
        texts.append((source_field, text))
    return texts


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    task_id = _optional_text(task.get("id")) or "task"
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
            texts.append((f"task.{task_id}.{field_name}", text))
    for field_name in ("files_or_modules", "files", "acceptance_criteria", "tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"task.{task_id}.{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(
        task.get("metadata"),
        prefix=f"task.{task_id}.metadata",
    ):
        texts.append((source_field, text))
    return texts


def _brief_payload(value: Mapping[str, Any] | ImplementationBrief | None) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, ImplementationBrief):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump") and not isinstance(value, ExecutionPlan):
        dumped = value.model_dump(mode="python")
        return dict(dumped) if isinstance(dumped, Mapping) else {}
    if isinstance(value, Mapping):
        try:
            dumped = ImplementationBrief.model_validate(value).model_dump(mode="python")
            return dict(dumped) if isinstance(dumped, Mapping) else {}
        except (TypeError, ValueError, ValidationError):
            return dict(value)
    return {}


def _plan_payload(value: Mapping[str, Any] | ExecutionPlan | None) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, ExecutionPlan):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        dumped = value.model_dump(mode="python")
        return dict(dumped) if isinstance(dumped, Mapping) else {}
    if isinstance(value, Mapping):
        try:
            dumped = ExecutionPlan.model_validate(value).model_dump(mode="python")
            return dict(dumped) if isinstance(dumped, Mapping) else {}
        except (TypeError, ValueError, ValidationError):
            return dict(value)
    return {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            dumped = item.model_dump(mode="python")
            if isinstance(dumped, Mapping):
                tasks.append(dict(dumped))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _looks_like_plan(value: Any) -> bool:
    if isinstance(value, ExecutionPlan):
        return True
    if isinstance(value, ImplementationBrief) or value is None:
        return False
    if isinstance(value, Mapping):
        return "tasks" in value or "implementation_brief_id" in value
    return hasattr(value, "tasks")


def _metadata_texts(value: Any, *, prefix: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            texts.extend(_metadata_texts(value[key], prefix=f"{prefix}.{key}"))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            texts.extend(_metadata_texts(item, prefix=f"{prefix}[{index}]"))
        return texts
    if text := _optional_text(value):
        return [(prefix, text)]
    return []


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


def _evidence(source_field: str, text: str) -> str:
    return f"{source_field}: {_clean_sentence(text)}"


def _clean_sentence(value: str) -> str:
    return _SPACE_RE.sub(" ", str(value)).strip().strip("`'\",;:()[]{}").rstrip(".")


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Any) -> tuple[Any, ...]:
    seen: set[Any] = set()
    result: list[Any] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return tuple(result)


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "ComplianceEvidenceDomain",
    "ComplianceEvidenceRiskLevel",
    "PlanComplianceEvidenceMatrix",
    "PlanComplianceEvidenceMatrixRow",
    "build_plan_compliance_evidence_matrix",
    "plan_compliance_evidence_matrix_to_dict",
    "plan_compliance_evidence_matrix_to_dicts",
    "plan_compliance_evidence_matrix_to_markdown",
    "summarize_plan_compliance_evidence",
]
