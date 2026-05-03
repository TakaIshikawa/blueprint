"""Build operational handoff readiness matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


OperationalHandoffDomain = Literal[
    "support_playbook",
    "oncall_runbook",
    "customer_success_briefing",
    "finance_operations",
    "security_operations",
    "data_operations",
    "vendor_operations",
]
OperationalHandoffArtifact = Literal[
    "support_playbook",
    "oncall_runbook",
    "customer_success_briefing",
    "finance_operations_runbook",
    "security_operations_runbook",
    "data_operations_runbook",
    "vendor_operations_handoff",
    "escalation_path",
    "operational_owner",
]
OperationalHandoffReadiness = Literal["ready", "partial", "blocked"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_DOMAIN_ORDER: tuple[OperationalHandoffDomain, ...] = (
    "support_playbook",
    "oncall_runbook",
    "customer_success_briefing",
    "finance_operations",
    "security_operations",
    "data_operations",
    "vendor_operations",
)
_ARTIFACT_ORDER: tuple[OperationalHandoffArtifact, ...] = (
    "support_playbook",
    "oncall_runbook",
    "customer_success_briefing",
    "finance_operations_runbook",
    "security_operations_runbook",
    "data_operations_runbook",
    "vendor_operations_handoff",
    "escalation_path",
    "operational_owner",
)
_READINESS_ORDER: dict[OperationalHandoffReadiness, int] = {
    "blocked": 0,
    "partial": 1,
    "ready": 2,
}

_DOMAIN_PATTERNS: dict[OperationalHandoffDomain, re.Pattern[str]] = {
    "support_playbook": re.compile(
        r"\b(?:support|support agents?|helpdesk|ticket(?:ing)?|zendesk|intercom|"
        r"support playbook|support docs?|customer support|triage)\b",
        re.I,
    ),
    "oncall_runbook": re.compile(
        r"\b(?:on[- ]?call|pager|pagerduty|opsgenie|incident|sev[ -]?[0123]|"
        r"rollback|launch watch|watch window|production operations?)\b",
        re.I,
    ),
    "customer_success_briefing": re.compile(
        r"\b(?:customer success|cs briefing|csm|account manager|customer briefing|"
        r"customer communication|release notes?|enablement|launch briefing)\b",
        re.I,
    ),
    "finance_operations": re.compile(
        r"\b(?:finance|finops|billing ops|billing operations|invoice|invoices|"
        r"payment|payments|revenue|refund|tax|credits?|reconciliation|month[- ]end)\b",
        re.I,
    ),
    "security_operations": re.compile(
        r"\b(?:security operations|secops|security|soc|siem|vulnerability|"
        r"audit|compliance|iam|key rotation|access review|threat)\b",
        re.I,
    ),
    "data_operations": re.compile(
        r"\b(?:data operations|data ops|data platform|analytics|warehouse|etl|pipeline|"
        r"backfill|migration|data quality|reconcile|reconciliation|dashboard)\b",
        re.I,
    ),
    "vendor_operations": re.compile(
        r"\b(?:vendor|third[- ]party|partner|external provider|saas provider|contractor|"
        r"stripe|salesforce|zendesk|intercom|snowflake|datadog)\b",
        re.I,
    ),
}
_PATH_PATTERNS: dict[OperationalHandoffDomain, re.Pattern[str]] = {
    "support_playbook": re.compile(r"(?:^|/)(?:support|helpdesk|support-playbooks?)(?:/|$)", re.I),
    "oncall_runbook": re.compile(r"(?:^|/)(?:ops|runbooks?|incidents?|monitoring|alerts?)(?:/|$)", re.I),
    "customer_success_briefing": re.compile(r"(?:^|/)(?:customer-success|cs|release-notes|comms)(?:/|$)", re.I),
    "finance_operations": re.compile(r"(?:^|/)(?:billing|finance|payments?|invoices?|revenue)(?:/|$)", re.I),
    "security_operations": re.compile(r"(?:^|/)(?:security|secops|iam|audit|compliance)(?:/|$)", re.I),
    "data_operations": re.compile(r"(?:^|/)(?:data|analytics|warehouse|etl|pipelines?|migrations?)(?:/|$)", re.I),
    "vendor_operations": re.compile(r"(?:^|/)(?:vendors?|partners?|integrations?)(?:/|$)", re.I),
}
_ARTIFACT_PATTERNS: dict[OperationalHandoffArtifact, re.Pattern[str]] = {
    "support_playbook": re.compile(r"\b(?:support playbook|support docs?|support guide|kb article|troubleshooting guide)\b", re.I),
    "oncall_runbook": re.compile(r"\b(?:on[- ]?call runbook|ops runbook|operational runbook|incident runbook|rollback runbook|launch watch)\b", re.I),
    "customer_success_briefing": re.compile(r"\b(?:customer success briefing|cs briefing|csm briefing|customer briefing|release notes?|customer comms?)\b", re.I),
    "finance_operations_runbook": re.compile(r"\b(?:finance operations runbook|finops runbook|billing ops runbook|invoice runbook|reconciliation runbook)\b", re.I),
    "security_operations_runbook": re.compile(r"\b(?:security operations runbook|secops runbook|soc runbook|security runbook|incident response runbook)\b", re.I),
    "data_operations_runbook": re.compile(r"\b(?:data operations runbook|data ops runbook|data runbook|pipeline runbook|backfill runbook|reconciliation guide)\b", re.I),
    "vendor_operations_handoff": re.compile(r"\b(?:vendor handoff|vendor operations handoff|partner handoff|third[- ]party handoff|external provider handoff)\b", re.I),
    "escalation_path": re.compile(r"\b(?:escalation path|escalation route|escalate to|tier 2|tier two|on[- ]?call route|support route)\b", re.I),
    "operational_owner": re.compile(r"\b(?:operational owner|handoff owner|support owner|ops owner|dri|responsible owner|runbook owner)\b", re.I),
}
_OWNER_KEY_RE = re.compile(r"\b(?:owner|dri|responsible|assignee|team|lead|oncall|on-call|queue)\b", re.I)
_ROLLOUT_RE = re.compile(r"\b(?:rollout|roll out|launch|release|deploy|deployment|cutover|go[-/ ]live|migration)\b", re.I)
_VALIDATION_RE = re.compile(r"\b(?:pytest|test|validate|validation|smoke|synthetic|monitor|dashboard|alert|check)\b", re.I)

_DOMAIN_ARTIFACTS: dict[OperationalHandoffDomain, tuple[OperationalHandoffArtifact, ...]] = {
    "support_playbook": ("support_playbook", "escalation_path", "operational_owner"),
    "oncall_runbook": ("oncall_runbook", "escalation_path", "operational_owner"),
    "customer_success_briefing": ("customer_success_briefing", "escalation_path", "operational_owner"),
    "finance_operations": ("finance_operations_runbook", "escalation_path", "operational_owner"),
    "security_operations": ("security_operations_runbook", "escalation_path", "operational_owner"),
    "data_operations": ("data_operations_runbook", "escalation_path", "operational_owner"),
    "vendor_operations": ("vendor_operations_handoff", "escalation_path", "operational_owner"),
}
_DEFAULT_OWNER_BY_DOMAIN: dict[OperationalHandoffDomain, str] = {
    "support_playbook": "Support operations",
    "oncall_runbook": "Operations on-call",
    "customer_success_briefing": "Customer Success",
    "finance_operations": "Finance Operations",
    "security_operations": "Security Operations",
    "data_operations": "Data Operations",
    "vendor_operations": "Vendor Operations",
}


@dataclass(frozen=True, slots=True)
class PlanOperationalHandoffReadinessRow:
    """One task-level operational handoff readiness row."""

    task_id: str
    title: str
    handoff_domains: tuple[OperationalHandoffDomain, ...]
    required_artifacts: tuple[OperationalHandoffArtifact, ...] = field(default_factory=tuple)
    owner_suggestions: tuple[str, ...] = field(default_factory=tuple)
    missing_artifacts: tuple[OperationalHandoffArtifact, ...] = field(default_factory=tuple)
    readiness_level: OperationalHandoffReadiness = "partial"
    handoff_timing: str = "before rollout"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def present_artifacts(self) -> tuple[OperationalHandoffArtifact, ...]:
        """Return required handoff artifacts already evidenced by the task."""
        missing = set(self.missing_artifacts)
        return tuple(artifact for artifact in self.required_artifacts if artifact not in missing)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "handoff_domains": list(self.handoff_domains),
            "required_artifacts": list(self.required_artifacts),
            "present_artifacts": list(self.present_artifacts),
            "owner_suggestions": list(self.owner_suggestions),
            "missing_artifacts": list(self.missing_artifacts),
            "readiness_level": self.readiness_level,
            "handoff_timing": self.handoff_timing,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanOperationalHandoffReadinessMatrix:
    """Plan-level operational handoff readiness matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanOperationalHandoffReadinessRow, ...] = field(default_factory=tuple)
    no_handoff_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanOperationalHandoffReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "no_handoff_task_ids": list(self.no_handoff_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return operational handoff rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Operational Handoff Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('handoff_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks require operational handoff "
                f"(blocked: {readiness_counts.get('blocked', 0)}, "
                f"partial: {readiness_counts.get('partial', 0)}, "
                f"ready: {readiness_counts.get('ready', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No operational handoff readiness rows were inferred."])
            if self.no_handoff_task_ids:
                lines.extend(["", f"No handoff signals: {_markdown_cell(', '.join(self.no_handoff_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Task | Title | Domains | Required Artifacts | Present Artifacts | "
                    "Missing Artifacts | Owners | Readiness | Timing | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.title)} | "
                f"{_markdown_cell(', '.join(row.handoff_domains))} | "
                f"{_markdown_cell(', '.join(row.required_artifacts) or 'none')} | "
                f"{_markdown_cell(', '.join(row.present_artifacts) or 'none')} | "
                f"{_markdown_cell(', '.join(row.missing_artifacts) or 'none')} | "
                f"{_markdown_cell('; '.join(row.owner_suggestions) or 'none')} | "
                f"{row.readiness_level} | "
                f"{_markdown_cell(row.handoff_timing)} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.no_handoff_task_ids:
            lines.extend(["", f"No handoff signals: {_markdown_cell(', '.join(self.no_handoff_task_ids))}"])
        return "\n".join(lines)


def build_plan_operational_handoff_readiness_matrix(source: Any) -> PlanOperationalHandoffReadinessMatrix:
    """Build task-level operational handoff readiness for an execution plan."""
    plan_id, plan_context, tasks = _source_payload(source)
    rows: list[PlanOperationalHandoffReadinessRow] = []
    no_handoff_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index, plan_context)
        if row:
            rows.append(row)
        else:
            no_handoff_task_ids.append(_task_id(task, index))

    rows.sort(
        key=lambda row: (
            _READINESS_ORDER[row.readiness_level],
            -len(row.missing_artifacts),
            _domain_index(row.handoff_domains),
            row.task_id,
        )
    )
    result = tuple(rows)
    return PlanOperationalHandoffReadinessMatrix(
        plan_id=plan_id,
        rows=result,
        no_handoff_task_ids=tuple(no_handoff_task_ids),
        summary=_summary(len(tasks), result),
    )


def generate_plan_operational_handoff_readiness_matrix(source: Any) -> PlanOperationalHandoffReadinessMatrix:
    """Generate an operational handoff readiness matrix from a plan-like source."""
    return build_plan_operational_handoff_readiness_matrix(source)


def analyze_plan_operational_handoff_readiness_matrix(source: Any) -> PlanOperationalHandoffReadinessMatrix:
    """Analyze an execution plan for operational handoff readiness."""
    if isinstance(source, PlanOperationalHandoffReadinessMatrix):
        return source
    return build_plan_operational_handoff_readiness_matrix(source)


def derive_plan_operational_handoff_readiness_matrix(source: Any) -> PlanOperationalHandoffReadinessMatrix:
    """Derive an operational handoff readiness matrix from a plan-like source."""
    return analyze_plan_operational_handoff_readiness_matrix(source)


def extract_plan_operational_handoff_readiness_matrix(source: Any) -> PlanOperationalHandoffReadinessMatrix:
    """Extract an operational handoff readiness matrix from a plan-like source."""
    return derive_plan_operational_handoff_readiness_matrix(source)


def summarize_plan_operational_handoff_readiness_matrix(
    matrix: PlanOperationalHandoffReadinessMatrix | Iterable[PlanOperationalHandoffReadinessRow] | Any,
) -> dict[str, Any] | PlanOperationalHandoffReadinessMatrix:
    """Return deterministic summary counts for a matrix, row iterable, or source."""
    if isinstance(matrix, PlanOperationalHandoffReadinessMatrix):
        return dict(matrix.summary)
    if _looks_like_plan(matrix) or _looks_like_task(matrix) or isinstance(matrix, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_operational_handoff_readiness_matrix(matrix)
    rows = list(matrix)
    return _summary(len(rows), rows)


def plan_operational_handoff_readiness_matrix_to_dict(
    matrix: PlanOperationalHandoffReadinessMatrix,
) -> dict[str, Any]:
    """Serialize an operational handoff readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_operational_handoff_readiness_matrix_to_dict.__test__ = False


def plan_operational_handoff_readiness_matrix_to_dicts(
    matrix: PlanOperationalHandoffReadinessMatrix | Iterable[PlanOperationalHandoffReadinessRow],
) -> list[dict[str, Any]]:
    """Serialize operational handoff rows to plain dictionaries."""
    if isinstance(matrix, PlanOperationalHandoffReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_operational_handoff_readiness_matrix_to_dicts.__test__ = False


def plan_operational_handoff_readiness_matrix_to_markdown(
    matrix: PlanOperationalHandoffReadinessMatrix,
) -> str:
    """Render an operational handoff readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_operational_handoff_readiness_matrix_to_markdown.__test__ = False


def _task_row(
    task: Mapping[str, Any],
    index: int,
    plan_context: tuple[tuple[str, str], ...],
) -> PlanOperationalHandoffReadinessRow | None:
    texts = [*_candidate_texts(task), *plan_context]
    domains, evidence = _domains(texts, task)
    if not domains:
        return None

    required_artifacts = _required_artifacts(domains)
    artifact_context = " ".join(text for field, text in texts if _artifact_context_field(field))
    present_artifacts = _present_artifacts(required_artifacts, artifact_context)
    missing_artifacts = tuple(artifact for artifact in required_artifacts if artifact not in present_artifacts)
    return PlanOperationalHandoffReadinessRow(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        handoff_domains=domains,
        required_artifacts=required_artifacts,
        owner_suggestions=_owner_suggestions(task, domains, texts),
        missing_artifacts=missing_artifacts,
        readiness_level=_readiness_level(required_artifacts, missing_artifacts),
        handoff_timing=_handoff_timing(task, domains, texts),
        evidence=evidence,
    )


def _summary(
    task_count: int,
    rows: Iterable[PlanOperationalHandoffReadinessRow],
) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "handoff_task_count": len(row_list),
        "no_handoff_task_count": task_count - len(row_list),
        "domain_counts": {
            domain: sum(1 for row in row_list if domain in row.handoff_domains)
            for domain in _DOMAIN_ORDER
        },
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness_level == readiness)
            for readiness in _READINESS_ORDER
        },
        "missing_artifact_counts": {
            artifact: sum(1 for row in row_list if artifact in row.missing_artifacts)
            for artifact in _ARTIFACT_ORDER
        },
    }


def _domains(
    texts: Iterable[tuple[str, str]],
    task: Mapping[str, Any],
) -> tuple[tuple[OperationalHandoffDomain, ...], tuple[str, ...]]:
    found: set[OperationalHandoffDomain] = set()
    evidence: list[str] = []
    for field, text in texts:
        for domain, pattern in _DOMAIN_PATTERNS.items():
            if pattern.search(text):
                found.add(domain)
                evidence.append(_evidence_snippet(field, text))
        if field.startswith(("files_or_modules", "files")):
            normalized = _normalized_path(text)
            for domain, pattern in _PATH_PATTERNS.items():
                if pattern.search(normalized):
                    found.add(domain)
                    evidence.append(_evidence_snippet(field, text))

    if _optional_text(task.get("owner_type")) in {"human", "either"} and found:
        evidence.append(f"owner_type: {_optional_text(task.get('owner_type'))}")
    return (
        tuple(domain for domain in _DOMAIN_ORDER if domain in found),
        tuple(_dedupe(evidence)),
    )


def _required_artifacts(
    domains: tuple[OperationalHandoffDomain, ...],
) -> tuple[OperationalHandoffArtifact, ...]:
    artifacts: list[OperationalHandoffArtifact] = []
    for domain in domains:
        artifacts.extend(_DOMAIN_ARTIFACTS[domain])
    return tuple(_ordered_dedupe(artifacts, _ARTIFACT_ORDER))


def _present_artifacts(
    required_artifacts: tuple[OperationalHandoffArtifact, ...],
    artifact_context: str,
) -> tuple[OperationalHandoffArtifact, ...]:
    return tuple(
        artifact
        for artifact in required_artifacts
        if _ARTIFACT_PATTERNS[artifact].search(artifact_context)
    )


def _owner_suggestions(
    task: Mapping[str, Any],
    domains: tuple[OperationalHandoffDomain, ...],
    texts: Iterable[tuple[str, str]],
) -> tuple[str, ...]:
    owners: list[str] = []
    for field, text in texts:
        normalized = field.replace("_", " ")
        if _OWNER_KEY_RE.search(normalized):
            owners.extend(_owner_values(text, allow_plain=True))
        elif _OWNER_KEY_RE.search(text) and len(text) <= 160:
            owners.extend(_owner_values(text))

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("owner", "owners", "handoff_owner", "support_owner", "ops_owner", "dri", "team", "queue"):
            owners.extend(_strings(metadata.get(key)))
    owners.extend(_DEFAULT_OWNER_BY_DOMAIN[domain] for domain in domains)
    return tuple(_dedupe(_clean_owner(owner) for owner in owners if _clean_owner(owner)))


def _owner_values(text: str, *, allow_plain: bool = False) -> list[str]:
    value = _text(text)
    labelled = re.search(
        r"\b(?:owner|owners|dri|responsible|team|lead|queue|on[- ]?call)\s*(?::|is|=)\s*([A-Z][A-Za-z0-9_.@& /-]{1,80})\b",
        value,
        re.I,
    )
    if labelled:
        return [labelled.group(1)]
    return [value] if allow_plain and 1 <= len(value) <= 80 and not _DOMAIN_SIGNAL(value) else []


def _clean_owner(value: str) -> str:
    return re.sub(r"^(?:owner|owners|dri|team|queue|lead)\s*[:=]\s*", "", _text(value), flags=re.I)


def _DOMAIN_SIGNAL(text: str) -> bool:
    return any(pattern.search(text) for pattern in _DOMAIN_PATTERNS.values())


def _readiness_level(
    required_artifacts: tuple[OperationalHandoffArtifact, ...],
    missing_artifacts: tuple[OperationalHandoffArtifact, ...],
) -> OperationalHandoffReadiness:
    if not missing_artifacts:
        return "ready"
    if len(missing_artifacts) == len(required_artifacts) or "operational_owner" in missing_artifacts:
        return "blocked"
    return "partial"


def _handoff_timing(
    task: Mapping[str, Any],
    domains: tuple[OperationalHandoffDomain, ...],
    texts: Iterable[tuple[str, str]],
) -> str:
    explicit = _explicit_timing(texts)
    if explicit:
        return explicit
    dependencies = tuple(_strings(task.get("depends_on") or task.get("dependencies")))
    if dependencies:
        return "after dependencies: " + ", ".join(dependencies)
    context = " ".join(text for _, text in texts)
    if "finance_operations" in domains and re.search(r"\b(?:month[- ]end|invoice|billing cycle|reconciliation)\b", context, re.I):
        return "before billing operations window"
    if "vendor_operations" in domains and re.search(r"\b(?:cutover|migration|go[-/ ]live)\b", context, re.I):
        return "before vendor cutover"
    if "oncall_runbook" in domains:
        return "before production launch"
    if _ROLLOUT_RE.search(context):
        return "before rollout"
    return "before operational handoff"


def _explicit_timing(texts: Iterable[tuple[str, str]]) -> str | None:
    for field, text in texts:
        normalized = field.casefold().replace("-", "_")
        if "handoff" in normalized and "tim" in normalized:
            return _text(text)
        match = re.search(r"\bhandoff timing\s*(?::|is|=)\s*([A-Za-z0-9 ,._:/#-]{3,100})\b", text, re.I)
        if match:
            return _text(match.group(1))
    return None


def _artifact_context_field(field: str) -> bool:
    normalized = field.casefold().replace("-", "_")
    return any(
        token in normalized
        for token in (
            "acceptance",
            "criteria",
            "definition",
            "metadata",
            "test_command",
            "validation",
            "notes",
            "handoff",
            "runbook",
            "support",
            "owner",
        )
    )


def _source_payload(source: Any) -> tuple[str | None, tuple[tuple[str, str], ...], list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return _optional_text(payload.get("id")), _plan_context(payload), _task_payloads(payload.get("tasks"))
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _plan_context(payload), _task_payloads(payload.get("tasks"))
        return None, (), [dict(source)]
    if _looks_like_task(source):
        return None, (), [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _plan_context(payload), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)
    except TypeError:
        return None, (), []

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
    return None, (), tasks


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


def _plan_context(plan: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for field_name in ("handoff_prompt", "test_strategy", "target_repo", "project_type"):
        if text := _optional_text(plan.get(field_name)):
            texts.append((f"plan.{field_name}", text))
    texts.extend((f"plan.{field}", text) for field, text in _metadata_texts(plan.get("metadata")))
    return tuple(texts)


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
    for field_name in (
        "depends_on",
        "dependencies",
        "mappings",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "definition_of_done",
        "validation_commands",
        "tags",
        "labels",
        "notes",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    texts.extend(_metadata_texts(task.get("metadata")))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.append((field, key_text))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            else:
                texts.append((field, key_text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


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
        "dependencies",
        "mappings",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "definition_of_done",
        "validation_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tags",
        "labels",
        "notes",
        "tasks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _domain_index(domains: tuple[OperationalHandoffDomain, ...]) -> int:
    if not domains:
        return len(_DOMAIN_ORDER)
    return _DOMAIN_ORDER.index(domains[0])


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


def _normalized_path(value: str) -> str:
    return _text(value).replace("\\", "/")


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


def _ordered_dedupe(values: Iterable[_T], order: tuple[_T, ...]) -> list[_T]:
    found = set(_dedupe(values))
    return [value for value in order if value in found]


__all__ = [
    "OperationalHandoffArtifact",
    "OperationalHandoffDomain",
    "OperationalHandoffReadiness",
    "PlanOperationalHandoffReadinessMatrix",
    "PlanOperationalHandoffReadinessRow",
    "analyze_plan_operational_handoff_readiness_matrix",
    "build_plan_operational_handoff_readiness_matrix",
    "derive_plan_operational_handoff_readiness_matrix",
    "extract_plan_operational_handoff_readiness_matrix",
    "generate_plan_operational_handoff_readiness_matrix",
    "plan_operational_handoff_readiness_matrix_to_dict",
    "plan_operational_handoff_readiness_matrix_to_dicts",
    "plan_operational_handoff_readiness_matrix_to_markdown",
    "summarize_plan_operational_handoff_readiness_matrix",
]
