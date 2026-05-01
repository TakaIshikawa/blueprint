"""Build stakeholder approval matrices for implementation briefs and plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import (
    ExecutionPlan,
    ExecutionTask,
    ImplementationBrief,
    SourceBrief,
)


StakeholderGroup = Literal[
    "product",
    "engineering",
    "security",
    "data",
    "legal",
    "support",
    "operations",
    "finance",
]
ApprovalStatus = Literal["required", "advisory"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_GROUP_ORDER: dict[StakeholderGroup, int] = {
    "product": 0,
    "engineering": 1,
    "security": 2,
    "data": 3,
    "legal": 4,
    "support": 5,
    "operations": 6,
    "finance": 7,
}
_REQUIRED_GROUPS: frozenset[StakeholderGroup] = frozenset(
    {"product", "security", "data", "legal", "operations", "finance"}
)

_SIGNAL_PATTERNS: dict[StakeholderGroup, re.Pattern[str]] = {
    "product": re.compile(
        r"\b(?:product|mvp|user journey|user experience|ux|ui|workflow|feature|"
        r"launch|beta|experiment|feature flag|roadmap|customer-facing|admin-facing|"
        r"acceptance criteria|definition of done)\b",
        re.IGNORECASE,
    ),
    "engineering": re.compile(
        r"\b(?:api|service|refactor|implementation|code|module|library|test|tests|"
        r"pytest|integration|dependency|architecture|backend|frontend|schema|migration)\b",
        re.IGNORECASE,
    ),
    "security": re.compile(
        r"\b(?:security|auth|authentication|authorization|permission|permissions|"
        r"rbac|sso|oauth|jwt|session|secret|secrets|encryption|kms|tls|ssl|"
        r"vulnerability|audit log|audit trail|threat|access control)\b",
        re.IGNORECASE,
    ),
    "data": re.compile(
        r"\b(?:data|database|db|schema|migration|analytics|tracking|etl|pipeline|"
        r"warehouse|reporting|export|import|retention|backup|cache|pii|personal data|"
        r"customer data|user data|profile data)\b",
        re.IGNORECASE,
    ),
    "legal": re.compile(
        r"\b(?:legal|privacy|gdpr|ccpa|hipaa|compliance|regulatory|policy|terms|"
        r"terms of service|consent|contract|license|copyright|data processing agreement|dpa)\b",
        re.IGNORECASE,
    ),
    "support": re.compile(
        r"\b(?:support|helpdesk|customer care|ticket|tickets|runbook|playbook|"
        r"escalation|customer communication|faq|knowledge base|incident response)\b",
        re.IGNORECASE,
    ),
    "operations": re.compile(
        r"\b(?:operations|ops|deploy|deployment|release|rollback|infra|infrastructure|"
        r"terraform|kubernetes|k8s|helm|monitoring|observability|alert|alerts|"
        r"incident|on-call|oncall|slo|sla|cron|queue|worker|backup|restore)\b",
        re.IGNORECASE,
    ),
    "finance": re.compile(
        r"\b(?:finance|billing|invoice|invoices|payment|payments|refund|chargeback|"
        r"subscription|tax|revenue|ledger|payout|checkout|pricing|discount|"
        r"purchase order|po)\b",
        re.IGNORECASE,
    ),
}


@dataclass(frozen=True, slots=True)
class PlanStakeholderApprovalRow:
    """One stakeholder approval row inferred from a brief or execution plan."""

    stakeholder_group: StakeholderGroup
    approval_status: ApprovalStatus
    affected_task_ids: tuple[str, ...]
    rationale: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    owners: tuple[str, ...] = field(default_factory=tuple)
    reviewers: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "stakeholder_group": self.stakeholder_group,
            "approval_status": self.approval_status,
            "owners": list(self.owners),
            "reviewers": list(self.reviewers),
            "affected_task_ids": list(self.affected_task_ids),
            "rationale": self.rationale,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanStakeholderApprovalMatrix:
    """Plan-level stakeholder approval matrix and rollup counts."""

    plan_id: str | None = None
    brief_id: str | None = None
    rows: tuple[PlanStakeholderApprovalRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "brief_id": self.brief_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return approval rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the approval matrix as deterministic Markdown."""
        title = "# Plan Stakeholder Approval Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        elif self.brief_id:
            title = f"{title}: {self.brief_id}"

        lines = [title]
        if not self.rows:
            lines.extend(["", "No stakeholder approvals were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Stakeholder | Status | Owners | Reviewers | Affected Tasks | Rationale | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.stakeholder_group} | "
                f"{row.approval_status} | "
                f"{_markdown_cell(', '.join(row.owners) or '-')} | "
                f"{_markdown_cell(', '.join(row.reviewers) or '-')} | "
                f"{_markdown_cell(', '.join(row.affected_task_ids) or '-')} | "
                f"{_markdown_cell(row.rationale)} | "
                f"{_markdown_cell('; '.join(row.evidence))} |"
            )
        return "\n".join(lines)


def build_plan_stakeholder_approval_matrix(
    source: (
        Mapping[str, Any]
        | ImplementationBrief
        | SourceBrief
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
    execution_plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> PlanStakeholderApprovalMatrix:
    """Build stakeholder approval rows from brief and execution-plan signals."""
    plan_id, brief_id, records = _source_records(source, execution_plan)
    signals: dict[StakeholderGroup, list[tuple[str | None, str]]] = {}
    owners: dict[StakeholderGroup, list[str]] = {}
    reviewers: dict[StakeholderGroup, list[str]] = {}

    for record in records:
        record_signals = _signals(record)
        for group, evidence in record_signals.items():
            for item in evidence:
                signals.setdefault(group, []).append((_optional_text(record.get("task_id")), item))

        explicit = _explicit_people(record, set(record_signals))
        for group, people in explicit["owners"].items():
            owners.setdefault(group, []).extend(people)
        for group, people in explicit["reviewers"].items():
            reviewers.setdefault(group, []).extend(people)

    rows = tuple(
        PlanStakeholderApprovalRow(
            stakeholder_group=group,
            approval_status=_approval_status(group, [evidence for _, evidence in entries]),
            owners=tuple(_dedupe(owners.get(group, ()))),
            reviewers=tuple(_dedupe(reviewers.get(group, ()))),
            affected_task_ids=tuple(
                _dedupe(task_id for task_id, _ in entries if task_id is not None)
            ),
            rationale=_rationale(group, _approval_status(group, [evidence for _, evidence in entries])),
            evidence=tuple(_dedupe(evidence for _, evidence in entries)),
        )
        for group, entries in sorted(signals.items(), key=lambda item: _GROUP_ORDER[item[0]])
        if entries
    )
    status_counts = {
        status: sum(1 for row in rows if row.approval_status == status)
        for status in ("required", "advisory")
    }
    group_counts = {
        group: sum(1 for row in rows if row.stakeholder_group == group)
        for group in _GROUP_ORDER
    }
    return PlanStakeholderApprovalMatrix(
        plan_id=plan_id,
        brief_id=brief_id,
        rows=rows,
        summary={
            "row_count": len(rows),
            "required_count": status_counts["required"],
            "advisory_count": status_counts["advisory"],
            "affected_task_count": len(
                {task_id for row in rows for task_id in row.affected_task_ids}
            ),
            "group_counts": group_counts,
        },
    )


def plan_stakeholder_approval_matrix_to_dict(
    result: PlanStakeholderApprovalMatrix,
) -> dict[str, Any]:
    """Serialize a stakeholder approval matrix to a plain dictionary."""
    return result.to_dict()


plan_stakeholder_approval_matrix_to_dict.__test__ = False


def plan_stakeholder_approval_matrix_to_markdown(
    result: PlanStakeholderApprovalMatrix,
) -> str:
    """Render a stakeholder approval matrix as Markdown."""
    return result.to_markdown()


plan_stakeholder_approval_matrix_to_markdown.__test__ = False


def summarize_plan_stakeholder_approvals(
    source: (
        Mapping[str, Any]
        | ImplementationBrief
        | SourceBrief
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
    execution_plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> PlanStakeholderApprovalMatrix:
    """Compatibility alias for building stakeholder approval matrices."""
    return build_plan_stakeholder_approval_matrix(source, execution_plan)


def _signals(record: Mapping[str, Any]) -> dict[StakeholderGroup, tuple[str, ...]]:
    signals: dict[StakeholderGroup, list[str]] = {}

    for path in _strings(record.get("files_or_modules") or record.get("files")):
        _add_path_signals(signals, path)

    for source_field, text in _text_fields(record):
        _add_text_signals(signals, source_field, text)

    for source_field, text in _metadata_texts(record.get("metadata")):
        if _is_person_signal_field(source_field):
            continue
        _add_text_signals(signals, source_field, text)

    return {group: tuple(_dedupe(evidence)) for group, evidence in signals.items() if evidence}


def _add_path_signals(
    signals: dict[StakeholderGroup, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    evidence = f"files_or_modules: {original}"

    if bool({"src", "lib", "app", "tests", "test", "api", "services", "workers"} & parts):
        _append(signals, "engineering", evidence)
    if bool({"ui", "ux", "frontend", "components", "pages", "views", "product"} & parts):
        _append(signals, "product", evidence)
    if bool({"auth", "security", "permissions", "rbac", "secrets", "kms", "tls"} & parts):
        _append(signals, "security", evidence)
    if any(token in name for token in ("auth", "permission", "secret", "encrypt")):
        _append(signals, "security", evidence)
    if bool({"data", "db", "database", "databases", "migrations", "models", "analytics"} & parts):
        _append(signals, "data", evidence)
    if any(token in name for token in ("schema", "migration", "export", "import")):
        _append(signals, "data", evidence)
    if bool({"privacy", "legal", "compliance", "policies", "policy", "consent"} & parts):
        _append(signals, "legal", evidence)
    if bool({"support", "helpdesk", "runbooks", "playbooks", "docs"} & parts):
        _append(signals, "support", evidence)
    if bool({"infra", "ops", "deploy", "deployment", "terraform", "k8s", "kubernetes"} & parts):
        _append(signals, "operations", evidence)
    if any(token in name for token in ("deploy", "rollback", "alert", "monitor")):
        _append(signals, "operations", evidence)
    if bool({"billing", "payments", "invoices", "ledger", "checkout", "finance"} & parts):
        _append(signals, "finance", evidence)
    if any(token in name for token in ("billing", "invoice", "payment", "ledger")):
        _append(signals, "finance", evidence)


def _add_text_signals(
    signals: dict[StakeholderGroup, list[str]],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    for group, pattern in _SIGNAL_PATTERNS.items():
        if pattern.search(text):
            _append(signals, group, evidence)


def _explicit_people(
    record: Mapping[str, Any],
    matched_groups: set[StakeholderGroup],
) -> dict[str, dict[StakeholderGroup, list[str]]]:
    result: dict[str, dict[StakeholderGroup, list[str]]] = {"owners": {}, "reviewers": {}}
    candidates = list(_person_fields(record))
    candidates.extend(_person_fields(record.get("metadata"), prefix="metadata"))

    for field, value in candidates:
        kind = "reviewers" if _is_reviewer_field(field) else "owners"
        groups = _groups_for_people_field(field, value, matched_groups)
        for group in groups:
            result[kind].setdefault(group, []).extend(_strings(value))
    return result


def _person_fields(value: Any, prefix: str = "") -> list[tuple[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    fields: list[tuple[str, Any]] = []
    for key in sorted(value, key=lambda item: str(item)):
        field = f"{prefix}.{key}" if prefix else str(key)
        child = value[key]
        normalized = str(key).casefold()
        if any(token in normalized for token in ("owner", "assignee", "approver", "reviewer")):
            fields.append((field, child))
        if isinstance(child, Mapping):
            fields.extend(_person_fields(child, field))
    return fields


def _groups_for_people_field(
    field: str,
    value: Any,
    matched_groups: set[StakeholderGroup],
) -> tuple[StakeholderGroup, ...]:
    field_text = field.casefold()
    value_text = " ".join(_strings(value)).casefold()
    groups = [
        group
        for group in _GROUP_ORDER
        if group in field_text or re.search(rf"\b{re.escape(group)}\b", value_text)
    ]
    if groups:
        return tuple(groups)
    if matched_groups:
        return tuple(sorted(matched_groups, key=lambda group: _GROUP_ORDER[group]))
    return ("engineering",)


def _is_reviewer_field(field: str) -> bool:
    normalized = field.casefold()
    return "reviewer" in normalized or "approver" in normalized


def _is_person_signal_field(field: str) -> bool:
    normalized = field.casefold()
    return any(
        token in normalized
        for token in ("owner", "assignee", "approver", "reviewer")
    )


def _approval_status(group: StakeholderGroup, evidence: list[str]) -> ApprovalStatus:
    if group in _REQUIRED_GROUPS:
        return "required"
    combined = " ".join(evidence)
    if re.search(r"\b(?:required|required approval|must approve|sign[- ]?off|gate)\b", combined, re.I):
        return "required"
    return "advisory"


def _rationale(group: StakeholderGroup, status: ApprovalStatus) -> str:
    if status == "required":
        return _REQUIRED_RATIONALE[group]
    return _ADVISORY_RATIONALE[group]


_REQUIRED_RATIONALE: dict[StakeholderGroup, str] = {
    "product": "Product approval is required because the plan changes user-facing behavior, launch scope, or acceptance expectations.",
    "engineering": "Engineering approval is required because the plan explicitly marks implementation review as a dispatch gate.",
    "security": "Security approval is required because the plan touches authentication, authorization, secrets, auditability, or access controls.",
    "data": "Data approval is required because the plan changes data storage, processing, analytics, retention, exports, or personal data handling.",
    "legal": "Legal approval is required because the plan touches privacy, compliance, consent, policy, contract, or regulatory obligations.",
    "support": "Support approval is required because the plan explicitly marks customer support readiness as a dispatch gate.",
    "operations": "Operations approval is required because the plan touches deployment, infrastructure, observability, rollback, incidents, or production operations.",
    "finance": "Finance approval is required because the plan touches billing, payments, invoices, pricing, tax, revenue, or ledger workflows.",
}
_ADVISORY_RATIONALE: dict[StakeholderGroup, str] = {
    "product": "Product review is advisory for confirming scope, user impact, and acceptance expectations.",
    "engineering": "Engineering review is advisory for confirming implementation ownership, test coverage, and code impact.",
    "security": "Security review is advisory for confirming no security gate is needed.",
    "data": "Data review is advisory for confirming data handling expectations.",
    "legal": "Legal review is advisory for confirming no legal gate is needed.",
    "support": "Support review is advisory for confirming readiness, runbooks, and customer communication impact.",
    "operations": "Operations review is advisory for confirming release and production readiness.",
    "finance": "Finance review is advisory for confirming no billing or revenue gate is needed.",
}


def _source_records(
    source: (
        Mapping[str, Any]
        | ImplementationBrief
        | SourceBrief
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
    ),
    execution_plan: Mapping[str, Any] | ExecutionPlan | None,
) -> tuple[str | None, str | None, list[dict[str, Any]]]:
    plan_id, plan_brief_id, plan_records = _plan_records(source)
    brief_id, brief_record = _brief_record(source)

    if execution_plan is not None:
        plan_id, plan_brief_id, plan_records = _plan_records(execution_plan)

    records = []
    if brief_record:
        records.append(brief_record)
    records.extend(plan_records)
    return plan_id, brief_id or plan_brief_id, records


def _plan_records(value: Any) -> tuple[str | None, str | None, list[dict[str, Any]]]:
    if isinstance(value, ExecutionTask):
        task = value.model_dump(mode="python")
        task["task_id"] = _optional_text(task.get("id"))
        return None, None, [task]
    if isinstance(value, ExecutionPlan):
        return (
            _optional_text(value.id),
            _optional_text(value.implementation_brief_id),
            [_task_record(task.model_dump(mode="python")) for task in value.tasks],
        )
    if isinstance(value, Mapping) and "tasks" in value:
        payload = _plan_payload(value)
        return (
            _optional_text(payload.get("id")),
            _optional_text(payload.get("implementation_brief_id")),
            [_task_record(task) for task in _task_payloads(payload.get("tasks"))],
        )
    if isinstance(value, Mapping) and not _is_task_like_mapping(value):
        return None, None, []
    if isinstance(value, Mapping):
        return None, None, [_task_record(dict(value))]

    try:
        iterator = iter(value)
    except TypeError:
        task = _task_like_payload(value)
        return None, None, [_task_record(task)] if task else []

    tasks = [_task_record(task) for item in iterator if (task := _task_like_payload(item))]
    return None, None, tasks


def _brief_record(value: Any) -> tuple[str | None, dict[str, Any] | None]:
    if isinstance(value, (ExecutionPlan, ExecutionTask)):
        return None, None
    if isinstance(value, Mapping) and "tasks" in value:
        return None, None
    if isinstance(value, Mapping) and _is_task_like_mapping(value):
        return None, None
    payload = _brief_payload(value)
    if not payload:
        return None, None
    brief_id = _optional_text(payload.get("id")) or _optional_text(payload.get("source_id"))
    payload["brief_id"] = brief_id
    payload["scope_name"] = _optional_text(payload.get("title")) or brief_id or "implementation brief"
    return brief_id, payload


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _brief_payload(brief: Any) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        value = brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    if isinstance(brief, Mapping):
        for model in (ImplementationBrief, SourceBrief):
            try:
                value = model.model_validate(brief).model_dump(mode="python")
                return dict(value) if isinstance(value, Mapping) else {}
            except (TypeError, ValueError, ValidationError):
                continue
        return dict(brief)
    return _object_payload(brief)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    return [task for item in items if (task := _task_like_payload(item))]


def _is_task_like_mapping(value: Mapping[str, Any]) -> bool:
    return any(
        key in value
        for key in (
            "acceptance_criteria",
            "depends_on",
            "files_or_modules",
            "files",
            "blocked_reason",
            "test_command",
            "suggested_engine",
        )
    )


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
        "implementation_brief_id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "target_user",
        "buyer",
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "product_surface",
        "scope",
        "non_goals",
        "assumptions",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "constraints",
        "validation_plan",
        "definition_of_done",
        "summary",
        "source_payload",
        "source_links",
        "milestone",
        "owner_type",
        "owner_role",
        "owner",
        "reviewer",
        "reviewers",
        "approver",
        "approvers",
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
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _task_record(task: Mapping[str, Any]) -> dict[str, Any]:
    record = dict(task)
    record["task_id"] = _optional_text(record.get("id"))
    record["scope_name"] = (
        _optional_text(record.get("title"))
        or _optional_text(record.get("id"))
        or "execution task"
    )
    return record


def _text_fields(record: Mapping[str, Any]) -> list[tuple[str, str]]:
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
        "summary",
        "milestone",
        "owner_type",
        "owner_role",
        "owner",
        "reviewer",
        "reviewers",
        "approver",
        "approvers",
        "suggested_engine",
        "risk_level",
        "test_command",
        "description",
        "blocked_reason",
    ):
        if text := _optional_text(record.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "risks",
        "constraints",
        "definition_of_done",
        "acceptance_criteria",
        "tags",
        "labels",
    ):
        for index, text in enumerate(_strings(record.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
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
    signals: dict[StakeholderGroup, list[str]],
    group: StakeholderGroup,
    evidence: str,
) -> None:
    signals.setdefault(group, []).append(evidence)


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
    "ApprovalStatus",
    "PlanStakeholderApprovalMatrix",
    "PlanStakeholderApprovalRow",
    "StakeholderGroup",
    "build_plan_stakeholder_approval_matrix",
    "plan_stakeholder_approval_matrix_to_dict",
    "plan_stakeholder_approval_matrix_to_markdown",
    "summarize_plan_stakeholder_approvals",
]
