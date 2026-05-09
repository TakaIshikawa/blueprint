"""Build plan-level cost estimation matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CostCategory = Literal[
    "infrastructure_costs_identified",
    "third_party_services_tracked",
    "labor_estimates_included",
    "licensing_fees_accounted",
    "operational_overhead_calculated",
    "resource_scaling_modeled",
    "data_transfer_costs_estimated",
    "api_calls_budgeted",
    "storage_growth_projected",
    "support_requirements_costed",
]
CostReadiness = Literal["ready", "partial", "blocked"]
CostRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[CostReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_RISK_ORDER: dict[CostRisk, int] = {"high": 0, "medium": 1, "low": 2}
_CATEGORY_ORDER: dict[CostCategory, int] = {
    "infrastructure_costs_identified": 0,
    "third_party_services_tracked": 1,
    "labor_estimates_included": 2,
    "licensing_fees_accounted": 3,
    "operational_overhead_calculated": 4,
    "resource_scaling_modeled": 5,
    "data_transfer_costs_estimated": 6,
    "api_calls_budgeted": 7,
    "storage_growth_projected": 8,
    "support_requirements_costed": 9,
}
_COST_ESTIMATION_RE = re.compile(
    r"\b(?:cost|costs|budget|budgeting|pricing|price|expense|expenses|spend|spending|"
    r"financial|finance|roi|tco|total cost of ownership|"
    r"infrastructure|reserved instance|spot instance|saas|subscription|licensing|"
    r"operational overhead|scaling|bandwidth|api call|storage growth|support tier|"
    r"contractor|headcount|fte|sla)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[CostCategory, re.Pattern[str]] = {
    "infrastructure_costs_identified": re.compile(
        r"\b(?:infrastructure cost|compute cost|server cost|cloud cost|cloud storage|vm cost|instance cost|"
        r"ec2|gcp compute|azure vm|reserved instances?|reserved capacity|spot instances?|spot pricing|on-demand|"
        r"cpu cost|memory cost|disk cost|storage cost|iops)\b",
        re.I,
    ),
    "third_party_services_tracked": re.compile(
        r"\b(?:third[- ]party|saas|api service|external service|vendor cost|"
        r"stripe|twilio|sendgrid|datadog|new relic|auth0|okta|"
        r"subscription|subscription fee|per-seat|per-user pricing)\b",
        re.I,
    ),
    "labor_estimates_included": re.compile(
        r"\b(?:labor cost|labor costs|labor estimate|labor estimates|personnel cost|developer cost|engineering cost|"
        r"team cost|headcount|fte|full-time equivalent|hourly rate|"
        r"contractor|contractor cost|consultant|staffing cost|salary cost)\b",
        re.I,
    ),
    "licensing_fees_accounted": re.compile(
        r"\b(?:license fee|licensing|software license|perpetual license|"
        r"subscription license|enterprise license|commercial license|"
        r"oracle license|microsoft license|license compliance)\b",
        re.I,
    ),
    "operational_overhead_calculated": re.compile(
        r"\b(?:operational overhead|ops overhead|support cost|maintenance cost|"
        r"incident response|on-call|monitoring cost|logging cost|"
        r"backup cost|disaster recovery cost|dr cost)\b",
        re.I,
    ),
    "resource_scaling_modeled": re.compile(
        r"\b(?:scaling cost|scaling costs|auto[- ]scaling|autoscaling|horizontal scaling|vertical scaling|"
        r"growth factor|capacity planning|load growth|traffic growth|"
        r"elasticity|scale-up|scale-out|scaling model)\b",
        re.I,
    ),
    "data_transfer_costs_estimated": re.compile(
        r"\b(?:data transfer|bandwidth cost|egress cost|network cost|"
        r"cross-region transfer|cross-az transfer|cdn cost|cloudfront|"
        r"data out|ingress|egress|gb transferred)\b",
        re.I,
    ),
    "api_calls_budgeted": re.compile(
        r"\b(?:api call|api calls|api call cost|per-call pricing|request cost|query cost|"
        r"transaction cost|usage-based pricing|metered billing|"
        r"rate limit cost|quota|api pricing tier)\b",
        re.I,
    ),
    "storage_growth_projected": re.compile(
        r"\b(?:storage growth|storage tiering|data growth|retention cost|archival cost|"
        r"s3 cost|blob storage|object storage|database storage|"
        r"backup storage|snapshot cost|volume growth)\b",
        re.I,
    ),
    "support_requirements_costed": re.compile(
        r"\b(?:support tier|support level|enterprise support|premium support|"
        r"business support|business sla|sla|sla cost|service level|technical support cost|"
        r"support contract|support plan)\b",
        re.I,
    ),
}
_OWNER_KEYS = (
    "owner",
    "owners",
    "owner_hint",
    "owner_team",
    "team",
    "dri",
    "finance_owner",
    "budget_owner",
    "finops_owner",
    "engineering_owner",
)
_DEFAULT_OWNERS: dict[CostCategory, str] = {
    "infrastructure_costs_identified": "infrastructure_owner",
    "third_party_services_tracked": "procurement_owner",
    "labor_estimates_included": "engineering_manager",
    "licensing_fees_accounted": "procurement_owner",
    "operational_overhead_calculated": "sre_owner",
    "resource_scaling_modeled": "infrastructure_owner",
    "data_transfer_costs_estimated": "infrastructure_owner",
    "api_calls_budgeted": "finops_owner",
    "storage_growth_projected": "infrastructure_owner",
    "support_requirements_costed": "customer_success_owner",
}
_GAP_MESSAGES: dict[CostCategory, str] = {
    "infrastructure_costs_identified": "Missing infrastructure cost estimates.",
    "third_party_services_tracked": "Missing third-party service cost tracking.",
    "labor_estimates_included": "Missing labor cost estimates.",
    "licensing_fees_accounted": "Missing licensing fee accounting.",
    "operational_overhead_calculated": "Missing operational overhead calculations.",
    "resource_scaling_modeled": "Missing resource scaling model.",
    "data_transfer_costs_estimated": "Missing data transfer cost estimates.",
    "api_calls_budgeted": "Missing API call budget.",
    "storage_growth_projected": "Missing storage growth projections.",
    "support_requirements_costed": "Missing support cost estimates.",
}
_NEXT_ACTIONS: dict[CostCategory, str] = {
    "infrastructure_costs_identified": "Identify and estimate compute, storage, and network infrastructure costs.",
    "third_party_services_tracked": "Track all third-party SaaS, API, and external service costs.",
    "labor_estimates_included": "Include development and operational labor cost estimates.",
    "licensing_fees_accounted": "Account for all software licensing fees and compliance costs.",
    "operational_overhead_calculated": "Calculate operational overhead including support, monitoring, and maintenance.",
    "resource_scaling_modeled": "Model resource scaling costs based on growth factors and capacity planning.",
    "data_transfer_costs_estimated": "Estimate bandwidth, egress, and cross-region data transfer costs.",
    "api_calls_budgeted": "Budget for per-call API pricing and usage-based costs.",
    "storage_growth_projected": "Project storage growth and retention costs over time.",
    "support_requirements_costed": "Cost support tier requirements and service level agreements.",
}
_HIGH_GAP_CATEGORIES: frozenset[CostCategory] = frozenset(
    {"infrastructure_costs_identified", "labor_estimates_included", "resource_scaling_modeled"}
)


@dataclass(frozen=True, slots=True)
class PlanCostEstimationRow:
    """One plan-level cost estimation row."""

    category: CostCategory
    owner: str
    estimates: tuple[str, ...] = field(default_factory=tuple)
    assumptions: tuple[str, ...] = field(default_factory=tuple)
    optimization_opportunities: tuple[str, ...] = field(default_factory=tuple)
    readiness: CostReadiness = "partial"
    risk: CostRisk = "medium"
    next_action: str = ""
    task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "owner": self.owner,
            "estimates": list(self.estimates),
            "assumptions": list(self.assumptions),
            "optimization_opportunities": list(self.optimization_opportunities),
            "readiness": self.readiness,
            "risk": self.risk,
            "next_action": self.next_action,
            "task_ids": list(self.task_ids),
        }


@dataclass(frozen=True, slots=True)
class PlanCostEstimationMatrix:
    """Plan-level cost estimation matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanCostEstimationRow, ...] = field(default_factory=tuple)
    cost_estimation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    gap_categories: tuple[CostCategory, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanCostEstimationRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "cost_estimation_task_ids": list(self.cost_estimation_task_ids),
            "gap_categories": list(self.gap_categories),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return cost estimation rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the cost estimation matrix as deterministic Markdown."""
        title = "# Plan Cost Estimation Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('ready_category_count', 0)} of "
                f"{self.summary.get('category_count', 0)} cost estimation categories ready "
                f"(high: {risk_counts.get('high', 0)}, medium: {risk_counts.get('medium', 0)}, "
                f"low: {risk_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No cost estimation rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Category | Owner | Readiness | Risk | Estimates | Assumptions | Optimization | Tasks |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.category} | "
                f"{_markdown_cell(row.owner)} | "
                f"{row.readiness} | "
                f"{row.risk} | "
                f"{_markdown_cell('; '.join(row.estimates) or 'none')} | "
                f"{_markdown_cell('; '.join(row.assumptions) or 'none')} | "
                f"{_markdown_cell('; '.join(row.optimization_opportunities) or 'none')} | "
                f"{_markdown_cell(', '.join(row.task_ids) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_cost_estimation_matrix(source: Any) -> PlanCostEstimationMatrix:
    """Build required cost estimation rows for an execution plan."""
    plan_id, tasks = _source_payload(source)
    category_estimates: dict[CostCategory, list[str]] = {category: [] for category in _CATEGORY_ORDER}
    category_assumptions: dict[CostCategory, list[str]] = {category: [] for category in _CATEGORY_ORDER}
    category_task_ids: dict[CostCategory, list[str]] = {category: [] for category in _CATEGORY_ORDER}
    owner_hints: dict[CostCategory, list[str]] = {category: [] for category in _CATEGORY_ORDER}
    cost_estimation_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        texts = _candidate_texts(task)
        context = " ".join(text for _, text in texts)
        task_has_cost_signal = bool(_COST_ESTIMATION_RE.search(context))
        owners = _owner_hints(task)
        for category, pattern in _CATEGORY_PATTERNS.items():
            matches = [
                _evidence_snippet(source_field, text)
                for source_field, text in texts
                if pattern.search(text)
            ]
            if matches:
                task_has_cost_signal = True
                category_estimates[category].extend(matches)
                category_task_ids[category].append(task_id)
                owner_hints[category].extend(owners)
        if task_has_cost_signal:
            cost_estimation_task_ids.append(task_id)
            for category in _CATEGORY_ORDER:
                owner_hints[category].extend(owners)

    if not cost_estimation_task_ids:
        rows: tuple[PlanCostEstimationRow, ...] = ()
    else:
        rows = tuple(
            _row(
                category,
                category_estimates[category],
                category_assumptions[category],
                category_task_ids[category],
                owner_hints[category],
            )
            for category in _CATEGORY_ORDER
        )
    return PlanCostEstimationMatrix(
        plan_id=plan_id,
        rows=rows,
        cost_estimation_task_ids=tuple(_dedupe(cost_estimation_task_ids)),
        gap_categories=tuple(row.category for row in rows if not row.estimates),
        summary=_summary(len(tasks), rows, cost_estimation_task_ids),
    )


def generate_plan_cost_estimation_matrix(source: Any) -> PlanCostEstimationMatrix:
    """Generate a cost estimation matrix from a plan-like source."""
    return build_plan_cost_estimation_matrix(source)


def plan_cost_estimation_matrix_to_dict(
    matrix: PlanCostEstimationMatrix,
) -> dict[str, Any]:
    """Serialize a cost estimation matrix to a plain dictionary."""
    return matrix.to_dict()


plan_cost_estimation_matrix_to_dict.__test__ = False


def plan_cost_estimation_matrix_to_dicts(
    matrix: PlanCostEstimationMatrix | Iterable[PlanCostEstimationRow],
) -> list[dict[str, Any]]:
    """Serialize cost estimation rows to plain dictionaries."""
    if isinstance(matrix, PlanCostEstimationMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_cost_estimation_matrix_to_dicts.__test__ = False


def plan_cost_estimation_matrix_to_markdown(
    matrix: PlanCostEstimationMatrix,
) -> str:
    """Render a cost estimation matrix as Markdown."""
    return matrix.to_markdown()


plan_cost_estimation_matrix_to_markdown.__test__ = False


def _row(
    category: CostCategory,
    estimates: Iterable[str],
    assumptions: Iterable[str],
    task_ids: Iterable[str],
    owners: Iterable[str],
) -> PlanCostEstimationRow:
    estimates_tuple = tuple(_dedupe(estimates))
    assumptions_tuple = tuple(_dedupe(assumptions))
    optimization_opportunities = _infer_optimization_opportunities(category, estimates_tuple)
    readiness: CostReadiness = "ready" if estimates_tuple else "partial"
    risk: CostRisk = "low" if estimates_tuple else ("high" if category in _HIGH_GAP_CATEGORIES else "medium")
    return PlanCostEstimationRow(
        category=category,
        owner=next(iter(_dedupe(owners)), _DEFAULT_OWNERS[category]),
        estimates=estimates_tuple,
        assumptions=assumptions_tuple,
        optimization_opportunities=optimization_opportunities,
        readiness=readiness,
        risk=risk,
        next_action="Ready for cost estimation review." if estimates_tuple else _NEXT_ACTIONS[category],
        task_ids=tuple(_dedupe(task_ids)),
    )


def _infer_optimization_opportunities(
    category: CostCategory,
    estimates: tuple[str, ...],
) -> tuple[str, ...]:
    """Infer optimization opportunities based on category and estimates."""
    if not estimates:
        return ()

    opportunities: list[str] = []
    context = " ".join(estimates).lower()

    if category == "infrastructure_costs_identified":
        if "reserved" not in context and "reserved capacity" not in context and "reserved instance" not in context:
            opportunities.append("Consider reserved capacity for predictable workloads")
        if "spot" not in context:
            opportunities.append("Evaluate spot instances for non-critical workloads")
    elif category == "resource_scaling_modeled":
        if "auto-scaling" not in context and "auto scaling" not in context and "autoscaling" not in context:
            opportunities.append("Implement auto-scaling to match demand")
    elif category == "data_transfer_costs_estimated":
        if "cdn" not in context:
            opportunities.append("Use CDN to reduce data transfer costs")
    elif category == "api_calls_budgeted":
        if "cach" not in context:
            opportunities.append("Implement caching to reduce API call volume")
    elif category == "storage_growth_projected":
        if "tier" not in context:
            opportunities.append("Use storage tiering for infrequently accessed data")
    elif category == "third_party_services_tracked":
        if "volume discount" not in context and "volume pricing" not in context:
            opportunities.append("Negotiate volume discounts with vendors")

    return tuple(opportunities)


def _summary(
    task_count: int,
    rows: Iterable[PlanCostEstimationRow],
    cost_estimation_task_ids: Iterable[str],
) -> dict[str, Any]:
    row_list = list(rows)
    total_accuracy_score = sum(
        30.0 if row.estimates else 0.0 for row in row_list
    )
    total_completeness_score = sum(
        25.0 if row.estimates and row.assumptions else (12.5 if row.estimates or row.assumptions else 0.0)
        for row in row_list
    )
    total_optimization_score = sum(
        25.0 if row.optimization_opportunities else 0.0 for row in row_list
    )
    total_assumptions_clarity_score = sum(
        20.0 if row.assumptions else 0.0 for row in row_list
    )

    max_possible_score = len(row_list) * 100.0
    overall_score = (
        total_accuracy_score + total_completeness_score +
        total_optimization_score + total_assumptions_clarity_score
    )

    return {
        "task_count": task_count,
        "category_count": len(row_list),
        "ready_category_count": sum(1 for row in row_list if row.readiness == "ready"),
        "gap_category_count": sum(1 for row in row_list if not row.estimates),
        "cost_estimation_task_count": len(tuple(_dedupe(cost_estimation_task_ids))),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "risk_counts": {risk: sum(1 for row in row_list if row.risk == risk) for risk in _RISK_ORDER},
        "scoring": {
            "accuracy": round(total_accuracy_score / max_possible_score * 100, 2) if max_possible_score > 0 else 0.0,
            "completeness": round(total_completeness_score / max_possible_score * 100, 2) if max_possible_score > 0 else 0.0,
            "optimization_opportunities": round(total_optimization_score / max_possible_score * 100, 2) if max_possible_score > 0 else 0.0,
            "assumptions_clarity": round(total_assumptions_clarity_score / max_possible_score * 100, 2) if max_possible_score > 0 else 0.0,
            "overall": round(overall_score / max_possible_score * 100, 2) if max_possible_score > 0 else 0.0,
        },
    }


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
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
    if _looks_like_task(source):
        return None, [_object_payload(source)]
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
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
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
    for field_name in ("title", "description", "milestone", "owner_type", "risk_level", "test_command", "blocked_reason"):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("depends_on", "files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes", "risks"):
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            elif key_text:
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


def _owner_hints(task: Mapping[str, Any]) -> list[str]:
    owners = []
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in _OWNER_KEYS:
            owners.extend(_strings(metadata.get(key)))
    return _dedupe(owners)


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
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "test_command",
        "status",
        "tags",
        "labels",
        "notes",
        "risks",
        "metadata",
        "blocked_reason",
        "tasks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
    "CostCategory",
    "CostReadiness",
    "CostRisk",
    "PlanCostEstimationMatrix",
    "PlanCostEstimationRow",
    "build_plan_cost_estimation_matrix",
    "generate_plan_cost_estimation_matrix",
    "plan_cost_estimation_matrix_to_dict",
    "plan_cost_estimation_matrix_to_dicts",
    "plan_cost_estimation_matrix_to_markdown",
]
