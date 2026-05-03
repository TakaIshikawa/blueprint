"""Build regional failover readiness matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


RegionalFailoverConcern = Literal[
    "multi_region_deployment",
    "regional_failover",
    "dns_routing",
    "regional_replica",
    "disaster_recovery",
    "regional_availability",
]
RegionalFailoverComponent = Literal[
    "application",
    "database",
    "infrastructure",
    "networking",
    "operations",
    "data_platform",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CONCERN_ORDER: tuple[RegionalFailoverConcern, ...] = (
    "multi_region_deployment",
    "regional_failover",
    "dns_routing",
    "regional_replica",
    "disaster_recovery",
    "regional_availability",
)
_COMPONENT_ORDER: tuple[RegionalFailoverComponent, ...] = (
    "application",
    "database",
    "infrastructure",
    "networking",
    "operations",
    "data_platform",
)
_CONCERN_PATTERNS: dict[RegionalFailoverConcern, re.Pattern[str]] = {
    "multi_region_deployment": re.compile(
        r"\b(?:multi[- ]region|multiple regions|regional deployment|cross[- ]region deployment|"
        r"deploy(?:ed)? to (?:two|multiple|secondary) regions|active[- /]active|active[- /]passive)\b",
        re.I,
    ),
    "regional_failover": re.compile(
        r"\b(?:failover|fail over|fail-over|regional failover|fallback region|region evacuation|"
        r"promote secondary|promote replica|primary region failure)\b",
        re.I,
    ),
    "dns_routing": re.compile(
        r"\b(?:dns routing|dns failover|route ?53|cloudflare|global accelerator|traffic steering|"
        r"weighted routing|geo routing|latency routing|global load balanc(?:er|ing))\b",
        re.I,
    ),
    "regional_replica": re.compile(
        r"\b(?:regional replicas?|read replicas?|cross[- ]region replicas?|replica lag|replication lag|"
        r"replicated database|secondary database|database replica)\b",
        re.I,
    ),
    "disaster_recovery": re.compile(
        r"\b(?:disaster recovery|dr plan|business continuity|regional outage|region outage|rto|rpo|"
        r"restore service|recovery objective)\b",
        re.I,
    ),
    "regional_availability": re.compile(
        r"\b(?:regional availability|availability by region|regional health|region health|regional uptime|"
        r"available in (?:us|eu|uk|apac|all regions)|service availability)\b",
        re.I,
    ),
}
_COMPONENT_PATTERNS: dict[RegionalFailoverComponent, re.Pattern[str]] = {
    "database": re.compile(r"\b(?:database|db|postgres|mysql|dynamo|spanner|replica|read replica|schema)\b", re.I),
    "networking": re.compile(r"\b(?:dns|route ?53|cloudflare|load balanc(?:er|ing)|traffic|routing|edge)\b", re.I),
    "operations": re.compile(r"\b(?:runbook|incident|on[- ]call|pager|drill|rehearsal|ops|rollback)\b", re.I),
    "data_platform": re.compile(r"\b(?:data platform|replication|cdc|warehouse|backup|snapshot|restore)\b", re.I),
    "infrastructure": re.compile(r"\b(?:terraform|kubernetes|k8s|cluster|region|infra|deployment|availability zone)\b", re.I),
    "application": re.compile(r"\b(?:api|service|worker|frontend|checkout|login|customer[- ]facing|application)\b", re.I),
}
_TRIGGER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("regional health trigger", re.compile(r"\b(?:health checks?|health signal|synthetic|regional health|readiness probe)\b", re.I)),
    ("manual failover trigger", re.compile(r"\b(?:manual trigger|go/no-go|decision point|incident commander|operator trigger)\b", re.I)),
    ("automatic failover trigger", re.compile(r"\b(?:automatic trigger|auto(?:mated)? failover|threshold|error budget|alarm)\b", re.I)),
    ("replica promotion dependency", re.compile(r"\b(?:promote replica|promote secondary|replica lag|replication lag|rpo|consistency)\b", re.I)),
    ("DNS traffic shift dependency", re.compile(r"\b(?:dns|route ?53|traffic weights?|weighted routing|geo routing|ttl|drain traffic)\b", re.I)),
    ("disaster recovery objective", re.compile(r"\b(?:rto|rpo|recovery objective|business continuity|dr plan)\b", re.I)),
)
_DATA_REPLICATION_RE = re.compile(
    r"\b(?:replication|replica|read replica|cdc|database|snapshot|backup|restore|rpo|consistency|checksum|"
    r"reconciliation|data loss|lag)\b",
    re.I,
)
_VALIDATION_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:validate|validation|test|drill|rehearsal|exercise|synthetic|monitoring|alert|dashboard)\b", re.I),
    re.compile(r"\b(?:checksum|reconciliation|row counts?|consistency check|replica lag|rto|rpo|rollback)\b", re.I),
)


@dataclass(frozen=True, slots=True)
class PlanRegionalFailoverMatrixRow:
    """One failover-relevant task or milestone readiness row."""

    source_id: str
    source_type: str
    component: RegionalFailoverComponent
    region_failover_concern: RegionalFailoverConcern
    trigger_or_dependency: str
    data_replication_concern: str
    validation_gap: str
    recommended_owner: str
    recommended_action: str
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "source_type": self.source_type,
            "component": self.component,
            "region_failover_concern": self.region_failover_concern,
            "trigger_or_dependency": self.trigger_or_dependency,
            "data_replication_concern": self.data_replication_concern,
            "validation_gap": self.validation_gap,
            "recommended_owner": self.recommended_owner,
            "recommended_action": self.recommended_action,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanRegionalFailoverMatrix:
    """Plan-level regional failover readiness matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanRegionalFailoverMatrixRow, ...] = field(default_factory=tuple)
    failover_source_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_source_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanRegionalFailoverMatrixRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "failover_source_ids": list(self.failover_source_ids),
            "no_signal_source_ids": list(self.no_signal_source_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return regional failover matrix rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Regional Failover Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('failover_source_count', 0)} of "
                f"{self.summary.get('source_count', 0)} sources require regional failover planning "
                f"({self.summary.get('row_count', 0)} rows)."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No regional failover matrix rows were inferred."])
            if self.no_signal_source_ids:
                lines.extend(["", f"No failover signals: {_markdown_cell(', '.join(self.no_signal_source_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Source | Component | Concern | Trigger or Dependency | Data Replication | "
                    "Validation Gap | Owner | Recommended Action | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.source_id)}` | "
                f"{row.component} | "
                f"{row.region_failover_concern} | "
                f"{_markdown_cell(row.trigger_or_dependency)} | "
                f"{_markdown_cell(row.data_replication_concern)} | "
                f"{_markdown_cell(row.validation_gap)} | "
                f"{_markdown_cell(row.recommended_owner)} | "
                f"{_markdown_cell(row.recommended_action)} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.no_signal_source_ids:
            lines.extend(["", f"No failover signals: {_markdown_cell(', '.join(self.no_signal_source_ids))}"])
        return "\n".join(lines)


def build_plan_regional_failover_matrix(source: Any) -> PlanRegionalFailoverMatrix:
    """Build regional failover readiness rows for plan tasks and milestones."""
    plan_id, sources = _source_payload(source)
    source_rows = [_source_rows(item, index) for index, item in enumerate(sources, start=1)]
    rows = tuple(row for rows_for_source in source_rows for row in rows_for_source)
    failover_source_ids = tuple(_dedupe(row.source_id for row in rows))
    no_signal_source_ids = tuple(
        _source_id(item, index)
        for index, item in enumerate(sources, start=1)
        if not source_rows[index - 1]
    )
    return PlanRegionalFailoverMatrix(
        plan_id=plan_id,
        rows=rows,
        failover_source_ids=failover_source_ids,
        no_signal_source_ids=no_signal_source_ids,
        summary=_summary(rows, source_count=len(sources), no_signal_source_ids=no_signal_source_ids),
    )


def generate_plan_regional_failover_matrix(source: Any) -> PlanRegionalFailoverMatrix:
    """Generate a regional failover matrix from a plan-like source."""
    return build_plan_regional_failover_matrix(source)


def derive_plan_regional_failover_matrix(source: Any) -> PlanRegionalFailoverMatrix:
    """Derive a regional failover matrix from a plan-like source."""
    if isinstance(source, PlanRegionalFailoverMatrix):
        return source
    return build_plan_regional_failover_matrix(source)


def extract_plan_regional_failover_matrix(source: Any) -> PlanRegionalFailoverMatrix:
    """Extract a regional failover matrix from a plan-like source."""
    return derive_plan_regional_failover_matrix(source)


def summarize_plan_regional_failover_matrix(
    matrix: PlanRegionalFailoverMatrix | Iterable[PlanRegionalFailoverMatrixRow] | Any,
) -> dict[str, Any] | PlanRegionalFailoverMatrix:
    """Return deterministic summary counts for a matrix, row iterable, or source."""
    if isinstance(matrix, PlanRegionalFailoverMatrix):
        return dict(matrix.summary)
    if _looks_like_plan(matrix) or _looks_like_task(matrix) or isinstance(matrix, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_regional_failover_matrix(matrix)
    rows = tuple(matrix)
    return _summary(rows, source_count=len(rows), no_signal_source_ids=())


def plan_regional_failover_matrix_to_dict(matrix: PlanRegionalFailoverMatrix) -> dict[str, Any]:
    """Serialize a regional failover matrix to a plain dictionary."""
    return matrix.to_dict()


plan_regional_failover_matrix_to_dict.__test__ = False


def plan_regional_failover_matrix_to_dicts(
    matrix: PlanRegionalFailoverMatrix | Iterable[PlanRegionalFailoverMatrixRow],
) -> list[dict[str, Any]]:
    """Serialize regional failover matrix rows to plain dictionaries."""
    if isinstance(matrix, PlanRegionalFailoverMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_regional_failover_matrix_to_dicts.__test__ = False


def plan_regional_failover_matrix_to_markdown(matrix: PlanRegionalFailoverMatrix) -> str:
    """Render a regional failover matrix as Markdown."""
    return matrix.to_markdown()


plan_regional_failover_matrix_to_markdown.__test__ = False


def _source_rows(item: Mapping[str, Any], index: int) -> tuple[PlanRegionalFailoverMatrixRow, ...]:
    text_pairs = _candidate_texts(item)
    concern_hits: set[RegionalFailoverConcern] = set()
    evidence: list[str] = []
    joined_text = " ".join(text for _, text in text_pairs)
    for source_field, text in text_pairs:
        matched = False
        for concern, pattern in _CONCERN_PATTERNS.items():
            if pattern.search(text):
                concern_hits.add(concern)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))
    if not concern_hits:
        return ()

    return tuple(
        PlanRegionalFailoverMatrixRow(
            source_id=_source_id(item, index),
            source_type=_source_type(item),
            component=_component(joined_text, item, concern),
            region_failover_concern=concern,
            trigger_or_dependency=_trigger_or_dependency(joined_text, concern),
            data_replication_concern=_data_replication_concern(joined_text, concern),
            validation_gap=_validation_gap(joined_text, concern),
            recommended_owner=_recommended_owner(_component(joined_text, item, concern), concern),
            recommended_action=_recommended_action(_component(joined_text, item, concern), concern, joined_text),
            evidence=tuple(_dedupe(evidence)),
        )
        for concern in _CONCERN_ORDER
        if concern in concern_hits
    )


def _component(
    text: str,
    item: Mapping[str, Any],
    concern: RegionalFailoverConcern,
) -> RegionalFailoverComponent:
    if concern == "dns_routing":
        return "networking"
    if concern == "regional_replica":
        return "database"
    if concern == "disaster_recovery":
        return "operations"
    owner = (_optional_text(item.get("owner_type")) or "").casefold()
    metadata = item.get("metadata")
    if isinstance(metadata, Mapping):
        owner = " ".join([owner, *(_optional_text(metadata.get(key)) or "" for key in ("owner", "team", "component"))])
    haystack = f"{owner} {text}"
    for component in _COMPONENT_ORDER:
        if _COMPONENT_PATTERNS[component].search(haystack):
            return component
    return "infrastructure"


def _trigger_or_dependency(text: str, concern: RegionalFailoverConcern) -> str:
    hits = tuple(label for label, pattern in _TRIGGER_PATTERNS if pattern.search(text))
    if hits:
        return ", ".join(hits)
    return {
        "multi_region_deployment": "Regional deployment dependency is mentioned, but promotion criteria are not explicit.",
        "regional_failover": "Failover trigger is not explicit.",
        "dns_routing": "DNS routing dependency is mentioned, but traffic-shift trigger is not explicit.",
        "regional_replica": "Replica dependency is mentioned, but promotion or lag threshold is not explicit.",
        "disaster_recovery": "Disaster recovery dependency is mentioned, but RTO/RPO trigger is not explicit.",
        "regional_availability": "Regional availability dependency is mentioned, but health trigger is not explicit.",
    }[concern]


def _data_replication_concern(text: str, concern: RegionalFailoverConcern) -> str:
    if not _DATA_REPLICATION_RE.search(text):
        if concern in {"regional_replica", "disaster_recovery"}:
            return "Data replication impact needs explicit validation."
        return "No data replication concern detected."
    if re.search(r"\b(?:checksum|reconciliation|row counts?|consistency check)\b", text, re.I):
        return "Replication consistency validation is specified."
    if re.search(r"\b(?:replica lag|replication lag|rpo|data loss)\b", text, re.I):
        return "Replication lag or data-loss bounds are in scope."
    if re.search(r"\b(?:backup|snapshot|restore)\b", text, re.I):
        return "Backup or restore data currency is in scope."
    return "Replicated data needs consistency, lag, and promotion validation."


def _validation_gap(text: str, concern: RegionalFailoverConcern) -> str:
    has_validation = any(pattern.search(text) for pattern in _VALIDATION_PATTERNS)
    if concern == "regional_replica" and not re.search(r"\b(?:replica lag|checksum|reconciliation|consistency|rpo)\b", text, re.I):
        return "Replica lag, consistency checks, and RPO validation are missing."
    if concern == "dns_routing" and not re.search(r"\b(?:health check|ttl|traffic weights?|synthetic|rollback|drain)\b", text, re.I):
        return "DNS health checks, TTL behavior, and rollback validation are missing."
    if concern == "regional_failover" and not re.search(r"\b(?:trigger|health check|runbook|drill|rehearsal|rollback)\b", text, re.I):
        return "Failover trigger, rehearsal, and rollback validation are missing."
    if concern == "disaster_recovery" and not re.search(r"\b(?:rto|rpo|drill|rehearsal|restore|runbook)\b", text, re.I):
        return "RTO/RPO, restore, and runbook rehearsal validation are missing."
    if not has_validation:
        return "Validation evidence is not explicit."
    return "Validation evidence is present; confirm it is exercised before launch."


def _recommended_owner(component: RegionalFailoverComponent, concern: RegionalFailoverConcern) -> str:
    if concern == "dns_routing" or component == "networking":
        return "networking_owner"
    if concern == "regional_replica" or component in {"database", "data_platform"}:
        return "data_platform_owner"
    if concern == "disaster_recovery" or component == "operations":
        return "operations_owner"
    if concern == "regional_failover":
        return "incident_owner"
    return "platform_owner"


def _recommended_action(component: RegionalFailoverComponent, concern: RegionalFailoverConcern, text: str) -> str:
    if concern == "dns_routing":
        return "Document DNS or load-balancer health checks, traffic weights, TTL behavior, and rollback steps."
    if concern == "disaster_recovery":
        return "Attach the DR runbook with RTO/RPO targets, owners, rehearsal evidence, and restore checks."
    if concern == "regional_failover":
        return "Name the failover trigger, decision owner, traffic shift path, and regional rollback validation."
    if concern == "regional_replica" or component in {"database", "data_platform"}:
        return "Define replica promotion, lag thresholds, consistency checks, and post-failover reconciliation."
    if concern == "regional_availability":
        return "Add regional health signals, availability targets, alerting, and customer-impact validation."
    if re.search(r"\b(?:active[- /]active|active[- /]passive)\b", text, re.I):
        return "Split deployment, routing, data, and operations tasks so each region mode can be verified."
    return "Capture owner, trigger, validation, and rollback expectations for the multi-region deployment."


def _summary(
    rows: tuple[PlanRegionalFailoverMatrixRow, ...],
    *,
    source_count: int,
    no_signal_source_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "row_count": len(rows),
        "failover_source_count": len(tuple(_dedupe(row.source_id for row in rows))),
        "no_signal_source_count": len(no_signal_source_ids),
        "concern_counts": {
            concern: sum(1 for row in rows if row.region_failover_concern == concern)
            for concern in _CONCERN_ORDER
        },
        "component_counts": {
            component: sum(1 for row in rows if row.component == component)
            for component in _COMPONENT_ORDER
        },
        "failover_source_ids": list(tuple(_dedupe(row.source_id for row in rows))),
        "no_signal_source_ids": list(no_signal_source_ids),
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [_mark_source(source.model_dump(mode="python"), "task")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [
            *(_mark_source(task.model_dump(mode="python"), "task") for task in source.tasks),
            *(_mark_source(milestone, "milestone") for milestone in source.milestones),
        ]
    if isinstance(source, Mapping):
        if "tasks" in source or "milestones" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), [
                *(_mark_source(task, "task") for task in _item_payloads(payload.get("tasks"))),
                *(_mark_source(milestone, "milestone") for milestone in _item_payloads(payload.get("milestones"))),
            ]
        return None, [_mark_source(dict(source), "task")]
    if _looks_like_task(source):
        return None, [_mark_source(_object_payload(source), "task")]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), [
            *(_mark_source(task, "task") for task in _item_payloads(payload.get("tasks"))),
            *(_mark_source(milestone, "milestone") for milestone in _item_payloads(payload.get("milestones"))),
        ]

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []
    return None, [_mark_source(item, "task") for item in (_item_payload(value) for value in iterator) if item]


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


def _item_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    return [payload for payload in (_item_payload(item) for item in items) if payload]


def _item_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        payload = value.model_dump(mode="python")
        return dict(payload) if isinstance(payload, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value) or _looks_like_plan(value):
        return _object_payload(value)
    return {}


def _mark_source(item: Mapping[str, Any], source_type: str) -> dict[str, Any]:
    payload = dict(item)
    payload["_source_type"] = source_type
    return payload


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description", "name")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "name",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "milestones",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(item: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "name",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "status",
        "blocked_reason",
    ):
        if text := _optional_text(item.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
    ):
        for index, text in enumerate(_strings(item.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(item.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _any_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
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


def _any_signal(text: str) -> bool:
    return (
        any(pattern.search(text) for pattern in _CONCERN_PATTERNS.values())
        or any(pattern.search(text) for pattern in _COMPONENT_PATTERNS.values())
        or any(pattern.search(text) for _, pattern in _TRIGGER_PATTERNS)
    )


def _source_id(item: Mapping[str, Any], index: int) -> str:
    if text := _optional_text(item.get("id")):
        return text
    if text := _optional_text(item.get("name")):
        return text
    return f"{_source_type(item)}-{index}"


def _source_type(item: Mapping[str, Any]) -> str:
    return _optional_text(item.get("_source_type")) or "task"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        values: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            values.extend(_strings(value[key]))
        return values
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        values: list[str] = []
        for item in items:
            values.extend(_strings(item))
        return values
    text = _optional_text(value)
    return [text] if text else []


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _evidence_snippet(source_field: str, text: str, *, limit: int = 180) -> str:
    normalized = _text(text)
    if len(normalized) > limit:
        normalized = f"{normalized[: limit - 1].rstrip()}..."
    return f"{source_field}: {normalized}"


def _dedupe(values: Iterable[_T]) -> list[_T]:
    seen: set[_T] = set()
    result: list[_T] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")
