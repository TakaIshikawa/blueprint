"""Evaluate emergency containment readiness for risky execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


KillSwitchRiskLevel = Literal["high", "medium", "low"]
KillSwitchSurface = Literal[
    "feature_launch",
    "data_migration",
    "background_job",
    "external_integration",
    "payment_or_account_flow",
    "broad_user_facing_change",
    "high_risk_change",
]
KillSwitchSafeguard = Literal[
    "feature_flag_or_config_toggle",
    "traffic_ramp_down_control",
    "rollback_command",
    "data_write_pause",
    "dependency_disable_path",
    "verification_after_disablement",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: dict[KillSwitchSurface, int] = {
    "feature_launch": 0,
    "data_migration": 1,
    "external_integration": 2,
    "background_job": 3,
    "payment_or_account_flow": 4,
    "broad_user_facing_change": 5,
    "high_risk_change": 6,
}
_SAFEGUARD_ORDER: dict[KillSwitchSafeguard, int] = {
    "feature_flag_or_config_toggle": 0,
    "traffic_ramp_down_control": 1,
    "rollback_command": 2,
    "data_write_pause": 3,
    "dependency_disable_path": 4,
    "verification_after_disablement": 5,
}
_RISK_ORDER: dict[KillSwitchRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}

_SURFACE_PATTERNS: tuple[tuple[KillSwitchSurface, re.Pattern[str]], ...] = (
    (
        "feature_launch",
        re.compile(
            r"\b(?:launch|rollout|roll out|release|go[- ]live|ship|enable|activate|"
            r"experiment|beta|canary|feature)\b",
            re.I,
        ),
    ),
    (
        "data_migration",
        re.compile(
            r"\b(?:migration|migrate|schema|ddl|backfill|data migration|database migration|"
            r"write migration|dual write|cutover)\b",
            re.I,
        ),
    ),
    (
        "background_job",
        re.compile(
            r"\b(?:background job|worker|queue|consumer|producer|cron|scheduled job|"
            r"batch job|etl|reindex|sync job)\b",
            re.I,
        ),
    ),
    (
        "external_integration",
        re.compile(
            r"\b(?:external|third[- ]party|vendor|partner|integration|webhook|oauth|"
            r"api provider|dependency|stripe|salesforce|slack|pagerduty)\b",
            re.I,
        ),
    ),
    (
        "payment_or_account_flow",
        re.compile(
            r"\b(?:payment|checkout|billing|invoice|subscription|ledger|refund|charge|"
            r"account|login|signup|sign[- ]up|authentication|authorization|permissions?)\b",
            re.I,
        ),
    ),
    (
        "broad_user_facing_change",
        re.compile(
            r"\b(?:all users|all customers|public|customer[- ]facing|user[- ]facing|"
            r"production traffic|global|everyone|entire fleet|tenant-wide|broad rollout)\b",
            re.I,
        ),
    ),
    (
        "high_risk_change",
        re.compile(r"\b(?:high risk|critical|sev[ -]?[0-9]|risky|blast radius|outage)\b", re.I),
    ),
)
_PATH_PATTERNS: tuple[tuple[KillSwitchSurface, re.Pattern[str]], ...] = (
    ("data_migration", re.compile(r"(?:^|/)(?:migrations?|schema|backfills?)(?:/|$)|\.sql$", re.I)),
    ("background_job", re.compile(r"(?:^|/)(?:jobs?|workers?|queues?|cron|consumers?)(?:/|$)", re.I)),
    (
        "external_integration",
        re.compile(r"(?:^|/)(?:integrations?|webhooks?|clients?|providers?|oauth)(?:/|$)", re.I),
    ),
    (
        "payment_or_account_flow",
        re.compile(r"(?:^|/)(?:payments?|billing|checkout|accounts?|auth|permissions?)(?:/|$)", re.I),
    ),
)
_NEGATIVE_RE = re.compile(
    r"\b(?:docs?|documentation|readme|copy|typo|formatting|comment-only|style-only|"
    r"test fixture|mock data|storybook)\b",
    re.I,
)
_SAFEGUARD_TEXT: dict[KillSwitchSafeguard, str] = {
    "feature_flag_or_config_toggle": "Add a feature flag or config toggle that can disable the change.",
    "traffic_ramp_down_control": "Define traffic or rollout ramp-down control for the affected audience.",
    "rollback_command": "Document the rollback command or revert procedure.",
    "data_write_pause": "Provide a data-write pause or read-only mode for affected writes.",
    "dependency_disable_path": "Provide a dependency disable, bypass, or fallback path.",
    "verification_after_disablement": "Verify customer impact and system health after disablement.",
}
_CONTAINMENT_TEXT: dict[KillSwitchSafeguard, str] = {
    "feature_flag_or_config_toggle": "Feature flag or config toggle",
    "traffic_ramp_down_control": "Traffic/ramp-down control",
    "rollback_command": "Rollback command or revert procedure",
    "data_write_pause": "Pause writes or switch to read-only mode",
    "dependency_disable_path": "Disable, bypass, or fall back from the dependency",
    "verification_after_disablement": "Post-disable verification",
}
_SAFEGUARD_PATTERNS: dict[KillSwitchSafeguard, re.Pattern[str]] = {
    "feature_flag_or_config_toggle": re.compile(
        r"\b(?:feature flag|flagged|kill switch|killswitch|config toggle|toggle|"
        r"remote config|runtime config|disable switch|enable switch)\b",
        re.I,
    ),
    "traffic_ramp_down_control": re.compile(
        r"\b(?:ramp[- ]down|ramp down|traffic control|traffic split|rollout percentage|"
        r"canary|wave|gradual rollout|reduce traffic|route traffic|dark launch)\b",
        re.I,
    ),
    "rollback_command": re.compile(
        r"\b(?:rollback|roll back|revert|restore previous|down migration|undo command|"
        r"abort command|deployment rollback)\b",
        re.I,
    ),
    "data_write_pause": re.compile(
        r"\b(?:pause writes?|stop writes?|disable writes?|read[- ]only|write freeze|"
        r"drain queue|pause queue|stop worker|halt backfill|write gate)\b",
        re.I,
    ),
    "dependency_disable_path": re.compile(
        r"\b(?:disable dependency|disable provider|disable integration|bypass|fallback|"
        r"circuit breaker|provider off|webhook off|disconnect vendor)\b",
        re.I,
    ),
    "verification_after_disablement": re.compile(
        r"\b(?:verify|verification|smoke test|health check|dashboard|monitor|confirm|"
        r"post[- ]disable|after disablement|after rollback|customer impact)\b",
        re.I,
    ),
}
_OWNER_RE = re.compile(r"\b(?:owner|approver|approval|dri|on[- ]call|sre|release manager|ops)\b", re.I)
_OWNER_KEYS = (
    "owner",
    "owners",
    "owner_type",
    "assignee",
    "assignees",
    "reviewer",
    "reviewers",
    "approver",
    "approvers",
    "dri",
    "oncall",
    "on_call",
)


@dataclass(frozen=True, slots=True)
class PlanKillSwitchReadinessRecord:
    """Emergency containment readiness for one risky task."""

    task_id: str
    title: str
    kill_switch_surface: KillSwitchSurface
    containment_options: tuple[str, ...] = field(default_factory=tuple)
    missing_acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    owner_approval_hints: tuple[str, ...] = field(default_factory=tuple)
    verification_steps: tuple[str, ...] = field(default_factory=tuple)
    risk_level: KillSwitchRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "kill_switch_surface": self.kill_switch_surface,
            "containment_options": list(self.containment_options),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "owner_approval_hints": list(self.owner_approval_hints),
            "verification_steps": list(self.verification_steps),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanKillSwitchReadinessMatrix:
    """Plan-level kill-switch readiness matrix and rollup counts."""

    plan_id: str | None = None
    records: tuple[PlanKillSwitchReadinessRecord, ...] = field(default_factory=tuple)
    ready_task_ids: tuple[str, ...] = field(default_factory=tuple)
    at_risk_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "ready_task_ids": list(self.ready_task_ids),
            "at_risk_task_ids": list(self.at_risk_task_ids),
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return kill-switch readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    @property
    def findings(self) -> tuple[PlanKillSwitchReadinessRecord, ...]:
        """Compatibility view matching planners that name task records findings."""
        return self.records

    def to_markdown(self) -> str:
        """Render the kill-switch readiness matrix as deterministic Markdown."""
        title = "# Plan Kill Switch Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Readiness record count: {self.summary.get('record_count', 0)}",
            f"- At-risk task count: {self.summary.get('at_risk_task_count', 0)}",
            f"- Missing acceptance criteria count: {self.summary.get('missing_acceptance_criteria_count', 0)}",
            (
                "- Risk counts: "
                f"high {risk_counts.get('high', 0)}, "
                f"medium {risk_counts.get('medium', 0)}, "
                f"low {risk_counts.get('low', 0)}"
            ),
        ]
        if not self.records:
            lines.extend(["", "No kill-switch readiness signals were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Surface | Risk | Containment Options | Missing Acceptance Criteria | Owners/Approvals | Verification | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.kill_switch_surface} | "
                f"{record.risk_level} | "
                f"{_markdown_cell('; '.join(record.containment_options) or 'none')} | "
                f"{_markdown_cell('; '.join(record.missing_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(record.owner_approval_hints) or 'none')} | "
                f"{_markdown_cell('; '.join(record.verification_steps) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_signal_task_ids:
            lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
        return "\n".join(lines)


def build_plan_kill_switch_readiness_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanKillSwitchReadinessMatrix:
    """Evaluate whether risky execution tasks have emergency containment paths."""
    plan_id, plan_context, tasks = _source_payload(source)
    records: list[PlanKillSwitchReadinessRecord] = []
    no_signal_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        record = _record(task, index, plan_context)
        if record:
            records.append(record)
        else:
            no_signal_task_ids.append(task_id)

    records.sort(
        key=lambda record: (
            _RISK_ORDER[record.risk_level],
            _SURFACE_ORDER[record.kill_switch_surface],
            record.task_id,
            record.title.casefold(),
        )
    )
    result = tuple(records)
    ready_task_ids = tuple(record.task_id for record in result if not record.missing_acceptance_criteria)
    at_risk_task_ids = tuple(record.task_id for record in result if record.missing_acceptance_criteria)
    risk_counts = {risk: sum(1 for record in result if record.risk_level == risk) for risk in _RISK_ORDER}
    surface_counts = {
        surface: sum(1 for record in result if record.kill_switch_surface == surface)
        for surface in _SURFACE_ORDER
    }
    return PlanKillSwitchReadinessMatrix(
        plan_id=plan_id,
        records=result,
        ready_task_ids=ready_task_ids,
        at_risk_task_ids=at_risk_task_ids,
        no_signal_task_ids=tuple(no_signal_task_ids),
        summary={
            "task_count": len(tasks),
            "record_count": len(result),
            "ready_task_count": len(ready_task_ids),
            "at_risk_task_count": len(at_risk_task_ids),
            "no_signal_task_count": len(no_signal_task_ids),
            "missing_acceptance_criteria_count": sum(
                len(record.missing_acceptance_criteria) for record in result
            ),
            "risk_counts": risk_counts,
            "surface_counts": surface_counts,
            "ready_task_ids": list(ready_task_ids),
            "at_risk_task_ids": list(at_risk_task_ids),
            "no_signal_task_ids": list(no_signal_task_ids),
        },
    )


def build_plan_kill_switch_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanKillSwitchReadinessMatrix:
    """Compatibility alias for building kill-switch readiness matrices."""
    return build_plan_kill_switch_readiness_matrix(source)


def derive_plan_kill_switch_readiness_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanKillSwitchReadinessMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanKillSwitchReadinessMatrix:
    """Compatibility alias for building kill-switch readiness matrices."""
    if isinstance(source, PlanKillSwitchReadinessMatrix):
        return source
    return build_plan_kill_switch_readiness_matrix(source)


def summarize_plan_kill_switch_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanKillSwitchReadinessMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanKillSwitchReadinessMatrix:
    """Summarize plan kill-switch readiness."""
    return derive_plan_kill_switch_readiness_matrix(source)


def plan_kill_switch_readiness_matrix_to_dict(
    matrix: PlanKillSwitchReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a kill-switch readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_kill_switch_readiness_matrix_to_dict.__test__ = False


def plan_kill_switch_readiness_to_dict(
    matrix: PlanKillSwitchReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a kill-switch readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_kill_switch_readiness_to_dict.__test__ = False


def plan_kill_switch_readiness_matrix_to_markdown(
    matrix: PlanKillSwitchReadinessMatrix,
) -> str:
    """Render a kill-switch readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_kill_switch_readiness_matrix_to_markdown.__test__ = False


def plan_kill_switch_readiness_to_markdown(
    matrix: PlanKillSwitchReadinessMatrix,
) -> str:
    """Render a kill-switch readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_kill_switch_readiness_to_markdown.__test__ = False


def _record(
    task: Mapping[str, Any],
    index: int,
    plan_context: tuple[tuple[str, str], ...],
) -> PlanKillSwitchReadinessRecord | None:
    surfaces, evidence = _signals(task, plan_context)
    if not surfaces:
        return None
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    surface = _primary_surface(surfaces)
    acceptance_context = _acceptance_context(task, plan_context)
    present = tuple(
        safeguard for safeguard in _SAFEGUARD_ORDER if _SAFEGUARD_PATTERNS[safeguard].search(acceptance_context)
    )
    missing = tuple(
        _SAFEGUARD_TEXT[safeguard] for safeguard in _SAFEGUARD_ORDER if safeguard not in present
    )
    return PlanKillSwitchReadinessRecord(
        task_id=task_id,
        title=title,
        kill_switch_surface=surface,
        containment_options=tuple(_CONTAINMENT_TEXT[safeguard] for safeguard in present),
        missing_acceptance_criteria=missing,
        owner_approval_hints=_owner_approval_hints(task, surfaces, acceptance_context),
        verification_steps=_verification_steps(present, missing),
        risk_level=_risk_level(task, surfaces, present, missing),
        evidence=evidence,
    )


def _signals(
    task: Mapping[str, Any],
    plan_context: tuple[tuple[str, str], ...],
) -> tuple[tuple[KillSwitchSurface, ...], tuple[str, ...]]:
    surfaces: list[KillSwitchSurface] = []
    evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        for surface, pattern in _PATH_PATTERNS:
            if pattern.search(normalized):
                surfaces.append(surface)
                evidence.append(f"files_or_modules: {path}")

    for source_field, text in (*_candidate_texts(task), *plan_context):
        if _coverage_only_field(source_field):
            continue
        before = len(surfaces)
        for surface, pattern in _SURFACE_PATTERNS:
            if pattern.search(text):
                surfaces.append(surface)
        if len(surfaces) > before:
            evidence.append(_evidence_snippet(source_field, text))

    surfaces = _ordered_dedupe(surfaces, _SURFACE_ORDER)
    if surfaces and _suppressed(task, plan_context) and set(surfaces) <= {"feature_launch"}:
        return (), ()
    if not surfaces and _suppressed(task, plan_context):
        return (), ()
    return tuple(surfaces), tuple(_dedupe(evidence))


def _primary_surface(surfaces: tuple[KillSwitchSurface, ...]) -> KillSwitchSurface:
    for surface in ("data_migration", "external_integration", "background_job"):
        if surface in surfaces:
            return surface
    return surfaces[0]


def _risk_level(
    task: Mapping[str, Any],
    surfaces: tuple[KillSwitchSurface, ...],
    present: tuple[KillSwitchSafeguard, ...],
    missing: tuple[str, ...],
) -> KillSwitchRiskLevel:
    if not missing:
        return "low"
    explicit_high = _explicit_high_risk(task)
    broad_or_sensitive = any(
        surface in {"broad_user_facing_change", "payment_or_account_flow", "data_migration"}
        for surface in surfaces
    )
    has_containment_path = any(
        safeguard in present
        for safeguard in (
            "feature_flag_or_config_toggle",
            "traffic_ramp_down_control",
            "rollback_command",
            "data_write_pause",
            "dependency_disable_path",
        )
    )
    if (explicit_high or broad_or_sensitive) and not has_containment_path:
        return "high"
    if len(missing) >= 4:
        return "high"
    return "medium"


def _explicit_high_risk(task: Mapping[str, Any]) -> bool:
    values = [*_strings(task.get("risk_level")), *_strings(task.get("risks"))]
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        values.extend(_strings(metadata.get("risk_level")))
        values.extend(_strings(metadata.get("risk")))
    return any(re.search(r"\b(?:high|critical|blocker|sev[ -]?[0-9])\b", value, re.I) for value in values)


def _owner_approval_hints(
    task: Mapping[str, Any],
    surfaces: tuple[KillSwitchSurface, ...],
    acceptance_context: str,
) -> tuple[str, ...]:
    hints: list[str] = []
    for key in _OWNER_KEYS:
        hints.extend(_strings(task.get(key)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in _OWNER_KEYS:
            hints.extend(_strings(metadata.get(key)))
    if _OWNER_RE.search(acceptance_context):
        hints.append("approval noted in acceptance criteria")
    if not hints:
        if "payment_or_account_flow" in surfaces:
            hints.extend(["payments owner", "release approver"])
        elif "external_integration" in surfaces:
            hints.extend(["integration owner", "vendor escalation owner"])
        elif "data_migration" in surfaces:
            hints.extend(["database owner", "release approver"])
        elif "background_job" in surfaces:
            hints.extend(["operations owner", "on-call owner"])
        else:
            hints.extend(["feature owner", "release approver"])
    return tuple(_dedupe(hints))


def _verification_steps(
    present: tuple[KillSwitchSafeguard, ...],
    missing: tuple[str, ...],
) -> tuple[str, ...]:
    steps = [
        "Exercise the disablement path in staging or a controlled canary.",
        "Confirm metrics, logs, and customer-facing behavior recover after disablement.",
    ]
    if "rollback_command" in present:
        steps.append("Run or dry-run the rollback command before launch approval.")
    if missing:
        steps.append("Block autonomous execution until missing containment criteria are added.")
    return tuple(steps)


def _acceptance_context(task: Mapping[str, Any], plan_context: tuple[tuple[str, str], ...]) -> str:
    values: list[str] = []
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "risk",
        "validation_commands",
        "test_command",
        "blocked_reason",
    ):
        values.extend(_strings(task.get(field_name)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for source_field, text in _metadata_texts(metadata):
            normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
            if any(
                keyword in normalized
                for keyword in (
                    "acceptance",
                    "criteria",
                    "kill",
                    "rollback",
                    "disable",
                    "containment",
                    "verification",
                    "approval",
                    "owner",
                    "validation",
                    "test",
                )
            ):
                values.append(text)
    values.extend(text for source_field, text in plan_context if _context_field_is_acceptance(source_field))
    return " ".join(values)


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
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("risks", "tags", "labels"):
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
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, tuple[tuple[str, str], ...], list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return (
            _optional_text(payload.get("id")),
            _plan_context(payload),
            _task_payloads(payload.get("tasks")),
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return _optional_text(plan.get("id")), _plan_context(plan), _task_payloads(plan.get("tasks"))
        return None, (), [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return _optional_text(plan.get("id")), _plan_context(plan), _task_payloads(plan.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, (), []
    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, (), tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _plan_context(plan: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for field_name in ("test_strategy", "handoff_prompt", "generation_prompt"):
        if text := _optional_text(plan.get(field_name)):
            texts.append((f"plan.{field_name}", text))
    for source_field, text in _metadata_texts(plan.get("metadata"), "plan.metadata"):
        texts.append((source_field, text))
    return tuple(texts)


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
    if value is None or isinstance(value, (str, bytes)):
        return {}
    data: dict[str, Any] = {}
    for name in dir(value):
        if name.startswith("_"):
            continue
        try:
            item = getattr(value, name)
        except Exception:
            continue
        if not callable(item):
            data[name] = item
    return data


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                key_text = str(key).replace("_", " ").replace("-", " ")
                texts.append((field, f"{key_text}: {text}"))
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


def _coverage_only_field(source_field: str) -> bool:
    normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
    return normalized.startswith(
        (
            "acceptance_criteria",
            "criteria",
            "definition_of_done",
            "metadata.acceptance",
            "metadata.criteria",
            "metadata.validation",
        )
    )


def _context_field_is_acceptance(source_field: str) -> bool:
    normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
    return any(
        keyword in normalized
        for keyword in (
            "acceptance",
            "definition",
            "validation",
            "test",
            "kill",
            "rollback",
            "disable",
            "containment",
            "approval",
        )
    )


def _suppressed(task: Mapping[str, Any], plan_context: tuple[tuple[str, str], ...]) -> bool:
    text = " ".join(value for _, value in (*_candidate_texts(task), *plan_context))
    return bool(_NEGATIVE_RE.search(text))


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/").casefold()


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


def _ordered_dedupe(values: Iterable[_T], preferred_order: Mapping[_T, int]) -> list[_T]:
    present = set(values)
    ordered = [item for item in preferred_order if item in present]
    ordered.extend(value for value in values if value not in ordered)
    return ordered


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
    "PlanKillSwitchReadinessMatrix",
    "PlanKillSwitchReadinessRecord",
    "build_plan_kill_switch_readiness",
    "build_plan_kill_switch_readiness_matrix",
    "derive_plan_kill_switch_readiness_matrix",
    "summarize_plan_kill_switch_readiness",
    "plan_kill_switch_readiness_to_dict",
    "plan_kill_switch_readiness_to_markdown",
    "plan_kill_switch_readiness_matrix_to_dict",
    "plan_kill_switch_readiness_matrix_to_markdown",
]
