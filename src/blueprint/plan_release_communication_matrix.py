"""Build release communication matrices from implementation briefs and execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask, ImplementationBrief


ReleaseCommunicationCategory = Literal[
    "internal_announcement",
    "customer_announcement",
    "status_page_update",
    "release_notes",
    "support_enablement",
    "sales_enablement",
    "incident_comms",
]
ReleaseCommunicationStatus = Literal["needs_plan", "needs_draft", "ready_to_schedule"]

_SPACE_RE = re.compile(r"\s+")
_CATEGORY_ORDER: dict[ReleaseCommunicationCategory, int] = {
    "internal_announcement": 0,
    "customer_announcement": 1,
    "status_page_update": 2,
    "release_notes": 3,
    "support_enablement": 4,
    "sales_enablement": 5,
    "incident_comms": 6,
}
_CATEGORY_PATTERNS: dict[ReleaseCommunicationCategory, re.Pattern[str]] = {
    "internal_announcement": re.compile(
        r"\b(?:internal announcement|internal comms?|team announcement|launch announce|"
        r"announce internally|stakeholder update|go[- ]to[- ]market|gtm|rollout notice|"
        r"launch communication|release communication)\b",
        re.I,
    ),
    "customer_announcement": re.compile(
        r"\b(?:customer announcement|announce to customers?|customer email|email customers?|"
        r"customer[- ]facing announcement|customer[- ]visible|customer[- ]facing|"
        r"end[- ]user announcement|tenant owner announcement|in[- ]app (?:message|banner|notification)|"
        r"notify customers?|external announcement)\b",
        re.I,
    ),
    "status_page_update": re.compile(
        r"\b(?:status page|statuspage|maintenance notice|maintenance window|scheduled maintenance|"
        r"downtime|degradation|service interruption|availability notice|incident update|"
        r"public status)\b",
        re.I,
    ),
    "release_notes": re.compile(
        r"\b(?:release notes?|changelog|change log|what'?s new|customer docs?|"
        r"documentation update|docs update|help center article|public docs?|launch notes?)\b",
        re.I,
    ),
    "support_enablement": re.compile(
        r"\b(?:support enablement|support brief|support macro|helpdesk|ticket macro|"
        r"support runbook|support agents?|support team|cs enablement|customer success enablement|"
        r"faq|known issues?|triage guide)\b",
        re.I,
    ),
    "sales_enablement": re.compile(
        r"\b(?:sales enablement|sales team|sales deck|battlecard|pricing page|"
        r"commercial narrative|account executives?|ae enablement|customer pitch|"
        r"talk track|renewal|expansion|upsell)\b",
        re.I,
    ),
    "incident_comms": re.compile(
        r"\b(?:incident comms?|incident communication|rollback comms?|rollback message|"
        r"all clear|postmortem|sev[ -]?[0123]|outage|degradation|rollback|"
        r"kill switch|launch watch|war room|escalation)\b",
        re.I,
    ),
}
@dataclass(frozen=True, slots=True)
class PlanReleaseCommunicationMatrixRow:
    """One release communication row grouped by communication category."""

    category: ReleaseCommunicationCategory
    communication_status: ReleaseCommunicationStatus
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_owner: str = ""
    recommended_channel: str = ""
    required_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "communication_status": self.communication_status,
            "affected_task_ids": list(self.affected_task_ids),
            "evidence": list(self.evidence),
            "recommended_owner": self.recommended_owner,
            "recommended_channel": self.recommended_channel,
            "required_questions": list(self.required_questions),
        }


@dataclass(frozen=True, slots=True)
class PlanReleaseCommunicationMatrix:
    """Release communication matrix for a brief, plan, or brief-plus-plan pair."""

    brief_id: str | None = None
    plan_id: str | None = None
    rows: tuple[PlanReleaseCommunicationMatrixRow, ...] = field(default_factory=tuple)

    @property
    def summary(self) -> dict[str, Any]:
        """Return compact rollup counts in stable key order."""
        return {
            "category_count": len(self.rows),
            "affected_task_count": len(
                {task_id for row in self.rows for task_id in row.affected_task_ids}
            ),
            "needs_plan_count": sum(
                1 for row in self.rows if row.communication_status == "needs_plan"
            ),
            "needs_draft_count": sum(
                1 for row in self.rows if row.communication_status == "needs_draft"
            ),
            "ready_to_schedule_count": sum(
                1 for row in self.rows if row.communication_status == "ready_to_schedule"
            ),
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
        """Return release communication rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Release Communication Matrix"
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
        lines = [title]
        if not self.rows:
            lines.extend(["", "No release communication rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Category | Status | Affected Tasks | Evidence | Owner | Channel | "
                    "Required Questions |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.category} | "
                f"{row.communication_status} | "
                f"{_markdown_cell(', '.join(row.affected_task_ids) or 'brief')} | "
                f"{_markdown_cell('; '.join(row.evidence))} | "
                f"{_markdown_cell(row.recommended_owner)} | "
                f"{_markdown_cell(row.recommended_channel)} | "
                f"{_markdown_cell('; '.join(row.required_questions))} |"
            )
        return "\n".join(lines)


def build_plan_release_communication_matrix(
    brief: Mapping[str, Any] | ImplementationBrief | ExecutionPlan | None = None,
    plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> PlanReleaseCommunicationMatrix:
    """Build release communication rows from an implementation brief and/or execution plan."""
    brief_payload: dict[str, Any] = {}
    plan_payload: dict[str, Any] = {}

    if plan is None and _looks_like_plan(brief):
        plan_payload = _plan_payload(brief)
    else:
        brief_payload = _brief_payload(brief)
        plan_payload = _plan_payload(plan)

    builders: dict[ReleaseCommunicationCategory, _RowBuilder] = {
        category: _RowBuilder(category) for category in _CATEGORY_ORDER
    }

    for source_field, text in _brief_texts(brief_payload):
        _add_signals(builders, source_field, text, task_id=None)

    tasks = _task_payloads(plan_payload.get("tasks"))
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        for source_field, text in _task_texts(task):
            _add_signals(builders, source_field, text, task_id=task_id)

    for source_field, text in _plan_texts(plan_payload):
        _add_signals(builders, source_field, text, task_id=None)

    rows = tuple(
        _row(builder)
        for builder in sorted(builders.values(), key=lambda item: _CATEGORY_ORDER[item.category])
        if builder.evidence
    )
    return PlanReleaseCommunicationMatrix(
        brief_id=_optional_text(brief_payload.get("id")),
        plan_id=_optional_text(plan_payload.get("id")),
        rows=rows,
    )


def generate_plan_release_communication_matrix(
    brief: Mapping[str, Any] | ImplementationBrief | ExecutionPlan | None = None,
    plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> PlanReleaseCommunicationMatrix:
    """Compatibility alias for building release communication matrices."""
    return build_plan_release_communication_matrix(brief, plan)


def plan_release_communication_matrix_to_dict(
    matrix: PlanReleaseCommunicationMatrix,
) -> dict[str, Any]:
    """Serialize a release communication matrix to a plain dictionary."""
    return matrix.to_dict()


plan_release_communication_matrix_to_dict.__test__ = False


def plan_release_communication_matrix_to_dicts(
    matrix: PlanReleaseCommunicationMatrix,
) -> list[dict[str, Any]]:
    """Serialize release communication matrix rows to plain dictionaries."""
    return matrix.to_dicts()


plan_release_communication_matrix_to_dicts.__test__ = False


def plan_release_communication_matrix_to_markdown(
    matrix: PlanReleaseCommunicationMatrix,
) -> str:
    """Render a release communication matrix as Markdown."""
    return matrix.to_markdown()


plan_release_communication_matrix_to_markdown.__test__ = False


@dataclass(slots=True)
class _RowBuilder:
    category: ReleaseCommunicationCategory
    task_ids: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)


def _row(builder: _RowBuilder) -> PlanReleaseCommunicationMatrixRow:
    category = builder.category
    return PlanReleaseCommunicationMatrixRow(
        category=category,
        communication_status=_status(category),
        affected_task_ids=tuple(_dedupe(builder.task_ids)),
        evidence=tuple(_dedupe(builder.evidence)),
        recommended_owner=_owner(category),
        recommended_channel=_channel(category),
        required_questions=_questions(category),
    )


def _status(category: ReleaseCommunicationCategory) -> ReleaseCommunicationStatus:
    if category in {"status_page_update", "incident_comms"}:
        return "needs_plan"
    return "needs_draft"


def _add_signals(
    builders: dict[ReleaseCommunicationCategory, _RowBuilder],
    source_field: str,
    text: str,
    *,
    task_id: str | None,
) -> None:
    snippet = _evidence(source_field, text)
    for category, pattern in _CATEGORY_PATTERNS.items():
        if not pattern.search(text):
            continue
        builder = builders[category]
        if task_id:
            builder.task_ids.append(task_id)
        builder.evidence.append(snippet)


def _owner(category: ReleaseCommunicationCategory) -> str:
    owners: dict[ReleaseCommunicationCategory, str] = {
        "internal_announcement": "product manager",
        "customer_announcement": "customer marketing",
        "status_page_update": "operations lead",
        "release_notes": "product marketing",
        "support_enablement": "support enablement lead",
        "sales_enablement": "sales enablement lead",
        "incident_comms": "incident commander",
    }
    return owners[category]


def _channel(category: ReleaseCommunicationCategory) -> str:
    channels: dict[ReleaseCommunicationCategory, str] = {
        "internal_announcement": "internal launch channel",
        "customer_announcement": "customer email and in-app notification",
        "status_page_update": "public status page",
        "release_notes": "release notes and changelog",
        "support_enablement": "support knowledge base and macros",
        "sales_enablement": "sales enablement workspace",
        "incident_comms": "incident channel and status updates",
    }
    return channels[category]


def _questions(category: ReleaseCommunicationCategory) -> tuple[str, ...]:
    questions: dict[ReleaseCommunicationCategory, tuple[str, ...]] = {
        "internal_announcement": (
            "Which internal teams need launch timing, scope, owner, and escalation context?",
            "What decision, action, or awareness is expected from each internal audience?",
        ),
        "customer_announcement": (
            "Which customer segments, admins, or end users should receive the announcement?",
            "What customer-safe value, impact, timing, and action should the message include?",
        ),
        "status_page_update": (
            "Does the launch require a scheduled maintenance, degradation, or availability notice?",
            "Who approves status page timing, wording, and follow-up cadence?",
        ),
        "release_notes": (
            "Which user-facing changes, limitations, and migration notes belong in release notes?",
            "Where should customers find the durable changelog or help center documentation?",
        ),
        "support_enablement": (
            "What symptoms, FAQs, macros, known issues, and escalation paths does support need?",
            "How will support distinguish expected launch behavior from defects or incidents?",
        ),
        "sales_enablement": (
            "Which positioning, packaging, pricing, or objection-handling points does sales need?",
            "Which customer segments, opportunities, or renewals should sales prioritize?",
        ),
        "incident_comms": (
            "What rollback, degradation, customer-impact, and all-clear messages are pre-drafted?",
            "Who sends incident updates and at what cadence during launch watch?",
        ),
    }
    return questions[category]


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
    for source_field, text in _metadata_texts(brief.get("metadata"), prefix="brief.metadata"):
        texts.append((source_field, text))
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
    "PlanReleaseCommunicationMatrix",
    "PlanReleaseCommunicationMatrixRow",
    "ReleaseCommunicationCategory",
    "ReleaseCommunicationStatus",
    "build_plan_release_communication_matrix",
    "generate_plan_release_communication_matrix",
    "plan_release_communication_matrix_to_dict",
    "plan_release_communication_matrix_to_dicts",
    "plan_release_communication_matrix_to_markdown",
]
