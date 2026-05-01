"""Extract implementation decision records from execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask, ImplementationBrief, SourceBrief


DecisionStatus = Literal["decided", "proposed", "inferred"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|(?:\r?\n|;)+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_DECISION_RE = re.compile(
    r"\b(?:decided|decision|choose|chooses|chosen|trade[- ]?off|adr|because|"
    r"alternative|constraint|rationale|must|shall|required|requires|prefer|"
    r"adopt|use|only|avoid|not include|out of scope|follow[- ]?up|followup|"
    r"todo|action required|next step)\b",
    re.I,
)
_EXPLICIT_RE = re.compile(
    r"\b(?:decided|decision|choose|chooses|chosen|adr|constraint|must|shall|"
    r"required|requires|rationale)\b",
    re.I,
)
_ADR_PATH_RE = re.compile(
    r"(?:^|/)(?:adr|adrs|architecture[-_]?decision[-_]?records?)(?:/|$)|"
    r"(?:^|/)\d{3,}[-_].*\.md$|\.adr\.(?:md|markdown)$",
    re.I,
)
_ALTERNATIVE_RE = re.compile(
    r"\b(?:alternative|alternatives|option|options|instead of|rather than)\b[:\s-]*(.+)",
    re.I,
)
_FOLLOW_UP_RE = re.compile(
    r"\b(?:follow[- ]?up|followup|todo|action required|next step|needs?)\b[:\s-]*(.+)",
    re.I,
)
_BECAUSE_RE = re.compile(r"\bbecause\b\s+(.+)", re.I)


@dataclass(frozen=True, slots=True)
class PlanDecisionRecord:
    """One implementation decision surfaced from a plan or related metadata."""

    decision_id: str
    title: str
    status: DecisionStatus
    rationale: str = ""
    alternatives: tuple[str, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    follow_up_actions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "decision_id": self.decision_id,
            "title": self.title,
            "status": self.status,
            "rationale": self.rationale,
            "alternatives": list(self.alternatives),
            "impacted_task_ids": list(self.impacted_task_ids),
            "evidence": list(self.evidence),
            "follow_up_actions": list(self.follow_up_actions),
        }


@dataclass(frozen=True, slots=True)
class PlanDecisionRegister:
    """Plan-level implementation decision register."""

    plan_id: str | None = None
    records: tuple[PlanDecisionRecord, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return decision records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the decision register as deterministic Markdown."""
        title = "# Plan Decision Register"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title, "", "## Summary", ""]
        lines.extend(
            [
                f"- Decision count: {self.summary.get('decision_count', 0)}",
                f"- Decided count: {self.summary.get('decided_count', 0)}",
                f"- Proposed count: {self.summary.get('proposed_count', 0)}",
                f"- Inferred count: {self.summary.get('inferred_count', 0)}",
            ]
        )
        if not self.records:
            lines.extend(["", "No implementation decisions were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Decision | Status | Rationale | Alternatives | Tasks | Evidence | Follow-up |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.decision_id)}` {_markdown_cell(record.title)} | "
                f"{record.status} | "
                f"{_markdown_cell(record.rationale or 'none')} | "
                f"{_markdown_cell('; '.join(record.alternatives) or 'none')} | "
                f"{_markdown_cell(', '.join(record.impacted_task_ids) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(record.follow_up_actions) or 'none')} |"
            )
        return "\n".join(lines)


@dataclass(slots=True)
class _DecisionDraft:
    title: str
    status: DecisionStatus
    rationale: str
    alternatives: list[str] = field(default_factory=list)
    impacted_task_ids: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    follow_up_actions: list[str] = field(default_factory=list)


def build_plan_decision_register(
    plan: Mapping[str, Any] | ExecutionPlan,
    implementation_brief: Mapping[str, Any] | ImplementationBrief | None = None,
    source_brief: Mapping[str, Any] | SourceBrief | None = None,
) -> PlanDecisionRegister:
    """Build an implementation decision register from a plan and optional source metadata."""
    plan_payload = _plan_payload(plan)
    tasks = _task_payloads(plan_payload.get("tasks"))
    drafts: dict[str, _DecisionDraft] = {}

    for draft in _structured_decisions(plan_payload.get("metadata"), "plan.metadata"):
        _merge_draft(drafts, draft)

    for field_path, text in _plan_texts(plan_payload):
        _maybe_add_text_decision(drafts, field_path, text, None)

    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        for draft in _structured_decisions(task.get("metadata"), f"task[{task_id}].metadata"):
            draft.impacted_task_ids.append(task_id)
            _merge_draft(drafts, draft)
        for field_path, text in _task_texts(task, task_id):
            _maybe_add_text_decision(drafts, field_path, text, task_id)
        for path in _strings(task.get("files_or_modules") or task.get("files")):
            if _ADR_PATH_RE.search(_normalized_path(path)):
                _merge_draft(
                    drafts,
                    _DecisionDraft(
                        title=f"Track ADR reference {path}",
                        status="inferred",
                        rationale="Task references an ADR or architecture decision record path.",
                        impacted_task_ids=[task_id],
                        evidence=[f"task[{task_id}].files_or_modules: {path}"],
                        follow_up_actions=[
                            "Review the referenced ADR for export or follow-up workflow needs."
                        ],
                    ),
                )

    if implementation_brief is not None:
        brief = _brief_payload(implementation_brief)
        for field_path, text in _brief_texts(brief, "brief"):
            _maybe_add_text_decision(drafts, field_path, text, None)

    if source_brief is not None:
        source = _source_brief_payload(source_brief)
        for field_path, text in _brief_texts(source, "source"):
            _maybe_add_text_decision(drafts, field_path, text, None)

    records = tuple(
        PlanDecisionRecord(
            decision_id=f"decision-{_slug(draft.title) or index}",
            title=draft.title,
            status=draft.status,
            rationale=draft.rationale,
            alternatives=tuple(_dedupe(draft.alternatives)),
            impacted_task_ids=tuple(sorted(_dedupe(draft.impacted_task_ids))),
            evidence=tuple(_dedupe(draft.evidence)),
            follow_up_actions=tuple(_dedupe(draft.follow_up_actions)),
        )
        for index, draft in enumerate(
            sorted(
                drafts.values(),
                key=lambda item: (_STATUS_ORDER[item.status], item.title.casefold()),
            ),
            start=1,
        )
    )
    status_counts = {
        status: sum(1 for record in records if record.status == status) for status in _STATUS_ORDER
    }
    return PlanDecisionRegister(
        plan_id=_optional_text(plan_payload.get("id")),
        records=records,
        summary={
            "decision_count": len(records),
            "decided_count": status_counts["decided"],
            "proposed_count": status_counts["proposed"],
            "inferred_count": status_counts["inferred"],
            "status_counts": status_counts,
        },
    )


def summarize_plan_decision_register(
    plan: Mapping[str, Any] | ExecutionPlan,
    implementation_brief: Mapping[str, Any] | ImplementationBrief | None = None,
    source_brief: Mapping[str, Any] | SourceBrief | None = None,
) -> PlanDecisionRegister:
    """Compatibility alias for building plan decision registers."""
    return build_plan_decision_register(plan, implementation_brief, source_brief)


def plan_decision_register_to_dict(register: PlanDecisionRegister) -> dict[str, Any]:
    """Serialize a plan decision register to a plain dictionary."""
    return register.to_dict()


plan_decision_register_to_dict.__test__ = False


def plan_decision_register_to_markdown(register: PlanDecisionRegister) -> str:
    """Render a plan decision register as Markdown."""
    return register.to_markdown()


plan_decision_register_to_markdown.__test__ = False


_STATUS_ORDER: dict[DecisionStatus, int] = {"decided": 0, "proposed": 1, "inferred": 2}


def _structured_decisions(value: Any, source_field: str) -> list[_DecisionDraft]:
    decisions: list[_DecisionDraft] = []
    metadata = value if isinstance(value, Mapping) else {}
    for key in ("decisions", "decision_register", "architecture_decisions", "adrs"):
        for index, item in enumerate(_items(metadata.get(key))):
            field_path = f"{source_field}.{key}[{index}]"
            if isinstance(item, Mapping):
                title = (
                    _optional_text(item.get("title"))
                    or _optional_text(item.get("decision"))
                    or _optional_text(item.get("decision_text"))
                    or _optional_text(item.get("summary"))
                )
                if not title:
                    continue
                decisions.append(
                    _DecisionDraft(
                        title=_record_title(title),
                        status=_status(item.get("status"), explicit=True),
                        rationale=_first_text(item, "rationale", "reason", "because", "context"),
                        alternatives=_strings(
                            item.get("alternatives")
                            or item.get("options")
                            or item.get("rejected_options")
                        ),
                        impacted_task_ids=_strings(
                            item.get("impacted_task_ids")
                            or item.get("task_ids")
                            or item.get("tasks")
                        ),
                        evidence=_dedupe(
                            [
                                *_strings(item.get("evidence")),
                                f"{field_path}: {title}",
                            ]
                        ),
                        follow_up_actions=_strings(
                            item.get("follow_up_actions")
                            or item.get("followups")
                            or item.get("follow_ups")
                            or item.get("actions")
                        ),
                    )
                )
            elif text := _optional_text(item):
                decisions.append(_text_draft(field_path, text, None, explicit=True))
    return decisions


def _maybe_add_text_decision(
    drafts: dict[str, _DecisionDraft],
    source_field: str,
    text: str,
    task_id: str | None,
) -> None:
    for sentence in _sentences(text):
        if _is_decision_register_reference(sentence):
            continue
        explicit = bool(_EXPLICIT_RE.search(sentence))
        if not explicit and not _DECISION_RE.search(sentence):
            continue
        _merge_draft(drafts, _text_draft(source_field, sentence, task_id, explicit=explicit))


def _text_draft(
    source_field: str,
    text: str,
    task_id: str | None,
    *,
    explicit: bool,
) -> _DecisionDraft:
    title = _decision_title(text)
    alternatives = _alternatives(text)
    follow_ups = _follow_ups(text)
    rationale = _rationale(text, source_field)
    return _DecisionDraft(
        title=title,
        status=_status(None, explicit=explicit),
        rationale=rationale,
        alternatives=alternatives,
        impacted_task_ids=[task_id] if task_id else [],
        evidence=[_evidence_snippet(source_field, text)],
        follow_up_actions=follow_ups,
    )


def _merge_draft(drafts: dict[str, _DecisionDraft], candidate: _DecisionDraft) -> None:
    key = _dedupe_key(candidate.title)
    existing = drafts.get(key)
    if existing is None:
        drafts[key] = candidate
        return
    existing.status = _stronger_status(existing.status, candidate.status)
    existing.rationale = existing.rationale or candidate.rationale
    existing.alternatives.extend(candidate.alternatives)
    existing.impacted_task_ids.extend(candidate.impacted_task_ids)
    existing.evidence.extend(candidate.evidence)
    existing.follow_up_actions.extend(candidate.follow_up_actions)


def _status(value: Any, *, explicit: bool) -> DecisionStatus:
    normalized = (_optional_text(value) or "").casefold().replace("-", "_").replace(" ", "_")
    if normalized in {"decided", "accepted", "approved", "adopted"}:
        return "decided"
    if normalized in {"proposed", "pending", "open", "draft"}:
        return "proposed"
    if normalized == "inferred":
        return "inferred"
    return "decided" if explicit else "inferred"


def _stronger_status(current: DecisionStatus, candidate: DecisionStatus) -> DecisionStatus:
    return current if _STATUS_ORDER[current] <= _STATUS_ORDER[candidate] else candidate


def _rationale(text: str, source_field: str) -> str:
    if "rationale" in source_field.casefold():
        return text
    if "assumption" in source_field.casefold():
        return f"Assumption: {text}"
    if "risk" in source_field.casefold():
        return f"Risk context: {text}"
    if match := _BECAUSE_RE.search(text):
        return _clean_sentence(match.group(1))
    return ""


def _alternatives(text: str) -> list[str]:
    match = _ALTERNATIVE_RE.search(text)
    return [_clean_sentence(match.group(1))] if match else []


def _follow_ups(text: str) -> list[str]:
    match = _FOLLOW_UP_RE.search(text)
    return [_clean_sentence(match.group(1))] if match else []


def _decision_title(text: str) -> str:
    value = _record_title(text)
    value = re.sub(
        r"^(?:we\s+)?(?:decided|decision|choose|chosen|use|adopt|prefer)\s+(?:to\s+)?",
        "",
        value,
        flags=re.I,
    )
    value = re.sub(
        r"^(?:follow[- ]?up|followup|todo|action required|next step)\s*:\s*",
        "",
        value,
        flags=re.I,
    )
    value = re.sub(r"\s+because\b.+$", "", value, flags=re.I)
    if len(value) > 96:
        value = f"{value[:93].rstrip()}..."
    return value[:1].upper() + value[1:] if value else "Implementation decision"


def _record_title(text: str) -> str:
    value = _clean_sentence(text)
    if len(value) > 96:
        value = f"{value[:93].rstrip()}..."
    return value[:1].upper() + value[1:] if value else "Implementation decision"


def _is_decision_register_reference(text: str) -> bool:
    lowered = text.casefold()
    return "decision register" in lowered and not re.search(
        r"\b(?:decided|choose|chooses|chosen|trade[- ]?off|adr|because|"
        r"alternative|constraint|rationale|must|shall|required|requires|"
        r"prefer|adopt|follow[- ]?up|followup)\b",
        lowered,
    )


def _plan_texts(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "target_engine",
        "target_repo",
        "project_type",
        "test_strategy",
        "handoff_prompt",
        "risk",
        "risks",
        "assumption",
        "assumptions",
        "source_references",
    ):
        for index, text in enumerate(_strings(plan.get(field_name))):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))
    for field_path, text in _metadata_texts(
        _metadata_without_structured_decisions(plan.get("metadata")), "metadata"
    ):
        texts.append((field_path, text))
    return texts


def _task_texts(task: Mapping[str, Any], task_id: str) -> list[tuple[str, str]]:
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
        "implementation_notes",
        "notes",
        "risks",
        "assumptions",
        "source_references",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            suffix = "" if index == 0 else f"[{index}]"
            texts.append((f"task[{task_id}].{field_name}{suffix}", text))
    for field_name in ("acceptance_criteria", "depends_on", "dependencies"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"task[{task_id}].{field_name}[{index}]", text))
    for field_path, text in _metadata_texts(
        _metadata_without_structured_decisions(task.get("metadata")), f"task[{task_id}].metadata"
    ):
        texts.append((field_path, text))
    return texts


def _brief_texts(source: Mapping[str, Any], prefix: str) -> list[tuple[str, str]]:
    fields = (
        "title",
        "summary",
        "scope",
        "non_goals",
        "assumptions",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "validation_plan",
        "definition_of_done",
        "source_payload",
        "source_links",
        "metadata",
    )
    texts: list[tuple[str, str]] = []
    for field_name in fields:
        for field_path, text in _metadata_texts(source.get(field_name), f"{prefix}.{field_name}"):
            texts.append((field_path, text))
    return texts


def _metadata_texts(value: Any, prefix: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field_path = f"{prefix}.{key}"
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field_path))
            elif text := _optional_text(child):
                texts.append((field_path, text))
                if _DECISION_RE.search(key_text):
                    texts.append((field_path, f"{key_text}: {text}"))
            elif _DECISION_RE.search(key_text):
                texts.append((field_path, str(key)))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field_path = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field_path))
            elif text := _optional_text(item):
                texts.append((field_path, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _metadata_without_structured_decisions(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    return {
        key: child
        for key, child in value.items()
        if key
        not in {
            "decisions",
            "decision_register",
            "architecture_decisions",
            "adrs",
        }
    }


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        value = brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ImplementationBrief.model_validate(brief).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(brief) if isinstance(brief, Mapping) else {}


def _source_brief_payload(source: Mapping[str, Any] | SourceBrief) -> dict[str, Any]:
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = SourceBrief.model_validate(source).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(source) if isinstance(source, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, set):
        return sorted(value, key=lambda item: str(item))
    return [value]


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


def _sentences(text: str) -> list[str]:
    return [
        sentence for part in _SPLIT_RE.split(text) if (sentence := _text(_BULLET_RE.sub("", part)))
    ]


def _first_text(source: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        if text := _optional_text(source.get(key)):
            return text
    return ""


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _clean_sentence(value: str) -> str:
    return _text(value).strip(" \t\r\n-–—:;.")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _dedupe_key(text: str) -> str:
    tokens = _TOKEN_RE.findall(text.casefold())
    return " ".join(tokens[:14]) or text.casefold()


def _slug(text: str) -> str:
    return "-".join(_TOKEN_RE.findall(text.casefold())[:8])


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
    "DecisionStatus",
    "PlanDecisionRecord",
    "PlanDecisionRegister",
    "build_plan_decision_register",
    "plan_decision_register_to_dict",
    "plan_decision_register_to_markdown",
    "summarize_plan_decision_register",
]
