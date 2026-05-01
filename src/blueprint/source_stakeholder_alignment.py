"""Extract stakeholder alignment signals from source briefs before planning."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError
import yaml

from blueprint.domain.models import ImplementationBrief, SourceBrief


StakeholderAlignmentStatus = Literal["aligned", "blocking", "informational"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_FRONTMATTER_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*(?:\n|\Z)(.*)\Z", re.S)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$")
_NAME_PREFIX_RE = re.compile(
    r"^\s*(?:[-*+]\s*)?(?P<name>[A-Z][A-Za-z0-9&/| ._-]{1,60}?)(?:\s*\((?P<role>[^)]{1,50})\))?\s*[:\-]\s*(?P<body>.+)$"
)
_APPROVAL_RE = re.compile(
    r"\b(?:approved?|approves|sign(?:ed)? off|sign-off|aligned|supports?|endorsed?|accepted|green[- ]?lit|go[- ]ahead)\b",
    re.I,
)
_BLOCKING_RE = re.compile(
    r"\b(?:blocks?|blocked|blocking|blocker|objects?|objection|opposes?|opposed|rejects?|rejected|"
    r"cannot approve|can't approve|no[- ]go|must not ship|do not ship|hard stop|critical concern)\b",
    re.I,
)
_INFO_RE = re.compile(
    r"\b(?:informational|fyi|keep informed|informed|notify|notified|consulted|observer|read-only|awareness)\b",
    re.I,
)
_QUESTION_RE = re.compile(
    r"\?|(?:\b(?:tbd|unknown|unclear|unresolved|open question|confirm|verify|pending approval|needs approval|"
    r"awaiting approval|need sign[- ]?off|requires sign[- ]?off|who approves|which stakeholder)\b)",
    re.I,
)
_CONFLICT_RE = re.compile(
    r"\b(?:conflict(?:ing)?|disagree(?:ment)?|trade[- ]?off|versus| vs\.? | while | whereas | but )\b",
    re.I,
)
_KNOWN_STAKEHOLDER_RE = re.compile(
    r"\b(?:Legal|Security|Privacy|Compliance|Design|Product|Engineering|Support|Sales|Marketing|Finance|"
    r"Operations|Ops|Customer Success|CS|Data|Analytics|QA|Leadership|Executives|GTM|RevOps)\b"
)
_STATUS_ORDER: dict[StakeholderAlignmentStatus, int] = {
    "blocking": 0,
    "aligned": 1,
    "informational": 2,
}
_SECTION_HINTS: dict[str, StakeholderAlignmentStatus] = {
    "approval": "aligned",
    "approvals": "aligned",
    "signoff": "aligned",
    "sign-off": "aligned",
    "objection": "blocking",
    "objections": "blocking",
    "blocker": "blocking",
    "blockers": "blocking",
    "informed": "informational",
    "informational": "informational",
    "fyi": "informational",
}


@dataclass(frozen=True, slots=True)
class SourceStakeholderRecord:
    """One named source-brief stakeholder alignment signal."""

    stakeholder: str
    status: StakeholderAlignmentStatus
    evidence: tuple[str, ...] = field(default_factory=tuple)
    signals: tuple[str, ...] = field(default_factory=tuple)
    role: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "stakeholder": self.stakeholder,
            "status": self.status,
            "role": self.role,
            "signals": list(self.signals),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceStakeholderAlignmentReport:
    """Source-brief stakeholder alignment report before implementation planning."""

    brief_id: str | None = None
    aligned_stakeholders: tuple[SourceStakeholderRecord, ...] = field(default_factory=tuple)
    blocking_objections: tuple[SourceStakeholderRecord, ...] = field(default_factory=tuple)
    informational_stakeholders: tuple[SourceStakeholderRecord, ...] = field(default_factory=tuple)
    unresolved_alignment_questions: tuple[str, ...] = field(default_factory=tuple)
    conflicting_priorities: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "aligned_stakeholders": [record.to_dict() for record in self.aligned_stakeholders],
            "blocking_objections": [record.to_dict() for record in self.blocking_objections],
            "informational_stakeholders": [record.to_dict() for record in self.informational_stakeholders],
            "unresolved_alignment_questions": list(self.unresolved_alignment_questions),
            "conflicting_priorities": list(self.conflicting_priorities),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> dict[str, list[dict[str, Any]]]:
        """Return stakeholder records grouped by alignment status."""
        return {
            "aligned_stakeholders": [record.to_dict() for record in self.aligned_stakeholders],
            "blocking_objections": [record.to_dict() for record in self.blocking_objections],
            "informational_stakeholders": [record.to_dict() for record in self.informational_stakeholders],
        }

    def to_markdown(self) -> str:
        """Render the alignment report as deterministic Markdown."""
        title = "# Source Stakeholder Alignment"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Aligned stakeholders: {self.summary.get('aligned_count', 0)}",
            f"- Blocking objections: {self.summary.get('blocking_count', 0)}",
            f"- Informational stakeholders: {self.summary.get('informational_count', 0)}",
            f"- Unresolved alignment questions: {self.summary.get('unresolved_question_count', 0)}",
            f"- Conflicting priorities: {self.summary.get('conflicting_priority_count', 0)}",
        ]
        if not (
            self.aligned_stakeholders
            or self.blocking_objections
            or self.informational_stakeholders
            or self.unresolved_alignment_questions
        ):
            lines.extend(["", "No stakeholder alignment signals were found in the source brief."])
            return "\n".join(lines)

        _append_record_table(lines, "Aligned Stakeholders", self.aligned_stakeholders)
        _append_record_table(lines, "Blocking Objections", self.blocking_objections)
        _append_record_table(lines, "Informational Stakeholders", self.informational_stakeholders)
        _append_list_section(lines, "Unresolved Alignment Questions", self.unresolved_alignment_questions)
        _append_list_section(lines, "Conflicting Priorities", self.conflicting_priorities)
        return "\n".join(lines)


def build_source_stakeholder_alignment(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceStakeholderAlignmentReport:
    """Extract stakeholder alignment signals from a source brief, dict, object, or raw text."""
    brief_id, payload = _source_payload(source)
    records = _merge_records([*_explicit_records(payload), *_inferred_records(payload)])
    aligned = tuple(record for record in records if record.status == "aligned")
    blocking = tuple(record for record in records if record.status == "blocking")
    informational = tuple(record for record in records if record.status == "informational")
    questions = tuple(_dedupe([*_explicit_questions(payload), *_inferred_questions(payload)]))
    conflicts = tuple(_dedupe(_conflicting_priorities(payload)))

    return SourceStakeholderAlignmentReport(
        brief_id=brief_id,
        aligned_stakeholders=aligned,
        blocking_objections=blocking,
        informational_stakeholders=informational,
        unresolved_alignment_questions=questions,
        conflicting_priorities=conflicts,
        summary={
            "aligned_count": len(aligned),
            "blocking_count": len(blocking),
            "informational_count": len(informational),
            "unresolved_question_count": len(questions),
            "conflicting_priority_count": len(conflicts),
            "stakeholder_count": len({record.stakeholder.casefold() for record in records}),
            "has_blocking_objections": bool(blocking),
            "ready_for_planning": not blocking and not questions,
        },
    )


def summarize_source_stakeholder_alignment(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceStakeholderAlignmentReport:
    """Compatibility alias for source stakeholder alignment analysis."""
    return build_source_stakeholder_alignment(source)


def source_stakeholder_alignment_to_dict(report: SourceStakeholderAlignmentReport) -> dict[str, Any]:
    """Serialize a source stakeholder alignment report to a plain dictionary."""
    return report.to_dict()


source_stakeholder_alignment_to_dict.__test__ = False


def source_stakeholder_alignment_to_markdown(report: SourceStakeholderAlignmentReport) -> str:
    """Render a source stakeholder alignment report as Markdown."""
    return report.to_markdown()


source_stakeholder_alignment_to_markdown.__test__ = False


def _explicit_records(payload: Mapping[str, Any]) -> list[SourceStakeholderRecord]:
    records: list[SourceStakeholderRecord] = []
    for source_field, metadata in _metadata_sources(payload):
        for field_name, default_status in (
            ("stakeholders", None),
            ("stakeholder_alignment", None),
            ("approvals", "aligned"),
            ("approved_by", "aligned"),
            ("objections", "blocking"),
            ("blocking_objections", "blocking"),
            ("informational_stakeholders", "informational"),
            ("informed", "informational"),
        ):
            for index, item in enumerate(_list_items(_mapping_get(metadata, field_name))):
                record = _record_from_item(item, f"{source_field}.{field_name}[{index}]", default_status)
                if record:
                    records.append(record)
    return records


def _record_from_item(
    item: Any,
    source_field: str,
    default_status: StakeholderAlignmentStatus | None,
) -> SourceStakeholderRecord | None:
    if isinstance(item, Mapping):
        if not any(key in item for key in ("stakeholder", "name", "group", "team", "owner")) and len(item) == 1:
            key, value = next(iter(item.items()))
            item = {"name": str(key), "evidence": value}
        name = _optional_text(
            item.get("stakeholder")
            or item.get("name")
            or item.get("group")
            or item.get("team")
            or item.get("owner")
        )
        role = _optional_text(item.get("role") or item.get("type"))
        evidence = _item_text(item) or _text(item)
        status_text = " ".join(
            _strings(
                [
                    item.get("status"),
                    item.get("alignment"),
                    item.get("approval"),
                    item.get("decision"),
                    item.get("objection"),
                    item.get("priority"),
                    evidence,
                ]
            )
        )
    else:
        evidence = _optional_text(item) or ""
        name, role, status_text = _parse_named_signal(evidence)
    if not name or not evidence:
        return None
    status = default_status or _status(status_text or evidence)
    if not status:
        return None
    return SourceStakeholderRecord(
        stakeholder=_clean_name(name),
        status=status,
        role=role,
        signals=_signals(status_text or evidence, status),
        evidence=(_evidence_snippet(source_field, evidence),),
    )


def _inferred_records(payload: Mapping[str, Any]) -> list[SourceStakeholderRecord]:
    records: list[SourceStakeholderRecord] = []
    current_section: StakeholderAlignmentStatus | None = None
    for source_field, text in _candidate_texts(payload):
        for line_index, line in enumerate(_lines(text)):
            heading = _heading(line)
            if heading is not None:
                current_section = _section_status(heading)
                continue
            body = _BULLET_RE.sub("", line).strip()
            if not body:
                continue
            status = _status(body) or current_section
            if not status:
                continue
            name, role, signal_text = _parse_named_signal(body)
            if not name:
                name = _known_stakeholder(body)
            if not name:
                continue
            records.append(
                SourceStakeholderRecord(
                    stakeholder=_clean_name(name),
                    status=status,
                    role=role,
                    signals=_signals(signal_text or body, status),
                    evidence=(_evidence_snippet(_line_field(source_field, line_index, text), body),),
                )
            )
    return records


def _explicit_questions(payload: Mapping[str, Any]) -> list[str]:
    questions: list[str] = []
    for source_field, metadata in _metadata_sources(payload):
        for field_name in ("alignment_questions", "open_questions", "questions", "unresolved_questions"):
            for item in _strings(_mapping_get(metadata, field_name)):
                questions.append(_evidence_snippet(f"{source_field}.{field_name}", item))
    return questions


def _inferred_questions(payload: Mapping[str, Any]) -> list[str]:
    questions = []
    for source_field, text in _candidate_texts(payload):
        if _question_source(source_field):
            continue
        current_question_section = False
        for line_index, line in enumerate(_lines(text)):
            heading = _heading(line)
            if heading is not None:
                current_question_section = any(word in heading.casefold() for word in ("question", "unresolved"))
                continue
            body = _BULLET_RE.sub("", line).strip()
            if body and (current_question_section or _QUESTION_RE.search(body)):
                questions.append(_evidence_snippet(_line_field(source_field, line_index, text), body))
    return questions


def _conflicting_priorities(payload: Mapping[str, Any]) -> list[str]:
    conflicts = []
    for source_field, text in _candidate_texts(payload):
        for line_index, line in enumerate(_lines(text)):
            body = _BULLET_RE.sub("", line).strip()
            if _CONFLICT_RE.search(f" {body} ") and len(_stakeholders_in_text(body)) >= 2:
                conflicts.append(_evidence_snippet(_line_field(source_field, line_index, text), body))
    return conflicts


def _merge_records(records: Iterable[SourceStakeholderRecord]) -> tuple[SourceStakeholderRecord, ...]:
    grouped: dict[tuple[str, StakeholderAlignmentStatus], list[SourceStakeholderRecord]] = {}
    for record in records:
        grouped.setdefault((record.stakeholder.casefold(), record.status), []).append(record)
    merged = []
    for (_, status), group in grouped.items():
        first = group[0]
        merged.append(
            SourceStakeholderRecord(
                stakeholder=first.stakeholder,
                status=status,
                role=first.role or next((record.role for record in group if record.role), None),
                signals=tuple(_dedupe(signal for record in group for signal in record.signals)),
                evidence=tuple(_dedupe(evidence for record in group for evidence in record.evidence)),
            )
        )
    return tuple(sorted(merged, key=lambda record: (_STATUS_ORDER[record.status], record.stakeholder.casefold())))


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        payload = _markdown_payload(source)
        return _optional_text(payload.get("id")), payload
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = source.model_dump(mode="python")
        return _optional_text(payload.get("id")), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _optional_text(payload.get("id")), payload
    if isinstance(source, Mapping):
        payload = _validated_payload(source)
        return _optional_text(payload.get("id")), payload
    payload = _object_payload(source)
    return _optional_text(payload.get("id")), payload


def _validated_payload(source: Mapping[str, Any]) -> dict[str, Any]:
    for model in (SourceBrief, ImplementationBrief):
        try:
            return dict(model.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            continue
    return dict(source)


def _markdown_payload(text: str) -> dict[str, Any]:
    match = _FRONTMATTER_RE.match(text)
    if not match:
        return {"body": text}
    metadata = yaml.safe_load(match.group(1)) or {}
    if not isinstance(metadata, Mapping):
        metadata = {}
    payload = dict(metadata)
    payload.setdefault("metadata", {})
    if isinstance(payload["metadata"], Mapping):
        payload["metadata"] = dict(payload["metadata"])
    payload["body"] = match.group(2)
    return payload


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    for field_name in ("title", "summary", "body", "description", "problem_statement", "mvp_goal", "workflow_context"):
        if text := _optional_block_text(payload.get(field_name)):
            texts.append((field_name, text))
        if text := _optional_block_text(source_payload.get(field_name)):
            texts.append((f"source_payload.{field_name}", text))
    for field_name in (
        "stakeholders",
        "approvals",
        "objections",
        "requirements",
        "constraints",
        "goals",
        "risks",
        "open_questions",
        "questions",
        "comments",
        "notes",
    ):
        texts.extend(_field_texts(payload.get(field_name), field_name))
        texts.extend(_field_texts(source_payload.get(field_name), f"source_payload.{field_name}"))
    for field, text in _metadata_texts(payload.get("metadata")):
        texts.append((field, text))
    for field, text in _metadata_texts(payload.get("brief_metadata"), "brief_metadata"):
        texts.append((field, text))
    for field, text in _metadata_texts(source_payload.get("metadata"), "source_payload.metadata"):
        texts.append((field, text))
    return [(field, text) for field, text in texts if text]


def _field_texts(value: Any, field_name: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            source_field = f"{field_name}.{key}"
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_field_texts(child, source_field))
            elif text := _optional_block_text(child):
                texts.append((source_field, text))
        return texts
    return [(f"{field_name}[{index}]", text) for index, text in enumerate(_strings(value))]


def _metadata_sources(payload: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    sources: list[tuple[str, Mapping[str, Any]]] = [("brief", payload)]
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    for field_name, value in (
        ("metadata", payload.get("metadata")),
        ("brief_metadata", payload.get("brief_metadata")),
        ("source_payload", source_payload),
        ("source_payload.metadata", source_payload.get("metadata")),
    ):
        if isinstance(value, Mapping):
            sources.append((field_name, value))
    return sources


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_block_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            texts.extend(_metadata_texts(item, f"{prefix}[{index}]"))
        return texts
    text = _optional_block_text(value)
    return [(prefix, text)] if text else []


def _status(text: str) -> StakeholderAlignmentStatus | None:
    if _BLOCKING_RE.search(text):
        return "blocking"
    if _APPROVAL_RE.search(text):
        return "aligned"
    if _INFO_RE.search(text):
        return "informational"
    return None


def _signals(text: str, status: StakeholderAlignmentStatus) -> tuple[str, ...]:
    signals = []
    if status == "blocking" and _BLOCKING_RE.search(text):
        signals.append("blocking_objection")
    if status == "aligned" and _APPROVAL_RE.search(text):
        signals.append("explicit_approval")
    if status == "informational" and _INFO_RE.search(text):
        signals.append("informational")
    return tuple(signals or [status])


def _parse_named_signal(text: str) -> tuple[str | None, str | None, str]:
    match = _NAME_PREFIX_RE.match(text)
    if match:
        return match.group("name"), match.group("role"), match.group("body")
    stakeholder = _known_stakeholder(text)
    return stakeholder, None, text


def _known_stakeholder(text: str) -> str | None:
    match = _KNOWN_STAKEHOLDER_RE.search(text)
    return match.group(0) if match else None


def _stakeholders_in_text(text: str) -> tuple[str, ...]:
    return tuple(_dedupe(match.group(0) for match in _KNOWN_STAKEHOLDER_RE.finditer(text)))


def _section_status(heading: str) -> StakeholderAlignmentStatus | None:
    normalized = re.sub(r"[^a-z0-9-]+", " ", heading.casefold()).strip()
    for key, status in _SECTION_HINTS.items():
        if key in normalized:
            return status
    return None


def _heading(line: str) -> str | None:
    match = _HEADING_RE.match(line)
    return _text(match.group(1)) if match else None


def _lines(text: str) -> list[str]:
    if "\n" in text:
        return [line.rstrip() for line in text.splitlines()]
    return [text]


def _line_field(source_field: str, line_index: int, text: str) -> str:
    return f"{source_field}:line{line_index + 1}" if "\n" in text else source_field


def _question_source(source_field: str) -> bool:
    normalized = source_field.casefold()
    return any(
        key in normalized
        for key in ("alignment_questions", "open_questions", "unresolved_questions", ".questions", "questions[")
    )


def _append_record_table(lines: list[str], heading: str, records: tuple[SourceStakeholderRecord, ...]) -> None:
    if not records:
        return
    lines.extend(
        [
            "",
            f"## {heading}",
            "",
            "| Stakeholder | Role | Signals | Evidence |",
            "| --- | --- | --- | --- |",
        ]
    )
    for record in records:
        lines.append(
            "| "
            f"{_markdown_cell(record.stakeholder)} | "
            f"{_markdown_cell(record.role or 'none')} | "
            f"{_markdown_cell('; '.join(record.signals) or 'none')} | "
            f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
        )


def _append_list_section(lines: list[str], heading: str, values: tuple[str, ...]) -> None:
    if not values:
        return
    lines.extend(["", f"## {heading}", ""])
    lines.extend(f"- {_markdown_cell(value)}" for value in values)


def _mapping_get(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _item_text(item: Any) -> str:
    if isinstance(item, Mapping):
        for key in ("evidence", "text", "summary", "description", "decision", "status", "priority", "reason", "comment"):
            if text := _optional_text(item.get(key)):
                return text
        return ""
    return _optional_text(item) or ""


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
        "source_payload",
        "source_links",
        "metadata",
        "brief_metadata",
        "stakeholders",
        "approvals",
        "objections",
        "open_questions",
        "questions",
        "requirements",
        "constraints",
        "goals",
        "risks",
        "comments",
        "notes",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _list_items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        return [
            {"name": str(key), "evidence": item}
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        ]
    if isinstance(value, (list, tuple, set)):
        return sorted(value, key=lambda item: str(item)) if isinstance(value, set) else list(value)
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


def _clean_name(value: str) -> str:
    return _text(value).strip("`'\".,;")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clip(text)
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _optional_block_text(value: Any) -> str | None:
    text = _block_text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _block_text(value: Any) -> str:
    if value is None:
        return ""
    lines = [_text(line) for line in str(value).splitlines()]
    return "\n".join(lines).strip()


def _clip(value: str) -> str:
    text = _text(value)
    return f"{text[:177].rstrip()}..." if len(text) > 180 else text


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[Any] = set()
    for value in values:
        key = value.casefold() if isinstance(value, str) else value
        if not value or key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "SourceStakeholderAlignmentReport",
    "SourceStakeholderRecord",
    "StakeholderAlignmentStatus",
    "build_source_stakeholder_alignment",
    "source_stakeholder_alignment_to_dict",
    "source_stakeholder_alignment_to_markdown",
    "summarize_source_stakeholder_alignment",
]
