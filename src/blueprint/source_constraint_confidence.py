"""Score source-backed confidence for implementation constraints."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ConstraintConfidenceLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_-]*")
_CONFIDENCE_ORDER: dict[ConstraintConfidenceLevel, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_QUESTION_RE = re.compile(r"\?|(?:\b(?:tbd|unknown|unclear|confirm|verify|whether|should we|open question)\b)", re.I)
_CONTRADICTION_RE = re.compile(
    r"\b(?:contradict|conflict|inconsistent|blocked by|cannot|can't|but source says|however|except|not\s+yet|unknown)\b",
    re.I,
)
_OWNER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("design", re.compile(r"\b(?:design|ux|ui|copy|content|accessibility|a11y|visual)\b", re.I)),
    ("product", re.compile(r"\b(?:scope|mvp|workflow|acceptance|requirement|priority|user|buyer)\b", re.I)),
    ("security", re.compile(r"\b(?:security|auth|permissions?|privacy|pii|compliance|audit)\b", re.I)),
    ("data", re.compile(r"\b(?:data|schema|migration|analytics|event|metric|report)\b", re.I)),
    ("engineering", re.compile(r"\b(?:api|backend|frontend|integration|performance|latency|cache|database)\b", re.I)),
)


@dataclass(frozen=True, slots=True)
class SourceConstraintConfidenceFinding:
    """Confidence assessment for one implementation constraint."""

    constraint_text: str
    confidence_level: ConstraintConfidenceLevel
    evidence_score: int
    source_references: tuple[str, ...] = field(default_factory=tuple)
    acceptance_criteria_support: tuple[str, ...] = field(default_factory=tuple)
    repeated_evidence: tuple[str, ...] = field(default_factory=tuple)
    assumption_support: tuple[str, ...] = field(default_factory=tuple)
    risk_support: tuple[str, ...] = field(default_factory=tuple)
    contradiction_signals: tuple[str, ...] = field(default_factory=tuple)
    question_signals: tuple[str, ...] = field(default_factory=tuple)
    recommended_follow_up: tuple[str, ...] = field(default_factory=tuple)
    owner_suggestion: str = "product"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "constraint_text": self.constraint_text,
            "confidence_level": self.confidence_level,
            "evidence_score": self.evidence_score,
            "source_references": list(self.source_references),
            "acceptance_criteria_support": list(self.acceptance_criteria_support),
            "repeated_evidence": list(self.repeated_evidence),
            "assumption_support": list(self.assumption_support),
            "risk_support": list(self.risk_support),
            "contradiction_signals": list(self.contradiction_signals),
            "question_signals": list(self.question_signals),
            "recommended_follow_up": list(self.recommended_follow_up),
            "owner_suggestion": self.owner_suggestion,
        }


@dataclass(frozen=True, slots=True)
class SourceConstraintConfidenceReport:
    """Brief-level source confidence report and summary counts."""

    brief_id: str | None = None
    constraints: tuple[SourceConstraintConfidenceFinding, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "constraints": [constraint.to_dict() for constraint in self.constraints],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return constraint findings as plain dictionaries."""
        return [constraint.to_dict() for constraint in self.constraints]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown with confidence counts."""
        title = "# Source Constraint Confidence Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Constraints analyzed: {self.summary.get('constraint_count', 0)}",
            (
                "- Confidence counts: "
                f"high {confidence_counts.get('high', 0)}, "
                f"medium {confidence_counts.get('medium', 0)}, "
                f"low {confidence_counts.get('low', 0)}"
            ),
            f"- Follow-ups recommended: {self.summary.get('follow_up_count', 0)}",
            f"- Contradiction-flagged constraints: {self.summary.get('contradiction_count', 0)}",
            f"- Question-backed constraints: {self.summary.get('question_count', 0)}",
        ]
        if not self.constraints:
            lines.extend(["", "No implementation constraints were available for confidence scoring."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Constraints",
                "",
                "| Constraint | Confidence | Score | Evidence | Signals | Follow-up | Owner |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for constraint in self.constraints:
            evidence = _dedupe(
                [
                    *constraint.source_references,
                    *constraint.acceptance_criteria_support,
                    *constraint.repeated_evidence,
                    *constraint.assumption_support,
                    *constraint.risk_support,
                ]
            )
            signals = _dedupe([*constraint.contradiction_signals, *constraint.question_signals])
            lines.append(
                "| "
                f"{_markdown_cell(constraint.constraint_text)} | "
                f"{constraint.confidence_level} | "
                f"{constraint.evidence_score} | "
                f"{_markdown_cell('; '.join(evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(signals) or 'none')} | "
                f"{_markdown_cell('; '.join(constraint.recommended_follow_up) or 'none')} | "
                f"{_markdown_cell(constraint.owner_suggestion)} |"
            )
        return "\n".join(lines)


def build_source_constraint_confidence_report(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceConstraintConfidenceReport:
    """Score implementation constraints using local source and brief evidence."""
    brief_id, payload = _source_payload(source)
    extracted = _extract_payload(payload)
    constraints = extracted.constraints
    corpus = _evidence_corpus(extracted)
    findings = tuple(
        sorted(
            (_finding(constraint, extracted, corpus) for constraint in constraints),
            key=lambda finding: (
                _CONFIDENCE_ORDER[finding.confidence_level],
                -finding.evidence_score,
                finding.constraint_text.casefold(),
            ),
        )
    )
    confidence_counts = {
        level: sum(1 for finding in findings if finding.confidence_level == level)
        for level in _CONFIDENCE_ORDER
    }
    return SourceConstraintConfidenceReport(
        brief_id=brief_id,
        constraints=findings,
        summary={
            "constraint_count": len(findings),
            "confidence_counts": confidence_counts,
            "follow_up_count": sum(1 for finding in findings if finding.recommended_follow_up),
            "contradiction_count": sum(1 for finding in findings if finding.contradiction_signals),
            "question_count": sum(1 for finding in findings if finding.question_signals),
            "source_reference_count": sum(1 for finding in findings if finding.source_references),
        },
    )


def summarize_source_constraint_confidence(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceConstraintConfidenceReport:
    """Compatibility alias for building source constraint confidence reports."""
    return build_source_constraint_confidence_report(source)


def source_constraint_confidence_report_to_dict(
    report: SourceConstraintConfidenceReport,
) -> dict[str, Any]:
    """Serialize a source constraint confidence report to a plain dictionary."""
    return report.to_dict()


source_constraint_confidence_report_to_dict.__test__ = False


def source_constraint_confidence_report_to_markdown(
    report: SourceConstraintConfidenceReport,
) -> str:
    """Render a source constraint confidence report as Markdown."""
    return report.to_markdown()


source_constraint_confidence_report_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _ConstraintCandidate:
    text: str
    field: str
    source_references: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class _ExtractedPayload:
    constraints: tuple[_ConstraintCandidate, ...] = field(default_factory=tuple)
    acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    assumptions: tuple[str, ...] = field(default_factory=tuple)
    risks: tuple[str, ...] = field(default_factory=tuple)
    open_questions: tuple[str, ...] = field(default_factory=tuple)
    source_references: tuple[str, ...] = field(default_factory=tuple)
    metadata_texts: tuple[str, ...] = field(default_factory=tuple)


def _finding(
    constraint: _ConstraintCandidate,
    extracted: _ExtractedPayload,
    corpus: tuple[tuple[str, str], ...],
) -> SourceConstraintConfidenceFinding:
    text = constraint.text
    references = tuple(_dedupe([*constraint.source_references, *extracted.source_references]))
    acceptance_support = _supporting_texts(text, extracted.acceptance_criteria, "acceptance_criteria")
    repeated = _repeated_evidence(text, corpus)
    assumption_support = _supporting_texts(text, extracted.assumptions, "assumptions")
    risk_support = _supporting_texts(text, extracted.risks, "risks")
    contradictions = _signal_texts(text, [*extracted.risks, *extracted.metadata_texts], _CONTRADICTION_RE)
    questions = _signal_texts(text, extracted.open_questions, _QUESTION_RE)
    if _QUESTION_RE.search(text):
        questions = tuple(_dedupe([*questions, f"constraint: {_clip(text)}"]))
    if _CONTRADICTION_RE.search(text):
        contradictions = tuple(_dedupe([*contradictions, f"constraint: {_clip(text)}"]))

    score = 0
    if constraint.source_references:
        score += 3
    elif references:
        score += 1
    if acceptance_support:
        score += 2
    if repeated:
        score += 2
    if assumption_support:
        score += 1
    score -= 3 * int(bool(contradictions))
    score -= 2 * int(bool(questions))
    if re.search(r"\b(?:assume|assuming|assumption)\b", text, re.I):
        score -= 2
    score = max(score, 0)
    confidence = _confidence(score, contradictions, questions)
    return SourceConstraintConfidenceFinding(
        constraint_text=text,
        confidence_level=confidence,
        evidence_score=score,
        source_references=references,
        acceptance_criteria_support=acceptance_support,
        repeated_evidence=repeated,
        assumption_support=assumption_support,
        risk_support=risk_support,
        contradiction_signals=contradictions,
        question_signals=questions,
        recommended_follow_up=_follow_up(confidence, contradictions, questions, references, acceptance_support),
        owner_suggestion=_owner_suggestion(text, contradictions, questions),
    )


def _confidence(
    score: int,
    contradictions: tuple[str, ...],
    questions: tuple[str, ...],
) -> ConstraintConfidenceLevel:
    if contradictions or questions:
        return "low" if score < 4 else "medium"
    if score >= 5:
        return "high"
    if score >= 3:
        return "medium"
    return "low"


def _follow_up(
    confidence: ConstraintConfidenceLevel,
    contradictions: tuple[str, ...],
    questions: tuple[str, ...],
    references: tuple[str, ...],
    acceptance_support: tuple[str, ...],
) -> tuple[str, ...]:
    follow_up: list[str] = []
    if contradictions:
        follow_up.append("Resolve contradictory source signals before turning this constraint into execution requirements.")
    if questions:
        follow_up.append("Answer the linked open question or replace the constraint with an explicit product decision.")
    if not references:
        follow_up.append("Attach a source reference, link, ticket, or source payload field for traceability.")
    if not acceptance_support:
        follow_up.append("Add or update acceptance criteria that prove this constraint is intentionally required.")
    if confidence == "low" and not follow_up:
        follow_up.append("Confirm this weakly evidenced constraint with product or source owner before implementation.")
    return tuple(_dedupe(follow_up))


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        return _optional_text(source.id), source.model_dump(mode="python")
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _optional_text(payload.get("id")), payload
    if isinstance(source, Mapping):
        payload = _validated_payload(source)
        return _optional_text(payload.get("id")), payload
    if not isinstance(source, (str, bytes)):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), payload
    return None, {}


def _validated_payload(source: Mapping[str, Any]) -> dict[str, Any]:
    for model in (SourceBrief, ImplementationBrief):
        try:
            return dict(model.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            continue
    return dict(source)


def _extract_payload(payload: Mapping[str, Any]) -> _ExtractedPayload:
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    metadata = _first_mapping(payload.get("metadata"), payload.get("brief_metadata"), source_payload.get("metadata"))
    constraints = _constraint_candidates(payload, "constraints")
    constraints.extend(_constraint_candidates(source_payload, "source_payload.constraints", actual_key="constraints"))
    for field_name in ("implementation_constraints", "requirements", "scope", "non_goals"):
        constraints.extend(_constraint_candidates(payload, field_name))
        constraints.extend(_constraint_candidates(source_payload, f"source_payload.{field_name}", actual_key=field_name))
    constraints = _dedupe_candidates(constraints)

    acceptance = _field_strings(payload, "acceptance_criteria")
    acceptance.extend(_field_strings(payload, "definition_of_done"))
    acceptance.extend(_field_strings(source_payload, "acceptance_criteria"))
    acceptance.extend(_field_strings(source_payload, "definition_of_done"))

    assumptions = _field_strings(payload, "assumptions")
    assumptions.extend(_field_strings(source_payload, "assumptions"))
    risks = _field_strings(payload, "risks")
    risks.extend(_field_strings(source_payload, "risks"))
    open_questions = _field_strings(payload, "open_questions")
    open_questions.extend(_field_strings(payload, "questions"))
    open_questions.extend(_field_strings(source_payload, "open_questions"))
    open_questions.extend(_field_strings(source_payload, "questions"))

    references = _source_references(payload)
    references.extend(_source_references(source_payload, prefix="source_payload"))
    references.extend(_source_references(metadata, prefix="metadata"))

    metadata_texts = [text for _, text in _metadata_texts(metadata)]
    metadata_texts.extend([text for _, text in _metadata_texts(payload.get("source_links"), "source_links")])

    return _ExtractedPayload(
        constraints=tuple(constraints),
        acceptance_criteria=tuple(_dedupe(acceptance)),
        assumptions=tuple(_dedupe(assumptions)),
        risks=tuple(_dedupe(risks)),
        open_questions=tuple(_dedupe(open_questions)),
        source_references=tuple(_dedupe(references)),
        metadata_texts=tuple(_dedupe(metadata_texts)),
    )


def _constraint_candidates(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    actual_key: str | None = None,
) -> list[_ConstraintCandidate]:
    key = actual_key or field_name
    value = payload.get(key)
    candidates: list[_ConstraintCandidate] = []
    if isinstance(value, Mapping):
        for map_key in sorted(value, key=lambda item: str(item)):
            item = value[map_key]
            text = _item_text(item)
            if not text:
                continue
            refs = _item_references(item)
            if ref := _optional_text(map_key):
                refs = tuple(_dedupe([*refs, f"{field_name}.{ref}"]))
            candidates.append(_ConstraintCandidate(text=text, field=field_name, source_references=refs))
        return candidates
    for index, item in enumerate(_list_items(value)):
        text = _item_text(item)
        if text:
            candidates.append(
                _ConstraintCandidate(
                    text=text,
                    field=f"{field_name}[{index}]",
                    source_references=_item_references(item),
                )
            )
    if isinstance(value, str) and (text := _optional_text(value)):
        candidates.append(_ConstraintCandidate(text=text, field=field_name))
    return candidates


def _field_strings(payload: Mapping[str, Any], field_name: str) -> list[str]:
    return [_item_text(item) for item in _list_items(payload.get(field_name)) if _item_text(item)]


def _item_text(item: Any) -> str:
    if isinstance(item, Mapping):
        for key in ("text", "constraint", "requirement", "title", "description", "summary", "value"):
            if text := _optional_text(item.get(key)):
                return text
        return ""
    return _optional_text(item) or ""


def _item_references(item: Any) -> tuple[str, ...]:
    if not isinstance(item, Mapping):
        return ()
    references: list[str] = []
    for key in ("source", "source_ref", "source_refs", "source_reference", "source_references", "citation", "citations", "evidence"):
        references.extend(_strings(item.get(key)))
    return tuple(_dedupe(references))


def _source_references(payload: Mapping[str, Any], prefix: str = "source") -> list[str]:
    references: list[str] = []
    for key in (
        "source_ref",
        "source_refs",
        "source_reference",
        "source_references",
        "source_links",
        "citations",
        "evidence",
    ):
        for value in _strings(payload.get(key)):
            references.append(f"{prefix}.{key}: {value}")
    for key in ("source_project", "source_entity_type", "source_id"):
        if value := _optional_text(payload.get(key)):
            references.append(f"{key}: {value}")
    return references


def _evidence_corpus(extracted: _ExtractedPayload) -> tuple[tuple[str, str], ...]:
    corpus: list[tuple[str, str]] = []
    for constraint in extracted.constraints:
        corpus.append((constraint.field, constraint.text))
    for label, values in (
        ("acceptance_criteria", extracted.acceptance_criteria),
        ("assumptions", extracted.assumptions),
        ("risks", extracted.risks),
        ("open_questions", extracted.open_questions),
        ("metadata", extracted.metadata_texts),
    ):
        for value in values:
            corpus.append((label, value))
    return tuple(corpus)


def _supporting_texts(text: str, candidates: Iterable[str], label: str) -> tuple[str, ...]:
    support = []
    for candidate in candidates:
        if _similar_enough(text, candidate):
            support.append(f"{label}: {_clip(candidate)}")
    return tuple(_dedupe(support))


def _repeated_evidence(text: str, corpus: tuple[tuple[str, str], ...]) -> tuple[str, ...]:
    matches = [
        f"{label}: {_clip(candidate)}"
        for label, candidate in corpus
        if candidate != text and _similar_enough(text, candidate)
    ]
    return tuple(_dedupe(matches))


def _signal_texts(text: str, candidates: Iterable[str], pattern: re.Pattern[str]) -> tuple[str, ...]:
    signals = []
    for candidate in candidates:
        if pattern.search(candidate) and _similar_enough(text, candidate, minimum=0.12):
            signals.append(_clip(candidate))
    return tuple(_dedupe(signals))


def _similar_enough(left: str, right: str, *, minimum: float = 0.20) -> bool:
    left_tokens = _tokens(left)
    right_tokens = _tokens(right)
    if not left_tokens or not right_tokens:
        return False
    overlap = left_tokens & right_tokens
    if len(overlap) >= 3:
        return True
    return len(overlap) / max(min(len(left_tokens), len(right_tokens)), 1) >= minimum


def _tokens(value: str) -> set[str]:
    stopwords = {
        "a",
        "an",
        "and",
        "are",
        "as",
        "be",
        "by",
        "for",
        "from",
        "in",
        "is",
        "of",
        "on",
        "or",
        "the",
        "to",
        "with",
    }
    tokens: set[str] = set()
    for token in _TOKEN_RE.findall(value.casefold()):
        if token in stopwords or len(token) <= 1:
            continue
        tokens.add(token[:-1] if token.endswith("s") and len(token) > 4 else token)
    return tokens


def _owner_suggestion(
    text: str,
    contradictions: tuple[str, ...],
    questions: tuple[str, ...],
) -> str:
    if contradictions or questions:
        return "product"
    for owner, pattern in _OWNER_PATTERNS:
        if pattern.search(text):
            return owner
    return "engineering"


def _dedupe_candidates(candidates: Iterable[_ConstraintCandidate]) -> list[_ConstraintCandidate]:
    deduped: list[_ConstraintCandidate] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = _text(candidate.text).casefold()
        if not key or key in seen:
            continue
        deduped.append(candidate)
        seen.add(key)
    return deduped


def _first_mapping(*values: Any) -> Mapping[str, Any]:
    for value in values:
        if isinstance(value, Mapping):
            return value
    return {}


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            texts.extend(_metadata_texts(item, f"{prefix}[{index}]"))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _list_items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        return [value]
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


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "domain",
        "summary",
        "source_project",
        "source_entity_type",
        "source_id",
        "source_payload",
        "source_links",
        "source_brief_id",
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
        "validation_plan",
        "definition_of_done",
        "acceptance_criteria",
        "constraints",
        "implementation_constraints",
        "requirements",
        "open_questions",
        "questions",
        "metadata",
        "brief_metadata",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _clip(value: str) -> str:
    text = _text(value)
    return f"{text[:177].rstrip()}..." if len(text) > 180 else text


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
    "ConstraintConfidenceLevel",
    "SourceConstraintConfidenceFinding",
    "SourceConstraintConfidenceReport",
    "build_source_constraint_confidence_report",
    "source_constraint_confidence_report_to_dict",
    "source_constraint_confidence_report_to_markdown",
    "summarize_source_constraint_confidence",
]
