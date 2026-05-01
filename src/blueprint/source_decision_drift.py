"""Detect drift between upstream source decisions and implementation plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask, ImplementationBrief, SourceBrief


DecisionDriftType = Literal["contradicted", "missing", "weakly_represented"]
DecisionConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_DRIFT_ORDER: dict[DecisionDriftType, int] = {
    "contradicted": 0,
    "missing": 1,
    "weakly_represented": 2,
}
_CONFIDENCE_ORDER: dict[DecisionConfidence, int] = {"low": 0, "medium": 1, "high": 2}
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)]|\[[ xX]\])\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$")
_LABEL_RE = re.compile(
    r"^\s*(?:[-*+]\s*)?(?P<label>[A-Za-z][A-Za-z0-9 _/-]{1,40})\s*[:\-]\s*(?P<text>.+)$"
)
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|(?:\r?\n|;)+")
_DECISION_KEY_RE = re.compile(
    r"\b(?:decision|decisions|decided|choice|chosen|approved|constraint|constraints|"
    r"requirement|requirements|required|must|shall|source of truth|metadata|risk|risks)\b",
    re.IGNORECASE,
)
_EXPLICIT_DECISION_RE = re.compile(
    r"\b(?:decision|decided|choose|chosen|approved|must|shall|required|requires|"
    r"require|only|source of truth|use|adopt|keep|exclude|avoid|do not|don't|"
    r"cannot|should)\b",
    re.IGNORECASE,
)
_HIGH_CONFIDENCE_RE = re.compile(
    r"\b(?:decision|decided|chosen|approved|must|shall|required|requires|only|"
    r"source of truth|do not|don't|exclude|avoid|cannot)\b",
    re.IGNORECASE,
)
_MEDIUM_CONFIDENCE_RE = re.compile(
    r"\b(?:should|use|adopt|keep|prefer|constraint|risk|metadata)\b",
    re.IGNORECASE,
)
_NEGATION_RE = re.compile(
    r"\b(?:do not|don't|not|never|avoid|exclude|without|instead of|rather than|"
    r"replace|replaced|replacing|drop|remove|disable|deprecate)\b",
    re.IGNORECASE,
)
_SOURCE_FIELDS = (
    "summary",
    "source_payload",
    "source_links",
    "source_metadata",
    "metadata",
    "risks",
    "constraints",
)
_SOURCE_PAYLOAD_DECISION_KEYS = {
    "decision",
    "decisions",
    "decision_log",
    "decision_logs",
    "accepted_decisions",
    "approved_decisions",
    "chosen",
    "choice",
    "constraints",
    "constraint",
    "requirements",
    "requirement",
    "must",
    "metadata_decisions",
    "risks",
    "risk",
}
_PLAN_BRIEF_FIELDS = (
    "title",
    "summary",
    "problem_statement",
    "mvp_goal",
    "scope",
    "non_goals",
    "assumptions",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "validation_plan",
    "definition_of_done",
    "metadata",
)
_PLAN_TASK_FIELDS = (
    "title",
    "description",
    "milestone",
    "files_or_modules",
    "acceptance_criteria",
    "risk_level",
    "test_command",
    "metadata",
)
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "be",
    "by",
    "can",
    "for",
    "from",
    "if",
    "in",
    "into",
    "is",
    "it",
    "may",
    "of",
    "on",
    "or",
    "that",
    "the",
    "then",
    "to",
    "with",
    "we",
    "will",
    "must",
    "shall",
    "should",
    "use",
    "using",
    "decision",
    "decided",
    "chosen",
    "required",
    "requires",
    "require",
}


@dataclass(frozen=True, slots=True)
class SourceDecisionDriftRecord:
    """One source decision that is absent, contradicted, or weak in the plan."""

    source_id: str
    source_index: int
    decision_text: str
    drift_type: DecisionDriftType
    confidence: DecisionConfidence
    evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_follow_up: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "source_index": self.source_index,
            "decision_text": self.decision_text,
            "drift_type": self.drift_type,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "suggested_follow_up": self.suggested_follow_up,
        }


@dataclass(frozen=True, slots=True)
class SourceDecisionDriftAnalysis:
    """Decision drift analysis for one plan against one or more source briefs."""

    plan_id: str | None = None
    drifts: tuple[SourceDecisionDriftRecord, ...] = field(default_factory=tuple)
    source_count: int = 0
    decision_count: int = 0

    @property
    def drift_count(self) -> int:
        """Return the number of detected drift records."""
        return len(self.drifts)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "source_count": self.source_count,
            "decision_count": self.decision_count,
            "drift_count": self.drift_count,
            "drifts": [drift.to_dict() for drift in self.drifts],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return drift records as plain dictionaries."""
        return [drift.to_dict() for drift in self.drifts]

    def to_markdown(self) -> str:
        """Render decision drift records as deterministic Markdown."""
        title = "# Source Decision Drift"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.drifts:
            lines.extend(["", "No source decision drift detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Source | Drift Type | Confidence | Decision | Evidence | Follow-up |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for drift in self.drifts:
            lines.append(
                "| "
                f"{_markdown_cell(drift.source_id)} | "
                f"{drift.drift_type} | "
                f"{drift.confidence} | "
                f"{_markdown_cell(drift.decision_text)} | "
                f"{_markdown_cell('; '.join(drift.evidence))} | "
                f"{_markdown_cell(drift.suggested_follow_up)} |"
            )
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class _SourceDecision:
    source_id: str
    source_index: int
    source_field: str
    text: str
    tokens: frozenset[str]
    confidence: DecisionConfidence
    severity: int


@dataclass(frozen=True, slots=True)
class _PlanSnippet:
    field_path: str
    text: str
    tokens: frozenset[str]


def analyze_source_decision_drift(
    source_briefs: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief],
    plan: Mapping[str, Any] | ImplementationBrief | ExecutionPlan,
) -> SourceDecisionDriftAnalysis:
    """Compare source decision signals against an implementation brief or execution plan."""
    sources = _source_payloads(source_briefs)
    decisions = [
        decision
        for source_index, source in enumerate(sources, start=1)
        for decision in _source_decisions(source, source_index)
    ]
    snippets = _plan_snippets(plan)
    drifts = [_drift_record(decision, snippets) for decision in decisions]
    drift_records = tuple(
        sorted(
            (drift for drift in drifts if drift is not None),
            key=lambda drift: (
                drift.source_index,
                _DRIFT_ORDER[drift.drift_type],
                -_CONFIDENCE_ORDER[drift.confidence],
                _normalized_phrase(drift.decision_text),
            ),
        )
    )
    return SourceDecisionDriftAnalysis(
        plan_id=_plan_id(plan),
        drifts=drift_records,
        source_count=len(sources),
        decision_count=len(decisions),
    )


def detect_source_decision_drift(
    source_briefs: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief],
    plan: Mapping[str, Any] | ImplementationBrief | ExecutionPlan,
) -> SourceDecisionDriftAnalysis:
    """Compatibility alias for source decision drift analysis."""
    return analyze_source_decision_drift(source_briefs, plan)


def source_decision_drift_analysis_to_dict(
    analysis: SourceDecisionDriftAnalysis,
) -> dict[str, Any]:
    """Serialize source decision drift analysis to a plain dictionary."""
    return analysis.to_dict()


source_decision_drift_analysis_to_dict.__test__ = False


def source_decision_drifts_to_dicts(
    analysis: SourceDecisionDriftAnalysis | Iterable[SourceDecisionDriftRecord],
) -> list[dict[str, Any]]:
    """Serialize source decision drift records to plain dictionaries."""
    if isinstance(analysis, SourceDecisionDriftAnalysis):
        return analysis.to_dicts()
    return [drift.to_dict() for drift in analysis]


source_decision_drifts_to_dicts.__test__ = False


def source_decision_drift_analysis_to_markdown(
    analysis: SourceDecisionDriftAnalysis,
) -> str:
    """Render source decision drift analysis as Markdown."""
    return analysis.to_markdown()


source_decision_drift_analysis_to_markdown.__test__ = False


def _drift_record(
    decision: _SourceDecision,
    snippets: list[_PlanSnippet],
) -> SourceDecisionDriftRecord | None:
    matches = _matching_snippets(decision, snippets)
    contradictions = [match for match in matches if _contradicts(decision, match)]
    if contradictions:
        evidence = _evidence(decision, contradictions[:3])
        return SourceDecisionDriftRecord(
            source_id=decision.source_id,
            source_index=decision.source_index,
            decision_text=decision.text,
            drift_type="contradicted",
            confidence=_highest_confidence(decision.confidence, "high"),
            evidence=evidence,
            suggested_follow_up=(
                "Reconcile the plan language with the source decision before implementation."
            ),
        )

    strong_matches = [
        match for match in matches if _match_score(decision.tokens, match.tokens) >= 0.86
    ]
    if strong_matches:
        return None

    weak_matches = [
        match for match in matches if _match_score(decision.tokens, match.tokens) >= 0.34
    ]
    if weak_matches:
        evidence = _evidence(decision, weak_matches[:3])
        return SourceDecisionDriftRecord(
            source_id=decision.source_id,
            source_index=decision.source_index,
            decision_text=decision.text,
            drift_type="weakly_represented",
            confidence=_lower_confidence(decision.confidence),
            evidence=evidence,
            suggested_follow_up=(
                "Strengthen the plan with explicit task or brief language for this decision."
            ),
        )

    return SourceDecisionDriftRecord(
        source_id=decision.source_id,
        source_index=decision.source_index,
        decision_text=decision.text,
        drift_type="missing",
        confidence=decision.confidence,
        evidence=(f"{decision.source_field}: {decision.text}",),
        suggested_follow_up="Add this source decision to the implementation brief or plan.",
    )


def _matching_snippets(
    decision: _SourceDecision,
    snippets: list[_PlanSnippet],
) -> list[_PlanSnippet]:
    matches = [
        snippet
        for snippet in snippets
        if _match_score(decision.tokens, snippet.tokens) >= 0.18
        or _normalized_phrase(decision.text) in _normalized_phrase(snippet.text)
    ]
    return sorted(
        matches,
        key=lambda snippet: (-_match_score(decision.tokens, snippet.tokens), snippet.field_path),
    )


def _contradicts(decision: _SourceDecision, snippet: _PlanSnippet) -> bool:
    decision_phrase = _normalized_phrase(decision.text)
    snippet_phrase = _normalized_phrase(snippet.text)
    if not (decision.tokens & snippet.tokens):
        return False
    if decision_phrase and f"instead of {decision_phrase}" in snippet_phrase:
        return True
    if "instead of" in snippet.text.casefold():
        replaced_text = snippet.text.casefold().split("instead of", maxsplit=1)[1]
        replaced_text = re.split(r"\b(?:and|but|while|or|then)\b|[.;,]", replaced_text, maxsplit=1)[
            0
        ]
        replaced_tokens = set(_normalized_tokens(replaced_text))
        return bool(replaced_tokens & set(_strong_tokens(decision.text)))
    if _NEGATION_RE.search(snippet.text):
        snippet_text = snippet.text.casefold()
        for token in _strong_tokens(decision.text):
            token_match = re.search(rf"\b{re.escape(token)}\w*\b", snippet_text)
            if token_match is None:
                continue
            prefix = snippet_text[max(0, token_match.start() - 48) : token_match.start()]
            if _NEGATION_RE.search(prefix):
                return True
    return False


def _source_payloads(
    value: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief],
) -> list[dict[str, Any]]:
    if isinstance(value, SourceBrief):
        return [value.model_dump(mode="python")]
    if isinstance(value, Mapping):
        if _looks_like_source_collection(value):
            sources = value.get("source_briefs") or value.get("sources") or value.get("briefs")
            return [
                _source_payload(item) for item in _iterable_items(sources) if _source_payload(item)
            ]
        return [_source_payload(value)]

    sources: list[dict[str, Any]] = []
    try:
        iterator = iter(value)
    except TypeError:
        return []
    for item in iterator:
        if source := _source_payload(item):
            sources.append(source)
    return sources


def _source_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, SourceBrief):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        dumped = value.model_dump(mode="python")
        return dict(dumped) if isinstance(dumped, Mapping) else {}
    if isinstance(value, Mapping):
        try:
            dumped = SourceBrief.model_validate(value).model_dump(mode="python")
            return dict(dumped) if isinstance(dumped, Mapping) else {}
        except (TypeError, ValueError, ValidationError):
            return dict(value)
    return _object_payload(
        value,
        (
            "id",
            "title",
            "summary",
            "source_project",
            "source_entity_type",
            "source_id",
            "source_payload",
            "source_links",
            "source_metadata",
            "metadata",
            "risks",
            "constraints",
        ),
    )


def _source_decisions(source: Mapping[str, Any], source_index: int) -> list[_SourceDecision]:
    source_id = _source_id(source, source_index)
    drafts: dict[str, _SourceDecision] = {}
    for field_name in _SOURCE_FIELDS:
        if field_name not in source:
            continue
        for source_field, text, confidence in _decision_texts(
            source.get(field_name),
            field_name,
            in_decision_context=field_name in {"risks", "constraints"},
        ):
            _add_decision(drafts, source_id, source_index, source_field, text, confidence)

    payload = source.get("source_payload")
    if isinstance(payload, Mapping):
        normalized = payload.get("normalized")
        if isinstance(normalized, Mapping):
            for key in ("decisions", "risks", "constraints"):
                for source_field, text, confidence in _decision_texts(
                    normalized.get(key),
                    f"source_payload.normalized.{key}",
                    in_decision_context=True,
                ):
                    _add_decision(drafts, source_id, source_index, source_field, text, confidence)

    return list(drafts.values())


def _add_decision(
    drafts: dict[str, _SourceDecision],
    source_id: str,
    source_index: int,
    source_field: str,
    text: str,
    confidence: DecisionConfidence,
) -> None:
    decision_text = _clean_text(text)
    if not decision_text or len(_normalized_tokens(decision_text)) == 0:
        return
    key = _dedupe_key(decision_text)
    existing = drafts.get(key)
    tokens = frozenset(_normalized_tokens(decision_text))
    severity = _source_severity(source_field, confidence)
    candidate = _SourceDecision(
        source_id=source_id,
        source_index=source_index,
        source_field=source_field,
        text=decision_text,
        tokens=tokens,
        confidence=confidence,
        severity=severity,
    )
    if existing is None:
        drafts[key] = candidate
        return
    if (severity, _CONFIDENCE_ORDER[confidence]) < (
        existing.severity,
        _CONFIDENCE_ORDER[existing.confidence],
    ):
        drafts[key] = candidate


def _decision_texts(
    value: Any,
    field_path: str,
    *,
    in_decision_context: bool = False,
) -> list[tuple[str, str, DecisionConfidence]]:
    if value is None:
        return []
    if isinstance(value, str):
        return _decision_texts_from_string(value, field_path, in_decision_context)
    if isinstance(value, Mapping):
        texts: list[tuple[str, str, DecisionConfidence]] = []
        for key in sorted(value, key=str):
            key_text = str(key)
            child_context = in_decision_context or _is_decision_key(key_text)
            child_path = f"{field_path}.{_field_slug(key_text)}"
            if child_context and isinstance(value[key], str):
                text = _clean_labeled_text(key_text, value[key])
                if _is_decision_like(text, child_context):
                    texts.append((child_path, text, _confidence(text, key_text, child_context)))
                    continue
            texts.extend(_decision_texts(value[key], child_path, in_decision_context=child_context))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        texts: list[tuple[str, str, DecisionConfidence]] = []
        for index, item in enumerate(items, start=1):
            texts.extend(
                _decision_texts(
                    item,
                    f"{field_path}.{index:03d}",
                    in_decision_context=in_decision_context,
                )
            )
        return texts

    text = _optional_text(value)
    if text and _is_decision_like(text, in_decision_context):
        return [(field_path, text, _confidence(text, field_path, in_decision_context))]
    return []


def _decision_texts_from_string(
    value: str,
    field_path: str,
    in_decision_context: bool,
) -> list[tuple[str, str, DecisionConfidence]]:
    texts: list[tuple[str, str, DecisionConfidence]] = []
    current_heading: str | None = None
    for line_index, raw_line in enumerate(value.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        heading = _heading_text(line)
        if heading is not None:
            current_heading = heading
            if _is_decision_key(heading):
                continue
        elif _plain_heading_text(line) is not None:
            current_heading = _plain_heading_text(line)
            continue
        label_match = _LABEL_RE.match(line)
        if label_match and _is_decision_key(label_match.group("label")):
            text = _clean_text(label_match.group("text"))
            texts.append(
                (
                    f"{field_path}.line_{line_index:03d}",
                    text,
                    _confidence(text, label_match.group("label"), True),
                )
            )
            continue
        if current_heading and _is_decision_key(current_heading):
            text = _clean_text(_BULLET_RE.sub("", line))
            if text and not _heading_text(text):
                texts.append(
                    (
                        f"{field_path}.{_field_slug(current_heading)}.{line_index:03d}",
                        text,
                        _confidence(text, current_heading, True),
                    )
                )
            continue

        for part in _SPLIT_RE.split(line):
            text = _clean_text(_BULLET_RE.sub("", part))
            if _is_decision_like(text, in_decision_context):
                texts.append(
                    (
                        f"{field_path}.line_{line_index:03d}",
                        text,
                        _confidence(text, field_path, in_decision_context),
                    )
                )

    if texts:
        return texts

    for index, part in enumerate(_SPLIT_RE.split(value), start=1):
        text = _clean_text(_BULLET_RE.sub("", part))
        if _is_decision_like(text, in_decision_context):
            texts.append(
                (
                    f"{field_path}.{index:03d}",
                    text,
                    _confidence(text, field_path, in_decision_context),
                )
            )
    return texts


def _plan_snippets(
    plan: Mapping[str, Any] | ImplementationBrief | ExecutionPlan
) -> list[_PlanSnippet]:
    payload = _plan_payload(plan)
    snippets: list[_PlanSnippet] = []
    if "tasks" in payload:
        for field_name in ("test_strategy", "handoff_prompt", "metadata"):
            snippets.extend(_snippets_from_value(payload.get(field_name), f"plan.{field_name}"))
        for task_index, task in enumerate(_task_payloads(payload.get("tasks")), start=1):
            task_id = _optional_text(task.get("id")) or f"task-{task_index}"
            for field_name in _PLAN_TASK_FIELDS:
                snippets.extend(
                    _snippets_from_value(task.get(field_name), f"task.{task_id}.{field_name}")
                )
        return snippets

    for field_name in _PLAN_BRIEF_FIELDS:
        snippets.extend(_snippets_from_value(payload.get(field_name), f"brief.{field_name}"))
    return snippets


def _plan_payload(plan: Mapping[str, Any] | ImplementationBrief | ExecutionPlan) -> dict[str, Any]:
    if isinstance(plan, (ImplementationBrief, ExecutionPlan)):
        return plan.model_dump(mode="python")
    if hasattr(plan, "model_dump"):
        dumped = plan.model_dump(mode="python")
        return dict(dumped) if isinstance(dumped, Mapping) else {}
    if isinstance(plan, Mapping):
        if "tasks" in plan:
            try:
                dumped = ExecutionPlan.model_validate(plan).model_dump(mode="python")
                return dict(dumped) if isinstance(dumped, Mapping) else {}
            except (TypeError, ValueError, ValidationError):
                return dict(plan)
        try:
            dumped = ImplementationBrief.model_validate(plan).model_dump(mode="python")
            return dict(dumped) if isinstance(dumped, Mapping) else {}
        except (TypeError, ValueError, ValidationError):
            return dict(plan)
    return _object_payload(plan, (*_PLAN_BRIEF_FIELDS, "id", "tasks"))


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=str) if isinstance(value, set) else value
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
        else:
            task = _object_payload(item, (*_PLAN_TASK_FIELDS, "id"))
            if task:
                tasks.append(task)
    return tasks


def _snippets_from_value(value: Any, field_path: str) -> list[_PlanSnippet]:
    if value is None:
        return []
    if isinstance(value, str):
        snippets = []
        for index, part in enumerate(_SPLIT_RE.split(value), start=1):
            text = _clean_text(_BULLET_RE.sub("", part))
            if text:
                snippets.append(_snippet(f"{field_path}.{index:03d}", text))
        return snippets
    if isinstance(value, Mapping):
        snippets: list[_PlanSnippet] = []
        for key in sorted(value, key=str):
            snippets.extend(
                _snippets_from_value(value[key], f"{field_path}.{_field_slug(str(key))}")
            )
        return snippets
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        snippets: list[_PlanSnippet] = []
        for index, item in enumerate(items, start=1):
            snippets.extend(_snippets_from_value(item, f"{field_path}.{index:03d}"))
        return snippets
    text = _optional_text(value)
    return [_snippet(field_path, text)] if text else []


def _snippet(field_path: str, text: str) -> _PlanSnippet:
    return _PlanSnippet(
        field_path=field_path, text=text, tokens=frozenset(_normalized_tokens(text))
    )


def _match_score(decision_tokens: frozenset[str], text_tokens: frozenset[str]) -> float:
    if not decision_tokens or not text_tokens:
        return 0.0
    overlap = decision_tokens & text_tokens
    if len(decision_tokens) == 1:
        token = next(iter(decision_tokens))
        return 1.0 if token in text_tokens and _strong_single_token(token) else 0.0
    if len(overlap) < min(2, len(decision_tokens)):
        return 0.0
    return len(overlap) / len(decision_tokens)


def _evidence(decision: _SourceDecision, snippets: list[_PlanSnippet]) -> tuple[str, ...]:
    values = [f"{decision.source_field}: {decision.text}"]
    values.extend(f"{snippet.field_path}: {snippet.text}" for snippet in snippets)
    return tuple(_dedupe(values))


def _source_id(source: Mapping[str, Any], source_index: int) -> str:
    return (
        _optional_text(source.get("source_id"))
        or _optional_text(source.get("id"))
        or f"source-{source_index}"
    )


def _plan_id(plan: Mapping[str, Any] | ImplementationBrief | ExecutionPlan) -> str | None:
    payload = _plan_payload(plan)
    return _optional_text(payload.get("id"))


def _clean_labeled_text(label: str, value: str) -> str:
    text = _clean_text(value)
    if _is_decision_key(label) and label.casefold().strip() in {"risk", "risks"}:
        return text
    return text


def _is_decision_like(text: str, in_decision_context: bool) -> bool:
    if not text:
        return False
    return in_decision_context or _EXPLICIT_DECISION_RE.search(text) is not None


def _is_decision_key(key: str) -> bool:
    normalized = key.strip().casefold().replace("-", "_").replace(" ", "_")
    return normalized in _SOURCE_PAYLOAD_DECISION_KEYS or _DECISION_KEY_RE.search(key) is not None


def _confidence(text: str, field_path: str, in_decision_context: bool) -> DecisionConfidence:
    if _HIGH_CONFIDENCE_RE.search(text) or _HIGH_CONFIDENCE_RE.search(field_path):
        return "high"
    if (
        in_decision_context
        or _MEDIUM_CONFIDENCE_RE.search(text)
        or _MEDIUM_CONFIDENCE_RE.search(field_path)
    ):
        return "medium"
    return "low"


def _highest_confidence(
    current: DecisionConfidence,
    candidate: DecisionConfidence,
) -> DecisionConfidence:
    return candidate if _CONFIDENCE_ORDER[candidate] > _CONFIDENCE_ORDER[current] else current


def _lower_confidence(value: DecisionConfidence) -> DecisionConfidence:
    if value == "high":
        return "medium"
    if value == "medium":
        return "low"
    return "low"


def _source_severity(field_path: str, confidence: DecisionConfidence) -> int:
    if "decision" in field_path:
        return 0
    if confidence == "high":
        return 1
    if "constraint" in field_path:
        return 2
    if "risk" in field_path:
        return 3
    return 4


def _heading_text(line: str) -> str | None:
    match = _HEADING_RE.match(line)
    if match:
        return _clean_text(match.group(1).strip("*:_- "))
    stripped = line.strip()
    if stripped and stripped.lower() in _SOURCE_PAYLOAD_DECISION_KEYS:
        return stripped
    return None


def _plain_heading_text(line: str) -> str | None:
    stripped = line.strip().strip(":")
    if _BULLET_RE.match(stripped) or _LABEL_RE.match(stripped):
        return None
    words = stripped.split()
    if 1 <= len(words) <= 6 and all(word[:1].isalpha() for word in words):
        return stripped
    return None


def _normalized_tokens(text: str) -> list[str]:
    return [
        _normalize_token(token)
        for token in _TOKEN_RE.findall(text.casefold())
        if _normalize_token(token) and _normalize_token(token) not in _STOPWORDS
    ]


def _strong_tokens(text: str) -> list[str]:
    return [token for token in _normalized_tokens(text) if _strong_single_token(token)]


def _strong_single_token(token: str) -> bool:
    return len(token) >= 6 and token not in {"create", "update", "support", "review"}


def _normalized_phrase(text: str) -> str:
    return " ".join(_normalized_tokens(text))


def _normalize_token(token: str) -> str:
    if token.endswith("ies") and len(token) > 4:
        return f"{token[:-3]}y"
    if token.endswith("s") and len(token) > 3:
        return token[:-1]
    return token


def _dedupe_key(text: str) -> str:
    return " ".join(_normalized_tokens(text))


def _field_slug(value: str) -> str:
    return "_".join(_TOKEN_RE.findall(value.casefold())) or "field"


def _clean_text(value: Any) -> str:
    return _SPACE_RE.sub(" ", str(value).strip()).strip("-*+ \t")


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _clean_text(value)
    return text or None


def _iterable_items(value: Any) -> list[Any]:
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return []


def _looks_like_source_collection(value: Mapping[str, Any]) -> bool:
    return any(key in value for key in ("source_briefs", "sources", "briefs"))


def _object_payload(value: Any, fields: tuple[str, ...]) -> dict[str, Any]:
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _dedupe(values: Iterable[_T]) -> list[_T]:
    seen: set[_T] = set()
    result: list[_T] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "DecisionConfidence",
    "DecisionDriftType",
    "SourceDecisionDriftAnalysis",
    "SourceDecisionDriftRecord",
    "analyze_source_decision_drift",
    "detect_source_decision_drift",
    "source_decision_drift_analysis_to_dict",
    "source_decision_drift_analysis_to_markdown",
    "source_decision_drifts_to_dicts",
]
