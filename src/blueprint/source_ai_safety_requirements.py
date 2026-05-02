"""Extract source-level AI safety requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


AISafetyRequirementCategory = Literal[
    "model_output_boundaries",
    "human_review",
    "evaluation_dataset",
    "sensitive_data_handling",
    "prompt_injection_defense",
    "safety_refusal_policy",
    "audit_logging",
    "fallback_behavior",
]
AISafetyRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[AISafetyRequirementCategory, ...] = (
    "model_output_boundaries",
    "human_review",
    "evaluation_dataset",
    "sensitive_data_handling",
    "prompt_injection_defense",
    "safety_refusal_policy",
    "audit_logging",
    "fallback_behavior",
)
_CONFIDENCE_ORDER: dict[AISafetyRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|cannot ship|guardrail|policy|control|"
    r"support|provide|include|define|validate|verify)\b",
    re.I,
)
_AI_CONTEXT_RE = re.compile(
    r"\b(?:ai|artificial intelligence|llm|large language model|language model|"
    r"generative ai|genai|gpt|openai|anthropic|claude|gemini|chatgpt|"
    r"model(?:[- ]generated| output| response| inference)?|inference|prompt(?:s|ed|ing)?|"
    r"completion(?:s)?|copilot|chatbot|virtual assistant|ai assistant|rag|"
    r"retrieval[- ]augmented|embedding(?:s)?|semantic search)\b",
    re.I,
)
_GENERIC_AUTOMATION_RE = re.compile(
    r"\b(?:automation|automated|auto[- ]?generated|workflow|bot|rules engine|scheduler|"
    r"batch job|script|macro|template)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:ai|llm|large language model|language model|generative ai|"
    r"gpt|copilot|chatbot|model[- ]generated|prompt|inference|ai safety).*?"
    r"\b(?:in scope|required|needed|changes?|feature|requirements?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:ai|llm|model|prompt|copilot|chatbot|assistant|inference|genai|safety|"
    r"eval|evaluation|red[-_ ]?team|guardrail|human[-_ ]?review|review|approval|"
    r"data|privacy|logging|audit|fallback|controls?|policy)",
    re.I,
)
_CATEGORY_PATTERNS: dict[AISafetyRequirementCategory, re.Pattern[str]] = {
    "model_output_boundaries": re.compile(
        r"\b(?:output boundaries?|answer boundaries?|model boundaries?|scope boundaries?|"
        r"stay within scope|only answer|do not answer|cite sources?|citations?|grounded "
        r"(?:answers?|responses?)|grounding|confidence score|uncertain|no legal advice|"
        r"no medical advice|no financial advice|format constraints?|structured output|"
        r"json schema|allowed responses?|disallowed responses?)\b",
        re.I,
    ),
    "human_review": re.compile(
        r"\b(?:human review|human[- ]in[- ]the[- ]loop|hitl|manual review|review queue|"
        r"agent review|operator review|editor approval|approval before (?:send|publish|action)|"
        r"review before (?:send|publish|execute)|human approval|escalate to human|"
        r"supervisor approval)\b",
        re.I,
    ),
    "evaluation_dataset": re.compile(
        r"\b(?:evaluation dataset|eval(?:uation)? set|golden (?:dataset|set|prompts?)|"
        r"prompt regression|offline eval(?:uation)?|model eval(?:uation)?|quality eval|"
        r"red[- ]team(?:ing)? set|adversarial prompts?|test prompts?|benchmark prompts?|"
        r"expected answers?|rubric|graded examples?)\b",
        re.I,
    ),
    "sensitive_data_handling": re.compile(
        r"\b(?:sensitive data|pii|personal data|phi|pci|secrets?|credentials?|api keys?|"
        r"redact(?:ion)?|mask(?:ing)?|data minimization|data retention|do not train|"
        r"training exclusion|prompt logging opt[- ]out|tenant data|customer data|"
        r"private documents?|confidential)\b",
        re.I,
    ),
    "prompt_injection_defense": re.compile(
        r"\b(?:prompt injection|prompt[- ]injection|jailbreak(?:ing)?|malicious prompt|"
        r"untrusted (?:content|context|documents?|input)|retrieved content|system prompt|"
        r"tool instructions?|tool calls?|ignore previous instructions|instruction hierarchy|"
        r"prompt attack|indirect prompt injection|data exfiltration)\b",
        re.I,
    ),
    "safety_refusal_policy": re.compile(
        r"\b(?:refusal policy|safety refusal|refuse(?:s|d)?|decline(?:s|d)?|disallowed "
        r"(?:content|request)|unsafe request|harmful request|self[- ]harm|violence|"
        r"hate(?:ful)? content|sexual content|illegal advice|medical advice|legal advice|"
        r"financial advice|policy violation|safety policy)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit log(?:s|ging)?|audit trail|ai audit|model audit|prompt log(?:s|ging)?|"
        r"response log(?:s|ging)?|decision log(?:s|ging)?|trace prompts?|trace responses?|"
        r"model version|prompt version|who approved|reviewer action log|usage logging)\b",
        re.I,
    ),
    "fallback_behavior": re.compile(
        r"\b(?:fallback|fall back|graceful degradation|model timeout|provider outage|"
        r"provider error|low confidence|no answer|handoff to human|retry limit|"
        r"disable ai|baseline behavior|safe default|manual path|search results only)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[AISafetyRequirementCategory, str] = {
    "model_output_boundaries": "product",
    "human_review": "operations",
    "evaluation_dataset": "ml_evaluation",
    "sensitive_data_handling": "security_privacy",
    "prompt_injection_defense": "security",
    "safety_refusal_policy": "trust_and_safety",
    "audit_logging": "compliance",
    "fallback_behavior": "engineering",
}
_PLANNING_NOTE_BY_CATEGORY: dict[AISafetyRequirementCategory, str] = {
    "model_output_boundaries": "Define allowed topics, response format, grounding rules, citations, and uncertainty behavior.",
    "human_review": "Specify review triggers, reviewer permissions, queue states, approval outcomes, and SLA expectations.",
    "evaluation_dataset": "Create representative, adversarial, and regression prompt sets with expected outputs and scoring rubrics.",
    "sensitive_data_handling": "Document data minimization, redaction, retention, model-provider use, and training exclusion rules.",
    "prompt_injection_defense": "Plan instruction hierarchy, untrusted-content treatment, tool-call constraints, and exfiltration tests.",
    "safety_refusal_policy": "Align refusal categories, safe-completion wording, escalation paths, and policy-owner approvals.",
    "audit_logging": "Capture prompt, response, model version, safety decisions, reviewer actions, and retention expectations.",
    "fallback_behavior": "Define behavior for timeouts, provider errors, low confidence, refusals, and human handoff.",
}
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "risks",
    "security",
    "privacy",
    "compliance",
    "operations",
    "support",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_IGNORED_FIELDS = {
    "id",
    "source_brief_id",
    "source_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
    "source_links",
}


@dataclass(frozen=True, slots=True)
class SourceAISafetyRequirement:
    """One source-backed AI safety requirement."""

    source_brief_id: str | None
    category: AISafetyRequirementCategory
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: AISafetyRequirementConfidence = "medium"
    suggested_owner: str = "product"
    suggested_planning_note: str = ""

    @property
    def planning_note(self) -> str:
        """Compatibility view for callers that use shorter planning-note naming."""
        return self.suggested_planning_note

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "suggested_owner": self.suggested_owner,
            "suggested_planning_note": self.suggested_planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceAISafetyRequirementsReport:
    """Source-level AI safety requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceAISafetyRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAISafetyRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return AI safety requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source AI Safety Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source AI safety requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Confidence | Owner | Source Field Paths | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.category} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.suggested_owner)} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.suggested_planning_note)} |"
            )
        return "\n".join(lines)


def build_source_ai_safety_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAISafetyRequirementsReport:
    """Extract AI safety requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _category_index(requirement.category),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceAISafetyRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_ai_safety_requirements(source: Any) -> SourceAISafetyRequirementsReport:
    """Compatibility alias for building an AI safety requirements report."""
    return build_source_ai_safety_requirements(source)


def generate_source_ai_safety_requirements(source: Any) -> SourceAISafetyRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_ai_safety_requirements(source)


def derive_source_ai_safety_requirements(source: Any) -> SourceAISafetyRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_ai_safety_requirements(source)


def summarize_source_ai_safety_requirements(source_or_result: Any) -> dict[str, Any]:
    """Return deterministic counts for extracted AI safety requirements."""
    if isinstance(source_or_result, SourceAISafetyRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_ai_safety_requirements(source_or_result).summary


def source_ai_safety_requirements_to_dict(report: SourceAISafetyRequirementsReport) -> dict[str, Any]:
    """Serialize an AI safety requirements report to a plain dictionary."""
    return report.to_dict()


source_ai_safety_requirements_to_dict.__test__ = False


def source_ai_safety_requirements_to_dicts(
    requirements: (
        tuple[SourceAISafetyRequirement, ...]
        | list[SourceAISafetyRequirement]
        | SourceAISafetyRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize AI safety requirement records to dictionaries."""
    if isinstance(requirements, SourceAISafetyRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_ai_safety_requirements_to_dicts.__test__ = False


def source_ai_safety_requirements_to_markdown(report: SourceAISafetyRequirementsReport) -> str:
    """Render an AI safety requirements report as Markdown."""
    return report.to_markdown()


source_ai_safety_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: AISafetyRequirementCategory
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]
    confidence: AISafetyRequirementConfidence


def _source_payloads(source: Any) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(
        source, "model_dump"
    ):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Any) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_id(payload), payload
    return None, {}


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidates_for_briefs(brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        segments = _candidate_segments(payload)
        payload_has_ai_context = _payload_has_ai_context(segments)
        payload_negated = any(_NEGATED_SCOPE_RE.search(segment.text) for segment in segments)
        if payload_negated and not any(_category_matches(segment) for segment in segments):
            continue
        for segment in segments:
            if not _is_ai_requirement(segment, payload_has_ai_context):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            categories = [
                category
                for category in _CATEGORY_ORDER
                if _CATEGORY_PATTERNS[category].search(searchable)
            ]
            for category in _dedupe(categories):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        source_field_path=segment.source_field,
                        matched_terms=_matched_terms(category, searchable),
                        confidence=_confidence(segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAISafetyRequirement]:
    grouped: dict[tuple[str | None, AISafetyRequirementCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.category), []).append(candidate)

    requirements: list[SourceAISafetyRequirement] = []
    for (source_brief_id, category), items in grouped.items():
        evidence = tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:5]
        source_field_paths = tuple(sorted(_dedupe(item.source_field_path for item in items), key=str.casefold))
        matched_terms = tuple(
            sorted(_dedupe(term for item in items for term in item.matched_terms), key=str.casefold)
        )
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        requirements.append(
            SourceAISafetyRequirement(
                source_brief_id=source_brief_id,
                category=category,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                confidence=confidence,
                suggested_owner=_OWNER_BY_CATEGORY[category],
                suggested_planning_note=_PLANNING_NOTE_BY_CATEGORY[category],
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_words = _field_words(source_field)
    field_context = section_context or bool(
        _STRUCTURED_FIELD_RE.search(field_words) or _AI_CONTEXT_RE.search(field_words)
    )
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _AI_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        text_context = field_context or bool(_AI_CONTEXT_RE.search(text))
        for segment_text in _segments(text):
            segments.append(_Segment(source_field, segment_text, text_context))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for raw_line in value.splitlines() or [value]:
        cleaned = _clean_text(raw_line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(raw_line) or _CHECKBOX_RE.match(raw_line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _payload_has_ai_context(segments: Iterable[_Segment]) -> bool:
    return any(
        _AI_CONTEXT_RE.search(f"{_field_words(segment.source_field)} {segment.text}")
        for segment in segments
    )


def _category_matches(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    return any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())


def _is_ai_requirement(segment: _Segment, payload_has_ai_context: bool) -> bool:
    if _NEGATED_SCOPE_RE.search(segment.text):
        return False
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if not any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values()):
        return False
    has_ai_context = bool(_AI_CONTEXT_RE.search(searchable)) or segment.section_context or payload_has_ai_context
    if not has_ai_context:
        return False
    if _GENERIC_AUTOMATION_RE.search(searchable) and not (
        _AI_CONTEXT_RE.search(searchable) or segment.section_context or payload_has_ai_context
    ):
        return False
    if segment.section_context:
        return True
    if segment.source_field == "title" and not _REQUIRED_RE.search(segment.text):
        return False
    return bool(_REQUIRED_RE.search(segment.text) or _AI_CONTEXT_RE.search(searchable))


def _matched_terms(category: AISafetyRequirementCategory, text: str) -> tuple[str, ...]:
    return tuple(_dedupe(_clean_text(match.group(0)) for match in _CATEGORY_PATTERNS[category].finditer(text)))


def _confidence(segment: _Segment) -> AISafetyRequirementConfidence:
    if _REQUIRED_RE.search(segment.text) and segment.section_context:
        return "high"
    if _REQUIRED_RE.search(segment.text):
        return "high"
    if segment.section_context:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceAISafetyRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "owner_counts": {
            owner: sum(1 for requirement in requirements if requirement.suggested_owner == owner)
            for owner in sorted(_dedupe(_OWNER_BY_CATEGORY.values()), key=str.casefold)
        },
        "categories": [requirement.category for requirement in requirements],
        "status": "ready_for_planning" if requirements else "no_ai_safety_language",
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
        "problem",
        "problem_statement",
        "goal",
        "goals",
        "requirements",
        "constraints",
        "acceptance_criteria",
        "definition_of_done",
        "risks",
        "security",
        "privacy",
        "compliance",
        "operations",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _category_index(category: AISafetyRequirementCategory) -> int:
    return _CATEGORY_ORDER.index(category)


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "AISafetyRequirementCategory",
    "AISafetyRequirementConfidence",
    "SourceAISafetyRequirement",
    "SourceAISafetyRequirementsReport",
    "build_source_ai_safety_requirements",
    "derive_source_ai_safety_requirements",
    "extract_source_ai_safety_requirements",
    "generate_source_ai_safety_requirements",
    "source_ai_safety_requirements_to_dict",
    "source_ai_safety_requirements_to_dicts",
    "source_ai_safety_requirements_to_markdown",
    "summarize_source_ai_safety_requirements",
]
