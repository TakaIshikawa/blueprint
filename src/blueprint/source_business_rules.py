"""Extract explicit business rules from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


BusinessRuleCategory = Literal[
    "eligibility",
    "threshold",
    "limit",
    "approval",
    "billing",
    "lifecycle",
    "exception",
    "access",
    "validation",
    "general",
]
BusinessRuleConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_MARKDOWN_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_STRONG_RULE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|only if|cannot|can't|can not|must not|may not|"
    r"unless|except(?:ion)?|approval|approve|approved|eligible|eligibility|threshold|limit|billing rule)\b",
    re.I,
)
_DIRECTIVE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|only|only if|cannot|can't|can not|must not|may not|"
    r"unless|eligible|eligibility|approve|approved|approval before|approval after|approval within)\b",
    re.I,
)
_WEAK_RULE_RE = re.compile(
    r"\b(?:should|needs? to|ensure|block|allow|deny|permitted|prohibited|state|status|"
    r"lifecycle|invoice|charge|refund|validate|validation)\b",
    re.I,
)
_CONCRETE_RE = re.compile(
    r"(?:\b\d+(?:[,.]\d+)?\b|[$€£]\s*\d+|\b(?:before|after|during|within|when|if|unless|"
    r"for|per|over|under|at least|more than|less than)\b)",
    re.I,
)
_EXPLICIT_FIELDS = (
    "business_rules",
    "business_rule",
    "rules",
    "rule",
    "eligibility_rules",
    "approval_rules",
    "billing_rules",
    "lifecycle_rules",
    "exception_rules",
    "validation_rules",
    "limits",
    "thresholds",
)
_SCANNED_TEXT_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "problem_statement",
    "mvp_goal",
    "workflow_context",
    "architecture_notes",
    "data_requirements",
    "validation_plan",
    "implementation_notes",
)
_SCANNED_COLLECTION_FIELDS = (
    "goals",
    "requirements",
    "constraints",
    "implementation_constraints",
    "acceptance_criteria",
    "definition_of_done",
    "scope",
    "non_goals",
    "assumptions",
    "risks",
    "open_questions",
    "questions",
    "integration_points",
)
_CONFIDENCE_ORDER: dict[BusinessRuleConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_CATEGORY_ORDER: tuple[BusinessRuleCategory, ...] = (
    "eligibility",
    "threshold",
    "limit",
    "approval",
    "billing",
    "lifecycle",
    "exception",
    "access",
    "validation",
    "general",
)
_CATEGORY_PATTERNS: dict[BusinessRuleCategory, re.Pattern[str]] = {
    "eligibility": re.compile(r"\b(?:eligible|eligibility|qualif(?:y|ies|ied)|criteria|only if)\b", re.I),
    "threshold": re.compile(r"\b(?:threshold|over|under|above|below|at least|more than|less than|minimum|maximum)\b", re.I),
    "limit": re.compile(r"\b(?:limit|limits|limited|cap|capped|quota|max(?:imum)?|min(?:imum)?)\b", re.I),
    "approval": re.compile(r"\b(?:approval|approve|approved|approver|review|sign[- ]off|authorization)\b", re.I),
    "billing": re.compile(r"\b(?:billing|billable|invoice|charge|charged|payment|refund|credit|subscription)\b", re.I),
    "lifecycle": re.compile(r"\b(?:lifecycle|state|status|transition|activate|deactivate|archive|expire|expiration|expired|renew)\b", re.I),
    "exception": re.compile(r"\b(?:exception|except|unless|override|waiver|fallback|grace period|error handling)\b", re.I),
    "access": re.compile(r"\b(?:access|permission|role|admin|owner|viewer|allow|deny|block|cannot|can not|can't)\b", re.I),
    "validation": re.compile(r"\b(?:validate|validation|required field|invalid|reject|must include|must show)\b", re.I),
    "general": re.compile(r"\b(?:must|shall|required|requires?|should|ensure|business rule)\b", re.I),
}


@dataclass(frozen=True, slots=True)
class SourceBusinessRule:
    """One source-backed business rule."""

    rule_text: str
    category: BusinessRuleCategory
    confidence: BusinessRuleConfidence
    source_field: str
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "rule_text": self.rule_text,
            "category": self.category,
            "confidence": self.confidence,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceBusinessRulesReport:
    """Brief-level business rules report."""

    brief_id: str | None = None
    title: str | None = None
    business_rules: tuple[SourceBusinessRule, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceBusinessRule, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.business_rules

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "business_rules": [rule.to_dict() for rule in self.business_rules],
            "records": [rule.to_dict() for rule in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return business rule records as plain dictionaries."""
        return [rule.to_dict() for rule in self.business_rules]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Business Rules Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        confidence_counts = self.summary.get("confidence_counts", {})
        category_counts = self.summary.get("category_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Business rules found: {self.summary.get('rule_count', 0)}",
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
        ]
        if not self.business_rules:
            lines.extend(["", "No source business rules were found in the brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Business Rules",
                "",
                "| Category | Rule | Confidence | Source Field | Evidence |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for rule in self.business_rules:
            lines.append(
                "| "
                f"{rule.category} | "
                f"{_markdown_cell(rule.rule_text)} | "
                f"{rule.confidence} | "
                f"{_markdown_cell(rule.source_field)} | "
                f"{_markdown_cell('; '.join(rule.evidence))} |"
            )
        return "\n".join(lines)


def build_source_business_rules(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceBusinessRulesReport:
    """Extract business rules from SourceBrief-like and implementation brief inputs."""
    brief_id, payload = _source_payload(source)
    rules = tuple(_dedupe_and_sort_rules(_rule_candidates(payload)))
    return SourceBusinessRulesReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        business_rules=rules,
        summary=_summary(rules),
    )


def summarize_source_business_rules(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceBusinessRulesReport:
    """Compatibility helper for callers that use summarize_* naming."""
    return build_source_business_rules(source)


def derive_source_business_rules(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceBusinessRulesReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_business_rules(source)


def generate_source_business_rules(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceBusinessRulesReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_business_rules(source)


def extract_source_business_rules(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceBusinessRule, ...]:
    """Return business rule records from brief-shaped input."""
    return build_source_business_rules(source).business_rules


def source_business_rules_to_dict(report: SourceBusinessRulesReport) -> dict[str, Any]:
    """Serialize a source business rules report to a plain dictionary."""
    return report.to_dict()


source_business_rules_to_dict.__test__ = False


def source_business_rules_to_dicts(
    rules: tuple[SourceBusinessRule, ...] | list[SourceBusinessRule] | SourceBusinessRulesReport,
) -> list[dict[str, Any]]:
    """Serialize business rule records to dictionaries."""
    if isinstance(rules, SourceBusinessRulesReport):
        return rules.to_dicts()
    return [rule.to_dict() for rule in rules]


source_business_rules_to_dicts.__test__ = False


def source_business_rules_to_markdown(report: SourceBusinessRulesReport) -> str:
    """Render a source business rules report as Markdown."""
    return report.to_markdown()


source_business_rules_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    rule_text: str
    category: BusinessRuleCategory
    confidence: BusinessRuleConfidence
    source_field: str
    evidence: str
    explicit: bool = False


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = source.model_dump(mode="python")
        return _brief_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _brief_id(payload), payload
    if isinstance(source, Mapping):
        payload = _validated_payload(source)
        return _brief_id(payload), payload
    payload = _object_payload(source)
    return _brief_id(payload), payload


def _validated_payload(source: Mapping[str, Any]) -> dict[str, Any]:
    for model in (SourceBrief, ImplementationBrief):
        try:
            return dict(model.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            continue
    return dict(source)


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _rule_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    candidates.extend(_explicit_rule_candidates(payload))
    for source_field, text, section_hint in _candidate_texts(payload):
        if not _is_rule_signal(text, source_field, section_hint):
            continue
        candidates.append(
            _Candidate(
                rule_text=_rule_text(text),
                category=_category(f"{section_hint or ''} {source_field} {text}"),
                confidence=_confidence(text, source_field, explicit=bool(section_hint)),
                source_field=source_field,
                evidence=_clip(text),
                explicit=bool(section_hint),
            )
        )
    return candidates


def _explicit_rule_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    for container_name, container in (
        ("payload", payload),
        ("source_payload", source_payload),
        *[(name, value) for name, value in _metadata_sources(payload)],
    ):
        if not isinstance(container, Mapping):
            continue
        prefix = "" if container_name == "payload" else container_name
        for field_name in _EXPLICIT_FIELDS:
            if field_name not in container:
                continue
            for index, item in enumerate(_list_items(container.get(field_name))):
                source_field = f"{prefix + '.' if prefix else ''}{field_name}[{index}]"
                candidate = _explicit_rule_candidate(item, source_field, field_name)
                if candidate:
                    candidates.append(candidate)
    return candidates


def _explicit_rule_candidate(item: Any, source_field: str, field_name: str) -> _Candidate | None:
    if isinstance(item, Mapping):
        evidence = _item_text(item) or _text(item)
        rule_text = _optional_text(
            item.get("rule_text")
            or item.get("rule")
            or item.get("business_rule")
            or item.get("requirement")
            or item.get("constraint")
            or item.get("text")
            or item.get("description")
            or item.get("name")
        ) or evidence
        category = _category(
            _optional_text(item.get("category") or item.get("type") or item.get("rule_type"))
            or f"{field_name} {rule_text}"
        )
        confidence = _confidence_value(item.get("confidence"), default="high")
    else:
        evidence = _optional_text(item) or ""
        rule_text = evidence
        category = _category(f"{field_name} {evidence}")
        confidence = "high"
    if not evidence or not _is_rule_signal(evidence, source_field, field_name):
        return None
    return _Candidate(
        rule_text=_rule_text(rule_text),
        category=category,
        confidence=confidence,
        source_field=source_field,
        evidence=_clip(evidence),
        explicit=True,
    )


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str, str | None]]:
    texts: list[tuple[str, str, str | None]] = []
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    for field_name in _SCANNED_TEXT_FIELDS:
        texts.extend(_text_segments(payload.get(field_name), field_name))
        texts.extend(_text_segments(source_payload.get(field_name), f"source_payload.{field_name}"))
    for field_name in _SCANNED_COLLECTION_FIELDS:
        texts.extend(_field_texts(payload.get(field_name), field_name))
        texts.extend(_field_texts(source_payload.get(field_name), f"source_payload.{field_name}"))
    for field, text, hint in _metadata_texts(payload.get("metadata")):
        texts.append((field, text, hint))
    for field, text, hint in _metadata_texts(payload.get("brief_metadata"), "brief_metadata"):
        texts.append((field, text, hint))
    for field, text, hint in _metadata_texts(source_payload.get("metadata"), "source_payload.metadata"):
        texts.append((field, text, hint))
    return [(field, text, hint) for field, text, hint in texts if text]


def _text_segments(value: Any, field_name: str) -> list[tuple[str, str, str | None]]:
    text = value.strip() if isinstance(value, str) else _optional_text(value)
    if text:
        return [
            (field_name if hint is None else f"{field_name}.{_slug(hint)}[{index}]", segment, hint)
            for index, (segment, hint) in enumerate(_segments(text))
        ]
    return []


def _field_texts(value: Any, field_name: str) -> list[tuple[str, str, str | None]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str, str | None]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{field_name}.{key}"
            key_hint = str(key).replace("_", " ")
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_field_texts(child, child_field))
            else:
                for text_field, text, hint in _text_segments(child, child_field):
                    texts.append((text_field, text, hint or key_hint if _section_is_ruleish(key_hint) else hint))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            texts.extend(_field_texts(item, f"{field_name}[{index}]"))
        return texts
    return _text_segments(value, field_name)


def _metadata_sources(payload: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    return [
        (field_name, value)
        for field_name, value in (
            ("metadata", payload.get("metadata")),
            ("brief_metadata", payload.get("brief_metadata")),
            ("source_payload.metadata", source_payload.get("metadata")),
        )
        if isinstance(value, Mapping)
    ]


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str, str | None]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str, str | None]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{prefix}.{key}"
            key_hint = str(key).replace("_", " ")
            hint = key_hint if _section_is_ruleish(key_hint) else None
            if str(key) in _EXPLICIT_FIELDS:
                continue
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, child_field))
            else:
                for field, text, child_hint in _text_segments(child, child_field):
                    texts.append((field, text, child_hint or hint))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            texts.extend(_metadata_texts(item, f"{prefix}[{index}]"))
        return texts
    return _text_segments(value, prefix)


def _segments(value: str) -> list[tuple[str, str | None]]:
    segments: list[tuple[str, str | None]] = []
    section_hint: str | None = None
    for raw_line in value.splitlines():
        if heading := _MARKDOWN_HEADING_RE.match(raw_line.strip()):
            title = _clean_text(heading.group("title"))
            section_hint = title if _section_is_ruleish(title) else None
            continue
        line = _clean_text(raw_line)
        if not line:
            continue
        if re.match(r"^\|?\s*:?-{3,}:?", line):
            continue
        for sentence in _SENTENCE_SPLIT_RE.split(line):
            for clause in _CLAUSE_SPLIT_RE.split(sentence):
                text = _clean_text(clause)
                if text and not re.fullmatch(r"\|?\s*-+\s*\|?", text):
                    segments.append((text, section_hint))
    return segments


def _section_is_ruleish(value: str) -> bool:
    return bool(
        re.search(
            r"\b(?:business rules?|rules?|eligibility|thresholds?|limits?|approvals?|billing|"
            r"lifecycle|states?|exceptions?|validation)\b",
            value,
            re.I,
        )
    )


def _is_rule_signal(text: str, source_field: str, section_hint: str | None) -> bool:
    if source_field == "title" and not _STRONG_RULE_RE.search(text):
        return False
    explicit_field = any(marker in source_field.casefold() for marker in ("rule", "criteria", "constraint"))
    return bool(
        (_STRONG_RULE_RE.search(text) and (_DIRECTIVE_RE.search(text) or _CONCRETE_RE.search(text) or explicit_field))
        or (_WEAK_RULE_RE.search(text) and _CONCRETE_RE.search(text))
        or (section_hint and (_WEAK_RULE_RE.search(text) or _CONCRETE_RE.search(text)))
    )


def _category(value: str) -> BusinessRuleCategory:
    normalized = _slug(value)
    if normalized in _CATEGORY_ORDER:
        return normalized  # type: ignore[return-value]
    for category in _CATEGORY_ORDER:
        if category != "general" and _CATEGORY_PATTERNS[category].search(value):
            return category
    return "general"


def _confidence(text: str, source_field: str, *, explicit: bool) -> BusinessRuleConfidence:
    normalized_field = source_field.replace("-", "_").casefold()
    if explicit:
        return "high"
    if _STRONG_RULE_RE.search(text) and (
        _CONCRETE_RE.search(text)
        or any(marker in normalized_field for marker in ("acceptance_criteria", "constraint", "definition_of_done"))
    ):
        return "high"
    if _STRONG_RULE_RE.search(text) or _WEAK_RULE_RE.search(text):
        return "medium"
    return "low"


def _confidence_value(value: Any, *, default: BusinessRuleConfidence) -> BusinessRuleConfidence:
    text = _text(value).casefold()
    return text if text in _CONFIDENCE_ORDER else default  # type: ignore[return-value]


def _rule_text(text: str) -> str:
    return _clip(re.sub(r"^(?:business rule|rule|policy|constraint)\s*[:=-]\s*", "", _clean_text(text), flags=re.I))


def _dedupe_and_sort_rules(candidates: Iterable[_Candidate]) -> list[SourceBusinessRule]:
    grouped: dict[str, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(_dedupe_key(candidate.rule_text), []).append(candidate)

    rules: list[SourceBusinessRule] = []
    for items in grouped.values():
        best = sorted(
            items,
            key=lambda item: (
                _CONFIDENCE_ORDER[item.confidence],
                0 if item.explicit else 1,
                _CATEGORY_ORDER.index(item.category),
                item.source_field.casefold(),
                item.rule_text.casefold(),
            ),
        )[0]
        evidence = tuple(_dedupe(item.evidence for item in sorted(items, key=lambda item: item.source_field.casefold())))[:4]
        rules.append(
            SourceBusinessRule(
                rule_text=best.rule_text,
                category=best.category,
                confidence=best.confidence,
                source_field=best.source_field,
                evidence=evidence,
            )
        )
    return sorted(
        rules,
        key=lambda rule: (
            _CONFIDENCE_ORDER[rule.confidence],
            _CATEGORY_ORDER.index(rule.category),
            rule.source_field.casefold(),
            rule.rule_text.casefold(),
        ),
    )


def _dedupe_key(value: str) -> str:
    text = re.sub(r"^(?:business rule|rule|policy|constraint)\s*[:=-]\s*", "", value, flags=re.I)
    text = re.sub(r"\b(?:must|shall|should|is required to|are required to)\b", "", text, flags=re.I)
    text = re.sub(r"[^a-z0-9]+", " ", text.casefold())
    return _SPACE_RE.sub(" ", text).strip()


def _summary(rules: tuple[SourceBusinessRule, ...]) -> dict[str, Any]:
    return {
        "rule_count": len(rules),
        "categories": [rule.category for rule in rules],
        "category_counts": {
            category: sum(1 for rule in rules if rule.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for rule in rules if rule.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "source_fields": sorted({rule.source_field for rule in rules}),
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
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
        "goals",
        "requirements",
        "constraints",
        "implementation_constraints",
        "acceptance_criteria",
        "definition_of_done",
        "scope",
        "non_goals",
        "assumptions",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "validation_plan",
        "open_questions",
        "questions",
        "metadata",
        "brief_metadata",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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


def _item_text(item: Any) -> str:
    if isinstance(item, Mapping):
        for key in ("rule_text", "rule", "business_rule", "text", "evidence", "description", "summary", "requirement", "constraint", "name", "value"):
            if text := _optional_text(item.get(key)):
                return text
        return ""
    return _optional_text(item) or ""


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    text = re.sub(r"^\|", "", text).strip()
    text = re.sub(r"\|$", "", text).strip()
    return _SPACE_RE.sub(" ", text).strip()


def _clip(value: str) -> str:
    text = _clean_text(value)
    return f"{text[:177].rstrip()}..." if len(text) > 180 else text


def _slug(value: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.casefold())).strip("_") or "general"


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
    "BusinessRuleCategory",
    "BusinessRuleConfidence",
    "SourceBusinessRule",
    "SourceBusinessRulesReport",
    "build_source_business_rules",
    "derive_source_business_rules",
    "extract_source_business_rules",
    "generate_source_business_rules",
    "source_business_rules_to_dict",
    "source_business_rules_to_dicts",
    "source_business_rules_to_markdown",
    "summarize_source_business_rules",
]
