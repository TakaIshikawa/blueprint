"""Extract regulatory and policy obligations from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


RegulatoryObligationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CONFIDENCE_ORDER: dict[RegulatoryObligationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_REGULATION_ORDER = (
    "GDPR",
    "CCPA/CPRA",
    "HIPAA",
    "SOC 2",
    "PCI DSS",
    "COPPA",
    "Accessibility",
    "Financial Retention",
    "Audit Logging",
    "Consent",
    "DPA",
    "Regional Data Handling",
)
_REGULATION_PATTERNS: dict[str, re.Pattern[str]] = {
    "GDPR": re.compile(r"\b(?:gdpr|general data protection regulation|data subject|right to erasure)\b", re.I),
    "CCPA/CPRA": re.compile(r"\b(?:ccpa|cpra|california privacy|do not sell|sensitive personal information)\b", re.I),
    "HIPAA": re.compile(r"\b(?:hipaa|phi|protected health information|business associate agreement|baa)\b", re.I),
    "SOC 2": re.compile(r"\b(?:soc\s*2|soc ii|trust services criteria|security availability confidentiality)\b", re.I),
    "PCI DSS": re.compile(r"\b(?:pci|pci[- ]?dss|cardholder data|payment card|pan token)\b", re.I),
    "COPPA": re.compile(r"\b(?:coppa|children'?s online privacy|under\s+13|child consent|parental consent)\b", re.I),
    "Accessibility": re.compile(r"\b(?:wcag|ada|section 508|accessibility|a11y|screen reader|keyboard navigation)\b", re.I),
    "Financial Retention": re.compile(
        r"\b(?:sox|finra|sec rule|financial retention|records retention|retain records|retention period)\b",
        re.I,
    ),
    "Audit Logging": re.compile(r"\b(?:audit log|audit trail|tamper[- ]?evident|immutable log|access log)\b", re.I),
    "Consent": re.compile(r"\b(?:consent|opt[- ]?in|opt[- ]?out|cookie banner|marketing preference)\b", re.I),
    "DPA": re.compile(r"\b(?:dpa|data processing agreement|processor agreement|subprocessor|standard contractual clauses|sccs)\b", re.I),
    "Regional Data Handling": re.compile(
        r"\b(?:data residency|data localisation|data localization|regional data|eu data|uk data|cross[- ]?border)\b",
        re.I,
    ),
}
_OBLIGATION_PATTERNS: dict[str, re.Pattern[str]] = {
    "consent": re.compile(r"\b(?:consent|opt[- ]?in|opt[- ]?out|authorization|permission)\b", re.I),
    "data_minimization": re.compile(r"\b(?:data minimization|minimise data|minimize data|least data|only collect)\b", re.I),
    "retention": re.compile(r"\b(?:retention|retain|retained|delete after|purge|recordkeeping|records)\b", re.I),
    "deletion": re.compile(r"\b(?:delete|deletion|erasure|right to be forgotten|purge)\b", re.I),
    "access_control": re.compile(r"\b(?:access control|access review|least privilege|role based|rbac|permission|authorize)\b", re.I),
    "audit_logging": re.compile(r"\b(?:audit log|audit logged|audit trail|access log|immutable log|tamper[- ]?evident)\b", re.I),
    "encryption": re.compile(r"\b(?:encrypt|encrypted|encryption|tokenize|tokenization|hash|hashed|masked|redact)\b", re.I),
    "breach_notice": re.compile(r"\b(?:breach|incident notice|notify regulator|notification deadline)\b", re.I),
    "data_residency": re.compile(r"\b(?:residency|localisation|localization|regional|cross[- ]?border|eu data)\b", re.I),
    "accessibility": re.compile(r"\b(?:wcag|screen reader|keyboard|contrast|captions|aria|section 508)\b", re.I),
    "vendor_agreement": re.compile(r"\b(?:dpa|baa|subprocessor|processor agreement|vendor agreement|sccs)\b", re.I),
    "security_control": re.compile(r"\b(?:soc\s*2|control|security review|availability|confidentiality|integrity)\b", re.I),
}
_QUESTION_RE = re.compile(r"\?|(?:\b(?:tbd|unknown|unclear|confirm|verify|whether|which region|which law|legal review)\b)", re.I)


@dataclass(frozen=True, slots=True)
class SourceRegulatoryObligation:
    """One source-backed regulatory or policy obligation."""

    regulation: str
    obligation_type: str
    confidence: RegulatoryObligationConfidence
    source_field: str
    evidence: str
    suggested_constraints: tuple[str, ...] = field(default_factory=tuple)
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)
    required_controls: tuple[str, ...] = field(default_factory=tuple)
    explicit: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "regulation": self.regulation,
            "obligation_type": self.obligation_type,
            "confidence": self.confidence,
            "source_field": self.source_field,
            "evidence": self.evidence,
            "suggested_constraints": list(self.suggested_constraints),
            "unresolved_questions": list(self.unresolved_questions),
            "required_controls": list(self.required_controls),
            "explicit": self.explicit,
        }


@dataclass(frozen=True, slots=True)
class SourceRegulatoryObligationsReport:
    """Brief-level regulatory obligation report."""

    brief_id: str | None = None
    obligations: tuple[SourceRegulatoryObligation, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "obligations": [obligation.to_dict() for obligation in self.obligations],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return regulatory obligations as plain dictionaries."""
        return [obligation.to_dict() for obligation in self.obligations]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Regulatory Obligations Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        confidence_counts = self.summary.get("confidence_counts", {})
        regulation_counts = self.summary.get("obligation_counts_by_regulation", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Obligations found: {self.summary.get('obligation_count', 0)}",
            "- Confidence counts: "
            f"high {confidence_counts.get('high', 0)}, "
            f"medium {confidence_counts.get('medium', 0)}, "
            f"low {confidence_counts.get('low', 0)}",
            f"- Explicit obligations: {self.summary.get('explicit_obligation_count', 0)}",
            f"- Suggested constraints: {self.summary.get('suggested_constraint_count', 0)}",
            f"- Unresolved questions: {self.summary.get('unresolved_question_count', 0)}",
            "- Regulation counts: "
            + (", ".join(f"{key} {regulation_counts[key]}" for key in sorted(regulation_counts)) or "none"),
        ]
        if not self.obligations:
            lines.extend(["", "No regulatory obligations were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Obligations",
                "",
                "| Regulation | Type | Confidence | Source | Evidence | Suggested Constraints | Questions | Controls |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for obligation in self.obligations:
            lines.append(
                "| "
                f"{_markdown_cell(obligation.regulation)} | "
                f"{_markdown_cell(obligation.obligation_type)} | "
                f"{obligation.confidence} | "
                f"{_markdown_cell(obligation.source_field)} | "
                f"{_markdown_cell(obligation.evidence)} | "
                f"{_markdown_cell('; '.join(obligation.suggested_constraints) or 'none')} | "
                f"{_markdown_cell('; '.join(obligation.unresolved_questions) or 'none')} | "
                f"{_markdown_cell('; '.join(obligation.required_controls) or 'none')} |"
            )
        return "\n".join(lines)


def build_source_regulatory_obligations(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceRegulatoryObligationsReport:
    """Extract regulatory obligations from SourceBrief-like and implementation brief inputs."""
    brief_id, payload = _source_payload(source)
    candidates = [*_explicit_obligations(payload), *_inferred_obligations(payload)]
    obligations = tuple(
        sorted(
            _dedupe_obligations(candidates),
            key=lambda obligation: (
                _CONFIDENCE_ORDER[obligation.confidence],
                _regulation_index(obligation.regulation),
                obligation.obligation_type,
                obligation.source_field,
                obligation.evidence.casefold(),
            ),
        )
    )
    confidence_counts = {
        confidence: sum(1 for obligation in obligations if obligation.confidence == confidence)
        for confidence in _CONFIDENCE_ORDER
    }
    regulation_counts = {
        regulation: sum(1 for obligation in obligations if obligation.regulation == regulation)
        for regulation in sorted({obligation.regulation for obligation in obligations})
    }
    suggested_constraints = tuple(
        _dedupe(constraint for obligation in obligations for constraint in obligation.suggested_constraints)
    )
    unresolved_questions = tuple(
        _dedupe(question for obligation in obligations for question in obligation.unresolved_questions)
    )
    return SourceRegulatoryObligationsReport(
        brief_id=brief_id,
        obligations=obligations,
        summary={
            "obligation_count": len(obligations),
            "obligation_counts_by_regulation": regulation_counts,
            "confidence_counts": confidence_counts,
            "explicit_obligation_count": sum(1 for obligation in obligations if obligation.explicit),
            "suggested_constraint_count": len(suggested_constraints),
            "unresolved_question_count": len(unresolved_questions),
            "suggested_constraints": list(suggested_constraints),
            "unresolved_questions": list(unresolved_questions),
        },
    )


def source_regulatory_obligations_to_dict(report: SourceRegulatoryObligationsReport) -> dict[str, Any]:
    """Serialize a source regulatory obligations report to a plain dictionary."""
    return report.to_dict()


source_regulatory_obligations_to_dict.__test__ = False


def source_regulatory_obligations_to_markdown(report: SourceRegulatoryObligationsReport) -> str:
    """Render a source regulatory obligations report as Markdown."""
    return report.to_markdown()


source_regulatory_obligations_to_markdown.__test__ = False


def _explicit_obligations(payload: Mapping[str, Any]) -> list[SourceRegulatoryObligation]:
    obligations: list[SourceRegulatoryObligation] = []
    for metadata_field, metadata in _metadata_sources(payload):
        for index, item in enumerate(_list_items(_mapping_get(metadata, "obligations", "regulatory_obligations", "policy_obligations"))):
            obligation = _explicit_obligation(item, f"{metadata_field}.obligations[{index}]")
            if obligation:
                obligations.append(obligation)
        for index, item in enumerate(_list_items(_mapping_get(metadata, "required_controls", "controls"))):
            obligation = _explicit_control_obligation(item, f"{metadata_field}.required_controls[{index}]")
            if obligation:
                obligations.append(obligation)
    return obligations


def _explicit_obligation(item: Any, source_field: str) -> SourceRegulatoryObligation | None:
    if isinstance(item, Mapping):
        evidence = _item_text(item) or _text(item)
        regulation = _canonical_regulation(_optional_text(item.get("regulation") or item.get("framework")) or evidence)
        obligation_type = _slug(_optional_text(item.get("type") or item.get("obligation_type") or item.get("control")) or "explicit_obligation")
        controls = tuple(_dedupe(_strings(item.get("required_controls") or item.get("controls"))))
        questions = tuple(_dedupe(_strings(item.get("unresolved_questions") or item.get("questions"))))
        constraints = tuple(_dedupe(_strings(item.get("suggested_constraints") or item.get("constraints"))))
        confidence = _confidence_value(item.get("confidence"), default="high")
    else:
        evidence = _optional_text(item) or ""
        regulation = _canonical_regulation(evidence)
        obligation_type = _first_obligation_type(evidence) or "explicit_obligation"
        controls = ()
        questions = ()
        constraints = ()
        confidence = "high"
    if not evidence:
        return None
    return SourceRegulatoryObligation(
        regulation=regulation,
        obligation_type=obligation_type,
        confidence=confidence,
        source_field=source_field,
        evidence=_clip(evidence),
        suggested_constraints=constraints or _suggested_constraints(regulation, obligation_type),
        unresolved_questions=questions,
        required_controls=controls,
        explicit=True,
    )


def _explicit_control_obligation(item: Any, source_field: str) -> SourceRegulatoryObligation | None:
    if isinstance(item, Mapping):
        evidence = _item_text(item) or _text(item)
        regulation = _canonical_regulation(_optional_text(item.get("regulation") or item.get("framework")) or evidence)
        control = _optional_text(item.get("control") or item.get("name") or item.get("type")) or evidence
        questions = tuple(_dedupe(_strings(item.get("unresolved_questions") or item.get("questions"))))
        confidence = _confidence_value(item.get("confidence"), default="high")
    else:
        evidence = _optional_text(item) or ""
        regulation = _canonical_regulation(evidence)
        control = evidence
        questions = ()
        confidence = "high"
    if not evidence:
        return None
    obligation_type = _first_obligation_type(f"{control} {evidence}") or "required_control"
    return SourceRegulatoryObligation(
        regulation=regulation,
        obligation_type=obligation_type,
        confidence=confidence,
        source_field=source_field,
        evidence=_clip(evidence),
        suggested_constraints=_suggested_constraints(regulation, obligation_type),
        unresolved_questions=questions,
        required_controls=tuple(_dedupe([control])),
        explicit=True,
    )


def _inferred_obligations(payload: Mapping[str, Any]) -> list[SourceRegulatoryObligation]:
    obligations: list[SourceRegulatoryObligation] = []
    for source_field, text in _candidate_texts(payload):
        regulations = [regulation for regulation, pattern in _REGULATION_PATTERNS.items() if pattern.search(text)]
        obligation_types = [name for name, pattern in _OBLIGATION_PATTERNS.items() if pattern.search(text)]
        if not regulations and obligation_types:
            regulations = _regulations_for_obligation_types(obligation_types)
        if not regulations:
            continue
        if not obligation_types:
            obligation_types = [_default_obligation_type(regulation) for regulation in regulations]
        for regulation in regulations:
            for obligation_type in obligation_types:
                obligations.append(
                    SourceRegulatoryObligation(
                        regulation=regulation,
                        obligation_type=obligation_type,
                        confidence=_inferred_confidence(text, regulation, obligation_type),
                        source_field=source_field,
                        evidence=_clip(text),
                        suggested_constraints=_suggested_constraints(regulation, obligation_type),
                        unresolved_questions=_unresolved_questions(text, regulation, obligation_type),
                        required_controls=_required_controls(regulation, obligation_type),
                        explicit=False,
                    )
                )
    return obligations


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


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    for field_name in (
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
    ):
        if text := _optional_text(payload.get(field_name)):
            texts.append((field_name, text))
        if text := _optional_text(source_payload.get(field_name)):
            texts.append((f"source_payload.{field_name}", text))
    for field_name in (
        "goals",
        "constraints",
        "implementation_constraints",
        "acceptance_criteria",
        "definition_of_done",
        "requirements",
        "scope",
        "non_goals",
        "assumptions",
        "risks",
        "open_questions",
        "questions",
        "integration_points",
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
        texts = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            source_field = f"{field_name}.{key}"
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_field_texts(child, source_field))
            elif text := _optional_text(child):
                texts.append((source_field, text))
        return texts
    return [(f"{field_name}[{index}]", text) for index, text in enumerate(_strings(value))]


def _metadata_sources(payload: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    sources = []
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    for field_name, value in (
        ("metadata", payload.get("metadata")),
        ("brief_metadata", payload.get("brief_metadata")),
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if _any_signal(key_text):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _any_signal(key_text):
                texts.append((field, str(key)))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            texts.extend(_metadata_texts(item, f"{prefix}[{index}]"))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in (*_REGULATION_PATTERNS.values(), *_OBLIGATION_PATTERNS.values()))


def _canonical_regulation(value: str) -> str:
    for regulation, pattern in _REGULATION_PATTERNS.items():
        if pattern.search(value):
            return regulation
    normalized = value.strip().upper()
    return normalized if normalized in _REGULATION_ORDER else "Policy"


def _first_obligation_type(value: str) -> str | None:
    for obligation_type, pattern in _OBLIGATION_PATTERNS.items():
        if pattern.search(value):
            return obligation_type
    return None


def _default_obligation_type(regulation: str) -> str:
    return {
        "Accessibility": "accessibility",
        "Audit Logging": "audit_logging",
        "Consent": "consent",
        "DPA": "vendor_agreement",
        "Financial Retention": "retention",
        "Regional Data Handling": "data_residency",
        "SOC 2": "security_control",
    }.get(regulation, "compliance_requirement")


def _regulations_for_obligation_types(obligation_types: Iterable[str]) -> list[str]:
    mapping = {
        "accessibility": "Accessibility",
        "audit_logging": "Audit Logging",
        "consent": "Consent",
        "data_residency": "Regional Data Handling",
        "retention": "Financial Retention",
        "vendor_agreement": "DPA",
        "security_control": "SOC 2",
    }
    return _dedupe(mapping[obligation_type] for obligation_type in obligation_types if obligation_type in mapping)


def _inferred_confidence(text: str, regulation: str, obligation_type: str) -> RegulatoryObligationConfidence:
    regulation_hit = _REGULATION_PATTERNS[regulation].search(text) if regulation in _REGULATION_PATTERNS else None
    obligation_hit = _OBLIGATION_PATTERNS[obligation_type].search(text) if obligation_type in _OBLIGATION_PATTERNS else None
    if regulation_hit and obligation_hit and re.search(r"\b(?:must|required|require|requires|shall|need|needs|cannot|never)\b", text, re.I):
        return "high"
    if regulation_hit and obligation_hit:
        return "medium"
    return "low"


def _suggested_constraints(regulation: str, obligation_type: str) -> tuple[str, ...]:
    constraints = {
        "consent": "Capture and enforce consent state before processing regulated personal data.",
        "data_minimization": "Limit collected and retained data to the fields required for the approved workflow.",
        "retention": "Define retention, archive, and deletion behavior for regulated records before launch.",
        "deletion": "Support deletion or erasure workflows with auditable exceptions for required retention.",
        "access_control": "Restrict regulated data access by role and record authorization decisions.",
        "audit_logging": "Emit tamper-evident audit logs for regulated access, changes, exports, and deletions.",
        "encryption": "Encrypt or tokenize regulated data in storage, transit, logs, analytics, and exports.",
        "breach_notice": "Define incident detection, escalation, and notification evidence for regulated data exposure.",
        "data_residency": "Route, store, and process regulated data only in approved regions and subprocessors.",
        "accessibility": "Validate the workflow against the required accessibility standard before release.",
        "vendor_agreement": "Confirm required processor, vendor, and subprocessor agreements before data sharing.",
        "security_control": "Map implementation evidence to required security, availability, and confidentiality controls.",
        "required_control": "Track the required control as an implementation constraint with acceptance evidence.",
        "compliance_requirement": "Translate the cited regulation into explicit implementation constraints and acceptance criteria.",
    }
    text = constraints.get(obligation_type, constraints["compliance_requirement"])
    if regulation != "Policy":
        text = f"{regulation}: {text}"
    return (text,)


def _required_controls(regulation: str, obligation_type: str) -> tuple[str, ...]:
    controls = {
        "HIPAA": ("access controls", "audit controls", "transmission security"),
        "PCI DSS": ("cardholder data tokenization", "restricted card data logging", "access controls"),
        "SOC 2": ("change management evidence", "access review evidence", "audit logging"),
        "Accessibility": ("WCAG validation", "keyboard navigation", "screen reader checks"),
    }
    if obligation_type == "audit_logging":
        return ("audit log retention", "log integrity review")
    return controls.get(regulation, ())


def _unresolved_questions(text: str, regulation: str, obligation_type: str) -> tuple[str, ...]:
    questions = []
    if _QUESTION_RE.search(text):
        questions.append(f"Resolve source question for {regulation} {obligation_type}: {_clip(text)}")
    if regulation == "Regional Data Handling" and not re.search(r"\b(?:eu|uk|us|ca|california|region|country)\b", text, re.I):
        questions.append("Confirm the exact regions, residency boundary, and cross-border transfer rules.")
    if regulation == "Policy" or obligation_type == "compliance_requirement":
        questions.append("Identify the owning regulation or policy clause for this obligation.")
    return tuple(_dedupe(questions))


def _dedupe_obligations(
    obligations: Iterable[SourceRegulatoryObligation],
) -> list[SourceRegulatoryObligation]:
    deduped: list[SourceRegulatoryObligation] = []
    seen: set[tuple[str, str, str]] = set()
    for obligation in obligations:
        key = (obligation.regulation.casefold(), obligation.obligation_type, obligation.evidence.casefold())
        if key in seen:
            continue
        deduped.append(obligation)
        seen.add(key)
    return deduped


def _mapping_get(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _item_text(item: Any) -> str:
    if isinstance(item, Mapping):
        for key in ("text", "evidence", "description", "summary", "requirement", "constraint", "name", "value"):
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
        "constraints",
        "implementation_constraints",
        "acceptance_criteria",
        "definition_of_done",
        "requirements",
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


def _confidence_value(value: Any, *, default: RegulatoryObligationConfidence) -> RegulatoryObligationConfidence:
    text = _text(value).casefold()
    return text if text in _CONFIDENCE_ORDER else default  # type: ignore[return-value]


def _slug(value: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.casefold())).strip("_") or "obligation"


def _regulation_index(regulation: str) -> int:
    try:
        return _REGULATION_ORDER.index(regulation)
    except ValueError:
        return len(_REGULATION_ORDER)


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
    "RegulatoryObligationConfidence",
    "SourceRegulatoryObligation",
    "SourceRegulatoryObligationsReport",
    "build_source_regulatory_obligations",
    "source_regulatory_obligations_to_dict",
    "source_regulatory_obligations_to_markdown",
]
