"""Extract source-level account deletion and right-to-erasure requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


AccountDeletionRequirementType = Literal[
    "account_deletion",
    "workspace_deletion",
    "soft_delete",
    "hard_delete",
    "anonymization",
    "cancellation_triggered_deletion",
    "restoration_window",
    "downstream_deletion",
    "audit_exception",
    "customer_confirmation",
]
AccountDeletionConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_REQUIREMENT_ORDER: tuple[AccountDeletionRequirementType, ...] = (
    "account_deletion",
    "workspace_deletion",
    "soft_delete",
    "hard_delete",
    "anonymization",
    "cancellation_triggered_deletion",
    "restoration_window",
    "downstream_deletion",
    "audit_exception",
    "customer_confirmation",
)
_CONFIDENCE_ORDER: dict[AccountDeletionConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_PLANNING_IMPACTS: dict[AccountDeletionRequirementType, tuple[str, ...]] = {
    "account_deletion": ("data purge jobs", "customer notices"),
    "workspace_deletion": ("tenant data purge jobs", "admin confirmation"),
    "soft_delete": ("recovery windows", "tombstone states"),
    "hard_delete": ("data purge jobs", "backup purge policy"),
    "anonymization": ("PII anonymization jobs", "retention exceptions"),
    "cancellation_triggered_deletion": ("cancellation workflow hooks", "data purge jobs"),
    "restoration_window": ("recovery windows", "customer notices"),
    "downstream_deletion": ("processor callbacks", "subprocessor deletion tracking"),
    "audit_exception": ("retention exceptions", "audit evidence carve-outs"),
    "customer_confirmation": ("customer notices", "confirmation receipts"),
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)]|\[[ xX]\])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SIGNAL_RE = re.compile(
    r"\b(?:account deletion|delete (?:their |my |the )?account|close account|account closure|right to erasure|"
    r"right to be forgotten|erasure request|deletion request|workspace deletion|delete workspace|"
    r"tenant deletion|organization deletion|soft delete|hard delete|permanent(?:ly)? delete|"
    r"purge|tombstone|anonymi[sz]e|de[- ]identify|redact|subscription cancellation|"
    r"account cancellation|cancel(?:led|lation)?|grace period|restore|restoration|recover|"
    r"downstream|processor|subprocessor|vendor|third[- ]party|audit exception|legal hold|"
    r"do not delete|retain audit|confirmation|notify customer|customer notice)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:account deletion|workspace deletion|right to erasure|"
    r"erasure|deletion)\b.{0,80}\b(?:required|requirements?|needed|changes?|scope|impact)\b",
    re.I,
)
_REQUIREMENT_LANGUAGE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|support|"
    r"allow|enable|provide|keep|trigger|upon|after|within|before|when|if|unless|acceptance|"
    r"done when|policy|privacy|compliance)\b",
    re.I,
)
_WINDOW_RE = re.compile(
    r"\b(?:(?:for|within|after|during|before|up to)\s+)?(?:\d+(?:\.\d+)?|one|two|three|"
    r"four|five|six|seven|eight|nine|ten|eleven|twelve|fourteen|fifteen|thirty|sixty|"
    r"ninety)\s+(?:calendar\s+)?(?:days?|weeks?|months?|years?|hours?)\b",
    re.I,
)
_TRIGGER_RE = re.compile(
    r"\b(?:upon|on|after|when|once|following)\s+(?:account deletion|account closure|"
    r"delete account request|deletion request|erasure request|right to erasure request|"
    r"workspace deletion|workspace closure|subscription cancellation|account cancellation|"
    r"contract termination|customer request|user request)\b",
    re.I,
)
_SCOPE_RE = re.compile(
    r"\b(?:account|user|customer|profile|personal|pii|workspace|tenant|organization|org|"
    r"project|billing|invoice|payment|audit|event|session|backup|log|analytics|support)"
    r"(?:[- ](?:account|user|customer|profile|personal|pii|data|records?|workspaces?|"
    r"tenants?|organizations?|orgs?|projects?|billing|invoices?|payments?|audit|events?|"
    r"sessions?|backups?|logs?|analytics|support|files?|attachments?))*\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:account|workspace|tenant|organization|deletion|delete|erasure|erase|forgotten|"
    r"privacy|gdpr|retention|anonymi[sz]ation|processor|subprocessor|audit|legal[_ -]?hold|"
    r"cancellation|grace|restore|recovery|confirmation|notice|requirements?|acceptance|"
    r"constraints?|definition[_ -]?of[_ -]?done)",
    re.I,
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
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
    "scope",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "privacy",
    "compliance",
    "retention",
    "risks",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_TYPE_PATTERNS: dict[AccountDeletionRequirementType, re.Pattern[str]] = {
    "account_deletion": re.compile(
        r"\b(?:account deletion|delete (?:their |my |the )?account|deleting accounts?|"
        r"close account|account closure|right to erasure|right to be forgotten|"
        r"erasure request|deletion request)\b",
        re.I,
    ),
    "workspace_deletion": re.compile(
        r"\b(?:workspace deletion|delete workspace|workspace closure|tenant deletion|"
        r"delete tenant|organization deletion|delete organization|org deletion|delete org)\b",
        re.I,
    ),
    "soft_delete": re.compile(r"\b(?:soft delete|soft-delete|tombstone|mark(?:ed)? as deleted)\b", re.I),
    "hard_delete": re.compile(
        r"\b(?:hard delete|hard-delete|permanent(?:ly)? delete|purge|physical deletion|"
        r"irreversible deletion)\b",
        re.I,
    ),
    "anonymization": re.compile(
        r"\b(?:anonymi[sz]e|anonymi[sz]ation|de[- ]identify|deidentification|redact|"
        r"pseudonymi[sz]e|scrub pii)\b",
        re.I,
    ),
    "cancellation_triggered_deletion": re.compile(
        r"\b(?:(?:subscription|account|plan|contract) cancellation|cancel(?:led|lation)?|"
        r"contract termination)\b.{0,90}\b(?:delete|deletion|erase|erasure|purge|close)\b|"
        r"\b(?:delete|deletion|erase|erasure|purge)\b.{0,90}\b(?:subscription|account|plan|contract) cancellation\b",
        re.I,
    ),
    "restoration_window": re.compile(
        r"\b(?:restore|restoration|recover|recovery|undo deletion|grace period|deletion window|"
        r"cooling[- ]off period)\b",
        re.I,
    ),
    "downstream_deletion": re.compile(
        r"\b(?:downstream|processor|subprocessor|vendor|third[- ]party|external provider|"
        r"replicated data|processor deletion|callback|webhook)\b.{0,90}\b(?:delete|deletion|erase|erasure|purge)\b|"
        r"\b(?:delete|deletion|erase|erasure|purge)\b.{0,90}\b(?:downstream|processor|subprocessor|vendor|third[- ]party|external provider|replicated data)\b",
        re.I,
    ),
    "audit_exception": re.compile(
        r"\b(?:audit exception|audit carve[- ]out|legal hold|litigation hold|regulatory hold|"
        r"retain audit|audit logs?.{0,60}(?:retain|keep|preserve|exception)|"
        r"do not delete.{0,60}(?:audit|legal hold))\b",
        re.I,
    ),
    "customer_confirmation": re.compile(
        r"\b(?:customer confirmation|confirmation (?:email|notice|receipt)|confirm(?:ation)? when complete|"
        r"notify (?:the )?(?:customer|user|admin)|customer notice|deletion receipt)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class SourceAccountDeletionRequirement:
    """One source-backed account deletion or right-to-erasure requirement."""

    source_brief_id: str | None
    requirement_type: AccountDeletionRequirementType
    data_scope: str | None = None
    trigger: str | None = None
    window: str | None = None
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: AccountDeletionConfidence = "medium"
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "data_scope": self.data_scope,
            "trigger": self.trigger,
            "window": self.window,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourceAccountDeletionRequirementsReport:
    """Source-level account deletion and right-to-erasure requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceAccountDeletionRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAccountDeletionRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAccountDeletionRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
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
        """Return account deletion requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Account Deletion Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("requirement_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Requirement type counts: "
            + ", ".join(
                f"{requirement_type} {type_counts.get(requirement_type, 0)}"
                for requirement_type in _REQUIREMENT_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source account deletion requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Type | Scope | Trigger | Window | Confidence | Source Field | Evidence | Plan Impacts |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell(requirement.data_scope or '')} | "
                f"{_markdown_cell(requirement.trigger or '')} | "
                f"{_markdown_cell(requirement.window or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(', '.join(requirement.suggested_plan_impacts))} |"
            )
        return "\n".join(lines)


def build_source_account_deletion_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAccountDeletionRequirementsReport:
    """Extract source-level account deletion requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceAccountDeletionRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_account_deletion_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAccountDeletionRequirementsReport:
    """Compatibility alias for building an account deletion requirements report."""
    return build_source_account_deletion_requirements(source)


def generate_source_account_deletion_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAccountDeletionRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_account_deletion_requirements(source)


def derive_source_account_deletion_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAccountDeletionRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_account_deletion_requirements(source)


def summarize_source_account_deletion_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAccountDeletionRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted account deletion requirements."""
    if isinstance(source_or_result, SourceAccountDeletionRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_account_deletion_requirements(source_or_result).summary


def source_account_deletion_requirements_to_dict(
    report: SourceAccountDeletionRequirementsReport,
) -> dict[str, Any]:
    """Serialize an account deletion requirements report to a plain dictionary."""
    return report.to_dict()


source_account_deletion_requirements_to_dict.__test__ = False


def source_account_deletion_requirements_to_dicts(
    requirements: (
        tuple[SourceAccountDeletionRequirement, ...]
        | list[SourceAccountDeletionRequirement]
        | SourceAccountDeletionRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize account deletion requirement records to dictionaries."""
    if isinstance(requirements, SourceAccountDeletionRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_account_deletion_requirements_to_dicts.__test__ = False


def source_account_deletion_requirements_to_markdown(
    report: SourceAccountDeletionRequirementsReport,
) -> str:
    """Render an account deletion requirements report as Markdown."""
    return report.to_markdown()


source_account_deletion_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_type: AccountDeletionRequirementType
    data_scope: str | None
    trigger: str | None
    window: str | None
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: AccountDeletionConfidence


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(
        source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)
    ) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), payload
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


def _requirement_candidates(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for source_field, segment, field_context in _candidate_segments(payload):
            if not _is_deletion_requirement_signal(source_field, segment, field_context):
                continue
            searchable = f"{_field_words(source_field)} {segment}"
            requirement_types = _requirement_types(searchable)
            if not requirement_types:
                continue
            for requirement_type in requirement_types:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        requirement_type=requirement_type,
                        data_scope=_data_scope(segment, requirement_type),
                        trigger=_trigger(segment, requirement_type),
                        window=_window(segment, requirement_type),
                        source_field=source_field,
                        evidence=_evidence_snippet(source_field, segment),
                        matched_terms=_matched_terms(requirement_type, searchable),
                        confidence=_confidence(source_field, segment, field_context),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAccountDeletionRequirement]:
    grouped: dict[tuple[str | None, AccountDeletionRequirementType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.requirement_type), []).append(
            candidate
        )

    requirements: list[SourceAccountDeletionRequirement] = []
    for (source_brief_id, requirement_type), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda item: _CONFIDENCE_ORDER[item]
        )
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda item: item.casefold(),
        )[0]
        requirements.append(
            SourceAccountDeletionRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                data_scope=_joined_details(item.data_scope for item in items),
                trigger=_joined_details(item.trigger for item in items),
                window=_joined_details(item.window for item in items),
                source_field=source_field,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                matched_terms=tuple(
                    _dedupe(
                        term
                        for item in items
                        for term in item.matched_terms
                        if term
                    )
                ),
                confidence=confidence,
                suggested_plan_impacts=_PLANNING_IMPACTS[requirement_type],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _REQUIREMENT_ORDER.index(requirement.requirement_type),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str, bool]]:
    values: list[tuple[str, str, bool]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key], False)
    return [(field, segment, context) for field, segment, context in values if segment]


def _append_value(
    values: list[tuple[str, str, bool]],
    source_field: str,
    value: Any,
    inherited_context: bool,
) -> None:
    field_words = _field_words(source_field)
    field_context = inherited_context or bool(_STRUCTURED_FIELD_RE.search(field_words))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _SIGNAL_RE.search(key_text)
            )
            if _SIGNAL_RE.search(key_text):
                values.append((child_field, key_text, child_context))
            _append_value(values, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            values.append((source_field, segment, field_context))


def _segments(value: str) -> tuple[str, ...]:
    segments: list[str] = []
    for sentence in _SENTENCE_SPLIT_RE.split(value):
        segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return tuple(_clean_text(part) for part in segments if _clean_text(part))


def _is_deletion_requirement_signal(source_field: str, text: str, field_context: bool) -> bool:
    if _NEGATED_SCOPE_RE.search(text):
        return False
    if _SIGNAL_RE.search(text):
        return True
    if field_context and _REQUIREMENT_LANGUAGE_RE.search(text) and re.search(
        r"\b(?:delete|deletion|erase|erasure|purge|restore|confirmation|processor)\b",
        text,
        re.I,
    ):
        return True
    if source_field == "title" and not _REQUIREMENT_LANGUAGE_RE.search(text):
        return False
    return False


def _requirement_types(text: str) -> tuple[AccountDeletionRequirementType, ...]:
    types = [
        requirement_type
        for requirement_type in _REQUIREMENT_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    ]
    if (
        "restoration_window" not in types
        and "soft_delete" in types
        and _WINDOW_RE.search(text)
    ):
        types.append("restoration_window")
    return tuple(types)


def _data_scope(text: str, requirement_type: AccountDeletionRequirementType) -> str | None:
    if requirement_type == "workspace_deletion":
        match = re.search(
            r"\b(?:erase|delete|purge)\s+(?P<scope>[A-Za-z0-9][A-Za-z0-9 &/_.+-]{1,80})\s+after\b",
            text,
            re.I,
        )
        if match:
            return _clean_scope(match.group("scope"))
    matches = [
        _clean_scope(match.group(0))
        for match in _SCOPE_RE.finditer(text)
        if _clean_scope(match.group(0)) not in {"data", "records", "logs"}
    ]
    return _dedupe(matches)[0] if matches else None


def _trigger(text: str, requirement_type: AccountDeletionRequirementType) -> str | None:
    if match := _TRIGGER_RE.search(text):
        return _clean_text(match.group(0))
    if requirement_type == "cancellation_triggered_deletion":
        if match := re.search(
            r"\b(?:after|upon|on|following)\s+(?:subscription|account|plan|contract) cancellation\b",
            text,
            re.I,
        ):
            return _clean_text(match.group(0))
        if match := re.search(r"\b(?:subscription|account|plan|contract) cancellation\b", text, re.I):
            return _clean_text(match.group(0))
    return None


def _window(text: str, requirement_type: AccountDeletionRequirementType) -> str | None:
    if requirement_type not in {"soft_delete", "hard_delete", "restoration_window"} and not re.search(
        r"\b(?:grace|restore|recovery|window|within|after|before)\b", text, re.I
    ):
        return None
    if match := _WINDOW_RE.search(text):
        return _clean_text(match.group(0))
    return None


def _matched_terms(
    requirement_type: AccountDeletionRequirementType, text: str
) -> tuple[str, ...]:
    terms: list[str] = []
    for match in _TYPE_PATTERNS[requirement_type].finditer(text):
        if match.group(0):
            terms.append(_clean_text(match.group(0)).casefold())
    return tuple(_dedupe(terms))


def _confidence(
    source_field: str,
    text: str,
    field_context: bool,
) -> AccountDeletionConfidence:
    normalized_field = _field_words(source_field).casefold()
    strong_field = any(
        marker in normalized_field
        for marker in ("acceptance", "definition of done", "requirement", "privacy", "compliance")
    )
    if _REQUIREMENT_LANGUAGE_RE.search(text) and (
        strong_field or field_context or _WINDOW_RE.search(text) or _TRIGGER_RE.search(text)
    ):
        return "high"
    if _REQUIREMENT_LANGUAGE_RE.search(text) or field_context or _WINDOW_RE.search(text):
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceAccountDeletionRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    plan_impacts = sorted(
        {
            impact
            for requirement in requirements
            for impact in requirement.suggested_plan_impacts
            if impact
        },
        key=str.casefold,
    )
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "requirement_type_counts": {
            requirement_type: sum(
                1
                for requirement in requirements
                if requirement.requirement_type == requirement_type
            )
            for requirement_type in _REQUIREMENT_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "plan_impacts": plan_impacts,
        "status": "ready_for_planning" if requirements else "no_account_deletion_language",
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
        "mvp_goal",
        "context",
        "workflow_context",
        "requirements",
        "constraints",
        "scope",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "privacy",
        "compliance",
        "retention",
        "risks",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _BULLET_RE.sub("", text.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _clean_scope(text: str) -> str:
    return _SPACE_RE.sub(" ", text.strip(" .,:;")).casefold()


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 200:
        value = f"{value[:197].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _joined_details(values: Iterable[str | None]) -> str | None:
    details = _dedupe(value for value in values if value)
    return ", ".join(details) if details else None


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
    return sorted(deduped, key=lambda item: item.casefold())


__all__ = [
    "AccountDeletionConfidence",
    "AccountDeletionRequirementType",
    "SourceAccountDeletionRequirement",
    "SourceAccountDeletionRequirementsReport",
    "build_source_account_deletion_requirements",
    "derive_source_account_deletion_requirements",
    "extract_source_account_deletion_requirements",
    "generate_source_account_deletion_requirements",
    "summarize_source_account_deletion_requirements",
    "source_account_deletion_requirements_to_dict",
    "source_account_deletion_requirements_to_dicts",
    "source_account_deletion_requirements_to_markdown",
]
