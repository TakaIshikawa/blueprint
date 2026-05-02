"""Extract source-level permission scope requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourcePermissionScopeRequirementType = Literal[
    "role_requirement",
    "permission_scope",
    "rbac",
    "abac",
    "least_privilege",
    "admin_capability",
    "delegation",
]
SourcePermissionScopeConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[SourcePermissionScopeRequirementType, ...] = (
    "role_requirement",
    "permission_scope",
    "rbac",
    "abac",
    "least_privilege",
    "admin_capability",
    "delegation",
)
_CONFIDENCE_ORDER: dict[SourcePermissionScopeConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:but|or)\s+", re.I)
_DIRECTIVE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|support|"
    r"allow|deny|restrict|limited to|only|cannot ship|acceptance|done when|before launch)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:roles?|permissions?|scopes?|rbac|abac|access[_ -]?control|authorization|"
    r"admin|delegat(?:e|ion)|least[_ -]?privilege|capabilit(?:y|ies)|requirements?|"
    r"acceptance|constraints?|source[_ -]?payload|security)",
    re.I,
)
_PERMISSION_CONTEXT_RE = re.compile(
    r"\b(?:role|roles|permission|permissions|scope|scopes|access control|authorization|"
    r"authorize|rbac|abac|least privilege|least-privilege|admin|administrator|"
    r"delegation|delegate|delegated|capability|capabilities|grant|grants|privilege|"
    r"policy|policies|entitlement|entitlements)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,120}"
    r"\b(?:roles?|permissions?|scopes?|rbac|abac|access control|authorization|"
    r"least privilege|admin capabilities?|delegation|delegated access|grants?)\b.{0,120}"
    r"\b(?:required|needed|in scope|changes?|work|support|planned|for this release)\b|"
    r"\b(?:roles?|permissions?|scopes?|rbac|abac|access control|authorization|"
    r"least privilege|admin capabilities?|delegation|delegated access|grants?)\b.{0,120}"
    r"\b(?:out of scope|not required|not needed|no changes?|no work|non-goal|non goal)\b",
    re.I,
)
_TYPE_PATTERNS: dict[SourcePermissionScopeRequirementType, re.Pattern[str]] = {
    "role_requirement": re.compile(
        r"\b(?:roles?|role based|role-based|owner|admins?|administrators?|manager|member|"
        r"viewer|editor|operator|approver|requester|reviewer|auditor|support agent|"
        r"super admin|tenant admin|workspace admin)\b",
        re.I,
    ),
    "permission_scope": re.compile(
        r"\b(?:permissions?|scopes?|grants?|entitlements?|access rights?|capabilities|"
        r"read[- ]only|read/write|write access|view access|edit access|delete access|"
        r"export access|manage users?|manage billing|resource scopes?)\b",
        re.I,
    ),
    "rbac": re.compile(r"\b(?:rbac|role[- ]based access control|role based access)\b", re.I),
    "abac": re.compile(
        r"\b(?:abac|attribute[- ]based access control|attribute based access|"
        r"policy conditions?|tenant attribute|department attribute|region attribute|"
        r"resource attributes?|user attributes?)\b",
        re.I,
    ),
    "least_privilege": re.compile(
        r"\b(?:least privilege|least-privilege|minimal permissions?|minimum permissions?|"
        r"minimum scopes?|only grant|only request|limited to|deny by default|"
        r"need[- ]to[- ]know)\b",
        re.I,
    ),
    "admin_capability": re.compile(
        r"\b(?:admin(?:istrator)? capabilities?|admin(?:istrator)? permissions?|"
        r"admin(?:istrator)? can|admin(?:istrator)?s? may|delegated admin|"
        r"super admin|tenant admin|workspace admin|privileged capability|"
        r"privileged action|manage users?|manage roles?|manage permissions?|"
        r"impersonat(?:e|ion))\b",
        re.I,
    ),
    "delegation": re.compile(
        r"\b(?:delegat(?:e|es|ed|ing|ion)|delegate access|delegated admin|"
        r"delegated approval|temporary access|time[- ]bound access|on behalf of|"
        r"proxy access|grant access to|revoke delegated access)\b",
        re.I,
    ),
}
_SCOPE_VALUE_RE = re.compile(
    r"\b(?:[a-z][a-z0-9_.-]+[:.][a-z0-9_*.-]+|[a-z0-9_.:-]+\.read|"
    r"[a-z0-9_.:-]+\.write|read[-_:][a-z0-9_.:-]+|write[-_:][a-z0-9_.:-]+)\b",
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
    "security",
    "authorization",
    "metadata",
    "brief_metadata",
    "source_payload",
)


@dataclass(frozen=True, slots=True)
class SourcePermissionScopeRequirement:
    """One source-backed permission scope requirement."""

    source_brief_id: str | None
    requirement_type: SourcePermissionScopeRequirementType
    summary: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field: str | None = None
    source_path: str | None = None
    confidence: SourcePermissionScopeConfidence = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "summary": self.summary,
            "evidence": list(self.evidence),
            "source_field": self.source_field,
            "source_path": self.source_path,
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SourcePermissionScopeRequirementsReport:
    """Source-level permission scope requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourcePermissionScopeRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourcePermissionScopeRequirement, ...]:
        """Compatibility view matching extractors that expose records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return permission scope requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Permission Scope Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Source count: {self.summary.get('source_count', 0)}",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement type counts: "
            + ", ".join(
                f"{requirement_type} {type_counts.get(requirement_type, 0)}"
                for requirement_type in _TYPE_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{confidence} {confidence_counts.get(confidence, 0)}"
                for confidence in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source permission scope requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Summary | Source Field | Source Path | Confidence | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell(requirement.summary)} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(requirement.source_path or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_permission_scope_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePermissionScopeRequirementsReport:
    """Extract permission scope requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _TYPE_ORDER.index(requirement.requirement_type),
                requirement.source_path or "",
                requirement.summary.casefold(),
                _CONFIDENCE_ORDER[requirement.confidence],
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourcePermissionScopeRequirementsReport(
        source_brief_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_permission_scope_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePermissionScopeRequirementsReport:
    """Compatibility alias for building a permission scope requirements report."""
    return build_source_permission_scope_requirements(source)


def generate_source_permission_scope_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePermissionScopeRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_permission_scope_requirements(source)


def derive_source_permission_scope_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePermissionScopeRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_permission_scope_requirements(source)


def summarize_source_permission_scope_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourcePermissionScopeRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted permission scope requirements."""
    if isinstance(source_or_result, SourcePermissionScopeRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_permission_scope_requirements(source_or_result).summary


def source_permission_scope_requirements_to_dict(
    report: SourcePermissionScopeRequirementsReport,
) -> dict[str, Any]:
    """Serialize a permission scope requirements report to a plain dictionary."""
    return report.to_dict()


source_permission_scope_requirements_to_dict.__test__ = False


def source_permission_scope_requirements_to_dicts(
    requirements: (
        tuple[SourcePermissionScopeRequirement, ...]
        | list[SourcePermissionScopeRequirement]
        | SourcePermissionScopeRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize permission scope requirement records to dictionaries."""
    if isinstance(requirements, SourcePermissionScopeRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_permission_scope_requirements_to_dicts.__test__ = False


def source_permission_scope_requirements_to_markdown(
    report: SourcePermissionScopeRequirementsReport,
) -> str:
    """Render a permission scope requirements report as Markdown."""
    return report.to_markdown()


source_permission_scope_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_type: SourcePermissionScopeRequirementType
    summary: str
    evidence: str
    source_field: str
    source_path: str
    confidence: SourcePermissionScopeConfidence


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
    if isinstance(source, (bytes, bytearray)):
        return None, {}
    payload = _object_payload(source)
    return _source_id(payload), payload


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for source_field, source_path, text in _candidate_segments(payload):
            for requirement_type in _requirement_types(source_field, text):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        requirement_type=requirement_type,
                        summary=_summary_text(requirement_type, text),
                        evidence=_evidence_snippet(source_path, text),
                        source_field=source_field,
                        source_path=source_path,
                        confidence=_confidence(source_field, text),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourcePermissionScopeRequirement]:
    grouped: dict[tuple[str | None, SourcePermissionScopeRequirementType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_brief_id,
                candidate.requirement_type,
            ),
            [],
        ).append(candidate)

    requirements: list[SourcePermissionScopeRequirement] = []
    for (source_brief_id, requirement_type), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        source_paths = tuple(sorted(_dedupe(item.source_path for item in items), key=str.casefold))
        source_fields = tuple(
            sorted(_dedupe(item.source_field for item in items), key=str.casefold)
        )
        evidence = tuple(
            sorted(
                _dedupe_evidence(item.evidence for item in items),
                key=lambda item: item.casefold(),
            )
        )[:5]
        source_field = (
            source_fields[0] if len(source_fields) == 1 else "; ".join(source_fields)
        )
        source_path = source_paths[0] if len(source_paths) == 1 else "; ".join(source_paths)
        requirements.append(
            SourcePermissionScopeRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                summary=items[0].summary,
                evidence=evidence,
                source_field=source_field,
                source_path=source_path,
                confidence=confidence,
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    values: list[tuple[str, str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), str(key), payload[key])
    return [(field, path, segment) for field, path, segment in values if segment]


def _append_value(
    values: list[tuple[str, str, str]],
    source_field: str,
    source_path: str,
    value: Any,
) -> None:
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(source_path)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_path = f"{source_path}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text))
            if child_context and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    values.append((source_field, child_path, _clean_text(f"{key_text}: {text}")))
                continue
            _append_value(values, source_field, child_path, child)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, source_field, f"{source_path}[{index}]", item)
        return
    if text := _optional_text(value):
        values.extend((source_field, source_path, segment) for segment in _segments(text))


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
            clauses = [part] if _NEGATED_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _requirement_types(
    source_field: str, text: str
) -> tuple[SourcePermissionScopeRequirementType, ...]:
    if _NEGATED_RE.search(text):
        return ()
    searchable = _searchable_text(source_field, text)
    field_words = _field_words(source_field)
    if not (
        _PERMISSION_CONTEXT_RE.search(searchable)
        or _STRUCTURED_FIELD_RE.search(field_words)
        or _SCOPE_VALUE_RE.search(searchable)
    ):
        return ()
    types = [
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(searchable)
    ]
    if "permission_scope" in types and re.search(
        r"\b(?:project|release|task|feature|page)\s+scope\b", searchable, re.I
    ):
        types.remove("permission_scope")
    if types and not (
        _DIRECTIVE_RE.search(searchable)
        or _STRUCTURED_FIELD_RE.search(field_words)
        or _SCOPE_VALUE_RE.search(searchable)
    ):
        return ()
    return tuple(_dedupe(types))


def _summary_text(requirement_type: SourcePermissionScopeRequirementType, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 140:
        cleaned = f"{cleaned[:137].rstrip()}..."
    return f"{requirement_type.replace('_', ' ')}: {cleaned}"


def _confidence(source_field: str, text: str) -> SourcePermissionScopeConfidence:
    searchable = _searchable_text(source_field, text)
    has_directive = bool(_DIRECTIVE_RE.search(searchable))
    has_structured_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    has_detail = bool(
        _SCOPE_VALUE_RE.search(searchable)
        or re.search(
            r"\b(?:only|limited to|deny by default|tenant|workspace|department|region|"
            r"admin|viewer|editor|owner)\b",
            searchable,
            re.I,
        )
    )
    if has_directive and (has_detail or has_structured_context):
        return "high"
    if has_directive or has_detail or has_structured_context:
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourcePermissionScopeRequirement, ...], source_count: int
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "type_counts": {
            requirement_type: sum(
                1
                for requirement in requirements
                if requirement.requirement_type == requirement_type
            )
            for requirement_type in _TYPE_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "requirement_types": [
            requirement_type
            for requirement_type in _TYPE_ORDER
            if any(
                requirement.requirement_type == requirement_type
                for requirement in requirements
            )
        ],
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "target_user",
        "buyer",
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
        "product_surface",
        "requirements",
        "constraints",
        "scope",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
        "source_links",
        "acceptance_criteria",
        "implementation_notes",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "definition_of_done",
        "validation_plan",
        "security",
        "authorization",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _searchable_text(source_field: str, text: str) -> str:
    return f"{_field_words(source_field)} {text}".replace("_", " ").replace("-", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _evidence_snippet(source_path: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_path}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe_key(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", _clean_text(value).casefold()).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = _clean_text(str(value)).casefold()
        if key in seen:
            continue
        result.append(value)
        seen.add(key)
    return result


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            continue
        result.append(value)
        seen.add(key)
    return result


__all__ = [
    "SourcePermissionScopeConfidence",
    "SourcePermissionScopeRequirement",
    "SourcePermissionScopeRequirementType",
    "SourcePermissionScopeRequirementsReport",
    "build_source_permission_scope_requirements",
    "derive_source_permission_scope_requirements",
    "extract_source_permission_scope_requirements",
    "generate_source_permission_scope_requirements",
    "source_permission_scope_requirements_to_dict",
    "source_permission_scope_requirements_to_dicts",
    "source_permission_scope_requirements_to_markdown",
    "summarize_source_permission_scope_requirements",
]
