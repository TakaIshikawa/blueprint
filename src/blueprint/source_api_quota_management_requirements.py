"""Extract API quota management requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


QuotaManagementRequirementType = Literal[
    "quota_types",
    "time_windows",
    "enforcement_strategy",
    "usage_tracking",
    "quota_increase",
    "overage_handling",
    "quota_reset",
    "quota_monitoring",
]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[QuotaManagementRequirementType, ...] = (
    "quota_types",
    "time_windows",
    "enforcement_strategy",
    "usage_tracking",
    "quota_increase",
    "overage_handling",
    "quota_reset",
    "quota_monitoring",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "integration_points",
    "integrations",
    "constraints",
    "metadata",
)

_TYPE_PATTERNS: dict[QuotaManagementRequirementType, re.Pattern[str]] = {
    "quota_types": re.compile(
        r"\b(?:per[- ](?:user|org(?:anization)?|api[- ]?key|account|tenant|application|ip|project)|"
        r"(?:user|org(?:anization)?|account|tenant|application|ip|project)[- ]level (?:quota|limit)|"
        r"global (?:quota|limit)|quota (?:type|scope)|shared (?:quota|limit))\b",
        re.I,
    ),
    "time_windows": re.compile(
        r"\b(?:per[- ](?:second|minute|hour|day|week|month|year)|"
        r"(?:second|minute|hour|day|week|month|year)ly (?:quota|limit|rate)|"
        r"time window|quota window|rolling window|fixed window|"
        r"billing period|rate period)\b",
        re.I,
    ),
    "enforcement_strategy": re.compile(
        r"\b(?:hard (?:limit|enforcement)|soft (?:limit|enforcement)|throttl(?:e|ing)|"
        r"enforce(?:ment)?|quota enforce(?:ment)?|limit enforce(?:ment)?|"
        r"block(?:ing)?|reject(?:ing)?|rejection|quota violation|exceed(?:ed)? (?:quota|limit))\b",
        re.I,
    ),
    "usage_tracking": re.compile(
        r"\b(?:usage track(?:ing)?|track (?:usage|metered)|meter(?:ing)?|metered usage|usage meter(?:ing)?|"
        r"usage count|consumption track(?:ing)?|monitor usage|"
        r"quota consumption|usage data|usage metric|usage report(?:ing)?)\b",
        re.I,
    ),
    "quota_increase": re.compile(
        r"\b(?:quota increase|increase quota|raise quota|request (?:higher|more) quota|"
        r"quota (?:upgrade|expansion|raise)|limit increase|increase limit|"
        r"quota request|request quota)\b",
        re.I,
    ),
    "overage_handling": re.compile(
        r"\b(?:overage|over quota|exceed(?:ed)? quota|quota exceed(?:ed)?|"
        r"billing|charge|overage (?:charge|fee)|pay[- ]as[- ]you[- ]go|"
        r"overage handling|burst quota|temporary exceed)\b",
        re.I,
    ),
    "quota_reset": re.compile(
        r"\b(?:quota reset|reset quota(?:s)?|quota renewal|renew quota|"
        r"quota cycle|quota period|reset (?:period|cycle)|"
        r"quota refresh|refresh quota|quota rollover|monthly reset|daily reset)\b",
        re.I,
    ),
    "quota_monitoring": re.compile(
        r"\b(?:quota monitor(?:ing)?|monitor quota|quota (?:alert|warning|notification)|"
        r"usage alert|quota threshold|approaching (?:quota|limit)|"
        r"quota dashboard|quota visibility|quota status)\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[QuotaManagementRequirementType, tuple[str, ...]] = {
    "quota_types": (
        "What quota scopes are required (per-user, per-org, per-API key, global)?",
        "Should quotas be shared across resources or isolated?",
    ),
    "time_windows": (
        "What time windows should quotas use (per-second, per-minute, per-day, per-month)?",
        "Should time windows be fixed or rolling?",
    ),
    "enforcement_strategy": (
        "Should limits be hard (blocking) or soft (warnings)?",
        "How should quota violations be handled and communicated?",
    ),
    "usage_tracking": (
        "How should usage be tracked and metered?",
        "What granularity of usage tracking is required?",
    ),
    "quota_increase": (
        "What is the workflow for requesting quota increases?",
        "Who can approve quota increase requests?",
    ),
    "overage_handling": (
        "How should usage beyond quota be handled?",
        "Is overage billing or temporary bursting supported?",
    ),
    "quota_reset": (
        "How often do quotas reset (daily, monthly, billing period)?",
        "Should unused quota carry over to the next period?",
    ),
    "quota_monitoring": (
        "What monitoring and alerting should be provided for quotas?",
        "At what thresholds should alerts be triggered?",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceApiQuotaManagementRequirement:
    """One source-backed API quota management requirement."""

    requirement_type: QuotaManagementRequirementType
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "follow_up_questions": list(self.follow_up_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceApiQuotaManagementRequirementsReport:
    """Source-level API quota management requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceApiQuotaManagementRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiQuotaManagementRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
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
        """Return API quota management requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Quota Management Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("type_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Source count: {self.summary.get('source_count', 1)}",
            f"- Policy coverage: {self.summary.get('policy_coverage', 0)}%",
            f"- Observability coverage: {self.summary.get('observability_coverage', 0)}%",
            f"- Flexibility coverage: {self.summary.get('flexibility_coverage', 0)}%",
            "- Requirement type counts: "
            + ", ".join(
                f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source API quota management requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Source Field Paths | Evidence | Follow-up Questions |",
                "| --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(requirement.follow_up_questions) or 'none')} |"
            )
        return "\n".join(lines)


def build_source_api_quota_management_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceApiQuotaManagementRequirementsReport:
    """Extract API quota management requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    grouped = _group_requirements(payload)
    requirements = _merge_requirements(grouped, source_brief_id)
    return SourceApiQuotaManagementRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_api_quota_management_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceApiQuotaManagementRequirement, ...]:
    """Return API quota management requirement records extracted from brief-shaped input."""
    return build_source_api_quota_management_requirements(source).requirements


def summarize_source_api_quota_management_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | SourceApiQuotaManagementRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic API quota management requirements summary."""
    if isinstance(source_or_result, SourceApiQuotaManagementRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_api_quota_management_requirements(source_or_result).summary


def source_api_quota_management_requirements_to_dict(
    report: SourceApiQuotaManagementRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API quota management requirements report to a plain dictionary."""
    return report.to_dict()


source_api_quota_management_requirements_to_dict.__test__ = False


def source_api_quota_management_requirements_to_dicts(
    requirements: (
        tuple[SourceApiQuotaManagementRequirement, ...]
        | list[SourceApiQuotaManagementRequirement]
        | SourceApiQuotaManagementRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source API quota management requirement records to dictionaries."""
    if isinstance(requirements, SourceApiQuotaManagementRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_quota_management_requirements_to_dicts.__test__ = False


def source_api_quota_management_requirements_to_markdown(
    report: SourceApiQuotaManagementRequirementsReport,
) -> str:
    """Render an API quota management requirements report as Markdown."""
    return report.to_markdown()


source_api_quota_management_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: QuotaManagementRequirementType
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_brief_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _source_brief_id(payload), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
            return _source_brief_id(payload), payload
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
            return _source_brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_brief_id(payload), payload
    return None, {}


def _source_brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _group_requirements(payload: Mapping[str, Any]) -> dict[QuotaManagementRequirementType, list[_Candidate]]:
    grouped: dict[QuotaManagementRequirementType, list[_Candidate]] = {}
    for source_field, text in _candidate_texts(payload):
        for segment in _segments(text):
            for requirement_type in _matched_requirement_types(segment):
                candidate = _Candidate(
                    requirement_type=requirement_type,
                    evidence=_evidence_snippet(source_field, segment),
                    source_field_path=source_field,
                    matched_terms=_matched_terms(requirement_type, segment),
                )
                grouped.setdefault(requirement_type, []).append(candidate)
    return grouped


def _merge_requirements(
    grouped: dict[QuotaManagementRequirementType, list[_Candidate]],
    source_brief_id: str | None,
) -> tuple[SourceApiQuotaManagementRequirement, ...]:
    requirements: list[SourceApiQuotaManagementRequirement] = []
    for requirement_type in _TYPE_ORDER:
        candidates = grouped.get(requirement_type, [])
        if not candidates:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in candidates))[:5]
        source_field_paths = tuple(
            sorted(_dedupe(item.source_field_path for item in candidates), key=str.casefold)
        )
        matched_terms = tuple(
            sorted(
                _dedupe(term for item in candidates for term in item.matched_terms),
                key=str.casefold,
            )
        )
        questions = _follow_up_questions(requirement_type, " ".join(evidence))
        requirements.append(
            SourceApiQuotaManagementRequirement(
                requirement_type=requirement_type,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                follow_up_questions=questions,
            )
        )
    return tuple(requirements)


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in _SCANNED_FIELDS:
        value = payload.get(field_name)
        if field_name == "metadata":
            texts.extend(_nested_texts(value, field_name))
            continue
        for index, text in enumerate(_strings(value)):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))

    if isinstance(payload.get("source_payload"), Mapping):
        for field_name in _SCANNED_FIELDS:
            if field_name in payload["source_payload"]:
                texts.extend(_nested_texts(payload["source_payload"][field_name], f"source_payload.{field_name}"))
    return texts


def _nested_texts(value: Any, prefix: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


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


def _segments(text: str) -> list[str]:
    segments: list[str] = []
    for raw_segment in _SENTENCE_SPLIT_RE.split(text):
        segment = _clean_text(raw_segment)
        if segment:
            segments.append(segment)
    return segments


def _matched_requirement_types(text: str) -> tuple[QuotaManagementRequirementType, ...]:
    return tuple(
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    )


def _matched_terms(
    requirement_type: QuotaManagementRequirementType,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)) for match in _TYPE_PATTERNS[requirement_type].finditer(text)
        )
    )


def _follow_up_questions(
    requirement_type: QuotaManagementRequirementType,
    evidence_text: str,
) -> tuple[str, ...]:
    questions = list(_BASE_QUESTIONS[requirement_type])
    # Reduce questions if evidence already provides specific answers
    if requirement_type == "quota_types" and re.search(
        r"\b(?:per[- ](?:user|org|api[- ]?key|account)|global)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Scope mentioned
    if requirement_type == "time_windows" and re.search(
        r"\b(?:per[- ](?:second|minute|hour|day|month)|fixed|rolling)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Window type mentioned
    if requirement_type == "enforcement_strategy" and re.search(
        r"\b(?:hard|soft|block|throttle|reject)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Strategy mentioned
    return tuple(_dedupe(questions))


def _summary(requirements: tuple[SourceApiQuotaManagementRequirement, ...]) -> dict[str, Any]:
    # Calculate coverage metrics
    policy = {"quota_types", "time_windows", "enforcement_strategy"}
    observability = {"usage_tracking", "quota_monitoring"}
    flexibility = {"quota_increase", "overage_handling", "quota_reset"}

    req_types = {req.requirement_type for req in requirements}
    policy_coverage = int(100 * len(req_types & policy) / len(policy)) if policy else 0
    observability_coverage = int(100 * len(req_types & observability) / len(observability)) if observability else 0
    flexibility_coverage = int(100 * len(req_types & flexibility) / len(flexibility)) if flexibility else 0

    return {
        "requirement_count": len(requirements),
        "source_count": 1,
        "type_counts": {
            req_type: sum(1 for req in requirements if req.requirement_type == req_type)
            for req_type in _TYPE_ORDER
        },
        "requirement_types": [req.requirement_type for req in requirements],
        "follow_up_question_count": sum(
            len(req.follow_up_questions) for req in requirements
        ),
        "policy_coverage": policy_coverage,
        "observability_coverage": observability_coverage,
        "flexibility_coverage": flexibility_coverage,
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
        "requirements",
        "acceptance_criteria",
        "acceptance",
        "constraints",
        "integration_points",
        "integrations",
        "metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _BULLET_RE.sub("", text.strip())
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
    "QuotaManagementRequirementType",
    "SourceApiQuotaManagementRequirement",
    "SourceApiQuotaManagementRequirementsReport",
    "build_source_api_quota_management_requirements",
    "extract_source_api_quota_management_requirements",
    "source_api_quota_management_requirements_to_dict",
    "source_api_quota_management_requirements_to_dicts",
    "source_api_quota_management_requirements_to_markdown",
    "summarize_source_api_quota_management_requirements",
]
