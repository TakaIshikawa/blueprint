"""Extract API client contract requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


ClientContractRequirementType = Literal[
    "generated_client",
    "sdk_compatibility",
    "openapi_schema",
    "consumer_contract_tests",
    "webhook_callbacks",
    "retry_guidance",
    "deprecation_window",
    "sample_requests",
]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[ClientContractRequirementType, ...] = (
    "openapi_schema",
    "generated_client",
    "sdk_compatibility",
    "consumer_contract_tests",
    "sample_requests",
    "retry_guidance",
    "webhook_callbacks",
    "deprecation_window",
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

_TYPE_PATTERNS: dict[ClientContractRequirementType, re.Pattern[str]] = {
    "generated_client": re.compile(
        r"\b(?:generated? clients?|client generation|auto[- ]generated clients?|"
        r"generate clients?|client codegen|code generation)\b|"
        r"\bgenerate\s+(?:\w+\s+(?:and|or)\s+)*(?:python|java|node|go|ruby|typescript|\.net)\s+clients?\b",
        re.I,
    ),
    "sdk_compatibility": re.compile(
        r"\b(?:sdk|sdks|client library|client libraries|language clients?|"
        r"sdk compatibility|client compatibility|java client|python client|"
        r"node client|go client|ruby client|\.net client|typescript client)\b",
        re.I,
    ),
    "openapi_schema": re.compile(
        r"\b(?:openapi|open api|swagger|api spec|api specification|"
        r"schema artifact|client schema|api schema|api definition|"
        r"openapi\.(?:yaml|json)|swagger\.(?:yaml|json))\b",
        re.I,
    ),
    "consumer_contract_tests": re.compile(
        r"\b(?:consumer contract|contract tests?|consumer tests?|"
        r"client contract tests?|pact|consumer[- ]driven|contract testing|"
        r"api contract tests?)\b",
        re.I,
    ),
    "webhook_callbacks": re.compile(
        r"\b(?:webhook|webhooks|callback|callbacks|callback url|"
        r"webhook endpoint|event delivery|webhook delivery)\b",
        re.I,
    ),
    "retry_guidance": re.compile(
        r"\b(?:retry|retries|retry logic|retry guidance|backoff|"
        r"exponential backoff|retry strategy|retry policy|"
        r"idempotent|idempotency|retry behavior)\b",
        re.I,
    ),
    "deprecation_window": re.compile(
        r"\b(?:deprecation|deprecated|sunset|deprecation window|"
        r"sunset window|deprecation timeline|migration window|"
        r"end[- ]of[- ]life|eol)\b",
        re.I,
    ),
    "sample_requests": re.compile(
        r"\b(?:sample requests?|example requests?|request examples?|"
        r"sample payloads?|example payloads?|curl examples?|"
        r"code examples?|usage examples?|sample code)\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[ClientContractRequirementType, tuple[str, ...]] = {
    "generated_client": (
        "Which languages or platforms require generated clients?",
        "What is the client generation process and artifact delivery mechanism?",
    ),
    "sdk_compatibility": (
        "Which SDK versions, languages, or client libraries must remain compatible?",
    ),
    "openapi_schema": (
        "Which OpenAPI version, validation rules, and schema publication process apply?",
    ),
    "consumer_contract_tests": (
        "Which consumer contract test framework, coverage expectations, and failure handling apply?",
    ),
    "webhook_callbacks": (
        "Which webhook events, signature verification, retry, and idempotency requirements apply?",
    ),
    "retry_guidance": (
        "Which retry policies, backoff strategies, and idempotency expectations must clients follow?",
    ),
    "deprecation_window": (
        "What deprecation notice period, sunset date, and migration support must be provided?",
    ),
    "sample_requests": (
        "Which API operations require sample requests, payloads, or code examples?",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceApiClientContractRequirement:
    """One source-backed API client contract requirement."""

    requirement_type: ClientContractRequirementType
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
class SourceApiClientContractRequirementsReport:
    """Source-level API client contract requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceApiClientContractRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiClientContractRequirement, ...]:
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
        """Return API client contract requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Client Contract Requirements Report"
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
            f"- Client artifact coverage: {self.summary.get('client_artifact_coverage', 0)}%",
            f"- Contract test coverage: {self.summary.get('contract_test_coverage', 0)}%",
            f"- Deprecation coverage: {self.summary.get('deprecation_coverage', 0)}%",
            "- Requirement type counts: "
            + ", ".join(
                f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source API client contract requirements were inferred."])
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


def build_source_api_client_contract_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceApiClientContractRequirementsReport:
    """Extract API client contract requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    grouped = _group_requirements(payload)
    requirements = _merge_requirements(grouped, source_brief_id)
    return SourceApiClientContractRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_api_client_contract_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceApiClientContractRequirement, ...]:
    """Return API client contract requirement records extracted from brief-shaped input."""
    return build_source_api_client_contract_requirements(source).requirements


def summarize_source_api_client_contract_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | SourceApiClientContractRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic API client contract requirements summary."""
    if isinstance(source_or_result, SourceApiClientContractRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_api_client_contract_requirements(source_or_result).summary


def source_api_client_contract_requirements_to_dict(
    report: SourceApiClientContractRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API client contract requirements report to a plain dictionary."""
    return report.to_dict()


source_api_client_contract_requirements_to_dict.__test__ = False


def source_api_client_contract_requirements_to_dicts(
    requirements: (
        tuple[SourceApiClientContractRequirement, ...]
        | list[SourceApiClientContractRequirement]
        | SourceApiClientContractRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source API client contract requirement records to dictionaries."""
    if isinstance(requirements, SourceApiClientContractRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_client_contract_requirements_to_dicts.__test__ = False


def source_api_client_contract_requirements_to_markdown(
    report: SourceApiClientContractRequirementsReport,
) -> str:
    """Render an API client contract requirements report as Markdown."""
    return report.to_markdown()


source_api_client_contract_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: ClientContractRequirementType
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


def _group_requirements(payload: Mapping[str, Any]) -> dict[ClientContractRequirementType, list[_Candidate]]:
    grouped: dict[ClientContractRequirementType, list[_Candidate]] = {}
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
    grouped: dict[ClientContractRequirementType, list[_Candidate]],
    source_brief_id: str | None,
) -> tuple[SourceApiClientContractRequirement, ...]:
    requirements: list[SourceApiClientContractRequirement] = []
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
            SourceApiClientContractRequirement(
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


def _matched_requirement_types(text: str) -> tuple[ClientContractRequirementType, ...]:
    return tuple(
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    )


def _matched_terms(
    requirement_type: ClientContractRequirementType,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)) for match in _TYPE_PATTERNS[requirement_type].finditer(text)
        )
    )


def _follow_up_questions(
    requirement_type: ClientContractRequirementType,
    evidence_text: str,
) -> tuple[str, ...]:
    questions = list(_BASE_QUESTIONS[requirement_type])
    # Reduce questions if evidence already provides specific answers
    if requirement_type == "generated_client" and re.search(
        r"\b(?:java|python|node|go|ruby|\.net|typescript)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Language already mentioned
    if requirement_type == "openapi_schema" and re.search(
        r"\bopenapi\s+[23]\b", evidence_text, re.I
    ):
        questions = []  # Version specified
    if requirement_type == "deprecation_window" and re.search(
        r"\b(?:\d+\s+(?:days?|weeks?|months?)|sunset date)\b", evidence_text, re.I
    ):
        questions = []  # Timeline provided
    return tuple(_dedupe(questions))


def _summary(requirements: tuple[SourceApiClientContractRequirement, ...]) -> dict[str, Any]:
    # Calculate coverage metrics
    client_artifacts = {"openapi_schema", "generated_client", "sdk_compatibility"}
    contract_tests = {"consumer_contract_tests"}
    deprecation = {"deprecation_window"}

    req_types = {req.requirement_type for req in requirements}
    client_artifact_coverage = int(100 * len(req_types & client_artifacts) / len(client_artifacts)) if client_artifacts else 0
    contract_test_coverage = int(100 * len(req_types & contract_tests) / len(contract_tests)) if contract_tests else 0
    deprecation_coverage = int(100 * len(req_types & deprecation) / len(deprecation)) if deprecation else 0

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
        "client_artifact_coverage": client_artifact_coverage,
        "contract_test_coverage": contract_test_coverage,
        "deprecation_coverage": deprecation_coverage,
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
    "ClientContractRequirementType",
    "SourceApiClientContractRequirement",
    "SourceApiClientContractRequirementsReport",
    "build_source_api_client_contract_requirements",
    "extract_source_api_client_contract_requirements",
    "source_api_client_contract_requirements_to_dict",
    "source_api_client_contract_requirements_to_dicts",
    "source_api_client_contract_requirements_to_markdown",
    "summarize_source_api_client_contract_requirements",
]
