"""Extract integration testing requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


IntegrationTestingRequirementType = Literal[
    "integration_points",
    "test_scenarios",
    "test_data_requirements",
    "environment_needs",
    "external_service_mocking",
    "test_isolation",
    "test_data_management",
    "flaky_tests",
    "environment_parity",
    "ci_cd_integration",
]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[IntegrationTestingRequirementType, ...] = (
    "integration_points",
    "test_scenarios",
    "test_data_requirements",
    "environment_needs",
    "external_service_mocking",
    "test_isolation",
    "test_data_management",
    "flaky_tests",
    "environment_parity",
    "ci_cd_integration",
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

_TYPE_PATTERNS: dict[IntegrationTestingRequirementType, re.Pattern[str]] = {
    "integration_points": re.compile(
        r"\b(?:integration point(?:s)?|api integration(?:s)?|service integration(?:s)?|"
        r"third[- ]?party integration(?:s)?|external api(?:s)?|webhook(?:s)?|"
        r"service (?:endpoint|boundary|interface)(?:s)?|inter[- ]?service|microservice(?:s)?|"
        r"rest api|graphql|grpc|messaging|message (?:queue|broker)|kafka|rabbitmq|redis)\b",
        re.I,
    ),
    "test_scenarios": re.compile(
        r"\b(?:test scenario(?:s)?|integration test(?:s)?|end[- ]?to[- ]?end test(?:s)?|e2e test(?:s)?|"
        r"test case(?:s)?|test plan(?:s)?|happy path|edge case(?:s)?|error case(?:s)?|"
        r"failure scenario(?:s)?|retry (?:logic|behavior)|timeout scenario(?:s)?|"
        r"contract test(?:s)?|consumer[- ]?driven contract(?:s)?|pact)\b",
        re.I,
    ),
    "test_data_requirements": re.compile(
        r"\b(?:test data|test fixture(?:s)?|mock data|seed data|sample data|"
        r"test database|test dataset(?:s)?|fixture(?:s)?|factory|factories|faker|"
        r"data (?:setup|teardown|cleanup)|test payload(?:s)?|test input(?:s)?)\b",
        re.I,
    ),
    "environment_needs": re.compile(
        r"\b(?:test environment(?:s)?|staging environment|qa environment|"
        r"docker[- ]?compose|testcontainer(?:s)?|local (?:environment|setup)|"
        r"environment (?:variable|config|setup)(?:s)?|infrastructure|"
        r"test (?:infrastructure|stack)|wiremock|localstack|mock (?:server|service)(?:s)?)\b",
        re.I,
    ),
    "external_service_mocking": re.compile(
        r"\b(?:mock(?:ing)?|stub(?:s|bing)?|fake|test double(?:s)?|"
        r"service (?:mock|stub|fake)(?:s)?|http mock(?:s)?|api mock(?:s)?|"
        r"mock (?:server|service|api|endpoint)(?:s)?|nock|sinon|msw|"
        r"mock service worker|wiremock|vcr|http (?:fixture|recording)(?:s)?)\b",
        re.I,
    ),
    "test_isolation": re.compile(
        r"\b(?:test isolation|isolat(?:ed|ion)|independent test(?:s)?|"
        r"test (?:independence|dependency|coupling)|parallel test(?:s)?|"
        r"test (?:interference|conflict)(?:s)?|database (?:rollback|transaction)(?:s)?|"
        r"cleanup|teardown|reset|fresh (?:state|environment)|test container(?:s)?)\b",
        re.I,
    ),
    "test_data_management": re.compile(
        r"\b(?:test data management|data (?:lifecycle|versioning|migration)(?:s)?|"
        r"schema (?:migration|evolution)(?:s)?|database migration(?:s)?|"
        r"test data (?:generation|maintenance|synchronization)|anonymiz(?:ed|ation)|"
        r"synthetic data|data masking|test data (?:refresh|update|sync))\b",
        re.I,
    ),
    "flaky_tests": re.compile(
        r"\b(?:flak(?:y|iness)|flake|non[- ]?deterministic|intermittent (?:failure|test)(?:s)?|"
        r"race condition(?:s)?|timing issue(?:s)?|retry (?:logic|mechanism)|"
        r"test (?:stability|reliability)|unstable test(?:s)?|eventual consistency)\b",
        re.I,
    ),
    "environment_parity": re.compile(
        r"\b(?:environment parity|production[- ]?like|prod[- ]?like|"
        r"dev[- ]?prod parity|staging[- ]?production parity|"
        r"environment (?:consistency|alignment)|configuration drift|"
        r"infrastructure (?:as code|parity)|iac|terraform|environment (?:replica|mirror))\b",
        re.I,
    ),
    "ci_cd_integration": re.compile(
        r"\b(?:ci[/]?cd|continuous integration|continuous (?:deployment|delivery)|"
        r"pipeline(?:s)?|github action(?:s)?|gitlab ci|jenkins|circleci|travis|"
        r"build pipeline(?:s)?|deployment pipeline(?:s)?|automated test(?:s)?|"
        r"test automation|test runner|jest|pytest|mocha|junit)\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[IntegrationTestingRequirementType, tuple[str, ...]] = {
    "integration_points": (
        "What are the specific integration points that need testing?",
        "Which external services and APIs require integration testing?",
    ),
    "test_scenarios": (
        "What are the critical integration test scenarios?",
        "Which edge cases and error scenarios should be covered?",
    ),
    "test_data_requirements": (
        "What test data and fixtures are required?",
        "How should test data be generated and maintained?",
    ),
    "environment_needs": (
        "What test environments and infrastructure are needed?",
        "Which services need to be available in the test environment?",
    ),
    "external_service_mocking": (
        "Which external services should be mocked?",
        "What mocking strategy and tools should be used?",
    ),
    "test_isolation": (
        "How should tests be isolated from each other?",
        "What cleanup and teardown strategies are needed?",
    ),
    "test_data_management": (
        "How should test data be managed across environments?",
        "What are the data migration and versioning requirements?",
    ),
    "flaky_tests": (
        "What strategies will prevent flaky tests?",
        "How should timing issues and race conditions be handled?",
    ),
    "environment_parity": (
        "How can test environments match production?",
        "What infrastructure-as-code is needed for environment parity?",
    ),
    "ci_cd_integration": (
        "How should integration tests run in CI/CD?",
        "What are the pipeline and automation requirements?",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceIntegrationTestingRequirement:
    """One source-backed integration testing requirement."""

    requirement_type: IntegrationTestingRequirementType
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
class SourceIntegrationTestingRequirementsReport:
    """Source-level integration testing requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceIntegrationTestingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceIntegrationTestingRequirement, ...]:
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
        """Return integration testing requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]


def extract_source_integration_testing_requirements(source: Any) -> SourceIntegrationTestingRequirementsReport:
    """Extract integration testing requirements from source brief."""
    brief_id, payload = _source_payload(source)
    scanned = _scanned_texts(payload)
    requirements: list[SourceIntegrationTestingRequirement] = []

    for requirement_type in _TYPE_ORDER:
        pattern = _TYPE_PATTERNS[requirement_type]
        evidence_list: list[str] = []
        field_paths: list[str] = []
        matched_terms: set[str] = set()

        for field_path, text in scanned:
            if pattern.search(text):
                evidence_list.append(_snippet(text))
                field_paths.append(field_path)
                for match in pattern.finditer(text):
                    matched_terms.add(match.group().strip())

        if evidence_list:
            requirements.append(
                SourceIntegrationTestingRequirement(
                    requirement_type=requirement_type,
                    evidence=tuple(_dedupe(evidence_list)),
                    source_field_paths=tuple(_dedupe(field_paths)),
                    matched_terms=tuple(sorted(matched_terms, key=str.casefold)),
                    follow_up_questions=_BASE_QUESTIONS[requirement_type],
                )
            )

    return SourceIntegrationTestingRequirementsReport(
        source_brief_id=brief_id,
        requirements=tuple(requirements),
        summary=_summary(requirements),
    )


def _source_payload(source: Any) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, SourceBrief):
        return source.id, source.model_dump(mode="python")
    if isinstance(source, Mapping):
        return source.get("id"), dict(source)
    if hasattr(source, "model_dump"):
        payload = source.model_dump(mode="python")
        return payload.get("id"), payload
    if hasattr(source, "id"):
        payload = {field: getattr(source, field) for field in _SCANNED_FIELDS if hasattr(source, field)}
        return getattr(source, "id", None), payload
    return None, {}


def _scanned_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field in _SCANNED_FIELDS:
        value = payload.get(field)
        if value:
            for field_path, text in _field_texts(field, value):
                texts.append((field_path, text))
    return texts


def _field_texts(field: str, value: Any, prefix: str = "") -> list[tuple[str, str]]:
    field_path = f"{prefix}.{field}" if prefix else field
    if isinstance(value, str):
        return [(field_path, value)]
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key, val in value.items():
            texts.extend(_field_texts(str(key), val, field_path))
        return texts
    if isinstance(value, (list, tuple)):
        texts = []
        for index, item in enumerate(value):
            texts.extend(_field_texts(f"[{index}]", item, field_path))
        return texts
    if value is not None:
        return [(field_path, str(value))]
    return []


def _snippet(text: str, max_length: int = 180) -> str:
    cleaned = _SPACE_RE.sub(" ", str(text)).strip()
    if len(cleaned) <= max_length:
        return cleaned
    return f"{cleaned[:max_length - 3].rstrip()}..."


def _summary(requirements: list[SourceIntegrationTestingRequirement]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "type_counts": {
            req_type: sum(1 for req in requirements if req.requirement_type == req_type)
            for req_type in _TYPE_ORDER
        },
    }


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
