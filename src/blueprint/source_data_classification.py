"""Extract source brief data-classification signals."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from blueprint.domain.models import SourceBrief


DataClassification = Literal[
    "restricted",
    "credentials",
    "health",
    "minors",
    "financial",
    "pii",
    "confidential",
    "internal",
    "telemetry",
    "public",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CLASSIFICATION_ORDER: dict[DataClassification, int] = {
    "restricted": 0,
    "credentials": 1,
    "health": 2,
    "minors": 3,
    "financial": 4,
    "pii": 5,
    "confidential": 6,
    "internal": 7,
    "telemetry": 8,
    "public": 9,
}
_RESTRICTED_CATEGORIES: tuple[DataClassification, ...] = (
    "credentials",
    "health",
    "minors",
    "financial",
    "pii",
)
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "problem_statement",
    "context",
    "data_requirements",
    "risks",
    "constraints",
    "metadata",
)
_MAX_EVIDENCE_PER_CLASSIFICATION = 5

_PATTERNS: dict[DataClassification, tuple[re.Pattern[str], ...]] = {
    "public": (
        re.compile(r"\b(?:public data|publicly available|open data|marketing site|press release)\b", re.I),
    ),
    "internal": (
        re.compile(r"\b(?:internal(?: only)?|employee data|staff data|company confidential|ops notes)\b", re.I),
    ),
    "confidential": (
        re.compile(
            r"\b(?:confidential|proprietary|trade secret|sensitive customer|customer data|"
            r"contract terms|nda|non[- ]disclosure)\b",
            re.I,
        ),
    ),
    "restricted": (
        re.compile(r"\b(?:restricted data|highly sensitive|special category|regulated data)\b", re.I),
    ),
    "pii": (
        re.compile(
            r"\b(?:pii|personally identifiable|personal data|email addresses?|phone numbers?|"
            r"home addresses?|date of birth|dob|ssn|social security|passport|driver'?s license|"
            r"government id|ip addresses?|user identifiers?)\b",
            re.I,
        ),
    ),
    "credentials": (
        re.compile(
            r"\b(?:credentials?|passwords?|passcodes?|api keys?|access tokens?|refresh tokens?|"
            r"auth tokens?|oauth tokens?|private keys?|secret keys?|client secrets?|ssh keys?)\b",
            re.I,
        ),
    ),
    "financial": (
        re.compile(
            r"\b(?:financial data|payment data|billing data|credit cards?|card numbers?|pan\b|"
            r"bank accounts?|routing numbers?|iban|tax ids?|invoices?|payroll|transaction history)\b",
            re.I,
        ),
    ),
    "health": (
        re.compile(
            r"\b(?:health data|medical records?|patient data|phi\b|hipaa|diagnos(?:is|es)|"
            r"prescriptions?|lab results?|clinical)\b",
            re.I,
        ),
    ),
    "minors": (
        re.compile(
            r"\b(?:minors?|children|kids|students?|under\s*13|under\s*16|underage|coppa|"
            r"parental consent)\b",
            re.I,
        ),
    ),
    "telemetry": (
        re.compile(
            r"\b(?:telemetry|analytics events?|event logs?|clickstream|usage metrics?|"
            r"product metrics?|session replay|logs?)\b",
            re.I,
        ),
    ),
}

_RECOMMENDED_HANDLING: dict[DataClassification, str] = {
    "public": "Treat as publishable data; confirm no private source material is mixed into public outputs.",
    "internal": "Keep access limited to the organization and avoid copying into public channels.",
    "confidential": "Limit to need-to-know reviewers, encrypt at rest, and document retention expectations.",
    "restricted": "Route through privacy and security review before storage, sharing, or implementation planning.",
    "pii": "Minimize collection, define retention, encrypt storage, and review consent or lawful basis.",
    "credentials": "Do not store plaintext secrets; use a secrets manager, rotation, and redaction in logs.",
    "financial": "Apply payment or financial-data controls, retention limits, audit logging, and vendor review.",
    "health": "Apply health-data handling controls, access auditing, retention limits, and regulatory review.",
    "minors": "Review age-gating, parental consent, retention, and child privacy obligations before build work.",
    "telemetry": "Define event minimization, aggregation, retention, and opt-out or consent expectations.",
}

_REVIEW_QUESTIONS: dict[DataClassification, tuple[str, ...]] = {
    "public": ("Is every referenced dataset approved for public disclosure?",),
    "internal": ("Which internal roles need access to this data?",),
    "confidential": ("What retention period and access approvals apply to the confidential material?",),
    "restricted": ("Which privacy, security, or compliance owner must approve restricted data handling?",),
    "pii": ("What personal data fields are strictly required, and can any be tokenized or omitted?",),
    "credentials": ("Where will secrets be stored, rotated, redacted, and audited?",),
    "financial": ("Do payment, tax, billing, or financial compliance controls apply?",),
    "health": ("Does this data fall under health privacy regulation or contractual health-data controls?",),
    "minors": ("Are age, consent, guardian, and retention requirements defined for minors?",),
    "telemetry": ("Which events are necessary, and what aggregation or retention limits should apply?",),
}


@dataclass(frozen=True, slots=True)
class SourceDataClassificationRecord:
    """One normalized data classification signal from source brief text."""

    classification: DataClassification
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_handling: tuple[str, ...] = field(default_factory=tuple)
    review_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "classification": self.classification,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "recommended_handling": list(self.recommended_handling),
            "review_questions": list(self.review_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceDataClassificationReport:
    """Data classification report for one source brief payload."""

    source_brief_id: str | None = None
    title: str | None = None
    classifications: tuple[SourceDataClassificationRecord, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "classifications": [record.to_dict() for record in self.classifications],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return classification records as plain dictionaries."""
        return [record.to_dict() for record in self.classifications]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Data Classification"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        lines = [title, "", "## Summary", ""]
        lines.extend(
            [
                f"- Classification count: {self.summary.get('classification_count', 0)}",
                f"- Restricted category count: {self.summary.get('restricted_category_count', 0)}",
                f"- Highest confidence: {self.summary.get('highest_confidence', 0.0):.2f}",
            ]
        )
        if not self.classifications:
            lines.extend(["", "No source data classification signals were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Classifications",
                "",
                "| Classification | Confidence | Evidence | Recommended Handling | Review Questions |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.classifications:
            lines.append(
                "| "
                f"{record.classification} | "
                f"{record.confidence:.2f} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_handling) or 'none')} | "
                f"{_markdown_cell('; '.join(record.review_questions) or 'none')} |"
            )
        return "\n".join(lines)


def build_source_data_classification_report(
    source: Mapping[str, Any] | SourceBrief | Any,
) -> SourceDataClassificationReport:
    """Build a normalized data-classification report for a source brief-like payload."""
    brief = _payload_dict(source)
    source_brief_id = _optional_text(brief.get("id")) or _optional_text(brief.get("source_id"))
    title = _optional_text(brief.get("title"))
    evidence_by_classification: dict[DataClassification, list[str]] = {
        classification: [] for classification in _CLASSIFICATION_ORDER
    }
    keyword_counts: dict[DataClassification, int] = {
        classification: 0 for classification in _CLASSIFICATION_ORDER
    }

    for source_field, text in _candidate_texts(brief):
        for classification, patterns in _PATTERNS.items():
            matched = False
            for pattern in patterns:
                if pattern.search(text):
                    matched = True
                    keyword_counts[classification] += len(pattern.findall(text)) or 1
            if matched:
                evidence_by_classification[classification].append(_evidence_snippet(source_field, text))

    if any(evidence_by_classification[category] for category in _RESTRICTED_CATEGORIES):
        for category in _RESTRICTED_CATEGORIES:
            evidence_by_classification["restricted"].extend(evidence_by_classification[category])
            keyword_counts["restricted"] += keyword_counts[category]

    records = tuple(
        sorted(
            (
                SourceDataClassificationRecord(
                    classification=classification,
                    confidence=_confidence(
                        classification=classification,
                        evidence_count=len(deduped_evidence),
                        keyword_count=keyword_counts[classification],
                    ),
                    evidence=tuple(deduped_evidence[:_MAX_EVIDENCE_PER_CLASSIFICATION]),
                    recommended_handling=(_RECOMMENDED_HANDLING[classification],),
                    review_questions=_REVIEW_QUESTIONS[classification],
                )
                for classification, evidence in evidence_by_classification.items()
                if (deduped_evidence := _dedupe(evidence))
            ),
            key=lambda record: (
                -record.confidence,
                _CLASSIFICATION_ORDER[record.classification],
                record.classification,
            ),
        )
    )
    return SourceDataClassificationReport(
        source_brief_id=source_brief_id,
        title=title,
        classifications=records,
        summary=_summary(records),
    )


def summarize_source_data_classification(
    source: Mapping[str, Any] | SourceBrief | Any,
) -> SourceDataClassificationReport:
    """Compatibility alias for building source data classification reports."""
    return build_source_data_classification_report(source)


def source_data_classification_report_to_dict(
    report: SourceDataClassificationReport,
) -> dict[str, Any]:
    """Serialize a source data classification report to a plain dictionary."""
    return report.to_dict()


source_data_classification_report_to_dict.__test__ = False


def source_data_classification_report_to_markdown(
    report: SourceDataClassificationReport,
) -> str:
    """Render a source data classification report as Markdown."""
    return report.to_markdown()


source_data_classification_report_to_markdown.__test__ = False


def _summary(records: tuple[SourceDataClassificationRecord, ...]) -> dict[str, Any]:
    restricted_classifications = {
        record.classification for record in records if record.classification in _RESTRICTED_CATEGORIES
    }
    return {
        "classification_count": len(records),
        "restricted_category_count": len(restricted_classifications),
        "highest_confidence": max((record.confidence for record in records), default=0.0),
        "classification_counts": {
            classification: sum(1 for record in records if record.classification == classification)
            for classification in _CLASSIFICATION_ORDER
        },
        "restricted_category_counts": {
            classification: sum(1 for record in records if record.classification == classification)
            for classification in _RESTRICTED_CATEGORIES
        },
    }


def _payload_dict(source: Mapping[str, Any] | SourceBrief | Any) -> dict[str, Any]:
    if isinstance(source, SourceBrief):
        return source.model_dump(mode="python")
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    if isinstance(source, Mapping):
        return dict(source)
    return {}


def _candidate_texts(brief: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in _SCANNED_FIELDS:
        value = brief.get(field_name)
        if field_name == "metadata":
            texts.extend(_nested_texts(value, field_name))
            continue
        for index, text in enumerate(_strings(value)):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))

    if isinstance(brief.get("source_payload"), Mapping):
        for field_name in _SCANNED_FIELDS:
            if field_name in brief["source_payload"]:
                texts.extend(_nested_texts(brief["source_payload"][field_name], f"source_payload.{field_name}"))
    return texts


def _nested_texts(value: Any, prefix: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(child, field))
            else:
                if text := _optional_text(child):
                    texts.append((field, text))
                if key_text and any(pattern.search(key_text) for patterns in _PATTERNS.values() for pattern in patterns):
                    texts.append((field, key_text))
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


def _confidence(
    *, classification: DataClassification, evidence_count: int, keyword_count: int
) -> float:
    score = 0.55 + (0.12 * min(evidence_count - 1, 3)) + (0.08 * min(keyword_count - 1, 3))
    if classification in ("restricted", "credentials", "health", "minors", "financial", "pii"):
        score += 0.08
    return round(min(score, 0.98), 2)


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


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


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "DataClassification",
    "SourceDataClassificationRecord",
    "SourceDataClassificationReport",
    "build_source_data_classification_report",
    "source_data_classification_report_to_dict",
    "source_data_classification_report_to_markdown",
    "summarize_source_data_classification",
]
