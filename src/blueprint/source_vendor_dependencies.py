"""Extract third-party vendor and SaaS dependency signals from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


VendorDependencyType = Literal[
    "payment_provider",
    "identity_provider",
    "analytics_tool",
    "email_sms_provider",
    "cloud_service",
    "marketplace_app",
    "sdk",
    "api",
    "vendor",
]
VendorDependencyConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_DEPENDENCY_ORDER: tuple[VendorDependencyType, ...] = (
    "payment_provider",
    "identity_provider",
    "analytics_tool",
    "email_sms_provider",
    "cloud_service",
    "marketplace_app",
    "sdk",
    "api",
    "vendor",
)
_CONFIDENCE_ORDER: dict[VendorDependencyConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_DEPENDENCY_PATTERNS: dict[VendorDependencyType, re.Pattern[str]] = {
    "payment_provider": re.compile(r"\b(?:payment|payments|checkout|card|billing|invoice|merchant|pci)\b", re.I),
    "identity_provider": re.compile(
        r"\b(?:identity provider|idp|sso|saml|oidc|openid connect|oauth|login|auth|authentication)\b",
        re.I,
    ),
    "analytics_tool": re.compile(
        r"\b(?:analytics|tracking|attribution|event instrumentation|experimentation|product metrics)\b",
        re.I,
    ),
    "email_sms_provider": re.compile(
        r"\b(?:email|sms|messaging|notification|deliverability|sender|otp|one[- ]time passcode)\b",
        re.I,
    ),
    "cloud_service": re.compile(
        r"\b(?:cloud|object storage|bucket|cdn|queue|serverless|compute|warehouse|kubernetes|database)\b",
        re.I,
    ),
    "marketplace_app": re.compile(
        r"\b(?:marketplace app|app marketplace|crm|helpdesk|ticketing|erp|ecommerce|storefront|workspace app)\b",
        re.I,
    ),
    "sdk": re.compile(r"\b(?:sdk|client library|package|npm package|python package|gem)\b", re.I),
    "api": re.compile(r"\b(?:api|apis|endpoint|webhook|graphql|rest|integration)\b", re.I),
    "vendor": re.compile(
        r"\b(?:third[- ]party|saas|processor|subprocessor|provider|partner|vendor dependenc(?:y|ies)|external vendor)\b",
        re.I,
    ),
}
_KNOWN_VENDORS: dict[str, VendorDependencyType] = {
    "stripe": "payment_provider",
    "adyen": "payment_provider",
    "paypal": "payment_provider",
    "braintree": "payment_provider",
    "checkout.com": "payment_provider",
    "auth0": "identity_provider",
    "okta": "identity_provider",
    "onelogin": "identity_provider",
    "azure ad": "identity_provider",
    "entra id": "identity_provider",
    "cognito": "identity_provider",
    "segment": "analytics_tool",
    "amplitude": "analytics_tool",
    "mixpanel": "analytics_tool",
    "google analytics": "analytics_tool",
    "looker": "analytics_tool",
    "sendgrid": "email_sms_provider",
    "mailgun": "email_sms_provider",
    "postmark": "email_sms_provider",
    "twilio": "email_sms_provider",
    "amazon ses": "email_sms_provider",
    "ses": "email_sms_provider",
    "aws": "cloud_service",
    "amazon web services": "cloud_service",
    "s3": "cloud_service",
    "lambda": "cloud_service",
    "sqs": "cloud_service",
    "sns": "cloud_service",
    "gcp": "cloud_service",
    "google cloud": "cloud_service",
    "azure": "cloud_service",
    "snowflake": "cloud_service",
    "datadog": "cloud_service",
    "shopify": "marketplace_app",
    "salesforce": "marketplace_app",
    "hubspot": "marketplace_app",
    "zendesk": "marketplace_app",
    "slack": "marketplace_app",
    "jira": "marketplace_app",
    "atlassian": "marketplace_app",
    "netsuite": "marketplace_app",
    "workday": "marketplace_app",
}
_KNOWN_VENDOR_RE = re.compile(
    r"\b(?P<name>"
    + "|".join(re.escape(name) for name in sorted(_KNOWN_VENDORS, key=len, reverse=True))
    + r")\b",
    re.I,
)
_PROVIDER_NAME_PATTERN = r"[A-Z][A-Za-z0-9&_.-]*(?:\s+[A-Z][A-Za-z0-9&_.-]*){0,4}"
_KEY_VALUE_VENDOR_RE = re.compile(
    r"\b(?:vendor|provider|processor|subprocessor|partner|service|platform|tool|app)\b\s*(?:is|=|:)\s*"
    rf"(?P<name>{_PROVIDER_NAME_PATTERN})"
)
_NEARBY_VENDOR_RE = re.compile(
    r"\b(?:with|from|to|via|against|through|using|integrate with|connect to|depends on)\s+"
    rf"(?P<name>{_PROVIDER_NAME_PATTERN})\s+"
    r"(?:API|SDK|webhook|OAuth|SSO|SAML|OIDC|app|marketplace|cloud|service|provider|integration)\b",
    re.I,
)
_PREFIX_VENDOR_RE = re.compile(
    rf"\b(?P<name>{_PROVIDER_NAME_PATTERN})\s+"
    r"(?:API|SDK|webhook|OAuth|SSO|SAML|OIDC|app|marketplace|cloud|service|provider|integration)\b"
)
_VENDOR_STOPWORDS = {
    "api",
    "external",
    "internal",
    "marketplace",
    "partner",
    "payment",
    "provider",
    "rest",
    "source",
    "third",
    "tbd",
    "vendor",
}
_HIGH_ATTENTION_RE = re.compile(
    r"\b(?:credential|credentials|secret|api key|token|oauth|client secret|regulated|personal data|pii|phi|"
    r"cardholder|pci|gdpr|hipaa|uptime|availability|sla|slo|outage|contract|msa|dpa|baa|terms|renewal|"
    r"rate limit|quota|throttle|429)\b",
    re.I,
)
_REQUIRED_RE = re.compile(r"\b(?:must|required|requires|need|needs|cannot|blocked|depends on|before launch)\b", re.I)


@dataclass(frozen=True, slots=True)
class SourceVendorDependency:
    """One source-backed third-party dependency candidate."""

    vendor_name: str
    dependency_type: VendorDependencyType
    confidence: VendorDependencyConfidence
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    planning_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "vendor_name": self.vendor_name,
            "dependency_type": self.dependency_type,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "recommended_checks": list(self.recommended_checks),
            "planning_questions": list(self.planning_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceVendorDependenciesReport:
    """Brief-level vendor dependency report."""

    brief_id: str | None = None
    dependencies: tuple[SourceVendorDependency, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "dependencies": [dependency.to_dict() for dependency in self.dependencies],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return dependencies as plain dictionaries."""
        return [dependency.to_dict() for dependency in self.dependencies]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Vendor Dependencies Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        type_counts = self.summary.get("dependency_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Dependencies found: {self.summary.get('dependency_count', 0)}",
            f"- High-attention dependencies: {self.summary.get('high_attention_count', 0)}",
            "- Confidence counts: "
            f"high {confidence_counts.get('high', 0)}, "
            f"medium {confidence_counts.get('medium', 0)}, "
            f"low {confidence_counts.get('low', 0)}",
            "- Dependency type counts: "
            + (", ".join(f"{key} {type_counts[key]}" for key in sorted(type_counts)) or "none"),
        ]
        if not self.dependencies:
            lines.extend(["", "No vendor dependencies were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Dependencies",
                "",
                "| Vendor | Type | Confidence | Evidence | Recommended Checks | Planning Questions |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for dependency in self.dependencies:
            lines.append(
                "| "
                f"{_markdown_cell(dependency.vendor_name or 'unknown')} | "
                f"{dependency.dependency_type} | "
                f"{dependency.confidence} | "
                f"{_markdown_cell('; '.join(dependency.evidence))} | "
                f"{_markdown_cell('; '.join(dependency.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(dependency.planning_questions) or 'none')} |"
            )
        return "\n".join(lines)


def build_source_vendor_dependencies(
    source: Mapping[str, Any] | SourceBrief | object,
) -> SourceVendorDependenciesReport:
    """Extract third-party vendor and SaaS dependency signals from a source brief."""
    brief_id, payload = _source_payload(source)
    grouped: dict[tuple[str, VendorDependencyType], dict[str, Any]] = {}
    for source_field, segment in _candidate_segments(payload):
        vendors = _vendor_names(segment, source_field)
        dependency_types = _dependency_types(segment, vendors)
        if not vendors and not dependency_types:
            continue
        if not vendors:
            vendors = ("",)
        if not dependency_types:
            dependency_types = ("vendor",)
        for vendor_name in vendors:
            for dependency_type in dependency_types:
                key = (_vendor_key(vendor_name), dependency_type)
                bucket = grouped.setdefault(
                    key,
                    {
                        "vendor_name": vendor_name,
                        "dependency_type": dependency_type,
                        "evidence": [],
                    },
                )
                if not bucket["vendor_name"] and vendor_name:
                    bucket["vendor_name"] = vendor_name
                bucket["evidence"].append(_evidence_snippet(source_field, segment))

    dependencies = tuple(
        sorted(
            (
                _dependency_from_bucket(bucket)
                for bucket in grouped.values()
                if bucket["evidence"]
            ),
            key=lambda dependency: (
                _CONFIDENCE_ORDER[dependency.confidence],
                _dependency_index(dependency.dependency_type),
                dependency.vendor_name.casefold(),
                dependency.evidence,
            ),
        )
    )
    type_counts = {
        dependency_type: sum(1 for dependency in dependencies if dependency.dependency_type == dependency_type)
        for dependency_type in _DEPENDENCY_ORDER
    }
    confidence_counts = {
        confidence: sum(1 for dependency in dependencies if dependency.confidence == confidence)
        for confidence in _CONFIDENCE_ORDER
    }
    high_attention_count = sum(1 for dependency in dependencies if _is_high_attention(dependency.evidence))
    return SourceVendorDependenciesReport(
        brief_id=brief_id,
        dependencies=dependencies,
        summary={
            "dependency_count": len(dependencies),
            "dependency_type_counts": type_counts,
            "confidence_counts": confidence_counts,
            "high_attention_count": high_attention_count,
            "vendors": [dependency.vendor_name for dependency in dependencies if dependency.vendor_name],
        },
    )


def generate_source_vendor_dependencies(
    source: Mapping[str, Any] | SourceBrief | object,
) -> SourceVendorDependenciesReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_vendor_dependencies(source)


def source_vendor_dependencies_to_dict(report: SourceVendorDependenciesReport) -> dict[str, Any]:
    """Serialize a vendor dependencies report to a plain dictionary."""
    return report.to_dict()


source_vendor_dependencies_to_dict.__test__ = False


def source_vendor_dependencies_to_markdown(report: SourceVendorDependenciesReport) -> str:
    """Render a vendor dependencies report as Markdown."""
    return report.to_markdown()


source_vendor_dependencies_to_markdown.__test__ = False


def _dependency_from_bucket(bucket: Mapping[str, Any]) -> SourceVendorDependency:
    vendor_name = _clean_vendor(bucket.get("vendor_name", ""))
    dependency_type = bucket["dependency_type"]
    evidence = tuple(sorted(_dedupe(_strings(bucket.get("evidence"))), key=lambda item: item.casefold()))
    evidence_text = " ".join(evidence)
    return SourceVendorDependency(
        vendor_name=vendor_name,
        dependency_type=dependency_type,
        confidence=_confidence(vendor_name, dependency_type, evidence_text),
        evidence=evidence,
        recommended_checks=_recommended_checks(dependency_type, evidence_text),
        planning_questions=_planning_questions(vendor_name, dependency_type, evidence_text),
    )


def _source_payload(source: Mapping[str, Any] | SourceBrief | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, SourceBrief):
        return _optional_text(source.id), source.model_dump(mode="python")
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _optional_text(payload.get("id")), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
        return _optional_text(payload.get("id")), payload
    if not isinstance(source, (str, bytes)):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), payload
    return None, {}


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    segments: list[tuple[str, str]] = []
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "requirements",
        "constraints",
        "risks",
        "metadata",
        "brief_metadata",
        "integration_points",
        "architecture_notes",
        "data_requirements",
    ):
        segments.extend(_field_segments(payload.get(field_name), field_name))
        segments.extend(_field_segments(source_payload.get(field_name), f"source_payload.{field_name}"))
    for field, text in _metadata_texts(payload.get("metadata")):
        segments.extend((field, segment) for segment in _segments(text))
    for field, text in _metadata_texts(payload.get("brief_metadata"), "brief_metadata"):
        segments.extend((field, segment) for segment in _segments(text))
    for field, text in _metadata_texts(source_payload.get("metadata"), "source_payload.metadata"):
        segments.extend((field, segment) for segment in _segments(text))
    return [(field, segment) for field, segment in segments if segment]


def _field_segments(value: Any, field_name: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        segments: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            source_field = f"{field_name}.{key}"
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if _any_signal(key_text):
                    segments.extend((source_field, segment) for segment in _segments(key_text))
                segments.extend(_field_segments(child, source_field))
            elif text := _optional_text(child):
                segments.extend((source_field, segment) for segment in _segments(text))
                if _any_signal(key_text):
                    segments.extend((source_field, segment) for segment in _segments(f"{key_text}: {text}"))
            elif _any_signal(key_text):
                segments.extend((source_field, segment) for segment in _segments(key_text))
        return segments
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        segments = []
        for index, item in enumerate(items):
            segments.extend(_field_segments(item, f"{field_name}[{index}]"))
        return segments
    return [(field_name, segment) for segment in _segments(value)]


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if _any_signal(key_text):
                    texts.append((field, key_text))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _any_signal(key_text):
                texts.append((field, key_text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            texts.extend(_metadata_texts(item, f"{prefix}[{index}]"))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _segments(value: Any) -> list[str]:
    text = _optional_text(value)
    if text is None:
        return []
    return [_clean_text(part) for part in _SENTENCE_SPLIT_RE.split(text) if _clean_text(part)]


def _vendor_names(text: str, source_field: str) -> tuple[str, ...]:
    names = [_canonical_known_vendor(match.group("name")) for match in _KNOWN_VENDOR_RE.finditer(text)]
    for pattern in (_KEY_VALUE_VENDOR_RE, _NEARBY_VENDOR_RE, _PREFIX_VENDOR_RE):
        match = pattern.search(text)
        if match is not None:
            names.append(_clean_vendor(match.group("name")))
    if re.search(r"(?:vendor|provider|processor|subprocessor|partner|service|platform|tool|app)", source_field, re.I):
        field_value = _clean_vendor(text)
        if _valid_vendor(field_value):
            names.append(field_value)
    return tuple(_dedupe(name for name in names if _valid_vendor(name)))


def _dependency_types(text: str, vendors: tuple[str, ...]) -> tuple[VendorDependencyType, ...]:
    types = [dependency_type for dependency_type in _DEPENDENCY_ORDER if _DEPENDENCY_PATTERNS[dependency_type].search(text)]
    for vendor in vendors:
        known_type = _KNOWN_VENDORS.get(vendor.casefold())
        if known_type:
            types.append(known_type)
    if vendors and not types:
        types.append("vendor")
    return tuple(_dedupe(types))


def _confidence(
    vendor_name: str,
    dependency_type: VendorDependencyType,
    evidence_text: str,
) -> VendorDependencyConfidence:
    if vendor_name and dependency_type != "vendor" and (_HIGH_ATTENTION_RE.search(evidence_text) or _REQUIRED_RE.search(evidence_text)):
        return "high"
    if vendor_name and dependency_type != "vendor":
        return "medium"
    if vendor_name or dependency_type in {"api", "sdk"}:
        return "medium"
    return "low"


def _recommended_checks(dependency_type: VendorDependencyType, evidence_text: str) -> tuple[str, ...]:
    checks = [
        "Confirm vendor owner, production account, sandbox access, and support path.",
        "Document authentication method, credential storage, rotation, and least-privilege scopes.",
    ]
    if dependency_type == "payment_provider":
        checks.append("Verify PCI scope, payment failure handling, refunds, disputes, and webhook replay behavior.")
    if dependency_type == "identity_provider":
        checks.append("Verify SSO/OAuth configuration, required claims, session lifetime, and break-glass access.")
    if dependency_type == "analytics_tool":
        checks.append("Verify consent, event schema, PII handling, and environment separation.")
    if dependency_type == "email_sms_provider":
        checks.append("Verify sender/domain setup, deliverability controls, opt-out rules, and quota behavior.")
    if dependency_type == "cloud_service":
        checks.append("Verify region, availability, IAM permissions, quotas, backups, and monitoring coverage.")
    if dependency_type == "marketplace_app":
        checks.append("Verify app review requirements, tenant installation flow, scopes, and marketplace terms.")
    if dependency_type in {"api", "sdk"}:
        checks.append("Verify versioning, rate limits, retries, idempotency, errors, and deprecation policy.")
    if _HIGH_ATTENTION_RE.search(evidence_text):
        checks.append("Escalate credentials, regulated data, uptime, contract, and rate-limit assumptions before sequencing work.")
    return tuple(_dedupe(checks))


def _planning_questions(
    vendor_name: str,
    dependency_type: VendorDependencyType,
    evidence_text: str,
) -> tuple[str, ...]:
    questions = []
    vendor_label = vendor_name or "the vendor"
    if not vendor_name:
        questions.append("Which third-party vendor or SaaS product owns this dependency?")
    questions.append(f"Who owns the relationship, credentials, and production configuration for {vendor_label}?")
    if dependency_type in {"api", "sdk"} or re.search(r"\b(?:api|sdk|webhook|endpoint)\b", evidence_text, re.I):
        questions.append(f"What API/SDK versions, limits, retries, and test environments are available for {vendor_label}?")
    if _HIGH_ATTENTION_RE.search(evidence_text):
        questions.append(f"Are legal, security, privacy, uptime, and rate-limit approvals complete for {vendor_label}?")
    return tuple(_dedupe(questions))


def _is_high_attention(evidence: Iterable[str]) -> bool:
    return any(_HIGH_ATTENTION_RE.search(item) for item in evidence)


def _any_signal(text: str) -> bool:
    return bool(_KNOWN_VENDOR_RE.search(text)) or any(pattern.search(text) for pattern in _DEPENDENCY_PATTERNS.values())


def _canonical_known_vendor(value: str) -> str:
    canonical = {
        "aws": "AWS",
        "gcp": "GCP",
        "s3": "S3",
        "sqs": "SQS",
        "sns": "SNS",
        "ses": "SES",
    }
    cleaned = _clean_vendor(value)
    return canonical.get(cleaned.casefold(), cleaned)


def _clean_vendor(value: Any) -> str:
    text = _clean_text(str(value))
    text = re.sub(r"\b(?:the|a|an|our|external|third[- ]party)\b\s*", "", text, flags=re.I)
    text = re.sub(
        r"\s+(?:API|SDK|webhook|OAuth|SSO|SAML|OIDC|app|marketplace|cloud|service|provider|integration)\b.*$",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(r"\s+(?:must|should|needs?|will|uses?|requires?|for|using|via|with|from|to)\b.*$", "", text, flags=re.I)
    return text.strip(" .,:;-")


def _valid_vendor(value: str) -> bool:
    if not value:
        return False
    if value.casefold() in _VENDOR_STOPWORDS:
        return False
    if value.casefold() in _KNOWN_VENDORS:
        return True
    if value.casefold() in {"aws", "gcp", "s3", "sqs", "sns", "ses"}:
        return True
    if re.search(r"\b(?:and|or|but|with|for|the|a|an|is|are|was|were)\b", value):
        return False
    words = value.split()
    return bool(words) and all(word[:1].isupper() or word.isupper() for word in words)


def _vendor_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def _dependency_index(dependency_type: VendorDependencyType) -> int:
    return _DEPENDENCY_ORDER.index(dependency_type)


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
        "requirements",
        "constraints",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
        "source_links",
        "integration_points",
        "architecture_notes",
        "data_requirements",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _SPACE_RE.sub(" ", str(value)).strip()
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[Any] = set()
    for value in values:
        key = value.casefold() if isinstance(value, str) else value
        if not value or key in seen:
            continue
        result.append(value)
        seen.add(key)
    return result


__all__ = [
    "SourceVendorDependenciesReport",
    "SourceVendorDependency",
    "VendorDependencyConfidence",
    "VendorDependencyType",
    "build_source_vendor_dependencies",
    "generate_source_vendor_dependencies",
    "source_vendor_dependencies_to_dict",
    "source_vendor_dependencies_to_markdown",
]
