"""Extract billing and entitlement requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


BillingEntitlementArea = Literal[
    "subscription",
    "plan_limit",
    "trial",
    "seat",
    "usage_metering",
    "invoice",
    "payment_failure",
    "entitlement_check",
    "upgrade_downgrade",
    "proration",
    "grace_period",
]
BillingEntitlementAudience = Literal["admins", "members", "trial_users", "paid_users", "users"]
BillingEntitlementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_AREA_ORDER: tuple[BillingEntitlementArea, ...] = (
    "subscription",
    "plan_limit",
    "trial",
    "seat",
    "usage_metering",
    "invoice",
    "payment_failure",
    "entitlement_check",
    "upgrade_downgrade",
    "proration",
    "grace_period",
)
_AUDIENCE_ORDER: tuple[BillingEntitlementAudience, ...] = (
    "admins",
    "members",
    "trial_users",
    "paid_users",
    "users",
)
_CONFIDENCE_ORDER: dict[BillingEntitlementConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but)\s+", re.I)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")

_AREA_PATTERNS: dict[BillingEntitlementArea, re.Pattern[str]] = {
    "subscription": re.compile(
        r"\b(?:subscription|subscriber|subscribed|billing status|paid plan|active plan|cancel(?:ed|led|lation)?)\b",
        re.I,
    ),
    "plan_limit": re.compile(
        r"\b(?:plan limit|plan limits|tier limit|quota|quotas|limit per plan|free tier|pro tier|enterprise tier|"
        r"cap|capped|allowance|feature limit|usage limit|seat limits?|member limits?|licensed user limits?)\b",
        re.I,
    ),
    "trial": re.compile(
        r"\b(?:trial|trial access|free trial|trialing|trial period|evaluation period)\b", re.I
    ),
    "seat": re.compile(
        r"\b(?:seat|seats|seat count|licensed user|license count|per[- ]seat|team member limit)\b",
        re.I,
    ),
    "usage_metering": re.compile(
        r"\b(?:usage metering|metered usage|usage[- ]based|usage based|metered billing|overage|"
        r"usage events?|billable events?|usage counter|consumption)\b",
        re.I,
    ),
    "invoice": re.compile(
        r"\b(?:invoice|invoices|receipt|billing history|tax invoice|billing portal)\b", re.I
    ),
    "payment_failure": re.compile(
        r"\b(?:payment failure|failed payment|card declined|declined card|past due|dunning|retry payment|"
        r"billing retry|unpaid|payment method failed)\b",
        re.I,
    ),
    "entitlement_check": re.compile(
        r"\b(?:entitlement|entitlements|entitlement check|feature gate|feature flag|gated feature|"
        r"access check|permission check|billing gate|paywall|locked feature|unlock)\b",
        re.I,
    ),
    "upgrade_downgrade": re.compile(
        r"\b(?:upgrade|downgrade|plan change|change plan|switch plans?)\b", re.I
    ),
    "proration": re.compile(
        r"\b(?:proration|prorate|prorated|mid[- ]cycle|billing adjustment)\b", re.I
    ),
    "grace_period": re.compile(
        r"\b(?:grace period|grace window|soft lock|suspension window|access grace)\b", re.I
    ),
}
_AUDIENCE_PATTERNS: dict[BillingEntitlementAudience, re.Pattern[str]] = {
    "admins": re.compile(
        r"\b(?:admin|admins|administrator|workspace owner|account owner|billing owner|owner)\b",
        re.I,
    ),
    "members": re.compile(
        r"\b(?:member|members|team member|teammate|workspace user|licensed user)\b", re.I
    ),
    "trial_users": re.compile(
        r"\b(?:trial user|trial users|trialing user|trialing users|free trial user)\b", re.I
    ),
    "paid_users": re.compile(
        r"\b(?:paid user|paid users|subscriber|subscribers|paying customers?|paid customers?)\b",
        re.I,
    ),
    "users": re.compile(
        r"\b(?:user|users|customer|customers|account|accounts|tenant|tenants)\b", re.I
    ),
}
_REQUIRED_RE = re.compile(
    r"\b(?:must|required|requires|need|needs|cannot|block|blocked|only if|before launch)\b", re.I
)


@dataclass(frozen=True, slots=True)
class SourceBillingEntitlementRequirement:
    """One source-backed billing or entitlement requirement."""

    entitlement_area: BillingEntitlementArea
    affected_audience: BillingEntitlementAudience
    confidence: BillingEntitlementConfidence
    evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_acceptance_criterion: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "entitlement_area": self.entitlement_area,
            "affected_audience": self.affected_audience,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "suggested_acceptance_criterion": self.suggested_acceptance_criterion,
        }


@dataclass(frozen=True, slots=True)
class SourceBillingEntitlementsReport:
    """Brief-level billing and entitlement requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceBillingEntitlementRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceBillingEntitlementRequirement, ...]:
        """Return requirements using the generic report record naming."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return billing entitlement requirements as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Billing Entitlements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        area_counts = self.summary.get("entitlement_area_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Confidence counts: "
            f"high {confidence_counts.get('high', 0)}, "
            f"medium {confidence_counts.get('medium', 0)}, "
            f"low {confidence_counts.get('low', 0)}",
            "- Entitlement area counts: "
            + (", ".join(f"{key} {area_counts[key]}" for key in sorted(area_counts)) or "none"),
        ]
        if not self.requirements:
            lines.extend(
                ["", "No billing entitlement requirements were found in the source brief."]
            )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Area | Audience | Confidence | Evidence | Suggested Acceptance Criterion |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.entitlement_area} | "
                f"{requirement.affected_audience} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.suggested_acceptance_criterion)} |"
            )
        return "\n".join(lines)


def build_source_billing_entitlements(
    source: Mapping[str, Any] | SourceBrief | object,
) -> SourceBillingEntitlementsReport:
    """Extract billing and entitlement requirement signals from a source brief."""
    source_id, payload = _source_payload(source)
    grouped: dict[tuple[BillingEntitlementArea, BillingEntitlementAudience], dict[str, Any]] = {}
    for source_field, segment in _candidate_segments(payload):
        areas = _entitlement_areas(segment)
        if not areas:
            continue
        audiences = _affected_audiences(segment)
        for area in areas:
            for audience in audiences:
                bucket = grouped.setdefault(
                    (area, audience),
                    {"entitlement_area": area, "affected_audience": audience, "evidence": []},
                )
                bucket["evidence"].append(_evidence_snippet(source_field, segment))

    requirements = tuple(
        sorted(
            (_requirement_from_bucket(bucket) for bucket in grouped.values() if bucket["evidence"]),
            key=lambda requirement: (
                _CONFIDENCE_ORDER[requirement.confidence],
                _area_index(requirement.entitlement_area),
                _audience_index(requirement.affected_audience),
                requirement.evidence,
            ),
        )
    )
    area_counts = {
        area: sum(1 for requirement in requirements if requirement.entitlement_area == area)
        for area in _AREA_ORDER
    }
    audience_counts = {
        audience: sum(
            1 for requirement in requirements if requirement.affected_audience == audience
        )
        for audience in _AUDIENCE_ORDER
    }
    confidence_counts = {
        confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
        for confidence in _CONFIDENCE_ORDER
    }
    return SourceBillingEntitlementsReport(
        source_id=source_id,
        requirements=requirements,
        summary={
            "requirement_count": len(requirements),
            "entitlement_area_counts": area_counts,
            "audience_counts": audience_counts,
            "confidence_counts": confidence_counts,
        },
    )


def generate_source_billing_entitlements(
    source: Mapping[str, Any] | SourceBrief | object,
) -> SourceBillingEntitlementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_billing_entitlements(source)


def source_billing_entitlements_to_dict(report: SourceBillingEntitlementsReport) -> dict[str, Any]:
    """Serialize a billing entitlements report to a plain dictionary."""
    return report.to_dict()


source_billing_entitlements_to_dict.__test__ = False


def source_billing_entitlements_to_markdown(report: SourceBillingEntitlementsReport) -> str:
    """Render a billing entitlements report as Markdown."""
    return report.to_markdown()


source_billing_entitlements_to_markdown.__test__ = False


def _requirement_from_bucket(bucket: Mapping[str, Any]) -> SourceBillingEntitlementRequirement:
    area = bucket["entitlement_area"]
    audience = bucket["affected_audience"]
    evidence = tuple(
        sorted(_dedupe(_strings(bucket.get("evidence"))), key=lambda item: item.casefold())
    )
    evidence_text = " ".join(evidence)
    return SourceBillingEntitlementRequirement(
        entitlement_area=area,
        affected_audience=audience,
        confidence=_confidence(area, audience, evidence_text, len(evidence)),
        evidence=evidence,
        suggested_acceptance_criterion=_suggested_acceptance_criterion(area, audience),
    )


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | object
) -> tuple[str | None, dict[str, Any]]:
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
    source_payload = (
        payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    )
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
        "billing",
        "entitlements",
        "acceptance_criteria",
        "implementation_notes",
    ):
        segments.extend(_field_segments(payload.get(field_name), field_name))
        segments.extend(
            _field_segments(source_payload.get(field_name), f"source_payload.{field_name}")
        )
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
                    segments.extend(
                        (source_field, segment) for segment in _segments(f"{key_text}: {text}")
                    )
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
    segments = []
    for sentence in _SENTENCE_SPLIT_RE.split(text):
        segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _entitlement_areas(text: str) -> tuple[BillingEntitlementArea, ...]:
    return tuple(area for area in _AREA_ORDER if _AREA_PATTERNS[area].search(text))


def _affected_audiences(text: str) -> tuple[BillingEntitlementAudience, ...]:
    audiences = [
        audience for audience in _AUDIENCE_ORDER[:-1] if _AUDIENCE_PATTERNS[audience].search(text)
    ]
    if not audiences and _AUDIENCE_PATTERNS["users"].search(text):
        audiences.append("users")
    if not audiences:
        audiences.append("users")
    return tuple(_dedupe(audiences))


def _confidence(
    area: BillingEntitlementArea,
    audience: BillingEntitlementAudience,
    evidence_text: str,
    evidence_count: int,
) -> BillingEntitlementConfidence:
    if _REQUIRED_RE.search(evidence_text) or area in {
        "payment_failure",
        "entitlement_check",
        "upgrade_downgrade",
        "proration",
        "grace_period",
    }:
        return "high"
    if audience != "users" or evidence_count > 1:
        return "medium"
    return "low"


def _suggested_acceptance_criterion(
    area: BillingEntitlementArea,
    audience: BillingEntitlementAudience,
) -> str:
    label = audience.replace("_", " ")
    criteria = {
        "subscription": f"Given {label} with active, inactive, or canceled subscription states, access follows the documented billing status rules.",
        "plan_limit": f"Given {label} at each plan tier, plan limits are enforced with clear messaging when a limit is reached.",
        "trial": f"Given {label} in trial and expired-trial states, trial access starts, expires, and converts according to the documented rules.",
        "seat": f"Given {label} managing seats, seat counts and licensed-user limits are enforced before changes are saved.",
        "usage_metering": f"Given {label} generating billable usage, usage is metered once, attributed correctly, and visible for billing review.",
        "invoice": f"Given {label} with billing access, invoices or receipts are available with the expected status and billing-period details.",
        "payment_failure": f"Given {label} with a failed payment, retry, dunning, grace, and access changes follow the documented failure policy.",
        "entitlement_check": f"Given {label} attempting gated actions, entitlement checks allow or block access according to the user's plan and status.",
        "upgrade_downgrade": f"Given {label} changing plans, upgrades and downgrades apply the correct access, timing, and billing behavior.",
        "proration": f"Given {label} changing billable state mid-cycle, prorated charges or credits match the documented calculation.",
        "grace_period": f"Given {label} entering a grace period, access is retained or restricted according to the documented grace-window rules.",
    }
    return criteria[area]


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _AREA_PATTERNS.values())


def _area_index(area: BillingEntitlementArea) -> int:
    return _AREA_ORDER.index(area)


def _audience_index(audience: BillingEntitlementAudience) -> int:
    return _AUDIENCE_ORDER.index(audience)


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
        "billing",
        "entitlements",
        "acceptance_criteria",
        "implementation_notes",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
    "BillingEntitlementArea",
    "BillingEntitlementAudience",
    "BillingEntitlementConfidence",
    "SourceBillingEntitlementRequirement",
    "SourceBillingEntitlementsReport",
    "build_source_billing_entitlements",
    "generate_source_billing_entitlements",
    "source_billing_entitlements_to_dict",
    "source_billing_entitlements_to_markdown",
]
