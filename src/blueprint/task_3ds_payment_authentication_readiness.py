"""Assess task readiness for 3DS, SCA, and payment requires_action flows."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


PaymentAuthenticationCategory = Literal[
    "challenge_flow",
    "frictionless_flow",
    "failure_fallback",
    "liability_shift",
    "provider_webhook",
    "saved_payment_method",
    "test_evidence",
]
PaymentAuthenticationReadinessSeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CATEGORY_ORDER: tuple[PaymentAuthenticationCategory, ...] = (
    "challenge_flow",
    "frictionless_flow",
    "failure_fallback",
    "liability_shift",
    "provider_webhook",
    "saved_payment_method",
    "test_evidence",
)
_SEVERITY_ORDER: dict[PaymentAuthenticationReadinessSeverity, int] = {"high": 0, "medium": 1, "low": 2}
_CORE_AUTH_CATEGORIES: set[PaymentAuthenticationCategory] = {
    "challenge_flow",
    "frictionless_flow",
    "failure_fallback",
    "liability_shift",
    "provider_webhook",
    "saved_payment_method",
}
_REQUIRED_BASE: set[PaymentAuthenticationCategory] = {
    "challenge_flow",
    "frictionless_flow",
    "failure_fallback",
    "liability_shift",
    "provider_webhook",
    "test_evidence",
}
_PATH_CATEGORY_PATTERNS: dict[PaymentAuthenticationCategory, re.Pattern[str]] = {
    "challenge_flow": re.compile(
        r"(?:^|/)(?:3ds|three[_-]?d[_-]?secure|sca|strong[_-]?customer[_-]?authentication|"
        r"step[_-]?up|requires[_-]?action|payment[_-]?auth)(?:/|$)|"
        r"(?:3ds|three[_-]?d[_-]?secure|sca|step[_-]?up|requires[_-]?action|authenticate[_-]?payment)",
        re.I,
    ),
    "frictionless_flow": re.compile(r"(?:frictionless|exemption|low[_-]?risk|tra|sca[_-]?exempt)", re.I),
    "failure_fallback": re.compile(r"(?:fallback|failure|decline|cancel|timeout|retry|recover|soft[_-]?decline)", re.I),
    "liability_shift": re.compile(r"(?:liability[_-]?shift|eci|cavv|xid|authentication[_-]?result|three[_-]?ds[_-]?result)", re.I),
    "provider_webhook": re.compile(r"(?:webhook|stripe|adyen|braintree|checkout|payment[_-]?intent|setup[_-]?intent)", re.I),
    "saved_payment_method": re.compile(r"(?:saved[_-]?payment|payment[_-]?method|card[_-]?on[_-]?file|setup[_-]?intent|mandate)", re.I),
    "test_evidence": re.compile(r"(?:test|spec|fixture|mock|validation|playwright|pytest|rspec|cypress)", re.I),
}
_TEXT_CATEGORY_PATTERNS: dict[PaymentAuthenticationCategory, re.Pattern[str]] = {
    "challenge_flow": re.compile(
        r"\b(?:3ds|3[- ]?d secure|three[- ]?d secure|sca|strong customer authentication|"
        r"step[- ]?up|challenge flow|challenge screen|issuer challenge|acs|requires_action|"
        r"requires action|authenticate payment|payment authentication|payment_intent\.requires_action|"
        r"next_action|use_stripe_sdk)\b",
        re.I,
    ),
    "frictionless_flow": re.compile(
        r"\b(?:frictionless|frictionless flow|no challenge|challenge not required|exemption|"
        r"sca exemption|low risk|tra exemption|merchant initiated transaction|mit|off[- ]?session)\b",
        re.I,
    ),
    "failure_fallback": re.compile(
        r"\b(?:fallback|fallback path|failure fallback|cancelled challenge|canceled challenge|"
        r"challenge timeout|authentication failed|authentication failure|auth failed|soft decline|"
        r"hard decline|retry payment|retry checkout|recover checkout|payment failed|decline)\b",
        re.I,
    ),
    "liability_shift": re.compile(
        r"\b(?:liability shift|liability shifted|eci|cavv|xid|ds transaction id|three ds result|"
        r"3ds result|authentication result|authenticated transaction|authentication status)\b",
        re.I,
    ),
    "provider_webhook": re.compile(
        r"\b(?:provider webhook|payment webhook|webhook|stripe webhook|adyen webhook|braintree webhook|"
        r"checkout\.com webhook|payment_intent\.(?:requires_action|succeeded|payment_failed)|"
        r"setup_intent\.(?:requires_action|succeeded|setup_failed)|invoice\.payment_action_required|"
        r"source\.chargeable|charge\.failed)\b",
        re.I,
    ),
    "saved_payment_method": re.compile(
        r"\b(?:saved payment method|saved card|card on file|card[- ]on[- ]file|stored credential|"
        r"payment method reuse|setup intent|setup_intent|mandate|off[- ]session|subscription renewal)\b",
        re.I,
    ),
    "test_evidence": re.compile(
        r"\b(?:test evidence|validation command|validation commands|tests? cover|unit test|integration test|"
        r"e2e|end[- ]to[- ]end|webhook fixture|stripe test card|3ds test card|requires_action test|"
        r"payment authentication test|pytest|rspec|jest|cypress|playwright)\b",
        re.I,
    ),
}
_SUGGESTED_ACCEPTANCE_CRITERIA: dict[PaymentAuthenticationCategory, str] = {
    "challenge_flow": "Acceptance criteria cover the 3DS/SCA challenge or requires_action handoff and successful return path.",
    "frictionless_flow": "Acceptance criteria cover frictionless or exemption decisions when no issuer challenge is presented.",
    "failure_fallback": "Acceptance criteria cover challenge cancellation, timeout, authentication failure, declines, and retry or recovery behavior.",
    "liability_shift": "Acceptance criteria cover persisted authentication result, ECI/CAVV/3DS metadata, and liability-shift handling.",
    "provider_webhook": "Acceptance criteria cover provider webhook events that complete, fail, or require payment authentication.",
    "saved_payment_method": "Acceptance criteria cover saved cards, setup intents, mandates, off-session, or subscription payment authentication.",
    "test_evidence": "Acceptance criteria or validation commands include automated evidence for challenge, fallback, webhook, and provider test-card paths.",
}
_SUGGESTED_TEST_EVIDENCE: dict[PaymentAuthenticationCategory, str] = {
    "challenge_flow": "Add an integration test for a provider test card that returns requires_action and completes the challenge.",
    "frictionless_flow": "Add a test for an exempt or frictionless authorization that succeeds without rendering a challenge.",
    "failure_fallback": "Add tests for canceled, timed-out, failed, and declined authentication with checkout recovery assertions.",
    "liability_shift": "Assert stored 3DS authentication metadata and liability-shift status on the payment record.",
    "provider_webhook": "Replay provider webhook fixtures for requires_action, succeeded, and failed authentication outcomes.",
    "saved_payment_method": "Test setup-intent, mandate, off-session, or subscription renewal authentication for saved payment methods.",
    "test_evidence": "Run task validation commands that exercise 3DS challenge, fallback, webhook, and saved-payment paths.",
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}"
    r"\b(?:3ds|3[- ]?d secure|three[- ]?d secure|sca|step[- ]?up|requires_action|payment authentication)\b"
    r".{0,120}\b(?:scope|impact|changes?|required|needed|supported|work)\b|"
    r"\b(?:3ds|3[- ]?d secure|three[- ]?d secure|sca|step[- ]?up|requires_action|payment authentication)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|no changes?|excluded)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class Task3dsPaymentAuthenticationReadinessRecord:
    """Readiness guidance for one 3DS/SCA payment authentication task."""

    task_id: str
    title: str
    detected_categories: tuple[PaymentAuthenticationCategory, ...]
    present_acceptance_criteria: tuple[PaymentAuthenticationCategory, ...] = field(default_factory=tuple)
    missing_acceptance_criteria: tuple[PaymentAuthenticationCategory, ...] = field(default_factory=tuple)
    severity: PaymentAuthenticationReadinessSeverity = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    evidence_paths: tuple[str, ...] = field(default_factory=tuple)
    suggested_test_evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def risk_level(self) -> PaymentAuthenticationReadinessSeverity:
        """Compatibility view for callers that expect risk_level."""
        return self.severity

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_categories": list(self.detected_categories),
            "present_acceptance_criteria": list(self.present_acceptance_criteria),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "severity": self.severity,
            "evidence": list(self.evidence),
            "evidence_paths": list(self.evidence_paths),
            "suggested_test_evidence": list(self.suggested_test_evidence),
        }


@dataclass(frozen=True, slots=True)
class Task3dsPaymentAuthenticationReadinessPlan:
    """Plan-level 3DS/SCA payment authentication readiness review."""

    plan_id: str | None = None
    records: tuple[Task3dsPaymentAuthenticationReadinessRecord, ...] = field(default_factory=tuple)
    payment_authentication_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def affected_task_ids(self) -> tuple[str, ...]:
        """Compatibility view for older task readiness callers."""
        return self.payment_authentication_task_ids

    @property
    def not_applicable_task_ids(self) -> tuple[str, ...]:
        """Compatibility view for modules that use not_applicable naming."""
        return self.no_impact_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "payment_authentication_task_ids": list(self.payment_authentication_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render 3DS/SCA payment authentication readiness as deterministic Markdown."""
        title = "# Task 3DS Payment Authentication Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Payment authentication task count: {self.summary.get('payment_authentication_task_count', 0)}",
            f"- Missing acceptance criteria count: {self.summary.get('missing_acceptance_criteria_count', 0)}",
            "- Severity counts: "
            + ", ".join(f"{severity} {severity_counts.get(severity, 0)}" for severity in _SEVERITY_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No 3DS/SCA payment authentication readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Severity | Categories | Present Criteria | Missing Criteria | Evidence Paths | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.severity} | "
                f"{_markdown_cell(', '.join(record.detected_categories) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell(', '.join(record.evidence_paths) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_3ds_payment_authentication_readiness_plan(
    source: Any,
) -> Task3dsPaymentAuthenticationReadinessPlan:
    """Build readiness records for 3DS/SCA payment authentication tasks."""
    if isinstance(source, Task3dsPaymentAuthenticationReadinessPlan):
        return source

    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_SEVERITY_ORDER[record.severity], record.task_id, record.title.casefold()),
        )
    )
    payment_task_ids = tuple(record.task_id for record in records)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return Task3dsPaymentAuthenticationReadinessPlan(
        plan_id=plan_id,
        records=records,
        payment_authentication_task_ids=payment_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_3ds_payment_authentication_readiness(source: Any) -> Task3dsPaymentAuthenticationReadinessPlan:
    """Compatibility alias for building 3DS/SCA payment authentication readiness plans."""
    return build_task_3ds_payment_authentication_readiness_plan(source)


def summarize_task_3ds_payment_authentication_readiness(source: Any) -> Task3dsPaymentAuthenticationReadinessPlan:
    """Compatibility alias for building 3DS/SCA payment authentication readiness plans."""
    return build_task_3ds_payment_authentication_readiness_plan(source)


def extract_task_3ds_payment_authentication_readiness(source: Any) -> Task3dsPaymentAuthenticationReadinessPlan:
    """Compatibility alias for extracting 3DS/SCA payment authentication readiness plans."""
    return build_task_3ds_payment_authentication_readiness_plan(source)


def generate_task_3ds_payment_authentication_readiness(source: Any) -> Task3dsPaymentAuthenticationReadinessPlan:
    """Compatibility alias for generating 3DS/SCA payment authentication readiness plans."""
    return build_task_3ds_payment_authentication_readiness_plan(source)


def task_3ds_payment_authentication_readiness_plan_to_dict(
    result: Task3dsPaymentAuthenticationReadinessPlan,
) -> dict[str, Any]:
    """Serialize a 3DS/SCA payment authentication readiness plan to a plain dictionary."""
    return result.to_dict()


task_3ds_payment_authentication_readiness_plan_to_dict.__test__ = False


def task_3ds_payment_authentication_readiness_plan_to_dicts(
    result: Task3dsPaymentAuthenticationReadinessPlan | Iterable[Task3dsPaymentAuthenticationReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize 3DS/SCA payment authentication readiness records to plain dictionaries."""
    if isinstance(result, Task3dsPaymentAuthenticationReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_3ds_payment_authentication_readiness_plan_to_dicts.__test__ = False


def task_3ds_payment_authentication_readiness_plan_to_markdown(
    result: Task3dsPaymentAuthenticationReadinessPlan,
) -> str:
    """Render a 3DS/SCA payment authentication readiness plan as Markdown."""
    return result.to_markdown()


task_3ds_payment_authentication_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    categories: tuple[PaymentAuthenticationCategory, ...] = field(default_factory=tuple)
    present_criteria: tuple[PaymentAuthenticationCategory, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    evidence_paths: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _task_record(task: Mapping[str, Any], index: int) -> Task3dsPaymentAuthenticationReadinessRecord | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not (set(signals.categories) & _CORE_AUTH_CATEGORIES):
        return None

    required = _required_criteria(signals.categories)
    present = set(signals.present_criteria)
    missing = tuple(category for category in _CATEGORY_ORDER if category in required and category not in present)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return Task3dsPaymentAuthenticationReadinessRecord(
        task_id=task_id,
        title=title,
        detected_categories=signals.categories,
        present_acceptance_criteria=signals.present_criteria,
        missing_acceptance_criteria=missing,
        severity=_severity(set(signals.categories), present, missing),
        evidence=signals.evidence,
        evidence_paths=signals.evidence_paths,
        suggested_test_evidence=tuple(_SUGGESTED_TEST_EVIDENCE[category] for category in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    category_hits: set[PaymentAuthenticationCategory] = set()
    criteria_hits: set[PaymentAuthenticationCategory] = set()
    evidence: list[str] = []
    evidence_paths: list[str] = []
    explicitly_no_impact = False

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for category, pattern in _PATH_CATEGORY_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                category_hits.add(category)
                matched = True
        if matched:
            evidence_paths.append(path)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_categories: set[PaymentAuthenticationCategory] = set()
        for category, pattern in _TEXT_CATEGORY_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                category_hits.add(category)
                matched_categories.add(category)
        if matched_categories:
            evidence.append(_evidence_snippet(source_field, text))
            if _field_is_acceptance_or_validation(source_field):
                criteria_hits.update(matched_categories)

    if category_hits & {"challenge_flow", "provider_webhook", "saved_payment_method"}:
        category_hits.add("challenge_flow")
    return _Signals(
        categories=tuple(category for category in _CATEGORY_ORDER if category in category_hits),
        present_criteria=tuple(category for category in _CATEGORY_ORDER if category in criteria_hits),
        evidence=tuple(_dedupe(evidence)),
        evidence_paths=tuple(_dedupe(evidence_paths)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _required_criteria(
    categories: tuple[PaymentAuthenticationCategory, ...],
) -> set[PaymentAuthenticationCategory]:
    category_set = set(categories)
    required = set(_REQUIRED_BASE)
    if "saved_payment_method" in category_set:
        required.add("saved_payment_method")
    return required


def _severity(
    categories: set[PaymentAuthenticationCategory],
    present: set[PaymentAuthenticationCategory],
    missing: tuple[PaymentAuthenticationCategory, ...],
) -> PaymentAuthenticationReadinessSeverity:
    if not missing:
        return "low"
    missing_set = set(missing)
    if {"challenge_flow", "failure_fallback", "provider_webhook"} <= missing_set:
        return "high"
    if "challenge_flow" in categories and {"challenge_flow", "failure_fallback"} <= missing_set:
        return "high"
    if "saved_payment_method" in categories and {"provider_webhook", "saved_payment_method"} <= missing_set:
        return "high"
    if len(missing) >= 4:
        return "high"
    if present:
        return "medium"
    return "medium"


def _summary(
    records: tuple[Task3dsPaymentAuthenticationReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "payment_authentication_task_count": len(records),
        "no_impact_task_ids": list(no_impact_task_ids),
        "missing_acceptance_criteria_count": sum(len(record.missing_acceptance_criteria) for record in records),
        "severity_counts": {
            severity: sum(1 for record in records if record.severity == severity) for severity in _SEVERITY_ORDER
        },
        "category_counts": {
            category: sum(1 for record in records if category in record.detected_categories)
            for category in _CATEGORY_ORDER
        },
        "present_acceptance_criteria_counts": {
            category: sum(1 for record in records if category in record.present_acceptance_criteria)
            for category in _CATEGORY_ORDER
        },
        "missing_acceptance_criteria_counts": {
            category: sum(1 for record in records if category in record.missing_acceptance_criteria)
            for category in _CATEGORY_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_category(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_category(key_text):
                    texts.append((field, f"{key_text}: {text}"))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _metadata_key_is_category(value: str) -> bool:
    return any(pattern.search(value) for pattern in _TEXT_CATEGORY_PATTERNS.values())


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value))
            else:
                commands.extend(_strings(metadata_value))
    return tuple(("validation_commands", command) for command in _dedupe(commands))


def _field_is_acceptance_or_validation(source_field: str) -> bool:
    return (
        source_field.startswith("acceptance_criteria")
        or source_field.startswith("validation")
        or source_field.startswith("test_command")
        or ".validation" in source_field
        or ".test_command" in source_field
        or ".test_commands" in source_field
    )


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


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


def _normalized_path(value: str) -> str:
    return str(PurePosixPath(value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")))


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


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


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


Task3dsPaymentAuthenticationReadinessSeverity = PaymentAuthenticationReadinessSeverity

__all__ = [
    "PaymentAuthenticationCategory",
    "PaymentAuthenticationReadinessSeverity",
    "Task3dsPaymentAuthenticationReadinessPlan",
    "Task3dsPaymentAuthenticationReadinessRecord",
    "Task3dsPaymentAuthenticationReadinessSeverity",
    "analyze_task_3ds_payment_authentication_readiness",
    "build_task_3ds_payment_authentication_readiness_plan",
    "extract_task_3ds_payment_authentication_readiness",
    "generate_task_3ds_payment_authentication_readiness",
    "summarize_task_3ds_payment_authentication_readiness",
    "task_3ds_payment_authentication_readiness_plan_to_dict",
    "task_3ds_payment_authentication_readiness_plan_to_dicts",
    "task_3ds_payment_authentication_readiness_plan_to_markdown",
]
