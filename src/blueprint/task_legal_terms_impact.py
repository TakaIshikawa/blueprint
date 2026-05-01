"""Plan legal terms and consent review needs for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


LegalSurface = Literal[
    "terms_of_service",
    "privacy_policy",
    "cookie_consent",
    "marketing_consent",
    "subscription_terms",
    "refund_policy",
    "payment_terms",
    "data_processing_agreement",
    "service_level_agreement",
    "age_restrictions",
    "regulated_claims",
]
LegalImpactLevel = Literal["critical", "high", "medium", "none"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: dict[LegalSurface, int] = {
    "regulated_claims": 0,
    "age_restrictions": 1,
    "data_processing_agreement": 2,
    "privacy_policy": 3,
    "terms_of_service": 4,
    "subscription_terms": 5,
    "refund_policy": 6,
    "payment_terms": 7,
    "service_level_agreement": 8,
    "cookie_consent": 9,
    "marketing_consent": 10,
}
_IMPACT_ORDER: dict[LegalImpactLevel, int] = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "none": 3,
}
_TEXT_SURFACE_PATTERNS: dict[LegalSurface, re.Pattern[str]] = {
    "terms_of_service": re.compile(
        r"\b(?:terms of service|terms of use|user agreement|customer agreement|"
        r"legal terms|terms acceptance|accept(?:ed|ing)? terms)\b",
        re.I,
    ),
    "privacy_policy": re.compile(
        r"\b(?:privacy policy|privacy notice|privacy statement|privacy disclosure|"
        r"notice of privacy|data privacy notice)\b",
        re.I,
    ),
    "cookie_consent": re.compile(
        r"\b(?:cookie consent|cookie banner|cookie notice|cookie preference|"
        r"cookie opt[- ]?(?:in|out)|tracking consent|analytics consent)\b",
        re.I,
    ),
    "marketing_consent": re.compile(
        r"\b(?:marketing consent|marketing opt[- ]?(?:in|out)|email consent|"
        r"sms consent|unsubscribe|preference center|promotional consent|"
        r"communications preference)\b",
        re.I,
    ),
    "subscription_terms": re.compile(
        r"\b(?:subscription terms?|renewal terms?|auto[- ]?renewal|cancellation terms?|"
        r"trial terms?|billing cycle|plan downgrade|plan cancellation)\b",
        re.I,
    ),
    "refund_policy": re.compile(
        r"\b(?:refund policy|refund terms?|refund eligibility|returns policy|"
        r"chargeback policy|cancellation refund|money[- ]?back)\b",
        re.I,
    ),
    "payment_terms": re.compile(
        r"\b(?:payment terms?|checkout terms?|billing terms?|invoice terms?|"
        r"late fee|card payment agreement|tax disclosure|payment authorization)\b",
        re.I,
    ),
    "data_processing_agreement": re.compile(
        r"\b(?:data processing agreement|dpa|data processor|subprocessor|"
        r"controller processor|standard contractual clauses|sccs?|data transfer addendum)\b",
        re.I,
    ),
    "service_level_agreement": re.compile(
        r"\b(?:service level agreement|sla|uptime commitment|availability commitment|"
        r"service credits?|support credits?|credit for downtime)\b",
        re.I,
    ),
    "age_restrictions": re.compile(
        r"\b(?:age restriction|age gate|age verification|minimum age|minor consent|"
        r"parental consent|children'?s privacy|coppa|under 13|under thirteen)\b",
        re.I,
    ),
    "regulated_claims": re.compile(
        r"\b(?:regulated claim|medical claim|health claim|financial advice|investment advice|"
        r"guaranteed return|earnings claim|therapeutic claim|diagnos(?:e|is|tic)|"
        r"cures?|treats?|fda claim|sec disclosure)\b",
        re.I,
    ),
}
_PATH_SURFACE_PATTERNS: dict[LegalSurface, re.Pattern[str]] = {
    "terms_of_service": re.compile(
        r"(?:^|/)[^/]*(?:terms|tos|terms-of-service|terms_of_service)[^/]*(?:/|\.|$)", re.I
    ),
    "privacy_policy": re.compile(
        r"(?:^|/)[^/]*(?:privacy-policy|privacy_policy|privacy-notice|privacy_notice)[^/]*(?:/|\.|$)",
        re.I,
    ),
    "cookie_consent": re.compile(
        r"(?:^|/)[^/]*(?:cookies?|cookie[_-]?consent|consent[_-]?banner)[^/]*(?:/|\.|$)", re.I
    ),
    "marketing_consent": re.compile(
        r"(?:^|/)[^/]*(?:marketing[_-]?consent|preference[_-]?center|unsubscribe)[^/]*(?:/|\.|$)",
        re.I,
    ),
    "subscription_terms": re.compile(
        r"(?:^|/)[^/]*(?:subscriptions?|billing[_-]?plans?|renewals?)[^/]*(?:/|\.|$)", re.I
    ),
    "refund_policy": re.compile(r"(?:^|/)(?:refunds?|returns?|chargebacks?)(?:/|\.|$)", re.I),
    "payment_terms": re.compile(r"(?:^|/)(?:payments?|checkout|invoices?|tax)(?:/|\.|$)", re.I),
    "data_processing_agreement": re.compile(
        r"(?:^|/)(?:dpa|data-processing|subprocessors?)(?:/|\.|$)", re.I
    ),
    "service_level_agreement": re.compile(
        r"(?:^|/)(?:sla|service-level|service-credits?|uptime)(?:/|\.|$)", re.I
    ),
    "age_restrictions": re.compile(
        r"(?:^|/)(?:age-gate|age-verification|minors?|parental-consent)(?:/|\.|$)", re.I
    ),
    "regulated_claims": re.compile(
        r"(?:^|/)(?:claims?|regulated|medical|financial-advice|disclosures?)(?:/|\.|$)", re.I
    ),
}
_DOC_TEST_PATH_RE = re.compile(
    r"(?:^|/)(?:docs?|documentation|tests?|spec|specs|fixtures?)(?:/|$)|"
    r"(?:^|/)(?:README|CHANGELOG|CONTRIBUTING)(?:\.[^/]*)?$|(?:_test|\.test|\.spec)\.",
    re.I,
)
_LEGAL_DOC_PATH_RE = re.compile(
    r"(?:^|/)(?:legal|policies?|terms|privacy|dpa|sla|consent|compliance)(?:/|$)",
    re.I,
)
_ACTION_RE = re.compile(
    r"\b(?:add|adds|adding|change|changes|update|updates|revise|require|requires|"
    r"collect|capture|enforce|launch|publish|display|gate|accept|consent|enable|disable)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskLegalTermsImpactRecord:
    """Legal terms and consent review guidance for one execution task."""

    task_id: str
    title: str
    impact_level: LegalImpactLevel
    legal_surfaces: tuple[LegalSurface, ...] = field(default_factory=tuple)
    required_reviewers: tuple[str, ...] = field(default_factory=tuple)
    required_artifacts: tuple[str, ...] = field(default_factory=tuple)
    safeguards: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "impact_level": self.impact_level,
            "legal_surfaces": list(self.legal_surfaces),
            "required_reviewers": list(self.required_reviewers),
            "required_artifacts": list(self.required_artifacts),
            "safeguards": list(self.safeguards),
            "follow_up_questions": list(self.follow_up_questions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskLegalTermsImpactPlan:
    """Plan-level legal terms and consent impact review."""

    plan_id: str | None = None
    records: tuple[TaskLegalTermsImpactRecord, ...] = field(default_factory=tuple)
    legal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "legal_task_ids": list(self.legal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return legal impact records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the legal terms impact plan as deterministic Markdown."""
        title = "# Task Legal Terms Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No execution tasks were available for legal terms impact planning."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Impact | Legal Surfaces | Reviewers | Artifacts | Follow-up Questions |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.impact_level} | "
                f"{_markdown_cell(', '.join(record.legal_surfaces) or 'none')} | "
                f"{_markdown_cell('; '.join(record.required_reviewers) or 'none')} | "
                f"{_markdown_cell('; '.join(record.required_artifacts) or 'none')} | "
                f"{_markdown_cell('; '.join(record.follow_up_questions) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_legal_terms_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskLegalTermsImpactPlan:
    """Build legal terms and consent review guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (_task_record(task, index) for index, task in enumerate(tasks, start=1)),
            key=lambda record: (
                _IMPACT_ORDER[record.impact_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    legal_task_ids = tuple(record.task_id for record in records if record.impact_level != "none")
    impact_counts = {
        impact: sum(1 for record in records if record.impact_level == impact)
        for impact in _IMPACT_ORDER
    }
    surface_counts = {
        surface: sum(1 for record in records if surface in record.legal_surfaces)
        for surface in _SURFACE_ORDER
    }
    return TaskLegalTermsImpactPlan(
        plan_id=plan_id,
        records=records,
        legal_task_ids=legal_task_ids,
        summary={
            "task_count": len(tasks),
            "legal_task_count": len(legal_task_ids),
            "impact_counts": impact_counts,
            "surface_counts": surface_counts,
        },
    )


def analyze_task_legal_terms_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskLegalTermsImpactPlan:
    """Compatibility alias for building legal terms impact plans."""
    return build_task_legal_terms_impact_plan(source)


def summarize_task_legal_terms_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskLegalTermsImpactPlan:
    """Compatibility alias for building legal terms impact plans."""
    return build_task_legal_terms_impact_plan(source)


def task_legal_terms_impact_plan_to_dict(result: TaskLegalTermsImpactPlan) -> dict[str, Any]:
    """Serialize a task legal terms impact plan to a plain dictionary."""
    return result.to_dict()


task_legal_terms_impact_plan_to_dict.__test__ = False


def task_legal_terms_impact_plan_to_markdown(result: TaskLegalTermsImpactPlan) -> str:
    """Render a task legal terms impact plan as Markdown."""
    return result.to_markdown()


task_legal_terms_impact_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[LegalSurface, ...] = field(default_factory=tuple)
    surface_evidence: tuple[str, ...] = field(default_factory=tuple)
    action_evidence: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskLegalTermsImpactRecord:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    impact = _impact_level(signals, task)
    surfaces = signals.surfaces if impact != "none" else ()
    return TaskLegalTermsImpactRecord(
        task_id=task_id,
        title=title,
        impact_level=impact,
        legal_surfaces=surfaces,
        required_reviewers=_required_reviewers(surfaces),
        required_artifacts=_required_artifacts(surfaces),
        safeguards=_safeguards(surfaces, impact),
        follow_up_questions=_follow_up_questions(surfaces, impact),
        evidence=signals.evidence if impact != "none" else (),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surfaces: set[LegalSurface] = set()
    surface_evidence: list[str] = []
    action_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_evidence = f"files_or_modules: {path}"
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for surface, pattern in _PATH_SURFACE_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(path_text):
                surfaces.add(surface)
                surface_evidence.append(path_evidence)

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        for surface, pattern in _TEXT_SURFACE_PATTERNS.items():
            if pattern.search(text):
                surfaces.add(surface)
                surface_evidence.append(snippet)
        if _ACTION_RE.search(text):
            action_evidence.append(snippet)

    ordered_surfaces = tuple(surface for surface in _SURFACE_ORDER if surface in surfaces)
    evidence = tuple(_dedupe([*surface_evidence, *action_evidence]))
    return _Signals(
        surfaces=ordered_surfaces,
        surface_evidence=tuple(_dedupe(surface_evidence)),
        action_evidence=tuple(_dedupe(action_evidence)),
        evidence=evidence,
    )


def _impact_level(signals: _Signals, task: Mapping[str, Any]) -> LegalImpactLevel:
    if not signals.surfaces:
        return "none"
    if _is_unrelated_doc_or_test_task(task, signals):
        return "none"
    if any(
        surface in signals.surfaces
        for surface in ("regulated_claims", "age_restrictions", "data_processing_agreement")
    ):
        return "critical"
    if any(
        surface in signals.surfaces
        for surface in (
            "terms_of_service",
            "privacy_policy",
            "subscription_terms",
            "refund_policy",
            "payment_terms",
            "service_level_agreement",
        )
    ):
        return "high"
    return "medium"


def _is_unrelated_doc_or_test_task(task: Mapping[str, Any], signals: _Signals) -> bool:
    paths = _strings(task.get("files_or_modules") or task.get("files"))
    if not paths:
        return False
    if not all(_DOC_TEST_PATH_RE.search(_normalized_path(path)) for path in paths):
        return False
    if any(_LEGAL_DOC_PATH_RE.search(_normalized_path(path)) for path in paths):
        return False
    text = " ".join(text for _, text in _candidate_texts(task))
    return not _ACTION_RE.search(text) and not any(
        surface in signals.surfaces
        for surface in (
            "regulated_claims",
            "age_restrictions",
            "data_processing_agreement",
            "terms_of_service",
            "privacy_policy",
        )
    )


def _required_reviewers(surfaces: tuple[LegalSurface, ...]) -> tuple[str, ...]:
    reviewers: list[str] = []
    if surfaces:
        reviewers.append("legal counsel")
        reviewers.append("product owner")
    if any(
        surface in surfaces
        for surface in (
            "privacy_policy",
            "cookie_consent",
            "marketing_consent",
            "data_processing_agreement",
            "age_restrictions",
        )
    ):
        reviewers.append("privacy counsel")
    if any(
        surface in surfaces
        for surface in (
            "subscription_terms",
            "refund_policy",
            "payment_terms",
            "service_level_agreement",
        )
    ):
        reviewers.append("finance or revenue operations")
    if "regulated_claims" in surfaces:
        reviewers.append("compliance reviewer")
    if "data_processing_agreement" in surfaces:
        reviewers.append("security reviewer")
    if any(surface in surfaces for surface in ("refund_policy", "service_level_agreement")):
        reviewers.append("support owner")
    return tuple(_dedupe(reviewers))


def _required_artifacts(surfaces: tuple[LegalSurface, ...]) -> tuple[str, ...]:
    artifacts: list[str] = []
    if "terms_of_service" in surfaces:
        artifacts.append("Updated terms of service or terms acceptance copy with effective date.")
    if "privacy_policy" in surfaces:
        artifacts.append("Updated privacy policy or privacy notice with data-use disclosure.")
    if "cookie_consent" in surfaces:
        artifacts.append("Cookie notice, consent banner copy, and preference-state mapping.")
    if "marketing_consent" in surfaces:
        artifacts.append(
            "Marketing consent language, unsubscribe path, and preference center behavior."
        )
    if "subscription_terms" in surfaces:
        artifacts.append("Subscription, renewal, cancellation, and trial terms.")
    if "refund_policy" in surfaces:
        artifacts.append("Refund or chargeback policy with eligibility and timing rules.")
    if "payment_terms" in surfaces:
        artifacts.append("Payment authorization, invoice, tax, and checkout terms.")
    if "data_processing_agreement" in surfaces:
        artifacts.append("DPA or data transfer addendum with subprocessors and transfer basis.")
    if "service_level_agreement" in surfaces:
        artifacts.append(
            "SLA, service credit, support commitment, and uptime measurement language."
        )
    if "age_restrictions" in surfaces:
        artifacts.append("Age gate, parental consent, and minor-data handling rules.")
    if "regulated_claims" in surfaces:
        artifacts.append("Approved regulated-claim substantiation and required disclaimers.")
    return tuple(_dedupe(artifacts))


def _safeguards(surfaces: tuple[LegalSurface, ...], impact: LegalImpactLevel) -> tuple[str, ...]:
    if impact == "none":
        return ()
    safeguards = [
        "Block launch until required legal copy, consent behavior, and owner sign-off are recorded.",
        "Capture evidence of the exact user-facing copy, version, locale, and effective date reviewed.",
    ]
    if impact == "critical":
        safeguards.append(
            "Require explicit legal approval before release because the task affects regulated claims, minors, or data-processing contracts."
        )
    if any(surface in surfaces for surface in ("cookie_consent", "marketing_consent")):
        safeguards.append("Verify opt-in, opt-out, withdrawal, and preference persistence paths.")
    if any(
        surface in surfaces
        for surface in (
            "subscription_terms",
            "refund_policy",
            "payment_terms",
            "service_level_agreement",
        )
    ):
        safeguards.append(
            "Confirm billing, credit, refund, and support obligations match operational capabilities."
        )
    if "data_processing_agreement" in surfaces:
        safeguards.append(
            "Confirm subprocessors, transfer mechanisms, and processor obligations are current."
        )
    if "regulated_claims" in surfaces:
        safeguards.append(
            "Keep substantiation and disclaimer evidence linked to every regulated claim."
        )
    return tuple(_dedupe(safeguards))


def _follow_up_questions(
    surfaces: tuple[LegalSurface, ...], impact: LegalImpactLevel
) -> tuple[str, ...]:
    if impact == "none":
        return ()
    questions = [
        "Who owns final legal approval and where will the approval evidence be stored?",
        "Which locales, customer segments, or contract templates need matching wording?",
    ]
    if any(surface in surfaces for surface in ("terms_of_service", "privacy_policy")):
        questions.append(
            "Does the change require customer notice, re-acceptance, or an effective-date window?"
        )
    if any(surface in surfaces for surface in ("cookie_consent", "marketing_consent")):
        questions.append(
            "How are consent withdrawal, audit history, and existing preferences preserved?"
        )
    if any(
        surface in surfaces for surface in ("subscription_terms", "refund_policy", "payment_terms")
    ):
        questions.append(
            "Which billing states, renewals, cancellations, refunds, taxes, or invoices are affected?"
        )
    if "service_level_agreement" in surfaces:
        questions.append(
            "How are uptime, service credits, exclusions, and support response times measured?"
        )
    if "data_processing_agreement" in surfaces:
        questions.append("Do subprocessors, transfer regions, or data-processing purposes change?")
    if "age_restrictions" in surfaces:
        questions.append(
            "What minimum-age, parental-consent, and minor-data rules apply in each market?"
        )
    if "regulated_claims" in surfaces:
        questions.append(
            "What evidence substantiates each regulated claim and which disclaimer must appear?"
        )
    return tuple(_dedupe(questions))


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes"):
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in _TEXT_SURFACE_PATTERNS.values()):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in _TEXT_SURFACE_PATTERNS.values()):
                texts.append((field, str(key)))
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
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


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
    "LegalImpactLevel",
    "LegalSurface",
    "TaskLegalTermsImpactPlan",
    "TaskLegalTermsImpactRecord",
    "analyze_task_legal_terms_impact",
    "build_task_legal_terms_impact_plan",
    "summarize_task_legal_terms_impact",
    "task_legal_terms_impact_plan_to_dict",
    "task_legal_terms_impact_plan_to_markdown",
]
