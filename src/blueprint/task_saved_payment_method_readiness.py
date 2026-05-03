"""Plan saved payment method readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


SavedPaymentMethodScenario = Literal[
    "saved_card",
    "payment_method_update",
    "wallet_update",
    "default_payment_instrument",
    "card_vaulting",
    "tokenization",
    "provider_reference",
    "customer_payment_update",
    "payment_method_deletion",
    "retry_flow",
]
SavedPaymentMethodSafeguard = Literal[
    "provider_tokenization",
    "pci_scope_avoidance",
    "default_method_audit_trail",
    "customer_notification",
    "retry_idempotency_behavior",
    "sca_step_up_handling",
    "deletion_semantics",
    "support_visibility",
    "test_coverage",
]
SavedPaymentMethodRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SCENARIO_ORDER: tuple[SavedPaymentMethodScenario, ...] = (
    "saved_card",
    "payment_method_update",
    "wallet_update",
    "default_payment_instrument",
    "card_vaulting",
    "tokenization",
    "provider_reference",
    "customer_payment_update",
    "payment_method_deletion",
    "retry_flow",
)
_SAFEGUARD_ORDER: tuple[SavedPaymentMethodSafeguard, ...] = (
    "provider_tokenization",
    "pci_scope_avoidance",
    "default_method_audit_trail",
    "customer_notification",
    "retry_idempotency_behavior",
    "sca_step_up_handling",
    "deletion_semantics",
    "support_visibility",
    "test_coverage",
)
_RISK_ORDER: dict[SavedPaymentMethodRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}

_PATH_SCENARIO_PATTERNS: dict[SavedPaymentMethodScenario, tuple[re.Pattern[str], ...]] = {
    "saved_card": (
        re.compile(r"(?:^|/)(?:saved[-_]?cards?|cards?)(?:/|$)|saved[-_]?cards?|card[-_]?on[-_]?file", re.I),
    ),
    "payment_method_update": (
        re.compile(r"(?:^|/)(?:payment[-_]?methods?|billing[-_]?methods?)(?:/|$)|payment[-_]?method[-_]?update", re.I),
    ),
    "wallet_update": (
        re.compile(r"(?:^|/)(?:wallets?|apple[-_]?pay|google[-_]?pay|paypal)(?:/|$)|wallet[-_]?update", re.I),
    ),
    "default_payment_instrument": (
        re.compile(r"default[-_]?(?:payment[-_]?)?(?:method|instrument|card)|primary[-_](?:card|payment)", re.I),
    ),
    "card_vaulting": (
        re.compile(r"(?:^|/)(?:vault|vaulting|card[-_]?vault)(?:/|$)|card[-_]?vault|vaulted[-_]?card", re.I),
    ),
    "tokenization": (
        re.compile(r"(?:^|/)(?:tokens?|tokenization|tokenisation|setup[-_]?intent)(?:/|$)|payment[-_]?token|card[-_]?token", re.I),
    ),
    "provider_reference": (
        re.compile(r"(?:^|/)(?:stripe|adyen|braintree|checkout|cybersource|authorize[-_]?net|spreedly)(?:/|$)", re.I),
    ),
    "customer_payment_update": (
        re.compile(r"customer[-_]?(?:payment|billing).*(?:update|edit)|billing[-_]?profile.*payment", re.I),
    ),
    "payment_method_deletion": (
        re.compile(r"(?:delete|remove|detach|archive)[-_]?(?:payment[-_]?)?(?:method|card|wallet)|payment[-_]?method[-_]?deletion", re.I),
    ),
    "retry_flow": (
        re.compile(r"(?:payment|invoice|charge)[-_]?retr(?:y|ies)|dunning|retry[-_]?(?:payment|charge|invoice)", re.I),
    ),
}
_TEXT_SCENARIO_PATTERNS: dict[SavedPaymentMethodScenario, tuple[re.Pattern[str], ...]] = {
    "saved_card": (
        re.compile(r"\b(?:saved cards?|saved payment methods?|card on file|cards? on file|stored cards?|stored payment method)\b", re.I),
    ),
    "payment_method_update": (
        re.compile(r"\b(?:payment method update|update payment methods?|edit payment methods?|billing methods?|add payment methods?|replace payment methods?|payment methods?)\b", re.I),
    ),
    "wallet_update": (
        re.compile(r"\b(?:wallet update|wallets?|digital wallet|apple pay|google pay|paypal wallet|wallet payment methods?)\b", re.I),
    ),
    "default_payment_instrument": (
        re.compile(r"\b(?:default payment method|default card|default payment instrument|primary payment method|primary card|set as default)\b", re.I),
    ),
    "card_vaulting": (
        re.compile(r"\b(?:card vault(?:ing)?|vaulted cards?|vault payment method|store cards?|save cards?|persist card|card storage)\b", re.I),
    ),
    "tokenization": (
        re.compile(r"\b(?:tokenization|tokenisation|payment token|card token|network token|setup intent|payment method token|tokenized card)\b", re.I),
    ),
    "provider_reference": (
        re.compile(r"\b(?:stripe|adyen|braintree|checkout\.com|cybersource|authorize\.net|spreedly|payment provider|processor reference|provider payment method id)\b", re.I),
    ),
    "customer_payment_update": (
        re.compile(r"\b(?:customer payment update|customer billing update|customers? (?:can )?(?:save|update|replace|edit)s? (?:cards?|payment methods?|billing methods?)|customer updates? payment|billing profile payment|customer portal payment)\b", re.I),
    ),
    "payment_method_deletion": (
        re.compile(r"\b(?:delete payment method|remove payment method|detach payment method|delete card|remove card|delete wallet|remove wallet|deletion semantics)\b", re.I),
    ),
    "retry_flow": (
        re.compile(r"\b(?:payment retry|retry payment|charge retry|invoice retry|retry failed payment|dunning|retry flow|retry schedule|retry idempotency|idempotent retry)\b", re.I),
    ),
}
_SAFEGUARD_PATTERNS: dict[SavedPaymentMethodSafeguard, tuple[re.Pattern[str], ...]] = {
    "provider_tokenization": (
        re.compile(r"\b(?:provider tokenization|provider tokenisation|tokenize with provider|tokenized by (?:stripe|adyen|braintree)|setup intent|payment method token|processor token|network token|vault token)\b", re.I),
    ),
    "pci_scope_avoidance": (
        re.compile(r"\b(?:pci scope avoidance|avoid pci scope|pci[- ]?compliant hosted fields|hosted fields|payment elements|never store pan|do not store card numbers?|no raw card data|sensitive authentication data|saq[- ]?a)\b", re.I),
    ),
    "default_method_audit_trail": (
        re.compile(r"\b(?:default method audit|default payment audit|audit trail|audit log|audit event|record actor|previous default|new default|who changed default)\b", re.I),
    ),
    "customer_notification": (
        re.compile(r"\b(?:customer notification|notify customer|email notification|receipt email|billing email|in-app notification|webhook notification|confirmation email)\b", re.I),
    ),
    "retry_idempotency_behavior": (
        re.compile(r"\b(?:retry idempotency|idempotency key|idempotent retry|duplicate charge prevention|dedupe retries|safe retry|retry behavior|retry semantics|at[- ]most[- ]once)\b", re.I),
    ),
    "sca_step_up_handling": (
        re.compile(r"\b(?:sca|strong customer authentication|3d secure|3ds|step[- ]up|challenge flow|requires action|payment authentication|mandate authentication)\b", re.I),
    ),
    "deletion_semantics": (
        re.compile(r"\b(?:deletion semantics|detach semantics|soft delete|hard delete|delete behavior|cannot delete default|remove default first|retain billing history|archive payment method)\b", re.I),
    ),
    "support_visibility": (
        re.compile(r"\b(?:support visibility|support view|agent visibility|support console|admin console|customer support can see|last4|card brand|payment method status|billing support)\b", re.I),
    ),
    "test_coverage": (
        re.compile(r"\b(?:test coverage|unit tests?|integration tests?|e2e tests?|end[- ]to[- ]end tests?|payment method tests?|card vault tests?|wallet tests?|retry tests?|sca tests?|pci tests?|test[_ -].*(?:payment|card|wallet|retry|sca|pci)|(?:payment|card|wallet|retry|sca|pci).*tests?)\b", re.I),
    ),
}
_RECOMMENDED_CHECKS: dict[SavedPaymentMethodSafeguard, str] = {
    "provider_tokenization": "Use provider tokenization so application code stores provider payment method identifiers rather than card numbers.",
    "pci_scope_avoidance": "Document PCI scope boundaries and confirm raw PAN, CVV, and sensitive authentication data never touch application storage or logs.",
    "default_method_audit_trail": "Record actor, previous value, new default payment instrument, timestamp, and source for every default method change.",
    "customer_notification": "Notify customers when saved payment methods are added, removed, replaced, set as default, retried, or require action.",
    "retry_idempotency_behavior": "Define idempotency keys, duplicate charge prevention, retry windows, and failure semantics for payment retries.",
    "sca_step_up_handling": "Handle SCA, 3DS, setup-intent requires_action states, and step-up recovery before saving or reusing payment methods.",
    "deletion_semantics": "Specify detach, archive, default replacement, active subscription, invoice history, and provider-side deletion behavior.",
    "support_visibility": "Expose safe support visibility such as brand, last4, status, default flag, provider reference, and recent payment method events.",
    "test_coverage": "Cover saved card, wallet, vaulting, tokenization, default change, deletion, retry, SCA, notification, and PCI boundary tests.",
}


@dataclass(frozen=True, slots=True)
class TaskSavedPaymentMethodReadinessRecord:
    """Saved payment method readiness guidance for one execution task."""

    task_id: str
    title: str
    payment_method_scenarios: tuple[SavedPaymentMethodScenario, ...]
    present_safeguards: tuple[SavedPaymentMethodSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[SavedPaymentMethodSafeguard, ...] = field(default_factory=tuple)
    risk_level: SavedPaymentMethodRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "payment_method_scenarios": list(self.payment_method_scenarios),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskSavedPaymentMethodReadinessPlan:
    """Plan-level saved payment method readiness review."""

    plan_id: str | None = None
    records: tuple[TaskSavedPaymentMethodReadinessRecord, ...] = field(default_factory=tuple)
    saved_payment_method_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskSavedPaymentMethodReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "saved_payment_method_task_ids": list(self.saved_payment_method_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render saved payment method readiness as deterministic Markdown."""
        title = "# Task Saved Payment Method Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Saved payment method task count: {self.summary.get('saved_payment_method_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No saved payment method readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Scenarios | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.payment_method_scenarios) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_saved_payment_method_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSavedPaymentMethodReadinessPlan:
    """Build readiness records for tasks that implement saved payment method flows."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    saved_payment_method_task_ids = tuple(record.task_id for record in records)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskSavedPaymentMethodReadinessPlan(
        plan_id=plan_id,
        records=records,
        saved_payment_method_task_ids=saved_payment_method_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_saved_payment_method_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSavedPaymentMethodReadinessPlan:
    """Compatibility alias for building saved payment method readiness plans."""
    return build_task_saved_payment_method_readiness_plan(source)


def summarize_task_saved_payment_method_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSavedPaymentMethodReadinessPlan:
    """Compatibility alias for building saved payment method readiness plans."""
    return build_task_saved_payment_method_readiness_plan(source)


def extract_task_saved_payment_method_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSavedPaymentMethodReadinessPlan:
    """Compatibility alias for building saved payment method readiness plans."""
    return build_task_saved_payment_method_readiness_plan(source)


def generate_task_saved_payment_method_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSavedPaymentMethodReadinessPlan:
    """Compatibility alias for generating saved payment method readiness plans."""
    return build_task_saved_payment_method_readiness_plan(source)


def recommend_task_saved_payment_method_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSavedPaymentMethodReadinessPlan:
    """Compatibility alias for recommending saved payment method readiness plans."""
    return build_task_saved_payment_method_readiness_plan(source)


def task_saved_payment_method_readiness_plan_to_dict(
    result: TaskSavedPaymentMethodReadinessPlan,
) -> dict[str, Any]:
    """Serialize a saved payment method readiness plan to a plain dictionary."""
    return result.to_dict()


task_saved_payment_method_readiness_plan_to_dict.__test__ = False


def task_saved_payment_method_readiness_plan_to_dicts(
    result: TaskSavedPaymentMethodReadinessPlan | Iterable[TaskSavedPaymentMethodReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize saved payment method readiness records to plain dictionaries."""
    if isinstance(result, TaskSavedPaymentMethodReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_saved_payment_method_readiness_plan_to_dicts.__test__ = False


def task_saved_payment_method_readiness_plan_to_markdown(
    result: TaskSavedPaymentMethodReadinessPlan,
) -> str:
    """Render a saved payment method readiness plan as Markdown."""
    return result.to_markdown()


task_saved_payment_method_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    scenarios: tuple[SavedPaymentMethodScenario, ...] = field(default_factory=tuple)
    scenario_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[SavedPaymentMethodSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskSavedPaymentMethodReadinessRecord | None:
    signals = _signals(task)
    if not signals.scenarios:
        return None

    missing = _missing_safeguards(signals.scenarios, signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskSavedPaymentMethodReadinessRecord(
        task_id=task_id,
        title=title,
        payment_method_scenarios=signals.scenarios,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals.scenarios, missing),
        evidence=tuple(_dedupe([*signals.scenario_evidence, *signals.safeguard_evidence])),
        recommended_checks=tuple(_RECOMMENDED_CHECKS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    scenario_hits: set[SavedPaymentMethodScenario] = set()
    safeguard_hits: set[SavedPaymentMethodSafeguard] = set()
    scenario_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_scenarios = _path_scenarios(normalized)
        if path_scenarios:
            scenario_hits.update(path_scenarios)
            scenario_evidence.append(f"files_or_modules: {path}")
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(searchable) or pattern.search(normalized) for pattern in patterns):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_scenario = False
        for scenario, patterns in _TEXT_SCENARIO_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                scenario_hits.add(scenario)
                matched_scenario = True
        if matched_scenario:
            scenario_evidence.append(snippet)
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    for command in _validation_commands(task):
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_scenario = False
        for scenario, patterns in _TEXT_SCENARIO_PATTERNS.items():
            if any(pattern.search(command) or pattern.search(command_text) for pattern in patterns):
                scenario_hits.add(scenario)
                matched_scenario = True
        for scenario, patterns in _PATH_SCENARIO_PATTERNS.items():
            if any(pattern.search(command) or pattern.search(command_text) for pattern in patterns):
                scenario_hits.add(scenario)
                matched_scenario = True
        if matched_scenario:
            scenario_evidence.append(snippet)
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(command) or pattern.search(command_text) for pattern in patterns):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    return _Signals(
        scenarios=tuple(scenario for scenario in _SCENARIO_ORDER if scenario in scenario_hits),
        scenario_evidence=tuple(_dedupe(scenario_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_scenarios(path: str) -> set[SavedPaymentMethodScenario]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    scenarios: set[SavedPaymentMethodScenario] = set()
    for scenario, patterns in _PATH_SCENARIO_PATTERNS.items():
        if any(pattern.search(normalized) or pattern.search(text) for pattern in patterns):
            scenarios.add(scenario)
    name = PurePosixPath(normalized).name
    if re.search(r"\b(?:saved cards?|card on file|stored cards?)\b", text) or name.startswith("card"):
        scenarios.add("saved_card")
    if re.search(r"\b(?:payment methods?|billing methods?)\b", text):
        scenarios.add("payment_method_update")
    if re.search(r"\b(?:wallets?|apple pay|google pay|paypal)\b", text):
        scenarios.add("wallet_update")
    if re.search(r"\b(?:default|primary)\b", text) and re.search(r"\b(?:payment|method|instrument|card)\b", text):
        scenarios.add("default_payment_instrument")
    if re.search(r"\b(?:vault|vaulting|vaulted)\b", text):
        scenarios.add("card_vaulting")
    if re.search(r"\b(?:token|tokenization|tokenisation|setup intent)\b", text):
        scenarios.add("tokenization")
    if re.search(r"\b(?:stripe|adyen|braintree|cybersource|spreedly|provider|processor)\b", text):
        scenarios.add("provider_reference")
    if re.search(r"\b(?:delete|remove|detach|archive)\b", text) and re.search(r"\b(?:payment|method|card|wallet)\b", text):
        scenarios.add("payment_method_deletion")
    if re.search(r"\b(?:retry|dunning)\b", text) and re.search(r"\b(?:payment|charge|invoice)\b", text):
        scenarios.add("retry_flow")
    return scenarios


def _missing_safeguards(
    scenarios: tuple[SavedPaymentMethodScenario, ...],
    present: tuple[SavedPaymentMethodSafeguard, ...],
) -> tuple[SavedPaymentMethodSafeguard, ...]:
    scenario_set = set(scenarios)
    required: set[SavedPaymentMethodSafeguard] = {"customer_notification", "support_visibility", "test_coverage"}
    if scenario_set & {"saved_card", "payment_method_update", "wallet_update", "customer_payment_update"}:
        required.update({"provider_tokenization", "pci_scope_avoidance", "sca_step_up_handling"})
    if scenario_set & {"card_vaulting", "tokenization", "provider_reference"}:
        required.update({"provider_tokenization", "pci_scope_avoidance", "sca_step_up_handling", "support_visibility"})
    if "default_payment_instrument" in scenario_set:
        required.update({"default_method_audit_trail", "customer_notification", "support_visibility"})
    if "payment_method_deletion" in scenario_set:
        required.update({"deletion_semantics", "customer_notification", "support_visibility"})
    if "retry_flow" in scenario_set:
        required.update({"retry_idempotency_behavior", "customer_notification", "support_visibility"})
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required and safeguard not in present)


def _risk_level(
    scenarios: tuple[SavedPaymentMethodScenario, ...],
    missing: tuple[SavedPaymentMethodSafeguard, ...],
) -> SavedPaymentMethodRiskLevel:
    if not missing:
        return "low"

    scenario_set = set(scenarios)
    missing_set = set(missing)
    if scenario_set & {"card_vaulting", "tokenization", "provider_reference"} and {
        "provider_tokenization",
        "pci_scope_avoidance",
    } & missing_set:
        return "high"
    if "default_payment_instrument" in scenario_set and "default_method_audit_trail" in missing_set:
        return "high"
    if "payment_method_deletion" in scenario_set and "deletion_semantics" in missing_set:
        return "high"
    if "retry_flow" in scenario_set and "retry_idempotency_behavior" in missing_set:
        return "high"
    if scenario_set & {"saved_card", "payment_method_update", "wallet_update", "customer_payment_update"} and len(missing_set) >= 4:
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskSavedPaymentMethodReadinessRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "saved_payment_method_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "scenario_counts": {
            scenario: sum(1 for record in records if scenario in record.payment_method_scenarios)
            for scenario in sorted({scenario for record in records for scenario in record.payment_method_scenarios})
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
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
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
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
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
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
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


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
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
        "validation_commands",
        "validation_command",
        "test_commands",
        "validation_plan",
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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    for source_field, text in _metadata_texts(task.get("validation_plan"), prefix="validation_plan"):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
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


def _metadata_key_is_signal(value: str) -> bool:
    return any(
        pattern.search(value)
        for patterns in (*_TEXT_SCENARIO_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
        for pattern in patterns
    )


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


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


__all__ = [
    "SavedPaymentMethodRiskLevel",
    "SavedPaymentMethodSafeguard",
    "SavedPaymentMethodScenario",
    "TaskSavedPaymentMethodReadinessPlan",
    "TaskSavedPaymentMethodReadinessRecord",
    "analyze_task_saved_payment_method_readiness",
    "build_task_saved_payment_method_readiness_plan",
    "extract_task_saved_payment_method_readiness",
    "generate_task_saved_payment_method_readiness",
    "recommend_task_saved_payment_method_readiness",
    "summarize_task_saved_payment_method_readiness",
    "task_saved_payment_method_readiness_plan_to_dict",
    "task_saved_payment_method_readiness_plan_to_dicts",
    "task_saved_payment_method_readiness_plan_to_markdown",
]
