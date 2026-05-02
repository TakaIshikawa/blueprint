"""Plan data anonymization and pseudonymization safeguards for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DataAnonymizationTransform = Literal[
    "anonymization",
    "pseudonymization",
    "hashing",
    "tokenization",
    "de_identification",
    "redaction",
    "test_data_generation",
    "privacy_safe_logs",
    "analytics_dataset",
    "anonymized_export",
]
DataAnonymizationSafeguard = Literal[
    "reidentification_risk_review",
    "irreversible_transform",
    "salt_key_management",
    "field_inventory",
    "sampling_policy",
    "downstream_contract",
    "retention_window",
    "validation_fixture",
]
DataAnonymizationRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[DataAnonymizationRisk, int] = {"high": 0, "medium": 1, "low": 2}
_TRANSFORM_ORDER: tuple[DataAnonymizationTransform, ...] = (
    "anonymization",
    "pseudonymization",
    "hashing",
    "tokenization",
    "de_identification",
    "redaction",
    "test_data_generation",
    "privacy_safe_logs",
    "analytics_dataset",
    "anonymized_export",
)
_SAFEGUARD_ORDER: tuple[DataAnonymizationSafeguard, ...] = (
    "reidentification_risk_review",
    "irreversible_transform",
    "salt_key_management",
    "field_inventory",
    "sampling_policy",
    "downstream_contract",
    "retention_window",
    "validation_fixture",
)
_HIGH_RISK_TRANSFORMS = {
    "pseudonymization",
    "hashing",
    "tokenization",
    "anonymized_export",
    "analytics_dataset",
}

_TRANSFORM_PATTERNS: dict[DataAnonymizationTransform, re.Pattern[str]] = {
    "anonymization": re.compile(
        r"\b(?:anonymi[sz](?:e|ed|es|ing|ation)|anonymous dataset)\b", re.I
    ),
    "pseudonymization": re.compile(
        r"\b(?:pseudonymi[sz](?:e|ed|es|ing|ation)|pseudonymous|pseudonymized identifiers?)\b", re.I
    ),
    "hashing": re.compile(r"\b(?:hash(?:ed|es|ing)?|one[- ]way hash|sha[- ]?256|hmac)\b", re.I),
    "tokenization": re.compile(
        r"\b(?:tokeni[sz](?:e|ed|es|ing|ation)|tokenized identifiers?|surrogate tokens?)\b", re.I
    ),
    "de_identification": re.compile(
        r"\b(?:de[- ]?identi(?:fy|fied|fication)|deidentified|de identify)\b", re.I
    ),
    "redaction": re.compile(
        r"\b(?:redact(?:ed|s|ing|ion)?|mask(?:ed|s|ing)?|scrub(?:bed|s|bing)?|remove pii)\b", re.I
    ),
    "test_data_generation": re.compile(
        r"\b(?:synthetic data|test data|generated fixtures?|fixture generation|seed data|sample data)\b",
        re.I,
    ),
    "privacy_safe_logs": re.compile(
        r"\b(?:privacy[- ]safe logs?|safe logs?|saniti[sz]ed logs?|scrub(?:bed|s|bing)? logs?|pii[- ]free logs?)\b",
        re.I,
    ),
    "analytics_dataset": re.compile(
        r"\b(?:analytics datasets?|analytics exports?|warehouse datasets?|bi datasets?|reporting datasets?|"
        r"data marts?|cohort datasets?)\b",
        re.I,
    ),
    "anonymized_export": re.compile(
        r"\b(?:anonymi[sz]ed exports?|anonymi[sz]ed reports?|privacy[- ]safe exports?|de[- ]identified exports?|"
        r"exports?.{0,60}\banonymi[sz]ed|anonymi[sz]ed.{0,60}\bexports?)\b",
        re.I,
    ),
}
_PATH_TRANSFORM_PATTERNS: dict[DataAnonymizationTransform, re.Pattern[str]] = {
    "anonymization": re.compile(r"(?:anonymi[sz]|anonymous)", re.I),
    "pseudonymization": re.compile(r"(?:pseudonymi[sz]|pseudonym)", re.I),
    "hashing": re.compile(r"(?:hash|hmac|sha256)", re.I),
    "tokenization": re.compile(r"(?:tokeni[sz]|tokens?)", re.I),
    "de_identification": re.compile(r"(?:de[-_]?identif|deidentif)", re.I),
    "redaction": re.compile(r"(?:redact|mask|scrub)", re.I),
    "test_data_generation": re.compile(
        r"(?:synthetic[-_]?data|test[-_]?data|fixtures?|seed[-_]?data)", re.I
    ),
    "privacy_safe_logs": re.compile(
        r"(?:privacy[-_]?safe[-_]?logs?|saniti[sz]ed[-_]?logs?|pii[-_]?free[-_]?logs?)", re.I
    ),
    "analytics_dataset": re.compile(
        r"(?:analytics|warehouse|bi|reporting|data[-_]?mart|cohort)", re.I
    ),
    "anonymized_export": re.compile(
        r"(?:anonymi[sz]ed[-_]?exports?|de[-_]?identified[-_]?exports?|privacy[-_]?safe[-_]?exports?)",
        re.I,
    ),
}
_SURFACE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "analytics dataset",
        re.compile(r"\banalytics datasets?\b|\bbi datasets?\b|\bcohort datasets?\b", re.I),
    ),
    ("exports", re.compile(r"\bexports?\b|\bdownloads?\b|\bcsv\b|\bparquet\b", re.I)),
    ("reports", re.compile(r"\breports?\b|\bdashboards?\b|\bmetrics?\b", re.I)),
    ("logs", re.compile(r"\blogs?\b|\baudit logs?\b|\btraces?\b|telemetry", re.I)),
    (
        "identifiers",
        re.compile(r"\bidentifiers?\b|\bids?\b|\buser_id\b|\bcustomer_id\b|\baccount_id\b", re.I),
    ),
    (
        "customer data",
        re.compile(r"\bcustomer data\b|\bcustomers?\b|\bprofiles?\b|\baccounts?\b", re.I),
    ),
    (
        "personal data",
        re.compile(
            r"\bpersonal data\b|\bpii\b|\bphi\b|\bemail(?:s)?\b|\bphone(?:s)?\b|\bip addresses?\b",
            re.I,
        ),
    ),
    (
        "test fixtures",
        re.compile(r"\bfixtures?\b|\btest data\b|\bseed data\b|\bsynthetic data\b", re.I),
    ),
    ("data warehouse", re.compile(r"\bwarehouse\b|\bdata lake\b|\bdata mart\b", re.I)),
    (
        "sharing dataset",
        re.compile(r"\bsharing\b|\bpartner datasets?\b|\bexternal datasets?\b", re.I),
    ),
)
_DATA_CONTEXT_RE = re.compile(
    r"\b(?:datasets?|exports?|reports?|analytics|warehouse|data lake|data mart|logs?|telemetry|events?|"
    r"fixtures?|test data|synthetic data|sharing|partner|downstream|csv|parquet|records?|fields?)\b",
    re.I,
)
_PERSONAL_DATA_RE = re.compile(
    r"\b(?:pii|phi|personal data|personally identifiable|sensitive data|customer(?:s)?|users?|profiles?|"
    r"email(?:s)?|phone(?:s)?|names?|addresses?|ip addresses?|ssn|social security|date of birth|dob|"
    r"identifiers?|user_id|customer_id|account_id|device ids?|cookies?)\b",
    re.I,
)
_UI_ONLY_RE = re.compile(
    r"\b(?:ui|button|label|copy|tooltip|modal|screen|view|form|frontend|css|component)\b", re.I
)
_SAFEGUARD_PATTERNS: dict[DataAnonymizationSafeguard, re.Pattern[str]] = {
    "reidentification_risk_review": re.compile(
        r"\b(?:re[- ]?identification risk|reidentification review|k[- ]anonymity|l[- ]diversity|privacy review|singling out)\b",
        re.I,
    ),
    "irreversible_transform": re.compile(
        r"\b(?:irreversible|one[- ]way|cannot be reversed|non[- ]reversible|drop raw values|destroy raw values)\b",
        re.I,
    ),
    "salt_key_management": re.compile(
        r"\b(?:salt(?:ed|s)?|pepper|hmac|secret key|key management|kms|rotate keys?|per[- ]tenant key)\b",
        re.I,
    ),
    "field_inventory": re.compile(
        r"\b(?:field inventory|field list|data inventory|pii inventory|column inventory|schema inventory|classify fields?)\b",
        re.I,
    ),
    "sampling_policy": re.compile(
        r"\b(?:sampling policy|sample size|minimum cohort|cohort threshold|k threshold|aggregation threshold|small cells?)\b",
        re.I,
    ),
    "downstream_contract": re.compile(
        r"\b(?:downstream contract|consumer contract|data contract|sharing agreement|partner contract|allowed use)\b",
        re.I,
    ),
    "retention_window": re.compile(
        r"\b(?:retention window|retention period|ttl|expire(?:s|d)?|delete after|purge after|data retention)\b",
        re.I,
    ),
    "validation_fixture": re.compile(
        r"\b(?:validation fixtures?|golden fixtures?|test fixtures?|fixture validation|privacy regression tests?|assert no pii)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[DataAnonymizationSafeguard, str] = {
    "reidentification_risk_review": "Review re-identification risk, including joins, rare cohorts, and quasi-identifiers.",
    "irreversible_transform": "Confirm transforms are irreversible or explicitly justify any reversible pseudonymization.",
    "salt_key_management": "Define salt, pepper, HMAC, or token key ownership, storage, and rotation behavior.",
    "field_inventory": "Inventory personal-data fields, identifiers, and quasi-identifiers before transforming data.",
    "sampling_policy": "Set sampling, aggregation, or minimum cohort rules that prevent small-cell disclosure.",
    "downstream_contract": "Document downstream contracts for permitted use, schema expectations, and re-sharing limits.",
    "retention_window": "Define retention windows for raw inputs, transformed outputs, and validation artifacts.",
    "validation_fixture": "Add validation fixtures that prove personal data is transformed or omitted as expected.",
}


@dataclass(frozen=True, slots=True)
class TaskDataAnonymizationReadinessFinding:
    """Anonymization readiness guidance for one execution task."""

    task_id: str
    title: str
    data_surfaces: tuple[str, ...] = field(default_factory=tuple)
    transform_types: tuple[DataAnonymizationTransform, ...] = field(default_factory=tuple)
    required_safeguards: tuple[DataAnonymizationSafeguard, ...] = field(default_factory=tuple)
    present_safeguards: tuple[DataAnonymizationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[DataAnonymizationSafeguard, ...] = field(default_factory=tuple)
    risk_level: DataAnonymizationRisk = "medium"
    recommended_validation_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "data_surfaces": list(self.data_surfaces),
            "transform_types": list(self.transform_types),
            "required_safeguards": list(self.required_safeguards),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "recommended_validation_checks": list(self.recommended_validation_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDataAnonymizationReadinessPlan:
    """Plan-level anonymization and pseudonymization readiness recommendations."""

    plan_id: str | None = None
    findings: tuple[TaskDataAnonymizationReadinessFinding, ...] = field(default_factory=tuple)
    anonymization_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskDataAnonymizationReadinessFinding, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.findings

    @property
    def recommendations(self) -> tuple[TaskDataAnonymizationReadinessFinding, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "records": [record.to_dict() for record in self.records],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "anonymization_task_ids": list(self.anonymization_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return anonymization readiness findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    def to_markdown(self) -> str:
        """Render anonymization readiness recommendations as deterministic Markdown."""
        title = "# Task Data Anonymization Readiness Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        transform_counts = self.summary.get("transform_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Anonymization task count: {self.summary.get('anonymization_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
            "- Transform counts: "
            + ", ".join(
                f"{transform} {transform_counts.get(transform, 0)}"
                for transform in _TRANSFORM_ORDER
            ),
        ]
        if not self.findings:
            lines.extend(["", "No task data anonymization readiness findings were inferred."])
            if self.ignored_task_ids:
                lines.extend(
                    ["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Findings",
                "",
                "| Task | Title | Risk | Data Surfaces | Transform Types | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for finding in self.findings:
            lines.append(
                "| "
                f"`{_markdown_cell(finding.task_id)}` | "
                f"{_markdown_cell(finding.title)} | "
                f"{finding.risk_level} | "
                f"{_markdown_cell(', '.join(finding.data_surfaces) or 'unspecified')} | "
                f"{_markdown_cell(', '.join(finding.transform_types) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.recommended_validation_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_data_anonymization_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDataAnonymizationReadinessPlan:
    """Build anonymization readiness recommendations for execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_finding(task, index) for index, task in enumerate(tasks, start=1)]
    findings = tuple(
        sorted(
            (finding for finding in candidates if finding is not None),
            key=lambda finding: (
                _RISK_ORDER[finding.risk_level],
                finding.task_id,
                finding.title.casefold(),
            ),
        )
    )
    ignored_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskDataAnonymizationReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        anonymization_task_ids=tuple(finding.task_id for finding in findings),
        ignored_task_ids=ignored_task_ids,
        summary=_summary(findings, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def analyze_task_data_anonymization_readiness(source: Any) -> TaskDataAnonymizationReadinessPlan:
    """Compatibility alias for building anonymization readiness recommendations."""
    return build_task_data_anonymization_readiness_plan(source)


def summarize_task_data_anonymization_readiness(source: Any) -> TaskDataAnonymizationReadinessPlan:
    """Compatibility alias for building anonymization readiness recommendations."""
    return build_task_data_anonymization_readiness_plan(source)


def extract_task_data_anonymization_readiness(source: Any) -> TaskDataAnonymizationReadinessPlan:
    """Compatibility alias for building anonymization readiness recommendations."""
    return build_task_data_anonymization_readiness_plan(source)


def generate_task_data_anonymization_readiness(source: Any) -> TaskDataAnonymizationReadinessPlan:
    """Compatibility alias for generating anonymization readiness recommendations."""
    return build_task_data_anonymization_readiness_plan(source)


def recommend_task_data_anonymization_readiness(source: Any) -> TaskDataAnonymizationReadinessPlan:
    """Compatibility alias for recommending anonymization safeguards."""
    return build_task_data_anonymization_readiness_plan(source)


def task_data_anonymization_readiness_plan_to_dict(
    result: TaskDataAnonymizationReadinessPlan,
) -> dict[str, Any]:
    """Serialize an anonymization readiness plan to a plain dictionary."""
    return result.to_dict()


task_data_anonymization_readiness_plan_to_dict.__test__ = False


def task_data_anonymization_readiness_plan_to_dicts(
    result: TaskDataAnonymizationReadinessPlan | Iterable[TaskDataAnonymizationReadinessFinding],
) -> list[dict[str, Any]]:
    """Serialize anonymization readiness findings to plain dictionaries."""
    if isinstance(result, TaskDataAnonymizationReadinessPlan):
        return result.to_dicts()
    return [finding.to_dict() for finding in result]


task_data_anonymization_readiness_plan_to_dicts.__test__ = False


def task_data_anonymization_readiness_plan_to_markdown(
    result: TaskDataAnonymizationReadinessPlan,
) -> str:
    """Render an anonymization readiness plan as Markdown."""
    return result.to_markdown()


task_data_anonymization_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    transforms: tuple[DataAnonymizationTransform, ...] = field(default_factory=tuple)
    surfaces: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[DataAnonymizationSafeguard, ...] = field(default_factory=tuple)
    transform_evidence: tuple[str, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)
    has_data_context: bool = False
    has_personal_data_context: bool = False
    ui_only_redaction: bool = False


def _finding(task: Mapping[str, Any], index: int) -> TaskDataAnonymizationReadinessFinding | None:
    signals = _signals(task)
    if not _is_anonymization_task(signals):
        return None

    required_safeguards = _required_safeguards(signals.transforms, signals.surfaces)
    missing_safeguards = tuple(
        safeguard
        for safeguard in required_safeguards
        if safeguard not in signals.present_safeguards
    )
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskDataAnonymizationReadinessFinding(
        task_id=task_id,
        title=title,
        data_surfaces=signals.surfaces or ("unspecified data surface",),
        transform_types=signals.transforms,
        required_safeguards=required_safeguards,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing_safeguards,
        risk_level=_risk_level(signals.transforms, signals.surfaces, missing_safeguards),
        recommended_validation_checks=tuple(
            _SAFEGUARD_GUIDANCE[safeguard] for safeguard in required_safeguards
        ),
        evidence=tuple(_dedupe([*signals.transform_evidence, *signals.safeguard_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    transform_hits: set[DataAnonymizationTransform] = set()
    surface_hits: set[str] = set()
    safeguard_hits: set[DataAnonymizationSafeguard] = set()
    transform_evidence: list[str] = []
    safeguard_evidence: list[str] = []
    has_data_context = False
    has_personal_data_context = False
    ui_only_redaction = False

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_transforms = _path_transforms(normalized)
        if path_transforms:
            transform_hits.update(path_transforms)
            transform_evidence.append(f"files_or_modules: {path}")
        path_surfaces = _surfaces(path_text)
        if path_surfaces:
            surface_hits.update(path_surfaces)
            has_data_context = True
            transform_evidence.append(f"files_or_modules: {path}")
        if _PERSONAL_DATA_RE.search(path_text):
            has_personal_data_context = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(path_text) or pattern.search(normalized):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_transform = False
        for transform, pattern in _TRANSFORM_PATTERNS.items():
            if pattern.search(text):
                transform_hits.add(transform)
                matched_transform = True
        matched_surfaces = _surfaces(text)
        if matched_surfaces:
            surface_hits.update(matched_surfaces)
        data_context = bool(_DATA_CONTEXT_RE.search(text) or matched_surfaces)
        personal_context = bool(_PERSONAL_DATA_RE.search(text))
        has_data_context = has_data_context or data_context
        has_personal_data_context = has_personal_data_context or personal_context
        if matched_transform or matched_surfaces or personal_context:
            transform_evidence.append(snippet)
        if (
            matched_transform
            and transform_hits == {"redaction"}
            and _UI_ONLY_RE.search(text)
            and not (data_context or personal_context)
        ):
            ui_only_redaction = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    transforms = tuple(transform for transform in _TRANSFORM_ORDER if transform in transform_hits)
    return _Signals(
        transforms=transforms,
        surfaces=tuple(
            surface for surface, _pattern in _SURFACE_PATTERNS if surface in surface_hits
        ),
        present_safeguards=tuple(
            safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits
        ),
        transform_evidence=tuple(_dedupe(transform_evidence)),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
        has_data_context=has_data_context,
        has_personal_data_context=has_personal_data_context,
        ui_only_redaction=ui_only_redaction,
    )


def _is_anonymization_task(signals: _Signals) -> bool:
    if not signals.transforms:
        return False
    if (
        signals.transforms == ("redaction",)
        and signals.ui_only_redaction
        and not signals.has_personal_data_context
    ):
        return False
    if any(
        transform in {"anonymization", "pseudonymization", "de_identification"}
        for transform in signals.transforms
    ):
        return (
            signals.has_data_context or signals.has_personal_data_context or bool(signals.surfaces)
        )
    if any(
        transform in {"hashing", "tokenization", "redaction"} for transform in signals.transforms
    ):
        return (
            signals.has_personal_data_context or signals.has_data_context or bool(signals.surfaces)
        )
    return True


def _path_transforms(path: str) -> set[DataAnonymizationTransform]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    transforms: set[DataAnonymizationTransform] = set()
    for transform, pattern in _PATH_TRANSFORM_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(text):
            transforms.add(transform)
    name = PurePosixPath(normalized).name
    if name in {"anonymize.py", "pseudonymize.py", "redaction.py", "tokenize.py"}:
        transforms.add(
            "tokenization"
            if "token" in name
            else "redaction" if "redaction" in name else "anonymization"
        )
    return transforms


def _surfaces(text: str) -> set[str]:
    return {surface for surface, pattern in _SURFACE_PATTERNS if pattern.search(text)}


def _required_safeguards(
    transforms: tuple[DataAnonymizationTransform, ...],
    surfaces: tuple[str, ...],
) -> tuple[DataAnonymizationSafeguard, ...]:
    required: set[DataAnonymizationSafeguard] = {
        "reidentification_risk_review",
        "field_inventory",
        "retention_window",
        "validation_fixture",
    }
    if any(
        transform in transforms
        for transform in ("anonymization", "hashing", "de_identification", "redaction")
    ):
        required.add("irreversible_transform")
    if any(
        transform in transforms for transform in ("hashing", "tokenization", "pseudonymization")
    ):
        required.update({"salt_key_management", "downstream_contract"})
    if any(transform in transforms for transform in ("analytics_dataset", "anonymized_export")):
        required.update({"sampling_policy", "downstream_contract"})
    if any(
        surface in surfaces
        for surface in ("analytics dataset", "reports", "sharing dataset", "data warehouse")
    ):
        required.add("sampling_policy")
    if any(surface in surfaces for surface in ("exports", "sharing dataset")):
        required.add("downstream_contract")
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required)


def _risk_level(
    transforms: tuple[DataAnonymizationTransform, ...],
    surfaces: tuple[str, ...],
    missing_safeguards: tuple[DataAnonymizationSafeguard, ...],
) -> DataAnonymizationRisk:
    if not missing_safeguards:
        return "low"
    if (
        any(transform in _HIGH_RISK_TRANSFORMS for transform in transforms)
        or "sharing dataset" in surfaces
    ):
        return "high"
    if len(missing_safeguards) >= 4 or "exports" in surfaces:
        return "medium"
    return "low"


def _summary(
    findings: tuple[TaskDataAnonymizationReadinessFinding, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "anonymization_task_count": len(findings),
        "ignored_task_ids": list(ignored_task_ids),
        "missing_safeguard_count": sum(len(finding.missing_safeguards) for finding in findings),
        "risk_counts": {
            risk: sum(1 for finding in findings if finding.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "transform_counts": {
            transform: sum(1 for finding in findings if transform in finding.transform_types)
            for transform in _TRANSFORM_ORDER
        },
        "surface_counts": {
            surface: sum(1 for finding in findings if surface in finding.data_surfaces)
            for surface in sorted(
                {surface for finding in findings for surface in finding.data_surfaces}
            )
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "anonymization_task_ids": [finding.task_id for finding in findings],
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
        if task := _task_payload(item):
            tasks.append(task)
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
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
    return bool(
        _DATA_CONTEXT_RE.search(value)
        or _PERSONAL_DATA_RE.search(value)
        or any(
            pattern.search(value)
            for pattern in [*_TRANSFORM_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()]
        )
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
    "DataAnonymizationRisk",
    "DataAnonymizationSafeguard",
    "DataAnonymizationTransform",
    "TaskDataAnonymizationReadinessFinding",
    "TaskDataAnonymizationReadinessPlan",
    "analyze_task_data_anonymization_readiness",
    "build_task_data_anonymization_readiness_plan",
    "extract_task_data_anonymization_readiness",
    "generate_task_data_anonymization_readiness",
    "recommend_task_data_anonymization_readiness",
    "summarize_task_data_anonymization_readiness",
    "task_data_anonymization_readiness_plan_to_dict",
    "task_data_anonymization_readiness_plan_to_dicts",
    "task_data_anonymization_readiness_plan_to_markdown",
]
