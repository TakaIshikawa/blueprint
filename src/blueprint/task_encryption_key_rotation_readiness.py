"""Plan encryption key rotation readiness controls for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


KeyRotationSurface = Literal[
    "encryption",
    "kms",
    "secret",
    "token",
    "certificate",
    "signing_key",
    "api_key",
    "database_encryption",
    "credential_migration",
]
KeyRotationSafeguard = Literal[
    "dual_read_window",
    "staged_key_rollout",
    "rollback_key_retention",
    "rotation_audit_log",
    "data_re_encryption_job",
    "key_ownership",
    "expiry_monitoring",
    "incident_fallback",
]
KeyRotationRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[KeyRotationRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER: tuple[KeyRotationSurface, ...] = (
    "encryption",
    "kms",
    "secret",
    "token",
    "certificate",
    "signing_key",
    "api_key",
    "database_encryption",
    "credential_migration",
)
_SAFEGUARD_ORDER: tuple[KeyRotationSafeguard, ...] = (
    "dual_read_window",
    "staged_key_rollout",
    "rollback_key_retention",
    "rotation_audit_log",
    "data_re_encryption_job",
    "key_ownership",
    "expiry_monitoring",
    "incident_fallback",
)
_HIGH_RISK_SURFACES = {
    "kms",
    "secret",
    "certificate",
    "signing_key",
    "database_encryption",
    "credential_migration",
}
_DATA_SURFACES = {"encryption", "kms", "database_encryption", "credential_migration"}

_PATH_SURFACE_PATTERNS: tuple[tuple[KeyRotationSurface, re.Pattern[str]], ...] = (
    ("encryption", re.compile(r"(?:^|/)(?:encrypt(?:ion)?|crypto|cipher|dek|cek)(?:/|\.|_|-|$)", re.I)),
    ("kms", re.compile(r"(?:^|/)(?:kms|key[-_]?management|cmk|keyring|key[-_]?vault)(?:/|\.|_|-|$)", re.I)),
    ("secret", re.compile(r"(?:^|/)(?:secrets?|secret[-_]?manager|vault|credentials?)(?:/|\.|_|-|$)", re.I)),
    ("token", re.compile(r"(?:^|/)(?:tokens?|jwt|oauth|refresh[-_]?token|access[-_]?token)(?:/|\.|_|-|$)", re.I)),
    ("certificate", re.compile(r"(?:^|/)(?:cert(?:ificate)?s?|tls|ssl|x509|pki|acm)(?:/|\.|_|-|$)", re.I)),
    ("signing_key", re.compile(r"(?:^|/)(?:signing[-_]?keys?|jwks|hmac|private[-_]?key|public[-_]?key)(?:/|\.|_|-|$)", re.I)),
    ("api_key", re.compile(r"(?:^|/)(?:api[-_]?keys?|apikeys?|client[-_]?secrets?)(?:/|\.|_|-|$)", re.I)),
    ("database_encryption", re.compile(r"(?:^|/)(?:database[-_]?encryption|db[-_]?encryption|tde|encrypted[-_]?columns?)(?:/|\.|_|-|$)", re.I)),
    ("credential_migration", re.compile(r"(?:^|/)(?:credential[-_]?migration|rotate[-_]?credentials?|credential[-_]?rotation)(?:/|\.|_|-|$)", re.I)),
)
_TEXT_SURFACE_PATTERNS: dict[KeyRotationSurface, re.Pattern[str]] = {
    "encryption": re.compile(r"\b(?:encrypt(?:ion|ed)?|crypto(?:graphic)?|ciphertext|data encryption key|dek|cek)\b", re.I),
    "kms": re.compile(r"\b(?:kms|key management service|customer managed keys?|cmk|keyring|key vault|aws kms|gcp kms|azure key vault)\b", re.I),
    "secret": re.compile(r"\b(?:secrets?|secret manager|vault secrets?|credentials?|password rotation|rotate passwords?)\b", re.I),
    "token": re.compile(r"\b(?:tokens?|jwt|oauth tokens?|refresh tokens?|access tokens?|bearer tokens?)\b", re.I),
    "certificate": re.compile(r"\b(?:certificates?|cert rotation|tls certs?|ssl certs?|x\.?509|pki|certificate authority|acm)\b", re.I),
    "signing_key": re.compile(r"\b(?:signing keys?|signature keys?|jwks|jwk set|hmac keys?|private keys?|public keys?)\b", re.I),
    "api_key": re.compile(r"\b(?:api keys?|client secrets?|developer keys?|service keys?)\b", re.I),
    "database_encryption": re.compile(r"\b(?:database encryption|db encryption|transparent data encryption|tde|encrypted columns?|field[- ]level encryption)\b", re.I),
    "credential_migration": re.compile(r"\b(?:credential migration|migrate credentials?|credential rotation|rotate credentials?|key migration|secret migration)\b", re.I),
}
_ROTATION_CONTEXT_RE = re.compile(
    r"\b(?:rotat(?:e|es|ed|ing|ion)|roll(?:out| over|over)|re[- ]?key|rekey|renew|expiry|"
    r"expire|migration|migrate|replace|deprecat(?:e|ing|ion)|re[- ]?encrypt|reencrypt)\b",
    re.I,
)
_SAFEGUARD_PATTERNS: dict[KeyRotationSafeguard, re.Pattern[str]] = {
    "dual_read_window": re.compile(r"\b(?:dual[- ]read|read old and new|old and new keys?|parallel decrypt|grace window|overlap window|compatibility window)\b", re.I),
    "staged_key_rollout": re.compile(r"\b(?:staged key rollout|staged rollout|phased rollout|canary key|progressive rollout|key rollout|roll out by tenant|percentage rollout)\b", re.I),
    "rollback_key_retention": re.compile(r"\b(?:rollback key retention|retain old keys?|keep old keys?|previous keys?|key retention|rollback key|restore old key|revert key)\b", re.I),
    "rotation_audit_log": re.compile(r"\b(?:rotation audit log|audit log|audit event|audit trail|key rotation event|rotation history|compliance log)\b", re.I),
    "data_re_encryption_job": re.compile(r"\b(?:data re[- ]?encryption job|re[- ]?encrypt(?: data| records| rows)?|backfill encryption|decrypt and re[- ]?encrypt|rewrap|key rewrap)\b", re.I),
    "key_ownership": re.compile(r"\b(?:key owner|key ownership|owning team|rotation owner|runbook owner|responsible owner|service owner)\b", re.I),
    "expiry_monitoring": re.compile(r"\b(?:expiry monitoring|expiration monitoring|expires?|expiry alert|expiration alert|certificate expiry|key age|key expiry|renewal alert)\b", re.I),
    "incident_fallback": re.compile(r"\b(?:incident fallback|break glass|emergency fallback|incident runbook|manual fallback|fallback key|degraded mode|emergency access)\b", re.I),
}
_SAFEGUARD_GUIDANCE: dict[KeyRotationSafeguard, str] = {
    "dual_read_window": "Define an overlap window where services can read or decrypt with both old and new key material.",
    "staged_key_rollout": "Roll out the new key in stages by environment, tenant, service, or traffic percentage.",
    "rollback_key_retention": "Retain prior keys long enough to support rollback and delayed readers.",
    "rotation_audit_log": "Emit audit events for key creation, activation, deactivation, rotation, and emergency access.",
    "data_re_encryption_job": "Plan any backfill, rewrap, or data re-encryption job needed for persisted ciphertext.",
    "key_ownership": "Name the owning team, runbook owner, or approver responsible for the rotation.",
    "expiry_monitoring": "Add expiry, age, and renewal monitoring before credentials or certificates become invalid.",
    "incident_fallback": "Document the incident fallback path for failed rotation, unavailable KMS, or revoked credentials.",
}


@dataclass(frozen=True, slots=True)
class TaskEncryptionKeyRotationReadinessRecommendation:
    """Key rotation readiness guidance for one execution task."""

    task_id: str
    title: str
    key_surfaces: tuple[KeyRotationSurface, ...] = field(default_factory=tuple)
    required_safeguards: tuple[KeyRotationSafeguard, ...] = field(default_factory=tuple)
    present_safeguards: tuple[KeyRotationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[KeyRotationSafeguard, ...] = field(default_factory=tuple)
    risk_level: KeyRotationRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_follow_up_actions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "key_surfaces": list(self.key_surfaces),
            "required_safeguards": list(self.required_safeguards),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_follow_up_actions": list(self.recommended_follow_up_actions),
        }


@dataclass(frozen=True, slots=True)
class TaskEncryptionKeyRotationReadinessPlan:
    """Plan-level encryption key rotation readiness recommendations."""

    plan_id: str | None = None
    recommendations: tuple[TaskEncryptionKeyRotationReadinessRecommendation, ...] = field(default_factory=tuple)
    rotation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskEncryptionKeyRotationReadinessRecommendation, ...]:
        """Compatibility view matching analyzers that expose rows as records."""
        return self.recommendations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [recommendation.to_dict() for recommendation in self.recommendations],
            "records": [record.to_dict() for record in self.records],
            "rotation_task_ids": list(self.rotation_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness recommendations as plain dictionaries."""
        return [recommendation.to_dict() for recommendation in self.recommendations]

    def to_markdown(self) -> str:
        """Render key rotation readiness recommendations as deterministic Markdown."""
        title = "# Task Encryption Key Rotation Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Rotation task count: {self.summary.get('rotation_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.recommendations:
            lines.extend(["", "No encryption key rotation readiness recommendations were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Recommendations",
                "",
                "| Task | Title | Risk | Key Surfaces | Present Safeguards | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for recommendation in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(recommendation.task_id)}` | "
                f"{_markdown_cell(recommendation.title)} | "
                f"{recommendation.risk_level} | "
                f"{_markdown_cell(', '.join(recommendation.key_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(recommendation.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(recommendation.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(recommendation.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_encryption_key_rotation_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskEncryptionKeyRotationReadinessPlan:
    """Build key rotation readiness recommendations for execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_recommendation(task, index) for index, task in enumerate(tasks, start=1)]
    recommendations = tuple(
        sorted(
            (recommendation for recommendation in candidates if recommendation is not None),
            key=lambda recommendation: (
                _RISK_ORDER[recommendation.risk_level],
                recommendation.task_id,
                recommendation.title.casefold(),
            ),
        )
    )
    ignored_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskEncryptionKeyRotationReadinessPlan(
        plan_id=plan_id,
        recommendations=recommendations,
        rotation_task_ids=tuple(recommendation.task_id for recommendation in recommendations),
        ignored_task_ids=ignored_task_ids,
        summary=_summary(recommendations, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def analyze_task_encryption_key_rotation_readiness(source: Any) -> TaskEncryptionKeyRotationReadinessPlan:
    """Compatibility alias for building key rotation readiness recommendations."""
    return build_task_encryption_key_rotation_readiness_plan(source)


def summarize_task_encryption_key_rotation_readiness(source: Any) -> TaskEncryptionKeyRotationReadinessPlan:
    """Compatibility alias for building key rotation readiness recommendations."""
    return build_task_encryption_key_rotation_readiness_plan(source)


def extract_task_encryption_key_rotation_readiness(source: Any) -> TaskEncryptionKeyRotationReadinessPlan:
    """Compatibility alias for building key rotation readiness recommendations."""
    return build_task_encryption_key_rotation_readiness_plan(source)


def generate_task_encryption_key_rotation_readiness(source: Any) -> TaskEncryptionKeyRotationReadinessPlan:
    """Compatibility alias for generating key rotation readiness recommendations."""
    return build_task_encryption_key_rotation_readiness_plan(source)


def recommend_task_encryption_key_rotation_readiness(source: Any) -> TaskEncryptionKeyRotationReadinessPlan:
    """Compatibility alias for recommending key rotation safeguards."""
    return build_task_encryption_key_rotation_readiness_plan(source)


def task_encryption_key_rotation_readiness_plan_to_dict(
    result: TaskEncryptionKeyRotationReadinessPlan,
) -> dict[str, Any]:
    """Serialize a key rotation readiness plan to a plain dictionary."""
    return result.to_dict()


task_encryption_key_rotation_readiness_plan_to_dict.__test__ = False


def task_encryption_key_rotation_readiness_plan_to_dicts(
    result: TaskEncryptionKeyRotationReadinessPlan
    | Iterable[TaskEncryptionKeyRotationReadinessRecommendation],
) -> list[dict[str, Any]]:
    """Serialize key rotation readiness records to plain dictionaries."""
    if isinstance(result, TaskEncryptionKeyRotationReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_encryption_key_rotation_readiness_plan_to_dicts.__test__ = False


def task_encryption_key_rotation_readiness_plan_to_markdown(
    result: TaskEncryptionKeyRotationReadinessPlan,
) -> str:
    """Render a key rotation readiness plan as Markdown."""
    return result.to_markdown()


task_encryption_key_rotation_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[KeyRotationSurface, ...] = field(default_factory=tuple)
    surface_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[KeyRotationSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_recommendation(
    task: Mapping[str, Any],
    index: int,
) -> TaskEncryptionKeyRotationReadinessRecommendation | None:
    signals = _signals(task)
    if not signals.surfaces:
        return None

    required_safeguards = _required_safeguards(signals.surfaces)
    missing_safeguards = tuple(
        safeguard for safeguard in required_safeguards if safeguard not in signals.present_safeguards
    )
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskEncryptionKeyRotationReadinessRecommendation(
        task_id=task_id,
        title=title,
        key_surfaces=signals.surfaces,
        required_safeguards=required_safeguards,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing_safeguards,
        risk_level=_risk_level(signals.surfaces, missing_safeguards),
        evidence=tuple(_dedupe([*signals.surface_evidence, *signals.safeguard_evidence])),
        recommended_follow_up_actions=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing_safeguards),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surface_hits: set[KeyRotationSurface] = set()
    safeguard_hits: set[KeyRotationSafeguard] = set()
    surface_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_surfaces = _path_surfaces(normalized)
        if path_surfaces:
            surface_hits.update(path_surfaces)
            surface_evidence.append(f"files_or_modules: {path}")
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(searchable) or pattern.search(normalized):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_surface = False
        for surface, pattern in _TEXT_SURFACE_PATTERNS.items():
            if pattern.search(text) and (_ROTATION_CONTEXT_RE.search(text) or _path_like_field(source_field)):
                surface_hits.add(surface)
                matched_surface = True
        if matched_surface:
            surface_evidence.append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    return _Signals(
        surfaces=tuple(surface for surface in _SURFACE_ORDER if surface in surface_hits),
        surface_evidence=tuple(_dedupe(surface_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_surfaces(path: str) -> set[KeyRotationSurface]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    surfaces: set[KeyRotationSurface] = set()
    for surface, pattern in _PATH_SURFACE_PATTERNS:
        if pattern.search(normalized) or pattern.search(text):
            surfaces.add(surface)
    name = PurePosixPath(normalized).name
    if name in {"kms.py", "secrets.py", "certificates.py", "tokens.py"}:
        surfaces.add({"kms.py": "kms", "secrets.py": "secret", "certificates.py": "certificate", "tokens.py": "token"}[name])
    return surfaces


def _required_safeguards(surfaces: tuple[KeyRotationSurface, ...]) -> tuple[KeyRotationSafeguard, ...]:
    required: set[KeyRotationSafeguard] = {
        "dual_read_window",
        "staged_key_rollout",
        "rollback_key_retention",
        "rotation_audit_log",
        "key_ownership",
        "expiry_monitoring",
        "incident_fallback",
    }
    if any(surface in _DATA_SURFACES for surface in surfaces):
        required.add("data_re_encryption_job")
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required)


def _risk_level(
    surfaces: tuple[KeyRotationSurface, ...],
    missing_safeguards: tuple[KeyRotationSafeguard, ...],
) -> KeyRotationRiskLevel:
    if not missing_safeguards:
        return "medium" if any(surface in _HIGH_RISK_SURFACES for surface in surfaces) else "low"
    if any(surface in _HIGH_RISK_SURFACES for surface in surfaces):
        return "high"
    if len(missing_safeguards) >= 4:
        return "medium"
    return "low"


def _summary(
    recommendations: tuple[TaskEncryptionKeyRotationReadinessRecommendation, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "rotation_task_count": len(recommendations),
        "ignored_task_ids": list(ignored_task_ids),
        "missing_safeguard_count": sum(len(recommendation.missing_safeguards) for recommendation in recommendations),
        "risk_counts": {
            risk: sum(1 for recommendation in recommendations if recommendation.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "surface_counts": {
            surface: sum(1 for recommendation in recommendations if surface in recommendation.key_surfaces)
            for surface in sorted(
                {surface for recommendation in recommendations for surface in recommendation.key_surfaces}
            )
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for recommendation in recommendations if safeguard in recommendation.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "rotation_task_ids": [recommendation.task_id for recommendation in recommendations],
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
        for pattern in [*_TEXT_SURFACE_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values(), _ROTATION_CONTEXT_RE]
    )


def _path_like_field(source_field: str) -> bool:
    return source_field.startswith("metadata.") and any(
        token in source_field.casefold() for token in ("file", "path", "module")
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
    "KeyRotationRiskLevel",
    "KeyRotationSafeguard",
    "KeyRotationSurface",
    "TaskEncryptionKeyRotationReadinessPlan",
    "TaskEncryptionKeyRotationReadinessRecommendation",
    "analyze_task_encryption_key_rotation_readiness",
    "build_task_encryption_key_rotation_readiness_plan",
    "extract_task_encryption_key_rotation_readiness",
    "generate_task_encryption_key_rotation_readiness",
    "recommend_task_encryption_key_rotation_readiness",
    "summarize_task_encryption_key_rotation_readiness",
    "task_encryption_key_rotation_readiness_plan_to_dict",
    "task_encryption_key_rotation_readiness_plan_to_dicts",
    "task_encryption_key_rotation_readiness_plan_to_markdown",
]
