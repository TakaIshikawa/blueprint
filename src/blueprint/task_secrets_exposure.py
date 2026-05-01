"""Assess execution-plan tasks for likely secrets exposure risk."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SecretsReviewClassification = Literal[
    "secrets_review_required",
    "no_secrets_review_needed",
]
SecretsSignal = Literal[
    "api_keys",
    "tokens",
    "oauth_clients",
    "certificates",
    "environment_variables",
    "config_files",
    "ci_cd_secrets",
    "credentials",
    "secret_storage",
    "logging_redaction",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_PATTERNS: dict[SecretsSignal, re.Pattern[str]] = {
    "api_keys": re.compile(
        r"\b(?:api key|api keys|apikey|api_key|x-api-key|access key|access keys)\b",
        re.IGNORECASE,
    ),
    "tokens": re.compile(
        r"\b(?:token|tokens|bearer token|access token|refresh token|jwt|pat|"
        r"personal access token)\b",
        re.IGNORECASE,
    ),
    "oauth_clients": re.compile(
        r"\b(?:oauth|oauth2|oidc|client id|client secret|client credentials|"
        r"authorization code|openid connect)\b",
        re.IGNORECASE,
    ),
    "certificates": re.compile(
        r"\b(?:certificate|certificates|cert|certs|private key|public key|"
        r"tls|ssl|mTLS|pem|p12|pfx|keystore|truststore)\b",
        re.IGNORECASE,
    ),
    "environment_variables": re.compile(
        r"\b(?:\.env|env var|env vars|environment variable|environment variables|"
        r"process\.env|dotenv|runtime env)\b",
        re.IGNORECASE,
    ),
    "config_files": re.compile(
        r"\b(?:config file|config files|configuration file|configuration files|"
        r"settings file|credentials file|secrets file)\b",
        re.IGNORECASE,
    ),
    "ci_cd_secrets": re.compile(
        r"\b(?:ci secret|ci secrets|cd secret|pipeline secret|pipeline secrets|"
        r"github actions secrets?|github secrets|gitlab ci variable|circleci context|"
        r"build secret|deployment secret|repository secret)\b",
        re.IGNORECASE,
    ),
    "credentials": re.compile(
        r"\b(?:credential|credentials|password|passwords|passphrase|username|"
        r"service account|service principal)\b",
        re.IGNORECASE,
    ),
    "secret_storage": re.compile(
        r"\b(?:secret|secrets|vault|kms|keychain|parameter store|secret manager|"
        r"secrets manager|sealed secret|external secrets)\b",
        re.IGNORECASE,
    ),
    "logging_redaction": re.compile(
        r"\b(?:redact|redaction|mask|masked|sanitize|sanitise|scrub|logging|logs?)\b",
        re.IGNORECASE,
    ),
}
_SIGNAL_ORDER: dict[SecretsSignal, int] = {
    "api_keys": 0,
    "tokens": 1,
    "oauth_clients": 2,
    "certificates": 3,
    "environment_variables": 4,
    "config_files": 5,
    "ci_cd_secrets": 6,
    "credentials": 7,
    "secret_storage": 8,
    "logging_redaction": 9,
}


@dataclass(frozen=True, slots=True)
class TaskSecretsExposureRecord:
    """Secrets exposure guidance for one execution task."""

    task_id: str
    task_title: str
    classification: SecretsReviewClassification
    detected_signals: tuple[SecretsSignal, ...]
    evidence_snippets: tuple[str, ...]
    checklist_items: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "classification": self.classification,
            "detected_signals": list(self.detected_signals),
            "evidence_snippets": list(self.evidence_snippets),
            "checklist_items": list(self.checklist_items),
        }


@dataclass(frozen=True, slots=True)
class TaskSecretsExposurePlan:
    """Plan-level secrets exposure guidance and rollup counts."""

    plan_id: str | None = None
    records: tuple[TaskSecretsExposureRecord, ...] = field(default_factory=tuple)
    secrets_review_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_review_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "secrets_review_task_ids": list(self.secrets_review_task_ids),
            "no_review_task_ids": list(self.no_review_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return secrets exposure records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render secrets exposure guidance as deterministic Markdown."""
        title = "# Task Secrets Exposure Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No execution tasks were found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Classification | Signals | Evidence | Checklist |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{record.classification} | "
                f"{_markdown_cell(', '.join(record.detected_signals))} | "
                f"{_markdown_cell('; '.join(record.evidence_snippets))} | "
                f"{_markdown_cell('; '.join(record.checklist_items))} |"
            )
        return "\n".join(lines)


def build_task_secrets_exposure_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskSecretsExposurePlan:
    """Build secrets exposure review guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (_task_record(task, index) for index, task in enumerate(tasks, start=1)),
            key=lambda record: (record.task_id, record.task_title),
        )
    )
    secrets_review_task_ids = tuple(
        record.task_id
        for record in records
        if record.classification == "secrets_review_required"
    )
    no_review_task_ids = tuple(
        record.task_id
        for record in records
        if record.classification == "no_secrets_review_needed"
    )
    classification_counts = {
        classification: sum(1 for record in records if record.classification == classification)
        for classification in ("secrets_review_required", "no_secrets_review_needed")
    }
    signal_counts = {
        signal: sum(1 for record in records if signal in record.detected_signals)
        for signal in _SIGNAL_ORDER
    }

    return TaskSecretsExposurePlan(
        plan_id=plan_id,
        records=records,
        secrets_review_task_ids=secrets_review_task_ids,
        no_review_task_ids=no_review_task_ids,
        summary={
            "record_count": len(records),
            "secrets_review_required_count": len(secrets_review_task_ids),
            "no_secrets_review_needed_count": len(no_review_task_ids),
            "classification_counts": classification_counts,
            "signal_counts": signal_counts,
        },
    )


def task_secrets_exposure_plan_to_dict(
    result: TaskSecretsExposurePlan,
) -> dict[str, Any]:
    """Serialize a secrets exposure plan to a plain dictionary."""
    return result.to_dict()


task_secrets_exposure_plan_to_dict.__test__ = False


def task_secrets_exposure_plan_to_markdown(
    result: TaskSecretsExposurePlan,
) -> str:
    """Render a secrets exposure plan as Markdown."""
    return result.to_markdown()


task_secrets_exposure_plan_to_markdown.__test__ = False


def summarize_task_secrets_exposure(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskSecretsExposurePlan:
    """Compatibility alias for building task secrets exposure plans."""
    return build_task_secrets_exposure_plan(source)


def _task_record(task: Mapping[str, Any], index: int) -> TaskSecretsExposureRecord:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    detected = tuple(sorted(signals, key=lambda signal: _SIGNAL_ORDER[signal]))
    has_secrets_signal = bool(detected)

    return TaskSecretsExposureRecord(
        task_id=task_id,
        task_title=title,
        classification=(
            "secrets_review_required"
            if has_secrets_signal
            else "no_secrets_review_needed"
        ),
        detected_signals=detected,
        evidence_snippets=tuple(
            _dedupe(evidence for signal in detected for evidence in signals.get(signal, ()))
        ),
        checklist_items=tuple(_checklist_items(detected, has_secrets_signal)),
    )


def _signals(task: Mapping[str, Any]) -> dict[SecretsSignal, tuple[str, ...]]:
    signals: dict[SecretsSignal, list[str]] = {}

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _add_path_signals(signals, path)

    for source_field, text in _candidate_texts(task):
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text):
                _append(signals, signal, _evidence_snippet(source_field, text))

    return {
        signal: tuple(_dedupe(evidence))
        for signal, evidence in signals.items()
        if evidence
    }


def _add_path_signals(signals: dict[SecretsSignal, list[str]], original: str) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    suffix = path.suffix
    evidence = f"files_or_modules: {original}"

    if name == ".env" or name.startswith(".env.") or suffix in {".env"}:
        _append(signals, "environment_variables", evidence)
        _append(signals, "config_files", evidence)
    if suffix in {".pem", ".crt", ".cer", ".key", ".p12", ".pfx", ".jks"}:
        _append(signals, "certificates", evidence)
    if suffix in {".tfvars", ".yml", ".yaml", ".json", ".toml", ".ini"} and (
        "secret" in name or "credential" in name or "env" in name
    ):
        _append(signals, "config_files", evidence)
    if bool(
        {
            ".github",
            "workflows",
            ".gitlab",
            "circleci",
            ".circleci",
            "ci",
            "cd",
            "pipelines",
            "deploy",
            "deployment",
        }
        & parts
    ) and any(token in normalized for token in ("secret", "token", "credential", "env")):
        _append(signals, "ci_cd_secrets", evidence)
    if bool({"secrets", "secret", "credentials", "vault", "kms"} & parts):
        _append(signals, "secret_storage", evidence)
    if any(token in name for token in ("secret", "credential", "password")):
        _append(signals, "credentials", evidence)


def _checklist_items(
    detected_signals: tuple[SecretsSignal, ...],
    has_secrets_signal: bool,
) -> list[str]:
    if not has_secrets_signal:
        return [
            "Confirm no secrets, credentials, tokens, certificates, or environment files are introduced or modified.",
            "Record the validation note with the task evidence before handoff.",
        ]

    items = [
        "Inventory every secret, credential, token, certificate, environment variable, and config file touched by the task.",
        "Store secrets only in the approved secret manager or CI/CD secret store; do not commit plaintext values.",
        "Define rotation or revocation steps for new, changed, migrated, or potentially exposed secrets.",
        "Verify logs, errors, telemetry, screenshots, and test output redact or omit secret values.",
        "Restrict read/write access using least-privilege roles, scoped tokens, and environment-specific permissions.",
        "Validate the integration with placeholder or sandbox secrets and document production verification ownership.",
    ]
    if "oauth_clients" in detected_signals:
        items.append("Confirm OAuth client IDs, client secrets, redirect URIs, and grant scopes are reviewed together.")
    if "certificates" in detected_signals:
        items.append("Confirm certificate trust chains, private-key handling, expiry monitoring, and renewal ownership.")
    if "ci_cd_secrets" in detected_signals:
        items.append("Confirm CI/CD secrets are scoped by repository, environment, branch, and deployment role.")
    return items


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
    for field_name in ("acceptance_criteria", "tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


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


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
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


def _append(
    signals: dict[SecretsSignal, list[str]],
    signal: SecretsSignal,
    evidence: str,
) -> None:
    signals.setdefault(signal, []).append(evidence)


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
    "SecretsReviewClassification",
    "SecretsSignal",
    "TaskSecretsExposurePlan",
    "TaskSecretsExposureRecord",
    "build_task_secrets_exposure_plan",
    "summarize_task_secrets_exposure",
    "task_secrets_exposure_plan_to_dict",
    "task_secrets_exposure_plan_to_markdown",
]
