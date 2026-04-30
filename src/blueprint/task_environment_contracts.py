"""Build task-level environment contracts for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class TaskEnvironmentEvidence:
    """One environment signal and the task field that contributed it."""

    signal_type: str
    value: str | int
    field: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "signal_type": self.signal_type,
            "value": self.value,
            "field": self.field,
        }


@dataclass(frozen=True, slots=True)
class TaskEnvironmentContract:
    """Environment prerequisites inferred for one execution-plan task."""

    task_id: str
    required_env_vars: tuple[str, ...] = field(default_factory=tuple)
    optional_env_vars: tuple[str, ...] = field(default_factory=tuple)
    config_paths: tuple[str, ...] = field(default_factory=tuple)
    services: tuple[str, ...] = field(default_factory=tuple)
    ports: tuple[int, ...] = field(default_factory=tuple)
    evidence: tuple[TaskEnvironmentEvidence, ...] = field(default_factory=tuple)
    missing_contract_warnings: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "required_env_vars": list(self.required_env_vars),
            "optional_env_vars": list(self.optional_env_vars),
            "config_paths": list(self.config_paths),
            "services": list(self.services),
            "ports": list(self.ports),
            "evidence": [item.to_dict() for item in self.evidence],
            "missing_contract_warnings": list(self.missing_contract_warnings),
        }


def build_task_environment_contracts(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> dict[str, TaskEnvironmentContract]:
    """Build one deterministic environment contract per task, keyed by task id."""
    payload = _plan_payload(plan)
    contracts: dict[str, TaskEnvironmentContract] = {}

    for index, task in enumerate(_task_payloads(payload.get("tasks")), start=1):
        task_id = _task_id(task, index)
        draft = _ContractDraft(task_id=task_id)
        _inspect_task(task, draft)
        contracts[task_id] = _contract_from_draft(draft)

    return contracts


def task_environment_contracts_to_dict(
    contracts: Mapping[str, TaskEnvironmentContract],
) -> dict[str, dict[str, Any]]:
    """Serialize task environment contracts to plain dictionaries."""
    return {task_id: contract.to_dict() for task_id, contract in contracts.items()}


task_environment_contracts_to_dict.__test__ = False


@dataclass
class _ContractDraft:
    task_id: str
    required_env_vars: list[str] = field(default_factory=list)
    optional_env_vars: list[str] = field(default_factory=list)
    config_paths: list[str] = field(default_factory=list)
    services: list[str] = field(default_factory=list)
    ports: list[int] = field(default_factory=list)
    evidence: list[TaskEnvironmentEvidence] = field(default_factory=list)
    secret_ambiguous_fields: list[str] = field(default_factory=list)
    config_ambiguous_fields: list[str] = field(default_factory=list)


def _inspect_task(task: Mapping[str, Any], draft: _ContractDraft) -> None:
    for field_name in ("test_command", "description"):
        text = _optional_text(task.get(field_name))
        if text:
            _inspect_text(text, field_name, draft)

    for field_name in ("acceptance_criteria", "files_or_modules"):
        for text in _strings(task.get(field_name)):
            _inspect_text(text, field_name, draft)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        _inspect_metadata(metadata, "metadata", draft)


def _inspect_metadata(value: Mapping[Any, Any], field_path: str, draft: _ContractDraft) -> None:
    for raw_key, raw_value in sorted(value.items(), key=lambda item: str(item[0])):
        key = _optional_text(raw_key)
        if not key:
            continue
        nested_path = f"{field_path}.{key}"
        normalized_key = _normalized_key(key)

        if normalized_key in _OPTIONAL_ENV_KEYS:
            _add_env_from_value(raw_value, nested_path, draft, optional=True)
        elif normalized_key in _REQUIRED_ENV_KEYS:
            _add_env_from_value(raw_value, nested_path, draft, optional=False)
        elif normalized_key in _CONFIG_PATH_KEYS:
            _add_config_paths_from_value(raw_value, nested_path, draft)
        elif normalized_key in _SERVICE_KEYS:
            _add_services_from_value(raw_value, nested_path, draft)
        elif normalized_key in _PORT_KEYS:
            _add_ports_from_value(raw_value, nested_path, draft)

        if isinstance(raw_value, Mapping):
            _inspect_mapping_keys(raw_value, normalized_key, nested_path, draft)
            _inspect_metadata(raw_value, nested_path, draft)
        else:
            for text in _strings(raw_value):
                _inspect_text(text, nested_path, draft)


def _inspect_mapping_keys(
    value: Mapping[Any, Any],
    normalized_key: str,
    field_path: str,
    draft: _ContractDraft,
) -> None:
    if normalized_key in _OPTIONAL_ENV_KEYS | _REQUIRED_ENV_KEYS:
        for nested_key, nested_value in sorted(value.items(), key=lambda item: str(item[0])):
            env_var = _env_var_from_name(nested_key)
            if env_var:
                optional = normalized_key in _OPTIONAL_ENV_KEYS or _value_marks_optional(nested_value)
                _add_env_var(env_var, field_path, draft, optional=optional)

    if normalized_key in _SERVICE_KEYS:
        for nested_key in sorted(value, key=str):
            service = _service_name(str(nested_key))
            if service:
                _add_service(service, field_path, draft)


def _inspect_text(text: str, field_name: str, draft: _ContractDraft) -> None:
    found_env = False
    found_config = False

    for env_var in _extract_env_vars(text):
        found_env = True
        _add_env_var(env_var, field_name, draft, optional=_optional_context(text, env_var))

    for config_path in _extract_config_paths(text):
        found_config = True
        _add_config_path(config_path, field_name, draft)

    for service in _extract_services(text):
        _add_service(service, field_name, draft)

    for port in _extract_ports(text):
        _add_port(port, field_name, draft)

    normalized = text.lower()
    if _SECRET_AMBIGUITY_RE.search(normalized) and not found_env and not found_config:
        draft.secret_ambiguous_fields.append(field_name)
    if _CONFIG_AMBIGUITY_RE.search(normalized) and not found_env and not found_config:
        draft.config_ambiguous_fields.append(field_name)


def _add_env_from_value(
    value: Any,
    field_name: str,
    draft: _ContractDraft,
    *,
    optional: bool,
) -> None:
    if isinstance(value, Mapping):
        for key, item in sorted(value.items(), key=lambda item: str(item[0])):
            env_var = _env_var_from_name(key)
            if env_var:
                _add_env_var(
                    env_var,
                    field_name,
                    draft,
                    optional=optional or _value_marks_optional(item),
                )
            for nested_env_var in _extract_env_vars_from_value(item):
                _add_env_var(
                    nested_env_var,
                    field_name,
                    draft,
                    optional=optional or _value_marks_optional(item),
                )
        return

    for env_var in _extract_env_vars_from_value(value):
        _add_env_var(env_var, field_name, draft, optional=optional)


def _add_config_paths_from_value(value: Any, field_name: str, draft: _ContractDraft) -> None:
    for text in _strings(value):
        for config_path in _extract_config_paths(text):
            _add_config_path(config_path, field_name, draft)


def _add_services_from_value(value: Any, field_name: str, draft: _ContractDraft) -> None:
    for text in _strings(value):
        service = _service_name(text)
        if service:
            _add_service(service, field_name, draft)
        for extracted in _extract_services(text):
            _add_service(extracted, field_name, draft)


def _add_ports_from_value(value: Any, field_name: str, draft: _ContractDraft) -> None:
    for port in _extract_ports_from_value(value):
        _add_port(port, field_name, draft)


def _add_env_var(
    env_var: str,
    field_name: str,
    draft: _ContractDraft,
    *,
    optional: bool,
) -> None:
    if optional:
        draft.optional_env_vars.append(env_var)
        _add_evidence(draft, "optional_env_var", env_var, field_name)
        return
    draft.required_env_vars.append(env_var)
    _add_evidence(draft, "required_env_var", env_var, field_name)


def _add_config_path(config_path: str, field_name: str, draft: _ContractDraft) -> None:
    draft.config_paths.append(config_path)
    _add_evidence(draft, "config_path", config_path, field_name)


def _add_service(service: str, field_name: str, draft: _ContractDraft) -> None:
    draft.services.append(service)
    _add_evidence(draft, "service", service, field_name)


def _add_port(port: int, field_name: str, draft: _ContractDraft) -> None:
    if 0 < port <= 65535:
        draft.ports.append(port)
        _add_evidence(draft, "port", port, field_name)


def _add_evidence(
    draft: _ContractDraft,
    signal_type: str,
    value: str | int,
    field_name: str,
) -> None:
    draft.evidence.append(
        TaskEnvironmentEvidence(signal_type=signal_type, value=value, field=field_name)
    )


def _contract_from_draft(draft: _ContractDraft) -> TaskEnvironmentContract:
    optional_env_vars = set(draft.optional_env_vars)
    required_env_vars = sorted(set(draft.required_env_vars) - optional_env_vars)
    config_paths = sorted(set(draft.config_paths))
    warnings: list[str] = []

    if draft.secret_ambiguous_fields and not required_env_vars and not optional_env_vars and not config_paths:
        warnings.append(
            "Secret-like requirement mentioned without an explicit env var or config path."
        )
    if draft.config_ambiguous_fields and not config_paths and not required_env_vars and not optional_env_vars:
        warnings.append(
            "Config requirement mentioned without an explicit config path or env var."
        )

    return TaskEnvironmentContract(
        task_id=draft.task_id,
        required_env_vars=tuple(required_env_vars),
        optional_env_vars=tuple(sorted(optional_env_vars)),
        config_paths=tuple(config_paths),
        services=tuple(sorted(set(draft.services))),
        ports=tuple(sorted(set(draft.ports))),
        evidence=tuple(
            sorted(
                _dedupe(draft.evidence),
                key=lambda item: (item.signal_type, str(item.value), item.field),
            )
        ),
        missing_contract_warnings=tuple(_dedupe(warnings)),
    )


def _extract_env_vars_from_value(value: Any) -> list[str]:
    values = []
    for text in _strings(value):
        env_var = _env_var_from_name(text)
        if env_var:
            values.append(env_var)
        values.extend(_extract_env_vars(text))
    return _dedupe(values)


def _extract_env_vars(text: str) -> list[str]:
    return sorted({match.group("name") for match in _ENV_VAR_RE.finditer(text)})


def _extract_config_paths(text: str) -> list[str]:
    paths: list[str] = []
    for match in _CONFIG_PATH_RE.finditer(text):
        path = _normalized_path(match.group("path"))
        if _is_config_path(path):
            paths.append(path)
    return sorted(set(paths))


def _extract_services(text: str) -> list[str]:
    tokens = set(_word_tokens(text))
    services = tokens & _KNOWN_SERVICES
    for match in _SERVICE_HINT_RE.finditer(text.lower()):
        service = _service_name(match.group("name"))
        if service:
            services.add(service)
    return sorted(services)


def _extract_ports(value: str) -> list[int]:
    ports = [int(match.group("port")) for match in _LOCALHOST_PORT_RE.finditer(value)]
    for match in _PORT_HINT_RE.finditer(value):
        ports.append(int(match.group("port")))
    return sorted(set(ports))


def _extract_ports_from_value(value: Any) -> list[int]:
    if isinstance(value, int):
        return [value]
    if isinstance(value, Mapping):
        ports: list[int] = []
        for item in value.values():
            ports.extend(_extract_ports_from_value(item))
        return sorted(set(ports))
    ports = []
    for text in _strings(value):
        if text.isdigit():
            ports.append(int(text))
        ports.extend(_extract_ports(text))
    return sorted(set(ports))


def _env_var_from_name(value: Any) -> str | None:
    text = _optional_text(value)
    if text and _ENV_VAR_NAME_RE.fullmatch(text):
        return text
    return None


def _service_name(value: str) -> str | None:
    normalized = _normalized_key(value)
    if normalized in _SERVICE_ALIASES:
        return _SERVICE_ALIASES[normalized]
    if normalized in _KNOWN_SERVICES:
        return normalized
    return None


def _optional_context(text: str, env_var: str) -> bool:
    window_start = max(0, text.find(env_var) - 40)
    window_end = min(len(text), text.find(env_var) + len(env_var) + 40)
    return bool(re.search(r"\b(optional|if available|when present)\b", text[window_start:window_end], re.I))


def _value_marks_optional(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key in ("required", "optional"):
            if key in value:
                return key == "optional" and bool(value[key]) or key == "required" and not bool(value[key])
    text = _optional_text(value)
    return bool(text and text.lower() in {"optional", "false", "not required"})


def _is_config_path(path: str) -> bool:
    path_obj = PurePosixPath(path.lower())
    return (
        path_obj.name in _CONFIG_FILENAMES
        or path_obj.name.startswith(".env.")
        or path_obj.suffix in _CONFIG_SUFFIXES
    )


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    if isinstance(plan, Mapping):
        return dict(plan)
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        pass
    return {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if isinstance(value, str):
        return " ".join(value.split())
    return ""


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for item in value.values():
            strings.extend(_strings(item))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    if isinstance(value, int):
        return [str(value)]
    return []


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _normalized_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _word_tokens(value: str) -> list[str]:
    return re.findall(r"[a-z0-9][a-z0-9-]*", value.lower())


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


_ENV_VAR_NAME_RE = re.compile(r"[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+")
_ENV_VAR_RE = re.compile(
    r"(?<![A-Z0-9_])(?:\$|\$\{)?(?P<name>[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)(?:\})?(?![A-Z0-9_])"
)
_CONFIG_PATH_RE = re.compile(
    r"(?P<path>(?:[A-Za-z0-9_.-]+/)*"
    r"(?:\.env(?:\.[A-Za-z0-9_.-]+)?|[A-Za-z0-9_.-]+\.(?:cfg|conf|env|ini|json|toml|ya?ml)))"
)
_LOCALHOST_PORT_RE = re.compile(
    r"\b(?:localhost|127\.0\.0\.1|0\.0\.0\.0|\[::1\])[:/](?P<port>[0-9]{2,5})\b"
)
_PORT_HINT_RE = re.compile(r"\bport\s+(?P<port>[0-9]{2,5})\b", re.I)
_SERVICE_HINT_RE = re.compile(r"\bservice\s*[:=]\s*(?P<name>[a-z0-9_-]+)\b")
_SECRET_AMBIGUITY_RE = re.compile(
    r"\b(secret|secrets|credential|credentials|token|api key|password)\b"
)
_CONFIG_AMBIGUITY_RE = re.compile(r"\b(config|configuration|dotenv|env file)\b")

_REQUIRED_ENV_KEYS = {
    "env",
    "env_var",
    "env_vars",
    "environment",
    "environment_variable",
    "environment_variables",
    "required_env",
    "required_env_var",
    "required_env_vars",
    "required_environment",
    "secrets",
    "secret",
}
_OPTIONAL_ENV_KEYS = {
    "optional_env",
    "optional_env_var",
    "optional_env_vars",
    "optional_environment",
    "optional_environment_variables",
}
_CONFIG_PATH_KEYS = {
    "config",
    "config_file",
    "config_files",
    "config_path",
    "config_paths",
    "dotenv",
    "dotenv_path",
    "settings_file",
}
_SERVICE_KEYS = {"service", "services", "required_services"}
_PORT_KEYS = {"port", "ports", "localhost_port", "local_ports"}
_CONFIG_FILENAMES = {
    ".env",
    ".env.local",
    ".env.production",
    "dockerfile",
    "package.json",
    "pyproject.toml",
    "settings.py",
}
_CONFIG_SUFFIXES = (".cfg", ".conf", ".env", ".ini", ".json", ".toml", ".yaml", ".yml")
_KNOWN_SERVICES = {
    "chromadb",
    "docker",
    "elasticsearch",
    "kafka",
    "memcached",
    "minio",
    "mongodb",
    "mysql",
    "opensearch",
    "postgres",
    "postgresql",
    "rabbitmq",
    "redis",
    "sqlite",
}
_SERVICE_ALIASES = {
    "postgresql": "postgres",
    "postgres_db": "postgres",
    "postgres_database": "postgres",
    "redis_cache": "redis",
}


__all__ = [
    "TaskEnvironmentContract",
    "TaskEnvironmentEvidence",
    "build_task_environment_contracts",
    "task_environment_contracts_to_dict",
]
