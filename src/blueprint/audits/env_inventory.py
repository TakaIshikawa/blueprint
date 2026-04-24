"""Environment and configuration inventory for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal


ConfigStatus = Literal["required", "optional", "unknown"]

_ENV_VAR_RE = re.compile(r"\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b")
_CONFIG_KEY_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?:[a-z][a-z0-9_]*\.)+[a-z][a-z0-9_]*(?![A-Za-z0-9_])"
)
_REQUIRED_RE = re.compile(
    r"\b(required|require|requires|must|need|needs|needed|set|provide|configure)\b",
    re.IGNORECASE,
)
_OPTIONAL_RE = re.compile(
    r"\b(optional|optionally|if configured|if present|default|fallback|override)\b",
    re.IGNORECASE,
)
_STATUS_RANK: dict[ConfigStatus, int] = {
    "unknown": 0,
    "optional": 1,
    "required": 2,
}


@dataclass(frozen=True)
class EnvInventorySource:
    """Where a configuration item was mentioned."""

    field: str
    task_id: str | None = None
    status: ConfigStatus = "unknown"

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "field": self.field,
            "status": self.status,
        }
        if self.task_id is not None:
            payload["task_id"] = self.task_id
        return payload


@dataclass(frozen=True)
class EnvInventoryItem:
    """A single environment variable or dotted configuration key."""

    name: str
    item_type: Literal["env_var", "config_key"]
    status: ConfigStatus
    sources: list[EnvInventorySource] = field(default_factory=list)

    @property
    def source_fields(self) -> list[str]:
        return sorted({source.field for source in self.sources})

    @property
    def task_ids(self) -> list[str]:
        return sorted({source.task_id for source in self.sources if source.task_id})

    @property
    def suggested_documentation(self) -> str:
        subject = "environment variable" if self.item_type == "env_var" else "configuration key"
        status_text = {
            "required": "Required",
            "optional": "Optional",
            "unknown": "Document whether this is required or optional",
        }[self.status]
        if self.status == "unknown":
            return f"{status_text}: `{self.name}` {subject} used by this plan."
        return f"{status_text} `{self.name}` {subject} used by this plan."

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.item_type,
            "status": self.status,
            "source_fields": self.source_fields,
            "task_ids": self.task_ids,
            "sources": [source.to_dict() for source in self.sources],
            "suggested_documentation": self.suggested_documentation,
        }


@dataclass(frozen=True)
class EnvInventoryResult:
    """Inventory of configuration mentioned by an execution plan."""

    plan_id: str
    brief_id: str
    items: list[EnvInventoryItem] = field(default_factory=list)

    def items_by_status(self) -> dict[ConfigStatus, list[EnvInventoryItem]]:
        return {
            "required": [item for item in self.items if item.status == "required"],
            "optional": [item for item in self.items if item.status == "optional"],
            "unknown": [item for item in self.items if item.status == "unknown"],
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "brief_id": self.brief_id,
            "items": [item.to_dict() for item in self.items],
        }


def build_env_inventory(
    implementation_brief: dict[str, Any],
    plan: dict[str, Any],
) -> EnvInventoryResult:
    """Extract environment variables and dotted config keys from plan context."""
    collector: dict[tuple[str, str], dict[str, Any]] = {}

    for field_name in (
        "data_requirements",
        "integration_points",
        "architecture_notes",
        "validation_plan",
    ):
        _scan_value(
            implementation_brief.get(field_name),
            field_name,
            task_id=None,
            collector=collector,
        )

    for task in _list_of_dicts(plan.get("tasks")):
        task_id = str(task.get("id") or "")
        task_ref = task_id or None
        _scan_value(task.get("description"), "tasks.description", task_ref, collector)
        _scan_value(
            task.get("acceptance_criteria"),
            "tasks.acceptance_criteria",
            task_ref,
            collector,
        )
        _scan_value(task.get("metadata"), "tasks.metadata", task_ref, collector)

    items = [
        EnvInventoryItem(
            name=payload["name"],
            item_type=payload["item_type"],
            status=payload["status"],
            sources=payload["sources"],
        )
        for payload in collector.values()
    ]
    items.sort(key=lambda item: item.name)

    return EnvInventoryResult(
        plan_id=str(plan.get("id") or ""),
        brief_id=str(implementation_brief.get("id") or plan.get("implementation_brief_id") or ""),
        items=items,
    )


def _scan_value(
    value: Any,
    field_name: str,
    task_id: str | None,
    collector: dict[tuple[str, str], dict[str, Any]],
) -> None:
    for text, source_field in _text_fragments(value, field_name):
        status = _infer_status(text)
        for name in sorted(_ENV_VAR_RE.findall(text)):
            _record_item(collector, name, "env_var", source_field, task_id, status)
        for name in sorted(_CONFIG_KEY_RE.findall(text)):
            _record_item(collector, name, "config_key", source_field, task_id, status)


def _record_item(
    collector: dict[tuple[str, str], dict[str, Any]],
    name: str,
    item_type: Literal["env_var", "config_key"],
    field_name: str,
    task_id: str | None,
    status: ConfigStatus,
) -> None:
    key = (item_type, name)
    payload = collector.setdefault(
        key,
        {
            "name": name,
            "item_type": item_type,
            "status": "unknown",
            "sources": [],
            "source_keys": set(),
        },
    )
    if _STATUS_RANK[status] > _STATUS_RANK[payload["status"]]:
        payload["status"] = status

    source_key = (field_name, task_id, status)
    if source_key in payload["source_keys"]:
        return
    payload["source_keys"].add(source_key)
    payload["sources"].append(
        EnvInventorySource(
            field=field_name,
            task_id=task_id,
            status=status,
        )
    )
    payload["sources"].sort(key=lambda source: (source.field, source.task_id or "", source.status))


def _infer_status(text: str) -> ConfigStatus:
    if _REQUIRED_RE.search(text):
        return "required"
    if _OPTIONAL_RE.search(text):
        return "optional"
    return "unknown"


def _text_fragments(value: Any, field_name: str) -> list[tuple[str, str]]:
    if value is None:
        return []
    if isinstance(value, str):
        return [(value, field_name)] if value.strip() else []
    if isinstance(value, dict):
        fragments: list[tuple[str, str]] = []
        for key in sorted(value):
            child_field = f"{field_name}.{key}"
            fragments.extend(_text_fragments(str(key), child_field))
            fragments.extend(_text_fragments(value[key], child_field))
        return fragments
    if isinstance(value, list):
        fragments = []
        for index, item in enumerate(value):
            fragments.extend(_text_fragments(item, f"{field_name}[{index}]"))
        return fragments
    return [(str(value), field_name)]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
