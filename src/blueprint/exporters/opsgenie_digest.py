"""Opsgenie escalation digest exporter for execution plans."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from blueprint.exporters.base import TargetExporter
from blueprint.validation_commands import flatten_validation_commands


SCHEMA_VERSION = "blueprint.opsgenie_digest.v1"

SECTION_ORDER = (
    "blocked",
    "high_risk",
    "production_touching",
    "rollback_sensitive",
    "dependency_sensitive",
)
HIGH_RISK_LEVELS = {"blocker", "critical", "high", "highest", "p0", "p1", "sev0", "sev1"}
MEDIUM_RISK_LEVELS = {"medium", "moderate", "p2", "sev2"}
PRODUCTION_SIGNALS = {
    "deploy",
    "deployment",
    "prod",
    "production",
    "release",
    "rollout",
}
ROLLBACK_SIGNALS = {
    "backout",
    "revert",
    "rollback",
    "restore",
}
DEPENDENCY_SIGNALS = {
    "api",
    "database",
    "dependency",
    "external",
    "integration",
    "queue",
    "schema",
    "third-party",
    "vendor",
    "webhook",
}


class OpsgenieDigestExporter(TargetExporter):
    """Export Opsgenie-oriented escalation context as deterministic JSON."""

    def get_format(self) -> str:
        """Get export format."""
        return "json"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".json"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export an Opsgenie escalation digest to JSON."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        payload = self.render_payload(plan, brief)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
            f.write("\n")

        return output_path

    def render_payload(self, plan: dict[str, Any], brief: dict[str, Any]) -> dict[str, Any]:
        """Render a validated execution plan and brief as an Opsgenie digest."""
        tasks = plan.get("tasks", [])
        sections = self._sections(tasks)
        return {
            "schema_version": SCHEMA_VERSION,
            "plan_metadata": self._plan_metadata(plan, brief, tasks),
            "routing_hints": self._plan_routing_hints(plan, brief),
            "sections": sections,
            "summary": {
                "escalation_task_count": len(
                    {task["id"] for section in sections for task in section["tasks"]}
                ),
                "section_counts": {
                    section["key"]: len(section["tasks"]) for section in sections
                },
                "task_count": len(tasks),
            },
        }

    def _plan_metadata(
        self,
        plan: Mapping[str, Any],
        brief: Mapping[str, Any],
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        milestones = [
            self._milestone_name(milestone, index)
            for index, milestone in enumerate(plan.get("milestones", []), 1)
        ]
        return {
            "brief_id": brief["id"],
            "brief_title": self._text(brief["title"]),
            "plan_id": plan["id"],
            "plan_status": self._value(plan.get("status") or "unknown"),
            "project_type": self._value(plan.get("project_type") or "unspecified"),
            "target_engine": self._value(plan.get("target_engine") or "unspecified"),
            "target_repo": self._value(plan.get("target_repo") or "unspecified"),
            "milestones": milestones,
            "task_count": len(tasks),
        }

    def _plan_routing_hints(
        self,
        plan: Mapping[str, Any],
        brief: Mapping[str, Any],
    ) -> dict[str, Any]:
        integration_points = self._strings(brief.get("integration_points"))
        return {
            "alias": f"{plan['id']}:opsgenie-digest",
            "default_priority": "P3",
            "message": f"{self._text(brief['title'])} escalation digest for {plan['id']}",
            "source": "blueprint",
            "tags": self._dedupe(
                [
                    "blueprint",
                    "execution-plan",
                    self._value(plan.get("target_engine")),
                    self._value(plan.get("project_type")),
                    *integration_points,
                ]
            ),
        }

    def _sections(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "key": key,
                "title": self._section_title(key),
                "priority": self._section_priority(key),
                "routing_hint": self._section_routing_hint(key),
                "tasks": [self._task_summary(task) for task in tasks if self._matches(key, task)],
            }
            for key in SECTION_ORDER
        ]

    def _matches(self, section: str, task: Mapping[str, Any]) -> bool:
        if section == "blocked":
            return self._status(task) == "blocked" or bool(self._blocked_reason(task))
        if section == "high_risk":
            return self._risk_level(task).lower() in HIGH_RISK_LEVELS
        if section == "production_touching":
            return bool(set(self._risk_signals(task)) & PRODUCTION_SIGNALS)
        if section == "rollback_sensitive":
            return bool(
                set(self._risk_signals(task)) & ROLLBACK_SIGNALS
                or self._runbook_or_rollback_hints(task)
            )
        if section == "dependency_sensitive":
            return bool(task.get("depends_on") or set(self._risk_signals(task)) & DEPENDENCY_SIGNALS)
        return False

    def _task_summary(self, task: Mapping[str, Any]) -> dict[str, Any]:
        dependencies = self._strings(task.get("depends_on"))
        signals = self._risk_signals(task)
        return {
            "id": self._value(task.get("id")),
            "title": self._compact_text(self._value(task.get("title")), 120),
            "status": self._status(task),
            "milestone": self._value(task.get("milestone") or "Ungrouped"),
            "owner_type": self._owner_type(task),
            "priority": self._priority(task),
            "risk_level": self._risk_level(task),
            "responders": self._responders(task),
            "routing_hint": self._task_routing_hint(task, signals),
            "dependencies": dependencies,
            "signals": signals,
            "evidence": self._evidence(task),
        }

    def _evidence(self, task: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "acceptance_criteria": self._strings(task.get("acceptance_criteria"))[:5],
            "blocked_reason": self._blocked_reason(task),
            "files_or_modules": self._strings(task.get("files_or_modules"))[:8],
            "runbook_or_rollback": self._runbook_or_rollback_hints(task)[:5],
            "validation_commands": self._validation_commands(task)[:5],
        }

    def _task_routing_hint(self, task: Mapping[str, Any], signals: list[str]) -> dict[str, Any]:
        metadata = self._metadata(task)
        tags = self._dedupe(
            [
                "blueprint-task",
                self._status(task),
                self._risk_level(task),
                *signals,
                *self._strings(metadata.get("opsgenie_tags")),
                *self._strings(metadata.get("tags")),
                *self._strings(metadata.get("labels")),
            ]
        )
        return {
            "alias": self._value(task.get("id")),
            "entity": self._value(task.get("milestone") or "Ungrouped"),
            "message": (
                f"{self._priority(task)} {self._value(task.get('id'))}: "
                f"{self._compact_text(self._value(task.get('title')), 100)}"
            ),
            "team": self._team(task),
            "tags": tags,
        }

    def _priority(self, task: Mapping[str, Any]) -> str:
        explicit = self._explicit_priority(task)
        if explicit:
            return explicit
        risk = self._risk_level(task).lower()
        signals = set(self._risk_signals(task))
        if self._status(task) == "blocked" and risk in HIGH_RISK_LEVELS:
            return "P1"
        if risk in {"blocker", "critical", "p0", "sev0"}:
            return "P1"
        if self._status(task) == "blocked" or risk in HIGH_RISK_LEVELS:
            return "P2"
        if signals & (PRODUCTION_SIGNALS | ROLLBACK_SIGNALS):
            return "P2"
        if task.get("depends_on") or risk in MEDIUM_RISK_LEVELS or signals & DEPENDENCY_SIGNALS:
            return "P3"
        return "P4"

    def _explicit_priority(self, task: Mapping[str, Any]) -> str:
        metadata = self._metadata(task)
        for value in (
            task.get("priority"),
            metadata.get("opsgenie_priority"),
            metadata.get("priority"),
            metadata.get("pagerduty_priority"),
        ):
            priority = self._optional_text(value)
            if priority:
                return priority.upper()
        return ""

    def _responders(self, task: Mapping[str, Any]) -> list[dict[str, str]]:
        metadata = self._metadata(task)
        responders = self._strings(
            metadata.get("opsgenie_responders")
            or metadata.get("responders")
            or metadata.get("oncall")
        )
        if not responders:
            team = self._team(task)
            responders = [team] if team else [self._owner_type(task)]

        return [
            {"type": self._responder_type(responder), "name": responder}
            for responder in self._dedupe(responders)
        ]

    def _team(self, task: Mapping[str, Any]) -> str:
        metadata = self._metadata(task)
        return (
            self._optional_text(metadata.get("opsgenie_team"))
            or self._optional_text(metadata.get("team"))
            or self._optional_text(metadata.get("owner"))
            or self._owner_type(task)
        )

    def _responder_type(self, responder: str) -> str:
        lowered = responder.lower()
        if "@" in responder:
            return "user"
        if "schedule" in lowered:
            return "schedule"
        if "escalation" in lowered or "policy" in lowered:
            return "escalation"
        return "team"

    def _risk_signals(self, task: Mapping[str, Any]) -> list[str]:
        context = " ".join(
            [
                self._status(task),
                self._value(task.get("title")),
                self._value(task.get("description")),
                self._value(task.get("milestone")),
                self._risk_level(task),
                self._blocked_reason(task),
                *self._strings(task.get("files_or_modules")),
                *self._strings(task.get("acceptance_criteria")),
                *self._signal_metadata(task),
            ]
        )
        tokens = set(re.findall(r"[a-z0-9][a-z0-9-]*", context.lower()))
        signals = sorted(tokens & (PRODUCTION_SIGNALS | ROLLBACK_SIGNALS | DEPENDENCY_SIGNALS))
        if self._risk_level(task).lower() in HIGH_RISK_LEVELS:
            signals.append("high-risk")
        if self._status(task) == "blocked":
            signals.append("blocked")
        return self._dedupe(signals)

    def _signal_metadata(self, task: Mapping[str, Any]) -> list[str]:
        metadata = self._metadata(task)
        values: list[str] = []
        for key in (
            "priority",
            "risk",
            "risk_level",
            "risk_signals",
            "labels",
            "tags",
            "environment",
            "opsgenie_tags",
        ):
            values.extend(self._strings(metadata.get(key)))
        return values

    def _validation_commands(self, task: Mapping[str, Any]) -> list[str]:
        commands: list[str] = []
        for key in ("test_command", "suggested_test_command", "validation_command"):
            command = self._optional_text(task.get(key))
            if command:
                commands.append(command)

        metadata = self._metadata(task)
        for key in ("validation_commands", "validation_command", "test_commands"):
            commands.extend(self._commands_from_value(metadata.get(key)))
        return self._dedupe(commands)

    def _runbook_or_rollback_hints(self, task: Mapping[str, Any]) -> list[str]:
        metadata = self._metadata(task)
        hints: list[str] = []
        for key in (
            "runbook",
            "runbook_hint",
            "runbook_hints",
            "runbook_url",
            "runbook_link",
            "rollback",
            "rollback_hint",
            "rollback_hints",
            "rollback_plan",
            "rollback_strategy",
            "rollback_steps",
            "restore_plan",
        ):
            hints.extend(self._strings(metadata.get(key)))
        return self._dedupe(hints)

    def _blocked_reason(self, task: Mapping[str, Any]) -> str:
        metadata = self._metadata(task)
        return (
            self._optional_text(task.get("blocked_reason"))
            or self._optional_text(metadata.get("blocked_reason"))
            or ""
        )

    def _owner_type(self, task: Mapping[str, Any]) -> str:
        metadata = self._metadata(task)
        return (
            self._optional_text(task.get("owner_type"))
            or self._optional_text(metadata.get("owner_type"))
            or "unassigned"
        )

    def _risk_level(self, task: Mapping[str, Any]) -> str:
        metadata = self._metadata(task)
        return (
            self._optional_text(task.get("risk_level"))
            or self._optional_text(metadata.get("risk_level"))
            or self._optional_text(metadata.get("risk"))
            or "unspecified"
        )

    def _status(self, task: Mapping[str, Any]) -> str:
        return self._value(task.get("status") or "pending")

    def _commands_from_value(self, value: Any) -> list[str]:
        if isinstance(value, Mapping):
            return flatten_validation_commands(value)
        return self._strings(value)

    def _metadata(self, task: Mapping[str, Any]) -> Mapping[str, Any]:
        metadata = task.get("metadata")
        return metadata if isinstance(metadata, Mapping) else {}

    def _strings(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            text = self._optional_text(value)
            return [text] if text else []
        if isinstance(value, Mapping):
            strings: list[str] = []
            for item in value.values():
                strings.extend(self._strings(item))
            return strings
        if isinstance(value, (list, tuple, set)):
            strings = []
            for item in value:
                strings.extend(self._strings(item))
            return strings
        text = self._optional_text(value)
        return [text] if text else []

    def _milestone_name(self, milestone: Any, index: int) -> str:
        if isinstance(milestone, Mapping):
            return self._value(milestone.get("name") or milestone.get("title") or f"Milestone {index}")
        return self._value(milestone or f"Milestone {index}")

    def _section_title(self, key: str) -> str:
        return {
            "blocked": "Blocked Tasks",
            "high_risk": "High-Risk Tasks",
            "production_touching": "Production-Touching Tasks",
            "rollback_sensitive": "Rollback-Sensitive Tasks",
            "dependency_sensitive": "Dependency-Sensitive Tasks",
        }[key]

    def _section_priority(self, key: str) -> str:
        return {
            "blocked": "P1",
            "high_risk": "P1",
            "production_touching": "P2",
            "rollback_sensitive": "P2",
            "dependency_sensitive": "P3",
        }[key]

    def _section_routing_hint(self, key: str) -> str:
        return {
            "blocked": "Page the current owner or escalation policy before work continues.",
            "high_risk": "Route to the primary service owner for approval and monitoring.",
            "production_touching": "Coordinate with release management and production on-call.",
            "rollback_sensitive": "Keep rollback owner and runbook context attached to the alert.",
            "dependency_sensitive": "Notify dependency owners before sequencing downstream work.",
        }[key]

    def _optional_text(self, value: Any) -> str | None:
        text = self._value(value)
        return text or None

    def _value(self, value: Any) -> str:
        return str(value).strip() if value is not None else ""

    def _text(self, value: str) -> str:
        return value.replace("\n", " ").strip()

    def _compact_text(self, value: str, max_length: int) -> str:
        text = self._text(value)
        if len(text) <= max_length:
            return text
        return text[: max_length - 1].rstrip() + "..."

    def _dedupe(self, values: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []
        for value in values:
            if value and value not in seen:
                seen.add(value)
                deduped.append(value)
        return deduped
