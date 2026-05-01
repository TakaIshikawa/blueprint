"""PagerDuty escalation digest exporter for execution plans."""

from __future__ import annotations

import re
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any

from blueprint.exporters.base import TargetExporter
from blueprint.validation_commands import flatten_validation_commands


BUCKETS = ("immediate", "scheduled", "watchlist", "informational")
HIGH_RISK_LEVELS = {"blocker", "critical", "high", "p0", "p1", "sev0", "sev1"}
WATCH_RISK_LEVELS = {"medium", "moderate", "p2", "sev2"}
IMMEDIATE_SIGNALS = {"blocked", "incident", "outage", "page", "sev0", "sev1", "critical"}
SCHEDULED_SIGNALS = {
    "deploy",
    "deployment",
    "external",
    "integration",
    "migration",
    "rollback",
    "runbook",
    "third-party",
    "vendor",
    "webhook",
}
WATCHLIST_SIGNALS = {
    "api",
    "backfill",
    "database",
    "dependency",
    "feature-flag",
    "ops",
    "queue",
    "risk",
    "schema",
}


class PagerDutyDigestExporter(TargetExporter):
    """Export operational escalation buckets as deterministic Markdown."""

    def get_format(self) -> str:
        """Get export format."""
        return "markdown"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".md"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export a PagerDuty escalation digest to Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render the PagerDuty escalation digest Markdown."""
        tasks = plan.get("tasks", [])
        buckets = self._bucket_tasks(tasks)
        counts = " | ".join(f"{bucket}: {len(buckets[bucket])}" for bucket in BUCKETS)

        lines = [
            f"# PagerDuty Escalation Digest: {plan['id']}",
            f"- Brief: {self._text(brief['title'])} (`{self._code(brief['id'])}`)",
            f"- Plan: `{self._code(plan['id'])}`",
            f"- Escalation buckets: {counts}",
            "",
        ]

        for bucket in BUCKETS:
            lines.append(f"## {bucket.title()}")
            lines.extend(self._task_lines(buckets[bucket]))
            lines.append("")

        return "\n".join(lines).rstrip() + "\n"

    def _bucket_tasks(
        self,
        tasks: list[dict[str, Any]],
    ) -> "OrderedDict[str, list[dict[str, Any]]]":
        bucketed: "OrderedDict[str, list[dict[str, Any]]]" = OrderedDict(
            (bucket, []) for bucket in BUCKETS
        )
        for task in tasks:
            bucketed[self._bucket_name(task)].append(task)
        return bucketed

    def _bucket_name(self, task: Mapping[str, Any]) -> str:
        status = self._value(task.get("status") or "pending").lower()
        risk = self._risk_level(task).lower()
        signals = set(self._risk_signals(task))

        if status == "blocked" or risk in {"blocker", "critical"} or signals & IMMEDIATE_SIGNALS:
            return "immediate"
        if status not in {"completed", "skipped"} and (
            risk in HIGH_RISK_LEVELS or signals & SCHEDULED_SIGNALS
        ):
            return "scheduled"
        if status not in {"completed", "skipped"} and (
            risk in WATCH_RISK_LEVELS or signals & WATCHLIST_SIGNALS
        ):
            return "watchlist"
        return "informational"

    def _task_lines(self, tasks: list[dict[str, Any]]) -> list[str]:
        if not tasks:
            return ["- None."]

        lines: list[str] = []
        for task in tasks:
            task_id = self._value(task.get("id"))
            title = self._value(task.get("title"))
            lines.append(f"- `{self._code(task_id)}` {self._text(title)}")
            lines.append(f"  - owner_type: {self._text(self._owner_type(task))}")
            lines.append(f"  - risk_level: {self._text(self._risk_level(task))}")
            lines.append(f"  - blocked_reason: {self._text(self._blocked_reason(task))}")
            lines.append(
                "  - risk_signals: "
                + self._text(", ".join(self._risk_signals(task)) or "none")
            )
            lines.append(
                "  - suggested_validation: "
                + self._text("; ".join(self._validation_commands(task)) or "Not specified")
            )
            lines.append(
                "  - runbook_or_rollback: "
                + self._text("; ".join(self._runbook_or_rollback_hints(task)) or "Not specified")
            )
        return lines

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

    def _blocked_reason(self, task: Mapping[str, Any]) -> str:
        metadata = self._metadata(task)
        return (
            self._optional_text(task.get("blocked_reason"))
            or self._optional_text(metadata.get("blocked_reason"))
            or "None"
        )

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

    def _risk_signals(self, task: Mapping[str, Any]) -> list[str]:
        context = " ".join(
            [
                self._value(task.get("status") or "pending"),
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
        signals = sorted(tokens & (IMMEDIATE_SIGNALS | SCHEDULED_SIGNALS | WATCHLIST_SIGNALS))
        if self._risk_level(task).lower() in HIGH_RISK_LEVELS:
            signals.append("high-risk")
        return self._dedupe(signals)

    def _signal_metadata(self, task: Mapping[str, Any]) -> list[str]:
        metadata = self._metadata(task)
        values: list[str] = []
        for key in ("priority", "risk", "risk_level", "risk_signals", "labels", "tags"):
            values.extend(self._strings(metadata.get(key)))
        return values

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

    def _optional_text(self, value: Any) -> str | None:
        text = self._value(value)
        return text or None

    def _value(self, value: Any) -> str:
        return str(value).strip() if value is not None else ""

    def _text(self, value: str) -> str:
        return value.replace("\n", " ").strip()

    def _code(self, value: str) -> str:
        return value.replace("`", "\\`")

    def _dedupe(self, values: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []
        for value in values:
            if value and value not in seen:
                seen.add(value)
                deduped.append(value)
        return deduped
