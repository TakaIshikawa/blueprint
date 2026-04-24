"""iCalendar exporter for execution plan schedules."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from blueprint.exporters.base import TargetExporter


@dataclass(frozen=True, slots=True)
class CalendarEvent:
    """A rendered iCalendar VEVENT entry."""

    uid: str
    summary: str
    description: str
    start: date | datetime
    end: date | datetime
    all_day: bool
    categories: list[str]
    source_id: str


@dataclass(frozen=True, slots=True)
class CalendarDateRange:
    """Resolved event date range."""

    start: date | datetime
    end: date | datetime
    all_day: bool


class CalendarExporter(TargetExporter):
    """Export dated execution-plan tasks and milestones to iCalendar."""

    DATE_KEYS = ("start_date", "due_date", "target_date")
    PRODUCT_ID = "-//Blueprint//Execution Plan Calendar//EN"

    def get_format(self) -> str:
        """Get export format."""
        return "icalendar"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".ics"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export an execution plan schedule to iCalendar."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w", newline="") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render iCalendar content for a validated plan and brief."""
        task_events, skipped_task_ids, invalid_task_ids = self._task_events(plan)
        milestone_events, skipped_milestones, invalid_milestones = self._milestone_events(plan)
        events = sorted(
            [*task_events, *milestone_events],
            key=lambda event: (self._sort_value(event.start), event.uid),
        )

        lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            f"PRODID:{self.PRODUCT_ID}",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            f"X-BLUEPRINT-PLAN-ID:{self._escape_text(plan['id'])}",
            f"X-BLUEPRINT-BRIEF-ID:{self._escape_text(brief['id'])}",
            f"X-BLUEPRINT-SKIPPED-TASK-COUNT:{len(skipped_task_ids)}",
            f"X-BLUEPRINT-SKIPPED-TASK-IDS:{self._escape_text(','.join(skipped_task_ids))}",
            f"X-BLUEPRINT-INVALID-TASK-DATE-IDS:{self._escape_text(','.join(invalid_task_ids))}",
            f"X-BLUEPRINT-SKIPPED-MILESTONE-COUNT:{len(skipped_milestones)}",
            f"X-BLUEPRINT-SKIPPED-MILESTONES:{self._escape_text(','.join(skipped_milestones))}",
            f"X-BLUEPRINT-INVALID-MILESTONES:{self._escape_text(','.join(invalid_milestones))}",
        ]

        for event in events:
            lines.extend(self._event_lines(event, plan["id"]))

        lines.append("END:VCALENDAR")
        return self._fold_lines(lines)

    def _task_events(
        self,
        plan: dict[str, Any],
    ) -> tuple[list[CalendarEvent], list[str], list[str]]:
        """Build task events and collect skipped task IDs."""
        events: list[CalendarEvent] = []
        skipped_task_ids: list[str] = []
        invalid_task_ids: list[str] = []

        for task in sorted(plan.get("tasks", []), key=lambda item: item["id"]):
            date_range, had_invalid_date = self._date_range(task)
            if had_invalid_date:
                invalid_task_ids.append(task["id"])
            if date_range is None:
                skipped_task_ids.append(task["id"])
                continue

            events.append(
                CalendarEvent(
                    uid=f"blueprint-{self._uid_token(plan['id'])}-{self._uid_token(task['id'])}@blueprint",
                    summary=f"{task['id']}: {task['title']}",
                    description=self._task_description(plan, task),
                    start=date_range.start,
                    end=date_range.end,
                    all_day=date_range.all_day,
                    categories=[
                        "Blueprint Task",
                        task.get("status") or "pending",
                        task.get("milestone") or "Ungrouped",
                    ],
                    source_id=task["id"],
                )
            )

        return events, skipped_task_ids, invalid_task_ids

    def _milestone_events(
        self,
        plan: dict[str, Any],
    ) -> tuple[list[CalendarEvent], list[str], list[str]]:
        """Build milestone summary events and collect skipped milestones."""
        events: list[CalendarEvent] = []
        skipped_milestones: list[str] = []
        invalid_milestones: list[str] = []
        tasks = plan.get("tasks", [])

        for index, milestone in enumerate(plan.get("milestones", []), 1):
            name = self._milestone_name(milestone, index)
            date_range, had_invalid_date = self._date_range(milestone)
            if had_invalid_date:
                invalid_milestones.append(name)
            if date_range is None:
                skipped_milestones.append(name)
                continue

            milestone_tasks = [task for task in tasks if task.get("milestone") == name]
            events.append(
                CalendarEvent(
                    uid=(
                        f"blueprint-{self._uid_token(plan['id'])}-"
                        f"milestone-{self._uid_token(name)}@blueprint"
                    ),
                    summary=f"Milestone: {name}",
                    description=self._milestone_description(plan, milestone, milestone_tasks),
                    start=date_range.start,
                    end=date_range.end,
                    all_day=date_range.all_day,
                    categories=["Blueprint Milestone"],
                    source_id=name,
                )
            )

        return events, skipped_milestones, invalid_milestones

    def _date_range(self, item: dict[str, Any]) -> tuple[CalendarDateRange | None, bool]:
        """Resolve event dates from direct fields or nested metadata."""
        start_value, start_invalid = self._date_value(item, "start_date")
        due_value, due_invalid = self._date_value(item, "due_date")
        target_value, target_invalid = self._date_value(item, "target_date")
        had_invalid_date = start_invalid or due_invalid or target_invalid

        end_source = due_value or target_value
        start_source = start_value or end_source
        if start_source is None:
            return None, had_invalid_date

        all_day = isinstance(start_source, date) and not isinstance(start_source, datetime)
        if end_source is not None:
            all_day = all_day and isinstance(end_source, date) and not isinstance(
                end_source,
                datetime,
            )

        start = start_source
        end = end_source or start_source
        if all_day:
            start_date = self._as_date(start)
            end_date = self._as_date(end)
            if end_date < start_date:
                end_date = start_date
            return CalendarDateRange(
                start=start_date,
                end=end_date + timedelta(days=1),
                all_day=True,
            ), had_invalid_date

        start_datetime = self._as_datetime(start)
        end_datetime = self._as_datetime(end)
        if end_datetime <= start_datetime:
            end_datetime = start_datetime + timedelta(hours=1)
        return CalendarDateRange(
            start=start_datetime,
            end=end_datetime,
            all_day=False,
        ), had_invalid_date

    def _date_value(self, item: dict[str, Any], key: str) -> tuple[date | datetime | None, bool]:
        """Read and parse one supported date key."""
        metadata = item.get("metadata") or {}
        raw_value = item.get(key)
        if raw_value in (None, ""):
            raw_value = metadata.get(key)
        if raw_value in (None, ""):
            return None, False

        parsed = self._parse_date(raw_value)
        return parsed, parsed is None

    def _parse_date(self, value: Any) -> date | datetime | None:
        """Parse ISO-like date and datetime values without external dependencies."""
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return value
        if not isinstance(value, str):
            return None

        candidate = value.strip()
        if not candidate:
            return None

        try:
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
                return date.fromisoformat(candidate)
            return datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _event_lines(self, event: CalendarEvent, plan_id: str) -> list[str]:
        """Render one VEVENT."""
        lines = [
            "BEGIN:VEVENT",
            f"UID:{self._escape_text(event.uid)}",
            "DTSTAMP:19700101T000000Z",
            f"SUMMARY:{self._escape_text(event.summary)}",
            f"DESCRIPTION:{self._escape_text(event.description)}",
            f"CATEGORIES:{self._escape_text(','.join(event.categories))}",
            f"X-BLUEPRINT-PLAN-ID:{self._escape_text(plan_id)}",
            f"X-BLUEPRINT-SOURCE-ID:{self._escape_text(event.source_id)}",
        ]
        if event.all_day:
            lines.extend(
                [
                    f"DTSTART;VALUE=DATE:{self._format_date(event.start)}",
                    f"DTEND;VALUE=DATE:{self._format_date(event.end)}",
                ]
            )
        else:
            lines.extend(
                [
                    f"DTSTART:{self._format_datetime(event.start)}",
                    f"DTEND:{self._format_datetime(event.end)}",
                ]
            )
        lines.append("END:VEVENT")
        return lines

    def _task_description(self, plan: dict[str, Any], task: dict[str, Any]) -> str:
        """Build task event details."""
        dependencies = ", ".join(task.get("depends_on") or []) or "none"
        criteria = "; ".join(task.get("acceptance_criteria") or []) or "none"
        return "\n".join(
            [
                f"Plan: {plan['id']}",
                f"Task: {task['id']}",
                f"Status: {task.get('status') or 'pending'}",
                f"Milestone: {task.get('milestone') or 'Ungrouped'}",
                f"Dependencies: {dependencies}",
                f"Acceptance criteria: {criteria}",
                task.get("description") or "",
            ]
        ).strip()

    def _milestone_description(
        self,
        plan: dict[str, Any],
        milestone: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> str:
        """Build milestone event details."""
        name = milestone.get("name") or milestone.get("title") or "Milestone"
        return "\n".join(
            [
                f"Plan: {plan['id']}",
                f"Milestone: {name}",
                f"Tasks: {len(tasks)}",
                milestone.get("description") or "",
            ]
        ).strip()

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Get a display name for a milestone."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"

    def _format_date(self, value: date | datetime) -> str:
        """Format an all-day iCalendar date."""
        return self._as_date(value).strftime("%Y%m%d")

    def _format_datetime(self, value: date | datetime) -> str:
        """Format an iCalendar UTC datetime."""
        value = self._as_datetime(value)
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def _as_date(self, value: date | datetime) -> date:
        """Convert a date-like value to a date."""
        if isinstance(value, datetime):
            return value.date()
        return value

    def _as_datetime(self, value: date | datetime) -> datetime:
        """Convert a date-like value to a datetime."""
        if isinstance(value, datetime):
            return value
        return datetime.combine(value, time(9, 0), tzinfo=timezone.utc)

    def _sort_value(self, value: date | datetime) -> datetime:
        """Normalize dates for deterministic sorting."""
        return self._as_datetime(value)

    def _uid_token(self, value: str) -> str:
        """Normalize a stable ID token for UID fields."""
        return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-") or "item"

    def _escape_text(self, value: str) -> str:
        """Escape iCalendar text values."""
        return (
            str(value)
            .replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "")
            .replace(";", "\\;")
            .replace(",", "\\,")
        )

    def _fold_lines(self, lines: list[str]) -> str:
        """Fold long iCalendar content lines and use CRLF line endings."""
        folded: list[str] = []
        for line in lines:
            current = line
            while len(current.encode("utf-8")) > 75:
                cut = self._fold_cut(current, 75)
                folded.append(current[:cut])
                current = " " + current[cut:]
            folded.append(current)
        return "\r\n".join(folded) + "\r\n"

    def _fold_cut(self, value: str, byte_limit: int) -> int:
        """Find a character boundary for a folded line."""
        total = 0
        for index, character in enumerate(value):
            total += len(character.encode("utf-8"))
            if total > byte_limit:
                return index
        return len(value)
