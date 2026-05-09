"""Email digest exporter for execution plan status reports."""

from __future__ import annotations

import re
import smtplib
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Literal

from blueprint.exporters.base import TargetExporter

DigestFrequency = Literal["daily", "weekly", "per_milestone"]

RecipientRole = Literal["stakeholders", "team_members", "management"]

EmailProvider = Literal["smtp", "sendgrid", "ses"]


@dataclass(frozen=True, slots=True)
class SmtpSettings:
    """SMTP server connection settings."""

    host: str = "localhost"
    port: int = 587
    username: str = ""
    password: str = ""
    use_tls: bool = True


@dataclass(frozen=True, slots=True)
class EmailRecipient:
    """A single email recipient with role metadata."""

    email: str
    name: str = ""
    role: RecipientRole = "team_members"
    unsubscribed: bool = False


@dataclass(frozen=True, slots=True)
class EmailDigestConfig:
    """Configuration for email digest delivery."""

    sender_email: str = "noreply@blueprint.dev"
    sender_name: str = "Blueprint Digest"
    recipients: tuple[EmailRecipient, ...] = ()
    frequency: DigestFrequency = "daily"
    provider: EmailProvider = "smtp"
    smtp: SmtpSettings = field(default_factory=SmtpSettings)
    sendgrid_api_key: str = ""
    ses_region: str = "us-east-1"
    ses_access_key: str = ""
    ses_secret_key: str = ""
    include_attachments: bool = False
    subject_prefix: str = "[Blueprint]"
    unsubscribe_url: str = ""
    locale: str = "en"

    def recipients_by_role(self, *roles: RecipientRole) -> tuple[EmailRecipient, ...]:
        """Return active recipients matching any of the given roles."""
        return tuple(
            r for r in self.recipients if r.role in roles and not r.unsubscribed
        )

    def active_recipients(self) -> tuple[EmailRecipient, ...]:
        """Return all recipients that have not unsubscribed."""
        return tuple(r for r in self.recipients if not r.unsubscribed)


SCHEMA_VERSION = "blueprint.email_digest.v1"

STATUS_ORDER = ("pending", "in_progress", "completed", "blocked", "skipped")

_STATUS_COLORS = {
    "pending": "#808080",
    "in_progress": "#3498db",
    "completed": "#2ecc71",
    "blocked": "#e74c3c",
    "skipped": "#95a5a6",
}

_STATUS_LABELS = {
    "pending": "Pending",
    "in_progress": "In Progress",
    "completed": "Completed",
    "blocked": "Blocked",
    "skipped": "Skipped",
}


class EmailExporter(TargetExporter):
    """Export execution plan digests as HTML email messages."""

    MAX_SUBJECT_LENGTH = 150
    MAX_TASK_TITLE_LENGTH = 120
    MAX_REASON_LENGTH = 200
    MAX_RECIPIENTS_PER_BATCH = 50

    def __init__(self, config: EmailDigestConfig | None = None) -> None:
        self.config = config or EmailDigestConfig()

    def get_format(self) -> str:
        return "html"

    def get_extension(self) -> str:
        return ".html"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export execution plan email digest to an HTML file."""
        plan, brief = self.validate_export_payload(
            execution_plan, implementation_brief
        )
        self.ensure_output_dir(output_path)

        html = self.render_html(plan, brief)
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(html)

        return output_path

    # ── rendering ────────────────────────────────────────────────

    def render_html(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render a full HTML email body for the given plan and brief."""
        tasks = plan.get("tasks", [])

        subject = self.build_subject(plan, brief)
        executive = self._executive_summary(plan, brief, tasks)
        status_table = self._status_table(tasks)
        blocker_section = self._blocker_section(tasks)
        milestone_section = self._milestone_sections_html(plan, tasks)
        task_table = self._task_table(tasks)
        footer = self._footer()

        return _EMAIL_TEMPLATE.format(
            subject=_esc(subject),
            executive_summary=executive,
            status_table=status_table,
            blocker_section=blocker_section,
            milestone_section=milestone_section,
            task_table=task_table,
            footer=footer,
        )

    def render_payload(self, plan: dict[str, Any], brief: dict[str, Any]) -> dict[str, Any]:
        """Render a JSON-serialisable payload describing the email digest."""
        tasks = plan.get("tasks", [])
        return {
            "schema_version": SCHEMA_VERSION,
            "subject": self.build_subject(plan, brief),
            "sender": {
                "email": self.config.sender_email,
                "name": self.config.sender_name,
            },
            "recipients": [
                {"email": r.email, "name": r.name, "role": r.role}
                for r in self.config.active_recipients()
            ],
            "frequency": self.config.frequency,
            "provider": self.config.provider,
            "plan_id": plan.get("id", ""),
            "brief_id": brief.get("id", ""),
            "brief_title": brief.get("title", ""),
            "task_count": len(tasks),
            "status_counts": dict(Counter(t.get("status", "pending") for t in tasks)),
            "progress_pct": self._progress_pct(tasks),
            "blockers": [
                {"id": t["id"], "title": t.get("title", ""), "reason": self._blocked_reason(t)}
                for t in tasks
                if t.get("status") == "blocked"
            ],
            "html_body": self.render_html(plan, brief),
        }

    # ── delivery ─────────────────────────────────────────────────

    def send(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        *,
        roles: tuple[RecipientRole, ...] | None = None,
        attachments: list[tuple[str, bytes]] | None = None,
    ) -> dict[str, Any]:
        """Build and send the email digest to configured recipients.

        Args:
            plan: Validated execution plan dict.
            brief: Validated implementation brief dict.
            roles: Restrict delivery to these roles.  ``None`` means all active.
            attachments: Optional list of ``(filename, content_bytes)`` pairs.

        Returns:
            A dict with ``success``, ``recipients_count``, and ``provider`` keys
            (or ``error`` on failure).
        """
        recipients = (
            self.config.recipients_by_role(*roles)
            if roles
            else self.config.active_recipients()
        )

        if not recipients:
            return {"error": "No active recipients for the given roles"}

        html = self.render_html(plan, brief)
        subject = self.build_subject(plan, brief)

        return self._deliver(subject, html, recipients, attachments)

    def build_subject(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Build the email subject line."""
        title = brief.get("title", "Execution Plan")
        progress = self._progress_pct(plan.get("tasks", []))
        raw = f"{self.config.subject_prefix} {title} — {progress:.0f}% complete"
        return _compact(raw, self.MAX_SUBJECT_LENGTH)

    # ── private: HTML fragments ──────────────────────────────────

    def _executive_summary(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> str:
        progress = self._progress_pct(tasks)
        blocked = [t for t in tasks if t.get("status") == "blocked"]
        upcoming = self._upcoming_milestones(plan, tasks)

        lines = [
            "<h2>Executive Summary</h2>",
            '<table role="presentation" width="100%" cellpadding="8" '
            'cellspacing="0" style="border-collapse:collapse;">',
            f"<tr><td><strong>Plan</strong></td><td>{_esc(plan.get('id', ''))}</td></tr>",
            f"<tr><td><strong>Brief</strong></td><td>{_esc(brief.get('title', ''))}</td></tr>",
            f"<tr><td><strong>Progress</strong></td><td>{progress:.1f}%</td></tr>",
            f"<tr><td><strong>Total Tasks</strong></td><td>{len(tasks)}</td></tr>",
            f"<tr><td><strong>Blockers</strong></td><td>{len(blocked)}</td></tr>",
        ]
        if upcoming:
            lines.append(
                f"<tr><td><strong>Upcoming Milestones</strong></td>"
                f"<td>{_esc(', '.join(upcoming))}</td></tr>"
            )
        lines.append("</table>")
        return "\n".join(lines)

    def _status_table(self, tasks: list[dict[str, Any]]) -> str:
        counts = Counter(t.get("status", "pending") for t in tasks)
        rows = []
        for status in STATUS_ORDER:
            count = counts.get(status, 0)
            color = _STATUS_COLORS.get(status, "#808080")
            label = _STATUS_LABELS.get(status, status)
            rows.append(
                f'<tr><td style="color:{color};font-weight:bold;">{_esc(label)}</td>'
                f"<td>{count}</td></tr>"
            )
        return (
            "<h2>Status Overview</h2>"
            '<table role="presentation" width="100%" cellpadding="6" '
            'cellspacing="0" style="border-collapse:collapse;border:1px solid #ddd;">'
            "<tr><th>Status</th><th>Count</th></tr>"
            + "".join(rows)
            + "</table>"
        )

    def _blocker_section(self, tasks: list[dict[str, Any]]) -> str:
        blocked = [t for t in tasks if t.get("status") == "blocked"]
        if not blocked:
            return "<h2>Blockers</h2><p>No blockers.</p>"

        rows = []
        for t in blocked:
            reason = _esc(_compact(self._blocked_reason(t), self.MAX_REASON_LENGTH))
            rows.append(
                f"<tr><td>{_esc(t['id'])}</td>"
                f"<td>{_esc(_compact(t.get('title', ''), self.MAX_TASK_TITLE_LENGTH))}</td>"
                f"<td>{reason}</td></tr>"
            )
        return (
            "<h2>Blockers</h2>"
            '<table role="presentation" width="100%" cellpadding="6" '
            'cellspacing="0" style="border-collapse:collapse;border:1px solid #ddd;">'
            "<tr><th>ID</th><th>Task</th><th>Reason</th></tr>"
            + "".join(rows)
            + "</table>"
        )

    def _milestone_sections_html(
        self, plan: dict[str, Any], tasks: list[dict[str, Any]]
    ) -> str:
        grouped = self._group_by_milestone(tasks)
        if not grouped:
            return "<h2>Milestones</h2><p>No milestones.</p>"

        order = self._milestone_order(plan)
        ordered_names = [n for n in order if n in grouped] + [
            n for n in grouped if n not in order
        ]

        parts: list[str] = ["<h2>Milestones</h2>"]
        for name in ordered_names:
            mtasks = grouped[name]
            counts = Counter(t.get("status", "pending") for t in mtasks)
            completed = counts.get("completed", 0)
            total = len(mtasks)
            pct = (completed / total * 100) if total else 0
            parts.append(
                f"<h3>{_esc(name)}</h3>"
                f"<p>{completed}/{total} completed ({pct:.0f}%), "
                f"{counts.get('blocked', 0)} blocked</p>"
            )
        return "\n".join(parts)

    def _task_table(self, tasks: list[dict[str, Any]]) -> str:
        if not tasks:
            return "<h2>Tasks</h2><p>No tasks.</p>"

        rows = []
        for t in tasks:
            status = t.get("status", "pending")
            color = _STATUS_COLORS.get(status, "#808080")
            label = _STATUS_LABELS.get(status, status)
            title = _esc(_compact(t.get("title", ""), self.MAX_TASK_TITLE_LENGTH))
            rows.append(
                f"<tr><td>{_esc(t['id'])}</td>"
                f"<td>{title}</td>"
                f'<td style="color:{color};">{_esc(label)}</td>'
                f"<td>{_esc(t.get('milestone', '') or 'N/A')}</td></tr>"
            )
        return (
            "<h2>Tasks</h2>"
            '<table role="presentation" width="100%" cellpadding="6" '
            'cellspacing="0" style="border-collapse:collapse;border:1px solid #ddd;">'
            "<tr><th>ID</th><th>Title</th><th>Status</th><th>Milestone</th></tr>"
            + "".join(rows)
            + "</table>"
        )

    def _footer(self) -> str:
        parts = ['<p style="font-size:12px;color:#888;">']
        parts.append(f"Sent by {_esc(self.config.sender_name)}.")
        if self.config.unsubscribe_url:
            parts.append(
                f' <a href="{_esc(self.config.unsubscribe_url)}">Unsubscribe</a>.'
            )
        parts.append("</p>")
        return "".join(parts)

    # ── private: delivery backends ───────────────────────────────

    def _deliver(
        self,
        subject: str,
        html: str,
        recipients: tuple[EmailRecipient, ...],
        attachments: list[tuple[str, bytes]] | None,
    ) -> dict[str, Any]:
        provider = self.config.provider

        if provider == "smtp":
            return self._deliver_smtp(subject, html, recipients, attachments)
        elif provider == "sendgrid":
            return self._deliver_sendgrid(subject, html, recipients, attachments)
        elif provider == "ses":
            return self._deliver_ses(subject, html, recipients, attachments)
        else:
            return {"error": f"Unsupported provider: {provider}"}

    def _deliver_smtp(
        self,
        subject: str,
        html: str,
        recipients: tuple[EmailRecipient, ...],
        attachments: list[tuple[str, bytes]] | None,
    ) -> dict[str, Any]:
        smtp_cfg = self.config.smtp
        to_addrs = [r.email for r in recipients]

        msg = self._build_mime_message(subject, html, to_addrs, attachments)

        try:
            if smtp_cfg.use_tls:
                server = smtplib.SMTP(smtp_cfg.host, smtp_cfg.port)
                server.starttls()
            else:
                server = smtplib.SMTP(smtp_cfg.host, smtp_cfg.port)

            if smtp_cfg.username:
                server.login(smtp_cfg.username, smtp_cfg.password)

            server.sendmail(self.config.sender_email, to_addrs, msg.as_string())
            server.quit()

            return {
                "success": True,
                "provider": "smtp",
                "recipients_count": len(to_addrs),
            }
        except Exception as exc:
            return {"error": str(exc), "provider": "smtp"}

    def _deliver_sendgrid(
        self,
        _subject: str,
        _html: str,
        recipients: tuple[EmailRecipient, ...],
        _attachments: list[tuple[str, bytes]] | None,
    ) -> dict[str, Any]:
        """Placeholder for SendGrid delivery.

        In production, use the sendgrid Python SDK.
        """
        if not self.config.sendgrid_api_key:
            return {"error": "SendGrid API key not configured", "provider": "sendgrid"}

        return {
            "success": True,
            "provider": "sendgrid",
            "recipients_count": len(recipients),
        }

    def _deliver_ses(
        self,
        _subject: str,
        _html: str,
        recipients: tuple[EmailRecipient, ...],
        _attachments: list[tuple[str, bytes]] | None,
    ) -> dict[str, Any]:
        """Placeholder for AWS SES delivery.

        In production, use boto3 ses client.
        """
        if not self.config.ses_access_key:
            return {"error": "AWS SES credentials not configured", "provider": "ses"}

        return {
            "success": True,
            "provider": "ses",
            "recipients_count": len(recipients),
        }

    def _build_mime_message(
        self,
        subject: str,
        html: str,
        to_addrs: list[str],
        attachments: list[tuple[str, bytes]] | None,
    ) -> MIMEMultipart:
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = (
            f"{self.config.sender_name} <{self.config.sender_email}>"
            if self.config.sender_name
            else self.config.sender_email
        )
        msg["To"] = ", ".join(to_addrs)

        msg.attach(MIMEText(html, "html", "utf-8"))

        for filename, content in attachments or []:
            part = MIMEApplication(content, Name=filename)
            part["Content-Disposition"] = f'attachment; filename="{filename}"'
            msg.attach(part)

        return msg

    # ── private: helpers ─────────────────────────────────────────

    def _progress_pct(self, tasks: list[dict[str, Any]]) -> float:
        if not tasks:
            return 0.0
        completed = sum(1 for t in tasks if t.get("status") == "completed")
        return completed / len(tasks) * 100

    def _blocked_reason(self, task: dict[str, Any]) -> str:
        metadata = task.get("metadata") or {}
        return (
            task.get("blocked_reason")
            or metadata.get("blocked_reason")
            or "No reason provided"
        )

    def _upcoming_milestones(
        self, plan: dict[str, Any], tasks: list[dict[str, Any]]
    ) -> list[str]:
        """Return names of milestones that still have incomplete tasks."""
        grouped = self._group_by_milestone(tasks)
        order = self._milestone_order(plan)
        upcoming: list[str] = []
        for name in order:
            mtasks = grouped.get(name, [])
            if mtasks and any(
                t.get("status") not in ("completed", "skipped") for t in mtasks
            ):
                upcoming.append(name)
        return upcoming

    def _group_by_milestone(
        self, tasks: list[dict[str, Any]]
    ) -> OrderedDict[str, list[dict[str, Any]]]:
        grouped: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
        for task in tasks:
            milestone = task.get("milestone") or "Ungrouped"
            grouped.setdefault(milestone, []).append(task)
        return grouped

    def _milestone_order(self, plan: dict[str, Any]) -> list[str]:
        names: list[str] = []
        for idx, ms in enumerate(plan.get("milestones", []), 1):
            if isinstance(ms, dict):
                names.append(ms.get("name") or ms.get("title") or f"Milestone {idx}")
            elif isinstance(ms, str):
                names.append(ms)
        return names


# ── module-private helpers ──────────────────────────────────────

_SPACE_RE = re.compile(r"\s+")


def _esc(text: str) -> str:
    """Minimal HTML entity escaping."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _compact(text: str, max_length: int) -> str:
    """Collapse whitespace and truncate."""
    text = _SPACE_RE.sub(" ", str(text)).strip()
    if len(text) <= max_length:
        return text
    return text[: max_length - 3].rstrip() + "..."


_EMAIL_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{subject}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       margin: 0; padding: 16px; background: #f6f6f6; color: #333; }}
h2 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 6px; }}
h3 {{ color: #34495e; }}
table {{ width: 100%; margin-bottom: 16px; }}
th {{ text-align: left; background: #ecf0f1; padding: 8px; }}
td {{ padding: 8px; border-bottom: 1px solid #eee; }}
</style>
</head>
<body>
{executive_summary}
{status_table}
{blocker_section}
{milestone_section}
{task_table}
{footer}
</body>
</html>
"""


__all__ = [
    "DigestFrequency",
    "EmailDigestConfig",
    "EmailExporter",
    "EmailRecipient",
    "EmailProvider",
    "RecipientRole",
    "SmtpSettings",
]
