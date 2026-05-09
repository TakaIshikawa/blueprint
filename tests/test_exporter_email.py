"""Tests for email digest exporter."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from blueprint.exporters.email import (
    EmailDigestConfig,
    EmailExporter,
    EmailRecipient,
    SmtpSettings,
)


# ── fixtures ────────────────────────────────────────────────────


@pytest.fixture
def sample_plan():
    """Minimal valid execution plan for testing."""
    return {
        "id": "plan-001",
        "implementation_brief_id": "brief-001",
        "tasks": [
            {
                "id": "task-1",
                "title": "Set up database schema",
                "description": "Create initial schema",
                "acceptance_criteria": ["Schema created"],
                "status": "completed",
                "milestone": "Alpha Release",
            },
            {
                "id": "task-2",
                "title": "Implement API endpoints",
                "description": "REST endpoints for CRUD",
                "acceptance_criteria": ["Endpoints pass tests"],
                "status": "in_progress",
                "milestone": "Alpha Release",
            },
            {
                "id": "task-3",
                "title": "Build frontend components",
                "description": "React components",
                "acceptance_criteria": ["Components render"],
                "status": "pending",
                "milestone": "Beta Release",
            },
            {
                "id": "task-4",
                "title": "Integration testing",
                "description": "End-to-end tests",
                "acceptance_criteria": ["All tests pass"],
                "status": "blocked",
                "blocked_reason": "Waiting for staging environment",
                "milestone": "Beta Release",
            },
        ],
        "milestones": [
            {"name": "Alpha Release"},
            {"name": "Beta Release"},
        ],
        "metadata": {},
    }


@pytest.fixture
def sample_brief():
    """Minimal valid implementation brief for testing."""
    return {
        "id": "brief-001",
        "source_brief_id": "source-001",
        "title": "Customer Portal MVP",
        "problem_statement": "Customers need self-service access",
        "mvp_goal": "Build self-service portal",
        "scope": ["User management", "Dashboard"],
        "non_goals": ["Advanced analytics"],
        "assumptions": ["Cloud deployment"],
        "risks": ["Timeline risk"],
        "validation_plan": "Integration tests",
        "definition_of_done": ["All tests pass"],
    }


@pytest.fixture
def email_config():
    """Sample email digest configuration."""
    return EmailDigestConfig(
        sender_email="digest@example.com",
        sender_name="Blueprint CI",
        recipients=(
            EmailRecipient(email="pm@example.com", name="PM", role="stakeholders"),
            EmailRecipient(email="dev@example.com", name="Dev", role="team_members"),
            EmailRecipient(email="cto@example.com", name="CTO", role="management"),
            EmailRecipient(
                email="former@example.com",
                name="Former",
                role="team_members",
                unsubscribed=True,
            ),
        ),
        frequency="daily",
        provider="smtp",
        smtp=SmtpSettings(host="mail.example.com", port=587, use_tls=True),
        subject_prefix="[Blueprint]",
        unsubscribe_url="https://example.com/unsubscribe",
    )


# ── initialisation ──────────────────────────────────────────────


def test_exporter_default_init():
    exporter = EmailExporter()
    assert exporter.config is not None
    assert exporter.config.provider == "smtp"


def test_exporter_custom_config(email_config):
    exporter = EmailExporter(email_config)
    assert exporter.config is email_config
    assert exporter.config.sender_email == "digest@example.com"


def test_get_format():
    assert EmailExporter().get_format() == "html"


def test_get_extension():
    assert EmailExporter().get_extension() == ".html"


# ── HTML template generation ────────────────────────────────────


def test_render_html_contains_key_sections(sample_plan, sample_brief):
    exporter = EmailExporter()
    html = exporter.render_html(sample_plan, sample_brief)

    assert "Executive Summary" in html
    assert "Status Overview" in html
    assert "Blockers" in html
    assert "Milestones" in html
    assert "Tasks" in html


def test_render_html_contains_plan_data(sample_plan, sample_brief):
    exporter = EmailExporter()
    html = exporter.render_html(sample_plan, sample_brief)

    assert "plan-001" in html
    assert "Customer Portal MVP" in html
    assert "task-1" in html
    assert "task-4" in html


def test_render_html_shows_progress(sample_plan, sample_brief):
    exporter = EmailExporter()
    html = exporter.render_html(sample_plan, sample_brief)

    # 1 of 4 tasks completed = 25%
    assert "25.0%" in html


def test_render_html_shows_blockers(sample_plan, sample_brief):
    exporter = EmailExporter()
    html = exporter.render_html(sample_plan, sample_brief)

    assert "Waiting for staging environment" in html


def test_render_html_milestone_progress(sample_plan, sample_brief):
    exporter = EmailExporter()
    html = exporter.render_html(sample_plan, sample_brief)

    assert "Alpha Release" in html
    assert "Beta Release" in html


def test_render_html_escapes_special_chars(sample_brief):
    plan = {
        "id": "plan-xss",
        "implementation_brief_id": "brief-001",
        "tasks": [
            {
                "id": "t1",
                "title": '<script>alert("xss")</script>',
                "description": "Test XSS",
                "acceptance_criteria": ["safe"],
                "status": "pending",
            }
        ],
        "milestones": [],
        "metadata": {},
    }
    exporter = EmailExporter()
    html = exporter.render_html(plan, sample_brief)

    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_render_html_empty_tasks(sample_brief):
    plan = {
        "id": "plan-empty",
        "implementation_brief_id": "brief-001",
        "tasks": [],
        "milestones": [],
        "metadata": {},
    }
    exporter = EmailExporter()
    html = exporter.render_html(plan, sample_brief)

    assert "No tasks" in html or "No blockers" in html


def test_render_html_footer_with_unsubscribe():
    config = EmailDigestConfig(
        unsubscribe_url="https://example.com/unsub",
        sender_name="Digest Bot",
    )
    exporter = EmailExporter(config)
    plan = {
        "id": "p1",
        "implementation_brief_id": "b1",
        "tasks": [],
        "milestones": [],
        "metadata": {},
    }
    brief = {
        "id": "b1",
        "source_brief_id": "s1",
        "title": "Test",
        "problem_statement": "p",
        "mvp_goal": "g",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "v",
        "definition_of_done": [],
    }
    html = exporter.render_html(plan, brief)

    assert "Unsubscribe" in html
    assert "https://example.com/unsub" in html


def test_render_html_footer_without_unsubscribe():
    config = EmailDigestConfig(unsubscribe_url="", sender_name="Bot")
    exporter = EmailExporter(config)
    plan = {
        "id": "p1",
        "implementation_brief_id": "b1",
        "tasks": [],
        "milestones": [],
        "metadata": {},
    }
    brief = {
        "id": "b1",
        "source_brief_id": "s1",
        "title": "Test",
        "problem_statement": "p",
        "mvp_goal": "g",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "v",
        "definition_of_done": [],
    }
    html = exporter.render_html(plan, brief)

    assert "Unsubscribe" not in html


# ── digest frequency ────────────────────────────────────────────


def test_frequency_daily():
    config = EmailDigestConfig(frequency="daily")
    assert config.frequency == "daily"


def test_frequency_weekly():
    config = EmailDigestConfig(frequency="weekly")
    assert config.frequency == "weekly"


def test_frequency_per_milestone():
    config = EmailDigestConfig(frequency="per_milestone")
    assert config.frequency == "per_milestone"


# ── recipient filtering ─────────────────────────────────────────


def test_recipients_by_role(email_config):
    stakeholders = email_config.recipients_by_role("stakeholders")
    assert len(stakeholders) == 1
    assert stakeholders[0].email == "pm@example.com"


def test_recipients_by_multiple_roles(email_config):
    result = email_config.recipients_by_role("stakeholders", "management")
    assert len(result) == 2


def test_active_recipients_excludes_unsubscribed(email_config):
    active = email_config.active_recipients()
    emails = [r.email for r in active]
    assert "former@example.com" not in emails
    assert len(active) == 3


def test_recipients_by_role_excludes_unsubscribed(email_config):
    team = email_config.recipients_by_role("team_members")
    assert len(team) == 1
    assert team[0].email == "dev@example.com"


def test_empty_recipients():
    config = EmailDigestConfig(recipients=())
    assert config.active_recipients() == ()
    assert config.recipients_by_role("management") == ()


def test_large_recipient_list():
    recipients = tuple(
        EmailRecipient(email=f"user{i}@example.com", role="team_members")
        for i in range(200)
    )
    config = EmailDigestConfig(recipients=recipients)
    assert len(config.active_recipients()) == 200


# ── export to file ──────────────────────────────────────────────


def test_export_writes_html_file(sample_plan, sample_brief):
    exporter = EmailExporter()

    with tempfile.TemporaryDirectory() as tmpdir:
        output = str(Path(tmpdir) / "digest.html")
        result = exporter.export(sample_plan, sample_brief, output)

        assert result == output
        assert Path(output).exists()

        content = Path(output).read_text()
        assert "<!DOCTYPE html>" in content
        assert "Executive Summary" in content


def test_export_creates_parent_dirs(sample_plan, sample_brief):
    exporter = EmailExporter()

    with tempfile.TemporaryDirectory() as tmpdir:
        output = str(Path(tmpdir) / "nested" / "dir" / "digest.html")
        result = exporter.export(sample_plan, sample_brief, output)

        assert Path(result).exists()


# ── subject line ────────────────────────────────────────────────


def test_subject_includes_title_and_progress(sample_plan, sample_brief):
    exporter = EmailExporter()
    subject = exporter.build_subject(sample_plan, sample_brief)

    assert "Customer Portal MVP" in subject
    assert "25%" in subject


def test_subject_prefix():
    config = EmailDigestConfig(subject_prefix="[MyOrg]")
    exporter = EmailExporter(config)

    plan = {"id": "p1", "tasks": []}
    brief = {"title": "Feature X"}
    subject = exporter.build_subject(plan, brief)

    assert subject.startswith("[MyOrg]")


def test_subject_truncation():
    exporter = EmailExporter()
    plan = {"id": "p1", "tasks": []}
    brief = {"title": "A" * 200}
    subject = exporter.build_subject(plan, brief)

    assert len(subject) <= exporter.MAX_SUBJECT_LENGTH


# ── render_payload ──────────────────────────────────────────────


def test_render_payload_structure(sample_plan, sample_brief, email_config):
    exporter = EmailExporter(email_config)
    payload = exporter.render_payload(sample_plan, sample_brief)

    assert payload["schema_version"] == "blueprint.email_digest.v1"
    assert "subject" in payload
    assert "sender" in payload
    assert "recipients" in payload
    assert "html_body" in payload
    assert payload["plan_id"] == "plan-001"
    assert payload["task_count"] == 4


def test_render_payload_blockers(sample_plan, sample_brief):
    exporter = EmailExporter()
    payload = exporter.render_payload(sample_plan, sample_brief)

    assert len(payload["blockers"]) == 1
    assert payload["blockers"][0]["id"] == "task-4"


def test_render_payload_status_counts(sample_plan, sample_brief):
    exporter = EmailExporter()
    payload = exporter.render_payload(sample_plan, sample_brief)

    counts = payload["status_counts"]
    assert counts["completed"] == 1
    assert counts["in_progress"] == 1
    assert counts["pending"] == 1
    assert counts["blocked"] == 1


def test_render_payload_progress(sample_plan, sample_brief):
    exporter = EmailExporter()
    payload = exporter.render_payload(sample_plan, sample_brief)

    assert payload["progress_pct"] == pytest.approx(25.0)


# ── SMTP delivery ───────────────────────────────────────────────


def test_send_no_recipients():
    config = EmailDigestConfig(recipients=())
    exporter = EmailExporter(config)

    plan = {"id": "p1", "tasks": [], "milestones": [], "metadata": {}}
    brief = {
        "id": "b1",
        "source_brief_id": "s1",
        "title": "T",
        "problem_statement": "p",
        "mvp_goal": "g",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "v",
        "definition_of_done": [],
    }
    result = exporter.send(plan, brief)
    assert "error" in result


def test_send_with_role_filter(email_config, sample_plan, sample_brief):
    exporter = EmailExporter(email_config)

    with patch.object(exporter, "_deliver_smtp") as mock_deliver:
        mock_deliver.return_value = {
            "success": True,
            "provider": "smtp",
            "recipients_count": 1,
        }
        exporter.send(
            sample_plan, sample_brief, roles=("management",)
        )

    assert mock_deliver.called
    recipients_arg = mock_deliver.call_args[0][2]
    assert len(recipients_arg) == 1
    assert recipients_arg[0].role == "management"


def test_send_smtp_success(email_config, sample_plan, sample_brief):
    exporter = EmailExporter(email_config)

    with patch("blueprint.exporters.email.smtplib.SMTP") as mock_smtp_class:
        mock_server = MagicMock()
        mock_smtp_class.return_value = mock_server

        result = exporter.send(sample_plan, sample_brief)

    assert result["success"] is True
    assert result["provider"] == "smtp"
    assert result["recipients_count"] == 3  # excludes unsubscribed


def test_send_smtp_failure(email_config, sample_plan, sample_brief):
    exporter = EmailExporter(email_config)

    with patch("blueprint.exporters.email.smtplib.SMTP") as mock_smtp_class:
        mock_smtp_class.side_effect = ConnectionRefusedError("Connection refused")

        result = exporter.send(sample_plan, sample_brief)

    assert "error" in result
    assert result["provider"] == "smtp"


def test_send_smtp_no_tls(sample_plan, sample_brief):
    config = EmailDigestConfig(
        recipients=(EmailRecipient(email="a@b.com"),),
        smtp=SmtpSettings(host="localhost", port=25, use_tls=False),
    )
    exporter = EmailExporter(config)

    with patch("blueprint.exporters.email.smtplib.SMTP") as mock_smtp_class:
        mock_server = MagicMock()
        mock_smtp_class.return_value = mock_server

        result = exporter.send(sample_plan, sample_brief)

    assert result["success"] is True
    mock_server.starttls.assert_not_called()


def test_send_smtp_with_auth(sample_plan, sample_brief):
    config = EmailDigestConfig(
        recipients=(EmailRecipient(email="a@b.com"),),
        smtp=SmtpSettings(
            host="smtp.example.com",
            port=587,
            username="user",
            password="pass",
            use_tls=True,
        ),
    )
    exporter = EmailExporter(config)

    with patch("blueprint.exporters.email.smtplib.SMTP") as mock_smtp_class:
        mock_server = MagicMock()
        mock_smtp_class.return_value = mock_server

        exporter.send(sample_plan, sample_brief)

    mock_server.login.assert_called_once_with("user", "pass")


# ── SendGrid provider ───────────────────────────────────────────


def test_send_sendgrid_no_api_key(sample_plan, sample_brief):
    config = EmailDigestConfig(
        recipients=(EmailRecipient(email="a@b.com"),),
        provider="sendgrid",
        sendgrid_api_key="",
    )
    exporter = EmailExporter(config)
    result = exporter.send(sample_plan, sample_brief)

    assert "error" in result
    assert result["provider"] == "sendgrid"


def test_send_sendgrid_with_key(sample_plan, sample_brief):
    config = EmailDigestConfig(
        recipients=(EmailRecipient(email="a@b.com"),),
        provider="sendgrid",
        sendgrid_api_key="SG.fake-key",
    )
    exporter = EmailExporter(config)
    result = exporter.send(sample_plan, sample_brief)

    assert result["success"] is True
    assert result["provider"] == "sendgrid"


# ── AWS SES provider ────────────────────────────────────────────


def test_send_ses_no_credentials(sample_plan, sample_brief):
    config = EmailDigestConfig(
        recipients=(EmailRecipient(email="a@b.com"),),
        provider="ses",
        ses_access_key="",
    )
    exporter = EmailExporter(config)
    result = exporter.send(sample_plan, sample_brief)

    assert "error" in result
    assert result["provider"] == "ses"


def test_send_ses_with_credentials(sample_plan, sample_brief):
    config = EmailDigestConfig(
        recipients=(EmailRecipient(email="a@b.com"),),
        provider="ses",
        ses_access_key="AKIA_FAKE",
        ses_secret_key="secret",
    )
    exporter = EmailExporter(config)
    result = exporter.send(sample_plan, sample_brief)

    assert result["success"] is True
    assert result["provider"] == "ses"


# ── unsupported provider ────────────────────────────────────────


def test_send_unsupported_provider(sample_plan, sample_brief):
    config = EmailDigestConfig(
        recipients=(EmailRecipient(email="a@b.com"),),
        provider="mailgun",  # type: ignore[arg-type]
    )
    exporter = EmailExporter(config)
    result = exporter.send(sample_plan, sample_brief)

    assert "error" in result


# ── attachments ─────────────────────────────────────────────────


def test_send_with_attachments(email_config, sample_plan, sample_brief):
    exporter = EmailExporter(email_config)

    with patch("blueprint.exporters.email.smtplib.SMTP") as mock_smtp_class:
        mock_server = MagicMock()
        mock_smtp_class.return_value = mock_server

        attachments = [("report.pdf", b"%PDF-1.4 fake content")]
        result = exporter.send(sample_plan, sample_brief, attachments=attachments)

    assert result["success"] is True
    # Verify sendmail was called with the message containing attachment
    call_args = mock_server.sendmail.call_args
    msg_str = call_args[0][2]
    assert "report.pdf" in msg_str


def test_build_mime_message_structure(email_config):
    exporter = EmailExporter(email_config)
    msg = exporter._build_mime_message(
        "Test Subject",
        "<p>HTML body</p>",
        ["a@b.com", "c@d.com"],
        [("file.txt", b"content")],
    )

    assert msg["Subject"] == "Test Subject"
    assert "a@b.com" in msg["To"]
    assert "c@d.com" in msg["To"]
    # Should have html part + attachment
    parts = list(msg.walk())
    content_types = [p.get_content_type() for p in parts]
    assert "text/html" in content_types
    assert "application/octet-stream" in content_types


def test_build_mime_message_no_attachments(email_config):
    exporter = EmailExporter(email_config)
    msg = exporter._build_mime_message(
        "Subject",
        "<p>Body</p>",
        ["a@b.com"],
        None,
    )

    parts = list(msg.walk())
    content_types = [p.get_content_type() for p in parts]
    assert "text/html" in content_types
    assert "application/octet-stream" not in content_types


# ── edge cases ──────────────────────────────────────────────────


def test_all_tasks_completed(sample_brief):
    plan = {
        "id": "plan-done",
        "implementation_brief_id": "brief-001",
        "tasks": [
            {
                "id": "t1",
                "title": "Done task",
                "description": "d",
                "acceptance_criteria": ["ac"],
                "status": "completed",
            },
            {
                "id": "t2",
                "title": "Also done",
                "description": "d",
                "acceptance_criteria": ["ac"],
                "status": "completed",
            },
        ],
        "milestones": [],
        "metadata": {},
    }
    exporter = EmailExporter()
    html = exporter.render_html(plan, sample_brief)

    assert "100.0%" in html
    assert "No blockers" in html


def test_all_tasks_blocked(sample_brief):
    plan = {
        "id": "plan-stuck",
        "implementation_brief_id": "brief-001",
        "tasks": [
            {
                "id": "t1",
                "title": "Blocked A",
                "description": "d",
                "acceptance_criteria": ["ac"],
                "status": "blocked",
                "blocked_reason": "Reason A",
            },
            {
                "id": "t2",
                "title": "Blocked B",
                "description": "d",
                "acceptance_criteria": ["ac"],
                "status": "blocked",
                "blocked_reason": "Reason B",
            },
        ],
        "milestones": [],
        "metadata": {},
    }
    exporter = EmailExporter()
    html = exporter.render_html(plan, sample_brief)

    assert "Reason A" in html
    assert "Reason B" in html


def test_blocked_reason_from_metadata(sample_brief):
    plan = {
        "id": "p1",
        "implementation_brief_id": "brief-001",
        "tasks": [
            {
                "id": "t1",
                "title": "Task",
                "description": "d",
                "acceptance_criteria": ["ac"],
                "status": "blocked",
                "metadata": {"blocked_reason": "From metadata"},
            },
        ],
        "milestones": [],
        "metadata": {},
    }
    exporter = EmailExporter()
    html = exporter.render_html(plan, sample_brief)

    assert "From metadata" in html


def test_blocked_reason_fallback(sample_brief):
    plan = {
        "id": "p1",
        "implementation_brief_id": "brief-001",
        "tasks": [
            {
                "id": "t1",
                "title": "Task",
                "description": "d",
                "acceptance_criteria": ["ac"],
                "status": "blocked",
            },
        ],
        "milestones": [],
        "metadata": {},
    }
    exporter = EmailExporter()
    html = exporter.render_html(plan, sample_brief)

    assert "No reason provided" in html


def test_task_without_milestone(sample_brief):
    plan = {
        "id": "p1",
        "implementation_brief_id": "brief-001",
        "tasks": [
            {
                "id": "t1",
                "title": "No milestone",
                "description": "d",
                "acceptance_criteria": ["ac"],
                "status": "pending",
            }
        ],
        "milestones": [],
        "metadata": {},
    }
    exporter = EmailExporter()
    html = exporter.render_html(plan, sample_brief)

    assert "N/A" in html


def test_long_task_title_truncation(sample_brief):
    plan = {
        "id": "p1",
        "implementation_brief_id": "brief-001",
        "tasks": [
            {
                "id": "t1",
                "title": "X" * 300,
                "description": "d",
                "acceptance_criteria": ["ac"],
                "status": "pending",
            }
        ],
        "milestones": [],
        "metadata": {},
    }
    exporter = EmailExporter()
    html = exporter.render_html(plan, sample_brief)

    # Title should be truncated
    assert "X" * 300 not in html
    assert "..." in html


def test_upcoming_milestones(sample_plan, sample_brief):
    exporter = EmailExporter()
    tasks = sample_plan["tasks"]
    upcoming = exporter._upcoming_milestones(sample_plan, tasks)

    # Both milestones have incomplete tasks
    assert "Alpha Release" in upcoming
    assert "Beta Release" in upcoming


def test_upcoming_milestones_all_done(sample_brief):
    plan = {
        "id": "p1",
        "implementation_brief_id": "brief-001",
        "tasks": [
            {
                "id": "t1",
                "title": "Done",
                "description": "d",
                "acceptance_criteria": ["ac"],
                "status": "completed",
                "milestone": "M1",
            }
        ],
        "milestones": [{"name": "M1"}],
        "metadata": {},
    }
    exporter = EmailExporter()
    upcoming = exporter._upcoming_milestones(plan, plan["tasks"])

    assert upcoming == []


def test_internationalization_characters(sample_brief):
    """Test handling of non-ASCII characters in plan data."""
    plan = {
        "id": "plan-i18n",
        "implementation_brief_id": "brief-001",
        "tasks": [
            {
                "id": "t1",
                "title": "Implementieren: Benutzeroberfläche",
                "description": "Deutsche Beschreibung",
                "acceptance_criteria": ["Fertig"],
                "status": "pending",
            },
            {
                "id": "t2",
                "title": "実装: ユーザーインターフェース",
                "description": "日本語の説明",
                "acceptance_criteria": ["完了"],
                "status": "completed",
            },
        ],
        "milestones": [],
        "metadata": {},
    }
    exporter = EmailExporter()
    html = exporter.render_html(plan, sample_brief)

    assert "Benutzeroberfläche" in html
    assert "ユーザーインターフェース" in html


def test_config_default_values():
    config = EmailDigestConfig()

    assert config.sender_email == "noreply@blueprint.dev"
    assert config.sender_name == "Blueprint Digest"
    assert config.recipients == ()
    assert config.frequency == "daily"
    assert config.provider == "smtp"
    assert config.locale == "en"


def test_smtp_settings_defaults():
    settings = SmtpSettings()

    assert settings.host == "localhost"
    assert settings.port == 587
    assert settings.use_tls is True
    assert settings.username == ""
    assert settings.password == ""


def test_email_recipient_defaults():
    r = EmailRecipient(email="test@example.com")

    assert r.name == ""
    assert r.role == "team_members"
    assert r.unsubscribed is False


def test_milestone_order_string_milestones():
    """Test milestone ordering with string-format milestones."""
    exporter = EmailExporter()
    plan = {"milestones": ["First", "Second", "Third"]}
    order = exporter._milestone_order(plan)

    assert order == ["First", "Second", "Third"]


def test_milestone_order_dict_milestones():
    """Test milestone ordering with dict-format milestones."""
    exporter = EmailExporter()
    plan = {"milestones": [{"name": "Alpha"}, {"title": "Beta"}, {}]}
    order = exporter._milestone_order(plan)

    assert order == ["Alpha", "Beta", "Milestone 3"]


def test_progress_pct_empty():
    exporter = EmailExporter()
    assert exporter._progress_pct([]) == 0.0


def test_progress_pct_partial():
    exporter = EmailExporter()
    tasks = [
        {"status": "completed"},
        {"status": "completed"},
        {"status": "pending"},
        {"status": "blocked"},
    ]
    assert exporter._progress_pct(tasks) == pytest.approx(50.0)
