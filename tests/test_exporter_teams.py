"""Tests for Microsoft Teams webhook exporter."""

import json
import tempfile
from pathlib import Path

import pytest

from blueprint.exporters.teams import (
    TeamsExporter,
    TeamsWebhookConfig,
)


@pytest.fixture
def sample_plan():
    """Sample execution plan for testing."""
    return {
        "id": "test-plan-123",
        "implementation_brief_id": "brief-456",
        "tasks": [
            {
                "id": "task-1",
                "title": "Implement feature A",
                "description": "Description A",
                "acceptance_criteria": ["AC1"],
                "status": "completed",
            },
            {
                "id": "task-2",
                "title": "Implement feature B",
                "description": "Description B",
                "acceptance_criteria": ["AC2"],
                "status": "in_progress",
            },
            {
                "id": "task-3",
                "title": "Implement feature C",
                "description": "Description C",
                "acceptance_criteria": ["AC3"],
                "status": "pending",
            },
            {
                "id": "task-4",
                "title": "Implement feature D",
                "description": "Description D",
                "acceptance_criteria": ["AC4"],
                "status": "blocked",
                "blocked_reason": "Waiting for approval",
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
    """Sample implementation brief for testing."""
    return {
        "id": "brief-456",
        "source_brief_id": "source-789",
        "title": "Build awesome new feature",
        "problem_statement": "Users need this feature",
        "mvp_goal": "Build the feature",
        "scope": ["Feature A", "Feature B"],
        "non_goals": ["Feature C"],
        "assumptions": ["Assumption 1"],
        "risks": ["Risk 1"],
        "validation_plan": "Test the feature",
        "definition_of_done": ["Done 1"],
    }


@pytest.fixture
def teams_config():
    """Sample Teams webhook configuration."""
    return TeamsWebhookConfig(
        webhooks={
            "default": "https://outlook.office.com/webhook/123/IncomingWebhook/456/abc",
            "engineering": "https://outlook.office.com/webhook/789/IncomingWebhook/012/def",
            "alerts": "https://outlook.office.com/webhook/345/IncomingWebhook/678/ghi",
        },
        routing_rules={
            "project-alpha": "engineering",
            "critical": "alerts",
        },
    )


def test_exporter_initialization():
    """Test Teams exporter initialization."""
    exporter = TeamsExporter()
    assert exporter is not None
    assert exporter.config is not None


def test_exporter_with_config():
    """Test Teams exporter with custom configuration."""
    config = TeamsWebhookConfig(
        webhooks={"test": "https://outlook.office.com/webhook/1/IncomingWebhook/2/a"}
    )
    exporter = TeamsExporter(config)
    assert exporter.config == config


def test_get_format():
    """Test get_format returns json."""
    exporter = TeamsExporter()
    assert exporter.get_format() == "json"


def test_get_extension():
    """Test get_extension returns .json."""
    exporter = TeamsExporter()
    assert exporter.get_extension() == ".json"


def test_build_adaptive_card_payload(sample_plan, sample_brief):
    """Test building Adaptive Card payload."""
    exporter = TeamsExporter()
    payload = exporter.build_adaptive_card_payload(sample_plan, sample_brief)

    assert "type" in payload
    assert "attachments" in payload
    assert len(payload["attachments"]) == 1
    attachment = payload["attachments"][0]
    assert attachment["contentType"] == "application/vnd.microsoft.card.adaptive"
    assert "content" in attachment
    card = attachment["content"]
    assert card["type"] == "AdaptiveCard"
    assert "body" in card


def test_build_message_card_payload(sample_plan, sample_brief):
    """Test building Message Card payload."""
    config = TeamsWebhookConfig(use_adaptive_cards=False)
    exporter = TeamsExporter(config)
    payload = exporter.build_message_card_payload(sample_plan, sample_brief)

    assert "@type" in payload
    assert payload["@type"] == "MessageCard"
    assert "sections" in payload
    assert "themeColor" in payload


def test_status_summary(sample_plan):
    """Test status summary generation."""
    exporter = TeamsExporter()
    tasks = sample_plan["tasks"]
    summary = exporter._status_summary(tasks)

    assert "completed" in summary
    assert "in_progress" in summary
    assert "pending" in summary
    assert "blocked" in summary


def test_color_determination_blocked(sample_plan):
    """Test color is red when tasks are blocked."""
    exporter = TeamsExporter()
    color = exporter._determine_color(sample_plan["tasks"])

    # Should be red due to blocked task
    assert color == "E74C3C"


def test_color_determination_completed():
    """Test color is green when all tasks are completed."""
    exporter = TeamsExporter()
    tasks = [
        {"id": "t1", "status": "completed"},
        {"id": "t2", "status": "completed"},
    ]
    color = exporter._determine_color(tasks)

    assert color == "2ECC71"


def test_export_writes_file(sample_plan, sample_brief):
    """Test export writes JSON payload to file."""
    exporter = TeamsExporter()

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = str(Path(tmpdir) / "output.json")
        result = exporter.export(sample_plan, sample_brief, output_path)

        assert result == output_path
        assert Path(output_path).exists()

        with open(output_path) as f:
            payload = json.load(f)

        assert "attachments" in payload


def test_webhook_config_get_webhook_url(teams_config):
    """Test webhook URL retrieval from config."""
    # Direct webhook mapping
    url = teams_config.get_webhook_url("engineering")
    assert "789" in url

    # Routing rule
    url = teams_config.get_webhook_url("project-alpha")
    assert "789" in url

    # Default webhook
    url = teams_config.get_webhook_url("unknown")
    assert "123" in url


def test_webhook_url_validation():
    """Test Teams webhook URL validation."""
    exporter = TeamsExporter()

    # Valid URLs
    assert exporter._is_valid_webhook_url(
        "https://outlook.office.com/webhook/123/IncomingWebhook/456/abc"
    )
    assert exporter._is_valid_webhook_url(
        "https://outlook.office365.com/webhook/123/webhookb2/456/abc"
    )

    # Invalid URLs
    assert not exporter._is_valid_webhook_url("https://example.com/webhook")
    assert not exporter._is_valid_webhook_url("not-a-url")
    assert not exporter._is_valid_webhook_url("https://teams.microsoft.com/other")


def test_plan_creation_notification(sample_plan, sample_brief, teams_config):
    """Test plan creation notification."""
    exporter = TeamsExporter(teams_config)
    result = exporter.notify(sample_plan, sample_brief, "plan_creation")

    assert "success" in result or "error" in result


def test_task_completion_notification(sample_plan, sample_brief):
    """Test task completion notification."""
    exporter = TeamsExporter()
    payload = exporter._task_completion_payload(sample_plan, sample_brief, sample_plan["tasks"])

    assert "attachments" in payload
    card = payload["attachments"][0]["content"]
    # Check for progress text
    body_texts = [item.get("text", "") for item in card["body"] if item.get("type") == "TextBlock"]
    assert any("Progress" in text for text in body_texts)


def test_milestone_reached_notification(sample_plan, sample_brief):
    """Test milestone reached notification."""
    exporter = TeamsExporter()
    payload = exporter._milestone_reached_payload(sample_plan, sample_brief, sample_plan["tasks"])

    assert "attachments" in payload
    card = payload["attachments"][0]["content"]
    body_texts = [item.get("text", "") for item in card["body"] if item.get("type") == "TextBlock"]
    assert any("Milestone" in text for text in body_texts)


def test_risk_detected_notification(sample_plan, sample_brief):
    """Test risk detected notification."""
    # Add high-risk tasks
    high_risk_plan = {
        **sample_plan,
        "tasks": [
            {
                **sample_plan["tasks"][0],
                "risk_level": "critical",
            }
        ],
    }

    exporter = TeamsExporter()
    payload = exporter._risk_detected_payload(high_risk_plan, sample_brief, high_risk_plan["tasks"])

    assert "attachments" in payload
    card = payload["attachments"][0]["content"]
    body_texts = [item.get("text", "") for item in card["body"] if item.get("type") == "TextBlock"]
    assert any("Risk" in text for text in body_texts)


def test_notification_trigger_disabled(sample_plan, sample_brief):
    """Test notification is skipped when trigger is disabled."""
    config = TeamsWebhookConfig(
        webhooks={"default": "https://outlook.office.com/webhook/1/IncomingWebhook/2/a"},
        enabled_triggers=("plan_creation",),
    )
    exporter = TeamsExporter(config)

    result = exporter.notify(sample_plan, sample_brief, "task_completion")

    assert "skipped" in result
    assert result["skipped"] is True


def test_notification_no_webhook(sample_plan, sample_brief):
    """Test notification fails when no webhook URL is configured."""
    config = TeamsWebhookConfig(webhooks={})
    exporter = TeamsExporter(config)

    result = exporter.notify(sample_plan, sample_brief, "plan_creation", route_key="nonexistent")

    assert "error" in result


def test_send_to_webhook_rate_limiting(teams_config):
    """Test rate limiting is applied between webhook requests."""
    exporter = TeamsExporter(teams_config)

    webhook_url = teams_config.get_webhook_url("default")
    payload = {"type": "message", "attachments": []}

    import time

    start = time.time()
    exporter.send_to_webhook(webhook_url, payload)
    exporter.send_to_webhook(webhook_url, payload)
    elapsed = time.time() - start

    assert elapsed >= teams_config.rate_limit_delay


def test_send_to_webhook_invalid_url():
    """Test sending to invalid webhook URL."""
    exporter = TeamsExporter()
    payload = {"type": "message", "attachments": []}

    result = exporter.send_to_webhook("https://invalid.com/webhook", payload)

    assert "error" in result


def test_truncate_text():
    """Test text truncation."""
    exporter = TeamsExporter()

    # Text shorter than limit
    short = "Short text"
    assert exporter._truncate(short, 100) == short

    # Text longer than limit
    long = "A" * 100
    truncated = exporter._truncate(long, 50)
    assert len(truncated) == 50
    assert truncated.endswith("...")


def test_build_facts(sample_plan, sample_brief):
    """Test building facts for adaptive cards."""
    exporter = TeamsExporter()
    tasks = sample_plan["tasks"]
    summary = exporter._status_summary(tasks)

    facts = exporter._build_facts(sample_plan, sample_brief, tasks, summary)

    assert isinstance(facts, list)
    assert all("title" in fact and "value" in fact for fact in facts)
    fact_titles = [f["title"] for f in facts]
    assert "Total Tasks" in fact_titles
    assert "Status" in fact_titles


def test_empty_tasks_list():
    """Test handling of empty tasks list."""
    exporter = TeamsExporter()
    plan = {"id": "test", "tasks": [], "milestones": [], "metadata": {}}
    brief = {
        "id": "brief-1",
        "source_brief_id": "source-1",
        "title": "Test",
        "problem_statement": "Test",
        "mvp_goal": "Goal",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Plan",
        "definition_of_done": [],
    }

    payload = exporter.build_adaptive_card_payload(plan, brief)

    assert "attachments" in payload
    card = payload["attachments"][0]["content"]
    assert card["type"] == "AdaptiveCard"


def test_status_summary_no_tasks():
    """Test status summary with no tasks."""
    exporter = TeamsExporter()
    summary = exporter._status_summary([])
    assert summary == "No tasks"


def test_webhook_config_default_values():
    """Test TeamsWebhookConfig default values."""
    config = TeamsWebhookConfig()

    assert config.webhooks == {}
    assert config.routing_rules == {}
    assert config.rate_limit_delay == 0.5
    assert config.use_adaptive_cards is True
    assert "plan_creation" in config.enabled_triggers


def test_notification_with_routing(sample_plan, sample_brief, teams_config):
    """Test notification with routing key."""
    exporter = TeamsExporter(teams_config)

    result = exporter.notify(sample_plan, sample_brief, "plan_creation", route_key="engineering")

    assert "success" in result or "webhook_url" in result
    if "webhook_url" in result:
        assert "789" in result["webhook_url"]


def test_all_notification_triggers(sample_plan, sample_brief, teams_config):
    """Test all notification trigger types."""
    exporter = TeamsExporter(teams_config)

    triggers = [
        "plan_creation",
        "task_completion",
        "milestone_reached",
        "risk_detected",
    ]

    for trigger in triggers:
        result = exporter.notify(sample_plan, sample_brief, trigger)
        assert "success" in result or "error" in result or "skipped" in result


def test_adaptive_card_schema_version():
    """Test Adaptive Card has correct schema version."""
    exporter = TeamsExporter()
    plan = {"id": "test", "tasks": [], "milestones": [], "metadata": {}}
    brief = {
        "id": "brief-1",
        "source_brief_id": "source-1",
        "title": "Test",
        "problem_statement": "Test",
        "mvp_goal": "Goal",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Plan",
        "definition_of_done": [],
    }

    payload = exporter.build_adaptive_card_payload(plan, brief)
    card = payload["attachments"][0]["content"]

    assert card["version"] == "1.4"
    assert "$schema" in card


def test_routing_by_team(teams_config):
    """Test webhook routing by team."""
    url = teams_config.get_webhook_url("engineering")
    assert "789" in url


def test_routing_by_project(teams_config):
    """Test webhook routing by project."""
    url = teams_config.get_webhook_url("project-alpha")
    assert "789" in url  # Should route to engineering


def test_routing_to_default(teams_config):
    """Test routing falls back to default."""
    url = teams_config.get_webhook_url("unknown-project")
    assert "123" in url
