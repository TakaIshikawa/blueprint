"""Tests for Discord webhook exporter."""

import json
import tempfile
from pathlib import Path

import pytest

from blueprint.exporters.discord import (
    DiscordExporter,
    DiscordWebhookConfig,
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
                "description": "Implement feature A description",
                "acceptance_criteria": ["Criterion 1"],
                "status": "completed",
            },
            {
                "id": "task-2",
                "title": "Implement feature B",
                "description": "Implement feature B description",
                "acceptance_criteria": ["Criterion 2"],
                "status": "in_progress",
            },
            {
                "id": "task-3",
                "title": "Implement feature C",
                "description": "Implement feature C description",
                "acceptance_criteria": ["Criterion 3"],
                "status": "pending",
            },
            {
                "id": "task-4",
                "title": "Implement feature D",
                "description": "Implement feature D description",
                "acceptance_criteria": ["Criterion 4"],
                "status": "blocked",
                "blocked_reason": "Waiting for API approval",
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
        "definition_of_done": ["Done criterion 1"],
    }


@pytest.fixture
def discord_config():
    """Sample Discord webhook configuration."""
    return DiscordWebhookConfig(
        webhooks={
            "default": "https://discord.com/api/webhooks/123/abc",
            "engineering": "https://discord.com/api/webhooks/456/def",
            "alerts": "https://discord.com/api/webhooks/789/ghi",
        },
        routing_rules={
            "project-alpha": "engineering",
            "critical": "alerts",
        },
    )


def test_exporter_initialization():
    """Test Discord exporter initialization."""
    exporter = DiscordExporter()
    assert exporter is not None
    assert exporter.config is not None


def test_exporter_with_config():
    """Test Discord exporter with custom configuration."""
    config = DiscordWebhookConfig(webhooks={"test": "https://discord.com/api/webhooks/1/a"})
    exporter = DiscordExporter(config)
    assert exporter.config == config


def test_get_format():
    """Test get_format returns json."""
    exporter = DiscordExporter()
    assert exporter.get_format() == "json"


def test_get_extension():
    """Test get_extension returns .json."""
    exporter = DiscordExporter()
    assert exporter.get_extension() == ".json"


def test_build_embed_payload(sample_plan, sample_brief):
    """Test building Discord embed payload."""
    exporter = DiscordExporter()
    payload = exporter.build_embed_payload(sample_plan, sample_brief)

    assert "embeds" in payload
    assert len(payload["embeds"]) == 1
    embed = payload["embeds"][0]
    assert "title" in embed
    assert "description" in embed
    assert "color" in embed
    assert "fields" in embed
    assert "footer" in embed
    assert "timestamp" in embed


def test_embed_title_truncation():
    """Test embed title is truncated to Discord limits."""
    exporter = DiscordExporter()
    long_title = "A" * 300
    plan = {"id": "test", "tasks": []}
    brief = {"title": long_title}

    payload = exporter.build_embed_payload(plan, brief)
    embed = payload["embeds"][0]

    assert len(embed["description"]) <= exporter.MAX_EMBED_DESCRIPTION


def test_status_summary(sample_plan):
    """Test status summary generation."""
    exporter = DiscordExporter()
    tasks = sample_plan["tasks"]
    summary = exporter._status_summary(tasks)

    assert "completed" in summary
    assert "in_progress" in summary
    assert "pending" in summary
    assert "blocked" in summary


def test_color_determination_blocked(sample_plan):
    """Test color is red when tasks are blocked."""
    exporter = DiscordExporter()
    color = exporter._determine_color(sample_plan["tasks"])

    # Should be red due to blocked task
    assert color == exporter.STATUS_COLORS["blocked"]


def test_color_determination_completed():
    """Test color is green when all tasks are completed."""
    exporter = DiscordExporter()
    tasks = [
        {"id": "t1", "status": "completed"},
        {"id": "t2", "status": "completed"},
    ]
    color = exporter._determine_color(tasks)

    assert color == exporter.STATUS_COLORS["completed"]


def test_color_determination_in_progress():
    """Test color is blue when tasks are in progress."""
    exporter = DiscordExporter()
    tasks = [
        {"id": "t1", "status": "completed"},
        {"id": "t2", "status": "in_progress"},
        {"id": "t3", "status": "pending"},
    ]
    color = exporter._determine_color(tasks)

    assert color == exporter.STATUS_COLORS["in_progress"]


def test_export_writes_file(sample_plan, sample_brief):
    """Test export writes JSON payload to file."""
    exporter = DiscordExporter()

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = str(Path(tmpdir) / "output.json")
        result = exporter.export(sample_plan, sample_brief, output_path)

        assert result == output_path
        assert Path(output_path).exists()

        with open(output_path) as f:
            payload = json.load(f)

        assert "embeds" in payload
        assert len(payload["embeds"]) == 1


def test_webhook_config_get_webhook_url(discord_config):
    """Test webhook URL retrieval from config."""
    # Direct webhook mapping
    url = discord_config.get_webhook_url("engineering")
    assert url == "https://discord.com/api/webhooks/456/def"

    # Routing rule
    url = discord_config.get_webhook_url("project-alpha")
    assert url == "https://discord.com/api/webhooks/456/def"

    # Default webhook
    url = discord_config.get_webhook_url("unknown")
    assert url == "https://discord.com/api/webhooks/123/abc"


def test_webhook_url_validation():
    """Test Discord webhook URL validation."""
    exporter = DiscordExporter()

    # Valid URLs
    assert exporter._is_valid_webhook_url("https://discord.com/api/webhooks/123/abc")
    assert exporter._is_valid_webhook_url("http://discord.com/api/webhooks/456/def")

    # Invalid URLs
    assert not exporter._is_valid_webhook_url("https://example.com/webhook")
    assert not exporter._is_valid_webhook_url("not-a-url")
    assert not exporter._is_valid_webhook_url("https://discord.com/other")


def test_plan_creation_notification(sample_plan, sample_brief, discord_config):
    """Test plan creation notification."""
    exporter = DiscordExporter(discord_config)
    result = exporter.notify(sample_plan, sample_brief, "plan_creation")

    assert "success" in result or "error" in result


def test_task_completion_notification(sample_plan, sample_brief, discord_config):
    """Test task completion notification."""
    exporter = DiscordExporter(discord_config)
    payload = exporter._task_completion_payload(sample_plan, sample_brief, sample_plan["tasks"])

    assert "embeds" in payload
    embed = payload["embeds"][0]
    assert "Task Completed" in embed["title"]
    assert "Progress" in embed["description"]


def test_milestone_reached_notification(sample_plan, sample_brief, discord_config):
    """Test milestone reached notification."""
    exporter = DiscordExporter(discord_config)
    payload = exporter._milestone_reached_payload(
        sample_plan, sample_brief, sample_plan["tasks"]
    )

    assert "embeds" in payload
    embed = payload["embeds"][0]
    assert "Milestone Reached" in embed["title"]


def test_blocking_issue_notification(sample_plan, sample_brief, discord_config):
    """Test blocking issue notification."""
    exporter = DiscordExporter(discord_config)
    payload = exporter._blocking_issue_payload(sample_plan, sample_brief, sample_plan["tasks"])

    assert "embeds" in payload
    embed = payload["embeds"][0]
    assert "Blocking Issues" in embed["title"]
    # Should mention blocked tasks
    assert any("Task:" in field["name"] for field in embed["fields"])


def test_notification_trigger_disabled(sample_plan, sample_brief):
    """Test notification is skipped when trigger is disabled."""
    config = DiscordWebhookConfig(
        webhooks={"default": "https://discord.com/api/webhooks/1/a"},
        enabled_triggers=("plan_creation",),  # Only plan_creation enabled
    )
    exporter = DiscordExporter(config)

    result = exporter.notify(sample_plan, sample_brief, "task_completion")

    assert "skipped" in result
    assert result["skipped"] is True


def test_notification_no_webhook(sample_plan, sample_brief):
    """Test notification fails when no webhook URL is configured."""
    config = DiscordWebhookConfig(webhooks={})
    exporter = DiscordExporter(config)

    result = exporter.notify(sample_plan, sample_brief, "plan_creation", route_key="nonexistent")

    assert "error" in result


def test_embed_fields_limited_to_max():
    """Test embed fields are limited to Discord max."""
    exporter = DiscordExporter()
    plan = {"id": "test", "tasks": [], "milestones": []}
    brief = {"title": "Test"}

    # Create many tasks to potentially exceed field limit
    tasks = [{"id": f"task-{i}", "status": "pending"} for i in range(30)]

    fields = exporter._build_fields(plan, brief, tasks, "test summary")

    # Should not exceed Discord's limit
    assert len(fields) <= exporter.MAX_EMBED_FIELDS


def test_high_priority_tasks_in_fields():
    """Test high priority tasks are included in fields."""
    exporter = DiscordExporter()

    plan = {
        "id": "test",
        "implementation_brief_id": "brief-1",
        "tasks": [
            {
                "id": "t1",
                "title": "High priority task",
                "description": "Test",
                "acceptance_criteria": ["AC1"],
                "priority": "critical",
                "status": "pending",
            }
        ],
        "milestones": [],
        "metadata": {},
    }
    brief = {
        "id": "brief-1",
        "source_brief_id": "source-1",
        "title": "Test",
        "problem_statement": "Test",
        "mvp_goal": "Goal",
        "scope": ["Scope"],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Plan",
        "definition_of_done": [],
    }

    payload = exporter.build_embed_payload(plan, brief)

    embed = payload["embeds"][0]
    field_names = [f["name"] for f in embed["fields"]]

    # Should include high priority field since we have a critical task
    assert "High Priority Tasks" in field_names


def test_blocked_tasks_in_fields(sample_plan, sample_brief):
    """Test blocked tasks are included in fields."""
    exporter = DiscordExporter()
    payload = exporter.build_embed_payload(sample_plan, sample_brief)

    embed = payload["embeds"][0]
    field_names = [f["name"] for f in embed["fields"]]

    # Should include blocked tasks field
    assert "Blocked Tasks" in field_names


def test_send_to_webhook_rate_limiting(discord_config):
    """Test rate limiting is applied between webhook requests."""
    exporter = DiscordExporter(discord_config)

    webhook_url = discord_config.get_webhook_url("default")
    payload = {"embeds": [{"title": "Test"}]}

    # Send multiple requests
    import time

    start = time.time()
    exporter.send_to_webhook(webhook_url, payload)
    exporter.send_to_webhook(webhook_url, payload)
    elapsed = time.time() - start

    # Should have applied rate limiting delay
    assert elapsed >= discord_config.rate_limit_delay


def test_send_to_webhook_invalid_url():
    """Test sending to invalid webhook URL."""
    exporter = DiscordExporter()
    payload = {"embeds": [{"title": "Test"}]}

    result = exporter.send_to_webhook("https://invalid.com/webhook", payload)

    assert "error" in result


def test_truncate_text():
    """Test text truncation."""
    exporter = DiscordExporter()

    # Text shorter than limit
    short = "Short text"
    assert exporter._truncate(short, 100) == short

    # Text longer than limit
    long = "A" * 100
    truncated = exporter._truncate(long, 50)
    assert len(truncated) == 50
    assert truncated.endswith("...")


def test_timestamp_format():
    """Test timestamp is in ISO 8601 format."""
    exporter = DiscordExporter()
    timestamp = exporter._get_timestamp()

    # Should be ISO 8601 format with timezone
    assert "T" in timestamp
    assert "+" in timestamp or "Z" in timestamp or "-" in timestamp.rsplit("T", 1)[1]


def test_multiple_blocked_tasks_in_blocking_notification():
    """Test blocking notification includes multiple blocked tasks."""
    exporter = DiscordExporter()
    plan = {
        "id": "test",
        "tasks": [
            {
                "id": "t1",
                "title": "Task 1",
                "status": "blocked",
                "blocked_reason": "Reason 1",
            },
            {
                "id": "t2",
                "title": "Task 2",
                "status": "blocked",
                "blocked_reason": "Reason 2",
            },
            {
                "id": "t3",
                "title": "Task 3",
                "status": "blocked",
                "blocked_reason": "Reason 3",
            },
        ],
    }
    brief = {"title": "Test"}

    payload = exporter._blocking_issue_payload(plan, brief, plan["tasks"])
    embed = payload["embeds"][0]

    # Should mention blocked count
    assert "3" in embed["description"]
    # Should have task fields (up to 3)
    task_fields = [f for f in embed["fields"] if "Task:" in f["name"]]
    assert len(task_fields) == 3


def test_routing_by_server(discord_config):
    """Test webhook routing by server."""
    url = discord_config.get_webhook_url("engineering")
    assert "456" in url


def test_routing_by_project(discord_config):
    """Test webhook routing by project."""
    url = discord_config.get_webhook_url("project-alpha")
    assert "456" in url  # Should route to engineering


def test_routing_to_default(discord_config):
    """Test routing falls back to default."""
    url = discord_config.get_webhook_url("unknown-project")
    assert "123" in url  # Should use default webhook


def test_empty_tasks_list():
    """Test handling of empty tasks list."""
    exporter = DiscordExporter()
    plan = {"id": "test", "tasks": []}
    brief = {"title": "Test"}

    payload = exporter.build_embed_payload(plan, brief)
    embed = payload["embeds"][0]

    # Should handle empty tasks gracefully
    assert embed["color"] == 0x808080  # Gray for empty


def test_status_summary_no_tasks():
    """Test status summary with no tasks."""
    exporter = DiscordExporter()
    summary = exporter._status_summary([])
    assert summary == "No tasks"


def test_embed_field_value_truncation():
    """Test embed field values are truncated to Discord limits."""
    exporter = DiscordExporter()
    plan = {
        "id": "test",
        "tasks": [
            {
                "id": "t1",
                "title": "Task",
                "status": "blocked",
                "blocked_reason": "A" * 2000,  # Very long reason
            }
        ],
    }
    brief = {"title": "Test"}

    payload = exporter._blocking_issue_payload(plan, brief, plan["tasks"])
    embed = payload["embeds"][0]

    # Field values should be within limits
    for field in embed["fields"]:
        assert len(field["value"]) <= exporter.MAX_FIELD_VALUE


def test_webhook_config_default_values():
    """Test DiscordWebhookConfig default values."""
    config = DiscordWebhookConfig()

    assert config.webhooks == {}
    assert config.routing_rules == {}
    assert config.rate_limit_delay == 0.5
    assert "plan_creation" in config.enabled_triggers
    assert "task_completion" in config.enabled_triggers


def test_notification_with_routing(sample_plan, sample_brief, discord_config):
    """Test notification with routing key."""
    exporter = DiscordExporter(discord_config)

    # Route to engineering channel
    result = exporter.notify(sample_plan, sample_brief, "plan_creation", route_key="engineering")

    assert "success" in result or "webhook_url" in result
    if "webhook_url" in result:
        assert "456" in result["webhook_url"]


def test_all_notification_triggers(sample_plan, sample_brief, discord_config):
    """Test all notification trigger types."""
    exporter = DiscordExporter(discord_config)

    triggers = [
        "plan_creation",
        "task_completion",
        "milestone_reached",
        "blocking_issue",
    ]

    for trigger in triggers:
        result = exporter.notify(sample_plan, sample_brief, trigger)
        # Should succeed or have specific error
        assert "success" in result or "error" in result or "skipped" in result
