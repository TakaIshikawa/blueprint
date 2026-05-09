"""Tests for Slack webhook exporter."""

import json
import tempfile
from pathlib import Path

import pytest

from blueprint.exporters.slack import (
    SlackExporter,
    SlackWebhookConfig,
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
def slack_config():
    """Sample Slack webhook configuration."""
    return SlackWebhookConfig(
        webhooks={
            "default": "https://hooks.slack.com/services/T00/B00/xxx",
            "engineering": "https://hooks.slack.com/services/T00/B01/yyy",
            "alerts": "https://hooks.slack.com/services/T00/B02/zzz",
        },
        routing_rules={
            "project-alpha": "engineering",
            "critical": "alerts",
        },
    )


def test_exporter_initialization():
    """Test Slack exporter initialization."""
    exporter = SlackExporter()
    assert exporter is not None
    assert exporter.config is not None


def test_exporter_with_config():
    """Test Slack exporter with custom configuration."""
    config = SlackWebhookConfig(webhooks={"test": "https://hooks.slack.com/services/T/B/x"})
    exporter = SlackExporter(config)
    assert exporter.config == config


def test_get_format():
    """Test get_format returns json."""
    exporter = SlackExporter()
    assert exporter.get_format() == "json"


def test_get_extension():
    """Test get_extension returns .json."""
    exporter = SlackExporter()
    assert exporter.get_extension() == ".json"


def test_build_block_kit_payload(sample_plan, sample_brief):
    """Test building Slack Block Kit payload."""
    exporter = SlackExporter()
    payload = exporter.build_block_kit_payload(sample_plan, sample_brief)

    assert "blocks" in payload
    assert len(payload["blocks"]) > 0
    # Check for header block
    assert any(block.get("type") == "header" for block in payload["blocks"])
    # Check for section blocks
    assert any(block.get("type") == "section" for block in payload["blocks"])


def test_build_attachment_payload(sample_plan, sample_brief):
    """Test building legacy Slack attachment payload."""
    config = SlackWebhookConfig(use_blocks=False)
    exporter = SlackExporter(config)
    payload = exporter.build_attachment_payload(sample_plan, sample_brief)

    assert "attachments" in payload
    assert len(payload["attachments"]) == 1
    attachment = payload["attachments"][0]
    assert "color" in attachment
    assert "title" in attachment
    assert "fields" in attachment


def test_block_kit_title_truncation():
    """Test block kit header text is truncated to Slack limits."""
    exporter = SlackExporter()
    long_id = "A" * 200
    plan = {"id": long_id, "tasks": []}
    brief = {"title": "Test"}

    payload = exporter.build_block_kit_payload(plan, brief)
    header_block = next(b for b in payload["blocks"] if b.get("type") == "header")

    assert len(header_block["text"]["text"]) <= 150


def test_status_summary(sample_plan):
    """Test status summary generation."""
    exporter = SlackExporter()
    tasks = sample_plan["tasks"]
    summary = exporter._status_summary(tasks)

    assert "completed" in summary
    assert "in_progress" in summary
    assert "pending" in summary
    assert "blocked" in summary


def test_color_determination_blocked(sample_plan):
    """Test color is red when tasks are blocked."""
    exporter = SlackExporter()
    color = exporter._determine_color(sample_plan["tasks"])

    # Should be red due to blocked task
    assert color == exporter.STATUS_COLORS["blocked"]


def test_color_determination_completed():
    """Test color is green when all tasks are completed."""
    exporter = SlackExporter()
    tasks = [
        {"id": "t1", "status": "completed"},
        {"id": "t2", "status": "completed"},
    ]
    color = exporter._determine_color(tasks)

    assert color == exporter.STATUS_COLORS["completed"]


def test_color_determination_in_progress():
    """Test color is blue when tasks are in progress."""
    exporter = SlackExporter()
    tasks = [
        {"id": "t1", "status": "completed"},
        {"id": "t2", "status": "in_progress"},
        {"id": "t3", "status": "pending"},
    ]
    color = exporter._determine_color(tasks)

    assert color == exporter.STATUS_COLORS["in_progress"]


def test_export_writes_file(sample_plan, sample_brief):
    """Test export writes JSON payload to file."""
    exporter = SlackExporter()

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = str(Path(tmpdir) / "output.json")
        result = exporter.export(sample_plan, sample_brief, output_path)

        assert result == output_path
        assert Path(output_path).exists()

        with open(output_path) as f:
            payload = json.load(f)

        assert "blocks" in payload
        assert len(payload["blocks"]) > 0


def test_webhook_config_get_webhook_url(slack_config):
    """Test webhook URL retrieval from config."""
    # Direct webhook mapping
    url = slack_config.get_webhook_url("engineering")
    assert url == "https://hooks.slack.com/services/T00/B01/yyy"

    # Routing rule
    url = slack_config.get_webhook_url("project-alpha")
    assert url == "https://hooks.slack.com/services/T00/B01/yyy"

    # Default webhook
    url = slack_config.get_webhook_url("unknown")
    assert url == "https://hooks.slack.com/services/T00/B00/xxx"


def test_webhook_url_validation():
    """Test Slack webhook URL validation."""
    exporter = SlackExporter()

    # Valid URLs
    assert exporter._is_valid_webhook_url("https://hooks.slack.com/services/T00/B00/xxx")
    assert exporter._is_valid_webhook_url("http://hooks.slack.com/services/T01/B01/yyy")

    # Invalid URLs
    assert not exporter._is_valid_webhook_url("https://example.com/webhook")
    assert not exporter._is_valid_webhook_url("not-a-url")
    assert not exporter._is_valid_webhook_url("https://slack.com/other")


def test_plan_creation_notification(sample_plan, sample_brief, slack_config):
    """Test plan creation notification."""
    exporter = SlackExporter(slack_config)
    result = exporter.notify(sample_plan, sample_brief, "plan_creation")

    assert "success" in result or "error" in result


def test_task_completion_notification(sample_plan, sample_brief):
    """Test task completion notification."""
    exporter = SlackExporter()
    payload = exporter._task_completion_payload(sample_plan, sample_brief, sample_plan["tasks"])

    assert "blocks" in payload
    # Check for task completion header
    header_block = next(b for b in payload["blocks"] if b.get("type") == "header")
    assert "Task Completed" in header_block["text"]["text"]


def test_milestone_reached_notification(sample_plan, sample_brief):
    """Test milestone reached notification."""
    exporter = SlackExporter()
    payload = exporter._milestone_reached_payload(sample_plan, sample_brief, sample_plan["tasks"])

    assert "blocks" in payload
    header_block = next(b for b in payload["blocks"] if b.get("type") == "header")
    assert "Milestone Reached" in header_block["text"]["text"]


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

    exporter = SlackExporter()
    payload = exporter._risk_detected_payload(high_risk_plan, sample_brief, high_risk_plan["tasks"])

    assert "blocks" in payload
    header_block = next(b for b in payload["blocks"] if b.get("type") == "header")
    assert "Risks Detected" in header_block["text"]["text"]


def test_notification_trigger_disabled(sample_plan, sample_brief):
    """Test notification is skipped when trigger is disabled."""
    config = SlackWebhookConfig(
        webhooks={"default": "https://hooks.slack.com/services/T/B/x"},
        enabled_triggers=("plan_creation",),
    )
    exporter = SlackExporter(config)

    result = exporter.notify(sample_plan, sample_brief, "task_completion")

    assert "skipped" in result
    assert result["skipped"] is True


def test_notification_no_webhook(sample_plan, sample_brief):
    """Test notification fails when no webhook URL is configured."""
    config = SlackWebhookConfig(webhooks={})
    exporter = SlackExporter(config)

    result = exporter.notify(sample_plan, sample_brief, "plan_creation", route_key="nonexistent")

    assert "error" in result


def test_blocks_limited_to_max():
    """Test blocks are limited to Slack max."""
    exporter = SlackExporter()
    plan = {"id": "test", "tasks": [], "milestones": []}
    brief = {"title": "Test"}

    payload = exporter.build_block_kit_payload(plan, brief)

    # Should not exceed Slack's limit
    assert len(payload["blocks"]) <= exporter.MAX_BLOCKS


def test_high_risk_tasks_in_fields():
    """Test high risk tasks are included in fields."""
    exporter = SlackExporter()

    plan = {
        "id": "test",
        "implementation_brief_id": "brief-1",
        "tasks": [
            {
                "id": "t1",
                "title": "High priority task",
                "description": "Test",
                "acceptance_criteria": ["AC1"],
                "risk_level": "critical",
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

    payload = exporter.build_block_kit_payload(plan, brief)

    # Find section block with fields
    section_blocks = [b for b in payload["blocks"] if b.get("type") == "section" and "fields" in b]
    fields = section_blocks[0]["fields"] if section_blocks else []
    field_texts = [f.get("text", "") for f in fields]

    # Should include high risk field
    assert any("High Risk Tasks" in text for text in field_texts)


def test_blocked_tasks_in_fields(sample_plan, sample_brief):
    """Test blocked tasks are included in fields."""
    exporter = SlackExporter()
    payload = exporter.build_block_kit_payload(sample_plan, sample_brief)

    # Find section block with fields
    section_blocks = [b for b in payload["blocks"] if b.get("type") == "section" and "fields" in b]
    fields = section_blocks[0]["fields"] if section_blocks else []
    field_texts = [f.get("text", "") for f in fields]

    # Should include blocked tasks field
    assert any("Blocked Tasks" in text for text in field_texts)


def test_send_to_webhook_rate_limiting(slack_config):
    """Test rate limiting is applied between webhook requests."""
    exporter = SlackExporter(slack_config)

    webhook_url = slack_config.get_webhook_url("default")
    payload = {"blocks": [{"type": "section", "text": {"type": "plain_text", "text": "Test"}}]}

    import time

    start = time.time()
    exporter.send_to_webhook(webhook_url, payload)
    exporter.send_to_webhook(webhook_url, payload)
    elapsed = time.time() - start

    # Should have applied rate limiting delay
    assert elapsed >= slack_config.rate_limit_delay


def test_send_to_webhook_invalid_url():
    """Test sending to invalid webhook URL."""
    exporter = SlackExporter()
    payload = {"blocks": [{"type": "section", "text": {"type": "plain_text", "text": "Test"}}]}

    result = exporter.send_to_webhook("https://invalid.com/webhook", payload)

    assert "error" in result


def test_truncate_text():
    """Test text truncation."""
    exporter = SlackExporter()

    # Text shorter than limit
    short = "Short text"
    assert exporter._truncate(short, 100) == short

    # Text longer than limit
    long = "A" * 100
    truncated = exporter._truncate(long, 50)
    assert len(truncated) == 50
    assert truncated.endswith("...")


def test_multiple_high_risk_tasks_in_risk_notification():
    """Test risk notification includes multiple high-risk tasks."""
    exporter = SlackExporter()
    plan = {
        "id": "test",
        "tasks": [
            {
                "id": "t1",
                "title": "Task 1",
                "status": "pending",
                "risk_level": "critical",
            },
            {
                "id": "t2",
                "title": "Task 2",
                "status": "pending",
                "risk_level": "high",
            },
            {
                "id": "t3",
                "title": "Task 3",
                "status": "pending",
                "risk_level": "blocker",
            },
        ],
    }
    brief = {"title": "Test"}

    payload = exporter._risk_detected_payload(plan, brief, plan["tasks"])

    # Should have blocks for high-risk tasks
    assert len(payload["blocks"]) >= 3


def test_routing_by_channel(slack_config):
    """Test webhook routing by channel."""
    url = slack_config.get_webhook_url("engineering")
    assert "B01" in url


def test_routing_by_project(slack_config):
    """Test webhook routing by project."""
    url = slack_config.get_webhook_url("project-alpha")
    assert "B01" in url  # Should route to engineering


def test_routing_to_default(slack_config):
    """Test routing falls back to default."""
    url = slack_config.get_webhook_url("unknown-project")
    assert "B00" in url  # Should use default webhook


def test_empty_tasks_list():
    """Test handling of empty tasks list."""
    exporter = SlackExporter()
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

    payload = exporter.build_block_kit_payload(plan, brief)

    # Should handle empty tasks gracefully
    assert "blocks" in payload
    assert len(payload["blocks"]) > 0


def test_status_summary_no_tasks():
    """Test status summary with no tasks."""
    exporter = SlackExporter()
    summary = exporter._status_summary([])
    assert summary == "No tasks"


def test_webhook_config_default_values():
    """Test SlackWebhookConfig default values."""
    config = SlackWebhookConfig()

    assert config.webhooks == {}
    assert config.routing_rules == {}
    assert config.rate_limit_delay == 1.0
    assert config.use_blocks is True
    assert "plan_creation" in config.enabled_triggers
    assert "task_completion" in config.enabled_triggers


def test_notification_with_routing(sample_plan, sample_brief, slack_config):
    """Test notification with routing key."""
    exporter = SlackExporter(slack_config)

    # Route to engineering channel
    result = exporter.notify(sample_plan, sample_brief, "plan_creation", route_key="engineering")

    assert "success" in result or "webhook_url" in result
    if "webhook_url" in result:
        assert "B01" in result["webhook_url"]


def test_all_notification_triggers(sample_plan, sample_brief, slack_config):
    """Test all notification trigger types."""
    exporter = SlackExporter(slack_config)

    triggers = [
        "plan_creation",
        "task_completion",
        "milestone_reached",
        "risk_detected",
    ]

    for trigger in triggers:
        result = exporter.notify(sample_plan, sample_brief, trigger)
        # Should succeed or have specific error
        assert "success" in result or "error" in result or "skipped" in result


def test_block_kit_emojis():
    """Test Block Kit uses emoji in headers."""
    exporter = SlackExporter()
    plan = {"id": "test", "tasks": []}
    brief = {"title": "Test"}

    payload = exporter._plan_creation_payload(plan, brief, [])
    header_block = next(b for b in payload["blocks"] if b.get("type") == "header")

    assert header_block["text"].get("emoji") is True


def test_attachment_payload_has_timestamp():
    """Test legacy attachment payload includes timestamp."""
    config = SlackWebhookConfig(use_blocks=False)
    exporter = SlackExporter(config)
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

    payload = exporter.build_attachment_payload(plan, brief)
    attachment = payload["attachments"][0]

    assert "ts" in attachment
    assert isinstance(attachment["ts"], int)


def test_context_footer_in_block_kit():
    """Test Block Kit includes context footer."""
    exporter = SlackExporter()
    plan = {"id": "test-plan-123", "tasks": [], "milestones": [], "metadata": {}}
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

    payload = exporter.build_block_kit_payload(plan, brief)

    # Should have a context block
    context_blocks = [b for b in payload["blocks"] if b.get("type") == "context"]
    assert len(context_blocks) > 0


def test_special_characters_in_title():
    """Test handling of special characters in title."""
    exporter = SlackExporter()
    plan = {"id": "test", "tasks": [], "milestones": [], "metadata": {}}
    brief = {
        "id": "brief-1",
        "source_brief_id": "source-1",
        "title": "Test with <special> & 'characters' \"quoted\"",
        "problem_statement": "Test",
        "mvp_goal": "Goal",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Plan",
        "definition_of_done": [],
    }

    payload = exporter.build_block_kit_payload(plan, brief)

    # Should handle special characters without errors
    assert "blocks" in payload


def test_large_plan_with_many_tasks():
    """Test handling of large plans with many tasks."""
    exporter = SlackExporter()

    # Create plan with 100 tasks
    tasks = [
        {
            "id": f"task-{i}",
            "title": f"Task {i}",
            "description": f"Description {i}",
            "acceptance_criteria": [f"AC {i}"],
            "status": "pending",
        }
        for i in range(100)
    ]

    plan = {
        "id": "large-plan",
        "implementation_brief_id": "brief-1",
        "tasks": tasks,
        "milestones": [],
        "metadata": {},
    }
    brief = {
        "id": "brief-1",
        "source_brief_id": "source-1",
        "title": "Large Plan",
        "problem_statement": "Test",
        "mvp_goal": "Goal",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Plan",
        "definition_of_done": [],
    }

    payload = exporter.build_block_kit_payload(plan, brief)

    # Should handle large plans without errors
    assert "blocks" in payload
    assert len(payload["blocks"]) <= exporter.MAX_BLOCKS


def test_mention_handling():
    """Test handling of @ mentions in text."""
    exporter = SlackExporter()
    plan = {"id": "test", "tasks": [], "milestones": [], "metadata": {}}
    brief = {
        "id": "brief-1",
        "source_brief_id": "source-1",
        "title": "Notify @channel about this feature",
        "problem_statement": "Test",
        "mvp_goal": "Goal",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Plan",
        "definition_of_done": [],
    }

    payload = exporter.build_block_kit_payload(plan, brief)

    # Should preserve mentions
    section_blocks = [b for b in payload["blocks"] if b.get("type") == "section"]
    assert any("@channel" in str(b) for b in section_blocks)


def test_export_with_attachment_format(sample_plan, sample_brief):
    """Test export with legacy attachment format."""
    config = SlackWebhookConfig(use_blocks=False)
    exporter = SlackExporter(config)

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = str(Path(tmpdir) / "output.json")
        result = exporter.export(sample_plan, sample_brief, output_path)

        assert result == output_path
        assert Path(output_path).exists()

        with open(output_path) as f:
            payload = json.load(f)

        assert "attachments" in payload
        assert len(payload["attachments"]) == 1
