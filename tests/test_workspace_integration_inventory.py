"""Tests for workspace integration inventory summaries."""

import json

from blueprint.workspace import TeamWorkspace, workspace_integration_inventory


def test_integration_inventory_summarizes_readiness_deterministically():
    manager = TeamWorkspace()
    workspace = manager.create_workspace(
        "Integrations",
        metadata={
            "integrations": [
                {
                    "id": "jira-main",
                    "type": "jira",
                    "enabled": True,
                    "owner": "platform",
                    "required_settings": ["base_url", "api_token"],
                    "settings": {
                        "base_url": "https://jira.example.test",
                        "api_token": "secret-token",
                    },
                },
                {
                    "id": "slack-alerts",
                    "type": "slack",
                    "enabled": True,
                    "owner": "ops",
                    "required_settings": ["webhook_url", "channel"],
                    "settings": {"channel": "#alerts"},
                },
                {
                    "id": "github-sync",
                    "type": "github",
                    "enabled": False,
                    "owner": "devtools",
                    "required_settings": ["app_id"],
                },
            ]
        },
    )

    inventory = manager.build_integration_inventory(workspace.workspace_id)

    assert inventory is not None
    assert inventory["inventory_type"] == "workspace_integration_inventory"
    assert inventory["workspace_id"] == workspace.workspace_id
    assert inventory["integration_count"] == 3
    assert inventory["readiness_counts"] == {
        "disabled": 1,
        "missing_required_settings": 1,
        "ready": 1,
    }
    assert [
        (entry["integration_type"], entry["readiness_status"])
        for entry in inventory["integrations"]
    ] == [
        ("github", "disabled"),
        ("jira", "ready"),
        ("slack", "missing_required_settings"),
    ]
    slack = inventory["integrations"][2]
    assert slack["owner"] == "ops"
    assert slack["enabled"] is True
    assert slack["missing_required_settings"] == ["webhook_url"]

    serialized = json.dumps(inventory)
    assert "secret-token" not in serialized
    assert "https://jira.example.test" not in serialized
    assert "#alerts" not in serialized


def test_integration_inventory_supports_dict_records_from_settings_metadata():
    manager = TeamWorkspace()
    workspace = manager.create_workspace(
        "Settings",
        settings={
            "metadata": {
                "workspace_integrations": {
                    "pagerduty": {
                        "provider": "pagerduty",
                        "enabled": "yes",
                        "configured_by": "incident-team",
                        "required": ["routing_key"],
                        "config": {"routing_key": "configured-secret"},
                    }
                }
            }
        },
    )

    inventory = workspace_integration_inventory(
        workspace,
        generated_at="2026-05-13T00:00:00+00:00",
    )

    assert inventory["generated_at"] == "2026-05-13T00:00:00+00:00"
    assert inventory["integrations"] == [
        {
            "integration_id": "pagerduty",
            "integration_type": "pagerduty",
            "enabled": True,
            "owner": "incident-team",
            "missing_required_settings": [],
            "readiness_status": "ready",
        }
    ]
    assert "configured-secret" not in json.dumps(inventory)


def test_empty_integration_inventory_has_stable_counts():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Empty")

    inventory = workspace_integration_inventory(
        workspace,
        generated_at="2026-05-13T00:00:00+00:00",
    )

    assert inventory == {
        "inventory_type": "workspace_integration_inventory",
        "generated_at": "2026-05-13T00:00:00+00:00",
        "workspace_id": workspace.workspace_id,
        "integration_count": 0,
        "readiness_counts": {
            "disabled": 0,
            "missing_required_settings": 0,
            "ready": 0,
        },
        "integrations": [],
    }


def test_integration_inventory_returns_none_for_missing_workspace():
    assert TeamWorkspace().build_integration_inventory("missing") is None
