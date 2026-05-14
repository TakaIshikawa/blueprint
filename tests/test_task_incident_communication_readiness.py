import json

from blueprint.task_incident_communication_readiness import (
    analyze_task_incident_communication_readiness,
    build_task_incident_communication_readiness_plan,
    recommend_task_incident_communication_readiness,
    task_incident_communication_readiness_plan_to_dict,
    task_incident_communication_readiness_plan_to_dicts,
    task_incident_communication_readiness_plan_to_markdown,
)


def test_complete_incident_communication_task_is_ready():
    result = build_task_incident_communication_readiness_plan(
        {
            "id": "plan-incident-comms",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Incident communication status page updates",
                    "description": "Customer incident notices and escalation broadcast cover service disruption notice flows.",
                    "acceptance_criteria": [
                        "Audience segmentation targets affected customers, tenant segment, region segment, and stakeholder groups.",
                        "Severity thresholds define Sev1, Sev2, and impact threshold rules.",
                        "Message templates include notification template, email template, and status template copy.",
                        "Approval owner names the incident commander, communications owner, approver, and sign-off.",
                        "Channel selection covers status page, email, SMS, Slack, in-app, webhook, and support portal.",
                        "Timing cadence covers initial update, update cadence every 30 minutes, and post-incident timing.",
                        "Localization and accessibility include translation, WCAG, screen reader, and plain language.",
                        "Tests include template tests, notification tests, and status page tests.",
                    ],
                    "files_or_modules": ["src/incidents/status_page/incident_notifications.py"],
                }
            ],
        }
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "audience_segmentation",
        "severity_thresholds",
        "message_templates",
        "approval_owner",
        "channel_selection",
        "timing_cadence",
        "localization_accessibility",
        "tests",
    )


def test_partial_incident_communication_reports_gaps_and_ignores_no_impact():
    result = analyze_task_incident_communication_readiness(
        [
            {
                "id": "task-partial",
                "title": "Add incident communication flow",
                "description": "Status page channel selection has an incident commander owner.",
                "metadata": {"incident": {"severity": "Severity thresholds include Sev1 incidents."}},
                "validation_commands": ["python -m pytest tests/incidents/test_status_page_notifications.py"],
            },
            {
                "id": "task-copy",
                "title": "Incident docs cleanup",
                "description": "No incident communication, status page, customer incident notices, or escalation broadcasts changes are planned.",
            },
        ]
    )

    record = result.records[0]
    assert result.ignored_task_ids == ("task-copy",)
    assert record.readiness == "partial"
    assert record.present_criteria == ("severity_thresholds", "approval_owner", "channel_selection", "tests")
    assert record.missing_criteria == ("audience_segmentation", "message_templates", "timing_cadence", "localization_accessibility")
    assert any("metadata.incident.severity" in item for item in record.evidence)
    assert any("validation_commands[0]" in item for item in record.evidence)


def test_incident_communication_path_hints_serialization_and_markdown_are_stable():
    result = build_task_incident_communication_readiness_plan(
        {"id": "plan-path", "tasks": [{"id": "task-path", "title": "Refactor notifier", "files_or_modules": ["src/status_page/incident_notice_sender.py"]}]}
    )
    payload = task_incident_communication_readiness_plan_to_dict(result)

    assert result.records[0].detected_signals == ("incident_communication", "status_page", "incident_notification")
    assert recommend_task_incident_communication_readiness(result) == result.records
    assert task_incident_communication_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-path"
    assert task_incident_communication_readiness_plan_to_markdown(result).startswith("# Task Incident Communication Readiness: plan-path")
