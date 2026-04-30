import copy
import re

from blueprint.plan_redaction import redact_plan_payload, redact_text


def test_redact_plan_payload_recurses_through_nested_dicts_and_lists():
    payload = {
        "title": "Sensitive handoff",
        "owner": "alex@example.com",
        "tasks": [
            {
                "id": "TASK-123",
                "notes": [
                    "Send update to ops@example.org",
                    "OPENAI_API_KEY=sk-proj-abcdefghijklmnopqrstuvwxyz123456",
                ],
                "estimate": 3,
                "done": False,
            },
            {
                "id": "TASK-124",
                "webhook": "https://hooks.slack.com/services/T000/B000/abcdef123456",
                "github": "ghp_abcdefghijklmnopqrstuvwxyz1234567890",
            },
        ],
        "metadata": None,
    }

    assert redact_plan_payload(payload) == {
        "title": "Sensitive handoff",
        "owner": "[REDACTED]",
        "tasks": [
            {
                "id": "TASK-123",
                "notes": [
                    "Send update to [REDACTED]",
                    "OPENAI_API_KEY=[REDACTED]",
                ],
                "estimate": 3,
                "done": False,
            },
            {
                "id": "TASK-124",
                "webhook": "[REDACTED]",
                "github": "[REDACTED]",
            },
        ],
        "metadata": None,
    }


def test_redact_plan_payload_does_not_mutate_caller_input():
    payload = {
        "brief": {
            "contact": "source@example.com",
            "commands": ["ANTHROPIC_API_KEY=sk-ant-api03-abcdefghijklmnopqrstuvwxyz"],
        }
    }
    original = copy.deepcopy(payload)

    redacted = redact_plan_payload(payload)

    assert payload == original
    assert redacted is not payload
    assert redacted["brief"] is not payload["brief"]
    assert redacted["brief"]["commands"] is not payload["brief"]["commands"]


def test_redact_text_covers_builtin_secret_patterns():
    text = "\n".join(
        [
            "email=dev@example.com",
            "token: super-secret-token",
            "github=github_pat_abcdefghijklmnopqrstuvwxyz_1234567890",
            "slack=https://hooks.slack.com/services/T111/B222/secret333",
            "openai=sk-abcdefghijklmnopqrstuvwxyz123456",
            "anthropic=sk-ant-api03-abcdefghijklmnopqrstuvwxyz",
            "aws=AKIA1234567890ABCDEF",
            "google=AIzaSyA1234567890abcdefghijklmnopqrstuvwxyz12",
        ]
    )

    redacted = redact_text(text)

    assert "dev@example.com" not in redacted
    assert "super-secret-token" not in redacted
    assert "github_pat_" not in redacted
    assert "hooks.slack.com" not in redacted
    assert "sk-abcdefghijklmnopqrstuvwxyz123456" not in redacted
    assert "sk-ant-api03-abcdefghijklmnopqrstuvwxyz" not in redacted
    assert "AKIA1234567890ABCDEF" not in redacted
    assert "AIzaSyA1234567890abcdefghijklmnopqrstuvwxyz12" not in redacted
    assert redacted.count("[REDACTED]") == 8


def test_redact_text_accepts_custom_string_and_compiled_patterns():
    text = "Customer ACME-123 uses vault path vault://prod/payments"

    assert redact_text(text, patterns=[r"ACME-\d+", re.compile(r"vault://\S+")]) == (
        "Customer [REDACTED] uses vault path [REDACTED]"
    )


def test_redact_text_supports_custom_replacement():
    assert redact_text("Notify dev@example.com", replacement="[secret]") == "Notify [secret]"


def test_redaction_avoids_ordinary_task_ids_and_non_secret_identifiers():
    text = "Work items TASK-123, PLAN-2024, and tokenization-task remain visible."

    assert redact_text(text) == text
