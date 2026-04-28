from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.importers.slack_thread_importer import parse_slack_thread_transcript
from blueprint.store import Store, init_db


def test_timestamped_transcript_parsing_captures_participants_and_timestamps():
    source_brief = parse_slack_thread_transcript(
        """# Checkout Reliability Thread
Channel: #checkout
Thread URL: https://acme.slack.com/archives/C123/p1705320600000000

[2026-01-15 09:30] Alice: Checkout retries are hard to diagnose.
[2026-01-15 09:34] Bob: We should surface retry reason codes in the admin.
""",
        file_path="threads/checkout.md",
    )

    assert source_brief["title"] == "Checkout Reliability Thread"
    assert source_brief["source_project"] == "slack"
    assert source_brief["source_entity_type"] == "thread_transcript"
    assert source_brief["source_id"] == (
        "https://acme.slack.com/archives/C123/p1705320600000000"
    )
    payload = source_brief["source_payload"]
    assert payload["participants"] == ["Alice", "Bob"]
    assert payload["timestamps"] == ["2026-01-15 09:30", "2026-01-15 09:34"]
    assert payload["metadata"]["channel"] == "#checkout"
    assert payload["metadata"]["thread_url"] == (
        "https://acme.slack.com/archives/C123/p1705320600000000"
    )
    assert "Alice: Checkout retries" in source_brief["summary"]


def test_plain_speaker_transcript_without_metadata_imports():
    source_brief = parse_slack_thread_transcript(
        """Alice: Search exports lose filters.
Bob: The CSV endpoint can reuse saved view state.
""",
        file_path="threads/search-export.txt",
    )

    assert source_brief["title"] == "Search Export"
    assert source_brief["source_id"].endswith("threads/search-export.txt")
    assert source_brief["source_payload"]["participants"] == ["Alice", "Bob"]
    assert source_brief["source_payload"]["timestamps"] == []
    assert source_brief["source_payload"]["metadata"]["channel"] is None


def test_action_item_extraction_from_markers_and_checkboxes():
    source_brief = parse_slack_thread_transcript(
        """# Import Cleanup

[2026-01-15 10:00] Alice: TODO document retry behavior.
Bob: Action: add fixture coverage for plain text transcripts.
- [ ] Confirm CLI output with design operations.
""",
        file_path="threads/actions.md",
    )

    action_items = source_brief["source_payload"]["action_items"]
    assert [item["text"] for item in action_items] == [
        "document retry behavior.",
        "add fixture coverage for plain text transcripts.",
        "Confirm CLI output with design operations.",
    ]
    assert action_items[0]["speaker"] == "Alice"
    assert action_items[0]["timestamp"] == "2026-01-15 10:00"
    assert action_items[2]["speaker"] is None


def test_frontmatter_and_thread_metadata_are_preserved():
    source_brief = parse_slack_thread_transcript(
        """---
title: Frontmatter Slack Title
domain: operations
team: Design Ops
source_id: slack/thread-123
---
Channel: #design-ops
Thread: Intake review

Alice: Need a faster way to turn threads into source briefs.
""",
        file_path="threads/intake.md",
    )

    assert source_brief["title"] == "Frontmatter Slack Title"
    assert source_brief["domain"] == "operations"
    assert source_brief["source_id"] == "slack/thread-123"
    normalized = source_brief["source_payload"]["normalized"]
    assert normalized["channel"] == "#design-ops"
    assert normalized["thread"] == "Intake review"
    assert normalized["source_metadata"]["frontmatter"]["team"] == "Design Ops"
    assert normalized["source_metadata"]["transcript_metadata"]["channel"] == "#design-ops"


def test_cli_import_slack_thread_persists_source_brief(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    transcript_path = tmp_path / "thread.md"
    transcript_path.write_text(
        """# Slack Import Thread
Channel: #planning

[2026-01-15 09:30] Alice: Import these transcripts into Blueprint.
Bob: Action: verify persistence.
"""
    )

    result = CliRunner().invoke(cli, ["import", "slack-thread", str(transcript_path)])

    assert result.exit_code == 0, result.output
    assert "Imported source brief" in result.output
    assert "from Slack thread" in result.output
    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="slack")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "Slack Import Thread"
    assert briefs[0]["source_payload"]["participants"] == ["Alice", "Bob"]
    assert briefs[0]["source_payload"]["action_items"][0]["text"] == "verify persistence."


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path}
"""
    )
    blueprint_config.reload_config()
