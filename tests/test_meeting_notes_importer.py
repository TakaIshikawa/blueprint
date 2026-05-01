from blueprint.domain import SourceBrief
from blueprint.importers import MeetingNotesImporter
from blueprint.importers.meeting_notes_importer import parse_meeting_notes


def test_parse_markdown_meeting_notes_with_frontmatter_and_sections():
    source_brief = parse_meeting_notes(
        """---
title: Checkout Planning Sync
domain: checkout
source_id: meetings/checkout-planning-2026-01-15
date: 2026-01-15
attendees:
  - Alice
summary: Frontmatter summary overrides the note overview.
decisions:
  - Keep checkout retries in the admin surface.
actions:
  - text: Draft retry reason copy.
    owner: Alice
open_questions:
  - Which teams need dashboard access?
---
# Checkout Reliability Working Session

## Attendees
- Bob
- Alice

## Overview
The team reviewed checkout retry visibility for support workflows.

## Decisions
- Emit retry reason codes into the admin event timeline.
- Keep checkout retries in the admin surface.

## Action Items
- [ ] Add event timeline fixture coverage. Owner: Bob; Due: Friday

## Risks
- Admin timeline volume may grow quickly.

## Open Questions
- Should failed retries page support export?
""",
        file_path="notes/checkout.md",
    )

    assert SourceBrief.model_validate(source_brief)
    assert source_brief["id"] == "sb-meeting-6231c07e9842"
    assert source_brief["title"] == "Checkout Planning Sync"
    assert source_brief["domain"] == "checkout"
    assert source_brief["summary"] == "Frontmatter summary overrides the note overview."
    assert source_brief["source_project"] == "meeting_notes"
    assert source_brief["source_entity_type"] == "meeting_notes"
    assert source_brief["source_id"] == "meetings/checkout-planning-2026-01-15"

    payload = source_brief["source_payload"]
    assert payload["date"] == "2026-01-15"
    assert payload["participants"] == ["Alice", "Bob"]
    assert payload["decisions"] == [
        "Keep checkout retries in the admin surface.",
        "Emit retry reason codes into the admin event timeline.",
    ]
    assert payload["action_items"] == [
        {
            "text": "Draft retry reason copy.",
            "owner": "Alice",
            "due": None,
            "raw": {
                "text": "Draft retry reason copy.",
                "owner": "Alice",
            },
        },
        {
            "text": "Add event timeline fixture coverage.",
            "owner": "Bob",
            "due": "Friday",
            "raw": "- [ ] Add event timeline fixture coverage. Owner: Bob; Due: Friday",
        },
    ]
    assert payload["risks"] == ["Admin timeline volume may grow quickly."]
    assert payload["unresolved_questions"] == [
        "Which teams need dashboard access?",
        "Should failed retries page support export?",
    ]
    assert payload["normalized"]["source_metadata"]["frontmatter"]["source_id"] == (
        "meetings/checkout-planning-2026-01-15"
    )
    assert payload["normalized"]["decisions"] == payload["decisions"]
    assert payload["normalized"]["actions"] == payload["action_items"]
    assert payload["normalized"]["unresolved_questions"] == payload["unresolved_questions"]


def test_parse_plain_text_meeting_notes_from_inline_metadata():
    source_brief = parse_meeting_notes(
        """Title: Search Export Review
Date: 2026-02-03
Attendees: Mira, Jo

Reviewed search export filter behavior.

Decisions
- Reuse saved view state for CSV export.

Actions
- Update endpoint contract. Owner: Jo

Questions
- Does export need async status?
""",
        file_path="notes/search-export.txt",
    )

    assert SourceBrief.model_validate(source_brief)
    assert source_brief["title"] == "Search Export Review"
    assert source_brief["source_id"] == "meeting-notes/search-export"
    assert source_brief["source_payload"]["date"] == "2026-02-03"
    assert source_brief["source_payload"]["participants"] == ["Mira", "Jo"]
    assert source_brief["source_payload"]["decisions"] == ["Reuse saved view state for CSV export."]
    assert source_brief["source_payload"]["action_items"][0]["owner"] == "Jo"
    assert source_brief["source_payload"]["unresolved_questions"] == [
        "Does export need async status?"
    ]


def test_meeting_notes_importer_imports_file_and_validates_source(tmp_path):
    notes_path = tmp_path / "planning.markdown"
    notes_path.write_text(
        """# Planning Sync

## Decisions
- Keep the MVP narrow.
""",
        encoding="utf-8",
    )
    unsupported_path = tmp_path / "planning.json"
    unsupported_path.write_text("{}", encoding="utf-8")
    importer = MeetingNotesImporter()

    source_brief = importer.import_from_source(str(notes_path))

    assert source_brief["title"] == "Planning Sync"
    assert importer.validate_source(str(notes_path)) is True
    assert importer.validate_source(str(tmp_path / "missing.md")) is False
    assert importer.validate_source(str(unsupported_path)) is False


def test_blueprint_importers_lazy_export_exposes_meeting_notes_importer():
    assert MeetingNotesImporter.__name__ == "MeetingNotesImporter"
