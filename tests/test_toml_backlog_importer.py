from blueprint.domain import SourceBrief
from blueprint.importers.toml_backlog_importer import TomlBacklogImporter


def test_toml_backlog_importer_imports_briefs_and_items(tmp_path):
    toml_path = _write_backlog(tmp_path)

    source_briefs = TomlBacklogImporter().import_file(str(toml_path))

    assert len(source_briefs) == 3
    assert [brief["source_id"] for brief in source_briefs] == [
        "BRIEF-1",
        "briefs-2",
        "ITEM-1",
    ]
    assert source_briefs[0]["id"] == TomlBacklogImporter().import_file(str(toml_path))[0]["id"]
    assert source_briefs[0]["title"] == "Import TOML backlog"
    assert source_briefs[0]["summary"] == "Normalize TOML backlog rows into SourceBriefs."
    assert source_briefs[0]["domain"] == "planning"
    assert source_briefs[0]["source_project"] == "toml-backlog"
    assert source_briefs[0]["source_entity_type"] == "brief"
    assert source_briefs[0]["source_links"]["links"] == ["https://example.com/spec"]
    assert source_briefs[0]["source_payload"]["normalized"]["tags"] == ["importer", "toml"]
    assert source_briefs[1]["summary"] == "Title fallback"
    assert source_briefs[2]["source_entity_type"] == "item"
    assert source_briefs[2]["source_links"]["links"] == [
        {"url": "https://example.com/item", "title": "Item"}
    ]


def test_toml_backlog_records_validate_as_source_briefs(tmp_path):
    toml_path = _write_backlog(tmp_path)

    source_briefs = TomlBacklogImporter().import_file(toml_path)

    for source_brief in source_briefs:
        validated = SourceBrief.model_validate(source_brief)
        assert validated.title
        assert validated.summary
        assert validated.source_id


def test_toml_backlog_importer_validation_rejects_missing_invalid_and_bad_records(tmp_path):
    valid_path = _write_backlog(tmp_path)
    invalid_path = tmp_path / "invalid.toml"
    invalid_path.write_text("[[briefs]\ntitle = 'Broken'\n")
    missing_title_path = tmp_path / "missing-title.toml"
    missing_title_path.write_text("[[briefs]]\nsummary = 'No title'\n")
    invalid_records_path = tmp_path / "invalid-records.toml"
    invalid_records_path.write_text("[briefs]\ntitle = 'Not an array'\n")

    importer = TomlBacklogImporter()

    assert importer.validate_source(str(valid_path)) is True
    assert importer.validate_source(str(tmp_path / "missing.toml")) is False
    assert importer.validate_source(str(invalid_path)) is False
    assert importer.validate_source(str(missing_title_path)) is False
    assert importer.validate_source(str(invalid_records_path)) is False


def test_toml_backlog_list_available_respects_limit_and_includes_metadata(tmp_path):
    toml_path = _write_backlog(tmp_path)

    available = TomlBacklogImporter(toml_path).list_available(limit=2)

    assert available == [
        {
            "id": "BRIEF-1",
            "title": "Import TOML backlog",
            "metadata": {
                "domain": "planning",
                "source_project": "toml-backlog",
                "source_entity_type": "brief",
                "file_path": str(toml_path.resolve()),
                "collection": "briefs",
                "index": 1,
            },
        },
        {
            "id": "briefs-2",
            "title": "Title fallback",
            "metadata": {
                "domain": None,
                "source_project": "toml-backlog",
                "source_entity_type": "brief",
                "file_path": str(toml_path.resolve()),
                "collection": "briefs",
                "index": 2,
            },
        },
    ]
    assert TomlBacklogImporter(toml_path).list_available(limit=0) == []
    assert TomlBacklogImporter(tmp_path / "missing.toml").list_available(limit=5) == []


def _write_backlog(tmp_path):
    toml_path = tmp_path / "backlog.toml"
    toml_path.write_text(
        """
[[briefs]]
id = "BRIEF-1"
title = "Import TOML backlog"
summary = "Normalize TOML backlog rows into SourceBriefs."
domain = "planning"
links = "https://example.com/spec"
tags = ["importer", "toml"]

[[briefs]]
title = "Title fallback"

[[items]]
source_id = "ITEM-1"
title = "List TOML records"
description = "Expose TOML backlog records for discovery."
domain = "cli"
links = [{ url = "https://example.com/item", title = "Item" }]
"""
    )
    return toml_path
