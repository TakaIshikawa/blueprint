import pytest

from blueprint.domain import SourceBrief
from blueprint.importers.yaml_backlog_importer import YamlBacklogImporter


def test_yaml_backlog_importer_imports_multiple_records(tmp_path):
    yaml_path = _write_backlog(tmp_path)

    source_briefs = YamlBacklogImporter().import_file(str(yaml_path))

    assert len(source_briefs) == 2
    assert [brief["source_id"] for brief in source_briefs] == ["YAML-1", "items-2"]
    assert source_briefs[0]["id"] == YamlBacklogImporter().import_file(str(yaml_path))[0]["id"]
    assert source_briefs[0]["title"] == "Import YAML backlog"
    assert source_briefs[0]["summary"] == "Normalize YAML tasks into SourceBriefs."
    assert source_briefs[0]["domain"] == "planning"
    assert source_briefs[0]["source_project"] == "yaml-backlog"
    assert source_briefs[0]["source_entity_type"] == "item"
    assert source_briefs[0]["source_links"]["links"] == ["https://example.com/spec"]
    assert source_briefs[0]["source_payload"]["normalized"]["labels"] == [
        "importer",
        "yaml",
    ]
    assert source_briefs[0]["source_payload"]["normalized"]["owner"] == "platform"
    assert source_briefs[0]["source_payload"]["normalized"]["priority"] == "high"
    assert source_briefs[0]["source_payload"]["normalized"]["acceptance_criteria"] == [
        "Multiple records import",
        "Payload metadata is retained",
    ]
    assert source_briefs[1]["source_entity_type"] == "item"
    assert source_briefs[1]["source_payload"]["normalized"]["scope"] == ["discovery"]


def test_yaml_backlog_importer_imports_single_record(tmp_path):
    yaml_path = tmp_path / "single.yaml"
    yaml_path.write_text(
        """
source_id: YAML-SINGLE
title: Single YAML backlog
summary: Import a single YAML mapping.
domain: cli
labels: cli, yaml
owner: ops
priority: 2
acceptance_criteria:
  - A single mapping becomes one SourceBrief
"""
    )

    source_brief = YamlBacklogImporter().import_from_source(str(yaml_path))

    assert source_brief["source_id"] == "YAML-SINGLE"
    assert source_brief["title"] == "Single YAML backlog"
    assert source_brief["summary"] == "Import a single YAML mapping."
    assert source_brief["source_payload"]["collection"] == "record"
    assert source_brief["source_payload"]["normalized"]["labels"] == ["cli", "yaml"]
    assert source_brief["source_payload"]["normalized"]["owner"] == "ops"
    assert source_brief["source_payload"]["normalized"]["priority"] == "2"
    assert source_brief["source_payload"]["normalized"]["acceptance_criteria"] == [
        "A single mapping becomes one SourceBrief"
    ]


def test_yaml_backlog_records_validate_as_source_briefs(tmp_path):
    yaml_path = _write_backlog(tmp_path)

    source_briefs = YamlBacklogImporter().import_file(yaml_path)

    for source_brief in source_briefs:
        validated = SourceBrief.model_validate(source_brief)
        assert validated.title
        assert validated.summary
        assert validated.source_id


def test_yaml_backlog_importer_reports_missing_title_and_summary(tmp_path):
    missing_title_path = tmp_path / "missing-title.yaml"
    missing_title_path.write_text(
        """
items:
  - source_id: YAML-1
    summary: Missing title
"""
    )
    missing_summary_path = tmp_path / "missing-summary.yaml"
    missing_summary_path.write_text(
        """
items:
  - source_id: YAML-1
    title: Missing summary
"""
    )

    importer = YamlBacklogImporter()

    with pytest.raises(
        ImportError,
        match=r"YAML items\[1\] record must include a non-empty title",
    ):
        importer.import_file(str(missing_title_path))

    with pytest.raises(
        ImportError,
        match=r"YAML items\[1\] record must include a non-empty summary",
    ):
        importer.import_file(str(missing_summary_path))

    assert importer.validate_source(str(missing_title_path)) is False
    assert importer.validate_source(str(missing_summary_path)) is False


def test_yaml_backlog_list_available_respects_limit_and_includes_metadata(tmp_path):
    yaml_path = _write_backlog(tmp_path)

    available = YamlBacklogImporter(yaml_path).list_available(limit=1)

    assert available == [
        {
            "id": "YAML-1",
            "title": "Import YAML backlog",
            "metadata": {
                "domain": "planning",
                "source_project": "yaml-backlog",
                "source_entity_type": "item",
                "file_path": str(yaml_path.resolve()),
                "collection": "items",
                "index": 1,
            },
        }
    ]
    assert YamlBacklogImporter(yaml_path).list_available(limit=0) == []
    assert YamlBacklogImporter(tmp_path / "missing.yaml").list_available(limit=5) == []


def _write_backlog(tmp_path):
    yaml_path = tmp_path / "backlog.yaml"
    yaml_path.write_text(
        """
items:
  - source_id: YAML-1
    title: Import YAML backlog
    summary: Normalize YAML tasks into SourceBriefs.
    domain: planning
    scope:
      - importer
    non_goals:
      - CLI wiring
    assumptions:
      - PyYAML is available
    risks:
      - Input shapes vary
    acceptance_criteria:
      - Multiple records import
      - Payload metadata is retained
    labels:
      - importer
      - yaml
    owner: platform
    priority: high
    links: https://example.com/spec
  - title: List YAML records
    summary: Expose YAML backlog records for discovery.
    domain: cli
    scope: discovery
"""
    )
    return yaml_path
