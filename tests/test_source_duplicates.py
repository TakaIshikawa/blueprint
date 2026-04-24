import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.source_duplicates import find_duplicate_source_brief_groups
from blueprint.cli import cli
from blueprint.store import init_db


def test_groups_duplicate_source_briefs_with_deterministic_canonical_id():
    older = _source_brief(
        "sb-b",
        source_project="manual",
        source_id="manual-1",
        created_at="2024-01-02T00:00:00",
    )
    newer = _source_brief(
        "sb-a",
        source_project="graph",
        source_id="graph-9",
        created_at="2024-01-03T00:00:00",
    )
    unrelated = _source_brief(
        "sb-unrelated",
        title="Billing Export",
        summary="Download invoice records for accounting reconciliation.",
        source_project="manual",
        source_id="manual-2",
        created_at="2024-01-01T00:00:00",
    )

    report = find_duplicate_source_brief_groups(
        [newer, unrelated, older],
        threshold=0.75,
        limit=10,
    )

    assert report.candidate_count == 3
    assert report.duplicate_count == 2
    assert len(report.groups) == 1
    assert report.groups[0].canonical_id == "sb-b"
    assert [brief.id for brief in report.groups[0].briefs] == ["sb-b", "sb-a"]
    assert report.groups[0].pairs[0].matched_fields == [
        "title",
        "summary",
        "source_links",
        "source_identity",
    ]


def test_threshold_and_limit_filter_duplicate_groups():
    first = _source_brief("sb-1", source_project="manual", source_id="one")
    second = _source_brief("sb-2", source_project="graph", source_id="two")

    included = find_duplicate_source_brief_groups(
        [first, second],
        threshold=0.75,
        limit=1,
    )
    excluded_by_threshold = find_duplicate_source_brief_groups(
        [first, second],
        threshold=0.95,
        limit=1,
    )
    excluded_by_limit = find_duplicate_source_brief_groups(
        [first, second],
        threshold=0.75,
        limit=0,
    )

    assert len(included.groups) == 1
    assert excluded_by_threshold.groups == []
    assert excluded_by_limit.groups == []


def test_cli_json_output_is_stable_and_filters_by_source_project(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(
        _source_brief("sb-manual-1", source_project="manual", source_id="manual-1")
    )
    store.insert_source_brief(
        _source_brief("sb-manual-2", source_project="manual", source_id="manual-2")
    )
    store.insert_source_brief(
        _source_brief("sb-graph-1", source_project="graph", source_id="graph-1")
    )

    result = CliRunner().invoke(
        cli,
        [
            "source",
            "duplicates",
            "--source-project",
            "manual",
            "--threshold",
            "0.8",
            "--limit",
            "1",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["threshold"] == 0.8
    assert payload["limit"] == 1
    assert payload["source_project"] == "manual"
    assert payload["candidate_count"] == 2
    assert payload["summary"] == {"groups": 1, "duplicates": 2}
    assert payload["groups"][0]["canonical_id"] == "sb-manual-1"
    assert [brief["id"] for brief in payload["groups"][0]["briefs"]] == [
        "sb-manual-1",
        "sb-manual-2",
    ]
    assert payload["groups"][0]["pairs"] == [
        {
            "left_id": "sb-manual-1",
            "right_id": "sb-manual-2",
            "score": 0.87,
            "matched_fields": [
                "title",
                "summary",
                "source_links",
                "source_identity",
            ],
        }
    ]


def test_cli_human_output_reports_canonical_suggestion(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief("sb-1", source_project="manual", source_id="one"))
    store.insert_source_brief(_source_brief("sb-2", source_project="graph", source_id="two"))

    result = CliRunner().invoke(cli, ["source", "duplicates", "--threshold", "0.75"])

    assert result.exit_code == 0, result.output
    assert "Source brief duplicate report" in result.output
    assert "canonical=sb-1" in result.output
    assert "sb-1<->sb-2" in result.output


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
"""
    )
    blueprint_config.reload_config()


def _source_brief(
    brief_id,
    *,
    title="Patient Intake Dashboard",
    summary="Give nurses a queue for reviewing patient intake forms.",
    source_project,
    source_id,
    created_at=None,
):
    brief = {
        "id": brief_id,
        "title": title,
        "domain": "healthcare",
        "summary": summary,
        "source_project": source_project,
        "source_entity_type": "note",
        "source_id": source_id,
        "source_payload": {"title": title},
        "source_links": {"path": f"{source_id}.md"},
    }
    if created_at is not None:
        brief["created_at"] = created_at
    return brief
