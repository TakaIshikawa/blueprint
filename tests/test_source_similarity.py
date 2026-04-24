import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.source_similarity import find_similar_source_briefs
from blueprint.cli import cli
from blueprint.store import Store, init_db


def test_finds_exact_duplicate_source_brief_without_returning_self():
    target = _source_brief("sb-target", source_id="alpha")
    duplicate = _source_brief("sb-duplicate", source_id="beta")
    unrelated = _source_brief(
        "sb-unrelated",
        title="Billing Export",
        summary="Download invoice records for accounting reconciliation.",
        source_id="gamma",
    )

    matches = find_similar_source_briefs(
        target,
        [target, unrelated, duplicate],
        threshold=0.5,
        limit=10,
    )

    assert [match.id for match in matches] == ["sb-duplicate"]
    assert matches[0].score == 0.8
    assert "title" in matches[0].matched_fields
    assert "summary" in matches[0].matched_fields


def test_finds_partial_title_and_summary_matches():
    target = _source_brief(
        "sb-target",
        title="Patient Intake Dashboard",
        summary="Give nurses a queue for reviewing patient intake forms.",
        source_id="alpha",
    )
    partial = _source_brief(
        "sb-partial",
        title="Patient Intake Review",
        summary="A nurse review queue for patient intake submissions.",
        source_id="beta",
    )

    matches = find_similar_source_briefs(target, [target, partial], threshold=0.5, limit=10)

    assert [match.id for match in matches] == ["sb-partial"]
    assert matches[0].matched_fields == ["title", "summary", "domain", "source_project"]


def test_threshold_filters_matches_deterministically():
    target = _source_brief("sb-target", source_id="alpha")
    duplicate = _source_brief("sb-duplicate", source_id="beta")

    included = find_similar_source_briefs(target, [target, duplicate], threshold=0.8, limit=10)
    excluded = find_similar_source_briefs(target, [target, duplicate], threshold=0.8001, limit=10)

    assert [match.id for match in included] == ["sb-duplicate"]
    assert excluded == []


def test_limit_selects_highest_scores_then_id_for_ties():
    target = _source_brief("sb-target", source_id="alpha")
    tie_b = _source_brief("sb-b", source_id="beta")
    tie_a = _source_brief("sb-a", source_id="gamma")
    partial = _source_brief(
        "sb-partial",
        title="Patient Intake Review",
        summary="A nurse review queue for patient intake submissions.",
        source_id="delta",
    )

    matches = find_similar_source_briefs(
        target,
        [partial, tie_b, target, tie_a],
        threshold=0.5,
        limit=2,
    )

    assert [match.id for match in matches] == ["sb-a", "sb-b"]


def test_cli_json_output_contains_stable_similarity_result_objects(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief("sb-target", source_id="alpha"))
    store.insert_source_brief(_source_brief("sb-duplicate", source_id="beta"))

    result = CliRunner().invoke(
        cli,
        ["source", "similar", "sb-target", "--threshold", "0.8", "--limit", "1", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == {
        "brief_id": "sb-target",
        "limit": 1,
        "threshold": 0.8,
        "matches": [
            {
                "id": "sb-duplicate",
                "title": "Patient Intake Dashboard",
                "source_project": "manual",
                "score": 0.8,
                "matched_fields": ["title", "summary", "domain", "source_project"],
            }
        ],
    }


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
    source_id,
):
    return {
        "id": brief_id,
        "title": title,
        "domain": "healthcare",
        "summary": summary,
        "source_project": "manual",
        "source_entity_type": "note",
        "source_id": source_id,
        "source_payload": {"title": title},
        "source_links": {"path": f"{source_id}.md"},
    }
