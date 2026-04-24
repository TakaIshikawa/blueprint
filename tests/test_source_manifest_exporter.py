from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.source_manifest import SourceManifestExporter
from blueprint.store import init_db


def test_source_manifest_exporter_groups_by_project_and_domain():
    rendered = SourceManifestExporter().render_markdown(
        [
            _source_brief(
                brief_id="sb-manual-1",
                title="Patient Intake Dashboard",
                source_project="manual",
                domain="healthcare",
                source_id="manual-1",
                source_links={"path": "briefs/patient-intake.md"},
                summary="Give nurses a queue for reviewing patient intake forms.",
            ),
            _source_brief(
                brief_id="sb-max-1",
                title="Claims Review Workspace",
                source_project="max",
                domain="finance",
                source_entity_type="design_brief",
                source_id="max-1",
                source_links={"html_url": "https://example.test/max-1"},
                summary="Help analysts review pending reimbursement claims.",
            ),
        ],
        limit=50,
    )

    assert rendered.startswith("# Source Brief Manifest\n")
    assert "## Source Project: manual" in rendered
    assert "### Domain: healthcare" in rendered
    assert "#### Patient Intake Dashboard" in rendered
    assert "- Source Brief ID: `sb-manual-1`" in rendered
    assert "- Source Identity: `manual/note/manual-1`" in rendered
    assert '  - path: "briefs/patient-intake.md"' in rendered
    assert "## Source Project: max" in rendered
    assert "### Domain: finance" in rendered
    assert "#### Claims Review Workspace" in rendered


def test_source_manifest_exporter_filters_to_selected_records(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(
        _source_brief(
            brief_id="sb-manual",
            title="Manual Planning Brief",
            source_project="manual",
            domain="operations",
            source_id="manual-1",
        )
    )
    store.insert_source_brief(
        _source_brief(
            brief_id="sb-max",
            title="Max Planning Brief",
            source_project="max",
            domain="operations",
            source_entity_type="design_brief",
            source_id="max-1",
        )
    )

    rendered = SourceManifestExporter().render_markdown(
        store.list_source_briefs(source_project="manual"),
        source_project="manual",
        limit=50,
    )

    assert "- Source Project: manual" in rendered
    assert "Manual Planning Brief" in rendered
    assert "sb-manual" in rendered
    assert "Max Planning Brief" not in rendered
    assert "sb-max" not in rendered


def test_source_manifest_exporter_renders_duplicate_and_similarity_hints():
    first = _source_brief(
        brief_id="sb-one",
        title="Patient Intake Dashboard",
        source_project="manual",
        domain="healthcare",
        source_id="intake-one",
        source_links={"path": "briefs/intake.md"},
        summary="Give nurses a queue for reviewing patient intake forms.",
    )
    second = _source_brief(
        brief_id="sb-two",
        title="Patient Intake Dashboard",
        source_project="manual",
        domain="healthcare",
        source_id="intake-two",
        source_links={"path": "briefs/intake.md"},
        summary="Give nurses a queue for reviewing patient intake forms.",
    )

    rendered = SourceManifestExporter().render_markdown([first, second], limit=50)

    assert "- Duplicate/Similarity Hints:" in rendered
    assert "Duplicate group canonical `sb-one`" in rendered
    assert "sb-one<->sb-two" in rendered
    assert "Similar briefs: `sb-two`" in rendered


def test_source_manifest_exporter_empty_selection_is_valid_markdown():
    rendered = SourceManifestExporter().render_markdown(
        [],
        source_project="missing",
        limit=10,
    )

    assert rendered == (
        "# Source Brief Manifest\n"
        "\n"
        "## Selection\n"
        "- Source Project: missing\n"
        "- Limit: 10\n"
        "- Briefs: 0\n"
        "\n"
        "## No Matching Source Briefs\n"
        "No source briefs matched the selected filters.\n"
    )


def test_source_manifest_cli_writes_empty_manifest_for_empty_database(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    output_path = tmp_path / "source-manifest.md"

    result = CliRunner().invoke(
        cli,
        [
            "source",
            "manifest",
            "--output",
            str(output_path),
            "--source-project",
            "manual",
            "--limit",
            "5",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output == ""
    assert output_path.read_text() == (
        "# Source Brief Manifest\n"
        "\n"
        "## Selection\n"
        "- Source Project: manual\n"
        "- Limit: 5\n"
        "- Briefs: 0\n"
        "\n"
        "## No Matching Source Briefs\n"
        "No source briefs matched the selected filters.\n"
    )


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
    *,
    brief_id: str,
    title: str,
    source_project: str,
    domain: str,
    source_id: str,
    source_entity_type: str = "note",
    summary: str = "Create a reliable planning intake for source briefs.",
    source_links: dict | None = None,
):
    return {
        "id": brief_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": source_project,
        "source_entity_type": source_entity_type,
        "source_id": source_id,
        "source_payload": {"title": title},
        "source_links": source_links or {"html_url": f"https://example.test/{source_id}"},
    }
