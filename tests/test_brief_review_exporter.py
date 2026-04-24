from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.brief_review import BriefReviewPacketExporter
from blueprint.store import Store, init_db


def test_brief_review_packet_renders_deterministic_markdown():
    packet = BriefReviewPacketExporter().render(_implementation_brief())

    assert packet.startswith("# Review Packet: Reviewable Brief\n")
    assert "## Traceability" in packet
    assert "- Implementation Brief ID: `ib-review`" in packet
    assert "- Source Brief ID: `sb-review`" in packet
    assert "## Normalized Summary" in packet
    assert "- Target Users: Operators" in packet
    assert "## Scope\n- Review packet exporter" in packet
    assert "## Non-Goals\n- Execution planning" in packet
    assert "## Assumptions\n- Reviewers approve briefs before planning" in packet
    assert "- Architecture Notes: Keep rendering deterministic" in packet
    assert "  - GitHub" in packet
    assert "## Risks\n- Review packets omit important context" in packet
    assert "## Validation Plan\nRun pytest" in packet
    assert "## Definition of Done\n- Packet includes review-ready sections" in packet
    assert "## Review Questions\n- None." in packet
    assert packet == BriefReviewPacketExporter().render(_implementation_brief())


def test_brief_review_packet_includes_source_payload_summary_when_requested():
    packet = BriefReviewPacketExporter().render(
        _implementation_brief(),
        source_brief=_source_brief(),
        include_source=True,
    )

    assert "## Source Metadata" in packet
    assert "- Source Brief ID: `sb-review`" in packet
    assert "- Source Project: product" in packet
    assert "- Source ID: `issue-123`" in packet
    assert "  - html_url: \"https://example.test/issues/123\"" in packet
    assert "- Source Payload Summary:" in packet
    assert (
        '  - body="Build a concise approval packet", labels=["planning", "review"], '
        'number=123, title="Approval Packet"'
    ) in packet


def test_brief_review_packet_turns_missing_optional_fields_into_questions():
    brief = _implementation_brief()
    for field in (
        "target_user",
        "buyer",
        "workflow_context",
        "product_surface",
        "architecture_notes",
        "data_requirements",
    ):
        brief[field] = None
    brief["integration_points"] = []

    packet = BriefReviewPacketExporter().render(brief)

    assert "- Target Users: N/A" in packet
    assert "- Who is the primary target user?" in packet
    assert "- Who is the buyer or decision-maker?" in packet
    assert "- What workflow context should planning preserve?" in packet
    assert "- What product surface is in scope?" in packet
    assert "- Are there architecture constraints or preferences?" in packet
    assert "- What data requirements must the plan account for?" in packet
    assert "- Which integration points should be considered?" in packet


def test_brief_review_packet_cli_prints_source_packet(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    store.insert_implementation_brief(_implementation_brief())

    result = CliRunner().invoke(
        cli,
        ["brief", "review-packet", "ib-review", "--source"],
    )

    assert result.exit_code == 0, result.output
    assert "# Review Packet: Reviewable Brief" in result.output
    assert "- Implementation Brief ID: `ib-review`" in result.output
    assert "- Source Project: product" in result.output
    assert "title=\"Approval Packet\"" in result.output


def test_brief_review_packet_cli_writes_output_file(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    store.insert_implementation_brief(_implementation_brief())
    output_path = tmp_path / "review.md"

    result = CliRunner().invoke(
        cli,
        ["brief", "review-packet", "ib-review", "--output", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    assert result.output == ""
    assert output_path.read_text().startswith("# Review Packet: Reviewable Brief\n")
    assert "## Source Metadata" not in output_path.read_text()


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


def _source_brief():
    return {
        "id": "sb-review",
        "title": "Approval Packet",
        "domain": "planning",
        "summary": "Teams need a compact review artifact.",
        "source_project": "product",
        "source_entity_type": "issue",
        "source_id": "issue-123",
        "source_payload": {
            "number": 123,
            "title": "Approval Packet",
            "body": "Build a concise approval packet",
            "labels": ["planning", "review"],
        },
        "source_links": {
            "html_url": "https://example.test/issues/123",
            "project": "https://example.test/projects/blueprint",
        },
    }


def _implementation_brief():
    return {
        "id": "ib-review",
        "source_brief_id": "sb-review",
        "title": "Reviewable Brief",
        "domain": "planning",
        "target_user": "Operators",
        "buyer": "Engineering leads",
        "workflow_context": "Brief approval before task generation",
        "problem_statement": "Teams need to approve briefs before planning.",
        "mvp_goal": "Render a compact Markdown review packet.",
        "product_surface": "CLI",
        "scope": ["Review packet exporter"],
        "non_goals": ["Execution planning"],
        "assumptions": ["Reviewers approve briefs before planning"],
        "architecture_notes": "Keep rendering deterministic",
        "data_requirements": "Source and implementation brief records",
        "integration_points": ["GitHub"],
        "risks": ["Review packets omit important context"],
        "validation_plan": "Run pytest",
        "definition_of_done": ["Packet includes review-ready sections"],
        "status": "draft",
    }
