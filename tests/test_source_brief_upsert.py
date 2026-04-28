import sqlite3
from datetime import datetime

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import Store, init_db


def test_store_gets_source_brief_by_upstream_identity(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief("sb-original", title="Original"))

    brief = store.get_source_brief_by_source("max", "design_brief", "dbf-123")

    assert brief["id"] == "sb-original"
    assert brief["title"] == "Original"


def test_upsert_source_brief_creates_new_source_brief(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))

    source_brief_id, created = store.upsert_source_brief(
        _source_brief("sb-new", title="New"),
    )

    assert source_brief_id == "sb-new"
    assert created is True
    briefs = store.list_source_briefs(source_project="max")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "New"


def test_upsert_source_brief_updates_existing_by_default(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    original = _source_brief("sb-original", title="Original")
    original["updated_at"] = datetime(2026, 1, 1, 0, 0, 0)
    store.insert_source_brief(original)

    source_brief_id, created = store.upsert_source_brief(
        _source_brief("sb-new", title="Updated"),
    )

    assert source_brief_id == "sb-original"
    assert created is False
    briefs = store.list_source_briefs(source_project="max")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "Updated"
    assert briefs[0]["updated_at"] != "2026-01-01T00:00:00"


def test_upsert_source_brief_can_skip_existing_without_changing_local_id(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief("sb-original", title="Original"))

    source_brief_id, created = store.upsert_source_brief(
        _source_brief("sb-new", title="Updated", summary="New summary"),
        skip_existing=True,
    )

    assert source_brief_id == "sb-original"
    assert created is False
    brief = store.get_source_brief("sb-original")
    assert brief["title"] == "Original"
    assert brief["summary"] == "Original summary"
    assert store.get_source_brief("sb-new") is None


def test_import_max_repeated_import_updates_existing_source_brief(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    max_db_path = tmp_path / "max.db"
    _write_max_db(max_db_path, title="Original")

    runner = CliRunner()
    first = runner.invoke(cli, ["import", "max", "dbf-123"])
    _update_max_title(max_db_path, "Updated")
    second = runner.invoke(cli, ["import", "max", "dbf-123"])

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert "Updated source brief" in second.output

    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="max")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "Updated"


def test_import_max_skip_existing_reuses_existing_source_brief(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    _write_max_db(tmp_path / "max.db", title="Original")

    runner = CliRunner()
    first = runner.invoke(cli, ["import", "max", "dbf-123"])
    second = runner.invoke(cli, ["import", "max", "dbf-123", "--skip-existing"])

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert "Skipped existing source brief" in second.output

    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="max")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "Original"


def test_import_max_rejects_conflicting_duplicate_options(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["import", "max", "dbf-123", "--replace", "--skip-existing"],
    )

    assert result.exit_code != 0
    assert "--replace and --skip-existing cannot be used together" in result.output


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
sources:
  max:
    db_path: {tmp_path / "max.db"}
"""
    )
    blueprint_config.reload_config()


def _write_max_db(db_path, title):
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE design_briefs (
                id TEXT PRIMARY KEY,
                title TEXT,
                domain TEXT,
                theme TEXT,
                readiness_score REAL,
                lead_idea_id TEXT,
                merged_product_concept TEXT,
                synthesis_rationale TEXT,
                why_this_now TEXT,
                mvp_scope TEXT,
                first_milestones TEXT,
                validation_plan TEXT,
                risks TEXT,
                buyer TEXT,
                specific_user TEXT,
                workflow_context TEXT,
                source_idea_ids TEXT,
                design_status TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE buildable_units (
                id TEXT PRIMARY KEY,
                title TEXT,
                one_liner TEXT,
                status TEXT,
                domain TEXT,
                category TEXT,
                problem TEXT,
                solution TEXT,
                value_proposition TEXT,
                buyer TEXT,
                specific_user TEXT,
                workflow_context TEXT,
                quality_score REAL,
                novelty_score REAL,
                usefulness_score REAL,
                tech_approach TEXT,
                suggested_stack TEXT,
                domain_risks TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE design_brief_sources (
                brief_id TEXT,
                idea_id TEXT,
                role TEXT,
                rank INTEGER
            )
            """
        )
        conn.execute(
            """
            INSERT INTO design_briefs VALUES (
                'dbf-123',
                ?,
                'testing',
                'developer tools',
                91.0,
                NULL,
                'Concept',
                'Rationale',
                'Now',
                '[]',
                '[]',
                'Validate with pytest',
                '[]',
                'Engineering',
                'Developer',
                'CLI',
                '[]',
                'candidate',
                '2026-01-01T00:00:00',
                '2026-01-01T00:00:00'
            )
            """,
            (title,),
        )


def _update_max_title(db_path, title):
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE design_briefs SET title = ? WHERE id = 'dbf-123'", (title,))


def _source_brief(brief_id, title, summary="Original summary"):
    return {
        "id": brief_id,
        "title": title,
        "domain": "testing",
        "summary": summary,
        "source_project": "max",
        "source_entity_type": "design_brief",
        "source_id": "dbf-123",
        "source_payload": {"title": title},
        "source_links": {"brief_id": "dbf-123"},
    }
