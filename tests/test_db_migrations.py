import sqlite3

from click.testing import CliRunner
from sqlalchemy import create_engine, inspect

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import current_db_revision, init_db, migrate_db


EXPECTED_TABLES = {
    "alembic_version",
    "execution_plans",
    "execution_tasks",
    "export_records",
    "implementation_briefs",
    "source_briefs",
    "status_events",
}


def test_fresh_database_can_be_created_through_alembic_migration(tmp_path):
    db_path = tmp_path / "blueprint.db"

    migrate_db(str(db_path))

    engine = create_engine(f"sqlite:///{db_path}")
    inspector = inspect(engine)
    assert EXPECTED_TABLES.issubset(set(inspector.get_table_names()))
    assert current_db_revision(str(db_path)) == "0001_initial_schema"

    task_columns = {column["name"] for column in inspector.get_columns("execution_tasks")}
    assert {
        "metadata",
        "estimated_hours",
        "risk_level",
        "test_command",
    }.issubset(task_columns)


def test_db_migrate_and_current_cli_use_configured_database(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)

    migrate_result = CliRunner().invoke(cli, ["db", "migrate"])
    current_result = CliRunner().invoke(cli, ["db", "current"])

    assert migrate_result.exit_code == 0, migrate_result.output
    assert "Database migrated to head" in migrate_result.output
    assert current_result.exit_code == 0, current_result.output
    assert current_result.output.strip() == "0001_initial_schema"


def test_db_init_remains_backward_compatible_and_stamps_head(tmp_path):
    db_path = tmp_path / "blueprint.db"

    store = init_db(str(db_path))

    inspector = inspect(store.engine)
    assert EXPECTED_TABLES.issubset(set(inspector.get_table_names()))
    assert current_db_revision(str(db_path)) == "0001_initial_schema"


def test_migrate_adopts_unversioned_existing_database(tmp_path):
    db_path = tmp_path / "blueprint.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE execution_tasks (id TEXT PRIMARY KEY)")

    migrate_db(str(db_path))

    engine = create_engine(f"sqlite:///{db_path}")
    inspector = inspect(engine)
    assert EXPECTED_TABLES.issubset(set(inspector.get_table_names()))
    assert current_db_revision(str(db_path)) == "0001_initial_schema"


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
