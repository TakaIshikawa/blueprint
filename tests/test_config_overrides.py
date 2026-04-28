from pathlib import Path

from blueprint.config import Config


def test_config_paths_expand_user_and_environment_variables(tmp_path, monkeypatch):
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    data_dir = "data"
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setenv("BLUEPRINT_TEST_DATA", data_dir)

    config_path = tmp_path / ".blueprint.yaml"
    config_path.write_text(
        """
database:
  path: ~/.blueprint/${BLUEPRINT_TEST_DATA}/blueprint.db
sources:
  max:
    db_path: ${BLUEPRINT_TEST_DATA}/max.db
  graph:
    path: ~/graphs/${BLUEPRINT_TEST_DATA}
exports:
  output_dir: ${BLUEPRINT_TEST_DATA}/exports
"""
    )

    config = Config(str(config_path))

    assert config.db_path == str(home_dir / ".blueprint" / data_dir / "blueprint.db")
    assert config.max_db_path == f"{data_dir}/max.db"
    assert config.get("sources.graph.path") == str(home_dir / "graphs" / data_dir)
    assert config.export_dir == f"{data_dir}/exports"


def test_environment_overrides_take_precedence_over_yaml(tmp_path, monkeypatch):
    yaml_db = tmp_path / "yaml" / "blueprint.db"
    yaml_max = tmp_path / "yaml" / "max.db"
    yaml_graph = tmp_path / "yaml" / "graph"
    yaml_exports = tmp_path / "yaml" / "exports"

    env_db = tmp_path / "env" / "blueprint.db"
    env_max = tmp_path / "env" / "max.db"
    env_graph = tmp_path / "env" / "graph"
    env_exports = tmp_path / "env" / "exports"

    monkeypatch.setenv("BLUEPRINT_DB_PATH", str(env_db))
    monkeypatch.setenv("BLUEPRINT_MAX_DB_PATH", str(env_max))
    monkeypatch.setenv("BLUEPRINT_GRAPH_PATH", str(env_graph))
    monkeypatch.setenv("BLUEPRINT_EXPORT_DIR", str(env_exports))

    config_path = tmp_path / ".blueprint.yaml"
    config_path.write_text(
        f"""
database:
  path: {yaml_db}
sources:
  max:
    db_path: {yaml_max}
  graph:
    path: {yaml_graph}
exports:
  output_dir: {yaml_exports}
"""
    )

    config = Config(str(config_path))

    assert config.db_path == str(env_db)
    assert config.max_db_path == str(env_max)
    assert config.get("sources.graph.path") == str(env_graph)
    assert config.export_dir == str(env_exports)


def test_generic_source_path_environment_overrides(tmp_path, monkeypatch):
    custom_db = tmp_path / "custom.db"
    custom_dir = tmp_path / "custom-dir"
    monkeypatch.setenv("BLUEPRINT_CUSTOM_DB_PATH", str(custom_db))
    monkeypatch.setenv("BLUEPRINT_NOTES_PATH", str(custom_dir))

    config_path = tmp_path / ".blueprint.yaml"
    config_path.write_text(
        f"""
sources:
  custom:
    db_path: {tmp_path / "yaml.db"}
  notes:
    path: {tmp_path / "yaml-notes"}
"""
    )

    config = Config(str(config_path))

    assert config.get("sources.custom.db_path") == str(custom_db)
    assert config.get("sources.notes.path") == str(custom_dir)


def test_defaults_still_load_without_config_or_overrides(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BLUEPRINT_DB_PATH", raising=False)
    monkeypatch.delenv("BLUEPRINT_EXPORT_DIR", raising=False)
    monkeypatch.delenv("BLUEPRINT_MAX_DB_PATH", raising=False)
    monkeypatch.delenv("BLUEPRINT_GRAPH_PATH", raising=False)

    config = Config()

    assert config.config_path is None
    assert config.db_path == str(Path.home() / ".blueprint" / "blueprint.db")
    assert config.max_db_path == str(Path.home() / "Project" / "experiments" / "max" / "max.db")
    assert config.export_dir == str(Path.home() / "blueprint-exports")
