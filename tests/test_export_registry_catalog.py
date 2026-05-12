import json

from blueprint.exporters.registry import (
    ExporterCatalogEntry,
    create_exporter,
    resolve_target_name,
    supported_target_aliases,
    supported_target_catalog,
    supported_target_catalog_dicts,
    supported_target_names,
)


def test_catalog_returns_one_record_per_canonical_target_in_display_order():
    catalog = supported_target_catalog()

    assert catalog
    assert all(isinstance(entry, ExporterCatalogEntry) for entry in catalog)
    assert tuple(entry.target for entry in catalog) == supported_target_names()
    assert len({entry.target for entry in catalog}) == len(catalog)


def test_catalog_entries_include_alias_metadata_and_binary_flag():
    catalog = {entry.target: entry for entry in supported_target_catalog()}

    relay_yaml = catalog["relay-yaml"]
    assert relay_yaml.default_format == "yaml"
    assert relay_yaml.extension == ".yaml"
    assert "relay_yaml" in relay_yaml.aliases
    assert relay_yaml.binary_like is False

    assert catalog["pdf-export"].binary_like is True
    assert catalog["docx-export"].binary_like is True


def test_catalog_dicts_are_json_compatible_and_lookup_behavior_remains():
    payload = supported_target_catalog_dicts()

    assert json.loads(json.dumps(payload)) == payload
    assert payload[0]["target"] == supported_target_names()[0]
    assert supported_target_aliases()["relay_yaml"] == "relay-yaml"
    assert resolve_target_name("relay_yaml") == "relay-yaml"
    assert create_exporter("relay_yaml") is not None
