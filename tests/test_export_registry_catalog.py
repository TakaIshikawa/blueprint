from blueprint.exporters.registry import (
    create_exporter,
    exporter_catalog,
    resolve_target_name,
    supported_target_aliases,
    supported_target_names,
)


def test_exporter_catalog_returns_one_record_per_canonical_target_in_order():
    catalog = exporter_catalog()

    assert [record["target"] for record in catalog] == list(supported_target_names())
    assert len(catalog) == len(supported_target_names())
    assert list(catalog[0]) == ["target", "default_format", "extension", "aliases", "binary_like"]
    assert catalog[0]["target"] == "adr"
    assert catalog[-1]["target"] == "docx-export"


def test_exporter_catalog_includes_aliases_and_binary_like_metadata():
    records = {record["target"]: record for record in exporter_catalog()}

    assert "relay_yaml" in records["relay-yaml"]["aliases"]
    assert "github_actions" in records["github-actions"]["aliases"]
    assert records["pdf-export"]["binary_like"] is True
    assert records["docx-export"]["binary_like"] is True
    assert records["relay"]["binary_like"] is False


def test_existing_registry_lookup_behavior_remains_compatible():
    aliases = supported_target_aliases()

    assert aliases["relay_yaml"] == "relay-yaml"
    assert resolve_target_name("relay_yaml") == "relay-yaml"
    assert create_exporter("relay_yaml") is not None
    assert "relay_yaml" in supported_target_names(include_aliases=True)
