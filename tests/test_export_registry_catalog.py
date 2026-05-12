from blueprint.exporters.registry import (
    ExporterCatalogEntry,
    create_exporter,
    get_exporter_registration,
    supported_target_aliases,
    supported_target_catalog,
    supported_target_catalog_dicts,
    supported_target_names,
)


def test_supported_target_catalog_returns_one_entry_per_canonical_target_in_order():
    catalog = supported_target_catalog()

    assert all(isinstance(entry, ExporterCatalogEntry) for entry in catalog)
    assert tuple(entry.target for entry in catalog) == supported_target_names()
    assert len(catalog) == len(set(entry.target for entry in catalog))
    assert supported_target_catalog_dicts()[0] == catalog[0].to_dict()


def test_catalog_includes_alias_metadata_and_default_export_details():
    catalog = {entry.target: entry for entry in supported_target_catalog()}

    relay_yaml = catalog["relay-yaml"]
    assert relay_yaml.default_format == "yaml"
    assert relay_yaml.extension == ".yaml"
    assert relay_yaml.aliases == ("relay_yaml",)
    assert relay_yaml.binary_like is False

    pdf = catalog["pdf-export"]
    assert pdf.default_format == "pdf"
    assert pdf.extension == ".pdf"
    assert pdf.aliases == ("pdf_export",)
    assert pdf.binary_like is True


def test_existing_registry_lookup_behavior_remains_compatible():
    aliases = supported_target_aliases()

    assert aliases["relay_yaml"] == "relay-yaml"
    assert supported_target_names(include_aliases=True).count("relay_yaml") == 1
    assert get_exporter_registration("relay_yaml").target == "relay-yaml"
    assert create_exporter("relay_yaml").__class__ is create_exporter("relay-yaml").__class__
