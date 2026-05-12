import pytest

from blueprint.export import (
    DataExportFormat,
    ExportCompression,
    ExportCompressionOption,
    get_export_compression_options,
    normalize_export_compression,
)


@pytest.mark.parametrize(
    ("export_format", "expected"),
    [
        ("json", ["none", "gzip", "zip"]),
        (DataExportFormat.JSONL, ["none", "gzip", "zip"]),
        ("csv", ["none", "gzip", "zip"]),
        ("sql", ["none", "gzip", "zip"]),
        ("parquet", ["none", "gzip"]),
        ("protobuf", ["none", "gzip"]),
    ],
)
def test_get_export_compression_options_returns_supported_modes(export_format, expected):
    options = get_export_compression_options(export_format)

    assert all(isinstance(option, ExportCompressionOption) for option in options)
    assert [option.compression for option in options] == expected
    assert [option.format for option in options] == [
        export_format.value if isinstance(export_format, DataExportFormat) else export_format
    ] * len(expected)


def test_get_export_compression_options_returns_independent_descriptors():
    options = get_export_compression_options(DataExportFormat.JSON)
    changed = options[0].model_copy(update={"mime_type": "changed"})

    assert changed.mime_type == "changed"
    assert get_export_compression_options(DataExportFormat.JSON)[0].mime_type == "application/json"


@pytest.mark.parametrize("requested", [None, "", " none ", ExportCompression.NONE])
def test_normalize_export_compression_defaults_to_none(requested):
    assert normalize_export_compression(DataExportFormat.CSV, requested) is ExportCompression.NONE


@pytest.mark.parametrize(
    ("export_format", "requested", "expected"),
    [
        (DataExportFormat.JSON, "GZIP", ExportCompression.GZIP),
        ("jsonl", " zip ", ExportCompression.ZIP),
        ("parquet", ExportCompression.GZIP, ExportCompression.GZIP),
        ("protobuf", "gzip", ExportCompression.GZIP),
    ],
)
def test_normalize_export_compression_accepts_supported_modes(
    export_format,
    requested,
    expected,
):
    assert normalize_export_compression(export_format, requested) is expected


@pytest.mark.parametrize("export_format", [DataExportFormat.PARQUET, "protobuf"])
def test_normalize_export_compression_rejects_unsupported_combinations(export_format):
    with pytest.raises(ValueError, match="compression 'zip' is not supported"):
        normalize_export_compression(export_format, "zip")


def test_normalize_export_compression_rejects_unknown_mode():
    with pytest.raises(ValueError, match="'brotli' is not a valid ExportCompression"):
        normalize_export_compression(DataExportFormat.JSON, "brotli")
