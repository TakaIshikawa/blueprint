from blueprint.export.data_exporter import (
    DataExportFormat,
    ExportFormatCapability,
    get_export_format_capability,
    list_export_format_capabilities,
)


def test_get_export_format_capability_returns_structured_descriptor():
    descriptor = get_export_format_capability(DataExportFormat.JSONL)

    assert isinstance(descriptor, ExportFormatCapability)
    assert descriptor.format == "jsonl"
    assert descriptor.mime_type == "application/x-ndjson"
    assert descriptor.file_extension == ".jsonl"
    assert descriptor.line_oriented is True
    assert descriptor.binary is False
    assert descriptor.supports_manifest_record_count_validation is True


def test_list_export_format_capabilities_covers_all_formats_in_enum_order():
    descriptors = list_export_format_capabilities()

    assert [descriptor.format for descriptor in descriptors] == [
        "json",
        "jsonl",
        "csv",
        "sql",
        "parquet",
        "protobuf",
    ]
    assert len(descriptors) == len(DataExportFormat)


def test_export_format_capabilities_describe_binary_and_record_count_support():
    by_format = {
        descriptor.format: descriptor
        for descriptor in list_export_format_capabilities()
    }

    assert by_format["json"].mime_type == "application/json"
    assert by_format["csv"].line_oriented is True
    assert by_format["csv"].supports_manifest_record_count_validation is False
    assert by_format["sql"].file_extension == ".sql"
    assert by_format["parquet"].binary is True
    assert by_format["protobuf"].binary is True

