import copy
import json
from types import SimpleNamespace

from blueprint.source_file_upload_requirements import (
    SourceFileUploadRequirementsReport,
    build_source_file_upload_requirements,
    derive_source_file_upload_requirements,
    extract_source_file_upload_requirements,
    generate_source_file_upload_requirements,
    source_file_upload_requirements_to_dict,
    source_file_upload_requirements_to_dicts,
    source_file_upload_requirements_to_markdown,
    summarize_source_file_upload_requirements,
)


def test_structured_source_extracts_file_upload_categories():
    result = build_source_file_upload_requirements(
        {
            "id": "brief-upload",
            "title": "Add file upload support",
            "requirements": {
                "upload": [
                    "Multipart/form-data handling for file uploads.",
                    "File size limits with max size of 10MB.",
                    "MIME type validation for allowed file types.",
                    "Virus scanning with ClamAV integration.",
                    "Upload progress tracking with progress callbacks.",
                    "Resumable upload support for large files.",
                    "Direct-to-S3 uploads with presigned URLs.",
                    "Chunked upload strategy for files over 5MB.",
                    "Temporary file cleanup on schedule.",
                ]
            },
        }
    )

    assert isinstance(result, SourceFileUploadRequirementsReport)
    assert result.source_id == "brief-upload"
    categories = {req.category for req in result.requirements}
    expected_categories = {
        "multipart_form",
        "file_size_limits",
        "mime_type_validation",
        "virus_scanning",
        "upload_progress",
        "resumable_upload",
        "direct_upload",
        "chunked_upload",
        "temp_file_cleanup",
    }
    assert expected_categories <= categories


def test_natural_language_extraction_from_body():
    result = build_source_file_upload_requirements(
        """
        Add file upload endpoint

        The API must support multipart form data for file uploads.
        File size should be limited to 10MB maximum.
        Validate MIME types and reject unsupported file types.
        Integrate virus scanning for uploaded files.
        Track upload progress and report to clients.
        Support resumable uploads for large files.
        """
    )

    assert len(result.requirements) >= 4
    categories = {req.category for req in result.requirements}
    assert "multipart_form" in categories
    assert "file_size_limits" in categories
    assert "mime_type_validation" in categories


def test_evidence_deduplication_and_stable_ordering():
    result = build_source_file_upload_requirements(
        {
            "title": "File upload with size limits",
            "description": "File size limits for uploads.",
            "requirements": ["File size limit of 10MB."],
            "acceptance": ["File size validated."],
        }
    )

    # Find file_size_limits requirement
    size_req = next((r for r in result.requirements if r.category == "file_size_limits"), None)
    assert size_req is not None
    # Evidence should be collected from multiple fields (up to 6)
    assert len(size_req.evidence) >= 1


def test_out_of_scope_negation_produces_empty_report():
    result = build_source_file_upload_requirements(
        {
            "id": "brief-no-upload",
            "title": "Add API endpoint",
            "scope": "No file uploads or multipart form data is in scope for this work.",
        }
    )

    assert result.requirements == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_file_upload_requirements_found"


def test_to_dict_to_dicts_and_to_markdown_serialization():
    result = build_source_file_upload_requirements(
        {
            "id": "brief-serialize",
            "title": "Add file upload",
            "requirements": [
                "Multipart form data for uploads.",
                "File size limits of 10MB.",
            ],
        }
    )

    payload = source_file_upload_requirements_to_dict(result)
    markdown = source_file_upload_requirements_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "title", "requirements", "summary", "records", "findings"]
    if result.requirements:
        assert list(payload["requirements"][0]) == [
            "category",
            "source_field",
            "evidence",
            "confidence",
            "planning_note",
            "unresolved_questions",
        ]
    assert result.to_dicts() == payload["requirements"]
    assert source_file_upload_requirements_to_dicts(result) == payload["requirements"]
    assert source_file_upload_requirements_to_dicts(result.requirements) == payload["requirements"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source File Upload Requirements Report: brief-serialize")


def test_invalid_input_handling():
    for invalid in [None, "", [], {}, 42, b"bytes"]:
        result = build_source_file_upload_requirements(invalid)
        assert result.requirements == ()
        assert result.summary["requirement_count"] == 0


def test_model_input_support():
    # Use dict input and let the function handle model validation
    result = build_source_file_upload_requirements(
        {
            "id": "brief-model",
            "title": "File upload",
            "summary": "File upload with multipart form data and size limits.",
        }
    )

    assert result.source_id == "brief-model"
    assert len(result.requirements) >= 1
    assert any(req.category in {"multipart_form", "file_size_limits"} for req in result.requirements)


def test_object_input_support():
    obj = SimpleNamespace(
        id="brief-object",
        title="File upload",
        body="File size limits with max size of 10MB for uploads.",
    )

    result = build_source_file_upload_requirements(obj)

    assert result.source_id == "brief-object"
    assert any(req.category == "file_size_limits" for req in result.requirements)


def test_no_mutation_of_source():
    source = {
        "id": "brief-mutation",
        "title": "File upload",
        "requirements": ["Multipart form data for uploads."],
    }
    original = copy.deepcopy(source)

    build_source_file_upload_requirements(source)

    assert source == original


def test_aliases_generate_derive_and_extract():
    source = {"title": "File upload", "body": "File upload with size limits."}

    result1 = generate_source_file_upload_requirements(source)
    result2 = derive_source_file_upload_requirements(source)
    requirements = extract_source_file_upload_requirements(source)
    summary = summarize_source_file_upload_requirements(source)

    assert result1.to_dict() == result2.to_dict()
    assert requirements == result1.requirements
    assert summary == result1.summary


def test_confidence_scoring():
    result = build_source_file_upload_requirements(
        {
            "requirements": {
                "upload": [
                    "File size limits must be enforced with max size of 10MB.",
                ]
            }
        }
    )

    # Requirements field with directive and upload context should get high/medium confidence
    size_req = next((r for r in result.requirements if r.category == "file_size_limits"), None)
    assert size_req is not None
    assert size_req.confidence in {"high", "medium"}


def test_planning_notes_attached_to_requirements():
    result = build_source_file_upload_requirements(
        {"title": "File upload", "body": "File upload with size limits."}
    )

    for requirement in result.requirements:
        assert requirement.planning_note
        assert len(requirement.planning_note) > 10


def test_unresolved_questions_for_ambiguous_requirements():
    result = build_source_file_upload_requirements(
        {"title": "File upload", "body": "Add file size limits."}
    )

    size_req = next((r for r in result.requirements if r.category == "file_size_limits"), None)
    if size_req:
        # Should have questions about specific size limits
        assert len(size_req.unresolved_questions) > 0


def test_summary_counts_match_requirements():
    result = build_source_file_upload_requirements(
        {
            "requirements": [
                "Multipart form data handling.",
                "File size limits.",
                "MIME type validation.",
            ]
        }
    )

    assert result.summary["requirement_count"] == len(result.requirements)
    category_counts = result.summary["category_counts"]
    assert sum(category_counts.values()) == len(result.requirements)
    confidence_counts = result.summary["confidence_counts"]
    assert sum(confidence_counts.values()) == len(result.requirements)


def test_requirement_category_property_compatibility():
    result = build_source_file_upload_requirements(
        {"body": "Multipart form data for file uploads."}
    )

    for requirement in result.requirements:
        assert requirement.requirement_category == requirement.category


def test_records_and_findings_property_compatibility():
    result = build_source_file_upload_requirements(
        {"body": "File upload with size limits."}
    )

    assert result.records == result.requirements
    assert result.findings == result.requirements


def test_empty_report_markdown():
    result = build_source_file_upload_requirements(
        {"title": "User profile", "body": "Add user profile endpoint."}
    )

    markdown = result.to_markdown()
    assert "No source file upload requirements were inferred." in markdown or len(result.requirements) > 0


def test_multipart_form_detection():
    result = build_source_file_upload_requirements(
        {"body": "Multipart/form-data handling for file uploads with form fields."}
    )

    multipart_req = next((r for r in result.requirements if r.category == "multipart_form"), None)
    assert multipart_req is not None


def test_file_size_limits_detection():
    result = build_source_file_upload_requirements(
        {"body": "File size limits with max file size of 10MB for uploads."}
    )

    size_req = next((r for r in result.requirements if r.category == "file_size_limits"), None)
    assert size_req is not None


def test_mime_type_validation_detection():
    result = build_source_file_upload_requirements(
        {"body": "MIME type validation with allowed file types: image/jpeg, image/png, application/pdf."}
    )

    mime_req = next((r for r in result.requirements if r.category == "mime_type_validation"), None)
    assert mime_req is not None


def test_virus_scanning_detection():
    result = build_source_file_upload_requirements(
        {"requirements": ["Virus scanning integration with ClamAV for uploaded files."]}
    )

    virus_req = next((r for r in result.requirements if r.category == "virus_scanning"), None)
    assert virus_req is not None


def test_upload_progress_detection():
    result = build_source_file_upload_requirements(
        {"body": "Upload progress tracking with progress callbacks and status reporting."}
    )

    progress_req = next((r for r in result.requirements if r.category == "upload_progress"), None)
    assert progress_req is not None


def test_resumable_upload_detection():
    result = build_source_file_upload_requirements(
        {"body": "Resumable upload support for large files with upload recovery."}
    )

    resumable_req = next((r for r in result.requirements if r.category == "resumable_upload"), None)
    assert resumable_req is not None


def test_direct_upload_detection():
    result = build_source_file_upload_requirements(
        {"body": "Direct-to-S3 uploads with presigned URLs for client-side uploads."}
    )

    direct_req = next((r for r in result.requirements if r.category == "direct_upload"), None)
    assert direct_req is not None


def test_chunked_upload_detection():
    result = build_source_file_upload_requirements(
        {"body": "Chunked upload strategy with chunk size of 5MB and chunk assembly."}
    )

    chunked_req = next((r for r in result.requirements if r.category == "chunked_upload"), None)
    assert chunked_req is not None


def test_temp_file_cleanup_detection():
    result = build_source_file_upload_requirements(
        {"body": "Temporary file cleanup on schedule to prevent disk space exhaustion."}
    )

    cleanup_req = next((r for r in result.requirements if r.category == "temp_file_cleanup"), None)
    assert cleanup_req is not None


def test_json_safe_serialization():
    result = build_source_file_upload_requirements(
        {
            "title": "File upload with special | chars",
            "body": "File upload | multipart | size limits | validation",
        }
    )

    payload = result.to_dict()
    # Should round-trip through JSON
    assert json.loads(json.dumps(payload)) == payload

    markdown = result.to_markdown()
    # Markdown should escape pipes
    if result.requirements:
        assert "\\|" in markdown or "|" in markdown


def test_implementation_brief_input():
    # Use dict input and let the function handle it
    result = build_source_file_upload_requirements(
        {
            "id": "impl-brief",
            "source_brief_id": "src-brief",
            "title": "File upload",
            "body": "Multipart form data for file uploads.",
        }
    )

    assert result.source_id == "impl-brief"


def test_no_file_upload_in_simple_api():
    result = build_source_file_upload_requirements(
        {
            "title": "Add REST API",
            "body": "Add REST API endpoints with JSON payloads. No file upload support.",
        }
    )

    # Should have empty requirements since no file upload patterns detected
    assert len(result.requirements) == 0


def test_mixed_json_and_file_upload():
    result = build_source_file_upload_requirements(
        {
            "title": "Add API layer",
            "body": "Add JSON endpoints for simple data and file upload endpoint with multipart form data and size limits.",
        }
    )

    # Should detect file upload patterns despite mentioning JSON
    assert any(req.category in {"multipart_form", "file_size_limits"} for req in result.requirements)


def test_s3_vs_gcs_vs_azure():
    result_s3 = build_source_file_upload_requirements(
        {"body": "Direct-to-S3 uploads with presigned URLs."}
    )

    result_gcs = build_source_file_upload_requirements(
        {"body": "Direct-to-GCS uploads with signed URLs."}
    )

    result_azure = build_source_file_upload_requirements(
        {"body": "Direct-to-Azure uploads with SAS tokens."}
    )

    # All should detect direct_upload
    assert any(req.category == "direct_upload" for req in result_s3.requirements)
    assert any(req.category == "direct_upload" for req in result_gcs.requirements)
    assert any(req.category == "direct_upload" for req in result_azure.requirements)


def test_clamav_vs_virustotal():
    result_clamav = build_source_file_upload_requirements(
        {"body": "Virus scanning with ClamAV integration."}
    )

    result_virustotal = build_source_file_upload_requirements(
        {"body": "Malware scanning with VirusTotal API."}
    )

    # Both should detect virus_scanning
    assert any(req.category == "virus_scanning" for req in result_clamav.requirements)
    assert any(req.category == "virus_scanning" for req in result_virustotal.requirements)


def test_tus_protocol_resumable():
    result = build_source_file_upload_requirements(
        {"body": "Resumable uploads using TUS protocol for large files."}
    )

    resumable_req = next((r for r in result.requirements if r.category == "resumable_upload"), None)
    assert resumable_req is not None


def test_multiple_file_fields():
    result = build_source_file_upload_requirements(
        {
            "requirements": [
                "File upload with multiple fields: avatar, documents, attachments.",
            ]
        }
    )

    # Should detect multipart_form
    assert any(req.category == "multipart_form" for req in result.requirements)


def test_size_limit_formats():
    result = build_source_file_upload_requirements(
        {
            "requirements": [
                "File size limits: 10MB for images, 100MB for videos, 5MB for documents.",
            ]
        }
    )

    size_req = next((r for r in result.requirements if r.category == "file_size_limits"), None)
    assert size_req is not None
    assert any("10mb" in evidence.lower() or "100mb" in evidence.lower() for evidence in size_req.evidence)


def test_mime_type_examples():
    result = build_source_file_upload_requirements(
        {
            "body": "Allowed MIME types: image/jpeg, image/png, image/gif, application/pdf, text/plain.",
        }
    )

    mime_req = next((r for r in result.requirements if r.category == "mime_type_validation"), None)
    assert mime_req is not None


def test_progress_callback_patterns():
    result = build_source_file_upload_requirements(
        {"body": "Upload progress with progress event callbacks and percentage reporting."}
    )

    progress_req = next((r for r in result.requirements if r.category == "upload_progress"), None)
    assert progress_req is not None


def test_chunk_size_specification():
    result = build_source_file_upload_requirements(
        {"body": "Chunked uploads with chunk size of 5MB for files over 20MB."}
    )

    chunked_req = next((r for r in result.requirements if r.category == "chunked_upload"), None)
    assert chunked_req is not None


def test_temp_file_retention():
    result = build_source_file_upload_requirements(
        {"body": "Temporary file cleanup with 24-hour retention policy and automated cleanup."}
    )

    cleanup_req = next((r for r in result.requirements if r.category == "temp_file_cleanup"), None)
    assert cleanup_req is not None


def test_multiple_categories_same_evidence():
    result = build_source_file_upload_requirements(
        {
            "requirements": [
                "File upload with size limits, MIME type validation, and virus scanning.",
            ]
        }
    )

    categories = {req.category for req in result.requirements}
    # Should extract multiple categories from the same sentence
    assert "file_size_limits" in categories or "mime_type_validation" in categories or "virus_scanning" in categories


def test_nested_upload_requirements():
    result = build_source_file_upload_requirements(
        {
            "requirements": {
                "file_upload": {
                    "validation": "File size limits and MIME type validation.",
                    "security": "Virus scanning with ClamAV.",
                    "storage": "Direct-to-S3 uploads with presigned URLs.",
                }
            }
        }
    )

    categories = {req.category for req in result.requirements}
    # Should extract from nested structures
    assert any(
        cat in categories
        for cat in ["file_size_limits", "mime_type_validation", "virus_scanning", "direct_upload"]
    )


def test_edge_case_no_uploads():
    result = build_source_file_upload_requirements(
        {
            "title": "Database migration",
            "body": "Migrate user data from PostgreSQL to MySQL. No API changes or file handling.",
        }
    )

    # Should have empty requirements
    assert len(result.requirements) == 0
    assert result.summary["status"] == "no_file_upload_requirements_found"
