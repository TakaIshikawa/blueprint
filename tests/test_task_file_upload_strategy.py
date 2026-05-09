"""Tests for file upload strategy analyzer."""

import pytest

from blueprint.task_file_upload_strategy import (
    FileUploadStrategy,
    analyze_file_upload_strategy,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_file_upload_strategy({})

    assert isinstance(result, FileUploadStrategy)
    assert result.file_size_limits_defined is False
    assert result.supported_formats_specified is False
    assert result.chunking_strategy_configured is False
    assert result.resumable_upload_enabled is False
    assert result.virus_scanning_implemented is False
    assert result.storage_backend_specified is False
    assert result.cdn_integration_planned is False
    assert result.presigned_urls_used is False
    assert result.metadata_extraction_configured is False
    assert result.thumbnail_generation_planned is False
    assert result.readiness_score == 0.0


def test_file_size_limit_detected():
    """Detect file size limit configuration in task data."""
    task = {
        "title": "Configure file upload",
        "description": "Set file size limit to 10MB for uploads",
    }

    result = analyze_file_upload_strategy(task)

    assert result.file_size_limits_defined is True
    assert result.supported_formats_specified is False
    assert result.readiness_score == 0.1


def test_supported_formats_detected():
    """Detect supported file formats in task data."""
    task = {
        "description": "Support image file formats including JPG, PNG, and supported formats validation",
        "acceptance_criteria": ["File type whitelist configured", "Validate file format"],
    }

    result = analyze_file_upload_strategy(task)

    assert result.supported_formats_specified is True
    assert result.file_size_limits_defined is False


def test_chunking_strategy_detected():
    """Detect chunking strategy in task data."""
    task = {
        "description": "Implement chunked upload with multipart upload strategy",
        "acceptance_criteria": ["Chunk size configured", "Upload in chunks"],
    }

    result = analyze_file_upload_strategy(task)

    assert result.chunking_strategy_configured is True
    assert result.resumable_upload_enabled is False


def test_resumable_upload_detected():
    """Detect resumable upload in task data."""
    task = {
        "description": "Enable resumable uploads with pause and resume capability",
        "acceptance_criteria": ["Support resumable upload", "Resume upload on failure"],
    }

    result = analyze_file_upload_strategy(task)

    assert result.resumable_upload_enabled is True


def test_virus_scanning_detected():
    """Detect virus scanning in task data."""
    task = {
        "description": "Implement virus scanning for uploaded files with malware detection",
        "acceptance_criteria": ["Scan for viruses", "Check malware"],
    }

    result = analyze_file_upload_strategy(task)

    assert result.virus_scanning_implemented is True


def test_storage_backend_detected():
    """Detect storage backend specification in task data."""
    task = {
        "description": "Upload to S3 bucket with object storage backend",
        "acceptance_criteria": ["Configure S3 storage", "Set up storage backend"],
    }

    result = analyze_file_upload_strategy(task)

    assert result.storage_backend_specified is True


def test_cdn_integration_detected():
    """Detect CDN integration in task data."""
    task = {
        "description": "Integrate CloudFront CDN for file delivery with content delivery network",
        "acceptance_criteria": ["Configure CDN integration", "Set up edge caching"],
    }

    result = analyze_file_upload_strategy(task)

    assert result.cdn_integration_planned is True


def test_presigned_url_detected():
    """Detect presigned URL usage in task data."""
    task = {
        "description": "Generate presigned URLs for direct upload with temporary upload URLs",
        "acceptance_criteria": ["Use presigned URLs", "Generate upload URLs"],
    }

    result = analyze_file_upload_strategy(task)

    assert result.presigned_urls_used is True


def test_metadata_extraction_detected():
    """Detect metadata extraction in task data."""
    task = {
        "description": "Extract file metadata including EXIF data from images",
        "acceptance_criteria": ["Extract metadata", "Parse image metadata"],
    }

    result = analyze_file_upload_strategy(task)

    assert result.metadata_extraction_configured is True


def test_thumbnail_generation_detected():
    """Detect thumbnail generation in task data."""
    task = {
        "description": "Generate thumbnails for uploaded images with preview generation",
        "acceptance_criteria": ["Create thumbnails", "Resize images"],
    }

    result = analyze_file_upload_strategy(task)

    assert result.thumbnail_generation_planned is True


def test_comprehensive_file_upload_all_detected():
    """Test comprehensive file upload with all aspects present."""
    task = {
        "title": "Complete file upload implementation",
        "description": (
            "Implement file upload with file size limit of 50MB and supported formats validation. "
            "Enable chunked upload with multipart upload for large files. "
            "Support resumable uploads with pause and resume capability. "
            "Implement virus scanning with malware detection. "
            "Upload to S3 storage backend with CloudFront CDN integration. "
            "Generate presigned URLs for direct client upload. "
            "Extract file metadata including EXIF data. "
            "Generate thumbnails for image preview."
        ),
        "acceptance_criteria": [
            "File size limits configured",
            "Supported file formats defined",
            "Chunking strategy implemented",
            "Resumable upload enabled",
            "Virus scanning active",
            "S3 storage backend configured",
            "CDN integration complete",
            "Presigned URLs generated",
            "Metadata extraction working",
            "Thumbnail generation implemented",
        ],
    }

    result = analyze_file_upload_strategy(task)

    assert result.file_size_limits_defined is True
    assert result.supported_formats_specified is True
    assert result.chunking_strategy_configured is True
    assert result.resumable_upload_enabled is True
    assert result.virus_scanning_implemented is True
    assert result.storage_backend_specified is True
    assert result.cdn_integration_planned is True
    assert result.presigned_urls_used is True
    assert result.metadata_extraction_configured is True
    assert result.thumbnail_generation_planned is True
    assert result.readiness_score == 1.0


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_file_upload_strategy(None)  # type: ignore

    assert isinstance(result, FileUploadStrategy)
    assert result.file_size_limits_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_file_upload_strategy([{"key": "value"}])  # type: ignore

    assert isinstance(result, FileUploadStrategy)
    assert result.file_size_limits_defined is False
    assert result.readiness_score == 0.0


def test_partial_file_upload_readiness():
    """Test partial file upload readiness with some aspects covered."""
    task = {
        "title": "Basic file upload",
        "description": "Upload files to storage",
        "acceptance_criteria": [
            "Set maximum file size to 10MB",
            "Support JPG and PNG formats",
        ],
    }

    result = analyze_file_upload_strategy(task)

    assert result.file_size_limits_defined is True
    assert result.supported_formats_specified is True
    assert result.chunking_strategy_configured is False
    assert result.resumable_upload_enabled is False
    assert result.virus_scanning_implemented is False
    assert result.storage_backend_specified is False
    assert result.cdn_integration_planned is False
    assert result.presigned_urls_used is False
    assert result.metadata_extraction_configured is False
    assert result.thumbnail_generation_planned is False
    assert result.readiness_score == 0.2


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "FILE SIZE LIMIT with CHUNKING STRATEGY and VIRUS SCAN",
        "acceptance_criteria": ["RESUMABLE UPLOAD enabled", "S3 STORAGE backend"],
    }

    result = analyze_file_upload_strategy(task)

    assert result.file_size_limits_defined is True
    assert result.chunking_strategy_configured is True
    assert result.virus_scanning_implemented is True
    assert result.resumable_upload_enabled is True
    assert result.storage_backend_specified is True


def test_large_file_upload_edge_case():
    """Test large file upload detection."""
    task = {
        "description": "Upload large files with chunked upload and resumable upload support",
        "acceptance_criteria": [
            "Chunking strategy for large files",
            "File size limit for uploads",
        ],
    }

    result = analyze_file_upload_strategy(task)

    assert result.file_size_limits_defined is True
    assert result.chunking_strategy_configured is True
    assert result.resumable_upload_enabled is True


def test_concurrent_upload_edge_case():
    """Test concurrent upload detection."""
    task = {
        "description": "Support concurrent uploads with multipart upload strategy",
        "acceptance_criteria": [
            "Chunking for parallel upload",
            "Storage backend configured",
        ],
    }

    result = analyze_file_upload_strategy(task)

    assert result.chunking_strategy_configured is True
    assert result.storage_backend_specified is True


def test_direct_to_s3_upload_edge_case():
    """Test direct-to-S3 upload detection."""
    task = {
        "description": "Direct upload to S3 using presigned URLs with client-side upload",
        "acceptance_criteria": [
            "Generate presigned URLs for S3",
            "Upload directly to S3 bucket",
        ],
    }

    result = analyze_file_upload_strategy(task)

    assert result.presigned_urls_used is True
    assert result.storage_backend_specified is True


def test_readiness_score_calculation():
    """Test readiness score calculation with different combinations."""
    task1 = {"description": "Generic task"}
    result1 = analyze_file_upload_strategy(task1)
    assert result1.readiness_score == 0.0

    task2 = {"description": "Set file size limit"}
    result2 = analyze_file_upload_strategy(task2)
    assert result2.readiness_score == 0.1

    task3 = {
        "description": "File size limit, supported formats, chunked upload, resumable upload, and virus scanning"
    }
    result3 = analyze_file_upload_strategy(task3)
    assert result3.readiness_score == 0.5


def test_to_dict_method():
    """Test FileUploadStrategy.to_dict() serialization."""
    strategy = FileUploadStrategy(
        file_size_limits_defined=True,
        supported_formats_specified=True,
        chunking_strategy_configured=False,
        resumable_upload_enabled=True,
        virus_scanning_implemented=False,
        storage_backend_specified=True,
        cdn_integration_planned=False,
        presigned_urls_used=True,
        metadata_extraction_configured=False,
        thumbnail_generation_planned=True,
    )

    result = strategy.to_dict()

    assert isinstance(result, dict)
    assert result["file_size_limits_defined"] is True
    assert result["supported_formats_specified"] is True
    assert result["chunking_strategy_configured"] is False
    assert result["resumable_upload_enabled"] is True
    assert result["virus_scanning_implemented"] is False
    assert result["storage_backend_specified"] is True
    assert result["cdn_integration_planned"] is False
    assert result["presigned_urls_used"] is True
    assert result["metadata_extraction_configured"] is False
    assert result["thumbnail_generation_planned"] is True
    assert result["readiness_score"] == 0.6


def test_dataclass_immutability():
    """Test that FileUploadStrategy is frozen/immutable."""
    strategy = FileUploadStrategy(file_size_limits_defined=True)

    with pytest.raises(AttributeError):
        strategy.file_size_limits_defined = False  # type: ignore


def test_alternative_terminology_size_maximum():
    """Test maximum file size terminology is recognized."""
    task = {
        "description": "Set maximum file size for upload",
    }

    result = analyze_file_upload_strategy(task)

    assert result.file_size_limits_defined is True


def test_alternative_terminology_formats_mime_type():
    """Test MIME type terminology is recognized."""
    task = {
        "description": "Validate MIME types for uploaded files",
    }

    result = analyze_file_upload_strategy(task)

    assert result.supported_formats_specified is True


def test_alternative_terminology_chunking_multipart():
    """Test multipart upload terminology is recognized."""
    task = {
        "description": "Use multipart upload for large files",
    }

    result = analyze_file_upload_strategy(task)

    assert result.chunking_strategy_configured is True


def test_alternative_terminology_resumable_continue():
    """Test continue upload terminology is recognized."""
    task = {
        "description": "Continue upload after interruption",
    }

    result = analyze_file_upload_strategy(task)

    assert result.resumable_upload_enabled is True


def test_alternative_terminology_virus_malware():
    """Test malware scan terminology is recognized."""
    task = {
        "description": "Scan for malware in uploaded files",
    }

    result = analyze_file_upload_strategy(task)

    assert result.virus_scanning_implemented is True


def test_alternative_terminology_storage_gcs():
    """Test GCS storage terminology is recognized."""
    task = {
        "description": "Upload to GCS bucket for storage",
    }

    result = analyze_file_upload_strategy(task)

    assert result.storage_backend_specified is True


def test_alternative_terminology_cdn_cloudfront():
    """Test CloudFront terminology is recognized."""
    task = {
        "description": "Use CloudFront for file delivery",
    }

    result = analyze_file_upload_strategy(task)

    assert result.cdn_integration_planned is True


def test_alternative_terminology_presigned_signed():
    """Test signed URL terminology is recognized."""
    task = {
        "description": "Generate signed URLs for upload",
    }

    result = analyze_file_upload_strategy(task)

    assert result.presigned_urls_used is True


def test_alternative_terminology_metadata_exif():
    """Test EXIF extraction terminology is recognized."""
    task = {
        "description": "Extract EXIF data from images",
    }

    result = analyze_file_upload_strategy(task)

    assert result.metadata_extraction_configured is True


def test_alternative_terminology_thumbnail_preview():
    """Test preview generation terminology is recognized."""
    task = {
        "description": "Generate preview images for uploads",
    }

    result = analyze_file_upload_strategy(task)

    assert result.thumbnail_generation_planned is True


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_file_upload_strategy(task)

    assert result.file_size_limits_defined is False
    assert result.readiness_score == 0.0


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "File upload system",
        "description": "Configure file size limit",
        "acceptance_criteria": ["Support multiple file formats"],
        "requirements": ["Implement chunked upload"],
        "notes": ["Enable virus scanning"],
    }

    result = analyze_file_upload_strategy(task)

    assert result.file_size_limits_defined is True
    assert result.supported_formats_specified is True
    assert result.chunking_strategy_configured is True
    assert result.virus_scanning_implemented is True
