"""Tests for batch processing strategy analyzer."""

import pytest

from blueprint.task_batch_processing_strategy import (
    BatchProcessingStrategy,
    analyze_batch_processing_strategy,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_batch_processing_strategy({})

    assert isinstance(result, BatchProcessingStrategy)
    assert result.batch_size_defined is False
    assert result.parallelism_configured is False
    assert result.checkpointing_enabled is False
    assert result.partial_failure_handled is False
    assert result.progress_tracked is False
    assert result.memory_constraints_managed is False
    assert result.timeout_configured is False
    assert result.retry_logic_implemented is False
    assert result.idempotency_ensured is False
    assert result.result_aggregation_planned is False
    assert result.readiness_score == 0.0


def test_batch_size_detected():
    """Detect batch size configuration in task data."""
    task = {
        "title": "Configure batch processing",
        "description": "Set batch size to 1000 records for bulk processing",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True
    assert result.parallelism_configured is False
    assert result.readiness_score == 0.1


def test_parallelism_detected():
    """Detect parallelism configuration in task data."""
    task = {
        "description": "Process with parallel execution using worker pool of 10 threads",
        "acceptance_criteria": ["Configure concurrent processing", "Set parallelism level"],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.parallelism_configured is True
    assert result.batch_size_defined is False


def test_checkpointing_detected():
    """Detect checkpointing strategy in task data."""
    task = {
        "description": "Implement checkpoint mechanism to save progress and resume from checkpoint",
        "acceptance_criteria": ["Progress tracking enabled", "Restart from last checkpoint"],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.checkpointing_enabled is True
    assert result.progress_tracked is True


def test_partial_failure_detected():
    """Detect partial failure handling in task data."""
    task = {
        "description": "Handle partial failures with error isolation and dead letter queue",
        "acceptance_criteria": ["Skip failed items", "Continue on failure"],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.partial_failure_handled is True
    assert result.batch_size_defined is False


def test_progress_tracking_detected():
    """Detect progress tracking in task data."""
    task = {
        "description": "Track batch progress with completion percentage and progress bar",
        "acceptance_criteria": ["Monitor batch status", "Report processed count"],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.progress_tracked is True
    assert result.batch_size_defined is False


def test_memory_constraints_detected():
    """Detect memory constraint management in task data."""
    task = {
        "description": "Manage memory usage with streaming processing to avoid memory exhaustion",
        "acceptance_criteria": ["Optimize memory footprint", "Limit heap size"],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.memory_constraints_managed is True


def test_timeout_detected():
    """Detect timeout configuration in task data."""
    task = {
        "description": "Configure batch timeout for long-running operations",
        "acceptance_criteria": ["Set processing timeout", "Handle timeout gracefully"],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.timeout_configured is True


def test_retry_logic_detected():
    """Detect retry logic in task data."""
    task = {
        "description": "Implement retry strategy with exponential backoff for failed batches",
        "acceptance_criteria": ["Configure retry policy", "Set maximum retry attempts"],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.retry_logic_implemented is True


def test_idempotency_detected():
    """Detect idempotency in task data."""
    task = {
        "description": "Ensure idempotent operations with idempotency key and duplicate detection",
        "acceptance_criteria": ["Implement idempotency token", "Safe to retry"],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.idempotency_ensured is True


def test_result_aggregation_detected():
    """Detect result aggregation in task data."""
    task = {
        "description": "Aggregate batch results and consolidate outcomes",
        "acceptance_criteria": ["Collect batch results", "Merge processing results"],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.result_aggregation_planned is True


def test_comprehensive_batch_processing_all_detected():
    """Test comprehensive batch processing with all aspects present."""
    task = {
        "title": "Complete batch processing implementation",
        "description": (
            "Implement batch processing with chunk size of 500 and parallel execution using 10 workers. "
            "Enable checkpointing to save progress and resume from last state. "
            "Handle partial failures with error isolation and dead letter queue. "
            "Track batch progress with completion status monitoring. "
            "Manage memory constraints with streaming processing to avoid memory leaks. "
            "Configure batch timeout for long-running tasks. "
            "Implement retry logic with exponential backoff and max retry limit. "
            "Ensure idempotent processing with duplicate prevention. "
            "Aggregate batch results for final consolidation."
        ),
        "acceptance_criteria": [
            "Batch size configured",
            "Concurrent processing enabled",
            "Checkpoint strategy implemented",
            "Partial failure handling verified",
            "Progress tracking active",
            "Memory optimization applied",
            "Timeout policy set",
            "Retry mechanism configured",
            "Idempotency ensured",
            "Result aggregation complete",
        ],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True
    assert result.parallelism_configured is True
    assert result.checkpointing_enabled is True
    assert result.partial_failure_handled is True
    assert result.progress_tracked is True
    assert result.memory_constraints_managed is True
    assert result.timeout_configured is True
    assert result.retry_logic_implemented is True
    assert result.idempotency_ensured is True
    assert result.result_aggregation_planned is True
    assert result.readiness_score == 1.0


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_batch_processing_strategy(None)  # type: ignore

    assert isinstance(result, BatchProcessingStrategy)
    assert result.batch_size_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_batch_processing_strategy([{"key": "value"}])  # type: ignore

    assert isinstance(result, BatchProcessingStrategy)
    assert result.batch_size_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_string():
    """Test with string input instead of mapping."""
    result = analyze_batch_processing_strategy("not a mapping")  # type: ignore

    assert isinstance(result, BatchProcessingStrategy)
    assert result.batch_size_defined is False


def test_invalid_task_data_tuple():
    """Test with tuple input instead of mapping."""
    result = analyze_batch_processing_strategy(("tuple", "data"))  # type: ignore

    assert isinstance(result, BatchProcessingStrategy)
    assert result.batch_size_defined is False


def test_partial_data_missing_fields():
    """Test with partial task data missing some fields."""
    task = {
        "title": "Batch processing",
        # Missing description, acceptance_criteria, etc.
    }

    result = analyze_batch_processing_strategy(task)

    assert isinstance(result, BatchProcessingStrategy)
    assert result.readiness_score == 0.0


def test_partial_batch_processing_readiness():
    """Test partial batch processing readiness with some aspects covered."""
    task = {
        "title": "Basic batch processing",
        "description": "Process data in batches",
        "acceptance_criteria": [
            "Define batch size of 100",
            "Configure parallel workers",
        ],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True
    assert result.parallelism_configured is True
    assert result.checkpointing_enabled is False
    assert result.partial_failure_handled is False
    assert result.progress_tracked is False
    assert result.memory_constraints_managed is False
    assert result.timeout_configured is False
    assert result.retry_logic_implemented is False
    assert result.idempotency_ensured is False
    assert result.result_aggregation_planned is False
    assert result.readiness_score == 0.2


def test_task_data_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    task = {
        "title": "Batch improvements",
        "acceptance_criteria": [
            "Set batch size for processing",
            "Enable checkpointing for progress",
            "Implement retry mechanism",
        ],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True
    assert result.checkpointing_enabled is True
    assert result.retry_logic_implemented is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    task = {
        "title": "Batch setup",
        "validation_command": "pytest tests/test_batch_size.py tests/test_checkpointing.py",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True
    assert result.checkpointing_enabled is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "BATCH SIZE with PARALLEL PROCESSING and RETRY LOGIC",
        "acceptance_criteria": ["CHECKPOINTING enabled", "TRACK progress"],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True
    assert result.parallelism_configured is True
    assert result.retry_logic_implemented is True
    assert result.checkpointing_enabled is True
    assert result.progress_tracked is True


def test_alternative_terminology_batch_chunk():
    """Test chunk size terminology is recognized as batch size."""
    task = {
        "description": "Process in chunks of 200 records",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True


def test_alternative_terminology_batch_page():
    """Test page size terminology is recognized as batch size."""
    task = {
        "description": "Configure page size for bulk operations",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True


def test_alternative_terminology_parallelism_concurrent():
    """Test concurrent processing terminology is recognized."""
    task = {
        "description": "Enable concurrent batch execution",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.parallelism_configured is True


def test_alternative_terminology_parallelism_async():
    """Test async processing terminology is recognized."""
    task = {
        "description": "Use asynchronous batch processing",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.parallelism_configured is True


def test_alternative_terminology_checkpoint_save_state():
    """Test save state terminology is recognized as checkpointing."""
    task = {
        "description": "Save batch state for resumability",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.checkpointing_enabled is True


def test_alternative_terminology_checkpoint_incremental():
    """Test incremental processing terminology is recognized."""
    task = {
        "description": "Use incremental processing with progress persistence",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.checkpointing_enabled is True


def test_alternative_terminology_failure_dlq():
    """Test dead letter queue terminology is recognized."""
    task = {
        "description": "Route failed items to DLQ",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.partial_failure_handled is True


def test_alternative_terminology_failure_fail_fast():
    """Test fail fast terminology is recognized."""
    task = {
        "description": "Implement fail fast strategy for batch errors",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.partial_failure_handled is True


def test_alternative_terminology_progress_completion():
    """Test completion tracking terminology is recognized."""
    task = {
        "description": "Monitor completion percentage for batches",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.progress_tracked is True


def test_alternative_terminology_progress_status():
    """Test batch status terminology is recognized."""
    task = {
        "description": "Track batch status and processed items",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.progress_tracked is True


def test_alternative_terminology_memory_streaming():
    """Test streaming processing terminology is recognized."""
    task = {
        "description": "Use streaming batch processing",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.memory_constraints_managed is True


def test_alternative_terminology_memory_buffer():
    """Test buffer management terminology is recognized."""
    task = {
        "description": "Manage buffer size for batch operations",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.memory_constraints_managed is True


def test_alternative_terminology_timeout_long_running():
    """Test long-running operation terminology is recognized."""
    task = {
        "description": "Handle long-running batch operations",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.timeout_configured is True


def test_alternative_terminology_retry_backoff():
    """Test backoff strategy terminology is recognized."""
    task = {
        "description": "Implement exponential backoff for retries",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.retry_logic_implemented is True


def test_alternative_terminology_retry_max_attempts():
    """Test max attempts terminology is recognized."""
    task = {
        "description": "Set maximum retry count for batch processing",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.retry_logic_implemented is True


def test_alternative_terminology_idempotency_duplicate_detection():
    """Test duplicate detection terminology is recognized."""
    task = {
        "description": "Implement duplicate detection for batch items",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.idempotency_ensured is True


def test_alternative_terminology_idempotency_at_least_once():
    """Test at-least-once delivery terminology is recognized."""
    task = {
        "description": "Support at-least-once processing semantics",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.idempotency_ensured is True


def test_alternative_terminology_aggregation_consolidate():
    """Test consolidate results terminology is recognized."""
    task = {
        "description": "Consolidate batch outcomes",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.result_aggregation_planned is True


def test_alternative_terminology_aggregation_merge():
    """Test merge results terminology is recognized."""
    task = {
        "description": "Merge batch results into final output",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.result_aggregation_planned is True


def test_to_dict_method():
    """Test BatchProcessingStrategy.to_dict() serialization."""
    strategy = BatchProcessingStrategy(
        batch_size_defined=True,
        parallelism_configured=True,
        checkpointing_enabled=False,
        partial_failure_handled=True,
        progress_tracked=False,
        memory_constraints_managed=True,
        timeout_configured=False,
        retry_logic_implemented=True,
        idempotency_ensured=False,
        result_aggregation_planned=True,
    )

    result = strategy.to_dict()

    assert isinstance(result, dict)
    assert result["batch_size_defined"] is True
    assert result["parallelism_configured"] is True
    assert result["checkpointing_enabled"] is False
    assert result["partial_failure_handled"] is True
    assert result["progress_tracked"] is False
    assert result["memory_constraints_managed"] is True
    assert result["timeout_configured"] is False
    assert result["retry_logic_implemented"] is True
    assert result["idempotency_ensured"] is False
    assert result["result_aggregation_planned"] is True
    assert result["readiness_score"] == 0.6


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "Batch processing",
        "description": "Configure batch size",
        "acceptance_criteria": ["Enable checkpointing"],
        "requirements": ["Implement retry logic"],
        "notes": ["Track progress"],
        "risks": ["No partial failure handling"],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True
    assert result.checkpointing_enabled is True
    assert result.retry_logic_implemented is True
    assert result.progress_tracked is True
    assert result.partial_failure_handled is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    task = {
        "validation_commands": [
            "test_batch_size.py",
            "test_idempotency.py",
        ],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True
    assert result.idempotency_ensured is True


def test_dataclass_immutability():
    """Test that BatchProcessingStrategy is frozen/immutable."""
    strategy = BatchProcessingStrategy(batch_size_defined=True)

    with pytest.raises(AttributeError):
        strategy.batch_size_defined = False  # type: ignore


def test_large_scale_batch_edge_case():
    """Test large-scale batch processing detection."""
    task = {
        "description": "Process millions of records in batches with parallel workers",
        "acceptance_criteria": [
            "Batch size optimized for throughput",
            "Memory constraints managed",
        ],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True
    assert result.parallelism_configured is True
    assert result.memory_constraints_managed is True


def test_distributed_batch_processing_edge_case():
    """Test distributed batch processing detection."""
    task = {
        "description": "Distribute batch processing across multiple nodes with parallel execution",
        "acceptance_criteria": [
            "Configure worker pool for distributed processing",
            "Implement result aggregation across nodes",
        ],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.parallelism_configured is True
    assert result.result_aggregation_planned is True


def test_batch_dependencies_edge_case():
    """Test batch with dependencies detection."""
    task = {
        "description": "Process dependent batches with checkpointing and retry on failure",
        "acceptance_criteria": [
            "Track progress across batch dependencies",
            "Handle partial failures in dependent batches",
        ],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.checkpointing_enabled is True
    assert result.retry_logic_implemented is True
    assert result.progress_tracked is True
    assert result.partial_failure_handled is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Define batch size and implement checkpointing",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True
    assert result.checkpointing_enabled is True


def test_readiness_score_calculation():
    """Test readiness score calculation with different combinations."""
    # 0/10 = 0.0
    task1 = {"description": "Generic task"}
    result1 = analyze_batch_processing_strategy(task1)
    assert result1.readiness_score == 0.0

    # 1/10 = 0.1
    task2 = {"description": "Set batch size"}
    result2 = analyze_batch_processing_strategy(task2)
    assert result2.readiness_score == 0.1

    # 5/10 = 0.5
    task3 = {
        "description": "Batch size, parallelism, checkpointing, retry logic, and track progress"
    }
    result3 = analyze_batch_processing_strategy(task3)
    assert result3.readiness_score == 0.5

    # 10/10 = 1.0
    task4 = {
        "description": (
            "Batch size, parallelism, checkpointing, handle partial failures, "
            "progress tracking, memory constraints, batch timeout, retry logic, "
            "idempotency, and result aggregation"
        )
    }
    result4 = analyze_batch_processing_strategy(task4)
    assert result4.readiness_score == 1.0


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is False
    assert result.readiness_score == 0.0


def test_worker_pool_pattern():
    """Test worker pool pattern detection."""
    task = {
        "description": "Configure worker pool with 20 threads for batch processing",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.parallelism_configured is True


def test_multi_threaded_pattern():
    """Test multi-threaded pattern detection."""
    task = {
        "description": "Use multi-threaded batch processing",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.parallelism_configured is True


def test_bulk_operation_pattern():
    """Test bulk operation pattern detection."""
    task = {
        "description": "Implement bulk processing with defined batch size",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.batch_size_defined is True


def test_progress_bar_pattern():
    """Test progress bar pattern detection."""
    task = {
        "description": "Display progress bar for batch operations",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.progress_tracked is True


def test_restart_from_checkpoint_pattern():
    """Test restart from checkpoint pattern detection."""
    task = {
        "description": "Enable restart from last checkpoint on failure",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.checkpointing_enabled is True


def test_continue_on_error_pattern():
    """Test continue on error pattern detection."""
    task = {
        "description": "Continue processing batch on error",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.partial_failure_handled is True


def test_avoid_memory_leak_pattern():
    """Test avoid memory leak pattern detection."""
    task = {
        "description": "Avoid memory leaks in batch processing",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.memory_constraints_managed is True


def test_execution_timeout_pattern():
    """Test execution timeout pattern detection."""
    task = {
        "description": "Set execution timeout for batch operations",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.timeout_configured is True


def test_retry_failed_batch_pattern():
    """Test retry failed batch pattern detection."""
    task = {
        "description": "Retry failed batches with backoff",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.retry_logic_implemented is True


def test_idempotency_key_pattern():
    """Test idempotency key pattern detection."""
    task = {
        "description": "Use idempotency key for batch operations",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.idempotency_ensured is True


def test_collect_results_pattern():
    """Test collect results pattern detection."""
    task = {
        "description": "Collect batch results for analysis",
    }

    result = analyze_batch_processing_strategy(task)

    assert result.result_aggregation_planned is True
