"""Tests for background job processing analyzer."""

import pytest

from blueprint.task_background_job_processing import (
    BackgroundJobProcessing,
    analyze_background_job_processing,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_background_job_processing({})

    assert isinstance(result, BackgroundJobProcessing)
    assert result.job_types_defined is False
    assert result.scheduling_patterns_configured is False
    assert result.priority_levels_configured is False
    assert result.timeout_configured is False
    assert result.retry_policy_implemented is False
    assert result.queue_management_planned is False
    assert result.idempotency_ensured is False
    assert result.failure_handling_implemented is False
    assert result.progress_tracking_enabled is False
    assert result.resource_contention_managed is False
    assert result.readiness_score == 0.0


def test_job_types_detected():
    """Detect job types configuration in task data."""
    task = {
        "title": "Configure background jobs",
        "description": "Define job types including scheduled jobs and async jobs",
    }

    result = analyze_background_job_processing(task)

    assert result.job_types_defined is True
    assert result.scheduling_patterns_configured is False
    assert result.readiness_score == 0.1


def test_scheduling_patterns_detected():
    """Detect scheduling patterns in task data."""
    task = {
        "description": "Configure job scheduling with cron expression and scheduled execution",
        "acceptance_criteria": ["Job frequency defined", "Run every hour"],
    }

    result = analyze_background_job_processing(task)

    assert result.scheduling_patterns_configured is True
    assert result.job_types_defined is False


def test_priority_levels_detected():
    """Detect priority levels in task data."""
    task = {
        "description": "Implement priority queue with high priority jobs and low priority jobs",
        "acceptance_criteria": ["Priority levels configured", "Priority-based execution"],
    }

    result = analyze_background_job_processing(task)

    assert result.priority_levels_configured is True


def test_timeout_detected():
    """Detect timeout configuration in task data."""
    task = {
        "description": "Configure job timeout for long-running jobs with execution timeout",
        "acceptance_criteria": ["Set timeout limit", "Maximum execution time defined"],
    }

    result = analyze_background_job_processing(task)

    assert result.timeout_configured is True


def test_retry_policy_detected():
    """Detect retry policy in task data."""
    task = {
        "description": "Implement retry strategy with exponential backoff and maximum retries",
        "acceptance_criteria": ["Retry policy configured", "Job retry on failure"],
    }

    result = analyze_background_job_processing(task)

    assert result.retry_policy_implemented is True


def test_queue_management_detected():
    """Detect queue management in task data."""
    task = {
        "description": "Configure job queues with multiple queues and queue workers",
        "acceptance_criteria": ["Queue management setup", "FIFO queue configured"],
    }

    result = analyze_background_job_processing(task)

    assert result.queue_management_planned is True


def test_idempotency_detected():
    """Detect idempotency in task data."""
    task = {
        "description": "Ensure idempotent jobs with duplicate detection and idempotency key",
        "acceptance_criteria": ["Idempotency ensured", "Safe to retry"],
    }

    result = analyze_background_job_processing(task)

    assert result.idempotency_ensured is True


def test_failure_handling_detected():
    """Detect failure handling in task data."""
    task = {
        "description": "Implement failure handling with dead letter queue and error recovery",
        "acceptance_criteria": ["Handle job failures", "Failed job processing"],
    }

    result = analyze_background_job_processing(task)

    assert result.failure_handling_implemented is True


def test_progress_tracking_detected():
    """Detect progress tracking in task data."""
    task = {
        "description": "Enable progress tracking with job status and progress monitoring",
        "acceptance_criteria": ["Track job progress", "Job completion status"],
    }

    result = analyze_background_job_processing(task)

    assert result.progress_tracking_enabled is True


def test_resource_contention_detected():
    """Detect resource contention management in task data."""
    task = {
        "description": "Manage resource contention with worker pool limits and rate limiting",
        "acceptance_criteria": ["Resource limits configured", "CPU usage constraints"],
    }

    result = analyze_background_job_processing(task)

    assert result.resource_contention_managed is True


def test_comprehensive_background_job_all_detected():
    """Test comprehensive background job processing with all aspects present."""
    task = {
        "title": "Complete background job implementation",
        "description": (
            "Define job types including Celery tasks and async jobs. "
            "Configure scheduling patterns with cron expressions. "
            "Implement priority queue with high priority levels. "
            "Set job timeout for long-running operations. "
            "Configure retry policy with exponential backoff. "
            "Set up queue management with multiple queues. "
            "Ensure idempotent processing with duplicate detection. "
            "Implement failure handling with dead letter queue. "
            "Enable progress tracking for job status. "
            "Manage resource contention with worker pool limits."
        ),
        "acceptance_criteria": [
            "Job types defined",
            "Scheduling configured",
            "Priority levels set",
            "Timeout configured",
            "Retry policy implemented",
            "Queue management active",
            "Idempotency ensured",
            "Failure handling verified",
            "Progress tracking enabled",
            "Resource limits managed",
        ],
    }

    result = analyze_background_job_processing(task)

    assert result.job_types_defined is True
    assert result.scheduling_patterns_configured is True
    assert result.priority_levels_configured is True
    assert result.timeout_configured is True
    assert result.retry_policy_implemented is True
    assert result.queue_management_planned is True
    assert result.idempotency_ensured is True
    assert result.failure_handling_implemented is True
    assert result.progress_tracking_enabled is True
    assert result.resource_contention_managed is True
    assert result.readiness_score == 1.0


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_background_job_processing(None)  # type: ignore

    assert isinstance(result, BackgroundJobProcessing)
    assert result.job_types_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_background_job_processing([{"key": "value"}])  # type: ignore

    assert isinstance(result, BackgroundJobProcessing)
    assert result.readiness_score == 0.0


def test_dataclass_immutability():
    """Test that BackgroundJobProcessing is frozen/immutable."""
    processing = BackgroundJobProcessing(job_types_defined=True)

    with pytest.raises(AttributeError):
        processing.job_types_defined = False  # type: ignore


def test_to_dict_method():
    """Test BackgroundJobProcessing.to_dict() serialization."""
    processing = BackgroundJobProcessing(
        job_types_defined=True,
        scheduling_patterns_configured=True,
        priority_levels_configured=False,
        timeout_configured=True,
        retry_policy_implemented=False,
        queue_management_planned=True,
        idempotency_ensured=False,
        failure_handling_implemented=True,
        progress_tracking_enabled=False,
        resource_contention_managed=True,
    )

    result = processing.to_dict()

    assert isinstance(result, dict)
    assert result["job_types_defined"] is True
    assert result["scheduling_patterns_configured"] is True
    assert result["priority_levels_configured"] is False
    assert result["timeout_configured"] is True
    assert result["retry_policy_implemented"] is False
    assert result["queue_management_planned"] is True
    assert result["idempotency_ensured"] is False
    assert result["failure_handling_implemented"] is True
    assert result["progress_tracking_enabled"] is False
    assert result["resource_contention_managed"] is True
    assert result["readiness_score"] == 0.6


def test_celery_pattern():
    """Test Celery pattern detection."""
    task = {
        "description": "Implement Celery tasks for background processing",
    }

    result = analyze_background_job_processing(task)

    assert result.job_types_defined is True


def test_sidekiq_pattern():
    """Test Sidekiq pattern detection."""
    task = {
        "description": "Configure Sidekiq jobs for async processing",
    }

    result = analyze_background_job_processing(task)

    assert result.job_types_defined is True


def test_cron_expression_pattern():
    """Test cron expression pattern detection."""
    task = {
        "description": "Set up cron expression for scheduled tasks",
    }

    result = analyze_background_job_processing(task)

    assert result.scheduling_patterns_configured is True


def test_priority_queue_pattern():
    """Test priority queue pattern detection."""
    task = {
        "description": "Implement priority queue for job processing",
    }

    result = analyze_background_job_processing(task)

    assert result.priority_levels_configured is True


def test_dead_letter_queue_pattern():
    """Test dead letter queue pattern detection."""
    task = {
        "description": "Configure DLQ for failed messages",
    }

    result = analyze_background_job_processing(task)

    assert result.failure_handling_implemented is True


def test_at_least_once_delivery_pattern():
    """Test at-least-once delivery pattern detection."""
    task = {
        "description": "Implement at-least-once processing for reliability",
    }

    result = analyze_background_job_processing(task)

    assert result.idempotency_ensured is True


def test_rate_limiting_pattern():
    """Test rate limiting pattern detection."""
    task = {
        "description": "Apply rate limiting to prevent resource exhaustion",
    }

    result = analyze_background_job_processing(task)

    assert result.resource_contention_managed is True


def test_throttling_pattern():
    """Test throttling pattern detection."""
    task = {
        "description": "Enable throttling for job execution",
    }

    result = analyze_background_job_processing(task)

    assert result.resource_contention_managed is True


def test_worker_pool_pattern():
    """Test worker pool pattern detection."""
    task = {
        "description": "Configure worker pool for concurrent job processing",
    }

    result = analyze_background_job_processing(task)

    assert result.resource_contention_managed is True


def test_backoff_strategy_pattern():
    """Test backoff strategy pattern detection."""
    task = {
        "description": "Implement exponential backoff for retries",
    }

    result = analyze_background_job_processing(task)

    assert result.retry_policy_implemented is True


def test_recurring_job_pattern():
    """Test recurring job pattern detection."""
    task = {
        "description": "Set up recurring jobs to run daily",
    }

    result = analyze_background_job_processing(task)

    assert result.job_types_defined is True


def test_delayed_execution_pattern():
    """Test delayed execution pattern detection."""
    task = {
        "description": "Schedule delayed execution for background tasks",
    }

    result = analyze_background_job_processing(task)

    assert result.scheduling_patterns_configured is True


def test_job_status_pattern():
    """Test job status tracking pattern detection."""
    task = {
        "description": "Monitor job status for all background tasks",
    }

    result = analyze_background_job_processing(task)

    assert result.progress_tracking_enabled is True


def test_duplicate_prevention_pattern():
    """Test duplicate prevention pattern detection."""
    task = {
        "description": "Prevent duplicate job execution with deduplication",
    }

    result = analyze_background_job_processing(task)

    assert result.idempotency_ensured is True


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "Job processing",
        "description": "Background jobs with retry policy",
        "acceptance_criteria": ["Configure job queues"],
        "requirements": ["Set timeout limits"],
        "notes": ["Track progress"],
    }

    result = analyze_background_job_processing(task)

    assert result.job_types_defined is True
    assert result.retry_policy_implemented is True
    assert result.queue_management_planned is True
    assert result.timeout_configured is True
    assert result.progress_tracking_enabled is True


def test_readiness_score_calculation():
    """Test readiness score calculation with different combinations."""
    # 0/10 = 0.0
    task1 = {"description": "Generic task"}
    result1 = analyze_background_job_processing(task1)
    assert result1.readiness_score == 0.0

    # 1/10 = 0.1
    task2 = {"description": "Configure background jobs"}
    result2 = analyze_background_job_processing(task2)
    assert result2.readiness_score == 0.1

    # 2/10 = 0.2 (only background jobs and retry policy match from the description)
    task3 = {
        "description": "Background jobs, scheduling, priority, timeout, and retry policy"
    }
    result3 = analyze_background_job_processing(task3)
    # Note: "scheduling", "priority", "timeout" alone don't match the full patterns
    # which require more context like "scheduling patterns", "priority levels", "job timeout"
    assert result3.readiness_score == 0.2


def test_job_chaining_edge_case():
    """Test job chaining detection."""
    task = {
        "description": "Implement job chaining for dependent background tasks",
        "acceptance_criteria": [
            "Configure job types for chaining",
            "Track progress across chained jobs",
        ],
    }

    result = analyze_background_job_processing(task)

    assert result.job_types_defined is True
    assert result.progress_tracking_enabled is True


def test_scheduled_jobs_edge_case():
    """Test scheduled jobs detection."""
    task = {
        "description": "Set up scheduled jobs to run at specific times with cron pattern",
        "acceptance_criteria": [
            "Job scheduling configured",
            "Recurring job support",
        ],
    }

    result = analyze_background_job_processing(task)

    assert result.scheduling_patterns_configured is True
    assert result.job_types_defined is True


def test_priority_queues_edge_case():
    """Test priority queues detection."""
    task = {
        "description": "Implement priority queue system with high and low priority jobs",
        "acceptance_criteria": [
            "Priority levels defined",
            "Queue management configured",
        ],
    }

    result = analyze_background_job_processing(task)

    assert result.priority_levels_configured is True
    assert result.queue_management_planned is True


def test_partial_readiness():
    """Test partial readiness with some aspects covered."""
    task = {
        "title": "Basic job setup",
        "description": "Configure background jobs",
        "acceptance_criteria": [
            "Define async jobs",
            "Set up retry policy",
        ],
    }

    result = analyze_background_job_processing(task)

    assert result.job_types_defined is True
    assert result.retry_policy_implemented is True
    assert result.scheduling_patterns_configured is False
    assert result.priority_levels_configured is False
    assert result.timeout_configured is False
    assert result.queue_management_planned is False
    assert result.idempotency_ensured is False
    assert result.failure_handling_implemented is False
    assert result.progress_tracking_enabled is False
    assert result.resource_contention_managed is False
    assert result.readiness_score == 0.2
