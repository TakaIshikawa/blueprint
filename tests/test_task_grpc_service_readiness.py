import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_grpc_service_readiness import (
    TaskGRPCServiceReadinessPlan,
    TaskGRPCServiceReadinessRecord,
    analyze_task_grpc_service_readiness,
    build_task_grpc_service_readiness_plan,
    derive_task_grpc_service_readiness,
    extract_task_grpc_service_readiness,
    generate_task_grpc_service_readiness,
    recommend_task_grpc_service_readiness,
    summarize_task_grpc_service_readiness,
    task_grpc_service_readiness_plan_to_dict,
    task_grpc_service_readiness_plan_to_dicts,
    task_grpc_service_readiness_plan_to_markdown,
    task_grpc_service_readiness_to_dicts,
)


def test_weak_grpc_service_task_sorts_first_and_separates_no_impact_ids():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Add gRPC service with TLS",
                    description="Configure protobuf compilation and service registration for gRPC services.",
                    acceptance_criteria=[
                        "Protobuf compilation and versioning configured.",
                        "Service registration setup complete.",
                        "Health checking protocol implemented.",
                    ],
                ),
                _task(
                    "task-copy",
                    title="Polish settings copy",
                    description="Update labels in the admin settings page.",
                ),
                _task(
                    "task-weak",
                    title="Enable gRPC service",
                    description="Add protobuf definition with streaming operation and service discovery.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskGRPCServiceReadinessPlan)
    assert result.grpc_task_ids == ("task-weak", "task-partial")
    assert result.impacted_task_ids == result.grpc_task_ids
    assert result.no_impact_task_ids == ("task-copy",)
    assert [record.task_id for record in result.records] == ["task-weak", "task-partial"]
    weak = result.records[0]
    assert isinstance(weak, TaskGRPCServiceReadinessRecord)
    assert weak.readiness == "weak"
    assert weak.impact == "high"
    assert {"protobuf_definition", "streaming_operation", "service_discovery"} <= set(weak.detected_signals)
    assert "protobuf_compilation" in weak.missing_safeguards
    assert "streaming_management" in weak.missing_safeguards


def test_strong_readiness_reflects_all_safeguards_and_summary_counts():
    result = analyze_task_grpc_service_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Add gRPC service with complete safeguards",
                    description=(
                        "Protobuf definition with streaming operation and service discovery. "
                        "Implement protobuf compilation, service registration, and error handling strategy. "
                        "Configure deadline enforcement and retry backoff policy."
                    ),
                    acceptance_criteria=[
                        "Protobuf compilation with version control.",
                        "Service registration and discovery setup.",
                        "Error handling strategy with status codes.",
                        "Deadline enforcement with timeout config.",
                        "Retry backoff policy with exponential backoff.",
                        "Streaming management with flow control.",
                        "Health checking protocol with liveness probes.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "strong"
    assert record.impact == "medium"
    assert record.present_safeguards == (
        "protobuf_compilation",
        "service_registration",
        "error_handling_strategy",
        "deadline_enforcement",
        "retry_backoff_policy",
        "streaming_management",
        "health_checking",
    )
    assert record.missing_safeguards == ()
    assert record.recommended_checks == ()
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "strong": 1}
    assert result.summary["impact_counts"] == {"high": 0, "medium": 1, "low": 0}
    assert result.summary["signal_counts"]["protobuf_definition"] == 1
    assert result.summary["present_safeguard_counts"]["health_checking"] == 1


def test_mapping_input_collects_title_description_paths_and_validation_command_evidence():
    result = build_task_grpc_service_readiness_plan(
        {
            "id": "task-mapping",
            "title": "Add gRPC protobuf compilation",
            "description": "Configure protobuf compilation for gRPC services with deadline enforcement and health checking.",
            "files_or_modules": [
                "proto/service.proto",
                "src/grpc/protobuf_compilation.py",
                "tests/test_health_checking.py",
            ],
            "validation_commands": {
                "tests": [
                    "poetry run pytest tests/test_grpc_service.py",
                    "poetry run pytest tests/test_health_checking.py",
                ]
            },
        }
    )

    record = result.records[0]
    assert {"protobuf_definition", "service_method"} <= set(record.detected_signals)
    assert {"protobuf_compilation", "health_checking"} <= set(record.present_safeguards)
    assert any(item == "title: Add gRPC protobuf compilation" for item in record.evidence)
    assert any("description: Configure protobuf compilation" in item for item in record.evidence)
    assert any(item == "files_or_modules: proto/service.proto" for item in record.evidence)
    assert any("validation_commands: poetry run pytest tests/test_health_checking.py" in item for item in record.evidence)


def test_execution_plan_execution_task_and_object_inputs_are_supported_without_mutation():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add protobuf schema",
        description="Enable protobuf definition with service method.",
        acceptance_criteria=["Health checking protocol implemented."],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Configure deadline enforcement",
            description="Implement deadline enforcement for gRPC requests.",
            acceptance_criteria=["Deadline enforcement prevents long-running requests."],
        )
    )
    source = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-covered",
                title="Add gRPC middleware",
                description="Protobuf definition with streaming operation and TLS.",
                acceptance_criteria=[
                    "Protobuf compilation configured.",
                    "Service registration setup.",
                    "Error handling strategy defined.",
                    "Deadline enforcement enabled.",
                    "Retry backoff policy implemented.",
                    "Streaming management configured.",
                    "Health checking protocol active.",
                ],
            ),
        ],
        plan_id="plan-grpc-objects",
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    result = summarize_task_grpc_service_readiness(source)

    assert source == original
    assert build_task_grpc_service_readiness_plan(object_task).records[0].task_id == "task-object"
    assert generate_task_grpc_service_readiness(model).plan_id == "plan-grpc-objects"
    assert derive_task_grpc_service_readiness(source).to_dict() == result.to_dict()
    assert recommend_task_grpc_service_readiness(source).to_dict() == result.to_dict()
    assert extract_task_grpc_service_readiness(source).to_dict() == result.to_dict()
    assert result.records == result.findings
    assert result.records == result.recommendations


def test_empty_state_markdown_and_summary_are_stable():
    result = build_task_grpc_service_readiness_plan(
        _plan([_task("task-ui", title="Polish dashboard", description="Adjust table spacing.")])
    )

    assert result.records == ()
    assert result.grpc_task_ids == ()
    assert result.no_impact_task_ids == ("task-ui",)
    assert result.summary["grpc_task_count"] == 0
    assert result.to_markdown() == "\n".join(
        [
            "# Task gRPC Service Readiness: plan-grpc-service",
            "",
            "## Summary",
            "",
            "- Task count: 1",
            "- gRPC task count: 0",
            "- Missing safeguard count: 0",
            "- Readiness counts: weak 0, partial 0, strong 0",
            "- Impact counts: high 0, medium 0, low 0",
            "",
            "No task gRPC service readiness records were inferred.",
            "",
            "No-impact tasks: task-ui",
        ]
    )


def test_serialization_aliases_to_dict_output_and_markdown_are_json_safe():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-grpc",
                    title="Add gRPC protobuf compilation",
                    description="Protobuf definition with service registration and deadline enforcement.",
                    acceptance_criteria=[
                        "Protobuf compilation configured.",
                        "Service registration setup.",
                        "Health checking protocol active.",
                    ],
                )
            ],
            plan_id="plan-serialization",
        )
    )
    payload = task_grpc_service_readiness_plan_to_dict(result)
    markdown = task_grpc_service_readiness_plan_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_grpc_service_readiness_plan_to_dicts(result) == payload["records"]
    assert task_grpc_service_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_grpc_service_readiness_to_dicts(result) == payload["records"]
    assert task_grpc_service_readiness_to_dicts(result.records) == payload["records"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task gRPC Service Readiness: plan-serialization")
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "grpc_task_ids",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness",
        "impact",
        "recommended_checks",
        "evidence",
    ]


def test_protobuf_definition_signal_detection_and_impact_assessment():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-protobuf",
                    title="Add protobuf definition",
                    description="Define protobuf schema with protocol buffer messages.",
                    acceptance_criteria=[
                        "Protobuf compilation with version control.",
                        "Proto file definition complete.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "protobuf_definition" in record.detected_signals
    assert "protobuf_compilation" in record.present_safeguards
    # Only 1 safeguard present, needs 3+ for partial
    assert record.readiness == "weak"


def test_service_method_signal_detection():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-service",
                    title="Add gRPC service methods",
                    description="Implement RPC method definitions with service implementation and error handling strategy.",
                    acceptance_criteria=[
                        "Service method implementation complete.",
                        "Error handling strategy handles status codes.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "service_method" in record.detected_signals
    assert "error_handling_strategy" in record.present_safeguards


def test_streaming_operation_signal_and_streaming_management_safeguard():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-streaming",
                    title="Add streaming RPCs",
                    description="Implement server streaming and bidirectional streaming with streaming management and flow control.",
                    acceptance_criteria=[
                        "Streaming management handles connection lifecycle.",
                        "Flow control prevents backpressure issues.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "streaming_operation" in record.detected_signals
    assert "streaming_management" in record.present_safeguards


def test_error_status_signal_detection():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-error",
                    title="Handle gRPC status codes",
                    description="Implement error status handling with gRPC status codes and error handling strategy.",
                    acceptance_criteria=[
                        "gRPC status codes mapped correctly.",
                        "Error handling strategy handles all error cases.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "error_status" in record.detected_signals
    assert "error_handling_strategy" in record.present_safeguards


def test_interceptor_middleware_signal_detection():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-interceptor",
                    title="Add gRPC interceptors",
                    description="Implement server interceptor and client interceptor for authentication with deadline enforcement.",
                    acceptance_criteria=[
                        "Unary interceptor handles authentication.",
                        "Stream interceptor chains middleware.",
                        "Deadline enforcement configured in interceptor.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "interceptor_middleware" in record.detected_signals
    assert "deadline_enforcement" in record.present_safeguards


def test_service_discovery_signal_and_impact():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-discovery",
                    title="Add service discovery",
                    description="Implement service registration with Consul and service discovery for load balancing.",
                    acceptance_criteria=[
                        "Service registration with Consul configured.",
                        "Service registry enables dynamic discovery.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "service_discovery" in record.detected_signals
    assert "service_registration" in record.present_safeguards
    # service_discovery is a high-impact signal
    assert record.impact in ("high", "medium")


def test_tls_mtls_signal_and_impact():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-tls",
                    title="Configure TLS/mTLS",
                    description="Enable mutual TLS with server certificate and client certificate for secure channel.",
                    acceptance_criteria=[
                        "TLS configuration with x509 certificates.",
                        "mTLS authentication enabled.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "tls_mtls" in record.detected_signals
    # tls_mtls is a high-impact signal
    assert record.impact in ("high", "medium")


def test_protobuf_compilation_safeguard():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-compilation",
                    title="Configure protobuf compilation for protobuf definition",
                    description="Setup protoc compiler with code generation and backward compatibility checks for proto files.",
                    acceptance_criteria=[
                        "Protobuf compilation generates code correctly.",
                        "Backward compatibility validated.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "protobuf_compilation" in record.present_safeguards


def test_service_registration_safeguard():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-registration",
                    title="Setup service registration for gRPC",
                    description="Configure service registry with register service and deregister logic for service method discovery.",
                    acceptance_criteria=[
                        "Service registration config enables discovery.",
                        "Registry config handles health registration.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "service_registration" in record.present_safeguards


def test_error_handling_strategy_safeguard():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-error-strategy",
                    title="Define error handling strategy for gRPC service",
                    description="Implement error handling strategy with status code mapping and circuit breaker for graceful degradation.",
                    acceptance_criteria=[
                        "Error handling strategy maps errors correctly.",
                        "Circuit breaker prevents cascading failures.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "error_handling_strategy" in record.present_safeguards


def test_deadline_enforcement_safeguard():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-deadline",
                    title="Configure deadline enforcement for gRPC service",
                    description="Implement deadline enforcement with context deadline and timeout policy for RPC timeout handling.",
                    acceptance_criteria=[
                        "Deadline enforcement configured.",
                        "Request timeout prevents resource exhaustion.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "deadline_enforcement" in record.present_safeguards


def test_retry_backoff_policy_safeguard():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-retry",
                    title="Implement retry backoff policy for protobuf service",
                    description="Configure retry policy with exponential backoff and jitter for retry strategy with max retry limit.",
                    acceptance_criteria=[
                        "Retry backoff policy with exponential backoff.",
                        "Retry attempt handling with jitter.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "retry_backoff_policy" in record.present_safeguards


def test_streaming_management_safeguard():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-stream-mgmt",
                    title="Configure streaming management for streaming operation",
                    description="Implement streaming management with flow control and backpressure for stream lifecycle management.",
                    acceptance_criteria=[
                        "Streaming management handles connection lifecycle.",
                        "Flow control prevents backpressure issues.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "streaming_management" in record.present_safeguards


def test_health_checking_safeguard():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-health",
                    title="Add gRPC health checking for service method",
                    description="Implement gRPC health check protocol with liveness and readiness probes for service health monitoring.",
                    acceptance_criteria=[
                        "Health checking protocol implemented.",
                        "Liveness and readiness probes configured.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "health_checking" in record.present_safeguards


def test_out_of_scope_tasks_produce_no_records():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task("task-db", title="Optimize database queries", description="Add indexes to user table."),
                _task("task-cache", title="Add Redis cache", description="Cache frequently accessed data."),
                _task("task-logging", title="Improve logging", description="Add structured logging."),
            ]
        )
    )

    assert result.records == ()
    assert result.grpc_task_ids == ()
    assert result.no_impact_task_ids == ("task-db", "task-cache", "task-logging")
    assert result.summary["grpc_task_count"] == 0


def test_invalid_inputs_produce_empty_results():
    for invalid in [None, "", [], {}, 42, b"bytes"]:
        result = build_task_grpc_service_readiness_plan(invalid)
        assert result.records == ()
        assert result.grpc_task_ids == ()
        assert result.summary["grpc_task_count"] == 0


def test_path_signal_detection_from_files_or_modules():
    result = build_task_grpc_service_readiness_plan(
        _task(
            "task-files",
            title="Update gRPC proto files",
            files_or_modules=[
                "proto/service.proto",
                "src/grpc/service_method.py",
                "src/grpc/stream_handler.py",
                "tests/test_health_checking.py",
            ],
        )
    )

    record = result.records[0]
    assert {"protobuf_definition", "service_method", "streaming_operation"} <= set(record.detected_signals)
    assert any("files_or_modules: proto/service.proto" in item for item in record.evidence)


def test_evidence_extraction_from_task_fields_and_validation_commands():
    result = build_task_grpc_service_readiness_plan(
        _task(
            "task-evidence",
            title="Add gRPC protobuf compilation",
            description="Configure protobuf compilation with deadline enforcement for gRPC service.",
            acceptance_criteria=[
                "Protobuf compilation with version control.",
                "Deadline enforcement prevents long-running requests.",
                "Health checking protocol implemented.",
            ],
            validation_commands=["npm run test:grpc", "npm run grpc-health-check"],
        )
    )

    record = result.records[0]
    assert any("title: Add gRPC protobuf compilation" in item for item in record.evidence)
    assert any("description: Configure protobuf compilation" in item for item in record.evidence)
    assert any("acceptance_criteria[0]: Protobuf compilation with version control" in item for item in record.evidence)
    assert any("validation_commands: npm run grpc-health-check" in item for item in record.evidence)


def test_metadata_field_signal_detection():
    result = build_task_grpc_service_readiness_plan(
        _task(
            "task-metadata",
            title="Update gRPC service",
            metadata={
                "grpc_config": {
                    "protobuf_compilation": True,
                    "deadline_enforcement": 5000,
                    "service_registration": "enabled",
                },
                "tls_mtls": "required",
            },
        )
    )

    record = result.records[0]
    # Metadata keys trigger signals
    assert {"tls_mtls"} <= set(record.detected_signals)
    assert {"protobuf_compilation", "deadline_enforcement", "service_registration"} <= set(record.present_safeguards)


def test_recommended_checks_match_missing_safeguards():
    result = build_task_grpc_service_readiness_plan(
        _task(
            "task-recommendations",
            title="Add protobuf schema",
            description="Define protobuf definition with service method.",
        )
    )

    record = result.records[0]
    assert len(record.recommended_checks) == len(record.missing_safeguards)
    assert all(check for check in record.recommended_checks)
    assert record.recommended_checks == record.recommendations
    assert record.recommended_checks == record.recommended_actions


def test_signal_and_safeguard_ordering_is_deterministic():
    result = build_task_grpc_service_readiness_plan(
        _task(
            "task-order",
            title="gRPC configuration",
            description=(
                "TLS/mTLS, service discovery, interceptor middleware, error status, "
                "streaming operation, service method, protobuf definition."
            ),
            acceptance_criteria=[
                "Health checking, streaming management, retry backoff policy, "
                "deadline enforcement, error handling strategy, service registration, protobuf compilation."
            ],
        )
    )

    record = result.records[0]
    # Signals should be in _SIGNAL_ORDER
    expected_signals = (
        "protobuf_definition",
        "service_method",
        "streaming_operation",
        "error_status",
        "interceptor_middleware",
        "service_discovery",
        "tls_mtls",
    )
    assert record.detected_signals == expected_signals

    # Safeguards should be in _SAFEGUARD_ORDER
    expected_safeguards = (
        "protobuf_compilation",
        "service_registration",
        "error_handling_strategy",
        "deadline_enforcement",
        "retry_backoff_policy",
        "streaming_management",
        "health_checking",
    )
    assert record.present_safeguards == expected_safeguards


def test_summary_counts_match_records():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task("weak-1", title="Add gRPC", description="Protobuf definition with service method."),
                _task(
                    "partial-1",
                    title="gRPC middleware",
                    description="Protobuf definition with protobuf compilation.",
                    acceptance_criteria=["Protobuf compilation.", "Service registration.", "Health checking."],
                ),
                _task(
                    "strong-1",
                    title="Full gRPC",
                    description="Protobuf definition with streaming operation.",
                    acceptance_criteria=[
                        "Protobuf compilation.",
                        "Service registration.",
                        "Error handling strategy.",
                        "Deadline enforcement.",
                        "Retry backoff policy.",
                        "Streaming management.",
                        "Health checking.",
                    ],
                ),
                _task("no-impact", title="Database migration", description="Add column."),
            ]
        )
    )

    assert result.summary["task_count"] == 4
    assert result.summary["grpc_task_count"] == 3
    assert result.summary["no_impact_task_count"] == 1
    assert result.summary["readiness_counts"]["weak"] == 1
    assert result.summary["readiness_counts"]["partial"] == 1
    assert result.summary["readiness_counts"]["strong"] == 1
    assert result.summary["signal_counts"]["protobuf_definition"] == 3
    # Both partial and strong tasks have health checking
    assert result.summary["present_safeguard_counts"]["health_checking"] == 2


def test_high_impact_with_missing_protobuf_compilation():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-high-impact",
                    title="Add protobuf schema",
                    description="Protobuf definition with service discovery.",
                    # No protobuf compilation safeguard
                )
            ]
        )
    )

    record = result.records[0]
    assert "protobuf_definition" in record.detected_signals
    assert "service_discovery" in record.detected_signals
    assert "protobuf_compilation" in record.missing_safeguards
    # High impact due to high-impact signal + missing protobuf_compilation
    assert record.impact == "high"


def test_high_impact_with_streaming_operation_and_missing_streaming_management():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-streaming-impact",
                    title="Add streaming RPCs",
                    description="Protobuf definition with server streaming and bidirectional streaming.",
                    # No streaming management
                )
            ]
        )
    )

    record = result.records[0]
    assert "protobuf_definition" in record.detected_signals
    assert "streaming_operation" in record.detected_signals
    assert "streaming_management" in record.missing_safeguards
    # High impact due to high-impact signal + streaming_operation + missing streaming_management
    assert record.impact == "high"


def test_partial_readiness_with_three_safeguards():
    result = build_task_grpc_service_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial-readiness",
                    title="Add gRPC service safeguards",
                    description="Protobuf definition with service method.",
                    acceptance_criteria=[
                        "Protobuf compilation configured.",
                        "Service registration setup.",
                        "Deadline enforcement enabled.",
                        # Missing 4 other safeguards
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert len(record.present_safeguards) == 3
    assert len(record.missing_safeguards) == 4
    assert record.readiness == "partial"


def test_property_aliases_work_correctly():
    result = build_task_grpc_service_readiness_plan(
        _task(
            "task-aliases",
            title="Add protobuf schema",
            description="Protobuf definition with protobuf compilation.",
            acceptance_criteria=["Protobuf compilation configured."],
        )
    )

    record = result.records[0]
    assert record.signals == record.detected_signals
    assert record.safeguards == record.present_safeguards
    assert result.findings == result.records
    assert result.recommendations == result.records
    assert result.impacted_task_ids == result.grpc_task_ids


def _plan(tasks, *, plan_id="plan-grpc-service"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-grpc-service",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    validation_commands=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    if validation_commands is not None:
        task["validation_commands"] = validation_commands
    return task
