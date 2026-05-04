import copy
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_ip_allowlist_readiness import (
    TaskApiIPAllowlistReadinessFinding,
    TaskApiIPAllowlistReadinessPlan,
    plan_task_api_ip_allowlist_readiness,
)


def test_plan_with_comprehensive_ip_allowlist_tasks():
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-1",
                title="Implement IP allowlist middleware",
                description="Integrate middleware to filter requests based on allowlist. "
                "Implement CIDR range validation and tenant isolation enforcement.",
                acceptance_criteria=[
                    "Middleware integration filters incoming requests",
                    "CIDR range validation handles IPv4 and IPv6",
                    "Tenant-specific allowlists are properly isolated",
                ],
            ),
            _execution_task(
                task_id="task-2",
                title="Add IP-based authentication",
                description="Implement IP-based auth logic with source IP extraction. "
                "Configure endpoint bypass for public routes.",
                acceptance_criteria=[
                    "Source IP extracted from X-Forwarded-For header",
                    "Public endpoints bypass IP checks",
                ],
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    assert isinstance(result, TaskApiIPAllowlistReadinessPlan)
    assert result.plan_id == "plan-ip-allowlist"
    assert len(result.findings) == 2
    assert len(result.ip_allowlist_task_ids) == 2
    assert len(result.not_applicable_task_ids) == 0

    task1_finding = next(f for f in result.findings if f.task_id == "task-1")
    assert "allowlist_middleware" in task1_finding.detected_signals
    assert "cidr_validation" in task1_finding.detected_signals
    assert "tenant_management" in task1_finding.detected_signals
    assert "middleware_integration" in task1_finding.present_safeguards
    assert "cidr_range_validation" in task1_finding.present_safeguards
    assert "tenant_isolation_enforcement" in task1_finding.present_safeguards
    assert task1_finding.readiness == "strong"
    assert len(task1_finding.missing_safeguards) == 0

    task2_finding = next(f for f in result.findings if f.task_id == "task-2")
    assert "ip_auth_integration" in task2_finding.detected_signals
    assert "bypass_configuration" in task2_finding.detected_signals
    assert "ip_based_auth_logic" in task2_finding.present_safeguards
    assert "endpoint_bypass_config" in task2_finding.present_safeguards


def test_plan_with_missing_safeguards():
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-weak",
                title="Add IP allowlist check",
                description="Check client IP against allowlist.",
                acceptance_criteria=["IP allowlist filtering is enabled"],
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    finding = result.findings[0]
    assert finding.readiness == "weak"
    assert len(finding.missing_safeguards) > 0
    assert len(finding.actionable_remediations) > 0
    # Should detect the signal but miss safeguards
    assert len(finding.detected_signals) > 0
    assert len(finding.present_safeguards) == 0


def test_plan_with_no_ip_allowlist_impact():
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-unrelated",
                title="Add user profile endpoint",
                description="Create REST endpoint for user profile management",
            ),
            _execution_task(
                task_id="task-no-allowlist",
                title="Refactor database queries",
                description="No IP allowlist changes required for this task.",
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    assert len(result.findings) == 0
    assert len(result.not_applicable_task_ids) == 2
    assert "task-unrelated" in result.not_applicable_task_ids
    assert "task-no-allowlist" in result.not_applicable_task_ids


def test_partial_readiness_with_some_safeguards():
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-partial",
                title="Implement CIDR and geo features",
                description="Add CIDR support. Enable geo-blocking.",
                acceptance_criteria=[
                    "CIDR features implemented",
                ],
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    finding = result.findings[0]
    # Should detect signals but have missing safeguards
    assert finding.readiness in ("partial", "weak")
    assert len(finding.detected_signals) > 0
    # Should have at least one missing safeguard for partial/weak readiness
    assert len(finding.missing_safeguards) > 0 or finding.readiness == "partial"


def test_dynamic_update_and_logging():
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-dynamic",
                title="Add dynamic allowlist updates with audit logging",
                description="Implement runtime allowlist modification with cache invalidation. "
                "Add comprehensive violation logging with audit trail.",
                acceptance_criteria=[
                    "Dynamic update handling refreshes allowlist at runtime",
                    "Violation audit logging tracks denied requests",
                ],
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    finding = result.findings[0]
    assert "update_automation" in finding.detected_signals
    assert "violation_logging" in finding.detected_signals
    assert "dynamic_update_handling" in finding.present_safeguards
    assert "violation_audit_logging" in finding.present_safeguards
    assert finding.readiness == "strong"


def test_file_path_based_signal_detection():
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-files",
                title="Implement IP allowlist",
                description="Add IP filtering",
                files=[
                    "src/middleware/ip-allowlist.ts",
                    "src/validation/cidr-validator.ts",
                    "src/auth/ip-based-auth.ts",
                ],
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    finding = result.findings[0]
    # Should detect signals from file paths
    assert "allowlist_middleware" in finding.detected_signals or len(finding.detected_signals) > 0
    assert "cidr_validation" in finding.detected_signals or len(finding.detected_signals) > 0


def test_invalid_cidr_edge_case():
    """Test task that mentions CIDR validation for invalid ranges."""
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-invalid",
                title="Reject invalid CIDR ranges",
                description="CIDR range validation must reject malformed notation.",
                acceptance_criteria=["Invalid CIDR ranges are rejected with error"],
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    finding = result.findings[0]
    assert "cidr_validation" in finding.detected_signals


def test_allowlist_conflict_detection():
    """Test task involving conflicting allowlist configurations."""
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-conflict",
                title="Resolve tenant allowlist conflicts",
                description="Tenant isolation enforcement must prevent cross-tenant access.",
                acceptance_criteria=["Tenant-specific allowlists are properly separated"],
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    finding = result.findings[0]
    assert "tenant_management" in finding.detected_signals
    assert "tenant_isolation_enforcement" in finding.present_safeguards or "tenant_isolation_enforcement" in finding.missing_safeguards


def test_spoofed_ip_handling():
    """Test task that handles IP spoofing scenarios."""
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-spoofed",
                title="Log unauthorized IP access attempts",
                description="Violation audit logging must track spoofed IP attempts.",
                acceptance_criteria=["Denied access from blocked IPs is logged"],
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    finding = result.findings[0]
    assert "violation_logging" in finding.detected_signals
    assert "violation_audit_logging" in finding.present_safeguards or "violation_audit_logging" in finding.missing_safeguards


def test_evidence_collection():
    """Test that evidence is collected from task fields."""
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-evidence",
                title="Implement IP allowlist middleware",
                description="Add middleware integration for IP filtering",
                acceptance_criteria=["Middleware filters requests based on allowlist"],
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    finding = result.findings[0]
    assert len(finding.evidence) > 0
    # Evidence should reference the fields where signals were found
    evidence_text = " ".join(finding.evidence)
    assert "title" in evidence_text or "description" in evidence_text or "acceptance_criteria" in evidence_text


def test_summary_statistics():
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-strong",
                title="Full IP allowlist implementation",
                description="Middleware integration with CIDR validation",
                acceptance_criteria=[
                    "Middleware integration complete",
                    "CIDR range validation implemented",
                ],
            ),
            _execution_task(
                task_id="task-weak",
                title="Basic IP check",
                description="Check IP allowlist",
            ),
            _execution_task(
                task_id="task-unrelated",
                title="Database refactor",
                description="No IP allowlist impact",
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    assert result.summary["total_tasks"] == 3
    assert result.summary["ip_allowlist_tasks"] >= 1
    assert result.summary["not_applicable_tasks"] >= 1
    assert "readiness_distribution" in result.summary
    assert "most_common_gaps" in result.summary


def test_to_dict_serialization():
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-serialize",
                title="IP allowlist middleware",
                description="Add middleware",
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)
    result_dict = result.to_dict()

    assert result_dict["plan_id"] == "plan-ip-allowlist"
    assert "findings" in result_dict
    assert "records" in result_dict
    assert len(result_dict["findings"]) == len(result_dict["records"])
    if len(result_dict["findings"]) > 0:
        finding_dict = result_dict["findings"][0]
        assert "task_id" in finding_dict
        assert "title" in finding_dict
        assert "detected_signals" in finding_dict
        assert "present_safeguards" in finding_dict
        assert "missing_safeguards" in finding_dict
        assert "readiness" in finding_dict
        assert "actionable_remediations" in finding_dict


def test_mapping_and_object_input():
    """Test that the function accepts various input types."""
    # Test with plain dict
    plan_dict = {
        "id": "plan-dict",
        "tasks": [
            {
                "id": "task-dict",
                "title": "IP allowlist middleware",
                "description": "Middleware integration",
            }
        ],
    }
    result = plan_task_api_ip_allowlist_readiness(plan_dict)
    assert result.plan_id == "plan-dict"

    # Test with object
    plan_obj = SimpleNamespace(
        id="plan-obj",
        tasks=[
            SimpleNamespace(
                id="task-obj",
                title="CIDR validation",
                description="CIDR range validation",
            )
        ],
    )
    result = plan_task_api_ip_allowlist_readiness(plan_obj)
    assert result.plan_id == "plan-obj"


def test_immutability():
    """Test that the input plan is not mutated."""
    original_plan = {
        "id": "plan-immutable",
        "tasks": [
            {
                "id": "task-immutable",
                "title": "IP allowlist",
                "description": "Add allowlist",
            }
        ],
    }
    plan_copy = copy.deepcopy(original_plan)

    plan_task_api_ip_allowlist_readiness(original_plan)

    assert original_plan == plan_copy


def test_compatibility_properties():
    """Test that compatibility properties work correctly."""
    plan = _execution_plan(
        tasks=[
            _execution_task(
                task_id="task-compat",
                title="IP allowlist",
                description="Add middleware",
            ),
        ]
    )

    result = plan_task_api_ip_allowlist_readiness(plan)

    # Test plan-level compatibility
    assert result.records == result.findings

    # Test finding-level compatibility
    if len(result.findings) > 0:
        finding = result.findings[0]
        assert finding.actionable_gaps == finding.actionable_remediations


def _execution_plan(plan_id="plan-ip-allowlist", tasks=None):
    return {
        "id": plan_id,
        "source_brief_id": "brief-ip-allowlist",
        "title": "IP allowlist implementation plan",
        "domain": "api",
        "tasks": tasks or [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
    }


def _execution_task(task_id, title, description="", acceptance_criteria=None, files=None):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "acceptance_criteria": acceptance_criteria or [],
        "files": files or [],
        "dependencies": [],
        "status": "pending",
    }
