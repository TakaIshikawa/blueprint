import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_dns_cutover_readiness import (
    TaskDnsCutoverReadinessPlan,
    TaskDnsCutoverReadinessRecord,
    analyze_task_dns_cutover_readiness,
    build_task_dns_cutover_readiness_plan,
    extract_task_dns_cutover_readiness,
    generate_task_dns_cutover_readiness,
    summarize_task_dns_cutover_readiness,
    task_dns_cutover_readiness_plan_to_dict,
    task_dns_cutover_readiness_plan_to_dicts,
    task_dns_cutover_readiness_plan_to_markdown,
)


def test_high_risk_traffic_cutover_without_rollback_or_monitoring():
    result = build_task_dns_cutover_readiness_plan(
        _plan(
            [
                _task(
                    "task-cutover",
                    title="Cut over app.example.com traffic",
                    description=(
                        "Update DNS CNAME and A record for the app domain, attach the TLS "
                        "certificate, and shift production traffic to Cloudflare."
                    ),
                    files_or_modules=[
                        "infra/route53/app.example.com.zone.tf",
                        "infra/cloudflare/traffic_cutover.tf",
                    ],
                    acceptance_criteria=["The new hostname serves HTTPS."],
                )
            ]
        )
    )

    assert isinstance(result, TaskDnsCutoverReadinessPlan)
    assert result.cutover_task_ids == ("task-cutover",)
    record = result.records[0]
    assert isinstance(record, TaskDnsCutoverReadinessRecord)
    assert record.detected_signals == (
        "dns_record_change",
        "tls_certificate",
        "traffic_shift",
    )
    assert record.present_safeguards == ("tls_certificate",)
    assert record.missing_safeguards == (
        "ttl_management",
        "rollback_record",
        "monitoring_probe",
        "owner_approval",
    )
    assert record.risk_level == "high"
    assert "files_or_modules: infra/route53/app.example.com.zone.tf" in record.evidence
    assert result.summary["cutover_task_count"] == 1
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}
    assert result.summary["signal_counts"]["traffic_shift"] == 1
    assert result.summary["missing_safeguard_counts"]["rollback_record"] == 1


def test_low_risk_fully_covered_dns_tls_cutover():
    result = analyze_task_dns_cutover_readiness(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Cut over www domain DNS",
                    description=(
                        "Lower TTL to 300 before the DNS cutover. Verify domain ownership with "
                        "TXT verification, validate the TLS certificate expiry and SAN coverage, "
                        "shift traffic after synthetic monitoring probes pass, store rollback "
                        "records for previous DNS values, and capture service owner approval."
                    ),
                    acceptance_criteria=[
                        "dig and curl health checks pass before and after cutover.",
                        "Previous CNAME and A record values are available for rollback.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == (
        "dns_record_change",
        "ttl_management",
        "tls_certificate",
        "domain_verification",
        "traffic_shift",
        "rollback_record",
        "monitoring_probe",
        "owner_approval",
    )
    assert record.present_safeguards == (
        "ttl_management",
        "tls_certificate",
        "domain_verification",
        "rollback_record",
        "monitoring_probe",
        "owner_approval",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "low"
    assert record.recommended_readiness_steps == ()
    assert result.summary["missing_safeguard_count"] == 0
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_metadata_tags_and_validation_commands_detect_cutover_readiness():
    result = build_task_dns_cutover_readiness_plan(
        _plan(
            [
                _task(
                    "task-mail",
                    title="Verify customer domain email DNS",
                    description="Add MX and TXT record verification for the customer domain.",
                    metadata={
                        "route53": {"hosted_zone": "customer.example.com"},
                        "domain_verification": "SPF, DKIM, and DMARC TXT verification records are required.",
                        "validation_commands": {
                            "dns": ["dig TXT customer.example.com", "curl http://customer.example.com/health"]
                        },
                    },
                    tags=["TTL lowered to 300", "domain owner approval"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == (
        "dns_record_change",
        "ttl_management",
        "domain_verification",
        "monitoring_probe",
        "owner_approval",
    )
    assert record.present_safeguards == (
        "ttl_management",
        "domain_verification",
        "monitoring_probe",
        "owner_approval",
    )
    assert record.missing_safeguards == ("rollback_record",)
    assert record.risk_level == "medium"
    assert any("metadata.domain_verification" in item for item in record.evidence)
    assert any("metadata.validation_commands.dns[0]: dig TXT customer.example.com" in item for item in record.evidence)
    assert result.summary["present_safeguard_counts"]["monitoring_probe"] == 1


def test_unrelated_tasks_return_empty_records_and_stable_markdown():
    result = build_task_dns_cutover_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="No DNS or TLS changes are in scope.",
                    files_or_modules=["src/blueprint/ui/dashboard_copy.py"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.cutover_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "cutover_task_count": 0,
        "not_applicable_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "dns_record_change": 0,
            "ttl_management": 0,
            "tls_certificate": 0,
            "domain_verification": 0,
            "traffic_shift": 0,
            "rollback_record": 0,
            "monitoring_probe": 0,
            "owner_approval": 0,
        },
        "present_safeguard_counts": {
            "ttl_management": 0,
            "tls_certificate": 0,
            "domain_verification": 0,
            "rollback_record": 0,
            "monitoring_probe": 0,
            "owner_approval": 0,
        },
        "missing_safeguard_counts": {
            "ttl_management": 0,
            "tls_certificate": 0,
            "domain_verification": 0,
            "rollback_record": 0,
            "monitoring_probe": 0,
            "owner_approval": 0,
        },
    }
    markdown = result.to_markdown()
    assert "No DNS cutover readiness records were inferred." in markdown
    assert "Not-applicable tasks: task-copy" in markdown


def test_serialization_aliases_markdown_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="DNS cutover | rollback",
                description=(
                    "Cutover DNS traffic with lower TTL to 300, synthetic probes, "
                    "rollback records, and owner approval."
                ),
            ),
            _task(
                "task-a",
                title="TLS certificate cutover",
                description="Update TLS certificate for app.example.com with owner approval.",
            ),
            _task("task-copy", title="Profile UI copy", description="Adjust labels."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_dns_cutover_readiness(plan)
    payload = task_dns_cutover_readiness_plan_to_dict(result)
    markdown = task_dns_cutover_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_dns_cutover_readiness_plan_to_dicts(result) == payload["records"]
    assert task_dns_cutover_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_dns_cutover_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_dns_cutover_readiness(plan).to_dict() == result.to_dict()
    assert result.affected_task_ids == result.cutover_task_ids
    assert result.no_signal_task_ids == result.not_applicable_task_ids
    assert result.cutover_task_ids == ("task-a", "task-z")
    assert list(payload) == [
        "plan_id",
        "records",
        "cutover_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "recommended_readiness_steps",
    ]
    assert [record.risk_level for record in result.records] == ["high", "low"]
    assert markdown.startswith("# Task DNS Cutover Readiness: plan-dns")
    assert "DNS cutover \\| rollback" in markdown


def test_execution_plan_task_and_object_like_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Update CDN TLS certificate",
        description=(
            "CDN TLS certificate cutover lowers TTL to 300, validates domain verification, "
            "runs synthetic monitoring probes, records rollback values, and has owner approval."
        ),
        files_or_modules=["infra/cdn/tls_certificate.tf"],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Route53 DNS cutover",
            description="Lower TTL and cutover Route53 CNAME traffic with rollback records.",
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan([task_model.model_dump(mode="python")], plan_id="plan-model")
    )

    iterable_result = build_task_dns_cutover_readiness_plan([object_task])
    task_result = build_task_dns_cutover_readiness_plan(task_model)
    plan_result = build_task_dns_cutover_readiness_plan(plan_model)

    assert iterable_result.records[0].task_id == "task-object"
    assert iterable_result.records[0].risk_level == "low"
    assert task_result.records[0].task_id == "task-model"
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-model"


def _plan(tasks, plan_id="plan-dns"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-dns",
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
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
