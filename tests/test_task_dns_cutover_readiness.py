import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_dns_cutover_readiness import (
    TaskDnsCutoverReadinessPlan,
    TaskDnsCutoverReadinessRecord,
    analyze_task_dns_cutover_readiness,
    build_task_dns_cutover_readiness_plan,
    summarize_task_dns_cutover_readiness,
    task_dns_cutover_readiness_plan_to_dict,
    task_dns_cutover_readiness_plan_to_markdown,
)


def test_dns_certificate_cdn_cutover_detects_signals_and_missing_safeguards():
    result = build_task_dns_cutover_readiness_plan(
        _plan(
            [
                _task(
                    "task-cutover",
                    title="Move app.example.com traffic to Cloudflare CDN",
                    description=(
                        "Update DNS CNAME and A record for the app subdomain, attach the TLS "
                        "certificate, and perform the traffic cutover through Cloudflare."
                    ),
                    files_or_modules=[
                        "infra/route53/app.example.com.zone.tf",
                        "infra/cloudflare/cdn_cutover.tf",
                    ],
                    acceptance_criteria=["The new hostname serves HTTPS."],
                )
            ]
        )
    )

    assert isinstance(result, TaskDnsCutoverReadinessPlan)
    assert result.affected_task_ids == ("task-cutover",)
    record = result.records[0]
    assert isinstance(record, TaskDnsCutoverReadinessRecord)
    assert record.detected_signals == (
        "dns",
        "cname",
        "a_record",
        "domain",
        "subdomain",
        "certificate",
        "tls",
        "cdn",
        "cloudflare",
        "route53",
        "traffic_cutover",
    )
    assert record.missing_safeguards == (
        "ttl_lowering",
        "staged_validation",
        "certificate_renewal_checks",
        "propagation_window",
        "rollback_records",
        "owner_approval",
    )
    assert record.readiness_level == "weak"
    assert "files_or_modules: infra/route53/app.example.com.zone.tf" in record.evidence
    assert result.summary["affected_task_count"] == 1
    assert result.summary["readiness_counts"] == {"weak": 1, "partial": 0, "strong": 0}
    assert result.summary["signal_counts"]["cloudflare"] == 1


def test_strong_readiness_requires_validation_propagation_and_rollback_evidence():
    incomplete = build_task_dns_cutover_readiness_plan(
        _plan(
            [
                _task(
                    "task-no-rollback",
                    title="Cut over www domain DNS",
                    description=(
                        "Lower TTL to 300, run staged validation with dig and curl, reserve a "
                        "DNS propagation window, check certificate renewal, and require owner approval."
                    ),
                )
            ]
        )
    )
    complete = build_task_dns_cutover_readiness_plan(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Cut over www domain DNS",
                    description=(
                        "Lower TTL to 300 before the DNS cutover. Run staged validation with dig, "
                        "curl, and TLS checks. Check certificate renewal and expiry. Reserve a DNS "
                        "propagation window, store rollback records for the previous CNAME, and "
                        "capture service owner approval."
                    ),
                )
            ]
        )
    )

    assert incomplete.records[0].readiness_level == "partial"
    assert "rollback_records" in incomplete.records[0].missing_safeguards
    assert complete.records[0].readiness_level == "strong"
    assert complete.records[0].missing_safeguards == ()
    assert complete.summary["readiness_counts"] == {"weak": 0, "partial": 0, "strong": 1}


def test_mx_txt_domain_and_route53_metadata_are_detected():
    result = analyze_task_dns_cutover_readiness(
        _plan(
            [
                _task(
                    "task-mail",
                    title="Verify customer domain email records",
                    description="Add MX and TXT record verification for the customer domain.",
                    metadata={
                        "route53": {"hosted_zone": "customer.example.com"},
                        "dns": {"records": ["SPF", "DKIM", "DMARC"]},
                    },
                    tags=["propagation monitoring", "TTL"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == (
        "dns",
        "mx",
        "txt",
        "domain",
        "route53",
        "propagation",
        "ttl",
    )
    assert record.missing_safeguards == (
        "ttl_lowering",
        "staged_validation",
        "propagation_window",
        "rollback_records",
        "owner_approval",
    )
    assert any("metadata.route53" in item for item in record.evidence)
    assert result.summary["affected_task_count"] == 1
    assert result.summary["signal_counts"]["txt"] == 1


def test_unrelated_tasks_are_suppressed_and_empty_summary_is_stable():
    result = build_task_dns_cutover_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="Adjust labels and helper text.",
                    files_or_modules=["src/blueprint/ui/dashboard_copy.py"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.affected_task_ids == ()
    assert result.no_signal_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary["task_count"] == 1
    assert result.summary["affected_task_count"] == 0
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "strong": 0}
    assert "No DNS, certificate, CDN, or traffic cutover tasks" in result.to_markdown()


def test_deterministic_serialization_markdown_no_mutation_and_aliases():
    task = _task(
        "task-pipes",
        title="DNS cutover | rollback",
        description=(
            "Cutover DNS traffic with lower TTL to 300, staged validation, propagation window, "
            "rollback records, and owner approval."
        ),
        metadata={
            "validation_commands": {
                "dns": ["dig app.example.com", "curl https://app.example.com/health"]
            }
        },
    )
    plan = _plan(
        [
            _task("task-z", title="Certificate renewal only", description="Renew TLS certificate."),
            task,
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_dns_cutover_readiness(plan)
    payload = task_dns_cutover_readiness_plan_to_dict(result)
    markdown = task_dns_cutover_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert list(payload) == [
        "plan_id",
        "records",
        "affected_task_ids",
        "no_signal_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "readiness_level",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "evidence",
        "recommended_acceptance_criteria",
    ]
    assert result.affected_task_ids == ("task-z", "task-pipes")
    assert [record.readiness_level for record in result.records] == ["weak", "partial"]
    assert markdown.startswith("# Task DNS Cutover Readiness Plan: plan-dns")
    assert "DNS cutover \\| rollback" in markdown


def test_execution_plan_model_and_object_like_task_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Update CDN TLS certificate",
        description=(
            "CDN TLS certificate renewal has lower TTL to 300, staged validation, "
            "propagation window, rollback records, and owner approval."
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

    first = build_task_dns_cutover_readiness_plan([object_task])
    second = build_task_dns_cutover_readiness_plan(plan_model)

    assert first.records[0].task_id == "task-object"
    assert "certificate" in first.records[0].detected_signals
    assert first.records[0].readiness_level == "strong"
    assert second.plan_id == "plan-model"
    assert second.records[0].task_id == "task-model"


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
