import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_cost_estimation import (
    PlanCostEstimationMatrix,
    PlanCostEstimationRow,
    build_plan_cost_estimation_matrix,
    generate_plan_cost_estimation_matrix,
    plan_cost_estimation_matrix_to_dict,
    plan_cost_estimation_matrix_to_dicts,
    plan_cost_estimation_matrix_to_markdown,
)


def test_complete_cost_estimation_plan_emits_ready_rows_with_owner_estimates_and_optimizations():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-cost-estimation",
                    title="Complete cost estimation for cloud infrastructure",
                    description=(
                        "Identify infrastructure costs including EC2 instances with reserved capacity, "
                        "track third-party SaaS services like Stripe and Datadog subscriptions, "
                        "include labor estimates for engineering team with hourly rates, "
                        "account for Oracle license fees and commercial licenses, "
                        "calculate operational overhead including monitoring cost and on-call support, "
                        "model resource scaling with auto-scaling for traffic growth, "
                        "estimate data transfer costs including egress and CDN bandwidth, "
                        "budget API call costs with per-call pricing for external services, "
                        "project storage growth for S3 and database storage with archival strategy, "
                        "cost enterprise support tier with premium SLA requirements."
                    ),
                    metadata={"finance_owner": "finops-team"},
                )
            ]
        )
    )

    assert isinstance(result, PlanCostEstimationMatrix)
    assert result.plan_id == "plan-cost"
    assert result.cost_estimation_task_ids == ("task-cost-estimation",)
    assert [row.category for row in result.rows] == [
        "infrastructure_costs_identified",
        "third_party_services_tracked",
        "labor_estimates_included",
        "licensing_fees_accounted",
        "operational_overhead_calculated",
        "resource_scaling_modeled",
        "data_transfer_costs_estimated",
        "api_calls_budgeted",
        "storage_growth_projected",
        "support_requirements_costed",
    ]
    assert all(isinstance(row, PlanCostEstimationRow) for row in result.rows)
    assert all(row.readiness == "ready" for row in result.rows)
    assert all(row.risk == "low" for row in result.rows)
    assert all(row.estimates for row in result.rows)
    assert all(row.owner == "finops-team" for row in result.rows)
    assert all(row.next_action == "Ready for cost estimation review." for row in result.rows)
    assert result.gap_categories == ()
    assert result.summary["readiness_counts"] == {"blocked": 0, "partial": 0, "ready": 10}
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 10}


def test_incomplete_cost_estimation_plan_marks_infrastructure_labor_scaling_as_high_gap():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-third-party",
                    title="Track third-party service costs",
                    description="Monitor SaaS subscription fees for external services.",
                    metadata={"budget_owner": "procurement"},
                )
            ]
        )
    )

    infrastructure = _row(result, "infrastructure_costs_identified")
    labor = _row(result, "labor_estimates_included")
    scaling = _row(result, "resource_scaling_modeled")
    licensing = _row(result, "licensing_fees_accounted")

    assert not infrastructure.estimates
    assert not labor.estimates
    assert not scaling.estimates
    assert not licensing.estimates
    assert infrastructure.risk == labor.risk == scaling.risk == "high"
    assert licensing.risk == "medium"
    assert all(row.readiness == "partial" for row in (infrastructure, labor, scaling, licensing))
    assert result.gap_categories == (
        "infrastructure_costs_identified",
        "labor_estimates_included",
        "licensing_fees_accounted",
        "operational_overhead_calculated",
        "resource_scaling_modeled",
        "data_transfer_costs_estimated",
        "api_calls_budgeted",
        "storage_growth_projected",
        "support_requirements_costed",
    )
    assert result.summary["risk_counts"]["high"] == 3
    assert "compute, storage, and network infrastructure" in infrastructure.next_action


def test_cost_estimation_detects_infrastructure_costs():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-infra",
                    title="Infrastructure cost estimation",
                    description="Estimate compute cost for EC2 instances, server cost for on-demand VMs, and cloud cost for GCP compute.",
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates
    assert any("description:" in est for est in row.estimates)
    assert row.readiness == "ready"
    assert row.risk == "low"


def test_cost_estimation_detects_third_party_services():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-saas",
                    title="SaaS cost tracking",
                    description="Track Stripe API service fees, Twilio per-user pricing, and Datadog subscription costs.",
                )
            ]
        )
    )

    row = _row(result, "third_party_services_tracked")
    assert row.estimates
    assert row.readiness == "ready"


def test_cost_estimation_detects_labor_estimates():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-labor",
                    title="Labor cost planning",
                    description="Include developer cost and engineering cost estimates, account for contractor hourly rate and FTE headcount.",
                )
            ]
        )
    )

    row = _row(result, "labor_estimates_included")
    assert row.estimates
    assert row.readiness == "ready"


def test_cost_estimation_detects_licensing_fees():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-licenses",
                    title="License fee accounting",
                    description="Account for Oracle license fees, Microsoft enterprise license, and commercial software licenses.",
                )
            ]
        )
    )

    row = _row(result, "licensing_fees_accounted")
    assert row.estimates
    assert row.readiness == "ready"


def test_cost_estimation_detects_operational_overhead():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-ops",
                    title="Operational overhead",
                    description="Calculate support cost, maintenance cost, incident response overhead, and on-call monitoring cost.",
                )
            ]
        )
    )

    row = _row(result, "operational_overhead_calculated")
    assert row.estimates
    assert row.readiness == "ready"


def test_cost_estimation_detects_resource_scaling():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-scaling",
                    title="Scaling model",
                    description="Model auto-scaling costs, horizontal scaling for traffic growth, and elasticity with capacity planning.",
                )
            ]
        )
    )

    row = _row(result, "resource_scaling_modeled")
    assert row.estimates
    assert row.readiness == "ready"


def test_cost_estimation_detects_data_transfer_costs():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-transfer",
                    title="Data transfer costs",
                    description="Estimate bandwidth cost, egress fees, cross-region transfer costs, and CDN CloudFront expenses.",
                )
            ]
        )
    )

    row = _row(result, "data_transfer_costs_estimated")
    assert row.estimates
    assert row.readiness == "ready"


def test_cost_estimation_detects_api_call_budgets():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-api-budget",
                    title="API call budgeting",
                    description="Budget for per-call pricing, request costs, usage-based pricing with metered billing.",
                )
            ]
        )
    )

    row = _row(result, "api_calls_budgeted")
    assert row.estimates
    assert row.readiness == "ready"


def test_cost_estimation_detects_storage_growth():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-storage",
                    title="Storage growth projection",
                    description="Project S3 storage growth, database storage costs, retention policies, and archival expenses.",
                )
            ]
        )
    )

    row = _row(result, "storage_growth_projected")
    assert row.estimates
    assert row.readiness == "ready"


def test_cost_estimation_detects_support_requirements():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-support",
                    title="Support tier costing",
                    description="Cost enterprise support tier, premium support with business SLA, and technical support contracts.",
                )
            ]
        )
    )

    row = _row(result, "support_requirements_costed")
    assert row.estimates
    assert row.readiness == "ready"


def test_cost_estimation_suggests_reserved_capacity_optimization():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-infra",
                    title="Infrastructure cost",
                    description="Estimate compute cost for EC2 instances and cloud servers.",
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.optimization_opportunities
    assert any("reserved capacity" in opt.lower() for opt in row.optimization_opportunities)


def test_cost_estimation_suggests_spot_instance_optimization():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-infra",
                    title="Infrastructure cost",
                    description="Estimate compute cost for cloud VMs and server instances.",
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.optimization_opportunities
    assert any("spot instance" in opt.lower() for opt in row.optimization_opportunities)


def test_cost_estimation_no_spot_suggestion_when_spot_mentioned():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-infra",
                    title="Infrastructure cost",
                    description="Estimate compute cost using spot instances for batch workloads.",
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert not any("spot instance" in opt.lower() for opt in row.optimization_opportunities)


def test_cost_estimation_suggests_auto_scaling_optimization():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-scaling",
                    title="Scaling model",
                    description="Model horizontal scaling and vertical scaling for capacity planning.",
                )
            ]
        )
    )

    row = _row(result, "resource_scaling_modeled")
    assert row.optimization_opportunities
    assert any("auto-scaling" in opt.lower() for opt in row.optimization_opportunities)


def test_cost_estimation_suggests_cdn_optimization():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-transfer",
                    title="Data transfer costs",
                    description="Estimate egress costs and bandwidth usage for cross-region transfer.",
                )
            ]
        )
    )

    row = _row(result, "data_transfer_costs_estimated")
    assert row.optimization_opportunities
    assert any("cdn" in opt.lower() for opt in row.optimization_opportunities)


def test_cost_estimation_suggests_caching_optimization():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    title="API budget",
                    description="Budget for per-call pricing and API request costs.",
                )
            ]
        )
    )

    row = _row(result, "api_calls_budgeted")
    assert row.optimization_opportunities
    assert any("caching" in opt.lower() for opt in row.optimization_opportunities)


def test_cost_estimation_suggests_tiering_optimization():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-storage",
                    title="Storage growth",
                    description="Project database storage and S3 growth over time.",
                )
            ]
        )
    )

    row = _row(result, "storage_growth_projected")
    assert row.optimization_opportunities
    assert any("tiering" in opt.lower() for opt in row.optimization_opportunities)


def test_cost_estimation_suggests_volume_discount_optimization():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-saas",
                    title="SaaS tracking",
                    description="Track Stripe and Datadog subscription costs.",
                )
            ]
        )
    )

    row = _row(result, "third_party_services_tracked")
    assert row.optimization_opportunities
    assert any("volume discount" in opt.lower() for opt in row.optimization_opportunities)


def test_cost_estimation_tracks_multiple_tasks():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-infra-1",
                    title="EC2 cost estimation",
                    description="Estimate EC2 compute costs.",
                ),
                _task(
                    "task-infra-2",
                    title="Storage cost estimation",
                    description="Estimate cloud storage costs.",
                ),
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert "task-infra-1" in row.task_ids
    assert "task-infra-2" in row.task_ids


def test_cost_estimation_uses_default_owners():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-infra",
                    title="Infrastructure cost",
                    description="Estimate compute cost.",
                )
            ]
        )
    )

    assert _row(result, "infrastructure_costs_identified").owner == "infrastructure_owner"
    assert _row(result, "third_party_services_tracked").owner == "procurement_owner"
    assert _row(result, "labor_estimates_included").owner == "engineering_manager"
    assert _row(result, "licensing_fees_accounted").owner == "procurement_owner"
    assert _row(result, "operational_overhead_calculated").owner == "sre_owner"
    assert _row(result, "resource_scaling_modeled").owner == "infrastructure_owner"
    assert _row(result, "data_transfer_costs_estimated").owner == "infrastructure_owner"
    assert _row(result, "api_calls_budgeted").owner == "finops_owner"
    assert _row(result, "storage_growth_projected").owner == "infrastructure_owner"
    assert _row(result, "support_requirements_costed").owner == "customer_success_owner"


def test_cost_estimation_uses_metadata_owner():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-cost",
                    title="Cost estimation",
                    description="Estimate all infrastructure costs.",
                    metadata={"finance_owner": "cost-team"},
                )
            ]
        )
    )

    assert all(row.owner == "cost-team" for row in result.rows)


def test_cost_estimation_deduplicates_estimates():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-1",
                    title="EC2 cost estimation",
                    description="Estimate EC2 compute cost.",
                ),
                _task(
                    "task-2",
                    title="EC2 cost estimation duplicate",
                    description="Estimate EC2 compute cost.",
                ),
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    estimate_texts = [est for est in row.estimates]
    assert len(estimate_texts) == len(set(estimate_texts))


def test_cost_estimation_handles_acceptance_criteria():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-infra",
                    title="Infrastructure planning",
                    description="Plan infrastructure.",
                    acceptance_criteria=["Infrastructure costs identified for all EC2 instances."],
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates
    assert any("acceptance_criteria" in est for est in row.estimates)


def test_cost_estimation_handles_metadata_fields():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-budget",
                    title="Budget planning",
                    description="Plan budget.",
                    metadata={"budget_notes": "Include compute cost for reserved instances."},
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates


def test_cost_estimation_no_rows_when_no_cost_signals():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-feature",
                    title="Add user authentication",
                    description="Implement login flow with OAuth.",
                )
            ]
        )
    )

    assert result.rows == ()
    assert result.cost_estimation_task_ids == ()
    assert result.gap_categories == ()
    assert result.summary["category_count"] == 0


def test_cost_estimation_calculates_scoring_metrics():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Complete cost estimation",
                    description=(
                        "Estimate infrastructure costs with EC2 reserved instances, "
                        "track third-party SaaS services, include labor costs, "
                        "account for licensing fees, calculate operational overhead, "
                        "model auto-scaling costs, estimate egress bandwidth, "
                        "budget API calls with caching, project storage tiering, "
                        "cost enterprise support."
                    ),
                )
            ]
        )
    )

    scoring = result.summary["scoring"]
    assert "accuracy" in scoring
    assert "completeness" in scoring
    assert "optimization_opportunities" in scoring
    assert "assumptions_clarity" in scoring
    assert "overall" in scoring
    assert scoring["accuracy"] == 30.0
    assert scoring["optimization_opportunities"] >= 15.0  # Not all categories have optimization opportunities


def test_cost_estimation_partial_completeness_scoring():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Partial cost estimation",
                    description="Estimate infrastructure compute cost.",
                )
            ]
        )
    )

    scoring = result.summary["scoring"]
    assert 0 < scoring["completeness"] < 25.0
    assert scoring["assumptions_clarity"] == 0.0


def test_cost_estimation_zero_scoring_when_no_estimates():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-budget",
                    title="Budget planning",
                    description="Plan project budget without specifics.",
                )
            ]
        )
    )

    scoring = result.summary["scoring"]
    assert scoring["accuracy"] == 0.0
    assert scoring["completeness"] == 0.0
    assert scoring["optimization_opportunities"] == 0.0
    assert scoring["assumptions_clarity"] == 0.0
    assert scoring["overall"] == 0.0


def test_cost_estimation_matrix_to_dict():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-cost",
                    title="Cost estimation",
                    description="Estimate infrastructure costs.",
                )
            ]
        )
    )

    data = plan_cost_estimation_matrix_to_dict(result)
    assert isinstance(data, dict)
    assert data["plan_id"] == "plan-cost"
    assert "rows" in data
    assert "records" in data
    assert data["rows"] == data["records"]
    assert "cost_estimation_task_ids" in data
    assert "gap_categories" in data
    assert "summary" in data


def test_cost_estimation_matrix_to_dicts_from_matrix():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-cost",
                    title="Cost estimation",
                    description="Estimate compute cost.",
                )
            ]
        )
    )

    dicts = plan_cost_estimation_matrix_to_dicts(result)
    assert isinstance(dicts, list)
    assert all(isinstance(d, dict) for d in dicts)
    assert len(dicts) == 10


def test_cost_estimation_matrix_to_dicts_from_rows():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-cost",
                    title="Cost estimation",
                    description="Estimate compute cost.",
                )
            ]
        )
    )

    dicts = plan_cost_estimation_matrix_to_dicts(result.rows)
    assert isinstance(dicts, list)
    assert all(isinstance(d, dict) for d in dicts)


def test_cost_estimation_matrix_to_markdown():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-cost",
                    title="Cost estimation",
                    description="Estimate infrastructure cost with EC2 reserved capacity.",
                )
            ]
        )
    )

    markdown = plan_cost_estimation_matrix_to_markdown(result)
    assert isinstance(markdown, str)
    assert "# Plan Cost Estimation Matrix: plan-cost" in markdown
    assert "| Category | Owner | Readiness | Risk |" in markdown
    assert "infrastructure_costs_identified" in markdown
    assert "reserved" in markdown.lower()


def test_cost_estimation_markdown_empty_matrix():
    result = build_plan_cost_estimation_matrix(_plan([]))

    markdown = plan_cost_estimation_matrix_to_markdown(result)
    assert "No cost estimation rows were inferred." in markdown


def test_cost_estimation_row_to_dict():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-cost",
                    title="Cost estimation",
                    description="Estimate compute cost.",
                )
            ]
        )
    )

    row = result.rows[0]
    data = row.to_dict()
    assert isinstance(data, dict)
    assert "category" in data
    assert "owner" in data
    assert "estimates" in data
    assert "assumptions" in data
    assert "optimization_opportunities" in data
    assert "readiness" in data
    assert "risk" in data
    assert "next_action" in data
    assert "task_ids" in data


def test_cost_estimation_handles_execution_plan_object():
    plan_dict = _plan(
        [
            _task(
                "task-cost",
                title="Cost estimation",
                description="Estimate compute cost.",
            )
        ]
    )
    plan = ExecutionPlan.model_validate(plan_dict)

    result = build_plan_cost_estimation_matrix(plan)
    assert result.plan_id == "plan-cost"
    assert result.cost_estimation_task_ids == ("task-cost",)


def test_cost_estimation_handles_task_list():
    tasks = [
        _task(
            "task-1",
            title="Infrastructure cost",
            description="Estimate EC2 cost.",
        ),
        _task(
            "task-2",
            title="SaaS cost",
            description="Track Stripe fees.",
        ),
    ]

    result = build_plan_cost_estimation_matrix(tasks)
    assert result.plan_id is None
    assert len(result.cost_estimation_task_ids) == 2


def test_cost_estimation_does_not_mutate_input():
    plan = _plan(
        [
            _task(
                "task-cost",
                title="Cost estimation",
                description="Estimate compute cost.",
            )
        ]
    )
    original = copy.deepcopy(plan)

    build_plan_cost_estimation_matrix(plan)

    assert plan == original


def test_generate_plan_cost_estimation_matrix_alias():
    result = generate_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-cost",
                    title="Cost estimation",
                    description="Estimate compute cost.",
                )
            ]
        )
    )

    assert isinstance(result, PlanCostEstimationMatrix)


def test_cost_estimation_summary_counts():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-infra",
                    title="Infrastructure cost",
                    description="Estimate compute cost for EC2.",
                ),
                _task(
                    "task-saas",
                    title="SaaS tracking",
                    description="Track third-party subscriptions.",
                ),
            ]
        )
    )

    summary = result.summary
    assert summary["task_count"] == 2
    assert summary["category_count"] == 10
    assert summary["ready_category_count"] == 2
    assert summary["gap_category_count"] == 8
    assert summary["cost_estimation_task_count"] == 2


def test_cost_estimation_records_property():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-cost",
                    title="Cost estimation",
                    description="Estimate compute cost.",
                )
            ]
        )
    )

    assert result.records == result.rows


def test_cost_estimation_detects_reserved_instance_terms():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-infra",
                    title="Infrastructure planning",
                    description="Use reserved instances for predictable workloads.",
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates
    assert not any("reserved capacity" in opt.lower() for opt in row.optimization_opportunities)


def test_cost_estimation_detects_spot_pricing():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-infra",
                    title="Cost optimization",
                    description="Leverage spot pricing for batch jobs.",
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates
    assert not any("spot instance" in opt.lower() for opt in row.optimization_opportunities)


def test_cost_estimation_detects_volume_discount():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-saas",
                    title="Vendor negotiation",
                    description="Negotiate volume discounts with Stripe for high transaction volumes.",
                )
            ]
        )
    )

    row = _row(result, "third_party_services_tracked")
    assert row.estimates
    assert not any("volume discount" in opt.lower() for opt in row.optimization_opportunities)


def test_cost_estimation_handles_case_insensitive_patterns():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-mixed",
                    title="COST ESTIMATION",
                    description="Estimate COMPUTE COST for EC2 INSTANCES.",
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates
    assert row.readiness == "ready"


def test_cost_estimation_handles_tags_field():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-tagged",
                    title="Tagged task",
                    description="Basic task",
                    tags=["infrastructure cost", "compute budget"],
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates


def test_cost_estimation_handles_notes_field():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-notes",
                    title="Task with notes",
                    description="Basic task",
                    notes=["Consider reserved capacity for stable workloads"],
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates


def test_cost_estimation_truncates_long_estimates():
    long_description = "Estimate compute cost for EC2 instances " + ("with additional details " * 30)
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-long",
                    title="Long description task",
                    description=long_description,
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates
    assert all(len(est) <= 200 for est in row.estimates)
    assert any(est.endswith("...") for est in row.estimates)


def test_cost_estimation_json_serialization():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-cost",
                    title="Cost estimation",
                    description="Estimate infrastructure cost with reserved instances.",
                )
            ]
        )
    )

    json_str = json.dumps(result.to_dict())
    data = json.loads(json_str)
    assert data["plan_id"] == "plan-cost"
    assert len(data["rows"]) == 10


def _plan(tasks):
    return {
        "id": "plan-cost",
        "implementation_brief_id": "ib-cost",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "web_service",
        "milestones": [
            {"name": "Planning", "description": "Cost planning phase"},
            {"name": "Implementation", "description": "Implementation phase"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Estimate costs",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": tasks,
    }


def _task(task_id, title, *, description="", acceptance_criteria=None, metadata=None, tags=None, notes=None):
    task_dict = {
        "id": task_id,
        "title": title,
        "description": description,
        "milestone": "Planning",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": acceptance_criteria or [],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        task_dict["tags"] = tags
    if notes is not None:
        task_dict["notes"] = notes
    return task_dict


def test_cost_estimation_detects_ec2_infrastructure():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-ec2",
                    title="EC2 deployment",
                    description="Deploy application on EC2 instances.",
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates


def test_cost_estimation_detects_gcp_compute():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-gcp",
                    title="GCP deployment",
                    description="Deploy using GCP compute engine.",
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates


def test_cost_estimation_detects_azure_vm():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-azure",
                    title="Azure deployment",
                    description="Deploy on Azure VM instances.",
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates


def test_cost_estimation_detects_on_demand_instances():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-on-demand",
                    title="On-demand infrastructure",
                    description="Use on-demand instances for variable workloads.",
                )
            ]
        )
    )

    row = _row(result, "infrastructure_costs_identified")
    assert row.estimates


def test_cost_estimation_detects_stripe_service():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-stripe",
                    title="Stripe integration",
                    description="Integrate Stripe payment processing.",
                )
            ]
        )
    )

    row = _row(result, "third_party_services_tracked")
    assert row.estimates


def test_cost_estimation_detects_twilio_service():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-twilio",
                    title="SMS notifications",
                    description="Send SMS via Twilio service.",
                )
            ]
        )
    )

    row = _row(result, "third_party_services_tracked")
    assert row.estimates


def test_cost_estimation_detects_datadog_monitoring():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-datadog",
                    title="Monitoring setup",
                    description="Set up Datadog monitoring and alerting.",
                )
            ]
        )
    )

    row = _row(result, "third_party_services_tracked")
    assert row.estimates


def test_cost_estimation_detects_headcount_labor():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-headcount",
                    title="Team planning",
                    description="Plan headcount for engineering team expansion.",
                )
            ]
        )
    )

    row = _row(result, "labor_estimates_included")
    assert row.estimates


def test_cost_estimation_detects_fte_labor():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-fte",
                    title="FTE planning",
                    description="Calculate FTE requirements for the project.",
                )
            ]
        )
    )

    row = _row(result, "labor_estimates_included")
    assert row.estimates


def test_cost_estimation_detects_contractor_labor():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-contractor",
                    title="Contractor engagement",
                    description="Engage contractor for implementation work.",
                )
            ]
        )
    )

    row = _row(result, "labor_estimates_included")
    assert row.estimates


def test_cost_estimation_detects_perpetual_license():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-license",
                    title="Software licensing",
                    description="Purchase perpetual license for database software.",
                )
            ]
        )
    )

    row = _row(result, "licensing_fees_accounted")
    assert row.estimates


def test_cost_estimation_detects_enterprise_license():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-enterprise",
                    title="Enterprise licensing",
                    description="Upgrade to enterprise license for advanced features.",
                )
            ]
        )
    )

    row = _row(result, "licensing_fees_accounted")
    assert row.estimates


def test_cost_estimation_detects_monitoring_overhead():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-monitoring",
                    title="Monitoring cost",
                    description="Account for monitoring cost and logging infrastructure.",
                )
            ]
        )
    )

    row = _row(result, "operational_overhead_calculated")
    assert row.estimates


def test_cost_estimation_detects_incident_response():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-incident",
                    title="Incident response",
                    description="Set up incident response team and on-call rotation.",
                )
            ]
        )
    )

    row = _row(result, "operational_overhead_calculated")
    assert row.estimates


def test_cost_estimation_detects_horizontal_scaling():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-horizontal",
                    title="Horizontal scaling",
                    description="Implement horizontal scaling for web tier.",
                )
            ]
        )
    )

    row = _row(result, "resource_scaling_modeled")
    assert row.estimates


def test_cost_estimation_detects_capacity_planning():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-capacity",
                    title="Capacity planning",
                    description="Perform capacity planning for expected traffic growth.",
                )
            ]
        )
    )

    row = _row(result, "resource_scaling_modeled")
    assert row.estimates


def test_cost_estimation_detects_egress_bandwidth():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-egress",
                    title="Egress optimization",
                    description="Optimize egress bandwidth for cost efficiency.",
                )
            ]
        )
    )

    row = _row(result, "data_transfer_costs_estimated")
    assert row.estimates


def test_cost_estimation_detects_cross_region_transfer():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-cross-region",
                    title="Multi-region setup",
                    description="Set up cross-region transfer for disaster recovery.",
                )
            ]
        )
    )

    row = _row(result, "data_transfer_costs_estimated")
    assert row.estimates


def test_cost_estimation_detects_metered_billing():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-metered",
                    title="Metered billing",
                    description="Implement metered billing for API usage.",
                )
            ]
        )
    )

    row = _row(result, "api_calls_budgeted")
    assert row.estimates


def test_cost_estimation_detects_usage_based_pricing():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-usage",
                    title="Usage-based pricing",
                    description="Set up usage-based pricing model for customers.",
                )
            ]
        )
    )

    row = _row(result, "api_calls_budgeted")
    assert row.estimates


def test_cost_estimation_detects_s3_storage():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-s3",
                    title="S3 storage",
                    description="Use S3 cost for object storage and backups.",
                )
            ]
        )
    )

    row = _row(result, "storage_growth_projected")
    assert row.estimates


def test_cost_estimation_detects_retention_policy():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-retention",
                    title="Data retention",
                    description="Implement retention cost policy for compliance.",
                )
            ]
        )
    )

    row = _row(result, "storage_growth_projected")
    assert row.estimates


def test_cost_estimation_detects_premium_support():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-premium",
                    title="Premium support",
                    description="Upgrade to premium support tier for critical systems.",
                )
            ]
        )
    )

    row = _row(result, "support_requirements_costed")
    assert row.estimates


def test_cost_estimation_detects_business_sla():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-sla",
                    title="SLA upgrade",
                    description="Purchase business SLA for guaranteed uptime.",
                )
            ]
        )
    )

    row = _row(result, "support_requirements_costed")
    assert row.estimates


def test_cost_estimation_handles_empty_plan():
    result = build_plan_cost_estimation_matrix(_plan([]))

    assert result.rows == ()
    assert result.cost_estimation_task_ids == ()
    assert result.summary["category_count"] == 0


def test_cost_estimation_handles_single_task():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-single",
                    title="Single infrastructure task",
                    description="Deploy single EC2 instance.",
                )
            ]
        )
    )

    assert result.cost_estimation_task_ids == ("task-single",)
    assert result.summary["task_count"] == 1


def test_cost_estimation_multiple_categories_single_task():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-multi",
                    title="Multi-category cost",
                    description="Estimate EC2 costs, Stripe fees, and labor estimates.",
                )
            ]
        )
    )

    infra = _row(result, "infrastructure_costs_identified")
    saas = _row(result, "third_party_services_tracked")
    labor = _row(result, "labor_estimates_included")

    assert infra.task_ids == ("task-multi",)
    assert saas.task_ids == ("task-multi",)
    assert labor.task_ids == ("task-multi",)


def test_cost_estimation_risk_levels():
    result = build_plan_cost_estimation_matrix(
        _plan(
            [
                _task(
                    "task-infra",
                    title="Infrastructure",
                    description="EC2 deployment.",
                ),
                _task(
                    "task-docs",
                    title="Documentation",
                    description="Write documentation.",
                ),
            ]
        )
    )

    # Infrastructure has estimates, so low risk
    assert _row(result, "infrastructure_costs_identified").risk == "low"
    # Labor has no estimates and is high-gap category, so high risk
    assert _row(result, "labor_estimates_included").risk == "high"
    # Third-party has no estimates and is not high-gap, so medium risk
    assert _row(result, "third_party_services_tracked").risk == "medium"


def _row(matrix, category):
    for row in matrix.rows:
        if row.category == category:
            return row
    raise ValueError(f"Category {category} not found in matrix rows")
