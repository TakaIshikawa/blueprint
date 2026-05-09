from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_frontend_rendering_strategy import (
    TaskFrontendRenderingStrategyFinding,
    TaskFrontendRenderingStrategyPlan,
    analyze_task_frontend_rendering_strategy,
    build_task_frontend_rendering_strategy_plan,
    extract_task_frontend_rendering_strategy,
    generate_task_frontend_rendering_strategy,
    recommend_task_frontend_rendering_strategy,
    summarize_task_frontend_rendering_strategy,
    task_frontend_rendering_strategy_plan_to_dict,
    task_frontend_rendering_strategy_plan_to_dicts,
)


def test_ready_frontend_rendering_task_has_no_recommended_checks():
    result = analyze_task_frontend_rendering_strategy(
        _plan(
            [
                _task(
                    "task-nextjs-ssr",
                    title="Implement Next.js SSR with ISR for product pages",
                    description=(
                        "Build server-side rendered product pages using Next.js with getServerSideProps. "
                        "Implement incremental static regeneration with revalidate intervals for dynamic content. "
                        "Use progressive hydration for interactive components and streaming SSR for improved TTFB. "
                        "Configure data fetching pattern with parallel queries using React Query. "
                        "Apply bundle splitting and code splitting with dynamic imports for route-based chunking. "
                        "Set up lazy loading for below-the-fold images using intersection observer. "
                        "Monitor performance with Web Vitals, targeting LCP < 2.5s and TTI < 3.5s. "
                        "Optimize SEO with meta tags, Open Graph tags, structured data, and XML sitemap. "
                        "Define time-to-interactive target of 3 seconds and enforce performance budgets."
                    ),
                    files_or_modules=[
                        "src/pages/products/[id].tsx",
                        "src/components/ProductHydration.tsx",
                        "src/lib/performance-monitoring.ts",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskFrontendRenderingStrategyPlan)
    assert result.plan_id == "plan-frontend"
    assert result.impacted_task_ids == ("task-nextjs-ssr",)
    finding = result.findings[0]
    assert isinstance(finding, TaskFrontendRenderingStrategyFinding)
    assert finding.detected_signals == (
        "ssr",
        "isr",
        "hydration",
        "data_fetching",
        "bundle_optimization",
        "performance_budget",
        "seo",
    )
    assert finding.present_strategies == (
        "server_side_rendering",
        "incremental_static_regeneration",
        "hydration_strategy",
        "data_fetching_pattern",
        "bundle_splitting",
        "code_splitting",
        "lazy_loading",
        "performance_monitoring",
        "seo_optimization",
        "time_to_interactive_target",
    )
    assert finding.missing_strategies == ("client_side_rendering", "static_site_generation")
    assert finding.recommended_checks == (
        "Specify client-side rendering approach, initial bundle size limits, and JavaScript execution strategy.",
        "Identify static generation paths, build-time data fetching, and fallback for dynamic routes.",
    )
    assert finding.readiness == "ready"
    assert "files_or_modules: src/pages/products/[id].tsx" in finding.evidence
    assert result.summary["impacted_task_count"] == 1
    assert result.summary["missing_strategy_count"] == 2
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "ready": 1}


def test_partial_rendering_task_reports_specific_recommended_checks():
    result = build_task_frontend_rendering_strategy_plan(
        _plan(
            [
                _task(
                    "task-partial-ssr",
                    title="Add SSR for homepage",
                    description=(
                        "Implement server-side rendering for homepage using Next.js getServerSideProps. "
                        "Render page content on the server and send HTML to client."
                    ),
                    files_or_modules=["src/pages/index.tsx"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-partial-ssr"
    # getServerSideProps detects both SSR and data_fetching signals
    assert finding.detected_signals == ("ssr", "data_fetching")
    assert "server_side_rendering" in finding.present_strategies
    assert "data_fetching_pattern" in finding.present_strategies
    assert "hydration_strategy" in finding.missing_strategies
    assert "performance_monitoring" in finding.missing_strategies
    assert finding.readiness == "weak"
    assert len(finding.recommended_checks) > 7
    assert any("hydration" in check for check in finding.recommended_checks)
    assert result.summary["present_strategy_counts"]["server_side_rendering"] == 1
    assert result.summary["present_strategy_counts"]["data_fetching_pattern"] == 1


def test_path_hints_contribute_to_detection():
    result = build_task_frontend_rendering_strategy_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Set up frontend rendering",
                    description="Configure rendering pipeline with performance monitoring and SEO.",
                    files_or_modules=[
                        "src/ssr/server-rendering.ts",
                        "src/hydration/progressive-hydration.tsx",
                        "src/performance/web-vitals.ts",
                        "src/seo/meta-tags.ts",
                        "src/bundle/code-splitting.ts",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert {"ssr", "hydration", "bundle_optimization", "performance_budget", "seo"} <= set(finding.detected_signals)
    assert "files_or_modules: src/ssr/server-rendering.ts" in finding.evidence
    assert "files_or_modules: src/hydration/progressive-hydration.tsx" in finding.evidence
    assert "hydration_strategy" in finding.present_strategies
    assert "performance_monitoring" in finding.present_strategies
    assert "seo_optimization" in finding.present_strategies


def test_unrelated_and_explicit_no_impact_tasks_are_not_applicable():
    result = build_task_frontend_rendering_strategy_plan(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update API docs",
                    description="Improve API endpoint documentation.",
                    files_or_modules=["docs/api.md"],
                ),
                _task(
                    "task-no-frontend",
                    title="Backend service",
                    description="This task has no frontend rendering or UI scope. Backend changes only.",
                    files_or_modules=["src/services/backend.py"],
                ),
            ]
        )
    )

    assert result.impacted_task_ids == ()
    assert result.not_applicable_task_ids == ("task-docs", "task-no-frontend")
    assert result.findings == ()
    assert result.summary["impacted_task_count"] == 0


def test_csr_spa_task_detects_client_side_rendering():
    result = analyze_task_frontend_rendering_strategy(
        _task(
            "task-spa",
            title="Build React SPA",
            description=(
                "Create single-page application with client-side rendering using React. "
                "Implement code splitting with React.lazy and bundle optimization with webpack. "
                "Monitor performance with Lighthouse and track Core Web Vitals."
            ),
            files_or_modules=["src/app/App.tsx", "webpack.config.js"],
        )
    )

    finding = result.findings[0]
    assert "csr" in finding.detected_signals
    assert "bundle_optimization" in finding.detected_signals
    assert "performance_budget" in finding.detected_signals
    assert "client_side_rendering" in finding.present_strategies
    assert "code_splitting" in finding.present_strategies
    assert "performance_monitoring" in finding.present_strategies


def test_ssg_jamstack_task_detects_static_generation():
    result = build_task_frontend_rendering_strategy_plan(
        [
            _task(
                "task-ssg",
                title="Generate static blog pages",
                description=(
                    "Use static site generation with getStaticProps and getStaticPaths for blog posts. "
                    "Pre-render all blog pages at build time with Jamstack approach. "
                    "Optimize SEO with meta tags and canonical URLs."
                ),
                files_or_modules=["src/pages/blog/[slug].tsx"],
            )
        ]
    )

    finding = result.findings[0]
    assert "ssg" in finding.detected_signals
    assert "seo" in finding.detected_signals
    assert "static_site_generation" in finding.present_strategies
    assert "seo_optimization" in finding.present_strategies


def test_progressive_enhancement_with_islands_architecture():
    result = analyze_task_frontend_rendering_strategy(
        _task(
            "task-islands",
            title="Implement islands architecture",
            description=(
                "Use islands architecture with selective hydration for interactive components. "
                "Static HTML with progressive enhancement and partial hydration for dynamic widgets. "
                "Lazy load interactive islands below the fold."
            ),
            files_or_modules=["src/islands/InteractiveWidget.tsx"],
        )
    )

    finding = result.findings[0]
    assert "hydration" in finding.detected_signals
    assert "bundle_optimization" in finding.detected_signals
    assert "hydration_strategy" in finding.present_strategies
    assert "lazy_loading" in finding.present_strategies


def test_streaming_ssr_with_suspense():
    result = build_task_frontend_rendering_strategy_plan(
        [
            _task(
                "task-streaming",
                title="Implement streaming SSR",
                description=(
                    "Use streaming SSR with React 18 Suspense to improve TTFB and time to interactive. "
                    "Stream HTML to client as components resolve, enabling progressive hydration. "
                    "Monitor performance with Web Vitals and set TTI target under 3 seconds."
                ),
                files_or_modules=["src/app/StreamingPage.tsx"],
            )
        ]
    )

    finding = result.findings[0]
    assert "ssr" in finding.detected_signals
    assert "hydration" in finding.detected_signals
    assert "performance_budget" in finding.detected_signals
    assert "server_side_rendering" in finding.present_strategies
    assert "hydration_strategy" in finding.present_strategies
    assert "performance_monitoring" in finding.present_strategies
    assert "time_to_interactive_target" in finding.present_strategies


def test_remix_loader_pattern_detection():
    result = analyze_task_frontend_rendering_strategy(
        _task(
            "task-remix",
            title="Migrate to Remix framework",
            description=(
                "Implement Remix with loader functions for server-side data fetching. "
                "Use nested routes with loader pattern for parallel data loading. "
                "Apply bundle splitting and optimize for Web Vitals."
            ),
            files_or_modules=["app/routes/products.$id.tsx"],
        )
    )

    finding = result.findings[0]
    assert "ssr" in finding.detected_signals
    assert "data_fetching" in finding.detected_signals
    assert "bundle_optimization" in finding.detected_signals
    assert "server_side_rendering" in finding.present_strategies
    assert "data_fetching_pattern" in finding.present_strategies
    assert "bundle_splitting" in finding.present_strategies


def test_weak_readiness_for_missing_critical_strategies():
    result = build_task_frontend_rendering_strategy_plan(
        [
            _task(
                "task-weak",
                title="Add SSR without strategy",
                description="Implement server-side rendering for pages.",
                files_or_modules=["src/pages/ssr.tsx"],
            )
        ]
    )

    finding = result.findings[0]
    assert finding.readiness == "weak"
    assert "hydration_strategy" in finding.missing_strategies
    assert "data_fetching_pattern" in finding.missing_strategies
    assert "performance_monitoring" in finding.missing_strategies
    assert len(finding.missing_strategies) >= 8


def test_acceptance_criteria_contributes_to_detection():
    result = analyze_task_frontend_rendering_strategy(
        _task(
            "task-ac",
            title="Frontend rendering",
            description="Build rendering pipeline.",
            acceptance_criteria=[
                "Server-side rendering with getServerSideProps",
                "Progressive hydration for interactive components",
                "Code splitting with dynamic imports",
                "LCP under 2.5 seconds measured with Lighthouse",
                "SEO meta tags and Open Graph tags present",
            ],
            files_or_modules=["src/pages/index.tsx"],
        )
    )

    finding = result.findings[0]
    assert "ssr" in finding.detected_signals
    assert "hydration" in finding.detected_signals
    assert "bundle_optimization" in finding.detected_signals
    assert "performance_budget" in finding.detected_signals
    assert "seo" in finding.detected_signals


def test_metadata_rendering_hints():
    result = build_task_frontend_rendering_strategy_plan(
        [
            _task(
                "task-meta",
                title="Frontend task",
                description="Rendering work.",
                metadata={
                    "rendering_approach": "SSR with ISR",
                    "hydration_strategy": "progressive",
                    "performance_target": "TTI < 3s",
                    "seo_requirements": "meta tags and sitemap",
                },
                files_or_modules=["src/app.tsx"],
            )
        ]
    )

    finding = result.findings[0]
    assert "ssr" in finding.detected_signals
    assert "isr" in finding.detected_signals
    assert "hydration" in finding.detected_signals
    assert "performance_budget" in finding.detected_signals
    assert "seo" in finding.detected_signals


def test_pydantic_execution_task_source():
    task = ExecutionTask(
        id="task-pydantic-ssr",
        title="SSR with hydration",
        description=(
            "Implement server-side rendering with progressive hydration and performance monitoring. "
            "Target time-to-interactive under 3 seconds."
        ),
        files_or_modules=["src/pages/product.tsx"],
        acceptance_criteria=[],
    )
    result = analyze_task_frontend_rendering_strategy(task)

    finding = result.findings[0]
    assert finding.task_id == "task-pydantic-ssr"
    assert "ssr" in finding.detected_signals
    assert "hydration" in finding.detected_signals
    assert "performance_budget" in finding.detected_signals


def test_pydantic_execution_plan_source():
    plan = ExecutionPlan(
        id="plan-pydantic-rendering",
        implementation_brief_id="brief-1",
        milestones=[],
        tasks=[
            ExecutionTask(
                id="task-1",
                title="SSR setup",
                description="Server-side rendering with Next.js and getServerSideProps.",
                acceptance_criteria=[],
            ),
            ExecutionTask(
                id="task-2",
                title="Backend API",
                description="No frontend impact.",
                acceptance_criteria=[],
            ),
        ],
    )
    result = build_task_frontend_rendering_strategy_plan(plan)

    assert result.plan_id == "plan-pydantic-rendering"
    assert result.impacted_task_ids == ("task-1",)
    assert result.not_applicable_task_ids == ("task-2",)
    assert len(result.findings) == 1


def test_list_of_tasks_source():
    tasks = [
        {"id": "task-a", "title": "CSR app", "description": "Client-side rendering with React SPA."},
        {"id": "task-b", "title": "SSG blog", "description": "Static site generation with getStaticProps."},
    ]
    result = analyze_task_frontend_rendering_strategy(tasks)

    assert len(result.findings) == 2
    assert result.impacted_task_ids == ("task-a", "task-b")


def test_simple_namespace_task():
    task = SimpleNamespace(
        id="task-ns-ssr",
        title="Namespace SSR",
        description="Server-side rendering with hydration strategy and performance monitoring.",
        files_or_modules=["src/pages/index.tsx"],
    )
    result = build_task_frontend_rendering_strategy_plan(task)

    finding = result.findings[0]
    assert finding.task_id == "task-ns-ssr"
    assert "ssr" in finding.detected_signals
    assert "hydration" in finding.detected_signals


def test_to_dict_serialization():
    result = analyze_task_frontend_rendering_strategy(
        _task(
            "task-dict",
            title="SSR task",
            description="Server-side rendering with getServerSideProps.",
        )
    )

    result_dict = result.to_dict()
    assert result_dict["plan_id"] is None
    assert result_dict["impacted_task_ids"] == ["task-dict"]
    assert isinstance(result_dict["findings"], list)
    assert result_dict["findings"][0]["task_id"] == "task-dict"

    finding_dict = result.findings[0].to_dict()
    assert finding_dict["task_id"] == "task-dict"
    assert isinstance(finding_dict["detected_signals"], list)
    assert isinstance(finding_dict["recommended_checks"], list)


def test_plan_to_dict_helper():
    result = build_task_frontend_rendering_strategy_plan(
        _task("task-helper", title="SSR", description="Server-side rendering.")
    )

    result_dict = task_frontend_rendering_strategy_plan_to_dict(result)
    assert result_dict["impacted_task_ids"] == ["task-helper"]

    findings_dicts = task_frontend_rendering_strategy_plan_to_dicts(result)
    assert len(findings_dicts) == 1
    assert findings_dicts[0]["task_id"] == "task-helper"


def test_plan_to_dicts_from_iterable():
    findings = [
        TaskFrontendRenderingStrategyFinding(
            task_id="task-iter-1",
            title="SSR task",
            detected_signals=("ssr",),
            present_strategies=("server_side_rendering",),
            missing_strategies=(),
            readiness="partial",
        ),
        TaskFrontendRenderingStrategyFinding(
            task_id="task-iter-2",
            title="CSR task",
            detected_signals=("csr",),
            present_strategies=("client_side_rendering",),
            missing_strategies=(),
            readiness="partial",
        ),
    ]

    findings_dicts = task_frontend_rendering_strategy_plan_to_dicts(findings)
    assert len(findings_dicts) == 2
    assert findings_dicts[0]["task_id"] == "task-iter-1"
    assert findings_dicts[1]["task_id"] == "task-iter-2"


def test_compatibility_aliases():
    task = _task("task-alias", title="SSR", description="Server-side rendering with Next.js.")

    result1 = build_task_frontend_rendering_strategy_plan(task)
    result2 = analyze_task_frontend_rendering_strategy(task)
    result3 = summarize_task_frontend_rendering_strategy(task)
    result4 = extract_task_frontend_rendering_strategy(task)
    result5 = generate_task_frontend_rendering_strategy(task)
    result6 = recommend_task_frontend_rendering_strategy(task)

    assert result1.findings[0].task_id == "task-alias"
    assert result2.findings[0].task_id == "task-alias"
    assert result3.findings[0].task_id == "task-alias"
    assert result4.findings[0].task_id == "task-alias"
    assert result5.findings[0].task_id == "task-alias"
    assert result6.findings[0].task_id == "task-alias"


def test_summary_aggregates_correctly():
    result = build_task_frontend_rendering_strategy_plan(
        _plan(
            [
                _task("task-1", title="SSR full", description="SSR with all strategies implemented."),
                _task("task-2", title="SSR partial", description="SSR only."),
                _task("task-3", title="Unrelated", description="Backend work."),
            ]
        )
    )

    summary = result.summary
    assert summary["total_task_count"] == 3
    assert summary["impacted_task_count"] == 2
    assert len(summary["not_applicable_task_ids"]) == 1
    assert isinstance(summary["missing_strategy_count"], int)
    assert "readiness_counts" in summary
    assert "signal_counts" in summary
    assert "present_strategy_counts" in summary
    assert "missing_strategy_counts" in summary


def test_records_property_compatibility():
    result = analyze_task_frontend_rendering_strategy(
        _task("task-rec", title="SSR", description="Server-side rendering.")
    )

    assert result.records == result.findings
    assert len(result.records) == 1
    assert result.records[0].task_id == "task-rec"


def test_task_id_fallback_to_index():
    result = build_task_frontend_rendering_strategy_plan(
        [
            {"title": "SSR without ID", "description": "Server-side rendering."},
            {"title": "CSR without ID", "description": "Client-side rendering."},
        ]
    )

    assert result.findings[0].task_id == "task-1"
    assert result.findings[1].task_id == "task-2"


def test_web_vitals_performance_signals():
    result = analyze_task_frontend_rendering_strategy(
        _task(
            "task-vitals",
            title="Optimize Web Vitals",
            description=(
                "Improve Core Web Vitals metrics: LCP under 2.5s, FID under 100ms, CLS under 0.1. "
                "Monitor TTFB, FCP, and INP. Set up Lighthouse CI and real user monitoring."
            ),
        )
    )

    finding = result.findings[0]
    assert "performance_budget" in finding.detected_signals
    assert "performance_monitoring" in finding.present_strategies


def test_graphql_and_tanstack_query_data_fetching():
    result = build_task_frontend_rendering_strategy_plan(
        [
            _task(
                "task-graphql",
                title="GraphQL data fetching",
                description=(
                    "Use Apollo Client for GraphQL queries with Tanstack Query for caching. "
                    "Implement parallel data fetching to avoid waterfalls."
                ),
            )
        ]
    )

    finding = result.findings[0]
    assert "data_fetching" in finding.detected_signals
    assert "data_fetching_pattern" in finding.present_strategies


def test_vite_and_turbopack_bundle_optimization():
    result = analyze_task_frontend_rendering_strategy(
        _task(
            "task-bundler",
            title="Optimize with Vite",
            description=(
                "Migrate to Vite for faster builds with tree-shaking and minification. "
                "Use dynamic imports for code splitting and lazy loading."
            ),
            files_or_modules=["vite.config.ts"],
        )
    )

    finding = result.findings[0]
    assert "bundle_optimization" in finding.detected_signals
    assert "code_splitting" in finding.present_strategies
    assert "lazy_loading" in finding.present_strategies


def test_nuxt_and_sveltekit_framework_detection():
    result = build_task_frontend_rendering_strategy_plan(
        [
            _task(
                "task-nuxt",
                title="Nuxt SSR",
                description="Server-side rendering with Nuxt.js and automatic code splitting.",
                files_or_modules=["pages/index.vue"],
            )
        ]
    )

    finding = result.findings[0]
    assert "ssr" in finding.detected_signals
    assert "server_side_rendering" in finding.present_strategies


def test_evidence_truncation_for_long_text():
    long_description = (
        "Implement server-side rendering with Next.js using getServerSideProps for dynamic data fetching. "
        "This is a very long description that should be truncated in the evidence snippet to ensure "
        "that evidence entries remain concise and readable in the output. "
        "Additional context about hydration strategy and performance optimization continues here."
    )
    result = analyze_task_frontend_rendering_strategy(
        _task("task-long", title="SSR", description=long_description)
    )

    finding = result.findings[0]
    evidence_entries = [e for e in finding.evidence if e.startswith("description:")]
    assert len(evidence_entries) > 0
    for evidence in evidence_entries:
        assert len(evidence) <= 200


def _plan(tasks):
    return {"id": "plan-frontend", "tasks": tasks}


def _task(task_id, **kwargs):
    return {"id": task_id, **kwargs}
