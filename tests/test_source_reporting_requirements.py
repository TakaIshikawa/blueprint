"""Tests for reporting requirements extractor."""

import json

from blueprint.domain.models import SourceBrief
from blueprint.source_reporting_requirements import (
    ReportingRequirement,
    ReportingRequirementsReport,
    build_reporting_requirements_report,
    extract_reporting_requirements,
    analyze_reporting_requirements,
    derive_reporting_requirements,
    generate_reporting_requirements,
    summarize_reporting_requirements,
)


def _source_brief(**kwargs):
    """Helper to create test source brief."""
    defaults = {
        "id": "test-001",
        "title": "Test Brief",
        "summary": "",
    }
    return {**defaults, **kwargs}


def test_extracts_multi_signal_reporting_requirements_with_evidence():
    """Test extraction of multiple reporting requirement types."""
    result = build_reporting_requirements_report(
        _source_brief(
            summary=(
                "Create scheduled reports and real-time dashboards with data from multiple sources. "
                "Support daily and monthly aggregation levels with interactive visualizations."
            ),
            source_payload={
                "requirements": [
                    "Generate daily reports and ad-hoc dashboards",
                    "Integrate data sources from SQL databases and REST APIs",
                    "Aggregate data at daily, monthly, and user-level granularity",
                    "Display bar charts, line graphs, and data tables",
                    "Refresh data hourly with real-time updates",
                    "Ensure data freshness within 5 minutes",
                    "Optimize query performance for sub-second response",
                    "Enable drill-down capabilities for detail views",
                    "Export reports to PDF, CSV, and Excel formats",
                    "Implement row-level security and access controls",
                ],
            },
        )
    )

    assert isinstance(result, ReportingRequirementsReport)
    assert all(isinstance(record, ReportingRequirement) for record in result.records)
    assert set(record.requirement_type for record in result.records) == {
        "report_types",
        "data_sources",
        "aggregation_levels",
        "visualization_types",
        "refresh_frequency",
        "data_freshness",
        "query_performance",
        "drill_down_capabilities",
        "export_formats",
        "access_controls",
    }

    by_type = {record.requirement_type: record for record in result.records}
    assert any("report" in item.lower() or "dashboard" in item.lower() for item in by_type["report_types"].evidence)
    assert any("data" in item.lower() and "source" in item.lower() for item in by_type["data_sources"].evidence)
    assert any("aggregat" in item.lower() or "daily" in item.lower() for item in by_type["aggregation_levels"].evidence)
    assert any("chart" in item.lower() or "graph" in item.lower() for item in by_type["visualization_types"].evidence)
    assert any("refresh" in item.lower() or "update" in item.lower() for item in by_type["refresh_frequency"].evidence)
    assert result.summary["requirement_count"] == 10
    assert result.summary["report_design_coverage"] == 100
    assert result.summary["data_architecture_coverage"] == 100
    assert result.summary["ux_coverage"] == 100


def test_brief_without_reporting_language_returns_stable_empty_report():
    """Test that briefs without reporting terms return empty report."""
    result = build_reporting_requirements_report(
        _source_brief(
            title="Database migration",
            summary="Add new column to users table with default value.",
            source_payload={
                "requirements": [
                    "Create migration script for schema change.",
                    "Ensure backward compatibility with existing code.",
                ],
            },
        )
    )
    repeat = build_reporting_requirements_report(
        _source_brief(
            title="Database migration",
            summary="Add new column to users table with default value.",
            source_payload={
                "requirements": [
                    "Create migration script for schema change.",
                    "Ensure backward compatibility with existing code.",
                ],
            },
        )
    )

    expected_summary = {
        "requirement_count": 0,
        "source_count": 1,
        "type_counts": {
            "report_types": 0,
            "data_sources": 0,
            "aggregation_levels": 0,
            "visualization_types": 0,
            "refresh_frequency": 0,
            "data_freshness": 0,
            "query_performance": 0,
            "drill_down_capabilities": 0,
            "export_formats": 0,
            "access_controls": 0,
        },
        "report_design_coverage": 0,
        "data_architecture_coverage": 0,
        "ux_coverage": 0,
    }
    assert result.summary == expected_summary
    assert result.requirements == ()
    assert result.records == ()
    assert result.summary == repeat.summary
    assert result.to_dict() == repeat.to_dict()
    assert json.dumps(result.to_dict(), sort_keys=True) == json.dumps(repeat.to_dict(), sort_keys=True)


def test_detects_report_types_requirements():
    """Test detection of report types patterns."""
    result = build_reporting_requirements_report(
        {"description": "Create scheduled reports and ad-hoc dashboards for executives"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "report_types"
    assert any("report" in term.lower() or "dashboard" in term.lower() for term in result.requirements[0].matched_terms)


def test_detects_data_sources_requirements():
    """Test detection of data sources patterns."""
    result = build_reporting_requirements_report(
        {"description": "Integrate data sources from PostgreSQL database and REST API endpoints"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "data_sources"
    assert any("data" in term.lower() and "source" in term.lower() for term in result.requirements[0].matched_terms)


def test_detects_aggregation_levels_requirements():
    """Test detection of aggregation levels patterns."""
    result = build_reporting_requirements_report(
        {"description": "Aggregate data at daily, monthly, and user-level granularity"}
    )
    assert len(result.requirements) >= 1
    assert any(req.requirement_type == "aggregation_levels" for req in result.requirements)
    agg_req = next(req for req in result.requirements if req.requirement_type == "aggregation_levels")
    assert any("aggregat" in term.lower() or "granularity" in term.lower() or "level" in term.lower() for term in agg_req.matched_terms)


def test_detects_visualization_types_requirements():
    """Test detection of visualization types patterns."""
    result = build_reporting_requirements_report(
        {"description": "Display bar charts, line graphs, and pie charts with data tables"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "visualization_types"


def test_detects_refresh_frequency_requirements():
    """Test detection of refresh frequency patterns."""
    result = build_reporting_requirements_report(
        {"description": "Enable real-time data refresh with hourly updates and auto-refresh"}
    )
    assert len(result.requirements) >= 1
    assert any(req.requirement_type == "refresh_frequency" for req in result.requirements)


def test_detects_data_freshness_requirements():
    """Test detection of data freshness patterns."""
    result = build_reporting_requirements_report(
        {"description": "Ensure data freshness within 5 minutes to avoid stale data"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "data_freshness"
    assert any("freshness" in term.lower() or "stale" in term.lower() for term in result.requirements[0].matched_terms)


def test_detects_query_performance_requirements():
    """Test detection of query performance patterns."""
    result = build_reporting_requirements_report(
        {"description": "Optimize query performance with indexing and query caching"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "query_performance"


def test_detects_drill_down_capabilities_requirements():
    """Test detection of drill-down capabilities patterns."""
    result = build_reporting_requirements_report(
        {"description": "Enable drill-down capabilities for interactive exploration and detail views"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "drill_down_capabilities"


def test_detects_export_formats_requirements():
    """Test detection of export formats patterns."""
    result = build_reporting_requirements_report(
        {"description": "Support export to PDF, CSV, and Excel formats"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "export_formats"
    assert any("pdf" in term.lower() or "csv" in term.lower() or "excel" in term.lower() for term in result.requirements[0].matched_terms)


def test_detects_access_controls_requirements():
    """Test detection of access controls patterns."""
    result = build_reporting_requirements_report(
        {"description": "Implement row-level security and user permissions for access control"}
    )
    assert len(result.requirements) == 1
    assert result.requirements[0].requirement_type == "access_controls"


def test_to_dict_serialization():
    """Test to_dict() method produces valid JSON."""
    result = build_reporting_requirements_report(
        {
            "title": "Reporting implementation",
            "summary": "Create dashboards with data sources and export capabilities",
        }
    )
    data = result.to_dict()
    assert isinstance(data, dict)
    assert "source_brief_id" in data
    assert "requirements" in data
    assert "summary" in data
    assert "records" in data
    json_str = json.dumps(data)
    assert json.loads(json_str) == data


def test_to_dicts_returns_list_of_dicts():
    """Test to_dicts() returns list of requirement dictionaries."""
    result = build_reporting_requirements_report(
        {"description": "Create scheduled reports with visualizations"}
    )
    dicts = result.to_dicts()
    assert isinstance(dicts, list)
    assert all(isinstance(item, dict) for item in dicts)
    assert all("requirement_type" in item for item in dicts)


def test_to_markdown_rendering():
    """Test to_markdown() renders valid Markdown."""
    result = build_reporting_requirements_report(
        {
            "id": "test-source-123",
            "summary": "Create dashboards with real-time refresh",
        }
    )
    markdown = result.to_markdown()
    assert isinstance(markdown, str)
    assert "# Reporting Requirements Report: test-source-123" in markdown
    assert "## Summary" in markdown
    assert "## Requirements" in markdown
    assert "| Type | Source Field Paths | Evidence | Follow-up Questions |" in markdown


def test_to_markdown_empty_report():
    """Test to_markdown() with empty report."""
    result = build_reporting_requirements_report({"description": "Database migration"})
    markdown = result.to_markdown()
    assert "No reporting requirements were inferred" in markdown


def test_extract_reporting_requirements_alias():
    """Test extract_reporting_requirements alias function."""
    requirements = extract_reporting_requirements(
        {"description": "Create scheduled reports with visualizations"}
    )
    assert isinstance(requirements, tuple)
    assert all(isinstance(req, ReportingRequirement) for req in requirements)


def test_compatibility_aliases():
    """Test that compatibility alias functions work."""
    source = {"description": "Create dashboards with export capabilities"}

    result1 = generate_reporting_requirements(source)
    result2 = analyze_reporting_requirements(source)
    result3 = derive_reporting_requirements(source)

    assert result1 == result2 == result3
    assert all(isinstance(req, ReportingRequirement) for req in result1)


def test_summarize_reporting_requirements():
    """Test summarize_reporting_requirements function."""
    summary = summarize_reporting_requirements(
        {"description": "Create dashboards with data sources and visualizations"}
    )
    assert isinstance(summary, dict)
    assert "requirement_count" in summary
    assert "report_design_coverage" in summary
    assert "data_architecture_coverage" in summary


def test_daily_reports():
    """Test detection of daily reports."""
    result = build_reporting_requirements_report(
        {"description": "Generate daily reports for sales metrics"}
    )
    assert any(req.requirement_type == "report_types" for req in result.requirements)


def test_weekly_reports():
    """Test detection of weekly reports."""
    result = build_reporting_requirements_report(
        {"description": "Create weekly reports for management"}
    )
    assert any(req.requirement_type == "report_types" for req in result.requirements)


def test_monthly_reports():
    """Test detection of monthly reports."""
    result = build_reporting_requirements_report(
        {"description": "Produce monthly reports with quarterly summaries"}
    )
    assert any(req.requirement_type == "report_types" for req in result.requirements)


def test_executive_dashboard():
    """Test detection of executive dashboards."""
    result = build_reporting_requirements_report(
        {"description": "Build executive dashboard for leadership team"}
    )
    assert any(req.requirement_type == "report_types" for req in result.requirements)


def test_operational_reports():
    """Test detection of operational reports."""
    result = build_reporting_requirements_report(
        {"description": "Create operational reports for daily monitoring"}
    )
    assert any(req.requirement_type == "report_types" for req in result.requirements)


def test_analytical_dashboard():
    """Test detection of analytical dashboards."""
    result = build_reporting_requirements_report(
        {"description": "Develop analytical dashboard for data analysts"}
    )
    assert any(req.requirement_type == "report_types" for req in result.requirements)


def test_real_time_dashboard():
    """Test detection of real-time dashboards."""
    result = build_reporting_requirements_report(
        {"description": "Implement real-time dashboard with live data"}
    )
    assert any(req.requirement_type == "report_types" for req in result.requirements)
    assert any(req.requirement_type == "refresh_frequency" for req in result.requirements)


def test_interactive_reports():
    """Test detection of interactive reports."""
    result = build_reporting_requirements_report(
        {"description": "Create interactive reports with user controls"}
    )
    assert any(req.requirement_type == "report_types" for req in result.requirements)


def test_database_query():
    """Test detection of database queries."""
    result = build_reporting_requirements_report(
        {"description": "Query PostgreSQL database for customer data"}
    )
    assert any(req.requirement_type == "data_sources" for req in result.requirements)


def test_api_integration():
    """Test detection of API integrations."""
    result = build_reporting_requirements_report(
        {"description": "Fetch data from REST API endpoints"}
    )
    assert any(req.requirement_type == "data_sources" for req in result.requirements)


def test_data_aggregation():
    """Test detection of data aggregation."""
    result = build_reporting_requirements_report(
        {"description": "Aggregate data from multiple sources"}
    )
    assert any(req.requirement_type == "data_sources" for req in result.requirements)


def test_data_warehouse():
    """Test detection of data warehouse sources."""
    result = build_reporting_requirements_report(
        {"description": "Connect to data warehouse for reporting"}
    )
    assert any(req.requirement_type == "data_sources" for req in result.requirements)


def test_data_lake():
    """Test detection of data lake sources."""
    result = build_reporting_requirements_report(
        {"description": "Query data lake for analytics"}
    )
    assert any(req.requirement_type == "data_sources" for req in result.requirements)


def test_sql_queries():
    """Test detection of SQL queries."""
    result = build_reporting_requirements_report(
        {"description": "Execute SQL queries for report generation"}
    )
    assert any(req.requirement_type == "data_sources" for req in result.requirements)


def test_data_pipeline():
    """Test detection of data pipelines."""
    result = build_reporting_requirements_report(
        {"description": "Process data pipeline for reporting"}
    )
    assert any(req.requirement_type == "data_sources" for req in result.requirements)


def test_daily_aggregation():
    """Test detection of daily aggregation."""
    result = build_reporting_requirements_report(
        {"description": "Group data by day for daily aggregation"}
    )
    assert any(req.requirement_type == "aggregation_levels" for req in result.requirements)


def test_monthly_aggregation():
    """Test detection of monthly aggregation."""
    result = build_reporting_requirements_report(
        {"description": "Summarize metrics at monthly aggregation level"}
    )
    assert any(req.requirement_type == "aggregation_levels" for req in result.requirements)


def test_user_level_aggregation():
    """Test detection of user-level aggregation."""
    result = build_reporting_requirements_report(
        {"description": "Aggregate data at user-level for personalization"}
    )
    assert any(req.requirement_type == "aggregation_levels" for req in result.requirements)


def test_group_by():
    """Test detection of group by clauses."""
    result = build_reporting_requirements_report(
        {"description": "Group by date and user for analysis"}
    )
    assert any(req.requirement_type == "aggregation_levels" for req in result.requirements)


def test_rollup():
    """Test detection of rollup operations."""
    result = build_reporting_requirements_report(
        {"description": "Roll-up data from detailed to summary level"}
    )
    assert any(req.requirement_type == "aggregation_levels" for req in result.requirements)


def test_granularity():
    """Test detection of granularity settings."""
    result = build_reporting_requirements_report(
        {"description": "Set granularity level for time-series data"}
    )
    assert any(req.requirement_type == "aggregation_levels" for req in result.requirements)


def test_bar_charts():
    """Test detection of bar charts."""
    result = build_reporting_requirements_report(
        {"description": "Display bar charts for comparison"}
    )
    assert any(req.requirement_type == "visualization_types" for req in result.requirements)


def test_line_graphs():
    """Test detection of line graphs."""
    result = build_reporting_requirements_report(
        {"description": "Show line graphs for trends over time"}
    )
    assert any(req.requirement_type == "visualization_types" for req in result.requirements)


def test_pie_charts():
    """Test detection of pie charts."""
    result = build_reporting_requirements_report(
        {"description": "Create pie charts for proportions"}
    )
    assert any(req.requirement_type == "visualization_types" for req in result.requirements)


def test_scatter_plots():
    """Test detection of scatter plots."""
    result = build_reporting_requirements_report(
        {"description": "Use scatter plots for correlation analysis"}
    )
    assert any(req.requirement_type == "visualization_types" for req in result.requirements)


def test_data_tables():
    """Test detection of data tables."""
    result = build_reporting_requirements_report(
        {"description": "Present data in tables with sorting"}
    )
    assert any(req.requirement_type == "visualization_types" for req in result.requirements)


def test_heatmaps():
    """Test detection of heatmaps."""
    result = build_reporting_requirements_report(
        {"description": "Generate heatmap for pattern visualization"}
    )
    assert any(req.requirement_type == "visualization_types" for req in result.requirements)


def test_time_series():
    """Test detection of time series visualizations."""
    result = build_reporting_requirements_report(
        {"description": "Plot time-series data for historical analysis"}
    )
    assert any(req.requirement_type == "visualization_types" for req in result.requirements)


def test_gauges():
    """Test detection of gauge visualizations."""
    result = build_reporting_requirements_report(
        {"description": "Display gauge charts for KPI tracking"}
    )
    assert any(req.requirement_type == "visualization_types" for req in result.requirements)


def test_sparklines():
    """Test detection of sparklines."""
    result = build_reporting_requirements_report(
        {"description": "Add sparklines for inline trends"}
    )
    assert any(req.requirement_type == "visualization_types" for req in result.requirements)


def test_hourly_refresh():
    """Test detection of hourly refresh."""
    result = build_reporting_requirements_report(
        {"description": "Refresh data hourly for up-to-date metrics"}
    )
    assert any(req.requirement_type == "refresh_frequency" for req in result.requirements)


def test_daily_refresh():
    """Test detection of daily refresh."""
    result = build_reporting_requirements_report(
        {"description": "Set daily refresh schedule for overnight updates"}
    )
    assert any(req.requirement_type == "refresh_frequency" for req in result.requirements)


def test_auto_refresh():
    """Test detection of auto-refresh."""
    result = build_reporting_requirements_report(
        {"description": "Enable auto-refresh every 5 minutes"}
    )
    assert any(req.requirement_type == "refresh_frequency" for req in result.requirements)


def test_streaming_updates():
    """Test detection of streaming updates."""
    result = build_reporting_requirements_report(
        {"description": "Support streaming data updates in real-time"}
    )
    assert any(req.requirement_type == "refresh_frequency" for req in result.requirements)


def test_cache_refresh():
    """Test detection of cache refresh."""
    result = build_reporting_requirements_report(
        {"description": "Invalidate cache and refresh on schedule"}
    )
    assert any(req.requirement_type == "refresh_frequency" for req in result.requirements)


def test_continuous_updates():
    """Test detection of continuous updates."""
    result = build_reporting_requirements_report(
        {"description": "Provide continuous updates for monitoring"}
    )
    assert any(req.requirement_type == "refresh_frequency" for req in result.requirements)


def test_data_latency():
    """Test detection of data latency requirements."""
    result = build_reporting_requirements_report(
        {"description": "Minimize data latency to under 1 minute"}
    )
    assert any(req.requirement_type == "data_freshness" for req in result.requirements)


def test_near_real_time():
    """Test detection of near real-time data."""
    result = build_reporting_requirements_report(
        {"description": "Provide near-real-time data for decision making"}
    )
    assert any(req.requirement_type == "data_freshness" for req in result.requirements)


def test_data_recency():
    """Test detection of data recency."""
    result = build_reporting_requirements_report(
        {"description": "Ensure data recency for accurate reporting"}
    )
    assert any(req.requirement_type == "data_freshness" for req in result.requirements)


def test_sync_delay():
    """Test detection of synchronization delay."""
    result = build_reporting_requirements_report(
        {"description": "Monitor sync delay between sources"}
    )
    assert any(req.requirement_type == "data_freshness" for req in result.requirements)


def test_query_optimization():
    """Test detection of query optimization."""
    result = build_reporting_requirements_report(
        {"description": "Optimize queries for better performance"}
    )
    assert any(req.requirement_type == "query_performance" for req in result.requirements)


def test_query_caching():
    """Test detection of query caching."""
    result = build_reporting_requirements_report(
        {"description": "Implement query caching to reduce load"}
    )
    assert any(req.requirement_type == "query_performance" for req in result.requirements)


def test_indexing_strategy():
    """Test detection of indexing strategy."""
    result = build_reporting_requirements_report(
        {"description": "Design indexing strategy for fast lookups"}
    )
    assert any(req.requirement_type == "query_performance" for req in result.requirements)


def test_materialized_views():
    """Test detection of materialized views."""
    result = build_reporting_requirements_report(
        {"description": "Use materialized views for complex queries"}
    )
    assert any(req.requirement_type == "query_performance" for req in result.requirements)


def test_query_timeout():
    """Test detection of query timeout."""
    result = build_reporting_requirements_report(
        {"description": "Set query timeout limits for safety"}
    )
    assert any(req.requirement_type == "query_performance" for req in result.requirements)


def test_drill_through():
    """Test detection of drill-through navigation."""
    result = build_reporting_requirements_report(
        {"description": "Enable drill-through to detail records"}
    )
    assert any(req.requirement_type == "drill_down_capabilities" for req in result.requirements)


def test_interactive_exploration():
    """Test detection of interactive exploration."""
    result = build_reporting_requirements_report(
        {"description": "Support interactive exploration of data"}
    )
    assert any(req.requirement_type == "drill_down_capabilities" for req in result.requirements)


def test_detail_view():
    """Test detection of detail views."""
    result = build_reporting_requirements_report(
        {"description": "Navigate to detail view for more information"}
    )
    assert any(req.requirement_type == "drill_down_capabilities" for req in result.requirements)


def test_hierarchical_navigation():
    """Test detection of hierarchical navigation."""
    result = build_reporting_requirements_report(
        {"description": "Provide hierarchical navigation for categories"}
    )
    assert any(req.requirement_type == "drill_down_capabilities" for req in result.requirements)


def test_expand_rows():
    """Test detection of row expansion."""
    result = build_reporting_requirements_report(
        {"description": "Allow users to expand rows for details"}
    )
    assert any(req.requirement_type == "drill_down_capabilities" for req in result.requirements)


def test_pdf_export():
    """Test detection of PDF export."""
    result = build_reporting_requirements_report(
        {"description": "Export reports to PDF format"}
    )
    assert any(req.requirement_type == "export_formats" for req in result.requirements)


def test_csv_export():
    """Test detection of CSV export."""
    result = build_reporting_requirements_report(
        {"description": "Download data as CSV file"}
    )
    assert any(req.requirement_type == "export_formats" for req in result.requirements)


def test_excel_export():
    """Test detection of Excel export."""
    result = build_reporting_requirements_report(
        {"description": "Export to Excel format with formatting"}
    )
    assert any(req.requirement_type == "export_formats" for req in result.requirements)


def test_json_export():
    """Test detection of JSON export."""
    result = build_reporting_requirements_report(
        {"description": "Provide JSON export for programmatic access"}
    )
    assert any(req.requirement_type == "export_formats" for req in result.requirements)


def test_xml_export():
    """Test detection of XML export."""
    result = build_reporting_requirements_report(
        {"description": "Support XML export for integration"}
    )
    assert any(req.requirement_type == "export_formats" for req in result.requirements)


def test_row_level_security():
    """Test detection of row-level security."""
    result = build_reporting_requirements_report(
        {"description": "Implement row-level security for data protection"}
    )
    assert any(req.requirement_type == "access_controls" for req in result.requirements)


def test_field_level_security():
    """Test detection of field-level security."""
    result = build_reporting_requirements_report(
        {"description": "Apply field-level security to sensitive columns"}
    )
    assert any(req.requirement_type == "access_controls" for req in result.requirements)


def test_user_permissions():
    """Test detection of user permissions."""
    result = build_reporting_requirements_report(
        {"description": "Configure user permissions for report access"}
    )
    assert any(req.requirement_type == "access_controls" for req in result.requirements)


def test_role_based_access():
    """Test detection of role-based access control."""
    result = build_reporting_requirements_report(
        {"description": "Set up RBAC for different user roles"}
    )
    assert any(req.requirement_type == "access_controls" for req in result.requirements)


def test_data_restrictions():
    """Test detection of data restrictions."""
    result = build_reporting_requirements_report(
        {"description": "Apply data restrictions based on user context"}
    )
    assert any(req.requirement_type == "access_controls" for req in result.requirements)


def test_comprehensive_all_types():
    """Test comprehensive reporting specification with all requirement types."""
    source = {
        "title": "Enterprise Reporting Platform",
        "description": (
            "Build scheduled reports and real-time dashboards using data from PostgreSQL and APIs. "
            "Support daily and monthly aggregation with bar charts and line graphs. "
            "Enable hourly refresh with data freshness under 5 minutes. "
            "Optimize query performance with caching and provide drill-down capabilities. "
            "Export to PDF and Excel with row-level security access controls."
        ),
    }

    result = extract_reporting_requirements(source)

    assert len(result) == 10
    types_found = {req.requirement_type for req in result}
    assert "report_types" in types_found
    assert "data_sources" in types_found
    assert "aggregation_levels" in types_found
    assert "visualization_types" in types_found
    assert "refresh_frequency" in types_found
    assert "data_freshness" in types_found
    assert "query_performance" in types_found
    assert "drill_down_capabilities" in types_found
    assert "export_formats" in types_found
    assert "access_controls" in types_found


def test_requirement_has_evidence():
    """Requirements should include evidence snippets."""
    source = {
        "description": "Create interactive dashboards with drill-down to detail views",
    }

    result = extract_reporting_requirements(source)

    req = next((r for r in result if r.requirement_type == "drill_down_capabilities"), None)
    assert req is not None
    assert len(req.evidence) > 0


def test_requirement_has_source_field_paths():
    """Requirements should track source field paths."""
    source = {
        "description": "Generate daily reports",
        "requirements": ["Export to PDF format"],
    }

    result = extract_reporting_requirements(source)

    assert len(result) > 0
    for req in result:
        assert len(req.source_field_paths) > 0


def test_requirement_has_matched_terms():
    """Requirements should include matched terms."""
    source = {
        "description": "Create bar charts and line graphs for visualization",
    }

    result = extract_reporting_requirements(source)

    req = next((r for r in result if r.requirement_type == "visualization_types"), None)
    assert req is not None
    assert len(req.matched_terms) > 0


def test_requirement_has_follow_up_questions():
    """Requirements should include follow-up questions."""
    source = {
        "description": "Aggregate data at user-level",
    }

    result = extract_reporting_requirements(source)

    req = next((r for r in result if r.requirement_type == "aggregation_levels"), None)
    assert req is not None
    assert len(req.follow_up_questions) > 0


def test_build_report_includes_summary():
    """Report should include summary statistics."""
    source = {
        "description": "Create dashboards with real-time data sources",
    }

    report = build_reporting_requirements_report(source)

    assert isinstance(report, ReportingRequirementsReport)
    assert "requirement_count" in report.summary
    assert "report_design_coverage" in report.summary
    assert "data_architecture_coverage" in report.summary
    assert "ux_coverage" in report.summary


def test_string_input_creates_body_field():
    """String input should be treated as body field."""
    result = extract_reporting_requirements("Generate scheduled reports with export to PDF")

    assert len(result) > 0


def test_case_insensitive_matching():
    """Pattern matching should be case-insensitive."""
    source = {
        "description": "DASHBOARD with REAL-TIME REFRESH and PDF EXPORT",
    }

    result = extract_reporting_requirements(source)

    assert any(req.requirement_type == "report_types" for req in result)
    assert any(req.requirement_type == "refresh_frequency" for req in result)
    assert any(req.requirement_type == "export_formats" for req in result)


def test_multiple_evidence_snippets():
    """Requirements should collect multiple evidence snippets."""
    source = {
        "description": "Create scheduled reports",
        "requirements": ["Generate daily dashboards", "Support ad-hoc reporting"],
        "acceptance_criteria": ["Reports run on schedule"],
    }

    result = extract_reporting_requirements(source)

    req = next((r for r in result if r.requirement_type == "report_types"), None)
    assert req is not None
    assert len(req.evidence) >= 1


def test_coverage_calculations():
    """Summary should calculate coverage percentages."""
    source = {
        "description": "Create dashboards with data sources and visualizations",
    }

    report = build_reporting_requirements_report(source)

    assert "report_design_coverage" in report.summary
    assert "data_architecture_coverage" in report.summary
    assert "ux_coverage" in report.summary
    assert all(0 <= report.summary[key] <= 100 for key in ["report_design_coverage", "data_architecture_coverage", "ux_coverage"])


def test_type_counts_in_summary():
    """Summary should include type counts."""
    source = {
        "description": "Create reports with dashboards and export",
    }

    report = build_reporting_requirements_report(source)

    assert "type_counts" in report.summary
    type_counts = report.summary["type_counts"]
    assert isinstance(type_counts, dict)
    assert "report_types" in type_counts
    assert "export_formats" in type_counts


def test_records_property():
    """Report should expose requirements via records property."""
    source = {
        "description": "Create dashboards with data sources",
    }

    report = build_reporting_requirements_report(source)

    assert hasattr(report, "records")
    assert report.records == report.requirements


def test_requirement_to_dict():
    """Requirement should serialize to dict."""
    source = {
        "description": "Generate scheduled reports",
    }

    result = extract_reporting_requirements(source)
    assert len(result) > 0
    req = result[0]
    req_dict = req.to_dict()

    assert isinstance(req_dict, dict)
    assert "requirement_type" in req_dict
    assert "evidence" in req_dict
    assert "source_field_paths" in req_dict
    assert "matched_terms" in req_dict
    assert "follow_up_questions" in req_dict


def test_source_brief_id_extraction():
    """Report should extract source brief ID."""
    source = {
        "id": "REPORT-456",
        "description": "Create dashboards",
    }

    report = build_reporting_requirements_report(source)

    assert report.source_brief_id == "REPORT-456"


def test_nested_metadata_scanning():
    """Extractor should scan nested metadata fields."""
    source = {
        "metadata": {
            "reporting_features": "Real-time dashboards with drill-down",
            "export_options": "PDF and Excel export",
        },
    }

    result = extract_reporting_requirements(source)

    assert any(req.requirement_type == "report_types" for req in result)
    assert any(req.requirement_type == "drill_down_capabilities" for req in result)
    assert any(req.requirement_type == "export_formats" for req in result)


def test_empty_fields_ignored():
    """Empty fields should be ignored."""
    source = {
        "description": "Create scheduled reports with real-time dashboards",
        "requirements": [],
        "acceptance_criteria": [""],
    }

    result = extract_reporting_requirements(source)

    assert any(req.requirement_type == "report_types" for req in result)


def test_edge_case_real_time_dashboard():
    """Test real-time dashboard edge case."""
    source = {
        "description": "Build real-time dashboard with live streaming data",
    }

    result = extract_reporting_requirements(source)

    assert any(req.requirement_type == "report_types" for req in result)
    assert any(req.requirement_type == "refresh_frequency" for req in result)


def test_edge_case_interactive_visualization():
    """Test interactive visualization edge case."""
    source = {
        "description": "Create interactive charts with click-through navigation",
    }

    result = extract_reporting_requirements(source)

    assert any(req.requirement_type == "visualization_types" for req in result)
    assert any(req.requirement_type == "drill_down_capabilities" for req in result)


def test_edge_case_scheduled_with_freshness():
    """Test scheduled reports with data freshness requirements."""
    source = {
        "description": "Generate daily scheduled reports with data freshness within 1 hour",
    }

    result = extract_reporting_requirements(source)

    assert any(req.requirement_type == "report_types" for req in result)
    assert any(req.requirement_type == "data_freshness" for req in result)
