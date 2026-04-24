"""Blueprint CLI commands."""

from __future__ import annotations

import json
import tempfile
from builtins import list as builtin_list
from typing import Any

import click
import yaml
from pydantic import ValidationError

from pathlib import Path

from blueprint.audits.critical_path import (
    CriticalPathError,
    CriticalPathResult,
    analyze_critical_path,
)
from blueprint.audits.blocked_impact import (
    BlockedImpactResult,
    audit_blocked_impact,
)
from blueprint.audits.acceptance_quality import (
    DEFAULT_MIN_LENGTH as ACCEPTANCE_QUALITY_DEFAULT_MIN_LENGTH,
    AcceptanceQualityResult,
    audit_acceptance_quality,
)
from blueprint.audits.brief_plan_coherence import (
    BriefPlanCoherenceResult,
    audit_brief_plan_coherence,
)
from blueprint.audits.execution_waves import (
    ExecutionWaveError,
    ExecutionWavesResult,
    analyze_execution_waves,
)
from blueprint.audits.dependency_repair import (
    DependencyRepairResult,
    suggest_dependency_repairs,
)
from blueprint.audits.dependency_gate import (
    DependencyGateResult,
    audit_dependency_gate,
)
from blueprint.audits.env_inventory import EnvInventoryResult, build_env_inventory
from blueprint.audits.env_readiness import (
    EnvReadinessResult,
    audit_env_readiness,
)
from blueprint.audits.milestone_dependencies import (
    MilestoneDependencyResult,
    audit_milestone_dependencies,
)
from blueprint.audits.ownership_gaps import (
    DEFAULT_OWNERSHIP_THRESHOLD,
    OwnershipGapFinding,
    OwnershipGapResult,
    audit_ownership_gaps,
)
from blueprint.audits.plan_audit import PlanAuditResult, audit_execution_plan
from blueprint.audits.plan_diff import PlanDiffResult, diff_execution_plans
from blueprint.audits.plan_metrics import PlanMetrics, calculate_plan_metrics
from blueprint.audits.plan_readiness import (
    PlanReadinessResult,
    evaluate_plan_readiness,
)
from blueprint.audits.brief_readiness import (
    BriefReadinessResult,
    audit_brief_readiness,
)
from blueprint.audits.risk_coverage import (
    RiskCoverageResult,
    audit_risk_coverage,
)
from blueprint.audits.task_completeness import (
    TaskCompletenessResult,
    audit_task_completeness,
)
from blueprint.audits.task_splitting import (
    TaskSplittingResult,
    audit_task_splitting,
)
from blueprint.audits.workload import WorkloadResult, analyze_workload
from blueprint.audits.source_similarity import (
    DEFAULT_LIMIT as SOURCE_SIMILARITY_DEFAULT_LIMIT,
    DEFAULT_THRESHOLD as SOURCE_SIMILARITY_DEFAULT_THRESHOLD,
    find_similar_source_briefs,
)
from blueprint.audits.source_duplicates import (
    DEFAULT_LIMIT as SOURCE_DUPLICATES_DEFAULT_LIMIT,
    DEFAULT_THRESHOLD as SOURCE_DUPLICATES_DEFAULT_THRESHOLD,
    SourceDuplicateReport,
    find_duplicate_source_brief_groups,
)
from blueprint.config import get_config
from blueprint.exporters.archive import ArchiveExporter
from blueprint.exporters.brief_review import BriefReviewPacketExporter
from blueprint.exporters.dependency_matrix import DependencyMatrixExporter
from blueprint.exporters.export_diff import compare_rendered_exports
from blueprint.exporters.export_validation import create_exporter, validate_export
from blueprint.exporters.manifest import ExportManifestExporter
from blueprint.exporters.mermaid import MermaidExporter
from blueprint.exporters.plan_graph import PlanGraphExporter, UnknownDependencyError
from blueprint.exporters.source_brief import SourceBriefExporter
from blueprint.exporters.source_manifest import SourceManifestExporter
from blueprint.exporters.status_timeline import StatusTimelineExporter
from blueprint.exporters.task_handoff import TaskHandoffExporter
from blueprint.exporters.task_roster import TaskRosterExporter
from blueprint.domain import (
    ImplementationBrief,
    UnknownSchemaModelError,
    get_all_model_json_schemas,
    get_model_json_schema,
)
from blueprint.generators.brief_generator import (
    BriefGenerator,
    generate_implementation_brief_id,
)
from blueprint.generators.brief_scaffold import scaffold_implementation_brief
from blueprint.generators.plan_generator import PlanGenerator
from blueprint.generators.plan_generator_staged import StagedPlanGenerator
from blueprint.generators.plan_reviser import PlanReviser
from blueprint.importers.csv_backlog_importer import CsvBacklogImporter
from blueprint.importers.graph_importer import GraphImporter
from blueprint.importers.github_issue_importer import GitHubIssueImporter
from blueprint.importers.manual_importer import ManualBriefImporter
from blueprint.importers.max_importer import MaxImporter
from blueprint.importers.obsidian_importer import ObsidianImporter
from blueprint.importers.plan_markdown_importer import (
    PlanMarkdownImporter,
    PlanMarkdownImportError,
)
from blueprint.importers.source_jsonl_importer import SourceJsonlImporter
from blueprint.llm.client import LLMClient
from blueprint.llm.estimator import PromptEstimate, estimate_prompt
from blueprint.llm.provider import LLMProvider
from blueprint.store import Store, init_db


BRIEF_STATUS_CHOICES = (
    "draft",
    "ready_for_planning",
    "planned",
    "queued",
    "in_progress",
    "implemented",
    "validated",
    "paused",
    "rejected",
)
PLAN_STATUS_CHOICES = ("draft", "ready", "queued", "in_progress", "completed", "failed")
TASK_STATUS_CHOICES = ("pending", "in_progress", "completed", "blocked", "skipped")
EXPORT_TARGET_CHOICES = (
    "adr",
    "agent-prompt-pack",
    "relay",
    "relay-yaml",
    "smoothie",
    "codex",
    "claude-code",
    "asana-csv",
    "azure-devops-csv",
    "calendar",
    "checklist",
    "coverage-matrix",
    "critical-path-report",
    "mermaid",
    "milestone-summary",
    "plan-snapshot",
    "csv-tasks",
    "file-impact-map",
    "gantt",
    "github-actions",
    "github-issues",
    "gitlab-issues",
    "html-report",
    "jira-csv",
    "linear",
    "junit-tasks",
    "kanban",
    "raci-matrix",
    "release-notes",
    "risk-register",
    "slack-digest",
    "status-report",
    "task-bundle",
    "taskfile",
    "task-queue-jsonl",
    "trello-json",
    "vscode-tasks",
    "wave-schedule",
    "all",
)


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """Blueprint: Implementation planning layer for design briefs."""
    pass


class HistoryGroup(click.Group):
    """Route `history ENTITY_ID` to the hidden show command."""

    def resolve_command(self, ctx, args):
        """Resolve history subcommands while preserving the legacy shorthand."""
        try:
            return super().resolve_command(ctx, args)
        except click.UsageError:
            if args and args[0] not in self.commands and "show" in self.commands:
                return "show", self.commands["show"], args
            raise


@cli.group(cls=HistoryGroup)
def history():
    """Show or export status history for a brief, plan, or task."""
    pass


@history.command(name="show", hidden=True)
@click.argument("entity_id")
@click.option("--limit", default=50, help="Maximum number of history events to show")
def history_show(entity_id: str, limit: int):
    """Show status history for a brief, plan, or task."""
    config = get_config()
    store = Store(config.db_path)

    events = store.list_status_events(entity_id=entity_id, limit=limit)
    if not events:
        click.echo(f"No history events found for {entity_id}")
        return

    click.echo(f"\n{'Created':<20} {'Entity':<8} {'Status':<35} Reason")
    click.echo("-" * 80)
    for event in events:
        created_at = (event["created_at"] or "")[:19]
        transition = f"{event['old_status']} -> {event['new_status']}"
        reason = event["reason"] or "N/A"
        click.echo(
            f"{created_at:<20} " f"{event['entity_type']:<8} " f"{transition:<35} " f"{reason}"
        )

    click.echo(f"\nTotal: {len(events)} events")


@history.command(name="export")
@click.argument("entity_id")
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Output Markdown timeline path",
)
@click.option("--json", "json_output", is_flag=True, help="Print JSON summary")
def history_export(entity_id: str, output: Path | None, json_output: bool):
    """Export status history for a brief, plan, or task."""
    config = get_config()
    store = Store(config.db_path)
    exporter = StatusTimelineExporter()
    events = store.list_status_events(entity_id=entity_id)

    if output:
        try:
            result_path = exporter.export(entity_id, events, str(output))
        except OSError as exc:
            raise click.ClickException(f"Could not write history export: {exc}") from exc
        if not json_output:
            click.echo(f"Exported status timeline to: {result_path}")

    if json_output:
        click.echo(
            json.dumps(
                exporter.render_json(entity_id, events),
                indent=2,
                sort_keys=True,
            )
        )
    elif not output:
        click.echo(exporter.render_markdown(entity_id, events), nl=False)


# ============================================================================
# Config Commands
# ============================================================================


@cli.group()
def config():
    """Configuration diagnostics."""
    pass


@config.command()
@click.option("--json", "json_output", is_flag=True, help="Output diagnostics as JSON")
def inspect(json_output: bool):
    """Inspect merged configuration and validation warnings."""
    diagnostics = get_config().diagnostics()

    if json_output:
        click.echo(json.dumps(diagnostics, indent=2, sort_keys=True))
        return

    click.echo("Blueprint configuration")
    click.echo(f"Config path: {diagnostics['config_path'] or 'defaults only'}")
    click.echo("\nMerged values:")
    for path, value in _flatten_dict(diagnostics["values"]):
        click.echo(f"  {path}: {value}")

    key_status = "set" if diagnostics["environment"]["ANTHROPIC_API_KEY"]["present"] else "missing"
    click.echo("\nEnvironment:")
    click.echo(f"  ANTHROPIC_API_KEY: {key_status}")

    warnings = diagnostics["warnings"]
    click.echo("\nValidation warnings:")
    if warnings:
        for warning in warnings:
            click.echo(f"  - {warning}")
    else:
        click.echo("  none")


def _flatten_dict(data: dict, prefix: str = ""):
    """Yield dot-separated paths for nested config dictionaries."""
    for key in sorted(data):
        path = f"{prefix}.{key}" if prefix else key
        value = data[key]
        if isinstance(value, dict):
            yield from _flatten_dict(value, path)
        else:
            yield path, value


def _emit_prompt_preview(prompt: str, output: Path | None) -> None:
    """Write a rendered prompt to a file or stdout."""
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(prompt)
        click.echo(f"✓ Wrote prompt to: {output}")
        return

    click.echo(prompt, nl=False)


def _emit_prompt_estimate(estimate: PromptEstimate, json_output: bool) -> None:
    """Write a prompt estimate as JSON or human-readable text."""
    payload = estimate.to_dict()
    if json_output:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return

    click.echo(f"Model: {payload['model']}")
    click.echo(f"Resolved model: {payload['resolved_model']}")
    click.echo(f"Characters: {payload['characters']}")
    click.echo(f"Words: {payload['words']}")
    click.echo(f"Estimated tokens: {payload['estimated_tokens']}")
    click.echo(f"Estimated input cost (USD): ${payload['estimated_input_cost_usd']:.6f}")


def _emit_export_preview(
    preview_content: str,
    output: Path | None,
) -> None:
    """Write a rendered export preview to a file or stdout."""
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(preview_content)
        click.echo(f"✓ Wrote preview to: {output}")
        return

    click.echo(preview_content, nl=False)


def _emit_json_payload(payload: dict, output: Path | None) -> None:
    """Write a JSON payload to a file or stdout."""
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered)
        return

    click.echo(rendered, nl=False)


def _emit_text_payload(rendered: str, output: Path | None) -> None:
    """Write a text payload to a file or stdout."""
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered)
        return

    click.echo(rendered, nl=False)


def _emit_export_manifest(payload: dict, output: Path | None, json_output: bool) -> None:
    """Write an export manifest to a file or stdout."""
    if output or json_output:
        _emit_json_payload(payload, output)
        return

    click.echo(f"Export manifest: {payload['plan_id']}")
    click.echo(f"Generated: {payload['generated_at']}")
    click.echo(f"Exports: {len(payload['exports'])}")
    for export_record in payload["exports"]:
        status = "present" if export_record["exists"] else "missing"
        detail = (
            f", {export_record['size_bytes']} bytes, sha256={export_record['checksum']}"
            if export_record["checksum"]
            else ""
        )
        click.echo(
            f"- {export_record['export_record_id']} "
            f"({export_record['target_engine']}, {export_record['format']}): "
            f"{status}{detail} {export_record['path']}"
        )


def _create_llm_provider(config) -> LLMProvider:
    """Create the configured LLM provider."""
    if config.llm_provider == "anthropic":
        return LLMClient(
            api_key=config.anthropic_api_key,
            default_model=config.default_model,
        )

    raise ValueError(f"Unsupported LLM provider configured: {config.llm_provider}")


def _resolve_llm_model(model: str) -> str:
    """Resolve CLI model aliases against the default provider."""
    return LLMClient.resolve_model(model)


@cli.command(name="export-manifest")
@click.argument("plan_id")
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write manifest JSON to a file",
)
@click.option("--json", "json_output", is_flag=True, help="Output manifest as JSON")
def export_manifest(plan_id: str, output: Path | None, json_output: bool):
    """Create a manifest of rendered export artifacts for one execution plan."""
    config = get_config()
    store = Store(config.db_path)

    if not store.get_execution_plan(plan_id):
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    manifest = ExportManifestExporter().build(store, plan_id)
    _emit_export_manifest(manifest, output, json_output)


# ============================================================================
# Schema Commands
# ============================================================================


@cli.group()
def schema():
    """Export domain record JSON Schemas."""
    pass


@schema.command(name="export")
@click.argument("model_name")
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write schema JSON to a file instead of stdout",
)
def schema_export(model_name: str, output: Path | None):
    """Export JSON Schema for a domain model, or all models."""
    try:
        payload = (
            get_all_model_json_schemas()
            if model_name == "all"
            else get_model_json_schema(model_name)
        )
    except UnknownSchemaModelError as e:
        raise click.ClickException(str(e)) from e

    _emit_json_payload(payload, output)


# ============================================================================
# Database Commands
# ============================================================================


@cli.group()
def db():
    """Database management commands."""
    pass


@db.command()
def init():
    """Initialize Blueprint database."""
    config = get_config()
    click.echo(f"Initializing database at {config.db_path}...")
    init_db(config.db_path)
    click.echo("✓ Database initialized successfully")


# ============================================================================
# Import Commands
# ============================================================================


@cli.group()
def import_cmd():
    """Import design briefs from various sources."""
    pass


# Register as 'import' (Python keyword workaround)
cli.add_command(import_cmd, name="import")


@import_cmd.command()
@click.option("--status", help="Filter by status (candidate, active, completed, shelved)")
@click.option("--limit", default=20, help="Maximum number of briefs to show")
def list_max(status: str | None, limit: int):
    """List available Max design briefs."""
    config = get_config()

    try:
        importer = MaxImporter(config.max_db_path)
        briefs = importer.list_available(limit=limit, status=status)

        if not briefs:
            click.echo("No Max design briefs found")
            return

        click.echo(f"\n{'ID':<15} {'Title':<40} {'Domain':<15} {'Readiness':<10} {'Status':<12}")
        click.echo("-" * 92)

        for brief in briefs:
            click.echo(
                f"{brief['id']:<15} "
                f"{brief['title'][:38]:<40} "
                f"{(brief['domain'] or 'N/A')[:13]:<15} "
                f"{brief['readiness_score']:>7.1f}/100 "
                f"{brief['status']:<12}"
            )

        click.echo(f"\nTotal: {len(briefs)} briefs")
        click.echo(f"\nTo import: blueprint import max <ID>")

    except FileNotFoundError as e:
        click.echo(f"✗ Error: {e}", err=True)
        click.echo(f"  Check your Max database path in config: {config.max_db_path}", err=True)
    except Exception as e:
        click.echo(f"✗ Failed to list Max briefs: {e}", err=True)


@import_cmd.command(name="list-github-issues")
@click.option(
    "--state",
    type=click.Choice(["open", "closed", "all"]),
    default="open",
    show_default=True,
    help="Filter by issue state",
)
@click.option("--limit", default=20, show_default=True, help="Maximum number of issues to show")
@click.option("--json", "as_json", is_flag=True, help="Output issue summaries as JSON")
def list_github_issues(state: str, limit: int, as_json: bool):
    """List available GitHub issues from the configured default repository."""
    config = get_config()
    importer = GitHubIssueImporter(
        token_env=config.github_token_env,
        default_owner=config.github_default_owner,
        default_repo=config.github_default_repo,
    )

    try:
        issues = importer.list_available(limit=limit, state=state)
    except Exception as e:
        raise click.ClickException(f"Failed to list GitHub issues: {e}") from e

    if as_json:
        click.echo(json.dumps(issues, indent=2))
        return

    if not issues:
        click.echo("No GitHub issues found")
        return

    click.echo(
        f"\n{'Issue':<18} {'State':<8} {'Title':<38} "
        f"{'Labels':<24} {'Assignees':<18} {'Updated':<20}"
    )
    click.echo("-" * 130)

    for issue in issues:
        labels = ", ".join(issue.get("labels") or [])
        assignees = ", ".join(issue.get("assignees") or [])
        click.echo(
            f"{issue['id']:<18} "
            f"{(issue.get('state') or 'N/A'):<8} "
            f"{issue['title'][:36]:<38} "
            f"{labels[:22]:<24} "
            f"{assignees[:16]:<18} "
            f"{(issue.get('updated_at') or 'N/A')[:19]:<20}"
        )

    click.echo(f"\nTotal: {len(issues)} issues")
    click.echo("\nTo import: blueprint import github-issue <OWNER/REPO#NUMBER>")


@import_cmd.command()
@click.argument("brief_id")
@click.option(
    "--replace", is_flag=True, help="Replace an existing imported brief from the same source"
)
@click.option(
    "--skip-existing", is_flag=True, help="Skip import if the source brief already exists"
)
def max(brief_id: str, replace: bool, skip_existing: bool):
    """Import a Max design brief by ID."""
    if replace and skip_existing:
        raise click.UsageError("--replace and --skip-existing cannot be used together")

    config = get_config()
    store = Store(config.db_path)

    try:
        # Initialize Max importer
        importer = MaxImporter(config.max_db_path)

        # Check if brief exists in Max
        if not importer.validate_source(brief_id):
            click.echo(f"✗ Design brief not found in Max: {brief_id}", err=True)
            return

        # Import the brief
        click.echo(f"Importing Max design brief: {brief_id}")
        source_brief = importer.import_from_source(brief_id)
        existing_source_brief = store.get_source_brief_by_source(
            source_project=source_brief["source_project"],
            source_entity_type=source_brief["source_entity_type"],
            source_id=source_brief["source_id"],
        )

        # Store in Blueprint database
        source_brief_id = store.upsert_source_brief(
            source_brief,
            replace=replace,
            skip_existing=skip_existing,
        )

        # Success message
        if existing_source_brief and replace:
            click.echo(
                f"✓ Replaced source brief {source_brief_id} from Max design brief {brief_id}"
            )
        elif existing_source_brief:
            click.echo(
                f"✓ Skipped existing source brief {source_brief_id} from Max design brief {brief_id}"
            )
        else:
            click.echo(
                f"✓ Imported source brief {source_brief_id} from Max design brief {brief_id}"
            )
        click.echo(f"  Title: {source_brief['title']}")
        click.echo(f"  Domain: {source_brief['domain']}")

    except FileNotFoundError as e:
        click.echo(f"✗ Error: {e}", err=True)
        click.echo(f"  Check your Max database path in config: {config.max_db_path}", err=True)
    except Exception as e:
        click.echo(f"✗ Import failed: {e}", err=True)


@import_cmd.command(name="github-issue")
@click.argument("issue_ref")
@click.option(
    "--replace", is_flag=True, help="Replace an existing imported brief from the same source"
)
@click.option(
    "--skip-existing", is_flag=True, help="Skip import if the source brief already exists"
)
def github_issue(issue_ref: str, replace: bool, skip_existing: bool):
    """Import a GitHub issue by OWNER/REPO#NUMBER."""
    if replace and skip_existing:
        raise click.UsageError("--replace and --skip-existing cannot be used together")

    config = get_config()
    store = Store(config.db_path)
    importer = GitHubIssueImporter(
        token_env=config.github_token_env,
        default_owner=config.github_default_owner,
        default_repo=config.github_default_repo,
    )

    try:
        click.echo(f"Importing GitHub issue: {issue_ref}")
        source_brief = importer.import_from_source(issue_ref)
        existing_source_brief = store.get_source_brief_by_source(
            source_project=source_brief["source_project"],
            source_entity_type=source_brief["source_entity_type"],
            source_id=source_brief["source_id"],
        )

        source_brief_id = store.upsert_source_brief(
            source_brief,
            replace=replace,
            skip_existing=skip_existing,
        )

        if existing_source_brief and replace:
            click.echo(
                f"✓ Replaced source brief {source_brief_id} from GitHub issue "
                f"{source_brief['source_id']}"
            )
        elif existing_source_brief:
            click.echo(
                f"✓ Skipped existing source brief {source_brief_id} from GitHub issue "
                f"{source_brief['source_id']}"
            )
        else:
            click.echo(
                f"✓ Imported source brief {source_brief_id} from GitHub issue "
                f"{source_brief['source_id']}"
            )
        click.echo(f"  Title: {source_brief['title']}")
        click.echo(f"  URL: {source_brief['source_links'].get('html_url') or 'N/A'}")

    except Exception as e:
        click.echo(f"✗ Import failed: {e}", err=True)


@import_cmd.command(name="graph-node")
@click.argument("file_path")
@click.option(
    "--replace", is_flag=True, help="Replace an existing imported brief from the same source"
)
@click.option(
    "--skip-existing", is_flag=True, help="Skip import if the source brief already exists"
)
def graph_node(file_path: str, replace: bool, skip_existing: bool):
    """Import a Graph node from an exported JSON file."""
    if replace and skip_existing:
        raise click.UsageError("--replace and --skip-existing cannot be used together")

    config = get_config()
    store = Store(config.db_path)
    importer = GraphImporter()

    try:
        click.echo(f"Importing Graph node: {file_path}")
        source_brief = importer.import_from_source(file_path)
        existing_source_brief = store.get_source_brief_by_source(
            source_project=source_brief["source_project"],
            source_entity_type=source_brief["source_entity_type"],
            source_id=source_brief["source_id"],
        )

        source_brief_id = store.upsert_source_brief(
            source_brief,
            replace=replace,
            skip_existing=skip_existing,
        )

        if existing_source_brief and replace:
            click.echo(
                f"✓ Replaced source brief {source_brief_id} from Graph node "
                f"{source_brief['source_id']}"
            )
        elif existing_source_brief:
            click.echo(
                f"✓ Skipped existing source brief {source_brief_id} from Graph node "
                f"{source_brief['source_id']}"
            )
        else:
            click.echo(
                f"✓ Imported source brief {source_brief_id} from Graph node "
                f"{source_brief['source_id']}"
            )
        click.echo(f"  Title: {source_brief['title']}")
        click.echo(f"  Domain: {source_brief['domain'] or 'N/A'}")

    except Exception as e:
        click.echo(f"✗ Import failed: {e}", err=True)


@import_cmd.command(name="csv-backlog")
@click.argument("file_path")
@click.option("--title-column", default="title", show_default=True, help="CSV title column")
@click.option("--summary-column", default="summary", show_default=True, help="CSV summary column")
@click.option("--domain-column", default="domain", show_default=True, help="CSV domain column")
@click.option("--id-column", default="source_id", show_default=True, help="CSV source ID column")
@click.option("--links-column", default="links", show_default=True, help="CSV links column")
@click.option("--tags-column", default="tags", show_default=True, help="CSV tags column")
@click.option("--replace", is_flag=True, help="Replace existing imported rows from the same source")
@click.option("--skip-existing", is_flag=True, help="Skip import if a source brief already exists")
def csv_backlog(
    file_path: str,
    title_column: str,
    summary_column: str,
    domain_column: str,
    id_column: str,
    links_column: str,
    tags_column: str,
    replace: bool,
    skip_existing: bool,
):
    """Import a CSV backlog where each row becomes a source brief."""
    if replace and skip_existing:
        raise click.UsageError("--replace and --skip-existing cannot be used together")

    config = get_config()
    store = Store(config.db_path)
    importer = CsvBacklogImporter(
        title_column=title_column,
        summary_column=summary_column,
        domain_column=domain_column,
        id_column=id_column,
        links_column=links_column,
        tags_column=tags_column,
    )

    click.echo(f"Importing CSV backlog from: {file_path}")
    try:
        source_briefs = importer.import_file(file_path)
    except Exception as e:
        click.echo(f"✗ Import failed: {e}", err=True)
        return

    counts = {"imported": 0, "skipped": 0, "replaced": 0}
    results: list[dict[str, str]] = []

    for source_brief in source_briefs:
        existing_source_brief = store.get_source_brief_by_source(
            source_project=source_brief["source_project"],
            source_entity_type=source_brief["source_entity_type"],
            source_id=source_brief["source_id"],
        )

        try:
            source_brief_id = store.upsert_source_brief(
                source_brief,
                replace=replace,
                skip_existing=skip_existing,
            )
        except Exception as e:
            click.echo(
                f"✗ Import failed for CSV row {source_brief['source_id']}: {e}",
                err=True,
            )
            continue

        if existing_source_brief and replace:
            status = "replaced"
            counts["replaced"] += 1
        elif existing_source_brief:
            status = "skipped"
            counts["skipped"] += 1
        else:
            status = "imported"
            counts["imported"] += 1

        results.append(
            {
                "status": status,
                "source_brief_id": source_brief_id,
                "source_id": source_brief["source_id"],
                "title": source_brief["title"],
            }
        )

    click.echo(f"Imported: {counts['imported']}")
    click.echo(f"Skipped: {counts['skipped']}")
    click.echo(f"Replaced: {counts['replaced']}")
    click.echo(f"Total rows: {len(source_briefs)}")

    for result in results:
        click.echo(
            f"- {result['status']}: {result['source_id']} "
            f"[{result['source_brief_id']}] {result['title']}"
        )


@import_cmd.command(name="source-jsonl")
@click.argument("file_path")
@click.option("--dry-run", is_flag=True, help="Validate and summarize without writing")
@click.option(
    "--continue-on-error",
    is_flag=True,
    help="Import valid lines while reporting failed lines",
)
@click.option(
    "--regenerate-missing-ids",
    is_flag=True,
    help="Generate SourceBrief IDs for records that omit id",
)
def source_jsonl(
    file_path: str,
    dry_run: bool,
    continue_on_error: bool,
    regenerate_missing_ids: bool,
):
    """Import newline-delimited SourceBrief records."""
    config = get_config()
    store = Store(config.db_path)
    importer = SourceJsonlImporter()

    click.echo(f"{'Validating' if dry_run else 'Importing'} source JSONL from: {file_path}")
    try:
        result = importer.import_file(
            file_path,
            store,
            dry_run=dry_run,
            continue_on_error=continue_on_error,
            regenerate_missing_ids=regenerate_missing_ids,
        )
    except Exception as e:
        raise click.ClickException(f"Import failed: {e}") from e

    click.echo(f"Inserted: {result.inserted}")
    click.echo(f"Updated: {result.updated}")
    click.echo(f"Skipped: {result.skipped}")
    click.echo(f"Errors: {result.error_count}")
    click.echo(f"Total lines: {result.total_lines}")

    for record in result.records:
        click.echo(
            f"- line {record.line_number}: {record.status} "
            f"{record.source_id} [{record.source_brief_id}] {record.title}"
        )

    for error in result.errors:
        click.echo(f"Line {error.line_number}: {error.message}", err=True)

    if result.errors and not continue_on_error:
        raise click.ClickException("Source JSONL import failed validation")


@import_cmd.command()
@click.argument("file_path")
@click.option(
    "--replace", is_flag=True, help="Replace an existing imported brief from the same source"
)
@click.option(
    "--skip-existing", is_flag=True, help="Skip import if the source brief already exists"
)
def manual(file_path: str, replace: bool, skip_existing: bool):
    """Import a manual design brief from markdown file."""
    if replace and skip_existing:
        raise click.UsageError("--replace and --skip-existing cannot be used together")

    config = get_config()
    store = Store(config.db_path)
    importer = ManualBriefImporter()

    click.echo(f"Importing manual brief from: {file_path}")
    try:
        source_brief = importer.import_from_source(file_path)
        existing_source_brief = store.get_source_brief_by_source(
            source_project=source_brief["source_project"],
            source_entity_type=source_brief["source_entity_type"],
            source_id=source_brief["source_id"],
        )

        source_brief_id = store.upsert_source_brief(
            source_brief,
            replace=replace,
            skip_existing=skip_existing,
        )

        if existing_source_brief and replace:
            click.echo(
                f"✓ Replaced source brief {source_brief_id} from manual brief "
                f"{source_brief['source_id']}"
            )
        elif existing_source_brief:
            click.echo(
                f"✓ Skipped existing source brief {source_brief_id} from manual brief "
                f"{source_brief['source_id']}"
            )
        else:
            click.echo(
                f"✓ Imported source brief {source_brief_id} from manual brief "
                f"{source_brief['source_id']}"
            )
        click.echo(f"  Title: {source_brief['title']}")
        click.echo(f"  Domain: {source_brief['domain'] or 'N/A'}")
    except Exception as e:
        click.echo(f"✗ Import failed: {e}", err=True)


@import_cmd.command(name="obsidian-note")
@click.argument("path")
@click.option(
    "--replace", is_flag=True, help="Replace an existing imported brief from the same source"
)
@click.option(
    "--skip-existing", is_flag=True, help="Skip import if the source brief already exists"
)
def obsidian_note(path: str, replace: bool, skip_existing: bool):
    """Import an Obsidian markdown note from a vault."""
    if replace and skip_existing:
        raise click.UsageError("--replace and --skip-existing cannot be used together")

    config = get_config()
    store = Store(config.db_path)
    importer = ObsidianImporter()

    click.echo(f"Importing Obsidian note from: {path}")
    try:
        source_brief = importer.import_from_source(path)
        existing_source_brief = store.get_source_brief_by_source(
            source_project=source_brief["source_project"],
            source_entity_type=source_brief["source_entity_type"],
            source_id=source_brief["source_id"],
        )

        source_brief_id = store.upsert_source_brief(
            source_brief,
            replace=replace,
            skip_existing=skip_existing,
        )

        if existing_source_brief and replace:
            click.echo(
                f"✓ Replaced source brief {source_brief_id} from Obsidian note "
                f"{source_brief['source_id']}"
            )
        elif existing_source_brief:
            click.echo(
                f"✓ Skipped existing source brief {source_brief_id} from Obsidian note "
                f"{source_brief['source_id']}"
            )
        else:
            click.echo(
                f"✓ Imported source brief {source_brief_id} from Obsidian note "
                f"{source_brief['source_id']}"
            )
        click.echo(f"  Title: {source_brief['title']}")
        click.echo(f"  Domain: {source_brief['domain'] or 'N/A'}")
    except Exception as e:
        click.echo(f"✗ Import failed: {e}", err=True)


@import_cmd.command(name="manual-dir")
@click.argument(
    "directory",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
)
@click.option(
    "--glob",
    "glob_pattern",
    default="*.md",
    show_default=True,
    help="File glob to import",
)
@click.option("--recursive", is_flag=True, help="Include files in nested directories")
@click.option(
    "--replace", is_flag=True, help="Replace existing imported briefs from the same source"
)
@click.option("--skip-existing", is_flag=True, help="Skip import if a source brief already exists")
@click.option("--json", "json_output", is_flag=True, help="Output import results as JSON")
def manual_dir(
    directory: Path,
    glob_pattern: str,
    recursive: bool,
    replace: bool,
    skip_existing: bool,
    json_output: bool,
):
    """Import manual design briefs from a directory of markdown files."""
    if replace and skip_existing:
        raise click.UsageError("--replace and --skip-existing cannot be used together")

    config = get_config()
    store = Store(config.db_path)
    importer = ManualBriefImporter()
    directory = directory.expanduser().resolve()
    paths = _find_manual_directory_files(directory, glob_pattern, recursive)

    results: list[dict[str, str | None]] = []
    counts = {"imported": 0, "skipped": 0, "failed": 0, "total": len(paths)}

    for path in paths:
        result: dict[str, str | None] = {
            "file": str(path),
            "relative_path": path.relative_to(directory).as_posix(),
            "status": None,
            "source_brief_id": None,
            "error": None,
        }

        try:
            source_brief = importer.import_from_source(str(path))
            existing_source_brief = store.get_source_brief_by_source(
                source_project=source_brief["source_project"],
                source_entity_type=source_brief["source_entity_type"],
                source_id=source_brief["source_id"],
            )
            source_brief_id = store.upsert_source_brief(
                source_brief,
                replace=replace,
                skip_existing=skip_existing,
            )
        except Exception as e:
            result["status"] = "failed"
            result["error"] = str(e)
            counts["failed"] += 1
            results.append(result)
            continue

        result["source_brief_id"] = source_brief_id
        if existing_source_brief and not replace:
            result["status"] = "skipped"
            counts["skipped"] += 1
        else:
            result["status"] = "imported"
            counts["imported"] += 1
        results.append(result)

    payload = {
        "directory": str(directory),
        "glob": glob_pattern,
        "recursive": recursive,
        "counts": counts,
        "files": results,
    }

    if json_output:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return

    click.echo(f"Imported: {counts['imported']}")
    click.echo(f"Skipped: {counts['skipped']}")
    click.echo(f"Failed: {counts['failed']}")
    click.echo(f"Total: {counts['total']}")

    for result in results:
        status = result["status"]
        source_brief_id = result["source_brief_id"] or "N/A"
        detail = f" ({result['error']})" if result["error"] else ""
        click.echo(f"- {status}: {result['relative_path']} [{source_brief_id}]{detail}")


@import_cmd.command(name="obsidian-dir")
@click.argument(
    "directory",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
)
@click.option(
    "--glob",
    "glob_pattern",
    default="*.md",
    show_default=True,
    help="File glob to import",
)
@click.option("--recursive", is_flag=True, help="Include files in nested directories")
@click.option(
    "--replace", is_flag=True, help="Replace existing imported briefs from the same source"
)
@click.option("--skip-existing", is_flag=True, help="Skip import if a source brief already exists")
@click.option("--json", "json_output", is_flag=True, help="Output import results as JSON")
def obsidian_dir(
    directory: Path,
    glob_pattern: str,
    recursive: bool,
    replace: bool,
    skip_existing: bool,
    json_output: bool,
):
    """Import Obsidian markdown notes from a vault directory."""
    if replace and skip_existing:
        raise click.UsageError("--replace and --skip-existing cannot be used together")

    config = get_config()
    store = Store(config.db_path)
    importer = ObsidianImporter()
    directory = directory.expanduser().resolve()
    paths = _find_manual_directory_files(directory, glob_pattern, recursive)

    results: list[dict[str, str | None]] = []
    counts = {
        "imported": 0,
        "skipped": 0,
        "replaced": 0,
        "failed": 0,
        "total": len(paths),
    }

    for path in paths:
        result: dict[str, str | None] = {
            "file": str(path),
            "relative_path": path.relative_to(directory).as_posix(),
            "status": None,
            "source_brief_id": None,
            "error": None,
        }

        try:
            source_brief = importer.import_from_source(str(path))
            existing_source_brief = store.get_source_brief_by_source(
                source_project=source_brief["source_project"],
                source_entity_type=source_brief["source_entity_type"],
                source_id=source_brief["source_id"],
            )
            source_brief_id = store.upsert_source_brief(
                source_brief,
                replace=replace,
                skip_existing=skip_existing,
            )
        except Exception as e:
            result["status"] = "failed"
            result["error"] = str(e)
            counts["failed"] += 1
            results.append(result)
            continue

        result["source_brief_id"] = source_brief_id
        if existing_source_brief and replace:
            result["status"] = "replaced"
            counts["replaced"] += 1
        elif existing_source_brief:
            result["status"] = "skipped"
            counts["skipped"] += 1
        else:
            result["status"] = "imported"
            counts["imported"] += 1
        results.append(result)

    payload = {
        "directory": str(directory),
        "glob": glob_pattern,
        "recursive": recursive,
        "counts": counts,
        "files": results,
    }

    if json_output:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return

    click.echo(f"Imported: {counts['imported']}")
    click.echo(f"Skipped: {counts['skipped']}")
    click.echo(f"Replaced: {counts['replaced']}")
    click.echo(f"Failed: {counts['failed']}")
    click.echo(f"Total: {counts['total']}")

    for result in results:
        status = result["status"]
        source_brief_id = result["source_brief_id"] or "N/A"
        detail = f" ({result['error']})" if result["error"] else ""
        click.echo(f"- {status}: {result['relative_path']} [{source_brief_id}]{detail}")


def _find_manual_directory_files(
    directory: Path,
    glob_pattern: str,
    recursive: bool,
) -> list[Path]:
    """Return matching directory files in deterministic relative-path order."""
    iterator = directory.rglob(glob_pattern) if recursive else directory.glob(glob_pattern)
    return sorted(
        (path.resolve() for path in iterator if path.is_file()),
        key=lambda path: path.relative_to(directory).as_posix(),
    )


# ============================================================================
# Source Brief Commands
# ============================================================================


@cli.group()
def source():
    """Manage source briefs."""
    pass


@source.command()
@click.option("--source-project", help="Filter by source project (max, graph, manual)")
@click.option("--limit", default=50, help="Maximum number of briefs to show")
def list(source_project: str | None, limit: int):
    """List source briefs."""
    config = get_config()
    store = Store(config.db_path)

    briefs = store.list_source_briefs(source_project=source_project, limit=limit)

    if not briefs:
        click.echo("No source briefs found")
        return

    click.echo(f"\n{'ID':<15} {'Source':<10} {'Type':<15} {'Title':<40} {'Created':<20}")
    click.echo("-" * 100)

    for brief in briefs:
        click.echo(
            f"{brief['id']:<15} "
            f"{brief['source_project']:<10} "
            f"{brief['source_entity_type']:<15} "
            f"{brief['title'][:38]:<40} "
            f"{brief['created_at'][:19]:<20}"
        )

    click.echo(f"\nTotal: {len(briefs)} briefs")


@source.command()
@click.argument("brief_id")
def inspect(brief_id: str):
    """Inspect a source brief in detail."""
    config = get_config()
    store = Store(config.db_path)

    brief = store.get_source_brief(brief_id)

    if not brief:
        click.echo(f"Source brief not found: {brief_id}", err=True)
        return

    click.echo(f"\n{'='*80}")
    click.echo(f"Source Brief: {brief['id']}")
    click.echo(f"{'='*80}\n")

    click.echo(f"Title:        {brief['title']}")
    click.echo(f"Domain:       {brief['domain'] or 'N/A'}")
    click.echo(f"Source:       {brief['source_project']} ({brief['source_entity_type']})")
    click.echo(f"Source ID:    {brief['source_id']}")
    click.echo(f"Created:      {brief['created_at']}")
    click.echo(f"\nSummary:\n{brief['summary']}\n")

    if brief["source_links"]:
        click.echo("Source Links:")
        for key, value in brief["source_links"].items():
            click.echo(f"  {key}: {value}")
        click.echo()


@source.command(name="export")
@click.argument("brief_id")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["markdown", "json"]),
    default="markdown",
    show_default=True,
    help="Export format",
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write export to a file instead of stdout",
)
def source_export(brief_id: str, output_format: str, output: Path | None):
    """Export a normalized source brief for external review."""
    config = get_config()
    store = Store(config.db_path)

    brief = store.get_source_brief(brief_id)
    if not brief:
        raise click.ClickException(f"Source brief not found: {brief_id}")

    rendered = SourceBriefExporter().render(brief, output_format=output_format)
    _emit_text_payload(rendered, output)


@source.command(name="manifest")
@click.option(
    "--output",
    required=True,
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write Markdown manifest to this path",
)
@click.option("--source-project", help="Filter by source project")
@click.option(
    "--limit",
    default=50,
    show_default=True,
    type=click.IntRange(min=0),
    help="Maximum number of source briefs to include",
)
def source_manifest(output: Path, source_project: str | None, limit: int):
    """Export a Markdown manifest of source briefs."""
    config = get_config()
    store = Store(config.db_path)

    briefs = store.list_source_briefs(source_project=source_project, limit=limit)
    SourceManifestExporter().export(
        briefs,
        str(output),
        source_project=source_project,
        limit=limit,
    )


@source.command()
@click.argument("brief_id")
@click.option(
    "--limit",
    default=SOURCE_SIMILARITY_DEFAULT_LIMIT,
    type=click.IntRange(min=0),
    help="Maximum number of similar briefs to show",
)
@click.option(
    "--threshold",
    default=SOURCE_SIMILARITY_DEFAULT_THRESHOLD,
    type=click.FloatRange(min=0.0, max=1.0),
    help="Minimum similarity score from 0.0 to 1.0",
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON")
def similar(brief_id: str, limit: int, threshold: float, json_output: bool):
    """List likely duplicate source briefs."""
    config = get_config()
    store = Store(config.db_path)

    brief = store.get_source_brief(brief_id)
    if not brief:
        raise click.ClickException(f"Source brief not found: {brief_id}")

    candidates = store.list_source_briefs(limit=10000)
    matches = find_similar_source_briefs(
        brief,
        candidates,
        threshold=threshold,
        limit=limit,
    )

    if json_output:
        payload = {
            "brief_id": brief_id,
            "threshold": threshold,
            "limit": limit,
            "matches": [match.to_dict() for match in matches],
        }
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return

    if not matches:
        click.echo(f"No similar source briefs found for {brief_id}")
        return

    click.echo(f"\n{'ID':<15} {'Score':<7} {'Source':<12} {'Matched Fields':<36} {'Title':<40}")
    click.echo("-" * 115)
    for match in matches:
        click.echo(
            f"{match.id:<15} "
            f"{match.score:<7.4f} "
            f"{match.source_project:<12} "
            f"{', '.join(match.matched_fields)[:34]:<36} "
            f"{match.title[:38]:<40}"
        )

    click.echo(f"\nTotal: {len(matches)} matches")


@source.command()
@click.option(
    "--limit",
    default=SOURCE_DUPLICATES_DEFAULT_LIMIT,
    type=click.IntRange(min=0),
    help="Maximum number of duplicate groups to show",
)
@click.option(
    "--threshold",
    default=SOURCE_DUPLICATES_DEFAULT_THRESHOLD,
    type=click.FloatRange(min=0.0, max=1.0),
    help="Minimum duplicate score from 0.0 to 1.0",
)
@click.option("--source-project", help="Filter candidate briefs by source project")
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON")
def duplicates(
    limit: int,
    threshold: float,
    source_project: str | None,
    json_output: bool,
):
    """Report likely duplicate source brief groups."""
    config = get_config()
    store = Store(config.db_path)

    candidates = store.list_source_briefs(source_project=source_project, limit=10000)
    report = find_duplicate_source_brief_groups(
        candidates,
        threshold=threshold,
        limit=limit,
        source_project=source_project,
    )

    _emit_source_duplicate_report(report, json_output)


def _emit_source_duplicate_report(
    report: SourceDuplicateReport,
    json_output: bool,
) -> None:
    if json_output:
        click.echo(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return

    if not report.groups:
        source_filter = (
            f" for source project {report.source_project}" if report.source_project else ""
        )
        click.echo(
            f"No duplicate source brief groups found{source_filter} "
            f"at threshold {report.threshold:.4f}"
        )
        return

    click.echo("Source brief duplicate report")
    click.echo(f"Candidates: {report.candidate_count}")
    if report.source_project:
        click.echo(f"Source project: {report.source_project}")
    click.echo(f"Threshold: {report.threshold:.4f}")
    click.echo(f"Groups: {len(report.groups)}")

    for index, group in enumerate(report.groups, start=1):
        click.echo("")
        click.echo(
            f"{index}. canonical={group.canonical_id} "
            f"score={group.score:.4f} briefs={len(group.briefs)}"
        )
        for brief in group.briefs:
            marker = "*" if brief.id == group.canonical_id else "-"
            source = f"{brief.source_project}/" f"{brief.source_entity_type}/" f"{brief.source_id}"
            click.echo(f"   {marker} {brief.id:<15} {source:<32} {brief.title[:60]}")
        pair_summaries = [
            f"{pair.left_id}<->{pair.right_id} {pair.score:.4f} "
            f"({', '.join(pair.matched_fields)})"
            for pair in group.pairs
        ]
        click.echo(f"   evidence: {'; '.join(pair_summaries)}")


# ============================================================================
# Implementation Brief Commands
# ============================================================================


@cli.group()
def brief():
    """Manage implementation briefs."""
    pass


@brief.command(name="prompt")
@click.argument("source_id")
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write the prompt to a file instead of stdout",
)
def brief_prompt(source_id: str, output: Path | None):
    """Render the implementation brief generation prompt for a source brief."""
    config = get_config()
    store = Store(config.db_path)

    source_brief = store.get_source_brief(source_id)
    if not source_brief:
        raise click.ClickException(f"Source brief not found: {source_id}")

    _emit_prompt_preview(BriefGenerator.build_prompt(source_brief), output)


@brief.command(name="estimate")
@click.argument("source_id")
@click.option(
    "--model",
    type=click.Choice(["opus", "sonnet"]),
    default="opus",
    help="LLM model to estimate (opus=claude-opus-4-6, sonnet=claude-sonnet-4-5)",
)
@click.option("--json", "json_output", is_flag=True, help="Output estimate as JSON")
def brief_estimate(source_id: str, model: str, json_output: bool):
    """Estimate implementation brief generation prompt size and input cost."""
    config = get_config()
    store = Store(config.db_path)

    source_brief = store.get_source_brief(source_id)
    if not source_brief:
        raise click.ClickException(f"Source brief not found: {source_id}")

    estimate = estimate_prompt(BriefGenerator.build_prompt(source_brief), model=model)
    _emit_prompt_estimate(estimate, json_output)


@brief.command()
@click.argument("source_id")
@click.option(
    "--model",
    type=click.Choice(["opus", "sonnet"]),
    default="opus",
    help="LLM model to use (opus=claude-opus-4-6, sonnet=claude-sonnet-4-5)",
)
def create(source_id: str, model: str):
    """Generate implementation brief from source brief."""
    config = get_config()
    store = Store(config.db_path)

    try:
        # Get source brief
        source_brief = store.get_source_brief(source_id)
        if not source_brief:
            click.echo(f"✗ Source brief not found: {source_id}", err=True)
            return

        # Initialize LLM client and generator
        llm_client = _create_llm_provider(config)
        generator = BriefGenerator(llm_client)

        # Generate brief
        click.echo(f"Generating implementation brief from {source_id} using {model}...")
        click.echo(f"Source: {source_brief['title']}")
        resolved_model = _resolve_llm_model(model)
        click.echo(f"\n⏳ Calling {resolved_model}... (this may take 10-30 seconds)\n")

        implementation_brief = generator.generate(
            source_brief=source_brief,
            model=resolved_model,
        )

        # Store in database
        brief_id = store.insert_implementation_brief(implementation_brief)

        # Success message
        click.echo(f"✓ Generated implementation brief {brief_id}")
        click.echo(f"  Title: {implementation_brief['title']}")
        click.echo(f"  MVP Goal: {implementation_brief['mvp_goal'][:100]}...")
        click.echo(f"  Tokens used: {implementation_brief['generation_tokens']}")
        click.echo(f"\nView full brief: blueprint brief inspect {brief_id}")

    except ValueError as e:
        if "ANTHROPIC_API_KEY" in str(e):
            click.echo(f"✗ Error: {e}", err=True)
            click.echo("  Set ANTHROPIC_API_KEY environment variable", err=True)
        else:
            click.echo(f"✗ Generation failed: {e}", err=True)
    except Exception as e:
        click.echo(f"✗ Generation failed: {e}", err=True)
        import traceback

        click.echo(traceback.format_exc(), err=True)


@brief.command(name="scaffold")
@click.argument("source_id")
@click.option(
    "--status",
    type=click.Choice(["draft", "ready_for_planning"]),
    default="draft",
    show_default=True,
    help="Initial implementation brief status",
)
@click.option("--json", "json_output", is_flag=True, help="Output created IDs as JSON")
def scaffold(source_id: str, status: str, json_output: bool):
    """Create an offline implementation brief scaffold from a source brief."""
    config = get_config()
    store = Store(config.db_path)

    source_brief = store.get_source_brief(source_id)
    if not source_brief:
        raise click.ClickException(f"Source brief not found: {source_id}")

    implementation_brief = scaffold_implementation_brief(source_brief)
    implementation_brief["status"] = status
    brief_id = store.insert_implementation_brief(implementation_brief)

    if json_output:
        click.echo(
            json.dumps(
                {
                    "brief_id": brief_id,
                    "source_brief_id": source_id,
                    "status": status,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return

    click.echo(f"✓ Scaffolded implementation brief {brief_id}")
    click.echo(f"  Source Brief: {source_id}")
    click.echo(f"  Title: {implementation_brief['title']}")
    click.echo(f"  Status: {status}")


@brief.command(name="import")
@click.argument(
    "file_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--source-id",
    "source_brief_id",
    required=True,
    help="Source brief ID to link this implementation brief to",
)
def import_brief(file_path: Path, source_brief_id: str):
    """Import a hand-authored implementation brief JSON or YAML file."""
    config = get_config()
    store = Store(config.db_path)

    source_brief = store.get_source_brief(source_brief_id)
    if not source_brief:
        raise click.ClickException(f"Source brief not found: {source_brief_id}")

    brief_payload = _load_implementation_brief_payload(file_path)

    brief_payload = _prepare_imported_implementation_brief(
        brief_payload,
        source_brief_id=source_brief_id,
    )
    try:
        implementation_brief = ImplementationBrief.model_validate(brief_payload).model_dump(
            mode="python",
            exclude_none=True,
        )
    except ValidationError as e:
        raise click.ClickException(f"Invalid implementation brief: {e}") from e

    brief_id = store.insert_implementation_brief(implementation_brief)

    click.echo(f"✓ Imported implementation brief {brief_id}")
    click.echo(f"  Source Brief: {source_brief_id}")
    click.echo(f"  Title: {implementation_brief['title']}")
    click.echo(f"  Status: {implementation_brief['status']}")


def _load_implementation_brief_payload(file_path: Path) -> dict:
    """Load an implementation brief payload from a supported authored format."""
    suffix = file_path.suffix.lower()
    raw_payload = file_path.read_text()

    if suffix == ".json":
        try:
            brief_payload = json.loads(raw_payload)
        except json.JSONDecodeError as e:
            raise click.ClickException(f"Invalid JSON in {file_path}: {e}") from e
        format_name = "JSON"
    elif suffix in {".yaml", ".yml"}:
        try:
            brief_payload = yaml.safe_load(raw_payload)
        except yaml.YAMLError as e:
            raise click.ClickException(f"Invalid YAML in {file_path}: {e}") from e
        format_name = "YAML"
    else:
        raise click.ClickException(
            f"Unsupported implementation brief file suffix: {file_path.suffix or '<none>'}. "
            "Use .json, .yaml, or .yml."
        )

    if not isinstance(brief_payload, dict):
        raise click.ClickException(f"Implementation brief {format_name} must be an object")

    return brief_payload


def _prepare_imported_implementation_brief(
    brief_payload: dict,
    *,
    source_brief_id: str,
) -> dict:
    """Apply CLI-owned defaults before validating an imported implementation brief."""
    prepared = dict(brief_payload)
    prepared["source_brief_id"] = source_brief_id

    if not prepared.get("id"):
        prepared["id"] = generate_implementation_brief_id()

    if prepared.get("status") not in BRIEF_STATUS_CHOICES:
        prepared["status"] = "draft"

    return prepared


@brief.command()
@click.option("--status", help="Filter by status")
@click.option("--limit", default=50, help="Maximum number of briefs to show")
def list(status: str | None, limit: int):
    """List implementation briefs."""
    config = get_config()
    store = Store(config.db_path)

    briefs = store.list_implementation_briefs(status=status, limit=limit)

    if not briefs:
        click.echo("No implementation briefs found")
        return

    click.echo(f"\n{'ID':<15} {'Title':<40} {'Status':<20} {'Created':<20}")
    click.echo("-" * 95)

    for brief in briefs:
        click.echo(
            f"{brief['id']:<15} "
            f"{brief['title'][:38]:<40} "
            f"{brief['status']:<20} "
            f"{brief['created_at'][:19]:<20}"
        )

    click.echo(f"\nTotal: {len(briefs)} briefs")


@brief.command()
@click.argument("brief_id")
def inspect(brief_id: str):
    """Inspect an implementation brief in detail."""
    config = get_config()
    store = Store(config.db_path)

    brief = store.get_implementation_brief(brief_id)

    if not brief:
        click.echo(f"Implementation brief not found: {brief_id}", err=True)
        return

    click.echo(f"\n{'='*80}")
    click.echo(f"Implementation Brief: {brief['id']}")
    click.echo(f"{'='*80}\n")

    click.echo(f"Title:           {brief['title']}")
    click.echo(f"Domain:          {brief['domain'] or 'N/A'}")
    click.echo(f"Status:          {brief['status']}")
    click.echo(f"Source Brief:    {brief['source_brief_id']}")
    click.echo(f"Created:         {brief['created_at']}")
    click.echo(f"\nProblem Statement:\n{brief['problem_statement']}\n")
    click.echo(f"MVP Goal:\n{brief['mvp_goal']}\n")

    if brief["scope"]:
        click.echo("In Scope:")
        for item in brief["scope"]:
            click.echo(f"  • {item}")
        click.echo()

    if brief["non_goals"]:
        click.echo("Non-Goals:")
        for item in brief["non_goals"]:
            click.echo(f"  • {item}")
        click.echo()


@brief.command()
@click.argument("brief_id")
@click.option("--status", required=True, type=click.Choice(BRIEF_STATUS_CHOICES))
@click.option("--reason", help="Reason for the status change")
def update(brief_id: str, status: str, reason: str | None):
    """Update implementation brief status."""
    config = get_config()
    store = Store(config.db_path)

    if store.update_implementation_brief_status(brief_id, status, reason=reason):
        click.echo(f"✓ Updated brief {brief_id} status to {status}")
    else:
        click.echo(f"Brief not found: {brief_id}", err=True)


@brief.command(name="readiness")
@click.argument("brief_id")
@click.option("--json", "json_output", is_flag=True, help="Output audit results as JSON")
def brief_readiness(brief_id: str, json_output: bool):
    """Audit whether an implementation brief is ready for plan generation."""
    config = get_config()
    store = Store(config.db_path)

    implementation_brief = store.get_implementation_brief(brief_id)
    if not implementation_brief:
        raise click.ClickException(f"Implementation brief not found: {brief_id}")

    result = audit_brief_readiness(implementation_brief)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_brief_readiness(result)

    if not result.passed:
        raise click.exceptions.Exit(1)


@brief.command(name="review-packet")
@click.argument("brief_id")
@click.option(
    "--source",
    "include_source",
    is_flag=True,
    help="Include linked source brief metadata and payload summary",
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write the review packet to a file instead of stdout",
)
def brief_review_packet(brief_id: str, include_source: bool, output: Path | None):
    """Render a Markdown review packet for an implementation brief."""
    config = get_config()
    store = Store(config.db_path)

    implementation_brief = store.get_implementation_brief(brief_id)
    if not implementation_brief:
        raise click.ClickException(f"Implementation brief not found: {brief_id}")

    source_brief = None
    if include_source:
        source_brief = store.get_source_brief(implementation_brief["source_brief_id"])
        if not source_brief:
            raise click.ClickException(
                f"Source brief not found: {implementation_brief['source_brief_id']}"
            )

    packet = BriefReviewPacketExporter().render(
        implementation_brief,
        source_brief=source_brief,
        include_source=include_source,
    )
    _emit_text_payload(packet, output)


def _emit_brief_readiness(result: BriefReadinessResult) -> None:
    """Render human-readable implementation brief readiness results."""
    click.echo(f"Brief readiness audit: {result.brief_id}")
    click.echo(
        f"Result: {'passed' if result.passed else 'failed'} "
        f"({result.blocking_count} blocking, {result.warning_count} warnings)"
    )

    grouped_findings = result.findings_by_severity()
    if grouped_findings["blocking"]:
        click.echo("\nBlocking findings:")
        for finding in grouped_findings["blocking"]:
            click.echo(f"  - {finding.field}: {finding.message}")
            click.echo(f"    Remediation: {finding.remediation}")
    else:
        click.echo("No blocking findings found.")

    if grouped_findings["warning"]:
        click.echo("\nWarnings:")
        for finding in grouped_findings["warning"]:
            click.echo(f"  - {finding.field}: {finding.message}")
            click.echo(f"    Remediation: {finding.remediation}")


@brief.command(name="risk-coverage")
@click.argument("brief_id")
@click.option("--plan-id", required=True, help="Execution plan ID to audit")
@click.option("--json", "json_output", is_flag=True, help="Output audit results as JSON")
def risk_coverage(brief_id: str, plan_id: str, json_output: bool):
    """Audit whether implementation risks are covered by an execution plan."""
    config = get_config()
    store = Store(config.db_path)

    implementation_brief = store.get_implementation_brief(brief_id)
    if not implementation_brief:
        raise click.ClickException(f"Implementation brief not found: {brief_id}")

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    plan_brief_id = str(plan.get("implementation_brief_id") or "")
    if plan_brief_id != brief_id:
        raise click.ClickException(
            f"Execution plan {plan_id} is linked to implementation brief "
            f"{plan_brief_id or 'N/A'}, not {brief_id}"
        )

    result = audit_risk_coverage(implementation_brief, plan)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_risk_coverage(result)

    if not result.ok:
        raise click.exceptions.Exit(1)


def _emit_risk_coverage(result: RiskCoverageResult) -> None:
    """Render human-readable implementation risk coverage results."""
    click.echo(f"Risk coverage audit: {result.brief_id} against {result.plan_id}")
    click.echo(
        f"Result: {'passed' if result.ok else 'failed'} "
        f"({len(result.covered_risks)}/{len(result.risks)} risks covered, "
        f"{result.coverage_ratio:.2%})"
    )

    if not result.risks:
        click.echo("No implementation risks found.")
        return

    if result.uncovered_risks:
        click.echo("\nUncovered risks:")
        for risk in result.uncovered_risks:
            click.echo(f"  - {risk.risk}")
    else:
        click.echo("No uncovered risks found.")

    if result.covered_risks:
        click.echo("\nCovered risks:")
        for risk in result.covered_risks:
            click.echo(f"  - {risk.risk} (tasks: {', '.join(risk.matching_task_ids)})")


# ============================================================================
# Execution Plan Commands
# ============================================================================


@cli.group()
def plan():
    """Manage execution plans."""
    pass


@plan.command(name="prompt")
@click.argument("brief_id")
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write the prompt to a file instead of stdout",
)
def plan_prompt(brief_id: str, output: Path | None):
    """Render the execution plan generation prompt for an implementation brief."""
    config = get_config()
    store = Store(config.db_path)

    implementation_brief = store.get_implementation_brief(brief_id)
    if not implementation_brief:
        raise click.ClickException(f"Implementation brief not found: {brief_id}")

    _emit_prompt_preview(PlanGenerator.build_prompt(implementation_brief), output)


@plan.command(name="estimate")
@click.argument("brief_id")
@click.option(
    "--model",
    type=click.Choice(["opus", "sonnet"]),
    default="opus",
    help="LLM model to estimate (opus=claude-opus-4-6, sonnet=claude-sonnet-4-5)",
)
@click.option("--json", "json_output", is_flag=True, help="Output estimate as JSON")
def plan_estimate(brief_id: str, model: str, json_output: bool):
    """Estimate execution plan generation prompt size and input cost."""
    config = get_config()
    store = Store(config.db_path)

    implementation_brief = store.get_implementation_brief(brief_id)
    if not implementation_brief:
        raise click.ClickException(f"Implementation brief not found: {brief_id}")

    estimate = estimate_prompt(PlanGenerator.build_prompt(implementation_brief), model=model)
    _emit_prompt_estimate(estimate, json_output)


@plan.command()
@click.argument("brief_id")
@click.option(
    "--model", type=click.Choice(["opus", "sonnet"]), default="opus", help="LLM model to use"
)
@click.option(
    "--staged",
    is_flag=True,
    default=False,
    help="Use staged generation (fixes JSON parsing issues)",
)
def create(brief_id: str, model: str, staged: bool):
    """Generate execution plan from implementation brief."""
    config = get_config()
    store = Store(config.db_path)

    try:
        # Get implementation brief
        implementation_brief = store.get_implementation_brief(brief_id)
        if not implementation_brief:
            click.echo(f"✗ Implementation brief not found: {brief_id}", err=True)
            return

        # Initialize LLM client and generator
        llm_client = _create_llm_provider(config)

        if staged:
            generator = StagedPlanGenerator(llm_client)
            click.echo(f"Using STAGED generation (fixes JSON parsing issues)")
        else:
            generator = PlanGenerator(llm_client)

        # Generate plan
        click.echo(f"Generating execution plan for {brief_id} using {model}...")
        click.echo(f"Brief: {implementation_brief['title']}")

        if staged:
            click.echo(f"\n⏳ Stage 1: Generating milestones...")
            click.echo(f"⏳ Stage 2: Generating tasks per milestone...")
            click.echo(f"⏳ Stage 3: Generating plan metadata...")
            click.echo(f"(This may take 30-90 seconds total)\n")
        else:
            click.echo(
                f"\n⏳ Calling {_resolve_llm_model(model)}... " f"(this may take 15-45 seconds)\n"
            )

        execution_plan, tasks = generator.generate(
            implementation_brief=implementation_brief,
            model=_resolve_llm_model(model),
        )

        # Store in database
        plan_id = store.insert_execution_plan(execution_plan, tasks)

        # Success message
        click.echo(f"✓ Generated execution plan {plan_id}")
        click.echo(f"  Milestones: {len(execution_plan['milestones'])}")
        click.echo(f"  Tasks: {len(tasks)}")
        click.echo(f"  Target: {execution_plan['target_engine'] or 'mixed'}")
        click.echo(f"  Project Type: {execution_plan['project_type']}")
        click.echo(f"  Tokens used: {execution_plan['generation_tokens']}")
        click.echo(f"\nView full plan: blueprint plan inspect {plan_id}")

    except ValueError as e:
        if "ANTHROPIC_API_KEY" in str(e):
            click.echo(f"✗ Error: {e}", err=True)
            click.echo("  Set ANTHROPIC_API_KEY environment variable", err=True)
        else:
            click.echo(f"✗ Generation failed: {e}", err=True)
    except Exception as e:
        click.echo(f"✗ Generation failed: {e}", err=True)
        import traceback

        click.echo(traceback.format_exc(), err=True)


@plan.command()
@click.argument("plan_id")
@click.option(
    "--feedback",
    required=True,
    help="Human feedback text, or a path to a file containing feedback",
)
@click.option(
    "--model",
    type=click.Choice(["opus", "sonnet"]),
    default="opus",
    help="LLM model to use",
)
def revise(plan_id: str, feedback: str, model: str):
    """Generate a revised execution plan from an existing plan and feedback."""
    config = get_config()
    store = Store(config.db_path)

    try:
        existing_plan = store.get_execution_plan(plan_id)
        if not existing_plan:
            click.echo(f"✗ Execution plan not found: {plan_id}", err=True)
            return

        implementation_brief = store.get_implementation_brief(
            existing_plan["implementation_brief_id"]
        )
        if not implementation_brief:
            click.echo(
                f"✗ Implementation brief not found: " f"{existing_plan['implementation_brief_id']}",
                err=True,
            )
            return

        feedback_text, feedback_source = _read_feedback(feedback)
        if not feedback_text.strip():
            raise click.UsageError("--feedback cannot be empty")

        llm_client = _create_llm_provider(config)
        generator = PlanReviser(llm_client)

        click.echo(f"Revising execution plan {plan_id} using {model}...")
        click.echo(f"Brief: {implementation_brief['title']}")
        click.echo(
            f"\n⏳ Calling {_resolve_llm_model(model)}... " f"(this may take 15-45 seconds)\n"
        )

        revised_plan, tasks = generator.generate(
            implementation_brief=implementation_brief,
            existing_plan=existing_plan,
            feedback=feedback_text,
            model=_resolve_llm_model(model),
            feedback_source=feedback_source,
        )

        revised_plan_id = store.insert_execution_plan(revised_plan, tasks)

        click.echo(f"✓ Generated revised execution plan {revised_plan_id}")
        click.echo(f"  Revised from: {plan_id}")
        click.echo(f"  Milestones: {len(revised_plan['milestones'])}")
        click.echo(f"  Tasks: {len(tasks)}")
        click.echo(f"  Target: {revised_plan['target_engine'] or 'mixed'}")
        click.echo(f"  Project Type: {revised_plan['project_type']}")
        click.echo(f"  Tokens used: {revised_plan['generation_tokens']}")
        click.echo(f"\nView full plan: blueprint plan inspect {revised_plan_id}")

    except ValueError as e:
        if "ANTHROPIC_API_KEY" in str(e):
            click.echo(f"✗ Error: {e}", err=True)
            click.echo("  Set ANTHROPIC_API_KEY environment variable", err=True)
        else:
            click.echo(f"✗ Revision failed: {e}", err=True)
    except click.ClickException:
        raise
    except Exception as e:
        click.echo(f"✗ Revision failed: {e}", err=True)
        import traceback

        click.echo(traceback.format_exc(), err=True)


def _read_feedback(feedback: str) -> tuple[str, str]:
    """Read feedback from a file path when it exists, otherwise use inline text."""
    feedback_path = Path(feedback)
    if feedback_path.exists():
        if not feedback_path.is_file():
            raise click.UsageError("--feedback path must point to a file")
        return feedback_path.read_text(), str(feedback_path)
    return feedback, "inline"


@plan.command()
@click.argument("plan_id")
@click.option(
    "--preserve-statuses",
    is_flag=True,
    help="Keep the source plan and task statuses on the clone",
)
@click.option("--json", "json_output", is_flag=True, help="Output clone details as JSON")
def clone(plan_id: str, preserve_statuses: bool, json_output: bool):
    """Clone an execution plan so it can be branched safely."""
    config = get_config()
    store = Store(config.db_path)

    cloned_plan_id = store.clone_execution_plan(
        plan_id,
        reset_statuses=not preserve_statuses,
    )
    if not cloned_plan_id:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    cloned_plan = store.get_execution_plan(cloned_plan_id)
    lineage = ((cloned_plan or {}).get("metadata") or {}).get("lineage") or {}
    task_id_map = lineage.get("task_id_map") or {}

    if json_output:
        click.echo(
            json.dumps(
                {
                    "id": cloned_plan_id,
                    "cloned_from_plan_id": plan_id,
                    "task_id_map": task_id_map,
                    "statuses_reset": not preserve_statuses,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return

    click.echo(f"✓ Cloned execution plan {plan_id} to {cloned_plan_id}")
    click.echo(f"  Tasks: {len(task_id_map)}")
    click.echo(f"  Statuses: {'preserved' if preserve_statuses else 'reset'}")
    click.echo(f"\nView full plan: blueprint plan inspect {cloned_plan_id}")


@plan.command(name="import-markdown")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--brief",
    "brief_id",
    required=True,
    help="Implementation brief ID to attach the imported plan to",
)
def plan_import_markdown(file: Path, brief_id: str):
    """Import an execution plan from structured markdown."""
    config = get_config()
    store = Store(config.db_path)
    importer = PlanMarkdownImporter()

    if not store.get_implementation_brief(brief_id):
        raise click.ClickException(f"Implementation brief not found: {brief_id}")

    try:
        parsed = importer.import_file(file, implementation_brief_id=brief_id)
        plan_id = store.insert_execution_plan(parsed.plan, parsed.tasks)
    except PlanMarkdownImportError as exc:
        raise click.ClickException(str(exc)) from exc
    except ValidationError as exc:
        raise click.ClickException(f"Imported plan failed validation: {exc}") from exc

    click.echo(f"✓ Imported execution plan {plan_id}")
    click.echo(f"  Brief: {brief_id}")
    click.echo(f"  Tasks: {len(parsed.tasks)}")
    click.echo(f"\nView full plan: blueprint plan inspect {plan_id}")


@plan.command()
@click.option("--brief-id", help="Filter by implementation brief ID")
@click.option("--status", help="Filter by status")
@click.option("--limit", default=50, help="Maximum number of plans to show")
def list(brief_id: str | None, status: str | None, limit: int):
    """List execution plans."""
    config = get_config()
    store = Store(config.db_path)

    plans = store.list_execution_plans(brief_id=brief_id, status=status, limit=limit)

    if not plans:
        click.echo("No execution plans found")
        return

    click.echo(f"\n{'ID':<15} {'Brief ID':<15} {'Target':<15} {'Status':<15} {'Created':<20}")
    click.echo("-" * 80)

    for plan in plans:
        target = plan["target_engine"] or "N/A"
        click.echo(
            f"{plan['id']:<15} "
            f"{plan['implementation_brief_id']:<15} "
            f"{target:<15} "
            f"{plan['status']:<15} "
            f"{plan['created_at'][:19]:<20}"
        )

    click.echo(f"\nTotal: {len(plans)} plans")


@plan.command(name="search")
@click.argument("query")
@click.option("--status", type=click.Choice(PLAN_STATUS_CHOICES), help="Filter by status")
@click.option("--target-engine", help="Filter by target engine")
@click.option("--limit", default=50, show_default=True, help="Maximum number of plans to show")
@click.option("--json", "json_output", is_flag=True, help="Output search results as JSON")
def plan_search(
    query: str,
    status: str | None,
    target_engine: str | None,
    limit: int,
    json_output: bool,
):
    """Search execution plans and tasks."""
    config = get_config()
    store = Store(config.db_path)

    results = store.search_execution_plans(
        query,
        status=status,
        target_engine=target_engine,
        limit=limit,
    )

    if json_output:
        click.echo(json.dumps(results, indent=2, sort_keys=True))
        return

    if not results:
        click.echo("No execution plans matched")
        return

    click.echo(f"\nSearch results for: {query}")
    click.echo("-" * 80)
    for result in results:
        target = result["target_engine"] or "N/A"
        task_ids = ", ".join(result["matched_task_ids"]) or "N/A"
        fields = ", ".join(result["matched_fields"])
        click.echo(f"{result['plan_id']} ({result['status']}, {target})")
        click.echo(f"  Tasks:  {task_ids}")
        click.echo(f"  Fields: {fields}")
        for match in result["matches"]:
            task_suffix = f" [{match['task_id']}]" if match.get("task_id") else ""
            click.echo(f"  - {match['field']}{task_suffix}: {match['snippet']}")
        click.echo("")

    click.echo(f"Total: {len(results)} plans")


@plan.command()
@click.argument("plan_id")
@click.option("--status", required=True, type=click.Choice(PLAN_STATUS_CHOICES))
@click.option("--reason", help="Reason for the status change")
def update(plan_id: str, status: str, reason: str | None):
    """Update execution plan status."""
    config = get_config()
    store = Store(config.db_path)

    if store.update_execution_plan_status(plan_id, status, reason=reason):
        click.echo(f"✓ Updated plan {plan_id} status to {status}")
    else:
        click.echo(f"Execution plan not found: {plan_id}", err=True)


@plan.command(name="graph")
@click.argument("plan_id")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["dot", "json"]),
    default="dot",
    show_default=True,
    help="Graph output format",
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write graph to a file instead of stdout",
)
def plan_graph(plan_id: str, output_format: str, output: Path | None):
    """Export a task dependency graph as DOT or JSON."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    exporter = PlanGraphExporter()
    try:
        content = exporter.render(plan, output_format)
    except UnknownDependencyError as e:
        raise click.ClickException(str(e)) from e

    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(content)
        click.echo(f"✓ Exported {output_format.upper()} graph to: {output}")
        return

    click.echo(content, nl=False)


@plan.command(name="dependency-matrix")
@click.argument("plan_id")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json"]),
    default="json",
    show_default=True,
    help="Dependency matrix output format",
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write dependency matrix to a file instead of stdout",
)
def plan_dependency_matrix(plan_id: str, output_format: str, output: Path | None):
    """Export the full task dependency matrix as JSON."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    exporter = DependencyMatrixExporter()
    try:
        payload = exporter.render(plan, output_format)
    except UnknownDependencyError as e:
        raise click.ClickException(str(e)) from e

    _emit_json_payload(payload, output)


@plan.command()
@click.argument("left_plan_id")
@click.argument("right_plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output diff as JSON")
def diff(left_plan_id: str, right_plan_id: str, json_output: bool):
    """Compare two execution plans and summarize the changes."""
    config = get_config()
    store = Store(config.db_path)

    left_plan = store.get_execution_plan(left_plan_id)
    if not left_plan:
        raise click.ClickException(f"Execution plan not found: {left_plan_id}")

    right_plan = store.get_execution_plan(right_plan_id)
    if not right_plan:
        raise click.ClickException(f"Execution plan not found: {right_plan_id}")

    result = diff_execution_plans(left_plan, right_plan)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_plan_diff(result)


@plan.command()
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output audit results as JSON")
def audit(plan_id: str, json_output: bool):
    """Audit whether an execution plan is structurally executable."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = audit_execution_plan(plan)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_plan_audit(result)

    if not result.ok:
        raise click.exceptions.Exit(1)


@plan.command(name="dependency-repair")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output repair suggestions as JSON")
@click.option("--apply", "apply_edits", is_flag=True, help="Apply qualifying dependency edits")
@click.option(
    "--min-confidence",
    type=click.FloatRange(0.0, 1.0),
    default=0.8,
    show_default=True,
    help="Minimum suggestion confidence required when applying edits",
)
def dependency_repair(
    plan_id: str,
    json_output: bool,
    apply_edits: bool,
    min_confidence: float,
):
    """Suggest or apply dependency edits for an execution plan."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = suggest_dependency_repairs(plan)

    if apply_edits:
        applied_edits = _apply_dependency_repairs(
            store,
            plan,
            result,
            min_confidence=min_confidence,
        )
        applied_plan = store.get_execution_plan(plan_id)
        if not applied_plan:
            raise click.ClickException(f"Execution plan not found after apply: {plan_id}")
        audit_result = audit_execution_plan(applied_plan)
        payload = {
            "plan_id": plan_id,
            "ok": audit_result.ok,
            "summary": {
                "applied": len(applied_edits),
                "audit_errors": audit_result.error_count,
                "audit_warnings": audit_result.warning_count,
            },
            "min_confidence": min_confidence,
            "applied_edits": applied_edits,
            "audit": audit_result.to_dict(),
        }

        if json_output:
            click.echo(json.dumps(payload, indent=2, sort_keys=True))
        else:
            _emit_dependency_repair_apply(payload)

        if not audit_result.ok:
            raise click.exceptions.Exit(1)
        return

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_dependency_repair(result)

    if not result.ok:
        raise click.exceptions.Exit(1)


@plan.command(name="milestone-dependencies")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output audit results as JSON")
def milestone_dependencies(plan_id: str, json_output: bool):
    """Audit milestone ordering and cross-milestone dependencies."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = audit_milestone_dependencies(plan)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_milestone_dependencies(result)

    if not result.ok:
        raise click.exceptions.Exit(1)


def _emit_plan_audit(result: PlanAuditResult) -> None:
    """Render human-readable plan audit results grouped by severity."""
    click.echo(f"Execution plan audit: {result.plan_id}")
    click.echo(
        f"Result: {'passed' if result.ok else 'failed'} "
        f"({result.error_count} errors, {result.warning_count} warnings)"
    )

    if not result.issues:
        click.echo("No structural issues found.")
        return

    by_severity = result.issues_by_severity()
    for severity, heading in (("error", "Errors"), ("warning", "Warnings")):
        issues = by_severity[severity]
        if not issues:
            continue
        click.echo(f"\n{heading}:")
        for issue in issues:
            click.echo(f"  - [{issue.code}] {issue.message}")


def _emit_milestone_dependencies(result: MilestoneDependencyResult) -> None:
    """Render human-readable milestone dependency findings by milestone."""
    click.echo(f"Milestone dependency audit: {result.plan_id}")
    click.echo(
        f"Result: {'passed' if result.ok else 'failed'} "
        f"({result.error_count} errors, {result.warning_count} warnings)"
    )

    if not result.findings:
        click.echo("No milestone dependency issues found.")
        return

    click.echo("\nFindings by milestone:")
    for milestone, findings in result.findings_by_milestone().items():
        click.echo(f"  {milestone}:")
        for finding in findings:
            target = f" Task {finding.task_id}:" if finding.task_id else ""
            click.echo(f"    - [{finding.severity}] {finding.code}:{target} " f"{finding.message}")
            if finding.dependency_task_id:
                click.echo(
                    f"      Dependency: {finding.dependency_task_id} "
                    f"({finding.dependency_milestone or 'unspecified'})"
                )
            if finding.chain_task_ids:
                click.echo("      Chain: " + " -> ".join(finding.chain_task_ids))


def _emit_dependency_repair(result: DependencyRepairResult) -> None:
    """Render actionable dependency repair suggestions."""
    click.echo(f"Dependency repair suggestions: {result.plan_id}")
    click.echo(
        f"Result: {'clean' if result.ok else 'repairs suggested'} "
        f"({result.suggestion_count} suggestions)"
    )

    if not result.suggestions:
        click.echo("No dependency repair suggestions found.")
        return

    click.echo("\nSuggested edits:")
    for suggestion in result.suggestions:
        if suggestion.action == "replace_dependency":
            edit = (
                f"replace {suggestion.dependency_id} with "
                f"{suggestion.replacement_dependency_id}"
            )
        else:
            edit = f"remove {suggestion.dependency_id}"
        affected = ", ".join(suggestion.affected_task_ids) or suggestion.task_id
        click.echo(
            f"  - [{suggestion.action}] Task {suggestion.task_id}: {edit} "
            f"(confidence {suggestion.confidence:.2f}; affected: {affected})"
        )
        click.echo(f"    Rationale: {suggestion.rationale}")


def _apply_dependency_repairs(
    store: Store,
    plan_dict: dict[str, Any],
    result: DependencyRepairResult,
    *,
    min_confidence: float,
) -> list[dict[str, Any]]:
    """Apply concrete dependency repair suggestions to the stored plan."""
    tasks_by_id = {
        str(task.get("id") or ""): task
        for task in plan_dict.get("tasks", [])
        if isinstance(task, dict) and task.get("id")
    }
    applied_edits: list[dict[str, Any]] = []

    for suggestion in result.suggestions:
        if suggestion.confidence < min_confidence:
            continue
        if suggestion.action not in {"remove_dependency", "replace_dependency"}:
            continue
        if suggestion.action == "replace_dependency" and not suggestion.replacement_dependency_id:
            continue

        task = tasks_by_id.get(suggestion.task_id)
        if not task:
            continue

        before = _string_list(task.get("depends_on"))
        if suggestion.action == "remove_dependency":
            after = [
                dependency_id
                for dependency_id in before
                if dependency_id != suggestion.dependency_id
            ]
        else:
            after = _replace_dependency(
                before,
                suggestion.dependency_id,
                str(suggestion.replacement_dependency_id),
            )

        if before == after:
            continue

        updated = store.update_execution_task_dependencies(
            result.plan_id,
            suggestion.task_id,
            after,
        )
        if not updated:
            continue

        task["depends_on"] = after
        edit = {
            "action": suggestion.action,
            "task_id": suggestion.task_id,
            "dependency_id": suggestion.dependency_id,
            "confidence": suggestion.confidence,
            "before_depends_on": before,
            "after_depends_on": after,
        }
        if suggestion.replacement_dependency_id is not None:
            edit["replacement_dependency_id"] = suggestion.replacement_dependency_id
        applied_edits.append(edit)

    return applied_edits


def _replace_dependency(
    dependencies: list[str],
    dependency_id: str,
    replacement_dependency_id: str,
) -> list[str]:
    """Replace one dependency while preserving order and avoiding duplicates."""
    replaced: list[str] = []
    for current_dependency_id in dependencies:
        next_dependency_id = (
            replacement_dependency_id
            if current_dependency_id == dependency_id
            else current_dependency_id
        )
        if next_dependency_id not in replaced:
            replaced.append(next_dependency_id)
    return replaced


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, builtin_list):
        return []
    return [str(item) for item in value if isinstance(item, str) and item.strip()]


def _emit_dependency_repair_apply(payload: dict[str, Any]) -> None:
    """Render applied dependency repair edits."""
    click.echo(f"Dependency repair apply: {payload['plan_id']}")
    summary = payload["summary"]
    click.echo(
        f"Applied: {summary['applied']} edits " f"(min confidence {payload['min_confidence']:.2f})"
    )
    click.echo(
        "Post-apply audit: "
        f"{'passed' if payload['audit']['ok'] else 'failed'} "
        f"({summary['audit_errors']} errors, {summary['audit_warnings']} warnings)"
    )

    if not payload["applied_edits"]:
        click.echo("No qualifying dependency edits applied.")
        return

    click.echo("\nApplied edits:")
    for edit in payload["applied_edits"]:
        if edit["action"] == "replace_dependency":
            action = f"replace {edit['dependency_id']} with " f"{edit['replacement_dependency_id']}"
        else:
            action = f"remove {edit['dependency_id']}"
        click.echo(
            f"  - [{edit['action']}] Task {edit['task_id']}: {action} "
            f"(confidence {edit['confidence']:.2f})"
        )


@plan.command()
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output audit results as JSON")
def coherence(plan_id: str, json_output: bool):
    """Audit whether an execution plan coherently reflects its implementation brief."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    implementation_brief_id = plan.get("implementation_brief_id")
    implementation_brief = store.get_implementation_brief(str(implementation_brief_id or ""))
    if not implementation_brief:
        raise click.ClickException(
            f"Implementation brief not found: {implementation_brief_id or 'N/A'}"
        )

    result = audit_brief_plan_coherence(plan, implementation_brief)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_brief_plan_coherence(result)

    if not result.ok:
        raise click.exceptions.Exit(1)


@plan.command(name="acceptance-audit")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output audit results as JSON")
@click.option(
    "--min-length",
    type=click.IntRange(1),
    default=ACCEPTANCE_QUALITY_DEFAULT_MIN_LENGTH,
    show_default=True,
    help="Minimum acceptance criterion length",
)
def plan_acceptance_audit(plan_id: str, json_output: bool, min_length: int):
    """Audit task acceptance criteria for observable validation quality."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = audit_acceptance_quality(plan, min_length=min_length)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_acceptance_quality(result)

    if not result.passed:
        raise click.exceptions.Exit(1)


def _emit_acceptance_quality(result: AcceptanceQualityResult) -> None:
    """Render human-readable acceptance criteria quality results."""
    click.echo(f"Acceptance criteria quality audit: {result.plan_id}")
    click.echo(
        f"Result: {'passed' if result.passed else 'failed'} "
        f"({result.high_count} high, {result.medium_count} medium findings)"
    )

    grouped_findings = result.findings_by_task()
    if not grouped_findings:
        click.echo("No weak acceptance criteria found.")
        return

    click.echo("\nFindings by task:")
    for task in result.tasks:
        findings = grouped_findings.get(task.task_id)
        if not findings:
            continue
        click.echo(f"  {task.task_id} ({task.title}):")
        for finding in findings:
            criterion = finding.criterion_text or "<missing>"
            click.echo(f"    - [{finding.severity}] {finding.code}: {criterion}")
            click.echo(f"      Reason: {finding.reason}")


@plan.command(name="readiness")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output readiness as JSON")
def plan_readiness(plan_id: str, json_output: bool):
    """Evaluate whether an execution plan is ready for autonomous handoff."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    implementation_brief_id = plan.get("implementation_brief_id")
    implementation_brief = store.get_implementation_brief(str(implementation_brief_id or ""))
    if not implementation_brief:
        raise click.ClickException(
            f"Implementation brief not found: {implementation_brief_id or 'N/A'}"
        )

    result = evaluate_plan_readiness(plan, implementation_brief)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_plan_readiness(result)

    if not result.ready:
        raise click.exceptions.Exit(1)


def _emit_plan_readiness(result: PlanReadinessResult) -> None:
    """Render human-readable aggregate readiness results."""
    click.echo(f"Plan readiness: {result.plan_id}")
    click.echo(f"Implementation brief: {result.implementation_brief_id}")
    click.echo(
        f"Result: {'ready' if result.ready else 'blocked'} "
        f"({len(result.blocking_reasons)} blocking reasons)"
    )

    click.echo("\nComponents:")
    click.echo(
        "  - Plan audit: "
        f"{'passed' if result.plan_audit.ok else 'failed'} "
        f"({result.plan_audit.error_count} errors, "
        f"{result.plan_audit.warning_count} warnings)"
    )
    click.echo(
        "  - Task completeness: "
        f"{'passed' if result.task_completeness.passed else 'failed'} "
        f"(score {result.task_completeness.score}/100, "
        f"{result.task_completeness.blocking_count} blocking, "
        f"{result.task_completeness.warning_count} warnings)"
    )
    click.echo(
        "  - Brief-plan coherence: "
        f"{'passed' if result.brief_plan_coherence.ok else 'failed'} "
        f"({result.brief_plan_coherence.error_count} errors, "
        f"{result.brief_plan_coherence.warning_count} warnings)"
    )
    click.echo(
        "  - Risk coverage: "
        f"{'passed' if result.risk_coverage.ok else 'failed'} "
        f"({len(result.risk_coverage.covered_risks)}/"
        f"{len(result.risk_coverage.risks)} risks covered)"
    )
    counts = result.env_inventory_counts
    click.echo(
        "  - Environment inventory: "
        f"{len(result.env_inventory.items)} items "
        f"({counts.required} required, {counts.optional} optional, "
        f"{counts.unknown} unknown, {counts.missing_required} missing required)"
    )

    if not result.blocking_reasons:
        click.echo("\nNo blocking reasons found.")
        return

    click.echo("\nBlocking reasons:")
    for reason in result.blocking_reasons:
        prefix = f"[{reason.component}:{reason.code}]"
        if reason.task_id:
            click.echo(f"  - {prefix} {reason.task_id}: {reason.message}")
        elif reason.item_name:
            click.echo(f"  - {prefix} {reason.item_name}: {reason.message}")
        else:
            click.echo(f"  - {prefix} {reason.message}")


def _emit_plan_diff(result: PlanDiffResult) -> None:
    """Render a human-readable execution plan diff."""
    click.echo(f"Execution plan diff: {result.left_plan_id} -> {result.right_plan_id}")
    click.echo(
        "Summary: "
        f"{len(result.added_milestones)} milestone additions, "
        f"{len(result.removed_milestones)} milestone removals, "
        f"{len(result.changed_milestones)} milestone changes, "
        f"{len(result.added_tasks)} task additions, "
        f"{len(result.removed_tasks)} task removals, "
        f"{len(result.changed_tasks)} task changes"
    )

    if not result.has_changes:
        click.echo("No differences found.")
        return

    _emit_plan_diff_section("Milestones added", result.added_milestones)
    _emit_plan_diff_section("Milestones removed", result.removed_milestones)
    if result.changed_milestones:
        click.echo("\nMilestones changed:")
        for milestone_change in result.changed_milestones:
            _emit_plan_diff_record(
                milestone_change.milestone_key,
                milestone_change.left,
                milestone_change.right,
                milestone_change.changes,
                indent="  ",
            )

    _emit_plan_diff_section("Tasks added", result.added_tasks)
    _emit_plan_diff_section("Tasks removed", result.removed_tasks)
    if result.changed_tasks:
        click.echo("\nTasks changed:")
        for task_change in result.changed_tasks:
            _emit_plan_diff_record(
                task_change.task_key,
                task_change.left,
                task_change.right,
                task_change.changes,
                indent="  ",
            )


def _emit_plan_diff_section(heading: str, items: list[dict[str, object]]) -> None:
    if not items:
        return
    click.echo(f"\n{heading}:")
    for item in items:
        _emit_plan_diff_snapshot(item)


def _emit_plan_diff_snapshot(item: dict[str, object]) -> None:
    label = item.get("id") or item.get("name") or item.get("title") or "unknown"
    summary_bits = []
    title = item.get("title")
    milestone = item.get("milestone")
    status = item.get("status")
    depends_on = item.get("depends_on")
    if isinstance(title, str) and title:
        summary_bits.append(title)
    if isinstance(milestone, str) and milestone:
        summary_bits.append(f"milestone={milestone}")
    if isinstance(status, str) and status:
        summary_bits.append(f"status={status}")
    if isinstance(depends_on, builtin_list) and depends_on:
        summary_bits.append(f"depends_on={', '.join(str(dep) for dep in depends_on)}")

    if summary_bits:
        click.echo(f"  - {label}: {', '.join(summary_bits)}")
    else:
        click.echo(f"  - {label}")


def _emit_plan_diff_record(
    item_label: str,
    left: dict[str, object],
    right: dict[str, object],
    changes,
    indent: str,
) -> None:
    title = left.get("title") or right.get("title")
    if title:
        click.echo(f"{indent}- {item_label}: {title}")
    else:
        click.echo(f"{indent}- {item_label}")
    for change in changes:
        left_value = _format_diff_value(change.left)
        right_value = _format_diff_value(change.right)
        click.echo(f"{indent}  {change.field}: {left_value} -> {right_value}")


def _format_diff_value(value: object) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, builtin_list):
        return ", ".join(_format_diff_value(item) for item in value) if value else "[]"
    return str(value)


def _emit_brief_plan_coherence(result: BriefPlanCoherenceResult) -> None:
    """Render human-readable brief/plan coherence results grouped by severity."""
    click.echo(f"Brief-plan coherence audit: {result.plan_id}")
    click.echo(
        f"Result: {'passed' if result.ok else 'failed'} "
        f"({result.error_count} errors, {result.warning_count} warnings)"
    )

    if not result.issues:
        click.echo("No coherence issues found.")
        return

    by_severity = result.issues_by_severity()
    for severity, heading in (("error", "Errors"), ("warning", "Warnings")):
        issues = by_severity[severity]
        if not issues:
            continue
        click.echo(f"\n{heading}:")
        for issue in issues:
            click.echo(f"  - [{issue.code}] {issue.message}")


@plan.command()
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output metrics as JSON")
def metrics(plan_id: str, json_output: bool):
    """Summarize execution plan size and readiness."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = calculate_plan_metrics(plan)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return

    _emit_plan_metrics(result)


def _emit_plan_metrics(result: PlanMetrics) -> None:
    """Render human-readable execution plan metrics."""
    click.echo(f"Execution plan metrics: {result.plan_id}")
    click.echo(f"\n{'Metric':<32} Value")
    click.echo("-" * 48)
    for label, value in (
        ("Tasks", result.task_count),
        ("Milestones", result.milestone_count),
        ("Ready tasks", result.ready_task_count),
        ("Blocked tasks", result.blocked_task_count),
        ("Completed", f"{result.completed_percent:.2f}%"),
        ("Dependency edges", result.dependency_edge_count),
        ("Avg dependencies per task", f"{result.average_dependencies_per_task:.2f}"),
    ):
        click.echo(f"{label:<32} {value}")

    _emit_metric_counts("Status", result.counts_by_status)
    _emit_metric_counts("Suggested engine", result.counts_by_suggested_engine)
    _emit_metric_counts("Estimated complexity", result.counts_by_estimated_complexity)


def _emit_metric_counts(label: str, counts: dict[str, int]) -> None:
    click.echo(f"\n{label}:")
    if not counts:
        click.echo("  none")
        return
    for key, count in counts.items():
        click.echo(f"  {key}: {count}")


@plan.command(name="env-inventory")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output inventory as JSON")
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write inventory to a file instead of stdout",
)
def env_inventory(plan_id: str, json_output: bool, output: Path | None):
    """Extract environment variables and config keys from an execution plan."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    implementation_brief_id = plan.get("implementation_brief_id")
    implementation_brief = store.get_implementation_brief(str(implementation_brief_id or ""))
    if not implementation_brief:
        raise click.ClickException(
            f"Implementation brief not found: {implementation_brief_id or 'N/A'}"
        )

    result = build_env_inventory(implementation_brief, plan)
    if json_output:
        _emit_json_payload(result.to_dict(), output)
        return

    _emit_text_payload(_render_env_inventory(result), output)


def _render_env_inventory(result: EnvInventoryResult) -> str:
    """Render a human-readable environment inventory."""
    lines = [
        f"Plan environment inventory: {result.plan_id}",
        f"Implementation brief: {result.brief_id}",
        f"Items: {len(result.items)}",
    ]

    for status, heading in (
        ("required", "Required"),
        ("optional", "Optional"),
        ("unknown", "Unknown"),
    ):
        items = result.items_by_status()[status]
        lines.append("")
        lines.append(f"{heading}:")
        if not items:
            lines.append("  none")
            continue
        for item in items:
            task_ids = ", ".join(item.task_ids) if item.task_ids else "none"
            fields = ", ".join(item.source_fields)
            lines.append(f"  - {item.name} ({item.item_type})")
            lines.append(f"    Fields: {fields}")
            lines.append(f"    Tasks: {task_ids}")
            lines.append(f"    Docs: {item.suggested_documentation}")

    return "\n".join(lines) + "\n"


@plan.command()
@click.argument("plan_id")
def inspect(plan_id: str):
    """Inspect an execution plan in detail."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)

    if not plan:
        click.echo(f"Execution plan not found: {plan_id}", err=True)
        return

    click.echo(f"\n{'='*80}")
    click.echo(f"Execution Plan: {plan['id']}")
    click.echo(f"{'='*80}\n")

    click.echo(f"Implementation Brief: {plan['implementation_brief_id']}")
    click.echo(f"Target Engine:        {plan['target_engine'] or 'N/A'}")
    click.echo(f"Target Repo:          {plan['target_repo'] or 'N/A'}")
    click.echo(f"Project Type:         {plan['project_type'] or 'N/A'}")
    click.echo(f"Status:               {plan['status']}")
    click.echo(f"Created:              {plan['created_at']}")
    plan_metadata = plan.get("metadata") or {}
    lineage = plan_metadata.get("lineage") or plan_metadata
    if lineage.get("revised_from_plan_id"):
        click.echo(f"Revised From:         {lineage['revised_from_plan_id']}")
    if lineage.get("cloned_from_plan_id"):
        click.echo(f"Cloned From:          {lineage['cloned_from_plan_id']}")

    if plan["milestones"]:
        click.echo(f"\nMilestones ({len(plan['milestones'])}):")
        for i, milestone in enumerate(plan["milestones"], 1):
            click.echo(f"  {i}. {milestone.get('title', 'Untitled')}")
            if milestone.get("description"):
                click.echo(f"     {milestone['description']}")

    if plan["tasks"]:
        click.echo(f"\nTasks ({len(plan['tasks'])}):")
        for task in plan["tasks"]:
            click.echo(f"  • {task['title']} ({task['status']})")
            if task["milestone"]:
                click.echo(f"    Milestone: {task['milestone']}")
            if task["depends_on"]:
                click.echo(f"    Depends on: {', '.join(task['depends_on'])}")


# ============================================================================
# Execution Task Commands
# ============================================================================


@cli.group()
def task():
    """Manage execution tasks."""
    pass


@task.command()
@click.option("--plan-id", help="Filter by execution plan ID")
@click.option("--status", help="Filter by status")
@click.option("--milestone", help="Filter by milestone")
@click.option("--limit", default=50, help="Maximum number of tasks to show")
def list(plan_id: str, status: str | None, milestone: str | None, limit: int):
    """List execution tasks."""
    config = get_config()
    store = Store(config.db_path)

    tasks = store.list_execution_tasks(
        plan_id=plan_id,
        status=status,
        milestone=milestone,
        limit=limit,
    )

    if not tasks:
        click.echo("No execution tasks found")
        return

    click.echo(f"\n{'ID':<15} {'Milestone':<18} {'Engine':<15} {'Status':<15} {'Title':<35}")
    click.echo("-" * 100)

    for current_task in tasks:
        engine = current_task["suggested_engine"] or "N/A"
        milestone_name = current_task["milestone"] or "N/A"
        click.echo(
            f"{current_task['id']:<15} "
            f"{milestone_name[:16]:<18} "
            f"{engine:<15} "
            f"{current_task['status']:<15} "
            f"{current_task['title'][:33]:<35}"
        )
        _echo_task_metadata(current_task, indent="  ")

    click.echo(f"\nTotal: {len(tasks)} tasks")


@task.command()
@click.option("--plan-id", help="Filter by execution plan ID")
@click.option("--engine", help="Filter by suggested execution engine")
@click.option("--limit", default=50, help="Maximum number of ready tasks to show")
@click.option("--json", "json_output", is_flag=True, help="Output ready tasks as JSON")
def queue(plan_id: str | None, engine: str | None, limit: int, json_output: bool):
    """Show pending execution tasks ready for an autonomous agent."""
    config = get_config()
    store = Store(config.db_path)

    if limit < 1:
        raise click.UsageError("--limit must be greater than zero")

    if plan_id:
        plan = store.get_execution_plan(plan_id)
        if not plan:
            raise click.ClickException(f"Execution plan not found: {plan_id}")
        tasks = plan.get("tasks", [])
    else:
        tasks = store.list_execution_tasks(limit=10000)

    ready_tasks = [
        _task_with_ready_reason(current_task)
        for current_task in _ready_execution_tasks(tasks)
        if engine is None or current_task.get("suggested_engine") == engine
    ][:limit]

    if json_output:
        click.echo(json.dumps(ready_tasks, indent=2, sort_keys=True))
        return

    if not ready_tasks:
        click.echo("No ready execution tasks found")
        return

    click.echo(
        f"\n{'ID':<15} {'Milestone':<18} {'Engine':<15} " f"{'Complexity':<12} {'Title':<35} Files"
    )
    click.echo("-" * 120)

    for current_task in ready_tasks:
        engine_name = current_task.get("suggested_engine") or "N/A"
        milestone_name = current_task.get("milestone") or "N/A"
        complexity = current_task.get("estimated_complexity") or "N/A"
        files = ", ".join(current_task.get("files_or_modules") or []) or "none"
        click.echo(
            f"{current_task['id']:<15} "
            f"{milestone_name[:16]:<18} "
            f"{engine_name:<15} "
            f"{complexity:<12} "
            f"{current_task['title'][:33]:<35} "
            f"{files}"
        )

    click.echo(f"\nTotal: {len(ready_tasks)} ready tasks")


@task.command()
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output blocker analysis as JSON")
def blockers(plan_id: str, json_output: bool):
    """Show blocked tasks and their downstream impact."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    blocker_analysis = _blocked_task_impact(plan.get("tasks", []))

    if json_output:
        click.echo(json.dumps(blocker_analysis, indent=2, sort_keys=True))
        return

    if not blocker_analysis:
        click.echo(f"No blocked execution tasks found in plan {plan_id}")
        return

    click.echo(f"\nBlocked tasks in plan {plan_id}:")
    for blocker in blocker_analysis:
        click.echo(f"\n{blocker['blocked_task_id']}")
        click.echo(f"  Reason: {blocker['blocked_reason'] or 'N/A'}")
        click.echo(
            "  Direct dependents: "
            + (", ".join(blocker["direct_dependents"]) if blocker["direct_dependents"] else "none")
        )
        click.echo(
            "  Transitive dependents: "
            + (
                ", ".join(blocker["transitive_dependents"])
                if blocker["transitive_dependents"]
                else "none"
            )
        )
        click.echo(f"  Impacted count: {blocker['impacted_count']}")


@task.command(name="blocked-impact")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output blocked impact as JSON")
def blocked_impact(plan_id: str, json_output: bool):
    """Audit how blocked tasks affect downstream execution."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = audit_blocked_impact(plan)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return

    _emit_blocked_impact(result)


@task.command(name="dependency-gate")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output dependency gate as JSON")
def dependency_gate(plan_id: str, json_output: bool):
    """Classify pending tasks by dependency readiness."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = audit_dependency_gate(plan)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_dependency_gate(result)

    if not result.passed:
        raise click.exceptions.Exit(1)


@task.command(name="critical-path")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output critical path as JSON")
def critical_path(plan_id: str, json_output: bool):
    """Show the longest weighted dependency chain in an execution plan."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    try:
        result = analyze_critical_path(plan)
    except CriticalPathError as e:
        raise click.ClickException(str(e)) from e

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return

    _emit_critical_path(result)


@task.command()
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output execution waves as JSON")
def waves(plan_id: str, json_output: bool):
    """Show dependency-ready execution task waves."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    try:
        result = analyze_execution_waves(plan)
    except ExecutionWaveError as e:
        raise click.ClickException(str(e)) from e

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return

    _emit_execution_waves(result)


@task.command(name="completeness")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output audit results as JSON")
def task_completeness(plan_id: str, json_output: bool):
    """Audit execution tasks for autonomous-agent readiness."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = audit_task_completeness(plan)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_task_completeness(result)

    if not result.passed:
        raise click.exceptions.Exit(1)


@task.command(name="env-readiness")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output audit results as JSON")
def task_env_readiness(plan_id: str, json_output: bool):
    """Audit task environment setup and verification readiness."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = audit_env_readiness(plan)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_env_readiness(result)

    if not result.passed:
        raise click.exceptions.Exit(1)


@task.command(name="split-audit")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output audit results as JSON")
def task_split_audit(plan_id: str, json_output: bool):
    """Recommend split points for tasks that are too broad."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = audit_task_splitting(plan)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_task_split_audit(result)

    if not result.passed:
        raise click.exceptions.Exit(1)


@task.command(name="workload")
@click.argument("plan_id")
@click.option("--json", "json_output", is_flag=True, help="Output workload as JSON")
def task_workload(plan_id: str, json_output: bool):
    """Summarize execution task workload distribution."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = analyze_workload(plan)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return

    _emit_task_workload(result)


@task.command(name="ownership-gaps")
@click.argument("plan_id")
@click.option(
    "--threshold",
    default=DEFAULT_OWNERSHIP_THRESHOLD,
    show_default=True,
    help="Maximum tasks allowed in one owner group",
)
@click.option("--json", "json_output", is_flag=True, help="Output audit results as JSON")
def task_ownership_gaps(plan_id: str, threshold: int, json_output: bool):
    """Audit ambiguous task ownership and overloaded owner lanes."""
    if threshold < 1:
        raise click.UsageError("--threshold must be greater than zero")

    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    result = audit_ownership_gaps(plan, threshold=threshold)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        _emit_ownership_gaps(result)

    if not result.passed:
        raise click.exceptions.Exit(1)


def _emit_critical_path(result: CriticalPathResult) -> None:
    """Render human-readable critical path analysis."""
    click.echo(f"Critical path for plan {result.plan_id}")
    click.echo(f"Total weight: {result.total_weight}")

    if not result.tasks:
        click.echo("No execution tasks found.")
        return

    click.echo("\nOrdered chain:")
    for index, task in enumerate(result.tasks, 1):
        click.echo(f"  {index}. {task.id} - {task.title} " f"[{task.milestone or 'N/A'}]")
        click.echo(f"     Weight: {task.weight} " f"(cumulative: {task.cumulative_weight})")
        click.echo(
            "     Blocking dependencies: "
            + (", ".join(task.blocking_dependencies) if task.blocking_dependencies else "none")
        )


def _emit_blocked_impact(result: BlockedImpactResult) -> None:
    """Render human-readable blocked task impact audit results."""
    click.echo(f"Blocked task impact audit: {result.plan_id}")

    if not result.blocked_tasks:
        click.echo("No blocked execution tasks found.")
        return

    for blocked_task in result.blocked_tasks:
        click.echo(f"\n{blocked_task.blocked_task_id} - {blocked_task.blocked_task_title}")
        click.echo(f"  Severity: {blocked_task.severity}")
        click.echo(f"  Reason: {blocked_task.blocked_reason or 'N/A'}")
        click.echo(f"  Milestone: {blocked_task.milestone or 'N/A'}")
        click.echo(
            "  Impacted milestones: "
            + (
                ", ".join(blocked_task.impacted_milestones)
                if blocked_task.impacted_milestones
                else "none"
            )
        )
        click.echo(
            "  Direct dependents: "
            + (
                ", ".join(blocked_task.direct_dependents)
                if blocked_task.direct_dependents
                else "none"
            )
        )
        click.echo(
            "  Transitive dependents: "
            + (
                ", ".join(blocked_task.transitive_dependents)
                if blocked_task.transitive_dependents
                else "none"
            )
        )
        click.echo(f"  Impacted count: {blocked_task.impacted_count}")
        click.echo(
            "  Critical dependency position: "
            + ("yes" if blocked_task.critical_dependency_position else "no")
        )


def _emit_dependency_gate(result: DependencyGateResult) -> None:
    """Render human-readable dependency gate classifications."""
    click.echo(f"Dependency gate audit: {result.plan_id}")
    click.echo(
        f"Result: {'passed' if result.passed else 'failed'} "
        f"({result.ready_count} ready, {result.waiting_count} waiting, "
        f"{result.blocked_count} blocked, {result.error_count} errors)"
    )

    if not result.tasks:
        click.echo("No pending execution tasks found.")
        return

    if result.ready_tasks:
        click.echo("\nReady tasks:")
        for task_result in result.ready_tasks:
            click.echo(f"  - {task_result.task_id} ({task_result.title})")

    if result.waiting_tasks:
        click.echo("\nWaiting tasks:")
        for task_result in result.waiting_tasks:
            click.echo(f"  - {task_result.task_id} ({task_result.title})")
            for reason in task_result.reasons:
                click.echo(f"    [{reason.code}] {reason.message}")

    if result.blocked_tasks:
        click.echo("\nBlocked tasks:")
        for task_result in result.blocked_tasks:
            click.echo(f"  - {task_result.task_id} ({task_result.title})")
            for reason in task_result.reasons:
                click.echo(f"    [{reason.code}] {reason.message}")


def _emit_execution_waves(result: ExecutionWavesResult) -> None:
    """Render human-readable execution wave analysis."""
    click.echo(f"Execution waves for plan {result.plan_id}")

    if not result.waves:
        click.echo("No execution tasks found.")
        return

    for wave in result.waves:
        click.echo(f"\nWave {wave.wave_number}")
        click.echo(f"{'ID':<15} {'Milestone':<18} {'Engine':<15} " f"{'Title':<35} Files")
        click.echo("-" * 110)
        for current_task in wave.tasks:
            milestone = current_task.milestone or "N/A"
            engine = current_task.suggested_engine or "N/A"
            files = ", ".join(current_task.files_or_modules) or "none"
            click.echo(
                f"{current_task.id:<15} "
                f"{milestone[:16]:<18} "
                f"{engine:<15} "
                f"{current_task.title[:33]:<35} "
                f"{files}"
            )

    click.echo(f"\nTotal: {result.task_count} tasks in {len(result.waves)} waves")


def _emit_task_completeness(result: TaskCompletenessResult) -> None:
    """Render human-readable task completeness audit results."""
    click.echo(f"Task completeness audit: {result.plan_id}")
    click.echo(
        f"Result: {'passed' if result.passed else 'failed'} "
        f"(score {result.score}/100, "
        f"{result.blocking_count} blocking, {result.warning_count} warnings)"
    )

    if not result.tasks:
        click.echo("No execution tasks found.")
        return

    grouped_findings = result.findings_by_severity()
    if grouped_findings["blocking"]:
        click.echo("\nBlocking findings:")
        for finding in grouped_findings["blocking"]:
            click.echo(
                f"  - {finding.task_id} ({finding.task_title}) "
                f"[{finding.code}]: {finding.message}"
            )
            click.echo(f"    Remediation: {finding.remediation}")
    else:
        click.echo("No blocking findings found.")

    if grouped_findings["warning"]:
        click.echo("\nWarnings:")
        for finding in grouped_findings["warning"]:
            click.echo(
                f"  - {finding.task_id} ({finding.task_title}) "
                f"[{finding.code}]: {finding.message}"
            )
            click.echo(f"    Remediation: {finding.remediation}")

    click.echo("\nTask scores:")
    for task_item in result.tasks:
        click.echo(f"  - {task_item.task_id} ({task_item.title}): " f"{task_item.score}/100")


def _emit_env_readiness(result: EnvReadinessResult) -> None:
    """Render human-readable environment readiness audit results."""
    click.echo(f"Task environment readiness audit: {result.plan_id}")
    click.echo(
        f"Result: {'passed' if result.passed else 'failed'} "
        f"({result.blocking_count} blocking, {result.warning_count} warnings)"
    )

    if not result.tasks:
        click.echo("No execution tasks found.")
        return

    grouped_findings = result.findings_by_severity()
    if grouped_findings["blocking"]:
        click.echo("\nBlocking findings:")
        for finding in grouped_findings["blocking"]:
            click.echo(
                f"  - {finding.task_id} ({finding.task_title}) "
                f"[{finding.code}]: {finding.message}"
            )
            click.echo(f"    Remediation: {finding.remediation}")
    else:
        click.echo("No blocking findings found.")

    if grouped_findings["warning"]:
        click.echo("\nWarnings:")
        for finding in grouped_findings["warning"]:
            click.echo(
                f"  - {finding.task_id} ({finding.task_title}) "
                f"[{finding.code}]: {finding.message}"
            )
            click.echo(f"    Remediation: {finding.remediation}")


def _emit_task_split_audit(result: TaskSplittingResult) -> None:
    """Render human-readable task split recommendations."""
    click.echo(f"Task split audit: {result.plan_id}")
    click.echo(
        f"Result: {'passed' if result.passed else 'recommendations found'} "
        f"({result.recommendation_count} recommendations)"
    )

    if not result.recommendations:
        click.echo("No split recommendations found.")
        return

    click.echo("\nRecommendations:")
    for recommendation in result.recommendations:
        click.echo(f"  - {recommendation.task_id} ({recommendation.title})")
        click.echo("    Reasons:")
        for reason in recommendation.reasons:
            click.echo(f"      - [{reason.code}] {reason.message} ({reason.value})")
        click.echo("    Suggested subtasks:")
        for index, suggested_subtask in enumerate(
            recommendation.suggested_subtasks,
            1,
        ):
            click.echo(f"      {index}. {suggested_subtask.title}")
            if suggested_subtask.files_or_modules:
                click.echo("         Files: " + ", ".join(suggested_subtask.files_or_modules))
            if suggested_subtask.acceptance_criteria:
                click.echo("         Criteria: " + "; ".join(suggested_subtask.acceptance_criteria))


def _emit_task_workload(result: WorkloadResult) -> None:
    """Render human-readable workload distribution results."""
    click.echo(f"Task workload audit: {result.plan_id}")
    click.echo(
        f"Tasks: {result.task_count} "
        f"(overload threshold: {result.overload_threshold} per owner/engine group)"
    )

    _emit_count_table(
        "Distribution",
        [
            ("owner_type", result.counts_by_owner_type),
            ("suggested_engine", result.counts_by_suggested_engine),
            ("milestone", result.counts_by_milestone),
            ("status", result.counts_by_status),
            ("complexity", result.complexity_buckets),
        ],
    )

    click.echo("\nFlags:")
    if not result.has_flags:
        click.echo("  none")
    if result.unassigned_task_ids:
        click.echo("  Unassigned tasks: " + ", ".join(result.unassigned_task_ids))
    for overloaded_group in result.overloaded_groups:
        click.echo(
            "  Overloaded "
            f"{overloaded_group.dimension}={overloaded_group.group}: "
            f"{overloaded_group.task_count} tasks "
            f"(threshold {overloaded_group.threshold})"
        )

    click.echo("\nCross-milestone dependencies:")
    if not result.cross_milestone_dependencies:
        click.echo("  none")
        return
    click.echo(f"  {'From':<20} {'To':<20} Count")
    click.echo("  " + "-" * 48)
    for dependency_count in result.cross_milestone_dependencies:
        click.echo(
            f"  {dependency_count.from_milestone[:18]:<20} "
            f"{dependency_count.to_milestone[:18]:<20} "
            f"{dependency_count.count}"
        )


def _emit_ownership_gaps(result: OwnershipGapResult) -> None:
    """Render human-readable task ownership audit results."""
    click.echo(f"Task ownership gap audit: {result.plan_id}")
    click.echo(
        f"Result: {'passed' if result.passed else 'failed'} "
        f"({result.blocking_count} blocking, {result.warning_count} warnings, "
        f"threshold {result.threshold})"
    )

    if not result.findings:
        click.echo("No ownership gaps found.")
        return

    grouped_findings = result.findings_by_severity()
    if grouped_findings["blocking"]:
        click.echo("\nBlocking findings:")
        for finding in grouped_findings["blocking"]:
            _emit_ownership_gap_finding(finding)

    if grouped_findings["warning"]:
        click.echo("\nWarnings:")
        for finding in grouped_findings["warning"]:
            _emit_ownership_gap_finding(finding)


def _emit_ownership_gap_finding(finding: OwnershipGapFinding) -> None:
    click.echo(f"  - [{finding.code}] {finding.message}")
    click.echo("    Task IDs: " + (", ".join(finding.task_ids) if finding.task_ids else "none"))
    if finding.owner_type:
        click.echo(f"    Owner type: {finding.owner_type}")
    if finding.suggested_engine:
        click.echo(f"    Suggested engine: {finding.suggested_engine}")
    click.echo(f"    Remediation: {finding.remediation}")


def _emit_count_table(
    title: str,
    sections: list[tuple[str, dict[str, int]]],
) -> None:
    click.echo(f"\n{title}:")
    click.echo(f"  {'Dimension':<18} {'Group':<24} Count")
    click.echo("  " + "-" * 52)
    for dimension, counts in sections:
        if not counts:
            click.echo(f"  {dimension:<18} {'none':<24} 0")
            continue
        for group, count in counts.items():
            click.echo(f"  {dimension:<18} {group[:22]:<24} {count}")


@task.command()
@click.argument("task_id")
def inspect(task_id: str):
    """Inspect an execution task in detail."""
    config = get_config()
    store = Store(config.db_path)

    current_task = store.get_execution_task(task_id)

    if not current_task:
        click.echo(f"Execution task not found: {task_id}", err=True)
        return

    click.echo(f"\n{'='*80}")
    click.echo(f"Execution Task: {current_task['id']}")
    click.echo(f"{'='*80}\n")

    click.echo(f"Title:           {current_task['title']}")
    click.echo(f"Plan:            {current_task['execution_plan_id']}")
    click.echo(f"Status:          {current_task['status']}")
    click.echo(f"Milestone:       {current_task['milestone'] or 'N/A'}")
    click.echo(f"Owner:           {current_task['owner_type'] or 'N/A'}")
    click.echo(f"Engine:          {current_task['suggested_engine'] or 'N/A'}")
    click.echo(f"Complexity:      {current_task['estimated_complexity'] or 'N/A'}")
    click.echo(f"Created:         {current_task['created_at']}")
    click.echo(f"\nDescription:\n{current_task['description']}\n")
    _echo_task_metadata(current_task)


@task.command()
@click.argument("plan_id")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(("markdown", "json")),
    default="markdown",
    show_default=True,
    help="Roster output format",
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write the roster to a file instead of stdout",
)
def roster(plan_id: str, output_format: str, output: Path | None):
    """Render a task assignment roster for an execution plan."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    exporter = TaskRosterExporter()
    if output_format == "json":
        content = json.dumps(exporter.render_json(plan), indent=2, sort_keys=True) + "\n"
    else:
        content = exporter.render_markdown(plan)

    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(content)
        click.echo(f"✓ Wrote task roster to: {output}")
        return

    click.echo(content, nl=False)


@task.command()
@click.argument("task_id")
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write the handoff to a file instead of stdout",
)
@click.option("--json", "json_output", is_flag=True, help="Output handoff as JSON")
def handoff(task_id: str, output: Path | None, json_output: bool):
    """Render a focused handoff for a single execution task."""
    config = get_config()
    store = Store(config.db_path)

    current_task = store.get_execution_task(task_id)
    if not current_task:
        raise click.ClickException(f"Execution task not found: {task_id}")

    plan = None
    brief = None
    plan_id = current_task.get("execution_plan_id")
    if plan_id:
        plan = store.get_execution_plan(plan_id)
        if plan:
            brief = store.get_implementation_brief(plan["implementation_brief_id"])

    dependency_tasks = _resolve_dependency_tasks(store, current_task, plan)
    exporter = TaskHandoffExporter()

    if json_output:
        content = json.dumps(
            exporter.render_json(current_task, dependency_tasks, plan, brief),
            indent=2,
            sort_keys=True,
        )
    else:
        content = exporter.render_markdown(current_task, dependency_tasks, plan, brief)

    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(content)
        click.echo(f"✓ Wrote task handoff to: {output}")
        return

    click.echo(content, nl=False)


@task.command()
@click.argument("task_id")
@click.option(
    "--status",
    required=True,
    type=click.Choice(TASK_STATUS_CHOICES),
)
@click.option("--blocked-reason", help="Reason the task is blocked")
@click.option("--reason", help="Reason for the status change")
def update(task_id: str, status: str, blocked_reason: str | None, reason: str | None):
    """Update execution task status."""
    config = get_config()
    store = Store(config.db_path)

    if blocked_reason and status != "blocked":
        raise click.UsageError("--blocked-reason can only be used with --status blocked")

    if store.update_execution_task_status(
        task_id,
        status,
        blocked_reason=blocked_reason,
        reason=reason,
    ):
        click.echo(f"✓ Updated task {task_id} status to {status}")
        if blocked_reason:
            click.echo(f"  Blocked reason: {blocked_reason}")
    else:
        click.echo(f"Execution task not found: {task_id}", err=True)


def _ready_execution_tasks(tasks: list[dict]) -> list[dict]:
    """Return tasks whose dependencies are satisfied within their execution plan."""
    tasks_by_plan: dict[str | None, dict[str, dict]] = {}
    for current_task in tasks:
        plan_tasks = tasks_by_plan.setdefault(current_task.get("execution_plan_id"), {})
        plan_tasks[current_task["id"]] = current_task

    ready_tasks = []
    for current_task in tasks:
        if current_task.get("status") != "pending":
            continue

        dependencies = current_task.get("depends_on") or []
        plan_tasks = tasks_by_plan.get(current_task.get("execution_plan_id"), {})
        if all(
            dependency_id in plan_tasks
            and plan_tasks[dependency_id].get("status") in {"completed", "skipped"}
            for dependency_id in dependencies
        ):
            ready_tasks.append(current_task)

    return ready_tasks


def _task_with_ready_reason(current_task: dict) -> dict:
    """Copy a ready task and attach a human-readable readiness reason."""
    task_payload = dict(current_task)
    dependencies = current_task.get("depends_on") or []
    if dependencies:
        task_payload["ready_reason"] = "All dependencies are completed or skipped: " + ", ".join(
            dependencies
        )
    else:
        task_payload["ready_reason"] = "Task is pending and has no dependencies"
    return task_payload


def _blocked_task_impact(tasks: list[dict]) -> list[dict]:
    """Return blocked tasks with direct and downstream dependents."""
    dependents_by_task_id: dict[str, list[str]] = {}
    tasks_by_id = {current_task["id"]: current_task for current_task in tasks}
    for current_task in tasks:
        for dependency_id in current_task.get("depends_on") or []:
            if dependency_id in tasks_by_id:
                dependents_by_task_id.setdefault(dependency_id, []).append(current_task["id"])

    impact = []
    for current_task in tasks:
        if current_task.get("status") != "blocked":
            continue

        blocked_task_id = current_task["id"]
        direct_dependents = dependents_by_task_id.get(blocked_task_id, [])
        downstream_dependents = _downstream_dependents(
            blocked_task_id,
            dependents_by_task_id,
        )
        direct_dependent_ids = set(direct_dependents)
        transitive_dependents = [
            task_id for task_id in downstream_dependents if task_id not in direct_dependent_ids
        ]

        impact.append(
            {
                "blocked_task_id": blocked_task_id,
                "blocked_reason": current_task.get("blocked_reason"),
                "direct_dependents": direct_dependents,
                "transitive_dependents": transitive_dependents,
                "impacted_count": len(set(direct_dependents + transitive_dependents)),
            }
        )

    return impact


def _downstream_dependents(
    task_id: str,
    dependents_by_task_id: dict[str, list[str]],
) -> list[str]:
    """Return all recursive dependents in dependency graph order."""
    downstream = []
    seen = {task_id}
    pending = dependents_by_task_id.get(task_id, [])[:]

    while pending:
        dependent_id = pending.pop(0)
        if dependent_id in seen:
            continue
        seen.add(dependent_id)
        downstream.append(dependent_id)
        pending.extend(dependents_by_task_id.get(dependent_id, []))

    return downstream


def _echo_task_metadata(current_task: dict, indent: str = ""):
    """Print task metadata shared by task list and inspect commands."""
    depends_on = current_task["depends_on"] or []
    files_or_modules = current_task["files_or_modules"] or []
    acceptance_criteria = current_task["acceptance_criteria"] or []

    click.echo(f"{indent}Dependencies: {', '.join(depends_on) if depends_on else 'none'}")
    click.echo(
        f"{indent}Files:        {', '.join(files_or_modules) if files_or_modules else 'none'}"
    )
    if current_task.get("blocked_reason"):
        click.echo(f"{indent}Blocked:      {current_task['blocked_reason']}")

    if acceptance_criteria:
        click.echo(f"{indent}Acceptance Criteria:")
        for criterion in acceptance_criteria:
            click.echo(f"{indent}  - {criterion}")
    else:
        click.echo(f"{indent}Acceptance Criteria: none")


def _resolve_dependency_tasks(
    store: Store,
    current_task: dict,
    plan: dict | None,
) -> list[dict]:
    """Resolve dependency task records while preserving dependency order."""
    dependency_ids = current_task.get("depends_on") or []
    if not dependency_ids:
        return []

    plan_tasks_by_id = {task["id"]: task for task in (plan or {}).get("tasks", [])}
    dependency_tasks = []
    for dependency_id in dependency_ids:
        dependency_task = plan_tasks_by_id.get(dependency_id)
        if not dependency_task:
            dependency_task = store.get_execution_task(dependency_id)
        if dependency_task:
            dependency_tasks.append(dependency_task)

    return dependency_tasks


# ============================================================================
# Export Commands
# ============================================================================


@cli.group()
def export():
    """Export execution plans to target engines."""
    pass


@export.command()
@click.argument("plan_id")
@click.option(
    "--output",
    required=True,
    type=click.Path(dir_okay=False, path_type=Path),
    help="Output .zip archive path",
)
def archive(plan_id: str, output: Path):
    """Export a complete portable archive for one execution plan."""
    config = get_config()
    store = Store(config.db_path)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    brief = store.get_implementation_brief(plan["implementation_brief_id"])
    if not brief:
        raise click.ClickException(
            f"Implementation brief not found: {plan['implementation_brief_id']}"
        )

    source_brief = store.get_source_brief(brief["source_brief_id"])
    result_path = ArchiveExporter().export(
        plan,
        brief,
        str(output),
        source_brief=source_brief,
    )
    click.echo(f"✓ Exported archive to: {result_path}")


@export.command()
@click.argument("plan_id")
@click.argument("target_arg", required=False)
@click.option(
    "--target",
    required=False,
    type=str,
    help="Target execution engine",
)
@click.option(
    "--require-coherence",
    is_flag=True,
    help="Block export if the plan and brief coherence audit reports errors",
)
def run(plan_id: str, target_arg: str | None, target: str | None, require_coherence: bool):
    """Export execution plan to target engine."""
    _run_export_command(plan_id, target_arg, target, require_coherence)


@export.command(name="render")
@click.argument("plan_id")
@click.argument("target_arg", required=False)
@click.option(
    "--target",
    required=False,
    type=str,
    help="Target execution engine",
)
@click.option(
    "--require-coherence",
    is_flag=True,
    help="Block export if the plan and brief coherence audit reports errors",
)
def render(plan_id: str, target_arg: str | None, target: str | None, require_coherence: bool):
    """Render execution plan export artifacts."""
    _run_export_command(plan_id, target_arg, target, require_coherence)


def _run_export_command(
    plan_id: str,
    target_arg: str | None,
    target: str | None,
    require_coherence: bool,
) -> None:
    """Export execution plan to target engine."""
    config = get_config()
    store = Store(config.db_path)

    try:
        target = _resolve_export_target(target_arg, target)
        if target not in EXPORT_TARGET_CHOICES:
            click.echo(f"✗ Unknown target: {target}", err=True)
            return

        # Get execution plan
        plan = store.get_execution_plan(plan_id)
        if not plan:
            click.echo(f"✗ Execution plan not found: {plan_id}", err=True)
            return

        # Get implementation brief
        brief = store.get_implementation_brief(plan["implementation_brief_id"])
        if not brief:
            click.echo(
                f"✗ Implementation brief not found: {plan['implementation_brief_id']}", err=True
            )
            return

        _maybe_require_brief_plan_coherence(
            plan,
            brief,
            require_coherence=require_coherence,
        )

        # Ensure export directory exists
        export_dir = Path(config.export_dir)
        export_dir.mkdir(parents=True, exist_ok=True)

        # Export to target(s)
        targets = (
            [choice for choice in EXPORT_TARGET_CHOICES if choice != "all"]
            if target == "all"
            else [target]
        )

        for target_name in targets:
            click.echo(f"\nExporting to {target_name}...")

            # Get exporter
            exporter = _get_exporter(target_name)
            if not exporter:
                click.echo(f"  ✗ Unknown target: {target_name}", err=True)
                continue

            # Build output path
            output_filename = f"{plan_id}-{target_name}{exporter.get_extension()}"
            output_path = str(export_dir / output_filename)

            # Export
            try:
                result_path = exporter.export(plan, brief, output_path)

                # Record export
                export_record = {
                    "id": f"exp-{plan_id[:12]}-{target_name}",
                    "execution_plan_id": plan_id,
                    "target_engine": target_name,
                    "export_format": exporter.get_format(),
                    "output_path": result_path,
                    "export_metadata": {
                        "brief_id": brief["id"],
                        "brief_title": brief["title"],
                    },
                }
                if hasattr(exporter, "get_export_metadata"):
                    export_record["export_metadata"].update(exporter.get_export_metadata())
                store.insert_export_record(export_record)

                click.echo(f"  ✓ Exported to: {result_path}")

            except Exception as e:
                click.echo(f"  ✗ Export failed: {e}", err=True)

        click.echo(f"\n✓ Export complete")
        click.echo(f"  Output directory: {export_dir}")

    except click.exceptions.Exit:
        raise
    except click.ClickException:
        raise
    except Exception as e:
        click.echo(f"✗ Export failed: {e}", err=True)
        import traceback

        click.echo(traceback.format_exc(), err=True)


@export.command()
@click.argument("plan_id")
@click.argument("target_arg", required=False)
@click.option(
    "--target",
    required=False,
    type=str,
    help="Target execution engine",
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write preview output to a file",
)
@click.option(
    "--require-coherence",
    is_flag=True,
    help="Block preview if the plan and brief coherence audit reports errors",
)
def preview(
    plan_id: str,
    target_arg: str | None,
    target: str | None,
    output: Path | None,
    require_coherence: bool,
):
    """Preview an export target without recording it."""
    config = get_config()
    store = Store(config.db_path)

    try:
        target = _resolve_export_target(target_arg, target)
        if target not in EXPORT_TARGET_CHOICES:
            click.echo(f"✗ Unknown target: {target}", err=True)
            return

        plan = store.get_execution_plan(plan_id)
        if not plan:
            click.echo(f"✗ Execution plan not found: {plan_id}", err=True)
            return

        brief = store.get_implementation_brief(plan["implementation_brief_id"])
        if not brief:
            click.echo(
                f"✗ Implementation brief not found: {plan['implementation_brief_id']}", err=True
            )
            return

        _maybe_require_brief_plan_coherence(
            plan,
            brief,
            require_coherence=require_coherence,
        )

        targets = (
            [choice for choice in EXPORT_TARGET_CHOICES if choice != "all"]
            if target == "all"
            else [target]
        )

        preview_sections = []
        for target_name in targets:
            exporter = _get_exporter(target_name)
            if not exporter:
                click.echo(f"✗ Unknown target: {target_name}", err=True)
                continue

            preview_sections.append(
                _render_export_preview(
                    plan,
                    brief,
                    target_name,
                    exporter,
                )
            )

        preview_output = "\n\n".join(preview_sections)
        _emit_export_preview(preview_output, output)

    except click.exceptions.Exit:
        raise
    except click.ClickException:
        raise
    except Exception as e:
        click.echo(f"✗ Export preview failed: {e}", err=True)
        import traceback

        click.echo(traceback.format_exc(), err=True)


@export.command(name="diff")
@click.argument("left_plan_id")
@click.argument("right_plan_id")
@click.option(
    "--target",
    required=True,
    type=click.Choice(
        [
            "adr",
            "agent-prompt-pack",
            "relay",
            "smoothie",
            "codex",
            "claude-code",
            "asana-csv",
            "azure-devops-csv",
            "calendar",
            "checklist",
            "coverage-matrix",
            "critical-path-report",
            "mermaid",
            "milestone-summary",
            "plan-snapshot",
            "csv-tasks",
            "file-impact-map",
            "gantt",
            "github-actions",
            "github-issues",
            "gitlab-issues",
            "html-report",
            "jira-csv",
            "linear",
            "junit-tasks",
            "kanban",
            "raci-matrix",
            "release-notes",
            "risk-register",
            "status-report",
            "task-bundle",
            "taskfile",
            "trello-json",
            "vscode-tasks",
            "wave-schedule",
        ]
    ),
    help="Target execution engine",
)
@click.option("--json", "json_output", is_flag=True, help="Output diff results as JSON")
def export_diff(left_plan_id: str, right_plan_id: str, target: str, json_output: bool):
    """Compare two rendered exports for the same target engine."""
    config = get_config()
    store = Store(config.db_path)

    left_plan = store.get_execution_plan(left_plan_id)
    if not left_plan:
        raise click.ClickException(f"Execution plan not found: {left_plan_id}")

    right_plan = store.get_execution_plan(right_plan_id)
    if not right_plan:
        raise click.ClickException(f"Execution plan not found: {right_plan_id}")

    left_brief = store.get_implementation_brief(left_plan["implementation_brief_id"])
    if not left_brief:
        raise click.ClickException(
            f"Implementation brief not found: {left_plan['implementation_brief_id']}"
        )

    right_brief = store.get_implementation_brief(right_plan["implementation_brief_id"])
    if not right_brief:
        raise click.ClickException(
            f"Implementation brief not found: {right_plan['implementation_brief_id']}"
        )

    result = compare_rendered_exports(left_plan, left_brief, right_plan, right_brief, target)
    _emit_export_diff_result(result, json_output)


@export.command()
@click.argument("plan_id")
@click.argument("target_arg", required=False)
@click.option(
    "--target",
    required=False,
    type=click.Choice(
        [
            "adr",
            "agent-prompt-pack",
            "relay",
            "smoothie",
            "codex",
            "claude-code",
            "asana-csv",
            "azure-devops-csv",
            "calendar",
            "checklist",
            "coverage-matrix",
            "critical-path-report",
            "mermaid",
            "milestone-summary",
            "plan-snapshot",
            "csv-tasks",
            "file-impact-map",
            "gantt",
            "github-actions",
            "github-issues",
            "gitlab-issues",
            "html-report",
            "jira-csv",
            "linear",
            "junit-tasks",
            "kanban",
            "raci-matrix",
            "release-notes",
            "risk-register",
            "status-report",
            "task-bundle",
            "taskfile",
            "trello-json",
            "vscode-tasks",
            "wave-schedule",
        ]
    ),
    help="Target execution engine",
)
@click.option("--json", "json_output", is_flag=True, help="Output validation results as JSON")
def validate(plan_id: str, target_arg: str | None, target: str | None, json_output: bool):
    """Validate a rendered export artifact for a target engine."""
    config = get_config()
    store = Store(config.db_path)

    target = _resolve_export_target(target_arg, target)

    plan = store.get_execution_plan(plan_id)
    if not plan:
        raise click.ClickException(f"Execution plan not found: {plan_id}")

    brief = store.get_implementation_brief(plan["implementation_brief_id"])
    if not brief:
        raise click.ClickException(
            f"Implementation brief not found: {plan['implementation_brief_id']}"
        )

    result = validate_export(plan, brief, target)
    _emit_export_validation_result(result, json_output)


@export.command()
@click.argument("plan_id")
@click.option(
    "--output",
    required=True,
    type=click.Path(dir_okay=False),
    help="Output .mmd path",
)
def graph(plan_id: str, output: str):
    """Export an execution plan dependency graph as Mermaid."""
    config = get_config()
    store = Store(config.db_path)

    try:
        plan = store.get_execution_plan(plan_id)
        if not plan:
            click.echo(f"✗ Execution plan not found: {plan_id}", err=True)
            return

        brief = store.get_implementation_brief(plan["implementation_brief_id"])
        if not brief:
            click.echo(
                f"✗ Implementation brief not found: {plan['implementation_brief_id']}",
                err=True,
            )
            return

        exporter = MermaidExporter()
        result_path = exporter.export(plan, brief, output)

        export_record = {
            "id": f"exp-{plan_id[:12]}-mermaid",
            "execution_plan_id": plan_id,
            "target_engine": "mermaid",
            "export_format": exporter.get_format(),
            "output_path": result_path,
            "export_metadata": {
                "brief_id": brief["id"],
                "brief_title": brief["title"],
            },
        }
        store.insert_export_record(export_record)

        click.echo(f"✓ Exported graph to: {result_path}")

    except Exception as e:
        click.echo(f"✗ Graph export failed: {e}", err=True)
        import traceback

        click.echo(traceback.format_exc(), err=True)


def _get_exporter(target: str):
    """Get exporter instance for target."""
    try:
        return create_exporter(target)
    except ValueError:
        return None


def _resolve_export_target(target_arg: str | None, target_option: str | None) -> str:
    """Resolve export target from positional or option syntax."""
    if target_arg and target_option and target_arg != target_option:
        raise click.ClickException(
            f"Conflicting export targets: positional '{target_arg}' and --target '{target_option}'"
        )
    resolved = target_option or target_arg
    if not resolved:
        raise click.ClickException("Missing export target. Provide TARGET or --target TARGET.")
    return resolved


def _maybe_require_brief_plan_coherence(
    plan: dict,
    brief: dict,
    *,
    require_coherence: bool,
) -> None:
    """Optionally gate export rendering on brief/plan coherence."""
    if not require_coherence:
        return

    result = audit_brief_plan_coherence(plan, brief)
    _emit_brief_plan_coherence(result)

    if not result.ok:
        click.echo("✗ Export blocked by brief-plan coherence errors", err=True)
        raise click.exceptions.Exit(1)


def _emit_export_validation_result(result, json_output: bool) -> None:
    """Render validation output and exit with the appropriate status code."""
    payload = result.to_dict()
    if json_output:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
    else:
        if result.passed:
            click.echo(f"✓ Validation passed for {result.target}")
        else:
            click.echo(f"✗ Validation failed for {result.target}", err=True)
            for finding in result.findings:
                prefix = f"[{finding.code}]"
                if finding.path:
                    prefix = f"{prefix} {finding.path}:"
                click.echo(f"  - {prefix} {finding.message}", err=True)

    raise click.exceptions.Exit(0 if result.passed else 1)


def _emit_export_diff_result(result, json_output: bool) -> None:
    """Render diff output and exit successfully."""
    payload = result.to_dict()
    if json_output:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        raise click.exceptions.Exit(0)

    click.echo(
        f"Export diff: {result.left_plan_id} -> {result.right_plan_id} "
        f"(target: {result.target})"
    )
    click.echo(f"Artifact type: {result.artifact_type}")
    click.echo(
        "Summary: "
        f"{len(result.added_files)} added, "
        f"{len(result.removed_files)} removed, "
        f"{len(result.changed_files)} changed"
    )

    if not result.has_changes:
        click.echo("No differences found after normalization.")
        raise click.exceptions.Exit(0)

    if result.added_files:
        click.echo("\nAdded files:")
        for change in result.added_files:
            click.echo(f"  - {change.path}")

    if result.removed_files:
        click.echo("\nRemoved files:")
        for change in result.removed_files:
            click.echo(f"  - {change.path}")

    if result.changed_files:
        click.echo("\nChanged files:")
        for change in result.changed_files:
            click.echo(f"  - {change.path}")
            for line in change.diff[:8]:
                click.echo(f"    {line}")
            if len(change.diff) > 8:
                click.echo("    ...")

    raise click.exceptions.Exit(0)


def _render_export_preview(
    plan: dict,
    brief: dict,
    target_name: str,
    exporter,
) -> str:
    """Render an export target into preview text without leaving permanent files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        preview_path = Path(tmp_dir) / f"{plan['id']}-{target_name}{exporter.get_extension()}"
        result_path = Path(exporter.export(plan, brief, str(preview_path)))
        return _read_preview_artifact(result_path)


def _read_preview_artifact(path: Path) -> str:
    """Read a rendered export artifact back into preview text."""
    if path.is_file():
        return path.read_text()

    if path.is_dir():
        sections = []
        for artifact_path in sorted(
            (candidate for candidate in path.rglob("*") if candidate.is_file()),
            key=lambda candidate: candidate.relative_to(path).as_posix(),
        ):
            sections.append(f"## {artifact_path.relative_to(path).as_posix()}")
            sections.append(artifact_path.read_text())
        return "\n\n".join(sections)

    raise FileNotFoundError(f"Rendered preview artifact not found: {path}")


@export.command()
@click.option("--plan-id", help="Filter by plan ID")
@click.option("--limit", default=50, help="Maximum number of exports to show")
def list(plan_id: str | None, limit: int):
    """List export records."""
    config = get_config()
    store = Store(config.db_path)

    exports = store.list_export_records(plan_id=plan_id, limit=limit)

    if not exports:
        click.echo("No export records found")
        return

    click.echo(f"\n{'Plan ID':<15} {'Target':<15} {'Format':<10} {'Exported':<20}")
    click.echo("-" * 60)

    for exp in exports:
        click.echo(
            f"{exp['execution_plan_id']:<15} "
            f"{exp['target_engine']:<15} "
            f"{exp['export_format']:<10} "
            f"{exp['exported_at'][:19]:<20}"
        )

    click.echo(f"\nTotal: {len(exports)} exports")


if __name__ == "__main__":
    cli()
