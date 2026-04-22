"""Blueprint CLI commands."""

from __future__ import annotations

import json

import click
from pydantic import ValidationError

from pathlib import Path

from blueprint.config import get_config
from blueprint.exporters.claude_code import ClaudeCodeExporter
from blueprint.exporters.codex import CodexExporter
from blueprint.exporters.csv_tasks import CsvTasksExporter
from blueprint.exporters.mermaid import MermaidExporter
from blueprint.exporters.plan_graph import PlanGraphExporter, UnknownDependencyError
from blueprint.exporters.relay import RelayExporter
from blueprint.exporters.smoothie import SmoothieExporter
from blueprint.exporters.status_report import StatusReportExporter
from blueprint.domain import ImplementationBrief
from blueprint.generators.brief_generator import (
    BriefGenerator,
    generate_implementation_brief_id,
)
from blueprint.generators.plan_generator import PlanGenerator
from blueprint.generators.plan_generator_staged import StagedPlanGenerator
from blueprint.generators.plan_reviser import PlanReviser
from blueprint.importers.github_issue_importer import GitHubIssueImporter
from blueprint.importers.max_importer import MaxImporter
from blueprint.llm.client import LLMClient
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


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """Blueprint: Implementation planning layer for design briefs."""
    pass


@cli.command()
@click.argument("entity_id")
@click.option("--limit", default=50, help="Maximum number of history events to show")
def history(entity_id: str, limit: int):
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


@import_cmd.command()
@click.argument("file_path")
def manual(file_path: str):
    """Import a manual design brief from markdown file."""
    click.echo(f"Importing manual brief from: {file_path}")
    click.echo("TODO: Implement manual import")


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


# ============================================================================
# Implementation Brief Commands
# ============================================================================


@cli.group()
def brief():
    """Manage implementation briefs."""
    pass


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
    """Import a hand-authored implementation brief JSON file."""
    config = get_config()
    store = Store(config.db_path)

    source_brief = store.get_source_brief(source_brief_id)
    if not source_brief:
        raise click.ClickException(f"Source brief not found: {source_brief_id}")

    try:
        brief_payload = json.loads(file_path.read_text())
    except json.JSONDecodeError as e:
        raise click.ClickException(f"Invalid JSON in {file_path}: {e}") from e

    if not isinstance(brief_payload, dict):
        raise click.ClickException("Implementation brief JSON must be an object")

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


# ============================================================================
# Execution Plan Commands
# ============================================================================


@cli.group()
def plan():
    """Manage execution plans."""
    pass


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
        f"\n{'ID':<15} {'Milestone':<18} {'Engine':<15} "
        f"{'Complexity':<12} {'Title':<35} Files"
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
            + (
                ", ".join(blocker["direct_dependents"])
                if blocker["direct_dependents"]
                else "none"
            )
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
        task_payload["ready_reason"] = (
            "All dependencies are completed or skipped: " + ", ".join(dependencies)
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
            task_id
            for task_id in downstream_dependents
            if task_id not in direct_dependent_ids
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
    "--target",
    required=True,
    type=click.Choice(
        [
            "relay",
            "smoothie",
            "codex",
            "claude-code",
            "mermaid",
            "csv-tasks",
            "status-report",
            "all",
        ]
    ),
    help="Target execution engine",
)
def run(plan_id: str, target: str):
    """Export execution plan to target engine."""
    config = get_config()
    store = Store(config.db_path)

    try:
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

        # Ensure export directory exists
        export_dir = Path(config.export_dir)
        export_dir.mkdir(parents=True, exist_ok=True)

        # Export to target(s)
        targets = (
            [
                "relay",
                "smoothie",
                "codex",
                "claude-code",
                "mermaid",
                "csv-tasks",
                "status-report",
            ]
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
                store.insert_export_record(export_record)

                click.echo(f"  ✓ Exported to: {result_path}")

            except Exception as e:
                click.echo(f"  ✗ Export failed: {e}", err=True)

        click.echo(f"\n✓ Export complete")
        click.echo(f"  Output directory: {export_dir}")

    except Exception as e:
        click.echo(f"✗ Export failed: {e}", err=True)
        import traceback

        click.echo(traceback.format_exc(), err=True)


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
    exporters = {
        "relay": RelayExporter(),
        "smoothie": SmoothieExporter(),
        "codex": CodexExporter(),
        "claude-code": ClaudeCodeExporter(),
        "mermaid": MermaidExporter(),
        "csv-tasks": CsvTasksExporter(),
        "status-report": StatusReportExporter(),
    }
    return exporters.get(target)


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
