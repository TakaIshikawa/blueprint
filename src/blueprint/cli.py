"""Blueprint CLI commands."""

import click

from blueprint.config import get_config
from blueprint.store import Store, init_db


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """Blueprint: Implementation planning layer for design briefs."""
    pass


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
@click.argument("brief_id")
def max(brief_id: str):
    """Import a Max design brief by ID."""
    click.echo(f"Importing Max design brief: {brief_id}")
    click.echo("TODO: Implement Max import")


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

    if brief['source_links']:
        click.echo("Source Links:")
        for key, value in brief['source_links'].items():
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
@click.option("--model", type=click.Choice(["opus", "sonnet"]), default="opus",
              help="LLM model to use (opus=claude-opus-4-6, sonnet=claude-sonnet-4-5)")
def create(source_id: str, model: str):
    """Generate implementation brief from source brief."""
    click.echo(f"Generating implementation brief from {source_id} using {model}...")
    click.echo("TODO: Implement brief generation")


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

    if brief['scope']:
        click.echo("In Scope:")
        for item in brief['scope']:
            click.echo(f"  • {item}")
        click.echo()

    if brief['non_goals']:
        click.echo("Non-Goals:")
        for item in brief['non_goals']:
            click.echo(f"  • {item}")
        click.echo()


@brief.command()
@click.argument("brief_id")
@click.option("--status", required=True,
              type=click.Choice(["draft", "ready_for_planning", "planned", "queued",
                               "in_progress", "implemented", "validated", "paused", "rejected"]))
def update(brief_id: str, status: str):
    """Update implementation brief status."""
    config = get_config()
    store = Store(config.db_path)

    if store.update_implementation_brief_status(brief_id, status):
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
@click.option("--model", type=click.Choice(["opus", "sonnet"]), default="opus",
              help="LLM model to use")
def create(brief_id: str, model: str):
    """Generate execution plan from implementation brief."""
    click.echo(f"Generating execution plan for {brief_id} using {model}...")
    click.echo("TODO: Implement plan generation")


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
        target = plan['target_engine'] or 'N/A'
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

    if plan['milestones']:
        click.echo(f"\nMilestones ({len(plan['milestones'])}):")
        for i, milestone in enumerate(plan['milestones'], 1):
            click.echo(f"  {i}. {milestone.get('title', 'Untitled')}")
            if milestone.get('description'):
                click.echo(f"     {milestone['description']}")

    if plan['tasks']:
        click.echo(f"\nTasks ({len(plan['tasks'])}):")
        for task in plan['tasks']:
            click.echo(f"  • {task['title']} ({task['status']})")
            if task['milestone']:
                click.echo(f"    Milestone: {task['milestone']}")
            if task['depends_on']:
                click.echo(f"    Depends on: {', '.join(task['depends_on'])}")


# ============================================================================
# Export Commands
# ============================================================================


@cli.group()
def export():
    """Export execution plans to target engines."""
    pass


@export.command()
@click.argument("plan_id")
@click.option("--target", required=True,
              type=click.Choice(["relay", "smoothie", "codex", "claude-code", "all"]),
              help="Target execution engine")
def run(plan_id: str, target: str):
    """Export execution plan to target engine."""
    click.echo(f"Exporting plan {plan_id} to {target}...")
    click.echo("TODO: Implement export")


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
