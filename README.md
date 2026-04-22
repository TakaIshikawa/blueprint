# Blueprint

Implementation planning layer that converts design briefs into execution-ready plans.

## Overview

Blueprint sits between upstream idea systems (Max, Graph) and downstream implementation engines (Relay, Smoothie, Codex, Claude Code). It normalizes design briefs from various sources, reasons about implementation scope, and generates task-level execution plans.

## Architecture

```
Input Sources              Blueprint Processing           Output Targets
─────────────────         ────────────────────           ──────────────
Max Design Briefs ─┐                                  ┌─→ Relay
Graph Nodes ───────┤                                  ├─→ Smoothie
Manual Files ──────┤─→ SourceBrief ─→ Implementation ─┤─→ Codex
GitHub Issues ─────┤      (normalize)    Brief (LLM)  ├─→ Claude Code
Obsidian Notes ────┘                         ↓         └─→ GitHub Issues
                                      Execution Plan
                                          (LLM)
```

## Installation

```bash
# Install dependencies
poetry install

# Initialize database
blueprint db init
```

## Configuration

Copy `.blueprint.yaml.example` to `~/.blueprint.yaml` or `./.blueprint.yaml` and customize:

```yaml
database:
  path: ~/.blueprint/blueprint.db

sources:
  max:
    db_path: ~/Project/experiments/max/max.db

llm:
  provider: anthropic
  default_model: claude-opus-4-6

exports:
  output_dir: ~/blueprint-exports
```

Set `ANTHROPIC_API_KEY` environment variable for LLM features.

## Usage

### Import design briefs

```bash
# Import from Max
blueprint import max dbf-abc123

# Import from markdown file
blueprint import manual ./my-brief.md
```

### Generate implementation brief

```bash
# List source briefs
blueprint source list

# Generate implementation brief (uses LLM)
blueprint brief create sb-xyz789

# View brief
blueprint brief inspect ib-def456
```

### Generate execution plan

```bash
# Generate plan (uses LLM)
blueprint plan create ib-def456

# View plan
blueprint plan inspect plan-ghi789
```

### Export to execution engines

```bash
# Export to specific engine
blueprint export plan-ghi789 --target relay
blueprint export plan-ghi789 --target smoothie
blueprint export plan-ghi789 --target codex
blueprint export plan-ghi789 --target claude-code

# Export to all engines
blueprint export plan-ghi789 --target all
```

## Core Concepts

### Source Brief
Normalized design brief from any source system. Preserves original structure in `source_payload` while extracting common fields (title, domain, summary).

### Implementation Brief
Engine-neutral implementation specification with:
- Problem statement and MVP goal
- Scope (what's in) and non-goals (what's out)
- Architecture notes and integration points
- Risks and validation plan
- Definition of done

### Execution Plan
Task-level breakdown with:
- 3-5 milestones (sequential phases)
- Tasks with dependencies
- Acceptance criteria per task
- Suggested execution engine per task
- Test strategy

## Development

```bash
# Run tests
pytest

# Format code
black src/ tests/

# Lint
ruff check src/ tests/
```

## License

MIT
