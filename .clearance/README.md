# Publication Clearance Configuration

This directory contains configuration and baseline data for publication clearance monitoring.

## Files

### `policy-snapshots.json`
Baseline snapshots of AI provider policy pages. This file is committed to the repository to enable ongoing monitoring of policy changes.

**Initial Snapshot Date:** 2026-04-23T07:06:02+00:00

**Monitored Providers:**
- **OpenAI** (3 sources): Terms index, Usage policies, Service terms
- **Anthropic** (2 sources): Claude Code data usage, Usage policy update

**Purpose:** The clearance monitor compares current policy page content against these baseline hashes. If a policy source changes, human review is required before accepting the new snapshot.

### `AI_PROVIDER_REVIEW.md` (gitignored)
Private review documentation capturing the AI provider policy compliance review. This file contains internal review decisions and is **not committed** to the public repository.

## Monitoring

Run policy monitoring periodically to detect changes:

```bash
# If clearance is installed:
clearance monitor --project . --report-dir clearance-report

# Or from the clearance source repository:
cd /path/to/clearance
PYTHONPATH=src python -m clearance monitor --project /path/to/blueprint --report-dir clearance-report
```

When a policy change is detected:
1. Review the changed policy page
2. Assess impact on codebase, examples, and documentation
3. Update code/docs if needed
4. Commit the new snapshot only after review is complete

## GitHub Actions

Consider adding a workflow to run `clearance check` and `clearance monitor` on a schedule (e.g., weekly) to catch policy changes early.
