"""Filesystem-only GitHub issue bundle exporter."""

from __future__ import annotations

import json
import re
from collections import OrderedDict
from pathlib import Path
from typing import Any

from blueprint.exporters.base import TargetExporter


class GitHubIssuesExporter(TargetExporter):
    """Export an execution plan as a GitHub-issue-ready bundle."""

    def get_format(self) -> str:
        """Get export format."""
        return "markdown"

    def get_extension(self) -> str:
        """Get file extension."""
        return ""

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Render a directory bundle containing issue drafts and a manifest."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )

        bundle_dir = Path(output_path)
        issues_dir = bundle_dir / "issues"
        issues_dir.mkdir(parents=True, exist_ok=True)

        task_issues = [
            (task, self._issue_filename(index, task))
            for index, task in enumerate(execution_plan.get("tasks", []), start=1)
        ]
        issue_payloads = []
        milestone_groups = self._milestone_groups(execution_plan, task_issues)

        for task, filename in task_issues:
            body = self._issue_body(execution_plan, implementation_brief, task)
            (bundle_dir / filename).write_text(body)
            issue_payloads.append(
                {
                    "task_id": task["id"],
                    "title": task["title"],
                    "body": body,
                    "labels": self._task_labels(task),
                    "depends_on": task.get("depends_on", []),
                    "milestone": task.get("milestone"),
                    "milestone_group": self._milestone_name_for_task(task),
                    "file_path": filename,
                    "relative_path": filename,
                }
            )

        manifest = self._manifest(
            execution_plan,
            implementation_brief,
            issue_payloads,
            milestone_groups,
        )
        (bundle_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

        return str(bundle_dir)

    def _manifest(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        issues: list[dict[str, Any]],
        milestone_groups: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build the manifest payload for the issue bundle."""
        return {
            "schema_version": "blueprint.github-issues.v1",
            "exporter": "github-issues",
            "repository": self._repository_metadata(plan),
            "plan": {
                "id": plan["id"],
                "implementation_brief_id": plan["implementation_brief_id"],
                "target_engine": plan.get("target_engine"),
                "target_repo": plan.get("target_repo"),
                "project_type": plan.get("project_type"),
                "test_strategy": plan.get("test_strategy"),
                "handoff_prompt": plan.get("handoff_prompt"),
            },
            "brief": {
                "id": brief["id"],
                "title": brief["title"],
                "validation_plan": brief.get("validation_plan"),
                "definition_of_done": brief.get("definition_of_done", []),
            },
            "milestone_groups": milestone_groups,
            "issues": issues,
        }

    def _issue_body(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Render a Markdown issue draft body."""
        labels = self._task_labels(task)
        dependencies = task.get("depends_on") or []
        lines = [
            f"# {task['title']}",
            "",
            "## Task Metadata",
            f"- Task ID: `{task['id']}`",
            f"- Plan ID: `{plan['id']}`",
            f"- Repository: {plan.get('target_repo') or 'N/A'}",
            f"- Milestone: {self._milestone_name_for_task(task)}",
            f"- Status: {task.get('status') or 'pending'}",
            f"- Labels: {self._inline_list(labels)}",
            f"- Dependencies: {self._inline_list(dependencies)}",
            f"- Suggested Engine: {task.get('suggested_engine') or 'N/A'}",
            "",
            "## Description",
            task["description"],
            "",
            "## Acceptance Criteria",
        ]
        lines.extend(self._bullet_lines(task.get("acceptance_criteria")))
        lines.extend(
            [
                "",
                "## Dependencies",
            ]
        )
        lines.extend(self._dependency_lines(dependencies))
        lines.extend(
            [
                "",
                "## Labels",
            ]
        )
        lines.extend(self._bullet_lines(labels))
        lines.extend(
            [
                "",
                "## Validation Context",
                f"- Test Strategy: {plan.get('test_strategy') or 'N/A'}",
                f"- Brief Validation Plan: {brief.get('validation_plan') or 'N/A'}",
                "- Definition of Done:",
            ]
        )
        lines.extend(self._bullet_lines(brief.get("definition_of_done"), indent="  "))
        lines.extend(
            [
                "",
                "## Milestone Group",
                self._milestone_name_for_task(task),
            ]
        )
        return "\n".join(lines) + "\n"

    def _milestone_groups(
        self,
        plan: dict[str, Any],
        task_issues: list[tuple[dict[str, Any], str]],
    ) -> list[dict[str, Any]]:
        """Group issue references by milestone while preserving execution order."""
        groups: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
        for index, milestone in enumerate(plan.get("milestones", []), start=1):
            groups[self._milestone_name(milestone, index)] = []

        for task, filename in task_issues:
            milestone_name = self._milestone_name_for_task(task)
            groups.setdefault(milestone_name, []).append(
                {
                    "task_id": task["id"],
                    "title": task["title"],
                    "file_path": filename,
                    "labels": self._task_labels(task),
                    "depends_on": task.get("depends_on", []),
                }
            )

        return [
            {
                "name": milestone_name,
                "issues": items,
                "issue_files": [item["file_path"] for item in items],
            }
            for milestone_name, items in groups.items()
            if items
        ]

    def _repository_metadata(self, plan: dict[str, Any]) -> dict[str, Any]:
        """Derive repository metadata from the target repo string."""
        raw_target_repo = plan.get("target_repo")
        owner, repo = self._parse_repo(raw_target_repo)
        full_name = f"{owner}/{repo}" if owner and repo else raw_target_repo
        return {
            "raw_target_repo": raw_target_repo,
            "owner": owner,
            "name": repo,
            "full_name": full_name,
            "html_url": f"https://github.com/{full_name}" if full_name else None,
            "issues_url": f"https://github.com/{full_name}/issues" if full_name else None,
        }

    def _milestone_name_for_task(self, task: dict[str, Any]) -> str:
        """Return the display milestone for a task."""
        if task.get("milestone"):
            return str(task["milestone"])
        return "Ungrouped"

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Return a display milestone name."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"

    def _issue_filename(self, index: int, task: dict[str, Any]) -> str:
        """Build a stable issue draft filename."""
        return f"issues/{index:03d}-{self._slug(task['id'])}.md"

    def _task_labels(self, task: dict[str, Any]) -> list[str]:
        """Extract task labels from validated task metadata."""
        metadata = task.get("metadata") or {}
        labels = metadata.get("labels") or []
        return [label for label in labels if isinstance(label, str) and label]

    def _dependency_lines(self, dependency_ids: list[str]) -> list[str]:
        """Render dependency IDs as Markdown bullets."""
        if not dependency_ids:
            return ["- None"]
        return [f"- `{dependency_id}`" for dependency_id in dependency_ids]

    def _inline_list(self, value: list[str] | None) -> str:
        """Render a list inline for metadata fields."""
        return ", ".join(value or []) or "None"

    def _bullet_lines(self, value: list[str] | None, indent: str = "") -> list[str]:
        """Render a list as Markdown bullets."""
        items = value or []
        if not items:
            return [f"{indent}- None"]
        return [f"{indent}- {item}" for item in items]

    def _parse_repo(self, value: str | None) -> tuple[str | None, str | None]:
        """Parse an OWNER/REPO target string."""
        if not value:
            return None, None
        parts = value.strip().rstrip("/").split("/", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return None, None
        return parts[0], parts[1]

    def _slug(self, value: str) -> str:
        """Normalize IDs for filesystem-safe filenames."""
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
        return slug or "task"
