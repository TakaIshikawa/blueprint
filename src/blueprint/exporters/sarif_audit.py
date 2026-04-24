"""SARIF exporter for Blueprint execution plan audit findings."""

from __future__ import annotations

import json
from typing import Any

from blueprint.audits.plan_audit import PlanAuditIssue, audit_execution_plan
from blueprint.exporters.base import TargetExporter


SARIF_VERSION = "2.1.0"
SARIF_SCHEMA_URI = "https://json.schemastore.org/sarif-2.1.0.json"
ARTIFACT_URI = "execution-plan.json"
TOOL_NAME = "Blueprint Plan Audit"


class SarifAuditExporter(TargetExporter):
    """Export structural plan audit findings as SARIF JSON."""

    def get_format(self) -> str:
        """Get export format."""
        return "json"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".json"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export plan audit findings to a SARIF 2.1.0 JSON artifact."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        payload = self.render_payload(plan, brief)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
            f.write("\n")

        return output_path

    def render_payload(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
    ) -> dict[str, Any]:
        """Build a SARIF payload from audit findings."""
        audit_result = audit_execution_plan(execution_plan)
        rules = [
            _rule_payload(code, issues)
            for code, issues in sorted(_issues_by_code(audit_result.issues).items())
        ]
        results = [_result_payload(issue) for issue in audit_result.issues]

        return {
            "$schema": SARIF_SCHEMA_URI,
            "version": SARIF_VERSION,
            "runs": [
                {
                    "tool": {
                        "driver": {
                            "name": TOOL_NAME,
                            "informationUri": "https://github.com/takaaa/blueprint",
                            "rules": rules,
                        }
                    },
                    "results": results,
                    "properties": {
                        "blueprintPlanId": execution_plan["id"],
                        "blueprintImplementationBriefId": implementation_brief["id"],
                        "auditErrorCount": audit_result.error_count,
                        "auditWarningCount": audit_result.warning_count,
                    },
                }
            ],
        }


def _issues_by_code(issues: list[PlanAuditIssue]) -> dict[str, list[PlanAuditIssue]]:
    grouped: dict[str, list[PlanAuditIssue]] = {}
    for issue in issues:
        grouped.setdefault(issue.code, []).append(issue)
    return grouped


def _rule_payload(code: str, issues: list[PlanAuditIssue]) -> dict[str, Any]:
    severities = {issue.severity for issue in issues}
    default_level = "error" if "error" in severities else "warning"
    return {
        "id": code,
        "name": code,
        "shortDescription": {"text": code.replace("_", " ").title()},
        "defaultConfiguration": {"level": default_level},
        "properties": {
            "auditCode": code,
            "auditSeverities": sorted(severities),
        },
    }


def _result_payload(issue: PlanAuditIssue) -> dict[str, Any]:
    properties: dict[str, Any] = {
        "auditSeverity": issue.severity,
    }
    if issue.task_id is not None:
        properties["taskId"] = issue.task_id
    if issue.dependency_id is not None:
        properties["dependencyId"] = issue.dependency_id
    if issue.milestone is not None:
        properties["milestone"] = issue.milestone
    if issue.cycle is not None:
        properties["cycle"] = issue.cycle

    return {
        "ruleId": issue.code,
        "level": issue.severity,
        "message": {"text": issue.message},
        "locations": [
            {
                "physicalLocation": {
                    "artifactLocation": {
                        "uri": ARTIFACT_URI,
                    }
                }
            }
        ],
        "properties": properties,
    }
