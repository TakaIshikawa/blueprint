"""Generate environment provisioning matrix for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar, cast

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


EnvironmentType = Literal[
    "development",
    "staging",
    "production",
    "qa",
    "preview",
    "disaster_recovery",
    "local",
]
InfrastructureCode = Literal[
    "terraform",
    "cloudformation",
    "pulumi",
    "ansible",
    "chef",
    "puppet",
    "kubernetes",
    "docker_compose",
    "cdk",
]
ConfigurationManagement = Literal[
    "ansible",
    "chef",
    "puppet",
    "salt",
    "configuration_file",
    "environment_variables",
    "secrets_manager",
    "config_maps",
]
SecretsManagement = Literal[
    "vault",
    "aws_secrets_manager",
    "azure_key_vault",
    "gcp_secret_manager",
    "kubernetes_secrets",
    "encrypted_files",
    "parameter_store",
    "doppler",
]
ProvisioningStrategy = Literal[
    "dev_environment",
    "staging_environment",
    "prod_environment",
    "terraform_iac",
    "ansible_config",
    "vault_secrets",
    "automated_provisioning",
    "environment_parity",
]
ProvisioningGap = Literal[
    "missing_dev_environment",
    "missing_staging_environment",
    "missing_prod_environment",
    "no_infrastructure_as_code",
    "no_configuration_management",
    "no_secrets_management",
    "manual_provisioning",
    "environment_drift",
    "missing_automation",
    "no_environment_parity",
    "hardcoded_credentials",
    "missing_disaster_recovery",
]

_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_DEV_ENV_RE = re.compile(
    r"\b(?:dev(?:elopment)?[\s-]?env(?:ironment)?|local[\s-]?env|dev[\s-]?setup|"
    r"development[\s-]?setup|developer[\s-]?environment|development,)\b",
    re.IGNORECASE,
)
_STAGING_ENV_RE = re.compile(
    r"\b(?:staging[\s-]?env(?:ironment)?|stage[\s-]?env|qa[\s-]?env(?:ironment)?|"
    r"test[\s-]?env(?:ironment)?|pre[\s-]?prod|staging,)\b",
    re.IGNORECASE,
)
_PROD_ENV_RE = re.compile(
    r"\b(?:prod(?:uction)?[\s-]?env(?:ironment)?|live[\s-]?env|production[\s-]?setup|production[\s-]?env|production,)\b",
    re.IGNORECASE,
)
_TERRAFORM_RE = re.compile(
    r"\b(?:terraform|\.tf\b|tfvars|terraform[\s-]?cloud|tf[\s-]?state)\b",
    re.IGNORECASE,
)
_ANSIBLE_RE = re.compile(
    r"\b(?:ansible|playbook|ansible[\s-]?galaxy|\.yml\s+playbook)\b",
    re.IGNORECASE,
)
_VAULT_RE = re.compile(
    r"\b(?:hashicorp[\s-]?vault|vault[\s-]?secret|vault[\s-]?agent|\bvault\b.*secret|\bvault\sfor\s)",
    re.IGNORECASE,
)
_AWS_SECRETS_RE = re.compile(
    r"\b(?:aws[\s-]?secrets?[\s-]?manager|secretsmanager|parameter[\s-]?store|ssm)\b",
    re.IGNORECASE,
)
_CLOUDFORMATION_RE = re.compile(
    r"\b(?:cloudformation|cfn|\.yaml\s+stack|aws[\s-]?stack)\b",
    re.IGNORECASE,
)
_PULUMI_RE = re.compile(
    r"\b(?:pulumi|pulumi[\s-]?stack|pulumi[\s-]?config)\b",
    re.IGNORECASE,
)
_KUBERNETES_RE = re.compile(
    r"\b(?:kubernetes|k8s|kubectl|kustomize|helm|k8s[\s-]?manifest)\b",
    re.IGNORECASE,
)
_DOCKER_RE = re.compile(
    r"\b(?:docker[\s-]?compose|dockerfile|docker[\s-]?container|docker[\s-]?image)\b",
    re.IGNORECASE,
)
_CONFIG_MGMT_RE = re.compile(
    r"\b(?:configuration[\s-]?management|config[\s-]?file|\.env\b|environment[\s-]?variable|"
    r"config[\s-]?map|configmap|environment[\s-]?specific[\s-]?configuration)\b",
    re.IGNORECASE,
)
_AUTOMATION_RE = re.compile(
    r"\b(?:automat(?:ed|ion)|ci[\s-]?cd|pipeline|self[\s-]?provision|auto[\s-]?deploy)\b",
    re.IGNORECASE,
)
_PARITY_RE = re.compile(
    r"\b(?:environment[\s-]?parity|dev[\s-]?prod[\s-]?parity|consistent[\s-]?env|"
    r"identical[\s-]?env|replicate[\s-]?prod)\b",
    re.IGNORECASE,
)
_DRIFT_RE = re.compile(
    r"\b(?:configuration[\s-]?drift|env(?:ironment)?[\s-]?drift|inconsistent[\s-]?config|"
    r"drift[\s-]?detection)\b",
    re.IGNORECASE,
)
_HARDCODED_RE = re.compile(
    r"\b(?:hardcoded|hard[\s-]?coded|embedded[\s-]?credential|plaintext[\s-]?password|"
    r"password[\s-]?in[\s-]?code)\b",
    re.IGNORECASE,
)
_DR_RE = re.compile(
    r"\b(?:disaster[\s-]?recovery|dr[\s-]?env|backup[\s-]?env|failover[\s-]?env|"
    r"recovery[\s-]?site)\b",
    re.IGNORECASE,
)
_CHEF_RE = re.compile(r"\b(?:chef|cookbook|chef[\s-]?recipe)\b", re.IGNORECASE)
_PUPPET_RE = re.compile(r"\b(?:puppet|puppet[\s-]?manifest)\b", re.IGNORECASE)
_AZURE_SECRETS_RE = re.compile(
    r"\b(?:azure[\s-]?key[\s-]?vault|keyvault)\b", re.IGNORECASE
)
_GCP_SECRETS_RE = re.compile(
    r"\b(?:gcp[\s-]?secret[\s-]?manager|google[\s-]?secret[\s-]?manager)\b",
    re.IGNORECASE,
)
_K8S_SECRETS_RE = re.compile(
    r"\b(?:kubernetes[\s-]?secret|k8s[\s-]?secret|secret.*(?:kubernetes|k8s))\b", re.IGNORECASE
)

_ENV_TYPE_ORDER: dict[EnvironmentType, int] = {
    "local": 0,
    "development": 1,
    "qa": 2,
    "preview": 3,
    "staging": 4,
    "production": 5,
    "disaster_recovery": 6,
}

_STRATEGY_ORDER: dict[ProvisioningStrategy, int] = {
    "dev_environment": 0,
    "staging_environment": 1,
    "prod_environment": 2,
    "terraform_iac": 3,
    "ansible_config": 4,
    "vault_secrets": 5,
    "automated_provisioning": 6,
    "environment_parity": 7,
}


@dataclass(frozen=True, slots=True)
class ProvisioningRequirement:
    """Individual provisioning requirement extracted from a task."""

    environment_type: EnvironmentType
    infrastructure_code: InfrastructureCode | None = None
    configuration_mgmt: ConfigurationManagement | None = None
    secrets_mgmt: SecretsManagement | None = None
    strategies: tuple[ProvisioningStrategy, ...] = field(default_factory=tuple)
    is_automated: bool = True
    description: str | None = None
    tools_mentioned: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "environment_type": self.environment_type,
            "infrastructure_code": self.infrastructure_code,
            "configuration_mgmt": self.configuration_mgmt,
            "secrets_mgmt": self.secrets_mgmt,
            "strategies": list(self.strategies),
            "is_automated": self.is_automated,
            "description": self.description,
            "tools_mentioned": list(self.tools_mentioned),
        }


@dataclass(frozen=True, slots=True)
class TaskProvisioningRecord:
    """Environment provisioning assessment for a single execution task."""

    task_id: str
    title: str
    environment_types: tuple[EnvironmentType, ...]
    infrastructure_codes: tuple[InfrastructureCode, ...]
    configuration_mgmts: tuple[ConfigurationManagement, ...]
    secrets_mgmts: tuple[SecretsManagement, ...]
    requirements: tuple[ProvisioningRequirement, ...]
    gaps: tuple[ProvisioningGap, ...]
    automation_score: float
    maturity_score: float
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "environment_types": list(self.environment_types),
            "infrastructure_codes": list(self.infrastructure_codes),
            "configuration_mgmts": list(self.configuration_mgmts),
            "secrets_mgmts": list(self.secrets_mgmts),
            "requirements": [req.to_dict() for req in self.requirements],
            "gaps": list(self.gaps),
            "automation_score": self.automation_score,
            "maturity_score": self.maturity_score,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class EnvironmentProvisioningMatrix:
    """Complete environment provisioning matrix for an execution plan."""

    plan_id: str | None = None
    records: tuple[TaskProvisioningRecord, ...] = field(default_factory=tuple)

    @property
    def summary(self) -> dict[str, Any]:
        """Return summary metrics."""
        if not self.records:
            return {
                "task_count": 0,
                "average_automation_score": 0.0,
                "average_maturity_score": 0.0,
                "environment_coverage": {},
                "infrastructure_coverage": {},
                "total_gaps_count": 0,
                "recommendations": [],
            }

        env_count: dict[str, int] = {}
        for record in self.records:
            for env_type in record.environment_types:
                env_count[env_type] = env_count.get(env_type, 0) + 1

        infra_count: dict[str, int] = {}
        for record in self.records:
            for infra in record.infrastructure_codes:
                infra_count[infra] = infra_count.get(infra, 0) + 1

        avg_automation = sum(r.automation_score for r in self.records) / len(
            self.records
        )
        avg_maturity = sum(r.maturity_score for r in self.records) / len(self.records)
        total_gaps = sum(len(r.gaps) for r in self.records)

        recommendations = _generate_recommendations(
            self.records, avg_automation, avg_maturity, env_count, total_gaps
        )

        return {
            "task_count": len(self.records),
            "average_automation_score": round(avg_automation, 2),
            "average_maturity_score": round(avg_maturity, 2),
            "environment_coverage": dict(sorted(env_count.items())),
            "infrastructure_coverage": dict(sorted(infra_count.items())),
            "total_gaps_count": total_gaps,
            "recommendations": recommendations,
        }

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "plan_id": self.plan_id,
            "summary": self.summary,
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return provisioning records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Environment Provisioning Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]

        if not self.records:
            lines.extend(["", "No execution tasks were analyzed."])
            return "\n".join(lines)

        summary = self.summary
        lines.extend(
            [
                "",
                "## Summary",
                f"- **Tasks analyzed**: {summary['task_count']}",
                f"- **Average automation**: {summary['average_automation_score']:.0%}",
                f"- **Average maturity**: {summary['average_maturity_score']:.0%}",
                f"- **Total gaps identified**: {summary['total_gaps_count']}",
                "",
            ]
        )

        if summary["recommendations"]:
            lines.append("### Recommendations")
            for rec in summary["recommendations"]:
                lines.append(f"- {rec}")
            lines.append("")

        lines.extend(
            [
                "## Provisioning Matrix",
                "| Task | Environments | IaC | Automation | Maturity | Gaps |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )

        for record in self.records:
            env_str = (
                ", ".join(record.environment_types)
                if record.environment_types
                else "none"
            )
            iac_str = (
                ", ".join(record.infrastructure_codes)
                if record.infrastructure_codes
                else "none"
            )
            gaps_str = f"{len(record.gaps)} gap(s)" if record.gaps else "none"
            lines.append(
                f"| {_markdown_cell(f'{record.task_id}: {record.title}')} | "
                f"{_markdown_cell(env_str)} | "
                f"{_markdown_cell(iac_str)} | "
                f"{record.automation_score:.0%} | "
                f"{record.maturity_score:.0%} | "
                f"{gaps_str} |"
            )

        return "\n".join(lines)


def build_environment_provisioning_matrix(
    source: Mapping[str, Any] | ExecutionPlan,
) -> EnvironmentProvisioningMatrix:
    """Generate environment provisioning matrix from an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))

    records = tuple(
        sorted(
            (
                _build_record(
                    task,
                    fallback_id=f"task-{index}",
                )
                for index, task in enumerate(tasks, start=1)
            ),
            key=_record_sort_key,
        )
    )

    return EnvironmentProvisioningMatrix(
        plan_id=_optional_text(plan.get("id")), records=records
    )


def environment_provisioning_matrix_to_dict(
    matrix: EnvironmentProvisioningMatrix,
) -> dict[str, Any]:
    """Serialize an environment provisioning matrix to a plain dictionary."""
    return matrix.to_dict()


environment_provisioning_matrix_to_dict.__test__ = False


def environment_provisioning_matrix_to_markdown(
    matrix: EnvironmentProvisioningMatrix,
) -> str:
    """Render an environment provisioning matrix as Markdown."""
    return matrix.to_markdown()


environment_provisioning_matrix_to_markdown.__test__ = False


def summarize_environment_provisioning(
    source: Mapping[str, Any] | ExecutionPlan | EnvironmentProvisioningMatrix,
) -> dict[str, Any]:
    """Return environment provisioning summary for a plan or matrix."""
    if isinstance(source, EnvironmentProvisioningMatrix):
        return source.summary
    return build_environment_provisioning_matrix(source).summary


summarize_environment_provisioning.__test__ = False


def _build_record(
    task: Mapping[str, Any],
    *,
    fallback_id: str,
) -> TaskProvisioningRecord:
    """Build a provisioning record for a single task."""
    task_id = _optional_text(task.get("id")) or fallback_id
    title = _optional_text(task.get("title")) or task_id
    context = _task_context(task)
    metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}

    # Extract provisioning signals
    environment_types = _extract_environment_types(context, metadata)
    infrastructure_codes = _extract_infrastructure_codes(context, metadata)
    configuration_mgmts = _extract_configuration_mgmts(context, metadata)
    secrets_mgmts = _extract_secrets_mgmts(context, metadata)
    requirements = _extract_requirements(
        context,
        metadata,
        environment_types,
        infrastructure_codes,
        configuration_mgmts,
        secrets_mgmts,
    )
    gaps = _identify_gaps(
        context,
        metadata,
        environment_types,
        infrastructure_codes,
        configuration_mgmts,
        secrets_mgmts,
        requirements,
    )
    automation_score = _calculate_automation_score(context, requirements, gaps)
    maturity_score = _calculate_maturity_score(
        environment_types,
        infrastructure_codes,
        configuration_mgmts,
        secrets_mgmts,
        gaps,
    )
    evidence = _build_evidence(
        context,
        environment_types,
        infrastructure_codes,
        configuration_mgmts,
        secrets_mgmts,
        requirements,
        gaps,
        automation_score,
        maturity_score,
    )

    return TaskProvisioningRecord(
        task_id=task_id,
        title=title,
        environment_types=environment_types,
        infrastructure_codes=infrastructure_codes,
        configuration_mgmts=configuration_mgmts,
        secrets_mgmts=secrets_mgmts,
        requirements=requirements,
        gaps=gaps,
        automation_score=automation_score,
        maturity_score=maturity_score,
        evidence=tuple(evidence),
    )


def _extract_environment_types(
    context: str, metadata: Mapping[str, Any]
) -> tuple[EnvironmentType, ...]:
    """Extract environment types mentioned in the context."""
    types: list[EnvironmentType] = []

    # Check for explicit metadata
    explicit = _metadata_value(metadata, "environment_types", "environments")
    if explicit:
        for env_type in explicit.lower().split(","):
            env_type = env_type.strip()
            if env_type in _ENV_TYPE_ORDER:
                types.append(cast(EnvironmentType, env_type))

    # Pattern matching
    if _DEV_ENV_RE.search(context) or re.search(r"\bdevelopment\b", context, re.IGNORECASE):
        types.append("development")
    if _STAGING_ENV_RE.search(context) or re.search(r"\bstaging\b", context, re.IGNORECASE):
        types.append("staging")
    if _PROD_ENV_RE.search(context) or re.search(r"\bproduction\b", context, re.IGNORECASE):
        types.append("production")
    if _DR_RE.search(context):
        types.append("disaster_recovery")
    if "qa" in context.lower() and "staging" not in types:
        types.append("qa")
    if "preview" in context.lower():
        types.append("preview")
    if "local" in context.lower() and not types:
        types.append("local")

    return tuple(_dedupe(types))


def _extract_infrastructure_codes(
    context: str, metadata: Mapping[str, Any]
) -> tuple[InfrastructureCode, ...]:
    """Extract infrastructure as code tools mentioned in the context."""
    codes: list[InfrastructureCode] = []

    # Check for explicit metadata
    explicit = _metadata_value(metadata, "infrastructure_code", "iac_tools")
    if explicit:
        for iac in explicit.lower().split(","):
            iac = iac.strip()
            if iac == "terraform":
                codes.append("terraform")
            elif iac == "cloudformation":
                codes.append("cloudformation")
            elif iac == "pulumi":
                codes.append("pulumi")
            elif iac == "kubernetes":
                codes.append("kubernetes")

    # Pattern matching
    if _TERRAFORM_RE.search(context):
        codes.append("terraform")
    if _CLOUDFORMATION_RE.search(context):
        codes.append("cloudformation")
    if _PULUMI_RE.search(context):
        codes.append("pulumi")
    if _ANSIBLE_RE.search(context):
        codes.append("ansible")
    if _KUBERNETES_RE.search(context):
        codes.append("kubernetes")
    if _DOCKER_RE.search(context):
        codes.append("docker_compose")
    if _CHEF_RE.search(context):
        codes.append("chef")
    if _PUPPET_RE.search(context):
        codes.append("puppet")
    if "cdk" in context.lower() or "aws cdk" in context.lower():
        codes.append("cdk")

    return tuple(_dedupe(codes))


def _extract_configuration_mgmts(
    context: str, metadata: Mapping[str, Any]
) -> tuple[ConfigurationManagement, ...]:
    """Extract configuration management tools mentioned in the context."""
    mgmts: list[ConfigurationManagement] = []

    # Pattern matching
    if _ANSIBLE_RE.search(context):
        mgmts.append("ansible")
    if _CHEF_RE.search(context):
        mgmts.append("chef")
    if _PUPPET_RE.search(context):
        mgmts.append("puppet")
    if _CONFIG_MGMT_RE.search(context):
        if "configmap" in context.lower() or "config map" in context.lower():
            mgmts.append("config_maps")
        elif ".env" in context.lower() or "environment variable" in context.lower():
            mgmts.append("environment_variables")
        else:
            mgmts.append("configuration_file")

    return tuple(_dedupe(mgmts))


def _extract_secrets_mgmts(
    context: str, metadata: Mapping[str, Any]
) -> tuple[SecretsManagement, ...]:
    """Extract secrets management tools mentioned in the context."""
    mgmts: list[SecretsManagement] = []

    # Check for explicit metadata
    explicit = _metadata_value(metadata, "secrets_management", "secrets_tools")
    if explicit:
        for secret_tool in explicit.lower().split(","):
            secret_tool = secret_tool.strip()
            if secret_tool == "vault":
                mgmts.append("vault")
            elif "aws" in secret_tool:
                mgmts.append("aws_secrets_manager")

    # Pattern matching
    if _VAULT_RE.search(context) or re.search(r"\bvault\b", context, re.IGNORECASE):
        mgmts.append("vault")
    if _AWS_SECRETS_RE.search(context):
        if "parameter" in context.lower() or "ssm" in context.lower():
            mgmts.append("parameter_store")
        else:
            mgmts.append("aws_secrets_manager")
    if _AZURE_SECRETS_RE.search(context):
        mgmts.append("azure_key_vault")
    if _GCP_SECRETS_RE.search(context):
        mgmts.append("gcp_secret_manager")
    if _K8S_SECRETS_RE.search(context):
        mgmts.append("kubernetes_secrets")
    if "doppler" in context.lower():
        mgmts.append("doppler")
    if "encrypted" in context.lower() and "file" in context.lower():
        mgmts.append("encrypted_files")

    return tuple(_dedupe(mgmts))


def _extract_requirements(
    context: str,
    metadata: Mapping[str, Any],
    environment_types: tuple[EnvironmentType, ...],
    infrastructure_codes: tuple[InfrastructureCode, ...],
    configuration_mgmts: tuple[ConfigurationManagement, ...],
    secrets_mgmts: tuple[SecretsManagement, ...],
) -> tuple[ProvisioningRequirement, ...]:
    """Extract detailed provisioning requirements."""
    requirements: list[ProvisioningRequirement] = []

    # Determine automation status
    is_automated = _AUTOMATION_RE.search(context) or bool(infrastructure_codes)
    if "manual" in context.lower():
        is_automated = False

    # Determine strategies
    strategies: list[ProvisioningStrategy] = []
    if "development" in environment_types:
        strategies.append("dev_environment")
    if "staging" in environment_types:
        strategies.append("staging_environment")
    if "production" in environment_types:
        strategies.append("prod_environment")
    if "terraform" in infrastructure_codes:
        strategies.append("terraform_iac")
    if "ansible" in infrastructure_codes or "ansible" in configuration_mgmts:
        strategies.append("ansible_config")
    if "vault" in secrets_mgmts:
        strategies.append("vault_secrets")
    if is_automated:
        strategies.append("automated_provisioning")
    if _PARITY_RE.search(context):
        strategies.append("environment_parity")

    # Extract tools
    tools: list[str] = []
    tool_patterns = [
        "terraform",
        "ansible",
        "vault",
        "docker",
        "kubernetes",
        "pulumi",
        "cloudformation",
        "chef",
        "puppet",
    ]
    for tool in tool_patterns:
        if re.search(rf"\b{tool}\b", context, re.IGNORECASE):
            tools.append(tool)

    # Create requirements for each environment type
    for env_type in environment_types:
        requirements.append(
            ProvisioningRequirement(
                environment_type=env_type,
                infrastructure_code=infrastructure_codes[0]
                if infrastructure_codes
                else None,
                configuration_mgmt=configuration_mgmts[0]
                if configuration_mgmts
                else None,
                secrets_mgmt=secrets_mgmts[0] if secrets_mgmts else None,
                strategies=tuple(_dedupe(strategies)),
                is_automated=is_automated,
                tools_mentioned=tuple(tools),
            )
        )

    # If no environment types but we have provisioning-related info
    if not environment_types and (
        infrastructure_codes or configuration_mgmts or secrets_mgmts or strategies
    ):
        # Infer environment type from context
        inferred_env: EnvironmentType = "development"  # default
        if _PROD_ENV_RE.search(context):
            inferred_env = "production"
        elif _STAGING_ENV_RE.search(context):
            inferred_env = "staging"

        requirements.append(
            ProvisioningRequirement(
                environment_type=inferred_env,
                infrastructure_code=infrastructure_codes[0]
                if infrastructure_codes
                else None,
                configuration_mgmt=configuration_mgmts[0]
                if configuration_mgmts
                else None,
                secrets_mgmt=secrets_mgmts[0] if secrets_mgmts else None,
                strategies=tuple(_dedupe(strategies)),
                is_automated=is_automated,
                tools_mentioned=tuple(tools),
            )
        )

    return tuple(requirements)


def _identify_gaps(
    context: str,
    metadata: Mapping[str, Any],
    environment_types: tuple[EnvironmentType, ...],
    infrastructure_codes: tuple[InfrastructureCode, ...],
    configuration_mgmts: tuple[ConfigurationManagement, ...],
    secrets_mgmts: tuple[SecretsManagement, ...],
    requirements: tuple[ProvisioningRequirement, ...],
) -> tuple[ProvisioningGap, ...]:
    """Identify provisioning gaps."""
    gaps: list[ProvisioningGap] = []

    # Check for missing environment types
    if "development" not in environment_types and _requires_dev_env(context):
        gaps.append("missing_dev_environment")
    if "staging" not in environment_types and _requires_staging_env(context):
        gaps.append("missing_staging_environment")
    if "production" not in environment_types and _requires_prod_env(context):
        gaps.append("missing_prod_environment")

    # Check for infrastructure as code
    if not infrastructure_codes and _requires_iac(context):
        gaps.append("no_infrastructure_as_code")

    # Check for configuration management
    if not configuration_mgmts and _requires_config_mgmt(context):
        gaps.append("no_configuration_management")

    # Check for secrets management
    if not secrets_mgmts and _requires_secrets_mgmt(context):
        gaps.append("no_secrets_management")

    # Check for hardcoded credentials
    if _HARDCODED_RE.search(context):
        gaps.append("hardcoded_credentials")

    # Check for manual provisioning
    if not requirements or all(not req.is_automated for req in requirements):
        if "manual" in context.lower() or not _AUTOMATION_RE.search(context):
            gaps.append("manual_provisioning")
            gaps.append("missing_automation")

    # Check for environment drift
    if _DRIFT_RE.search(context):
        gaps.append("environment_drift")

    # Check for environment parity
    if _should_have_parity(context) and not _PARITY_RE.search(context):
        gaps.append("no_environment_parity")

    # Check for disaster recovery
    if _should_have_dr(context) and "disaster_recovery" not in environment_types:
        gaps.append("missing_disaster_recovery")

    return tuple(_dedupe(gaps))


def _calculate_automation_score(
    context: str,
    requirements: tuple[ProvisioningRequirement, ...],
    gaps: tuple[ProvisioningGap, ...],
) -> float:
    """Calculate automation score (0.0 to 1.0)."""
    score = 0.0

    # Base score from automated requirements (40%)
    if requirements:
        automated_count = sum(1 for req in requirements if req.is_automated)
        score += 0.4 * (automated_count / len(requirements))

    # IaC tools presence (30%)
    if requirements and any(req.infrastructure_code for req in requirements):
        score += 0.3

    # Configuration and secrets automation (20%)
    if requirements:
        if any(req.configuration_mgmt for req in requirements):
            score += 0.1
        if any(req.secrets_mgmt for req in requirements):
            score += 0.1

    # Strategies bonus (10%)
    if requirements:
        strategies_set = set()
        for req in requirements:
            strategies_set.update(req.strategies)
        if "automated_provisioning" in strategies_set:
            score += 0.1

    # Penalties for gaps
    gap_penalties = {
        "manual_provisioning": 0.2,
        "missing_automation": 0.15,
        "no_infrastructure_as_code": 0.15,
    }
    for gap in gaps:
        score -= gap_penalties.get(gap, 0.0)

    return max(0.0, min(1.0, score))


def _calculate_maturity_score(
    environment_types: tuple[EnvironmentType, ...],
    infrastructure_codes: tuple[InfrastructureCode, ...],
    configuration_mgmts: tuple[ConfigurationManagement, ...],
    secrets_mgmts: tuple[SecretsManagement, ...],
    gaps: tuple[ProvisioningGap, ...],
) -> float:
    """Calculate maturity score (0.0 to 1.0)."""
    score = 0.0

    # Environment coverage (25%)
    env_weights = {
        "development": 0.08,
        "staging": 0.08,
        "production": 0.09,
    }
    for env_type in environment_types:
        score += env_weights.get(env_type, 0.0)

    # Infrastructure as code (25%)
    if infrastructure_codes:
        score += 0.25

    # Configuration management (20%)
    if configuration_mgmts:
        score += 0.20

    # Secrets management (20%)
    if secrets_mgmts:
        score += 0.20

    # Environment parity and DR (10%)
    if "disaster_recovery" in environment_types:
        score += 0.05
    if len(environment_types) >= 3:  # Multiple environments suggest parity
        score += 0.05

    # Penalties for critical gaps
    critical_gaps = {
        "missing_prod_environment": 0.15,
        "no_infrastructure_as_code": 0.15,
        "no_secrets_management": 0.10,
        "hardcoded_credentials": 0.15,
        "environment_drift": 0.05,
    }
    for gap in gaps:
        score -= critical_gaps.get(gap, 0.0)

    return max(0.0, min(1.0, score))


def _build_evidence(
    context: str,
    environment_types: tuple[EnvironmentType, ...],
    infrastructure_codes: tuple[InfrastructureCode, ...],
    configuration_mgmts: tuple[ConfigurationManagement, ...],
    secrets_mgmts: tuple[SecretsManagement, ...],
    requirements: tuple[ProvisioningRequirement, ...],
    gaps: tuple[ProvisioningGap, ...],
    automation_score: float,
    maturity_score: float,
) -> list[str]:
    """Build evidence list for the provisioning assessment."""
    evidence: list[str] = []

    if environment_types:
        evidence.append(f"Environments identified: {', '.join(environment_types)}.")
    else:
        evidence.append("No environment types identified.")

    if infrastructure_codes:
        evidence.append(f"IaC tools: {', '.join(infrastructure_codes)}.")

    if configuration_mgmts:
        evidence.append(
            f"Configuration management: {', '.join(configuration_mgmts)}."
        )

    if secrets_mgmts:
        evidence.append(f"Secrets management: {', '.join(secrets_mgmts)}.")

    if requirements:
        evidence.append(f"{len(requirements)} provisioning requirement(s) extracted.")
        automated_count = sum(1 for req in requirements if req.is_automated)
        if automated_count > 0:
            evidence.append(f"{automated_count} automated requirement(s).")

    if gaps:
        evidence.append(f"{len(gaps)} gap(s) identified: {', '.join(gaps[:3])}.")

    evidence.append(f"Automation score: {automation_score:.0%}.")
    evidence.append(f"Maturity score: {maturity_score:.0%}.")

    return evidence


def _generate_recommendations(
    records: tuple[TaskProvisioningRecord, ...],
    avg_automation: float,
    avg_maturity: float,
    env_count: dict[str, int],
    total_gaps: int,
) -> list[str]:
    """Generate provisioning recommendations based on analysis."""
    recommendations: list[str] = []

    if avg_automation < 0.5:
        recommendations.append(
            "Low automation detected. Consider implementing infrastructure as code."
        )

    if avg_maturity < 0.5:
        recommendations.append(
            "Low maturity score. Improve environment coverage and tooling."
        )

    if env_count.get("production", 0) < len(records) * 0.5:
        recommendations.append("Ensure production environment provisioning is defined.")

    if env_count.get("staging", 0) == 0 and len(records) > 3:
        recommendations.append("Add staging environment for pre-production validation.")

    if not any("terraform" in r.infrastructure_codes for r in records) and not any(
        "kubernetes" in r.infrastructure_codes for r in records
    ):
        recommendations.append("Consider adopting IaC tools like Terraform or Kubernetes.")

    secrets_count = sum(1 for r in records if r.secrets_mgmts)
    if secrets_count < len(records) * 0.3:
        recommendations.append(
            "Implement secrets management solution (Vault, AWS Secrets Manager, etc.)."
        )

    if total_gaps > len(records) * 2:
        recommendations.append("Address provisioning gaps to improve infrastructure maturity.")

    manual_count = sum(
        1 for r in records if "manual_provisioning" in r.gaps
    )
    if manual_count > len(records) * 0.3:
        recommendations.append("Automate manual provisioning processes.")

    return recommendations


def _requires_dev_env(context: str) -> bool:
    """Check if task requires development environment."""
    return bool(_DEV_ENV_RE.search(context))


def _requires_staging_env(context: str) -> bool:
    """Check if task requires staging environment."""
    return bool(
        re.search(
            r"\b(?:test|qa|pre[\s-]?prod|validation|staging|preview)\b",
            context,
            re.IGNORECASE,
        )
    )


def _requires_prod_env(context: str) -> bool:
    """Check if task requires production environment."""
    return bool(
        re.search(
            r"\b(?:prod(?:uction)?|live|deploy(?:ment)?|release)\b",
            context,
            re.IGNORECASE,
        )
    )


def _requires_iac(context: str) -> bool:
    """Check if task requires infrastructure as code."""
    return bool(
        re.search(
            r"\b(?:infrastructure|provision|deploy|cloud|aws|azure|gcp|server|container)\b",
            context,
            re.IGNORECASE,
        )
    )


def _requires_config_mgmt(context: str) -> bool:
    """Check if task requires configuration management."""
    return bool(
        re.search(
            r"\b(?:config(?:uration)?|environment|setup|install|deploy)\b",
            context,
            re.IGNORECASE,
        )
    )


def _requires_secrets_mgmt(context: str) -> bool:
    """Check if task requires secrets management."""
    return bool(
        re.search(
            r"\b(?:secret|password|credential|api[\s-]?key|token|auth|certificate)\b",
            context,
            re.IGNORECASE,
        )
    )


def _should_have_parity(context: str) -> bool:
    """Check if task should have environment parity."""
    return bool(
        re.search(
            r"\b(?:multiple[\s-]?env|all[\s-]?env|consistent|replicate|identical)\b",
            context,
            re.IGNORECASE,
        )
    )


def _should_have_dr(context: str) -> bool:
    """Check if task should have disaster recovery environment."""
    return bool(
        re.search(
            r"\b(?:critical|high[\s-]?availability|ha|failover|backup|resilience)\b",
            context,
            re.IGNORECASE,
        )
    )


def _record_sort_key(record: TaskProvisioningRecord) -> tuple[float, float, str]:
    """Sort records by maturity (desc), automation (desc), then task_id."""
    return (
        -record.maturity_score,  # Higher maturity first
        -record.automation_score,  # Higher automation first
        record.task_id,
    )


def _task_context(task: Mapping[str, Any]) -> str:
    """Build context string from task fields."""
    metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}
    values = [
        _text(task.get("title")),
        _text(task.get("description")),
        *_strings(task.get("acceptance_criteria")),
        *_strings(task.get("files_or_modules")),
        *_strings(metadata),
    ]
    return " ".join(value for value in values if value)


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    """Convert plan to dictionary."""
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    """Convert tasks to list of dictionaries."""
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            dumped = item.model_dump(mode="python")
            if isinstance(dumped, Mapping):
                tasks.append(dict(dumped))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _metadata_value(metadata: Mapping[str, Any], *keys: str) -> str | None:
    """Extract first matching metadata value."""
    values = _metadata_values(metadata, *keys)
    return values[0] if values else None


def _metadata_values(metadata: Mapping[str, Any], *keys: str) -> list[str]:
    """Extract all matching metadata values."""
    values: list[str] = []
    wanted = {key.lower() for key in keys}
    for key, value in metadata.items():
        normalized = str(key).lower()
        if normalized in wanted:
            values.extend(_strings(value))
        elif isinstance(value, Mapping):
            values.extend(_metadata_values(value, *keys))
    return values


def _strings(value: Any) -> list[str]:
    """Extract strings from various data structures."""
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _optional_text(value: Any) -> str | None:
    """Convert value to optional text."""
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    """Convert value to normalized text."""
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> tuple[_T, ...]:
    """Remove duplicates while preserving order."""
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return tuple(deduped)


def _markdown_cell(value: str) -> str:
    """Escape markdown table cell content."""
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "ConfigurationManagement",
    "EnvironmentProvisioningMatrix",
    "EnvironmentType",
    "InfrastructureCode",
    "ProvisioningGap",
    "ProvisioningRequirement",
    "ProvisioningStrategy",
    "SecretsManagement",
    "TaskProvisioningRecord",
    "build_environment_provisioning_matrix",
    "environment_provisioning_matrix_to_dict",
    "environment_provisioning_matrix_to_markdown",
    "summarize_environment_provisioning",
]
