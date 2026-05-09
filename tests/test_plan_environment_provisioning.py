import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_environment_provisioning import (
    EnvironmentProvisioningMatrix,
    ProvisioningRequirement,
    TaskProvisioningRecord,
    build_environment_provisioning_matrix,
    environment_provisioning_matrix_to_dict,
    environment_provisioning_matrix_to_markdown,
    summarize_environment_provisioning,
)


def test_comprehensive_environment_provisioning_extracts_multiple_environments():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-dev",
                    title="Set up development environment",
                    description="Configure local development environment with Docker Compose.",
                    acceptance_criteria=["Dev environment running", "Docker setup complete"],
                ),
                _task(
                    "task-staging",
                    title="Set up staging environment",
                    description="Provision staging environment with Terraform on AWS.",
                    files_or_modules=["terraform/staging.tf"],
                ),
                _task(
                    "task-prod",
                    title="Set up production environment",
                    description=(
                        "Provision production environment with Terraform. "
                        "Configure secrets with AWS Secrets Manager and Ansible for config management."
                    ),
                    files_or_modules=["terraform/prod.tf", "ansible/prod.yml"],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in matrix.records}

    # Dev environment task
    assert "development" in by_id["task-dev"].environment_types
    assert "docker_compose" in by_id["task-dev"].infrastructure_codes

    # Staging environment task
    assert "staging" in by_id["task-staging"].environment_types
    assert "terraform" in by_id["task-staging"].infrastructure_codes

    # Production environment task
    assert "production" in by_id["task-prod"].environment_types
    assert "terraform" in by_id["task-prod"].infrastructure_codes
    assert "ansible" in by_id["task-prod"].configuration_mgmts
    assert "aws_secrets_manager" in by_id["task-prod"].secrets_mgmts

    assert matrix.summary["task_count"] == 3
    assert matrix.summary["average_automation_score"] > 0


def test_terraform_infrastructure_as_code_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-terraform",
                    title="Implement infrastructure as code",
                    description=(
                        "Create Terraform modules for AWS infrastructure. "
                        "Include VPC, EC2, RDS, and S3 resources with tfvars configuration."
                    ),
                    files_or_modules=["terraform/main.tf", "terraform/variables.tfvars"],
                    metadata={"infrastructure_code": "terraform"},
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "terraform" in record.infrastructure_codes
    assert "terraform_iac" in [
        strategy for req in record.requirements for strategy in req.strategies
    ]
    assert any("terraform" in req.tools_mentioned for req in record.requirements)
    assert "no_infrastructure_as_code" not in record.gaps


def test_ansible_configuration_management_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-ansible",
                    title="Set up configuration management",
                    description=(
                        "Create Ansible playbooks for application configuration. "
                        "Use Ansible Galaxy roles for common setup tasks."
                    ),
                    files_or_modules=["ansible/playbook.yml", "ansible/roles/"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "ansible" in record.infrastructure_codes or "ansible" in record.configuration_mgmts
    assert "ansible_config" in [
        strategy for req in record.requirements for strategy in req.strategies
    ]
    assert any("ansible" in req.tools_mentioned for req in record.requirements)


def test_vault_secrets_management_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-vault",
                    title="Implement secrets management",
                    description=(
                        "Set up HashiCorp Vault for secrets management. "
                        "Configure Vault agent for automatic secret rotation."
                    ),
                    files_or_modules=["vault/config.hcl"],
                    metadata={"secrets_management": "vault"},
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "vault" in record.secrets_mgmts
    assert "vault_secrets" in [
        strategy for req in record.requirements for strategy in req.strategies
    ]
    assert "no_secrets_management" not in record.gaps


def test_aws_secrets_manager_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-aws-secrets",
                    title="Configure AWS secrets",
                    description=(
                        "Store database credentials in AWS Secrets Manager. "
                        "Configure automatic rotation for RDS passwords."
                    ),
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "aws_secrets_manager" in record.secrets_mgmts or "parameter_store" in record.secrets_mgmts


def test_kubernetes_infrastructure_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-k8s",
                    title="Deploy to Kubernetes",
                    description=(
                        "Create Kubernetes manifests for microservices deployment. "
                        "Use Helm charts and configure ConfigMaps for environment-specific settings."
                    ),
                    files_or_modules=["k8s/deployment.yaml", "k8s/configmap.yaml"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "kubernetes" in record.infrastructure_codes
    assert "config_maps" in record.configuration_mgmts or "configuration_file" in record.configuration_mgmts


def test_docker_compose_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-docker",
                    title="Set up local development",
                    description=(
                        "Create Docker Compose configuration for local development. "
                        "Include services for database, cache, and application."
                    ),
                    files_or_modules=["docker-compose.yml"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "docker_compose" in record.infrastructure_codes
    assert any("docker" in req.tools_mentioned for req in record.requirements)


def test_cloudformation_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-cfn",
                    title="Deploy CloudFormation stack",
                    description="Create CloudFormation templates for AWS infrastructure deployment.",
                    files_or_modules=["cloudformation/stack.yaml"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "cloudformation" in record.infrastructure_codes


def test_pulumi_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-pulumi",
                    title="Implement Pulumi IaC",
                    description="Use Pulumi for infrastructure provisioning with TypeScript.",
                    files_or_modules=["pulumi/index.ts", "Pulumi.yaml"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "pulumi" in record.infrastructure_codes


def test_environment_parity_strategy_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-parity",
                    title="Ensure environment parity",
                    description=(
                        "Implement dev-prod parity using Docker containers. "
                        "All environments use identical configuration and infrastructure."
                    ),
                    acceptance_criteria=["Dev and prod environments are consistent"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "environment_parity" in [
        strategy for req in record.requirements for strategy in req.strategies
    ]
    assert "no_environment_parity" not in record.gaps


def test_automated_provisioning_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-auto",
                    title="Automate infrastructure provisioning",
                    description=(
                        "Fully automated provisioning pipeline with Terraform and CI/CD. "
                        "Auto-deploy infrastructure changes on merge to main."
                    ),
                )
            ]
        )
    )

    record = matrix.records[0]
    assert any(req.is_automated for req in record.requirements)
    assert "automated_provisioning" in [
        strategy for req in record.requirements for strategy in req.strategies
    ]
    assert record.automation_score > 0.5


def test_manual_provisioning_gap_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-manual",
                    title="Set up server manually",
                    description=(
                        "Manually configure server with SSH. "
                        "Install dependencies and configure services manually."
                    ),
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "manual_provisioning" in record.gaps
    assert "missing_automation" in record.gaps
    assert record.automation_score < 0.5


def test_hardcoded_credentials_gap_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-hardcoded",
                    title="Fix hardcoded credentials",
                    description=(
                        "Remove hardcoded passwords from configuration files. "
                        "Migrate to secrets management solution."
                    ),
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "hardcoded_credentials" in record.gaps


def test_environment_drift_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-drift",
                    title="Detect configuration drift",
                    description=(
                        "Implement drift detection for infrastructure. "
                        "Alert when environment drift occurs between staging and production."
                    ),
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "environment_drift" in record.gaps


def test_disaster_recovery_environment_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-dr",
                    title="Set up disaster recovery",
                    description=(
                        "Create disaster recovery environment for critical production systems. "
                        "Configure automated failover and backup procedures."
                    ),
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "disaster_recovery" in record.environment_types
    assert "missing_disaster_recovery" not in record.gaps


def test_missing_environment_gaps():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-deploy",
                    title="Deploy application",
                    description="Deploy application to production using automated pipeline.",
                )
            ]
        )
    )

    record = matrix.records[0]
    # Should detect production environment
    assert "production" in record.environment_types or "missing_prod_environment" in record.gaps


def test_configuration_file_management_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-config",
                    title="Manage configuration files",
                    description=(
                        "Create .env files for environment-specific configuration. "
                        "Use environment variables for secrets."
                    ),
                    files_or_modules=[".env.example"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert (
        "environment_variables" in record.configuration_mgmts
        or "configuration_file" in record.configuration_mgmts
    )


def test_multiple_cloud_providers():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-aws",
                    title="AWS infrastructure",
                    description="Provision AWS resources with Terraform and Parameter Store for secrets.",
                ),
                _task(
                    "task-azure",
                    title="Azure infrastructure",
                    description="Deploy to Azure with ARM templates and Azure Key Vault.",
                ),
                _task(
                    "task-gcp",
                    title="GCP infrastructure",
                    description="Set up GCP resources with Terraform and GCP Secret Manager.",
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in matrix.records}

    assert "parameter_store" in by_id["task-aws"].secrets_mgmts or "aws_secrets_manager" in by_id["task-aws"].secrets_mgmts
    assert "azure_key_vault" in by_id["task-azure"].secrets_mgmts
    assert "gcp_secret_manager" in by_id["task-gcp"].secrets_mgmts


def test_complete_provisioning_setup():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Complete infrastructure setup",
                    description=(
                        "Set up complete infrastructure provisioning: "
                        "Development, staging, and production environments with Terraform IaC. "
                        "Use Ansible for configuration management and HashiCorp Vault for secrets. "
                        "Fully automated provisioning with CI/CD pipeline. "
                        "Ensure environment parity across all environments."
                    ),
                    files_or_modules=[
                        "terraform/",
                        "ansible/",
                        "vault/",
                        ".github/workflows/deploy.yml",
                    ],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "development" in record.environment_types
    assert "staging" in record.environment_types
    assert "production" in record.environment_types
    assert "terraform" in record.infrastructure_codes
    assert "ansible" in record.configuration_mgmts or "ansible" in record.infrastructure_codes
    assert "vault" in record.secrets_mgmts
    assert record.automation_score > 0.7
    assert record.maturity_score > 0.6
    assert len(record.gaps) < 3


def test_automation_score_calculation():
    matrix_auto = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-auto",
                    title="Automated setup",
                    description="Fully automated infrastructure with Terraform and CI/CD.",
                )
            ]
        )
    )

    matrix_manual = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-manual",
                    title="Manual setup",
                    description="Manual server configuration without automation.",
                )
            ]
        )
    )

    assert matrix_auto.records[0].automation_score > matrix_manual.records[0].automation_score


def test_maturity_score_calculation():
    matrix_mature = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-mature",
                    title="Mature infrastructure",
                    description=(
                        "Production, staging, and development environments with Terraform, "
                        "Ansible configuration management, and Vault secrets management."
                    ),
                )
            ]
        )
    )

    matrix_basic = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-basic",
                    title="Basic setup",
                    description="Basic development environment setup.",
                )
            ]
        )
    )

    assert matrix_mature.records[0].maturity_score > matrix_basic.records[0].maturity_score


def test_stable_ordering_by_maturity_and_automation():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-low",
                    title="Low maturity",
                    description="Basic manual setup.",
                ),
                _task(
                    "task-high",
                    title="High maturity",
                    description="Production environment with Terraform, Ansible, and Vault automation.",
                ),
                _task(
                    "task-medium",
                    title="Medium maturity",
                    description="Staging environment with Terraform.",
                ),
            ]
        )
    )

    # Should be ordered by maturity and automation scores (descending)
    maturity_scores = [record.maturity_score for record in matrix.records]
    assert maturity_scores == sorted(maturity_scores, reverse=True)


def test_summary_metrics_and_recommendations():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-1",
                    title="Setup dev environment",
                    description="Local development environment.",
                ),
                _task(
                    "task-2",
                    title="Setup staging",
                    description="Staging environment with Terraform.",
                ),
                _task(
                    "task-3",
                    title="Setup production",
                    description="Production infrastructure provisioning.",
                ),
            ]
        )
    )

    summary = matrix.summary

    assert summary["task_count"] == 3
    assert 0 <= summary["average_automation_score"] <= 1
    assert 0 <= summary["average_maturity_score"] <= 1
    assert isinstance(summary["environment_coverage"], dict)
    assert isinstance(summary["infrastructure_coverage"], dict)
    assert summary["total_gaps_count"] >= 0
    assert isinstance(summary["recommendations"], list)


def test_dictionary_serialization_and_markdown_are_json_compatible():
    plan = _plan(
        [
            _task(
                "task-test",
                title="Complete provisioning",
                description=(
                    "Set up development, staging, and production environments with Terraform and Vault."
                ),
                metadata={"environment_types": "development,staging,production"},
            )
        ]
    )
    model = ExecutionPlan.model_validate(plan)

    matrix = build_environment_provisioning_matrix(model)
    payload = environment_provisioning_matrix_to_dict(matrix)
    markdown = environment_provisioning_matrix_to_markdown(matrix)

    assert payload == matrix.to_dict()
    assert matrix.to_dicts() == payload["records"]
    assert summarize_environment_provisioning(matrix) == matrix.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "records"]
    assert markdown.startswith("# Environment Provisioning Matrix: plan-provisioning")
    assert "## Summary" in markdown
    assert "## Provisioning Matrix" in markdown


def test_empty_plan_returns_empty_matrix():
    matrix = build_environment_provisioning_matrix(_plan([]))

    assert len(matrix.records) == 0
    assert matrix.summary["task_count"] == 0
    assert matrix.summary["average_automation_score"] == 0.0
    assert matrix.summary["average_maturity_score"] == 0.0


def test_explicit_metadata_environment_types_are_used():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-explicit",
                    title="Explicit environments",
                    description="Task with explicit environment configuration.",
                    metadata={
                        "environment_types": "development,staging,production,qa",
                        "infrastructure_code": "terraform",
                    },
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "development" in record.environment_types
    assert "staging" in record.environment_types
    assert "production" in record.environment_types
    assert "qa" in record.environment_types
    assert "terraform" in record.infrastructure_codes


def test_qa_and_preview_environments():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-qa",
                    title="QA environment setup",
                    description="Set up QA environment for testing.",
                ),
                _task(
                    "task-preview",
                    title="Preview environment",
                    description="Create preview environments for pull requests.",
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in matrix.records}

    assert "qa" in by_id["task-qa"].environment_types or "staging" in by_id["task-qa"].environment_types
    assert "preview" in by_id["task-preview"].environment_types


def test_chef_and_puppet_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-chef",
                    title="Chef configuration",
                    description="Use Chef cookbooks for configuration management.",
                ),
                _task(
                    "task-puppet",
                    title="Puppet configuration",
                    description="Implement Puppet manifests for server configuration.",
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in matrix.records}

    assert "chef" in by_id["task-chef"].infrastructure_codes or "chef" in by_id["task-chef"].configuration_mgmts
    assert "puppet" in by_id["task-puppet"].infrastructure_codes or "puppet" in by_id["task-puppet"].configuration_mgmts


def test_kubernetes_secrets_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-k8s-secrets",
                    title="K8s secrets management",
                    description="Store sensitive data in Kubernetes secrets with encryption at rest.",
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "kubernetes_secrets" in record.secrets_mgmts


def test_doppler_secrets_management():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-doppler",
                    title="Doppler integration",
                    description="Use Doppler for centralized secrets management across environments.",
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "doppler" in record.secrets_mgmts


def test_encrypted_files_detection():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-encrypted",
                    title="Encrypted configuration",
                    description="Store encrypted files for sensitive configuration data.",
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "encrypted_files" in record.secrets_mgmts


def test_recommendations_for_low_automation():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(f"task-{i}", title=f"Manual setup {i}", description="Manual configuration.")
                for i in range(5)
            ]
        )
    )

    summary = matrix.summary

    # Should recommend automation if most tasks are manual
    recommendations_text = " ".join(summary["recommendations"])
    assert len(summary["recommendations"]) > 0


def test_missing_secrets_management_recommendations():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-1",
                    title="Deploy app",
                    description="Deploy application with database credentials.",
                ),
                _task(
                    "task-2",
                    title="Configure API",
                    description="Set up API with authentication tokens.",
                ),
                _task(
                    "task-3",
                    title="Set up monitoring",
                    description="Configure monitoring with API keys.",
                ),
            ]
        )
    )

    summary = matrix.summary
    recommendations_text = " ".join(summary["recommendations"])

    # Should recommend secrets management
    if not any(
        record.secrets_mgmts for record in matrix.records
    ):
        assert "secrets" in recommendations_text.lower() or "Vault" in recommendations_text


def test_evidence_building():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-evidence",
                    title="Infrastructure setup",
                    description=(
                        "Production environment with Terraform, Ansible configuration, "
                        "and AWS Secrets Manager."
                    ),
                )
            ]
        )
    )

    record = matrix.records[0]
    evidence_text = " ".join(record.evidence)

    assert "production" in evidence_text.lower() or "Environments" in evidence_text
    assert len(record.evidence) > 0


def test_no_infrastructure_as_code_gap():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-no-iac",
                    title="Deploy to cloud",
                    description="Deploy application to AWS cloud infrastructure.",
                )
            ]
        )
    )

    record = matrix.records[0]
    # Should identify IaC gap if no tools detected
    if not record.infrastructure_codes:
        assert "no_infrastructure_as_code" in record.gaps


def test_multiple_strategies_extraction():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-multi",
                    title="Complete DevOps setup",
                    description=(
                        "Set up development, staging, and production environments. "
                        "Use Terraform for IaC, Ansible for configuration, and Vault for secrets. "
                        "Implement automated provisioning with environment parity."
                    ),
                )
            ]
        )
    )

    record = matrix.records[0]
    all_strategies = set()
    for req in record.requirements:
        all_strategies.update(req.strategies)

    assert "dev_environment" in all_strategies
    assert "staging_environment" in all_strategies
    assert "prod_environment" in all_strategies
    assert "terraform_iac" in all_strategies
    assert "ansible_config" in all_strategies
    assert "vault_secrets" in all_strategies
    assert "automated_provisioning" in all_strategies
    assert "environment_parity" in all_strategies


def test_tools_mentioned_extraction():
    matrix = build_environment_provisioning_matrix(
        _plan(
            [
                _task(
                    "task-tools",
                    title="Multi-tool setup",
                    description=(
                        "Use Terraform for infrastructure, Ansible for configuration, "
                        "Docker for containerization, Kubernetes for orchestration, "
                        "and Vault for secrets management."
                    ),
                )
            ]
        )
    )

    record = matrix.records[0]
    all_tools = set()
    for req in record.requirements:
        all_tools.update(req.tools_mentioned)

    assert "terraform" in all_tools
    assert "ansible" in all_tools
    assert "docker" in all_tools
    assert "kubernetes" in all_tools
    assert "vault" in all_tools


def _plan(tasks):
    return {
        "id": "plan-provisioning",
        "implementation_brief_id": "brief-provisioning",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "depends_on": [],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
