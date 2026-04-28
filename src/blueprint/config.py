"""Configuration management for Blueprint."""

import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


class Config:
    """Blueprint configuration."""

    def __init__(self, config_path: str | None = None):
        """Initialize config from file or defaults."""
        self.config_path = config_path or self._find_config_file()
        self.data = self._load_config()

    def _find_config_file(self) -> str | None:
        """Find config file in standard locations."""
        # Check current directory
        local_config = Path(".blueprint.yaml")
        if local_config.exists():
            return str(local_config)

        # Check home directory
        home_config = Path.home() / ".blueprint.yaml"
        if home_config.exists():
            return str(home_config)

        return None

    def _load_config(self) -> dict[str, Any]:
        """Load config from file or return defaults."""
        defaults = {
            "database": {
                "path": str(Path.home() / ".blueprint" / "blueprint.db"),
            },
            "sources": {
                "max": {
                    "db_path": str(Path.home() / "Project" / "experiments" / "max" / "max.db"),
                },
                "graph": {
                    "path": str(Path.home() / "Project" / "experiments" / "graph"),
                },
                "github": {
                    "token_env": "GITHUB_TOKEN",
                    "default_owner": None,
                    "default_repo": None,
                },
            },
            "llm": {
                "provider": "anthropic",
                "default_model": "claude-opus-4-6",
            },
            "planning": {
                "rules_files": [],
            },
            "exports": {
                "output_dir": str(Path.home() / "blueprint-exports"),
                "formats": {
                    "relay": "json",
                    "smoothie": "markdown",
                    "codex": "markdown",
                    "claude_code": "markdown",
                    "csv-tasks": "csv",
                    "status-report": "markdown",
                },
                "templates": {
                    "smoothie": {},
                    "codex": {},
                    "claude_code": {},
                },
            },
        }

        if not self.config_path:
            return self._prepare_config(defaults)

        try:
            with open(self.config_path) as f:
                config = yaml.safe_load(f) or {}
                # Merge with defaults
                return self._prepare_config(self._merge_dicts(defaults, config))
        except Exception as e:
            print(f"Warning: Could not load config from {self.config_path}: {e}")
            return self._prepare_config(defaults)

    def _merge_dicts(self, base: dict, override: dict) -> dict:
        """Deep merge two dicts, with override taking precedence."""
        result = deepcopy(base)
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_dicts(result[key], value)
            else:
                result[key] = value
        return result

    def _prepare_config(self, config: dict[str, Any]) -> dict[str, Any]:
        """Apply environment overrides and expand configured path values."""
        self._apply_environment_overrides(config)
        self._expand_config_paths(config)
        return config

    def _apply_environment_overrides(self, config: dict[str, Any]) -> None:
        """Apply supported environment variable overrides to config values."""
        self._set_env_override(config, "BLUEPRINT_DB_PATH", ("database", "path"))
        self._set_env_override(config, "BLUEPRINT_EXPORT_DIR", ("exports", "output_dir"))
        if os.getenv("BLUEPRINT_OBSIDIAN_PATH") is not None:
            self._set_env_override(
                config,
                "BLUEPRINT_OBSIDIAN_PATH",
                ("sources", "obsidian", "path"),
            )

        sources = config.get("sources")
        if not isinstance(sources, dict):
            return

        for source_name, source_config in sources.items():
            if not isinstance(source_config, dict):
                continue

            env_prefix = f"BLUEPRINT_{source_name.upper().replace('-', '_')}"
            if "db_path" in source_config:
                self._set_env_override(
                    config, f"{env_prefix}_DB_PATH", ("sources", source_name, "db_path")
                )
            if "path" in source_config:
                self._set_env_override(
                    config, f"{env_prefix}_PATH", ("sources", source_name, "path")
                )

    def _set_env_override(
        self, config: dict[str, Any], env_name: str, path: tuple[str, ...]
    ) -> None:
        """Set a config value from an environment variable when present."""
        value = os.getenv(env_name)
        if value is None:
            return

        target = config
        for key in path[:-1]:
            next_target = target.setdefault(key, {})
            if not isinstance(next_target, dict):
                next_target = {}
                target[key] = next_target
            target = next_target
        target[path[-1]] = value

    def _expand_config_paths(self, config: dict[str, Any]) -> None:
        """Expand user and environment variable references in configured paths."""
        self._expand_path_value(config, ("database", "path"))
        self._expand_path_value(config, ("exports", "output_dir"))

        sources = config.get("sources")
        if isinstance(sources, dict):
            for source_name, source_config in sources.items():
                if not isinstance(source_config, dict):
                    continue
                self._expand_path_value(config, ("sources", source_name, "db_path"))
                self._expand_path_value(config, ("sources", source_name, "path"))

        templates = self.get_from(config, ("exports", "templates"))
        if isinstance(templates, dict):
            for target_name, template_config in templates.items():
                if isinstance(template_config, str):
                    templates[target_name] = self._expand_path(template_config)
                elif isinstance(template_config, dict):
                    self._expand_path_value(config, ("exports", "templates", target_name, "path"))
                    self._expand_path_value(
                        config, ("exports", "templates", target_name, "task_path")
                    )

        rules_files = self.get_from(config, ("planning", "rules_files"))
        if isinstance(rules_files, list):
            config["planning"]["rules_files"] = [
                self._expand_path(path) if isinstance(path, str) else path for path in rules_files
            ]

    def _expand_path_value(self, config: dict[str, Any], path: tuple[str, ...]) -> None:
        """Expand a string path value at a nested config path when it exists."""
        target = self.get_from(config, path[:-1])
        if not isinstance(target, dict):
            return

        value = target.get(path[-1])
        if isinstance(value, str):
            target[path[-1]] = self._expand_path(value)

    def _expand_path(self, path: str) -> str:
        """Expand ${VAR} and ~ references in a path string."""
        return os.path.expanduser(os.path.expandvars(path))

    def get_from(self, data: dict[str, Any], path: tuple[str, ...]) -> Any:
        """Get a nested value from the provided mapping."""
        value: Any = data
        for key in path:
            if not isinstance(value, dict):
                return None
            value = value.get(key)
            if value is None:
                return None
        return value

    def get(self, path: str, default: Any = None) -> Any:
        """Get config value by dot-separated path."""
        keys = path.split(".")
        value = self.data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        return value

    def diagnostics(self) -> dict[str, Any]:
        """Return merged config values and validation warnings."""
        checks = self.validation_checks()
        warnings = [check["message"] for check in checks if check["status"] == "fail"]

        github_token_env = self.github_token_env
        if not isinstance(github_token_env, str) or not github_token_env:
            github_token_env = "GITHUB_TOKEN"

        return {
            "config_path": self.config_path,
            "values": self.redacted_data(),
            "environment": {
                "ANTHROPIC_API_KEY": {
                    "present": self.has_anthropic_api_key(),
                },
                github_token_env: {
                    "present": self.has_github_token(),
                },
            },
            "warnings": warnings,
            "checks": checks,
            "valid": not warnings,
        }

    def redacted_data(self) -> dict[str, Any]:
        """Return config values with secret-like fields redacted."""
        return self._redact_secrets(self.data)

    def _redact_secrets(self, value: Any, key: str = "") -> Any:
        """Recursively redact secret-like config values."""
        if isinstance(value, dict):
            return {
                item_key: self._redact_secrets(item_value, item_key)
                for item_key, item_value in value.items()
            }
        if isinstance(value, list):
            return [self._redact_secrets(item) for item in value]
        if isinstance(value, str) and self._is_secret_key(key):
            return "[redacted]" if value else value
        return value

    def _is_secret_key(self, key: str) -> bool:
        """Return whether a config key should be treated as sensitive."""
        normalized = key.lower().replace("-", "_")
        if normalized in {"token_env", "token_environment"}:
            return False
        return any(
            marker in normalized for marker in ("api_key", "apikey", "token", "secret", "password")
        )

    def validate(self) -> list[str]:
        """Validate config values without raising for missing optional paths."""
        return [check["message"] for check in self.validation_checks() if check["status"] == "fail"]

    def validation_checks(self) -> list[dict[str, Any]]:
        """Return structured configuration validation checks."""
        checks: list[dict[str, Any]] = []

        db_path = self.get("database.path")
        if not isinstance(db_path, str) or not db_path:
            self._add_check(
                checks, "database.path", False, "database.path must be a non-empty string"
            )
        else:
            db_parent = Path(db_path).expanduser().parent
            if not db_parent.exists():
                self._add_check(
                    checks,
                    "database.path",
                    False,
                    f"Database path parent directory does not exist: {db_parent}",
                    str(db_parent),
                )
            elif not db_parent.is_dir():
                self._add_check(
                    checks,
                    "database.path",
                    False,
                    f"Database path parent is not a directory: {db_parent}",
                    str(db_parent),
                )
            else:
                self._add_check(
                    checks,
                    "database.path",
                    True,
                    f"Database path parent directory exists: {db_parent}",
                    str(db_parent),
                )

        sources = self.get("sources", {})
        if not isinstance(sources, dict):
            self._add_check(checks, "sources", False, "sources must be a mapping")
        else:
            for source_name, source_config in sources.items():
                if source_name == "github":
                    self._validate_github_source(source_config, checks)
                else:
                    self._validate_source(source_name, source_config, checks)

        export_dir = self.get("exports.output_dir")
        if not isinstance(export_dir, str) or not export_dir:
            self._add_check(
                checks,
                "exports.output_dir",
                False,
                "exports.output_dir must be a non-empty string",
            )
        else:
            export_path = Path(export_dir).expanduser()
            if export_path.exists() and not export_path.is_dir():
                self._add_check(
                    checks,
                    "exports.output_dir",
                    False,
                    f"Export path is not a directory: {export_path}",
                    str(export_path),
                )
            elif not export_path.exists():
                self._add_check(
                    checks,
                    "exports.output_dir",
                    False,
                    f"Export directory does not exist: {export_path}",
                    str(export_path),
                )
            elif not os.access(export_path, os.W_OK):
                self._add_check(
                    checks,
                    "exports.output_dir",
                    False,
                    f"Export directory is not writable: {export_path}",
                    str(export_path),
                )
            else:
                self._add_check(
                    checks,
                    "exports.output_dir",
                    True,
                    f"Export directory is writable: {export_path}",
                    str(export_path),
                )

        provider = self.get("llm.provider")
        if not isinstance(provider, str) or not provider:
            self._add_check(
                checks, "llm.provider", False, "llm.provider must be a non-empty string"
            )
        elif provider != "anthropic":
            self._add_check(
                checks,
                "llm.provider",
                False,
                f"Unsupported LLM provider configured: {provider}",
            )
        else:
            self._add_check(checks, "llm.provider", True, f"LLM provider is supported: {provider}")

        default_model = self.get("llm.default_model")
        if not isinstance(default_model, str) or not default_model:
            self._add_check(
                checks,
                "llm.default_model",
                False,
                "llm.default_model must be a non-empty string",
            )
        else:
            self._add_check(
                checks,
                "llm.default_model",
                True,
                f"LLM default model is configured: {default_model}",
            )

        if provider == "anthropic" and not self.has_anthropic_api_key():
            self._add_check(
                checks,
                "environment.ANTHROPIC_API_KEY",
                False,
                "ANTHROPIC_API_KEY environment variable is not set",
            )
        elif provider == "anthropic":
            self._add_check(
                checks,
                "environment.ANTHROPIC_API_KEY",
                True,
                "ANTHROPIC_API_KEY environment variable is set",
            )

        rules_files = self.get("planning.rules_files", [])
        if not isinstance(rules_files, list):
            self._add_check(
                checks,
                "planning.rules_files",
                False,
                "planning.rules_files must be a list of path strings",
            )
        elif not all(isinstance(path, str) and path for path in rules_files):
            self._add_check(
                checks,
                "planning.rules_files",
                False,
                "planning.rules_files entries must be non-empty strings",
            )
        else:
            self._add_check(
                checks,
                "planning.rules_files",
                True,
                f"Planning rules files configured: {len(rules_files)}",
            )

        return checks

    def _add_check(
        self,
        checks: list[dict[str, Any]],
        name: str,
        passed: bool,
        message: str,
        path: str | None = None,
    ) -> None:
        """Append a structured validation check."""
        check: dict[str, Any] = {
            "name": name,
            "status": "pass" if passed else "fail",
            "message": message,
        }
        if path is not None:
            check["path"] = path
        checks.append(check)

    def _validate_source(
        self,
        source_name: str,
        source_config: Any,
        checks: list[dict[str, Any]],
    ) -> None:
        """Validate a configured source path."""
        check_name = f"sources.{source_name}"
        if not isinstance(source_config, dict):
            self._add_check(checks, check_name, False, f"sources.{source_name} must be a mapping")
            return

        path_value = source_config.get("db_path") or source_config.get("path")
        if path_value is None:
            self._add_check(
                checks,
                check_name,
                False,
                f"sources.{source_name} must define db_path or path",
            )
            return

        if not isinstance(path_value, str) or not path_value:
            self._add_check(
                checks,
                check_name,
                False,
                f"sources.{source_name} path must be a non-empty string",
            )
            return

        source_path = Path(path_value).expanduser()
        if not source_path.exists():
            self._add_check(
                checks,
                check_name,
                False,
                f"Configured source '{source_name}' path does not exist: {source_path}",
                str(source_path),
            )
        else:
            self._add_check(
                checks,
                check_name,
                True,
                f"Configured source '{source_name}' path exists: {source_path}",
                str(source_path),
            )

    def _validate_github_source(
        self,
        source_config: Any,
        checks: list[dict[str, Any]],
    ) -> None:
        """Validate GitHub importer configuration."""
        if not isinstance(source_config, dict):
            self._add_check(checks, "sources.github", False, "sources.github must be a mapping")
            return

        token_env = source_config.get("token_env")
        if not isinstance(token_env, str) or not token_env:
            self._add_check(
                checks,
                "sources.github.token_env",
                False,
                "sources.github.token_env must be a non-empty string",
            )
        else:
            self._add_check(
                checks,
                "sources.github.token_env",
                True,
                f"GitHub token environment variable name is configured: {token_env}",
            )

        default_owner = source_config.get("default_owner")
        default_repo = source_config.get("default_repo")
        if (default_owner and not default_repo) or (default_repo and not default_owner):
            self._add_check(
                checks,
                "sources.github.default_repository",
                False,
                "sources.github.default_owner and default_repo must be configured together",
            )
        else:
            self._add_check(
                checks,
                "sources.github.default_repository",
                True,
                "GitHub default repository settings are consistent",
            )
        for key, value in (
            ("default_owner", default_owner),
            ("default_repo", default_repo),
        ):
            if value is not None and not isinstance(value, str):
                self._add_check(
                    checks,
                    f"sources.github.{key}",
                    False,
                    f"sources.github.{key} must be a string when set",
                )

    def has_anthropic_api_key(self) -> bool:
        """Return whether the Anthropic API key is present without exposing it."""
        return bool(os.getenv("ANTHROPIC_API_KEY"))

    def has_github_token(self) -> bool:
        """Return whether the configured GitHub token is present without exposing it."""
        token_env = self.github_token_env
        return isinstance(token_env, str) and bool(os.getenv(token_env))

    @property
    def db_path(self) -> str:
        """Get database path."""
        return self.get("database.path")

    @property
    def max_db_path(self) -> str:
        """Get Max database path."""
        return self.get("sources.max.db_path")

    @property
    def obsidian_vault_path(self) -> str | None:
        """Get Obsidian vault path."""
        return self.get("sources.obsidian.path")

    @property
    def github_token_env(self) -> str:
        """Get GitHub token environment variable name."""
        return self.get("sources.github.token_env")

    @property
    def github_default_owner(self) -> str | None:
        """Get default GitHub repository owner."""
        return self.get("sources.github.default_owner")

    @property
    def github_default_repo(self) -> str | None:
        """Get default GitHub repository name."""
        return self.get("sources.github.default_repo")

    @property
    def llm_provider(self) -> str:
        """Get configured LLM provider."""
        return self.get("llm.provider")

    @property
    def default_model(self) -> str:
        """Get default LLM model."""
        return self.get("llm.default_model")

    @property
    def export_dir(self) -> str:
        """Get export directory."""
        return self.get("exports.output_dir")

    @property
    def planning_rules_files(self) -> list[str]:
        """Get optional repository rules files for plan generation."""
        rules_files = self.get("planning.rules_files", [])
        if not isinstance(rules_files, list):
            return []
        return [path for path in rules_files if isinstance(path, str) and path]

    @property
    def anthropic_api_key(self) -> str:
        """Get Anthropic API key from environment."""
        key = os.getenv("ANTHROPIC_API_KEY")
        if not key:
            raise ValueError(
                "ANTHROPIC_API_KEY environment variable not set. "
                "Please set it to use LLM features."
            )
        return key


# Global config instance
_config: Config | None = None


def get_config(config_path: str | None = None) -> Config:
    """Get or create global config instance."""
    global _config
    if _config is None:
        _config = Config(config_path)
    return _config


def reload_config(config_path: str | None = None) -> Config:
    """Reload config (useful for testing)."""
    global _config
    _config = Config(config_path)
    return _config
