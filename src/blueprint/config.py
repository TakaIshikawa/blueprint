"""Configuration management for Blueprint."""

import os
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
                    "db_path": str(
                        Path.home() / "Project" / "experiments" / "max" / "max.db"
                    ),
                },
            },
            "llm": {
                "provider": "anthropic",
                "default_model": "claude-opus-4-6",
            },
            "exports": {
                "output_dir": str(Path.home() / "blueprint-exports"),
                "formats": {
                    "relay": "json",
                    "smoothie": "markdown",
                    "codex": "markdown",
                    "claude_code": "markdown",
                },
            },
        }

        if not self.config_path:
            return defaults

        try:
            with open(self.config_path) as f:
                config = yaml.safe_load(f) or {}
                # Merge with defaults
                return self._merge_dicts(defaults, config)
        except Exception as e:
            print(f"Warning: Could not load config from {self.config_path}: {e}")
            return defaults

    def _merge_dicts(self, base: dict, override: dict) -> dict:
        """Deep merge two dicts, with override taking precedence."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_dicts(result[key], value)
            else:
                result[key] = value
        return result

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
        warnings = self.validate()

        return {
            "config_path": self.config_path,
            "values": self.data,
            "environment": {
                "ANTHROPIC_API_KEY": {
                    "present": self.has_anthropic_api_key(),
                },
            },
            "warnings": warnings,
            "valid": not warnings,
        }

    def validate(self) -> list[str]:
        """Validate config values without raising for missing optional paths."""
        warnings: list[str] = []

        db_path = self.get("database.path")
        if not isinstance(db_path, str) or not db_path:
            warnings.append("database.path must be a non-empty string")
        else:
            db_parent = Path(db_path).expanduser().parent
            if not db_parent.exists():
                warnings.append(
                    f"Database path parent directory does not exist: {db_parent}"
                )

        sources = self.get("sources", {})
        if not isinstance(sources, dict):
            warnings.append("sources must be a mapping")
        else:
            for source_name, source_config in sources.items():
                self._validate_source(source_name, source_config, warnings)

        export_dir = self.get("exports.output_dir")
        if not isinstance(export_dir, str) or not export_dir:
            warnings.append("exports.output_dir must be a non-empty string")
        else:
            export_path = Path(export_dir).expanduser()
            if export_path.exists() and not export_path.is_dir():
                warnings.append(f"Export path is not a directory: {export_path}")
            elif not export_path.exists():
                warnings.append(f"Export directory does not exist: {export_path}")

        provider = self.get("llm.provider")
        if not isinstance(provider, str) or not provider:
            warnings.append("llm.provider must be a non-empty string")
        elif provider != "anthropic":
            warnings.append(f"Unsupported LLM provider configured: {provider}")

        default_model = self.get("llm.default_model")
        if not isinstance(default_model, str) or not default_model:
            warnings.append("llm.default_model must be a non-empty string")

        if provider == "anthropic" and not self.has_anthropic_api_key():
            warnings.append("ANTHROPIC_API_KEY environment variable is not set")

        return warnings

    def _validate_source(
        self,
        source_name: str,
        source_config: Any,
        warnings: list[str],
    ) -> None:
        """Validate a configured source path."""
        if not isinstance(source_config, dict):
            warnings.append(f"sources.{source_name} must be a mapping")
            return

        path_value = source_config.get("db_path") or source_config.get("path")
        if path_value is None:
            warnings.append(f"sources.{source_name} must define db_path or path")
            return

        if not isinstance(path_value, str) or not path_value:
            warnings.append(f"sources.{source_name} path must be a non-empty string")
            return

        source_path = Path(path_value).expanduser()
        if not source_path.exists():
            warnings.append(
                f"Configured source '{source_name}' path does not exist: {source_path}"
            )

    def has_anthropic_api_key(self) -> bool:
        """Return whether the Anthropic API key is present without exposing it."""
        return bool(os.getenv("ANTHROPIC_API_KEY"))

    @property
    def db_path(self) -> str:
        """Get database path."""
        return self.get("database.path")

    @property
    def max_db_path(self) -> str:
        """Get Max database path."""
        return self.get("sources.max.db_path")

    @property
    def default_model(self) -> str:
        """Get default LLM model."""
        return self.get("llm.default_model")

    @property
    def export_dir(self) -> str:
        """Get export directory."""
        return self.get("exports.output_dir")

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
