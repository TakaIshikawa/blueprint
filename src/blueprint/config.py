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
