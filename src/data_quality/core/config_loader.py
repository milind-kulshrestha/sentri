"""Configuration loader with YAML parsing and validation."""

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml
from pydantic import ValidationError as PydanticValidationError

from data_quality.core.config_schema import DQConfig
from data_quality.core.exceptions import ConfigurationError
from data_quality.utils.logger import get_logger


class ConfigLoader:
    """
    Loads and validates configuration from YAML files or dictionaries.

    Supports environment variable substitution using ${VAR_NAME} syntax.
    """

    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """
        Initialize ConfigLoader.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = Path(config_path) if config_path else None
        self._config_dict: Optional[Dict[str, Any]] = None
        self.logger = get_logger("config_loader")

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "ConfigLoader":
        """
        Create ConfigLoader from a dictionary.

        Args:
            config_dict: Configuration dictionary

        Returns:
            ConfigLoader instance
        """
        loader = cls()
        loader._config_dict = config_dict
        return loader

    def load(self) -> DQConfig:
        """
        Load and validate configuration.

        Returns:
            Validated DQConfig object

        Raises:
            ConfigurationError: If configuration is invalid or cannot be loaded
        """
        if self._config_dict is not None:
            raw_config = self._config_dict
        elif self.config_path is not None:
            raw_config = self._load_yaml()
        else:
            raise ConfigurationError("No configuration source provided")

        # Substitute environment variables
        processed_config = self._substitute_env_vars(raw_config)

        # Validate with Pydantic
        try:
            config = DQConfig(**processed_config)
            self.logger.info(
                f"Configuration loaded successfully: {config.metadata.dq_check_name}"
            )
            return config
        except PydanticValidationError as e:
            error_messages = []
            for error in e.errors():
                loc = " -> ".join(str(x) for x in error["loc"])
                msg = error["msg"]
                error_messages.append(f"  - {loc}: {msg}")

            raise ConfigurationError(
                f"Configuration validation failed:\n" + "\n".join(error_messages),
                context={"errors": e.errors()},
            )

    def _load_yaml(self) -> Dict[str, Any]:
        """
        Load YAML file from disk.

        Returns:
            Raw configuration dictionary

        Raises:
            ConfigurationError: If file cannot be loaded
        """
        if not self.config_path.exists():
            raise ConfigurationError(
                f"Configuration file not found: {self.config_path}",
                context={"path": str(self.config_path)},
            )

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                content = f.read()
                return yaml.safe_load(content)
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"YAML syntax error in configuration file: {e}",
                context={"path": str(self.config_path)},
            )
        except Exception as e:
            raise ConfigurationError(
                f"Failed to read configuration file: {e}",
                context={"path": str(self.config_path)},
            )

    def _substitute_env_vars(self, obj: Any, path: str = "") -> Any:
        """
        Recursively substitute environment variables in configuration.

        Uses ${VAR_NAME} syntax for substitution.

        Args:
            obj: Configuration object (dict, list, or scalar)
            path: Current path in configuration (for error messages)

        Returns:
            Configuration with environment variables substituted

        Raises:
            ConfigurationError: If referenced environment variable is not set
        """
        if isinstance(obj, dict):
            return {
                key: self._substitute_env_vars(value, f"{path}.{key}" if path else key)
                for key, value in obj.items()
            }
        elif isinstance(obj, list):
            return [
                self._substitute_env_vars(item, f"{path}[{i}]")
                for i, item in enumerate(obj)
            ]
        elif isinstance(obj, str):
            return self._substitute_string(obj, path)
        else:
            return obj

    def _substitute_string(self, value: str, path: str) -> str:
        """
        Substitute environment variables in a string.

        Args:
            value: String value potentially containing ${VAR_NAME}
            path: Path in configuration for error messages

        Returns:
            String with environment variables substituted

        Raises:
            ConfigurationError: If environment variable is not set
        """
        # Pattern to match ${VAR_NAME}
        pattern = r"\$\{([^}]+)\}"

        def replacer(match):
            var_name = match.group(1)
            env_value = os.environ.get(var_name)

            if env_value is None:
                raise ConfigurationError(
                    f"Environment variable '{var_name}' is not set",
                    field_path=path,
                    suggestion=f"Set the environment variable: export {var_name}=value",
                )

            return env_value

        return re.sub(pattern, replacer, value)


def load_config(
    config_path: Optional[Union[str, Path]] = None,
    config_dict: Optional[Dict[str, Any]] = None,
) -> DQConfig:
    """
    Convenience function to load configuration.

    Args:
        config_path: Path to YAML configuration file
        config_dict: Configuration dictionary (alternative to file)

    Returns:
        Validated DQConfig object

    Raises:
        ConfigurationError: If configuration is invalid
    """
    if config_dict is not None:
        loader = ConfigLoader.from_dict(config_dict)
    elif config_path is not None:
        loader = ConfigLoader(config_path)
    else:
        raise ConfigurationError("Either config_path or config_dict must be provided")

    return loader.load()
