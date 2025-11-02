import json
from typing import Optional

from api_ingest.app_context import AppContext, MetadataConfig, AuthConfig, HistoryLoadConfig


class ConfigLoader:
    """
    Class to load all config into unified Appcontext class for reuse in all app components
    """

    def __init__(self, config_path="config/config.json"):
        self.config_validator = ConfigValidator()
        with open(config_path, "r") as f:
            self.config = json.load(f)
            print("Validating config...")
            self.config_validator.validate_history_load(self.config)
            print("Validation complete...")

    def get(self, key: str, default=None):
        return self.config.get(key, default)

    def _set_metadata_config(self) -> MetadataConfig:
        return MetadataConfig(self.config.get("metadata").get("kind"), self.config.get("metadata").get("url"))

    def _set_auth_config(self) -> AuthConfig:
        return AuthConfig(self.config.get("auth").get("name"), self.config.get("auth").get("password"))

    def _set_params(self) -> dict[str]:
        return self.config.get("params")

    def _set_history_load_config(self):
        enabled: str = self.config.get("history-load").get("enabled")
        start_date: str = self.config.get("history-load").get("start-date")
        end_date: str = self.config.get("history-load").get("end-date")
        return HistoryLoadConfig(enabled,
                                 start_date,
                                 end_date)

    def get_app_context(self) -> AppContext:
        return AppContext(self.config.get("base-url"),
                          self._set_metadata_config(),
                          self._set_auth_config(),
                          self._set_history_load_config(),
                          self._set_params(),
                          self.config.get("headers"),
                          self.config.get("endpoints"))


class ConfigValidator:
    def validate_history_load(self, cfg: dict):
        section = cfg.get("history-load", {})
        enabled_value: bool = self._to_bool(section.get("enabled"))
        if section.get("enabled") and enabled_value is True:
            required_keys = ["start-date", "end-date"]
            invalid = [key for key in required_keys if not section.get(key)]
            if invalid:
                raise ValueError(
                    f"Invalid configuration: cannot perform history load: start/end date not set."
                )
        elif not section.get("enabled"):
            raise ValueError(
                f"Invalid configuration: Cannot verify history load_settings"
            )

    def _to_bool(self, value):
        """Convert various string representations to boolean."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("true", "1", "yes", "y"):
                return True
            elif v in ("false", "0", "no", "n", ""):
                return False
        raise ValueError(f"Invalid boolean value: {value!r}")
