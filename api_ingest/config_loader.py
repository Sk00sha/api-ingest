import json
from typing import Optional

from api_ingest.app_context import AppContext, MetadataConfig, AuthConfig, HistoryLoadConfig


class ConfigLoader:
    def __init__(self, config_path="config/config.json"):
        with open(config_path, "r") as f:
            self.config = json.load(f)

    def get(self, key: str, default=None):
        return self.config.get(key, default)

    def _set_metadata_config(self) -> MetadataConfig:
        return MetadataConfig(self.config.get("metadata").get("kind"), self.config.get("metadata").get("url"))

    def _set_auth_config(self) -> AuthConfig:
        return AuthConfig(self.config.get("auth").get("name"), self.config.get("auth").get("password"))

    def _set_params(self) -> dict[str]:
        return self.config.get("params")

    def _set_history_load_config(self):
        enabled: bool = self.config.get("history-load").get("enabled")
        start_date: str = self.config.get("history-load").get("start-date")
        end_date: str = self.config.get("history-load").get("end-date")
        if start_date is None or end_date is None:
            raise Exception("History load enabled but start and end date not provided")
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
