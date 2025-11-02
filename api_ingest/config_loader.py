import json
from typing import Optional

from api_ingest.app_context import AppContext, MetadataConfig, AuthConfig


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

    def get_app_context(self) -> AppContext:
        return AppContext(self.config.get("url"),
                          self.config.get("headers"),
                          self.config.get("endpoints"),
                          self._set_metadata_config(),
                          self._set_auth_config())
