from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class MetadataConfig:
    kind: Optional[str] = None
    url: [str] = None


@dataclass
class AuthConfig:
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class HistoryLoadConfig:
    enabled: bool
    start_date: Optional[str] = None
    end_date: Optional[str] = None


@dataclass
class AppContext:
    url: str
    metadata: MetadataConfig
    auth: AuthConfig
    history_load: HistoryLoadConfig
    params: Optional[dict] = None
    headers: Optional[dict[str, str]] = None
    endpoints: List[str] = field(default_factory=list)
