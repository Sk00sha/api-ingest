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
class AppContext:
    url: str
    headers: dict[str, str] = None
    endpoints: List[str] = field(default_factory=list)
    metadata: Optional[MetadataConfig] = None
    auth: Optional[AuthConfig] = None
