"""Environment configuration for wattfrosch-store."""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Literal
from urllib.parse import quote, urlsplit, urlunsplit

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_DATABASE_URL = "postgresql://wattfrosch:wattfrosch@localhost:5432/wattfrosch"

Duration = timedelta

_DURATION_RE = re.compile(r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>[smh])$")

_UNIT_SECONDS = {
    "s": 1,
    "m": 60,
    "h": 3600,
}


class ConfigError(ValueError):
    """Invalid configuration (maps to exit code 1)."""


def _parse_duration(value: str) -> timedelta:
    match = _DURATION_RE.match(value.strip())
    if match is None:
        raise ValueError(
            f"Invalid duration {value!r}: expected a number with unit s, m or h (e.g. 30s, 1m, 2h) "
            "or an ISO 8601 duration (e.g. PT30S)"
        )
    seconds = float(match.group("value")) * _UNIT_SECONDS[match.group("unit")]
    return timedelta(seconds=seconds)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = DEFAULT_DATABASE_URL
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    batch_size: int = Field(default=1000, ge=1)
    migrate_lock_timeout: Duration = timedelta(seconds=30)

    @field_validator("migrate_lock_timeout", mode="before")
    @classmethod
    def _parse_migrate_lock_timeout(cls, value: object) -> object:
        if isinstance(value, str) and _DURATION_RE.match(value.strip()) is not None:
            return _parse_duration(value)
        return value  # ISO 8601 strings are handled by pydantic itself

    @property
    def masked_dsn(self) -> str:
        """DSN with the password replaced by ``***`` (safe for startup logs)."""
        parts = urlsplit(self.database_url)
        if parts.password is None:
            return self.database_url
        host = parts.hostname or ""
        if ":" in host:  # IPv6 literal
            host = f"[{host}]"
        netloc = f"{quote(parts.username or '')}:***@"
        if parts.port is not None:
            netloc += f"{host}:{parts.port}"
        else:
            netloc += host
        return urlunsplit(
            (parts.scheme, netloc, parts.path, parts.query, parts.fragment)
        )


def load_settings() -> Settings:
    """Read the configuration from the environment.

    Raises ``ConfigError`` (not a pydantic exception) so callers only need to
    handle one exception type for configuration problems.
    """
    try:
        return Settings()
    except ValidationError as exc:
        raise ConfigError(str(exc)) from exc
