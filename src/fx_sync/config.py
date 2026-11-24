"""Runtime configuration: environment variables (prefix ``FX_``), ``.env`` file, CLI overrides."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

DEFAULT_SYMBOLS = ("EUR", "GBP", "JPY", "BRL", "MXN", "CAD", "AUD", "CHF", "CNY", "INR")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FX_", env_file=".env", extra="ignore")

    db_path: Path = Path("fx_rates.db")
    api_url: str = "https://api.frankfurter.dev/v1"
    base: str = Field(default="USD", pattern=r"^[A-Z]{3}$")
    symbols: Annotated[tuple[str, ...], NoDecode] = DEFAULT_SYMBOLS

    chunk_days: int = Field(default=90, ge=1, le=366)
    concurrency: int = Field(default=3, ge=1, le=16)
    lookback_days: int = Field(default=3, ge=0, le=60)
    initial_lookback_days: int = Field(default=30, ge=1)

    http_timeout: float = Field(default=15.0, gt=0)
    max_retries: int = Field(default=4, ge=0, le=10)
    backoff_base: float = Field(default=0.5, gt=0)
    backoff_max: float = Field(default=30.0, gt=0)
    breaker_threshold: int = Field(default=3, ge=1)

    anomaly_pct: float = Field(default=10.0, gt=0)
    webhook_url: str | None = None

    lock_ttl_seconds: int = Field(default=900, ge=30)
    stale_after_hours: float = Field(default=48.0, gt=0)

    log_level: str = "INFO"
    log_format: str = Field(default="text", pattern=r"^(text|json)$")

    @field_validator("symbols", mode="before")
    @classmethod
    def _split_symbols(cls, value: Any) -> Any:
        if isinstance(value, str):
            value = [part for part in value.replace(" ", "").split(",") if part]
        if isinstance(value, list | tuple):
            return tuple(str(s).upper() for s in value)
        return value

    @field_validator("symbols")
    @classmethod
    def _check_symbols(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for symbol in value:
            if len(symbol) != 3 or not symbol.isalpha():
                raise ValueError(f"invalid currency code {symbol!r}")
        return value
