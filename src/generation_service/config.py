"""Validated configuration shared by all worker processes."""

from functools import lru_cache
from typing import Self

from pydantic import (
    AnyHttpUrl,
    Field,
    PositiveFloat,
    PostgresDsn,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        allow_inf_nan=False,
    )

    database_url: PostgresDsn = Field(repr=False)

    @field_validator("database_url")
    @classmethod
    def require_asyncpg(cls, value: PostgresDsn) -> PostgresDsn:
        if value.scheme != "postgresql+asyncpg":
            raise ValueError("DATABASE_URL must use postgresql+asyncpg")
        if len(value.hosts()) != 1 or not value.path or value.path == "/":
            raise ValueError("DATABASE_URL must name one host and a database")
        return value


class Settings(DatabaseSettings):
    alpha_url: AnyHttpUrl = AnyHttpUrl("http://alpha:8000")
    beta_url: AnyHttpUrl = AnyHttpUrl("http://beta:8000")
    alpha_webhook_secret: SecretStr = Field(min_length=1)
    beta_webhook_secret: SecretStr = Field(min_length=1)
    payments_webhook_secret: SecretStr = Field(min_length=1)

    http_timeout: PositiveFloat = 5
    dispatch_lease: PositiveFloat = 15
    provider_timeout: PositiveFloat = 60
    poll_interval: PositiveFloat = 1

    @model_validator(mode="after")
    def validate_deadlines(self) -> Self:
        if not self.http_timeout < self.dispatch_lease < self.provider_timeout:
            raise ValueError(
                "Required: HTTP_TIMEOUT < DISPATCH_LEASE < PROVIDER_TIMEOUT"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
