from typing import Literal

from pydantic import Field, SecretStr

from generation_service.config import DatabaseSettings


class ProviderSettings(DatabaseSettings):
    provider_name: Literal["alpha", "beta"]
    provider_webhook_secret: SecretStr = Field(min_length=1)
