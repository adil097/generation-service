from typing import Literal

from pydantic import Field, SecretStr, model_validator

from generation_service.config import DatabaseSettings


class ProviderSettings(DatabaseSettings):
    provider_name: Literal["alpha", "beta"]
    provider_webhook_secret: SecretStr = Field(min_length=1)
    failure_rate: float = Field(default=0, ge=0, le=1)
    failure_type: Literal["technical", "moderation", "payload"] = "technical"
    sync_mode: Literal["none", "503", "timeout"] = "none"
    response_delay: float = Field(default=10, gt=0)
    delay_min: float = Field(default=2, ge=0)
    delay_max: float = Field(default=20, ge=0)
    duplicate_webhook: bool = False
    concurrent_duplicates: bool = False
    no_webhook: bool = False
    late_completed: bool = False
    provider_timeout: float = Field(default=60, gt=0)
    late_delay: float = Field(default=65, gt=0)
    poll_interval: float = Field(default=1, gt=0)
    webhook_timeout: float = Field(default=5, gt=0)
    delivery_lease: float = Field(default=15, gt=0)
    retry_interval: float = Field(default=5, gt=0, le=60)

    @model_validator(mode="after")
    def validate_intervals(self):
        if self.delay_min > self.delay_max:
            raise ValueError("DELAY_MIN must not exceed DELAY_MAX")
        if self.late_completed and self.late_delay <= self.provider_timeout:
            raise ValueError("LATE_DELAY must exceed PROVIDER_TIMEOUT")
        if self.webhook_timeout >= self.delivery_lease:
            raise ValueError("WEBHOOK_TIMEOUT must be less than DELIVERY_LEASE")
        return self
