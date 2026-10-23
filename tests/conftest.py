import pytest


@pytest.fixture
def settings_values() -> dict[str, str]:
    return {
        "database_url": "postgresql+asyncpg://test:test@localhost:5432/test",
        "alpha_webhook_secret": "alpha-test-secret",
        "beta_webhook_secret": "beta-test-secret",
        "payments_webhook_secret": "payments-test-secret",
    }
