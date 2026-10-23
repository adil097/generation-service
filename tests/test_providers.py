import importlib

import pytest
from httpx import ASGITransport, AsyncClient

from fake_providers.config import ProviderSettings


@pytest.mark.parametrize("name", ["alpha", "beta"])
async def test_provider_scaffold(monkeypatch, settings_values, name):
    monkeypatch.setenv("DATABASE_URL", settings_values["database_url"])
    monkeypatch.setenv("PROVIDER_NAME", name)
    monkeypatch.setenv("PROVIDER_WEBHOOK_SECRET", "provider-test-secret")
    module = importlib.import_module("fake_providers.main")
    app = module.create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health/live")
        assert response.json() == {"status": "ok", "service": name}
        response = await client.post("/tasks", json={})
        assert response.status_code == 501


def test_unknown_provider_is_rejected(settings_values):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ProviderSettings(
            _env_file=None,
            database_url=settings_values["database_url"],
            provider_name="unknown",
            provider_webhook_secret="test-secret",
        )
