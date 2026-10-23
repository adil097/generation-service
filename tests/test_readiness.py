import importlib
import os
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import OperationalError

from generation_service.config import Settings


def application(monkeypatch, settings_values):
    for name, value in settings_values.items():
        monkeypatch.setenv(name.upper(), value)
    module = importlib.import_module("generation_service.main")
    return module.create_app(Settings(_env_file=None, **settings_values))


@pytest.mark.parametrize(
    "error", [OperationalError("SELECT 1", {}, Exception()), OSError(), TimeoutError()]
)
async def test_readiness_returns_503_without_leaking_database_error(
    monkeypatch, settings_values, error
):
    app = application(monkeypatch, settings_values)
    engine = MagicMock()
    engine.connect.return_value.__aenter__ = AsyncMock(side_effect=error)
    app.state.engine = engine
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health/ready")
        assert response.status_code == 503
        assert response.json() == {"detail": "Database is unavailable"}
        assert (await client.get("/health/live")).status_code == 200


async def test_readiness_with_real_postgres(monkeypatch, settings_values):
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Set TEST_DATABASE_URL or run the Compose test service")
    settings_values["database_url"] = database_url
    app = application(monkeypatch, settings_values)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/health/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "generation"}
