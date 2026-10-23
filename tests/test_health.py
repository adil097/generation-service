import importlib

from httpx import ASGITransport, AsyncClient


async def test_application_import_and_liveness(monkeypatch, settings_values):
    for name, value in settings_values.items():
        monkeypatch.setenv(name.upper(), value)
    main = importlib.import_module("generation_service.main")
    app = main.create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "generation"}
