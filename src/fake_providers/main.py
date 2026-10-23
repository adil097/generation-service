from fastapi import FastAPI, HTTPException

from fake_providers.config import ProviderSettings
from generation_service.api.health import router as health_router
from generation_service.db.runtime import database_lifespan


def create_app(settings: ProviderSettings | None = None) -> FastAPI:
    settings = settings if settings is not None else ProviderSettings()
    app = FastAPI(
        title=f"Fake provider: {settings.provider_name}",
        version="0.1.0",
        lifespan=database_lifespan,
    )
    app.state.settings = settings
    app.include_router(health_router)

    @app.post("/tasks", status_code=202)
    async def create_task() -> None:
        raise HTTPException(status_code=501, detail="Provider tasks are planned for stage 7")

    return app


app = create_app()
