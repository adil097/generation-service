from fastapi import FastAPI

from generation_service.api.health import router as health_router
from generation_service.config import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    app = FastAPI(title="Generation service", version="0.1.0")
    app.state.settings = settings if settings is not None else get_settings()
    app.include_router(health_router)
    return app


app = create_app()
