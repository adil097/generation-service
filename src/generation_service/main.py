from fastapi import FastAPI

from generation_service.api.errors import install_error_handlers
from generation_service.api.health import router as health_router
from generation_service.api.routes import router as api_router
from generation_service.background import service_lifespan
from generation_service.config import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    app = FastAPI(
        title="Generation service", version="0.1.0", lifespan=service_lifespan
    )
    app.state.settings = settings if settings is not None else get_settings()
    app.include_router(health_router)
    app.include_router(api_router)
    install_error_handlers(app)
    return app


app = create_app()
