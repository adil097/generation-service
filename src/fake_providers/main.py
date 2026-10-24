import asyncio
from contextlib import asynccontextmanager
from uuid import UUID

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, HttpUrl, StrictStr, field_validator

from fake_providers.config import ProviderSettings
from fake_providers.service import (
    ProviderService,
    RequestConflict,
    SynchronousFailure,
    run_worker,
)
from generation_service.api.health import router as health_router
from generation_service.db.runtime import database_lifespan
from generation_service.db.transactions import DatabaseUnavailable


class TaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    client_attempt_id: UUID
    model_slug: StrictStr
    prompt: StrictStr
    ref_images: list[StrictStr]
    callback_url: HttpUrl

    @field_validator("client_attempt_id", mode="before")
    @classmethod
    def canonical_uuid(cls, value):
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError("client_attempt_id must be a canonical UUID string")
        return value

    @field_validator("model_slug", "prompt")
    @classmethod
    def nonempty(cls, value):
        if not value.strip():
            raise ValueError("Must not be blank")
        return value

    @field_validator("ref_images")
    @classmethod
    def nonempty_refs(cls, values):
        if any(not value.strip() for value in values):
            raise ValueError("References must not be blank")
        return values


@asynccontextmanager
async def provider_lifespan(app):
    async with database_lifespan(app):
        settings = app.state.settings
        service = ProviderService(app.state.transactions, settings)
        app.state.provider = service
        async with httpx.AsyncClient() as client:
            workers = [
                asyncio.create_task(
                    run_worker(service.generate_one, settings.poll_interval)
                ),
                asyncio.create_task(
                    run_worker(
                        lambda: service.deliver_one(client), settings.poll_interval
                    )
                ),
            ]
            try:
                yield
            finally:
                for worker in workers:
                    worker.cancel()
                await asyncio.gather(*workers, return_exceptions=True)


def create_app(settings: ProviderSettings | None = None) -> FastAPI:
    settings = settings if settings is not None else ProviderSettings()
    app = FastAPI(
        title=f"Fake provider: {settings.provider_name}",
        version="0.1.0",
        lifespan=provider_lifespan,
    )
    app.state.settings = settings
    app.include_router(health_router)

    @app.post("/tasks", status_code=202)
    async def create_task(payload: TaskRequest, request: Request) -> dict[str, str]:
        # Preserve JSON values, including URL spelling, for semantic comparison.
        try:
            result = await app.state.provider.accept(await request.json())
        except RequestConflict as error:
            raise HTTPException(409, str(error)) from error
        except (SynchronousFailure, DatabaseUnavailable) as error:
            raise HTTPException(503, "Provider temporarily unavailable") from error
        if result.response_delay:
            await asyncio.sleep(result.response_delay)
        return {"external_id": result.external_id}

    return app


app = create_app()
