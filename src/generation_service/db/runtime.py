from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine


@asynccontextmanager
async def database_lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Each Uvicorn worker owns its connection pool; application state lives in PostgreSQL.
    engine = create_async_engine(
        str(app.state.settings.database_url),
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        connect_args={"timeout": 3},
    )
    app.state.engine = engine
    try:
        yield
    finally:
        await engine.dispose()
