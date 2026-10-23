import asyncio

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
async def live(request: Request) -> dict[str, str]:
    service = getattr(request.app.state.settings, "provider_name", "generation")
    return {"status": "ok", "service": service}


@router.get("/ready")
async def ready(request: Request) -> dict[str, str]:
    try:
        async with asyncio.timeout(3):
            async with request.app.state.engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
    except (SQLAlchemyError, OSError, TimeoutError):
        raise HTTPException(status_code=503, detail="Database is unavailable") from None
    return await live(request)
