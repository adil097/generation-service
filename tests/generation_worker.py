import asyncio
import os

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from generation_service.db.transactions import TransactionRunner
from generation_service.services.errors import ActiveTaskLimit
from generation_service.services.generation import GenerationRequest, GenerationService


def create_batch(url, user_id):
    async def run():
        engine = create_async_engine(url)
        try:
            service = GenerationService(
                TransactionRunner(async_sessionmaker(engine, expire_on_commit=False))
            )
            results = []
            for _ in range(2):
                try:
                    result = await service.create(
                        GenerationRequest(
                            user_id=user_id,
                            model_slug="image-fast",
                            prompt="process test",
                        )
                    )
                    results.append(str(result.task_id))
                except ActiveTaskLimit:
                    results.append(None)
            return os.getpid(), results
        finally:
            await engine.dispose()

    return asyncio.run(run())
