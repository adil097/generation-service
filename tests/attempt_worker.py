import asyncio
import os

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from generation_service.db.transactions import TransactionRunner
from generation_service.services.attempts import AttemptService, FinishCommand


def finish_batch(url, values):
    async def run():
        engine = create_async_engine(url)
        try:
            service = AttemptService(
                TransactionRunner(async_sessionmaker(engine, expire_on_commit=False))
            )
            command = FinishCommand.model_validate(values)
            results = [await service.finish_attempt(command) for _ in range(5)]
            return os.getpid(), [result.status for result in results]
        finally:
            await engine.dispose()

    return asyncio.run(run())
