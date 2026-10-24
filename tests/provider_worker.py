import asyncio
import os

import httpx

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from fake_providers.config import ProviderSettings
from fake_providers.service import ProviderService
from generation_service.db.transactions import TransactionRunner
from tests.finance_worker import wait_for_peers


def accept_batch(url, payload, barrier):
    wait_for_peers(barrier)

    async def run():
        engine = create_async_engine(url)
        try:
            settings = ProviderSettings(
                _env_file=None,
                database_url=url,
                provider_name="alpha",
                provider_webhook_secret="test-secret",
            )
            service = ProviderService(
                TransactionRunner(async_sessionmaker(engine)), settings
            )
            return [(await service.accept(payload)).external_id for _ in range(5)]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def crash_after_send(url, pipe):
    async def run():
        engine = create_async_engine(url)
        config = ProviderSettings(
            _env_file=None,
            database_url=url,
            provider_name="alpha",
            provider_webhook_secret="test-secret",
        )
        service = ProviderService(TransactionRunner(async_sessionmaker(engine)), config)

        def receive(request):
            pipe.send(request.content)
            return httpx.Response(200)

        async def crash(*args):
            # Abrupt process exit: no finally, disposal or delivery acknowledgement.
            os._exit(0)

        service.acknowledge = crash
        async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
            await service.deliver_one(client)
        os._exit(1)

    asyncio.run(run())
