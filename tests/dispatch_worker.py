import asyncio
import os

import httpx
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from fake_providers.config import ProviderSettings
from fake_providers.service import ProviderService
from generation_service.config import Settings
from generation_service.db.transactions import TransactionRunner
from generation_service.services.dispatch import DispatchService, WatchdogService
from tests.finance_worker import wait_for_peers


def run_batch(urls, settings_values, barrier, watchdog=False):
    wait_for_peers(barrier)

    async def run():
        engine = create_async_engine(urls["app"])
        provider_engine = create_async_engine(urls["provider"])
        transactions = TransactionRunner(async_sessionmaker(engine))
        config = Settings(_env_file=None, **settings_values)
        dispatch = DispatchService(transactions, config)
        providers = {
            name: ProviderService(
                TransactionRunner(async_sessionmaker(provider_engine)),
                ProviderSettings(
                    _env_file=None,
                    database_url=urls["provider"],
                    provider_name=name,
                    provider_webhook_secret="test-secret",
                ),
            )
            for name in ("alpha", "beta")
        }
        calls = 0

        async def receive(request):
            nonlocal calls
            calls += 1
            import json

            accepted = await providers[request.url.host].accept(
                json.loads(request.content)
            )
            return httpx.Response(202, json={"external_id": accepted.external_id})

        try:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(receive)
            ) as client:
                for _ in range(5):
                    if watchdog:
                        await WatchdogService(transactions).run_once()
                    else:
                        await dispatch.dispatch_next(client)
            return os.getpid(), calls
        finally:
            await engine.dispose()
            await provider_engine.dispose()

    return asyncio.run(run())


def crash_after_accept(urls, settings_values, attempt_id, pipe):
    async def run():
        import json
        from uuid import UUID

        engine = create_async_engine(urls["app"])
        provider_engine = create_async_engine(urls["provider"])
        dispatch = DispatchService(
            TransactionRunner(async_sessionmaker(engine)),
            Settings(_env_file=None, **settings_values),
        )
        provider = ProviderService(
            TransactionRunner(async_sessionmaker(provider_engine)),
            ProviderSettings(
                _env_file=None,
                database_url=urls["provider"],
                provider_name="alpha",
                provider_webhook_secret="test-secret",
            ),
        )

        async def receive(request):
            payload = json.loads(request.content)
            accepted = await provider.accept(payload)
            pipe.send((accepted.external_id, payload))
            return httpx.Response(202, json={"external_id": accepted.external_id})

        async def crash(*args):
            os._exit(0)

        dispatch.save_accepted = crash
        async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
            await dispatch.dispatch(UUID(attempt_id), client)
        os._exit(1)

    asyncio.run(run())
