"""Per-process executors; PostgreSQL remains the source of pending work."""

import asyncio
import logging
from contextlib import asynccontextmanager

import httpx

from generation_service.db.runtime import database_lifespan
from generation_service.services.dispatch import DispatchService, WatchdogService

logger = logging.getLogger(__name__)


async def run_worker(operation, interval: float):
    while True:
        try:
            if await operation():
                continue
        except Exception:
            logger.exception(
                "Background operation failed; persisted work will be retried"
            )
        await asyncio.sleep(interval)


@asynccontextmanager
async def service_lifespan(app):
    async with database_lifespan(app):
        settings = app.state.settings
        dispatch = DispatchService(app.state.transactions, settings)
        watchdog = WatchdogService(app.state.transactions)
        app.state.dispatch = dispatch
        app.state.watchdog = watchdog
        # HTTPX's default transport does not retry HTTP requests or connections.
        async with httpx.AsyncClient() as client:
            app.state.http_client = client
            workers = [
                asyncio.create_task(
                    run_worker(
                        lambda: dispatch.dispatch_next(client), settings.poll_interval
                    )
                ),
                asyncio.create_task(
                    run_worker(watchdog.run_once, settings.poll_interval)
                ),
            ]
            try:
                yield
            finally:
                for worker in workers:
                    worker.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
