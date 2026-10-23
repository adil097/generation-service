"""Spawned processes share PostgreSQL, but no Python sessions or state."""

import asyncio
import os

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from generation_service.db.transactions import TransactionRunner
from generation_service.services.finance import FinancialService, PaymentEvent


def wait_for_peers(barrier):
    barrier.wait(timeout=20)


def credit_batch(url, user_id, payment_id):
    async def run():
        engine = create_async_engine(url)
        try:
            service = FinancialService(
                TransactionRunner(async_sessionmaker(engine, expire_on_commit=False))
            )
            event = PaymentEvent(
                payment_id=payment_id, user_id=user_id, tokens=7, status="success"
            )
            results = [await service.credit_payment(event) for _ in range(5)]
            return os.getpid(), sum(result.applied for result in results)
        finally:
            await engine.dispose()

    return asyncio.run(run())
