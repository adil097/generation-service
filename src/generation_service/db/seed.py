"""Opt-in, idempotent demo balances represented entirely by ledger entries."""

import asyncio
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from generation_service.config import DatabaseSettings
from generation_service.db.locking import lock_user
from generation_service.db.models import LedgerEntry, User

DEMO_USERS = ((1, 100), (2, 50))


async def seed_demo(connection: AsyncConnection) -> None:
    for user_id, tokens in DEMO_USERS:
        await connection.execute(
            insert(User).values(id=user_id).on_conflict_do_nothing()
        )
        await lock_user(connection, user_id)
        payment_id = f"demo:initial:user:{user_id}"
        await connection.execute(
            insert(LedgerEntry)
            .values(
                id=uuid5(NAMESPACE_URL, payment_id),
                user_id=user_id,
                kind="payment_credit",
                amount=tokens,
                payment_id=payment_id,
            )
            .on_conflict_do_nothing()
        )
        entry = (
            await connection.execute(
                select(
                    LedgerEntry.user_id,
                    LedgerEntry.amount,
                    LedgerEntry.kind,
                    LedgerEntry.task_id,
                ).where(LedgerEntry.payment_id == payment_id)
            )
        ).one()
        if tuple(entry) != (user_id, tokens, "payment_credit", None):
            raise ValueError(f"Conflicting demo payment: {payment_id}")


async def main() -> None:
    engine = create_async_engine(str(DatabaseSettings().database_url))
    try:
        async with engine.begin() as connection:
            await seed_demo(connection)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
