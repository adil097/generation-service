from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from generation_service.db.models import GenerationTask, User
from generation_service.services.errors import TaskNotFound, UserNotFound

Transaction = AsyncSession | AsyncConnection


@dataclass(frozen=True)
class LockedTask:
    id: UUID
    user_id: int
    price_tokens: int
    status: str


async def lock_user(transaction: Transaction, user_id: int) -> None:
    if not transaction.in_transaction():
        raise RuntimeError("An outer transaction is required")
    found = await transaction.scalar(
        select(User.id).where(User.id == user_id).with_for_update()
    )
    if found is None:
        raise UserNotFound(user_id)


async def lock_task(transaction: Transaction, task_id: UUID) -> LockedTask:
    if not transaction.in_transaction():
        raise RuntimeError("An outer transaction is required")
    # This first read identifies the immutable owner; no decision uses stale task state.
    user_id = await transaction.scalar(
        select(GenerationTask.user_id).where(GenerationTask.id == task_id)
    )
    if user_id is None:
        raise TaskNotFound(task_id)
    await lock_user(transaction, user_id)
    task = (
        (
            await transaction.execute(
                select(
                    GenerationTask.id,
                    GenerationTask.user_id,
                    GenerationTask.price_tokens,
                    GenerationTask.status,
                )
                .where(GenerationTask.id == task_id)
                .with_for_update()
            )
        )
        .mappings()
        .one_or_none()
    )
    if task is None:
        raise TaskNotFound(task_id)
    return LockedTask(**task)
