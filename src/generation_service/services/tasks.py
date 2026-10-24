"""Public task history read from one repeatable-read snapshot."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import select

from generation_service.db.models import (
    GenerationAttempt,
    GenerationModel,
    GenerationTask,
    ModelLeg,
)
from generation_service.db.transactions import TransactionRunner
from generation_service.services.errors import TaskNotFound


class LegSnapshot(BaseModel):
    leg_id: int
    provider_name: str
    priority: int


class AttemptSnapshot(LegSnapshot):
    attempt_id: UUID
    attempt_number: int
    external_id: str | None
    status: Literal["pending", "awaiting_result", "completed", "failed", "timed_out"]
    error_type: Literal["technical", "moderation", "payload"] | None
    error_details: str | None
    created_at: datetime
    sent_at: datetime | None
    deadline_at: datetime | None
    finished_at: datetime | None


class TaskSnapshot(BaseModel):
    task_id: UUID
    user_id: int
    model_slug: str
    price_tokens: int
    prompt: str
    ref_images: list[str]
    status: Literal["queued", "processing", "completed", "failed"]
    result_url: str | None
    created_at: datetime
    finished_at: datetime | None
    current_leg: LegSnapshot | None
    attempts: list[AttemptSnapshot]


async def _read_task(session, task_id: UUID) -> TaskSnapshot:
    task = (
        (
            await session.execute(
                select(
                    GenerationTask.id.label("task_id"),
                    GenerationTask.user_id,
                    GenerationModel.slug.label("model_slug"),
                    GenerationTask.price_tokens,
                    GenerationTask.prompt,
                    GenerationTask.ref_images,
                    GenerationTask.status,
                    GenerationTask.result_url,
                    GenerationTask.created_at,
                    GenerationTask.finished_at,
                )
                .join(GenerationModel, GenerationModel.id == GenerationTask.model_id)
                .where(GenerationTask.id == task_id)
            )
        )
        .mappings()
        .one_or_none()
    )
    if task is None:
        raise TaskNotFound(task_id)
    rows = (
        (
            await session.execute(
                select(
                    GenerationAttempt.id.label("attempt_id"),
                    GenerationAttempt.leg_id,
                    GenerationAttempt.provider_name,
                    ModelLeg.priority,
                    GenerationAttempt.attempt_number,
                    GenerationAttempt.external_id,
                    GenerationAttempt.status,
                    GenerationAttempt.error_type,
                    GenerationAttempt.error_details,
                    GenerationAttempt.created_at,
                    GenerationAttempt.sent_at,
                    GenerationAttempt.deadline_at,
                    GenerationAttempt.finished_at,
                )
                .join(ModelLeg, ModelLeg.id == GenerationAttempt.leg_id)
                .where(GenerationAttempt.task_id == task_id)
                .order_by(GenerationAttempt.attempt_number)
            )
        )
        .mappings()
        .all()
    )
    history = [AttemptSnapshot(**row) for row in rows]
    active = next(
        (item for item in history if item.status in {"pending", "awaiting_result"}),
        None,
    )
    current = (
        None
        if active is None
        else LegSnapshot(
            leg_id=active.leg_id,
            provider_name=active.provider_name,
            priority=active.priority,
        )
    )
    return TaskSnapshot(**task, current_leg=current, attempts=history)


class TaskService:
    def __init__(self, transactions: TransactionRunner):
        self.transactions = transactions

    async def get(self, task_id: UUID) -> TaskSnapshot:
        return await self.transactions.run(
            lambda session: _read_task(session, task_id), read_only=True
        )
