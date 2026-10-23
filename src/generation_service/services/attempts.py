"""One atomic transition for provider results, dispatch errors and watchdog timeouts."""

import logging
from dataclasses import dataclass
from typing import Literal, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, StrictStr, field_validator, model_validator
from sqlalchemy import func, insert, select, update

from generation_service.db.locking import Transaction, lock_user
from generation_service.db.models import GenerationAttempt, GenerationTask, ModelLeg
from generation_service.db.transactions import TransactionRunner
from generation_service.services.errors import (
    AttemptNotFound,
    InvalidAttemptEvent,
    LedgerIntegrityError,
)
from generation_service.services.finance import (
    check_active_task_finances,
    refund_generation,
)
from generation_service.services.generation import ACTIVE_STATUSES

logger = logging.getLogger(__name__)
ACTIVE_ATTEMPTS = ("pending", "awaiting_result")


class FinishCommand(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    attempt_id: UUID
    source: Literal["provider_webhook", "dispatch_error", "watchdog"]
    outcome: Literal["completed", "failed", "timeout"]
    provider_name: Literal["alpha", "beta"]
    external_id: StrictStr | None = None
    dispatch_token: UUID | None = None
    result_url: StrictStr | None = None
    error_type: Literal["technical", "moderation", "payload"] | None = None
    error_details: StrictStr | None = None

    @field_validator("external_id", "result_url")
    @classmethod
    def nonempty_text(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("Identifiers and result URLs must not be empty")
        return value

    @model_validator(mode="after")
    def consistent_event(self) -> Self:
        if self.outcome == "completed":
            if (
                self.result_url is None
                or self.error_type is not None
                or self.error_details is not None
            ):
                raise ValueError("Completed requires a result URL and no error fields")
        elif self.result_url is not None:
            raise ValueError("Only completed may contain a result URL")
        if self.outcome == "failed" and self.error_type is None:
            raise ValueError("Failed requires an error type")
        if self.outcome == "timeout" and self.error_type is not None:
            raise ValueError("Watchdog timeout is normalized to technical internally")
        if self.source == "provider_webhook":
            if (
                self.external_id is None
                or self.outcome == "timeout"
                or self.dispatch_token is not None
            ):
                raise ValueError(
                    "Provider webhook requires external_id and completed/failed"
                )
        elif self.source == "dispatch_error":
            if (
                self.outcome != "failed"
                or self.error_type != "technical"
                or self.dispatch_token is None
            ):
                raise ValueError(
                    "Dispatch error requires a token and a technical failure"
                )
        elif self.outcome != "timeout" or self.dispatch_token is not None:
            raise ValueError("Watchdog requires timeout and no dispatch token")
        return self


@dataclass(frozen=True)
class FinishResult:
    status: Literal[
        "completed", "next_attempt_created", "failed_refunded", "ignored", "deferred"
    ]
    task_id: UUID
    attempt_id: UUID
    next_attempt_id: UUID | None = None
    reason: str | None = None


def _next_payload(attempt, next_id: UUID, provider_name: str) -> dict:
    previous = attempt["request_payload"]
    callback = previous.get("callback_url")
    suffix = f"/webhooks/providers/{attempt['provider_name']}"
    if not isinstance(callback, str) or not callback.endswith(suffix):
        raise LedgerIntegrityError("Saved provider request has an invalid callback URL")
    return previous | {
        "client_attempt_id": str(next_id),
        "callback_url": callback[: -len(suffix)]
        + f"/webhooks/providers/{provider_name}",
    }


async def _next_leg(transaction: Transaction, task, attempt, error_type: str):
    priority = await transaction.scalar(
        select(ModelLeg.priority).where(ModelLeg.id == attempt["leg_id"])
    )
    used = select(GenerationAttempt.leg_id).where(
        GenerationAttempt.task_id == task["id"]
    )
    query = select(ModelLeg.id, ModelLeg.provider_name).where(
        ModelLeg.model_id == task["model_id"],
        ModelLeg.priority > priority,
        ModelLeg.max_ref_images >= len(task["ref_images"]),
        ModelLeg.id.not_in(used),
    )
    if error_type == "moderation":
        query = query.where(ModelLeg.skip_on_moderation.is_(False))
    return (
        await transaction.execute(query.order_by(ModelLeg.priority).limit(1))
    ).one_or_none()


async def _finish_attempt(
    transaction: Transaction,
    command: FinishCommand,
    skip_locked: bool,
) -> FinishResult:
    owner = (
        await transaction.execute(
            select(GenerationTask.user_id, GenerationTask.id)
            .join(GenerationAttempt, GenerationAttempt.task_id == GenerationTask.id)
            .where(GenerationAttempt.id == command.attempt_id)
        )
    ).one_or_none()
    if owner is None:
        raise AttemptNotFound(command.attempt_id)

    def result(status, *, reason=None, next_attempt_id=None):
        return FinishResult(
            status=status,
            task_id=owner.id,
            attempt_id=command.attempt_id,
            reason=reason,
            next_attempt_id=next_attempt_id,
        )

    if not await lock_user(transaction, owner.user_id, skip_locked=skip_locked):
        return result("deferred", reason="user_locked")
    task = (
        (
            await transaction.execute(
                select(GenerationTask.__table__)
                .where(GenerationTask.id == owner.id)
                .with_for_update(skip_locked=skip_locked)
            )
        )
        .mappings()
        .one_or_none()
    )
    if task is None:
        if skip_locked:
            return result("deferred", reason="task_locked")
        raise AttemptNotFound(command.attempt_id)
    attempt = (
        (
            await transaction.execute(
                select(GenerationAttempt.__table__)
                .where(
                    GenerationAttempt.id == command.attempt_id,
                    GenerationAttempt.task_id == task["id"],
                )
                .with_for_update(skip_locked=skip_locked)
            )
        )
        .mappings()
        .one_or_none()
    )
    if attempt is None:
        if skip_locked:
            return result("deferred", reason="attempt_locked")
        raise AttemptNotFound(command.attempt_id)

    # Validate correlation even for terminal attempts; a mismatched event is not a duplicate.
    if command.provider_name != attempt["provider_name"]:
        raise InvalidAttemptEvent("Provider does not match the attempt")
    if (
        command.external_id is not None
        and attempt["external_id"] is not None
        and command.external_id != attempt["external_id"]
    ):
        raise InvalidAttemptEvent("External identifier does not match the attempt")
    if command.source == "provider_webhook":
        other = await transaction.scalar(
            select(GenerationAttempt.id).where(
                GenerationAttempt.provider_name == command.provider_name,
                GenerationAttempt.external_id == command.external_id,
                GenerationAttempt.id != command.attempt_id,
            )
        )
        if other is not None:
            raise InvalidAttemptEvent("External identifier belongs to another attempt")
    if (
        task["status"] not in ACTIVE_STATUSES
        or attempt["status"] not in ACTIVE_ATTEMPTS
    ):
        return result("ignored", reason="already_finished")
    # The partial unique index guarantees this active attempt is the task's only one.
    observed_at = await transaction.scalar(select(func.clock_timestamp()))
    if command.source == "provider_webhook":
        if attempt["status"] == "pending" and attempt["sent_at"] is None:
            return result("ignored", reason="not_started")
    elif command.source == "dispatch_error":
        if (
            attempt["status"] != "pending"
            or attempt["sent_at"] is None
            or attempt["dispatch_token"] != command.dispatch_token
            or attempt["dispatch_lease_until"] is None
            or attempt["dispatch_lease_until"] <= observed_at
        ):
            return result("ignored", reason="stale_dispatch")
    elif attempt["deadline_at"] is None or attempt["deadline_at"] > observed_at:
        return result("ignored", reason="deadline_not_reached")

    await check_active_task_finances(
        transaction, task["id"], task["user_id"], task["price_tokens"]
    )
    # Wall-clock corrections must not produce a completion preceding creation.
    # Lease/deadline eligibility above uses the actual observed database clock.
    applied_at = max(observed_at, task["created_at"], attempt["created_at"])
    status = "timed_out" if command.outcome == "timeout" else command.outcome
    error_type = "technical" if command.outcome == "timeout" else command.error_type
    changes = {
        "status": status,
        "error_type": error_type,
        "finished_at": applied_at,
        "error_details": command.error_details
        or ("Provider deadline expired" if status == "timed_out" else None),
        "dispatch_token": None,
        "dispatch_lease_until": None,
    }
    if command.source == "provider_webhook":
        changes["external_id"] = command.external_id
    # Execute before inserting the next attempt, to release the unique active-attempt slot.
    await transaction.execute(
        update(GenerationAttempt)
        .where(GenerationAttempt.id == command.attempt_id)
        .values(**changes)
    )
    if command.outcome == "completed":
        await transaction.execute(
            update(GenerationTask)
            .where(GenerationTask.id == task["id"])
            .values(
                status="completed",
                result_url=command.result_url,
                finished_at=applied_at,
            )
        )
        return result("completed")

    leg = (
        None
        if error_type == "payload"
        else await _next_leg(transaction, task, attempt, error_type)
    )
    if leg is not None:
        next_id = uuid4()
        await transaction.execute(
            insert(GenerationAttempt).values(
                id=next_id,
                task_id=task["id"],
                model_id=task["model_id"],
                leg_id=leg.id,
                provider_name=leg.provider_name,
                attempt_number=attempt["attempt_number"] + 1,
                status="pending",
                created_at=applied_at,
                request_payload=_next_payload(attempt, next_id, leg.provider_name),
            )
        )
        await transaction.execute(
            update(GenerationTask)
            .where(GenerationTask.id == task["id"])
            .values(status="queued", result_url=None, finished_at=None)
        )
        return result("next_attempt_created", next_attempt_id=next_id)

    await transaction.execute(
        update(GenerationTask)
        .where(GenerationTask.id == task["id"])
        .values(status="failed", result_url=None, finished_at=applied_at)
    )
    await refund_generation(transaction, task["id"])
    return result("failed_refunded")


class AttemptService:
    def __init__(self, transactions: TransactionRunner) -> None:
        self.transactions = transactions

    async def finish_attempt(
        self, command: FinishCommand, *, skip_locked: bool = False
    ) -> FinishResult:
        if skip_locked and command.source != "watchdog":
            raise ValueError("skip_locked is only supported for the watchdog")
        result = await self.transactions.run(
            lambda session: _finish_attempt(session, command, skip_locked)
        )
        if result.status == "ignored":
            logger.info(
                "Ignored attempt event task_id=%s attempt_id=%s provider=%s external_id=%s reason=%s",
                result.task_id,
                command.attempt_id,
                command.provider_name,
                command.external_id,
                result.reason,
            )
        return result
