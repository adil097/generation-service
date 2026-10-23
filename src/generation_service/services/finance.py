"""Ledger operations. Mutations join the caller's transaction and never commit it."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from generation_service.db.locking import Transaction, lock_task, lock_user
from generation_service.db.models import LedgerEntry, User
from generation_service.db.transactions import TransactionRunner
from generation_service.services.errors import (
    InsufficientFunds,
    InvalidTaskState,
    LedgerIntegrityError,
    PaymentConflict,
    UserNotFound,
)

BIGINT_MAX = 2**63 - 1
LedgerKind = Literal["payment_credit", "generation_debit", "generation_refund"]


class PaymentEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    payment_id: StrictStr
    user_id: StrictInt = Field(ge=-(2**63), le=BIGINT_MAX)
    tokens: StrictInt = Field(gt=0, le=BIGINT_MAX)
    status: Literal["success", "pending", "failed"]

    @field_validator("payment_id")
    @classmethod
    def nonempty_payment_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("payment_id must not be empty")
        return value


@dataclass(frozen=True)
class LedgerRecord:
    id: UUID
    user_id: int
    kind: LedgerKind
    amount: int
    payment_id: str | None
    task_id: UUID | None
    created_at: datetime


@dataclass(frozen=True)
class LedgerMutation:
    entry: LedgerRecord | None
    applied: bool


@dataclass(frozen=True)
class BalanceSnapshot:
    user_id: int
    balance: int
    entries: tuple[LedgerRecord, ...]


def _record(row) -> LedgerRecord:
    return LedgerRecord(**row)


async def _balance_value(transaction: Transaction, user_id: int) -> int:
    value = int(
        await transaction.scalar(
            select(func.coalesce(func.sum(LedgerEntry.amount), 0)).where(
                LedgerEntry.user_id == user_id
            )
        )
    )
    if value < 0:
        raise LedgerIntegrityError("The ledger already has a negative balance")
    return value


async def ensure_funds(transaction: Transaction, user_id: int, tokens: int) -> None:
    """Check funds in a fresh statement after the caller has locked the user."""
    if await _balance_value(transaction, user_id) < tokens:
        raise InsufficientFunds(user_id)


async def _task_entry(
    transaction: Transaction, task_id: UUID, kind: LedgerKind
) -> LedgerRecord | None:
    row = (
        (
            await transaction.execute(
                select(LedgerEntry.__table__).where(
                    LedgerEntry.task_id == task_id, LedgerEntry.kind == kind
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    return _record(row) if row is not None else None


def _check_task_entry(entry: LedgerRecord, user_id: int, amount: int) -> None:
    if entry.user_id != user_id or entry.amount != amount:
        raise LedgerIntegrityError(
            "Existing task operation has conflicting financial data"
        )


async def check_active_task_finances(
    transaction: Transaction,
    task_id: UUID,
    user_id: int,
    price_tokens: int,
) -> None:
    """Validate the financial basis before applying a result to an active task."""
    debit = await _task_entry(transaction, task_id, "generation_debit")
    if debit is None:
        raise LedgerIntegrityError("Active task has no debit")
    _check_task_entry(debit, user_id, -price_tokens)
    if await _task_entry(transaction, task_id, "generation_refund") is not None:
        raise LedgerIntegrityError("Active task already has a refund")


async def credit_payment(
    transaction: Transaction, event: PaymentEvent
) -> LedgerMutation:
    if event.status != "success":
        return LedgerMutation(entry=None, applied=False)
    await lock_user(transaction, event.user_id)
    row = (
        (
            await transaction.execute(
                insert(LedgerEntry)
                .values(
                    user_id=event.user_id,
                    kind="payment_credit",
                    amount=event.tokens,
                    payment_id=event.payment_id,
                )
                .on_conflict_do_nothing(index_elements=["payment_id"])
                .returning(*LedgerEntry.__table__.columns)
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is not None:
        return LedgerMutation(entry=_record(row), applied=True)
    # A separate READ COMMITTED statement sees a concurrent winner's committed entry.
    existing = _record(
        (
            await transaction.execute(
                select(LedgerEntry.__table__).where(
                    LedgerEntry.payment_id == event.payment_id
                )
            )
        )
        .mappings()
        .one()
    )
    if existing.user_id != event.user_id or existing.amount != event.tokens:
        raise PaymentConflict(event.payment_id)
    return LedgerMutation(entry=existing, applied=False)


async def debit_generation(transaction: Transaction, task_id: UUID) -> LedgerMutation:
    task = await lock_task(transaction, task_id)
    existing = await _task_entry(transaction, task_id, "generation_debit")
    if existing is not None:
        _check_task_entry(existing, task.user_id, -task.price_tokens)
        return LedgerMutation(entry=existing, applied=False)
    if task.status not in {"queued", "processing"}:
        raise InvalidTaskState("A new debit requires an active task")
    # Read the balance in a new statement after waiting for the user's row lock.
    await ensure_funds(transaction, task.user_id, task.price_tokens)
    row = (
        (
            await transaction.execute(
                insert(LedgerEntry)
                .values(
                    user_id=task.user_id,
                    kind="generation_debit",
                    amount=-task.price_tokens,
                    task_id=task_id,
                )
                .on_conflict_do_nothing(index_elements=["task_id", "kind"])
                .returning(*LedgerEntry.__table__.columns)
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is not None:
        return LedgerMutation(entry=_record(row), applied=True)
    existing = await _task_entry(transaction, task_id, "generation_debit")
    if existing is None:
        raise LedgerIntegrityError("Debit conflict has no corresponding entry")
    _check_task_entry(existing, task.user_id, -task.price_tokens)
    return LedgerMutation(entry=existing, applied=False)


async def refund_generation(transaction: Transaction, task_id: UUID) -> LedgerMutation:
    task = await lock_task(transaction, task_id)
    if task.status != "failed":
        raise InvalidTaskState("A refund requires a failed task")
    debit = await _task_entry(transaction, task_id, "generation_debit")
    if debit is None:
        raise LedgerIntegrityError("Cannot refund a task without a debit")
    _check_task_entry(debit, task.user_id, -task.price_tokens)
    row = (
        (
            await transaction.execute(
                insert(LedgerEntry)
                .values(
                    user_id=task.user_id,
                    kind="generation_refund",
                    amount=-debit.amount,
                    task_id=task_id,
                )
                .on_conflict_do_nothing(index_elements=["task_id", "kind"])
                .returning(*LedgerEntry.__table__.columns)
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is not None:
        return LedgerMutation(entry=_record(row), applied=True)
    existing = await _task_entry(transaction, task_id, "generation_refund")
    if existing is None:
        raise LedgerIntegrityError("Refund conflict has no corresponding entry")
    _check_task_entry(existing, task.user_id, -debit.amount)
    return LedgerMutation(entry=existing, applied=False)


async def read_balance(
    transaction: Transaction, user_id: int, history_limit: int = 20
) -> BalanceSnapshot:
    """Use inside a read-only REPEATABLE READ transaction; see FinancialService."""
    if type(history_limit) is not int or not 1 <= history_limit <= 100:
        raise ValueError("history_limit must be between 1 and 100")
    if await transaction.scalar(select(User.id).where(User.id == user_id)) is None:
        raise UserNotFound(user_id)
    balance = await _balance_value(transaction, user_id)
    rows = (
        (
            await transaction.execute(
                select(LedgerEntry.__table__)
                .where(LedgerEntry.user_id == user_id)
                .order_by(LedgerEntry.created_at.desc(), LedgerEntry.id)
                .limit(history_limit)
            )
        )
        .mappings()
        .all()
    )
    return BalanceSnapshot(
        user_id=user_id, balance=balance, entries=tuple(_record(row) for row in rows)
    )


class FinancialService:
    def __init__(self, transactions: TransactionRunner) -> None:
        self.transactions = transactions

    async def credit_payment(self, event: PaymentEvent) -> LedgerMutation:
        return await self.transactions.run(
            lambda session: credit_payment(session, event)
        )

    async def get_balance(
        self, user_id: int, history_limit: int = 20
    ) -> BalanceSnapshot:
        return await self.transactions.run(
            lambda session: read_balance(session, user_id, history_limit),
            read_only=True,
        )
