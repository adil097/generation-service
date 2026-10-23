import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from generation_service.db.locking import lock_task, lock_user
from generation_service.db.models import (
    GenerationModel,
    GenerationTask,
    LedgerEntry,
    User,
)
from generation_service.db.seed import seed_demo
from generation_service.db.transactions import DatabaseUnavailable, TransactionRunner
from generation_service.services import finance
from generation_service.services.errors import (
    InsufficientFunds,
    InvalidTaskState,
    LedgerIntegrityError,
    PaymentConflict,
    TaskNotFound,
    UserNotFound,
)
from generation_service.services.finance import (
    FinancialService,
    PaymentEvent,
    credit_payment,
    debit_generation,
    refund_generation,
)
from tests.db_support import alembic
from tests.finance_worker import credit_batch, wait_for_peers


@pytest.fixture(scope="module")
def finance_urls(database_factory):
    urls = database_factory()
    alembic(urls["migration"], "upgrade", "head")
    return urls


@pytest.fixture
async def context(finance_urls):
    engine = create_async_engine(finance_urls["app"], pool_size=25, max_overflow=0)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    transactions = TransactionRunner(sessions)
    user_id = uuid4().int % (2**62)
    async with engine.begin() as connection:
        await connection.execute(sa.insert(User).values(id=user_id))
        await connection.execute(sa.insert(User).values(id=user_id + 1))
    yield SimpleNamespace(
        engine=engine,
        sessions=sessions,
        transactions=transactions,
        service=FinancialService(transactions),
        user_id=user_id,
        urls=finance_urls,
    )
    await engine.dispose()


def payment(context, *, tokens=10, payment_id=None, user_id=None, status="success"):
    return PaymentEvent(
        payment_id=payment_id or str(uuid4()),
        tokens=tokens,
        user_id=context.user_id if user_id is None else user_id,
        status=status,
    )


async def new_task(
    context, *, status="queued", task_id=None, session=None, user_id=None
):
    async def create(transaction):
        owner = context.user_id if user_id is None else user_id
        await lock_user(transaction, owner)
        model_id = await transaction.scalar(
            sa.select(GenerationModel.id).where(GenerationModel.slug == "image-fast")
        )
        identifier = task_id or uuid4()
        await transaction.execute(
            sa.insert(GenerationTask).values(
                id=identifier,
                user_id=owner,
                model_id=model_id,
                price_tokens=2,
                prompt="test",
                ref_images=[],
                status=status,
                finished_at=datetime.now(timezone.utc)
                if status in {"completed", "failed"}
                else None,
                result_url="https://files.test/result"
                if status == "completed"
                else None,
            )
        )
        return identifier

    return (
        await create(session)
        if session is not None
        else await context.transactions.run(create)
    )


async def fail_and_refund(session, task_id):
    task = await lock_task(session, task_id)
    if task.status in {"queued", "processing"}:
        await session.execute(
            sa.update(GenerationTask)
            .where(GenerationTask.id == task_id)
            .values(status="failed", finished_at=sa.func.clock_timestamp())
        )
    return await refund_generation(session, task_id)


@pytest.mark.parametrize(
    "overrides",
    [
        {"tokens": True},
        {"tokens": 1.5},
        {"tokens": "10"},
        {"tokens": 0},
        {"tokens": -1},
        {"tokens": 2**63},
        {"user_id": True},
        {"payment_id": "  "},
        {"status": "unknown"},
    ],
)
def test_payment_validation(overrides):
    values = {"payment_id": "payment", "user_id": 1, "tokens": 10, "status": "success"}
    with pytest.raises(ValidationError):
        PaymentEvent(**(values | overrides))


async def test_empty_balance_and_missing_objects(context):
    snapshot = await context.service.get_balance(context.user_id)
    assert snapshot.balance == 0 and snapshot.entries == ()
    with pytest.raises(UserNotFound):
        await context.service.get_balance(-1)
    with pytest.raises(UserNotFound):
        await context.service.credit_payment(payment(context, user_id=-1))
    with pytest.raises(TaskNotFound):
        await context.transactions.run(
            lambda session: debit_generation(session, uuid4())
        )


@pytest.mark.parametrize("status", ["pending", "failed"])
async def test_unconfirmed_payments_have_no_effect(context, status):
    result = await context.service.credit_payment(payment(context, status=status))
    assert result.entry is None and not result.applied
    assert (await context.service.get_balance(context.user_id)).balance == 0


async def test_twenty_concurrent_payment_duplicates(context):
    event = payment(context, tokens=17)
    results = await asyncio.gather(
        *(context.service.credit_payment(event) for _ in range(20))
    )
    assert sum(result.applied for result in results) == 1
    assert len({result.entry.id for result in results}) == 1
    snapshot = await context.service.get_balance(context.user_id)
    assert snapshot.balance == 17 and len(snapshot.entries) == 1


async def test_payment_conflicts_do_not_change_balances(context):
    event = payment(context)
    await context.service.credit_payment(event)
    for overrides in [{"tokens": 11}, {"user_id": context.user_id + 1}]:
        conflicting = PaymentEvent(**(event.model_dump() | overrides))
        with pytest.raises(PaymentConflict):
            await context.service.credit_payment(conflicting)
    assert (await context.service.get_balance(context.user_id)).balance == 10
    assert (await context.service.get_balance(context.user_id + 1)).balance == 0


async def test_same_payment_id_racing_for_different_users(context):
    event = payment(context)
    other = PaymentEvent(**(event.model_dump() | {"user_id": context.user_id + 1}))
    results = await asyncio.gather(
        context.service.credit_payment(event),
        context.service.credit_payment(other),
        return_exceptions=True,
    )
    assert sum(isinstance(result, PaymentConflict) for result in results) == 1
    balances = [
        await context.service.get_balance(user)
        for user in (context.user_id, context.user_id + 1)
    ]
    assert sorted(snapshot.balance for snapshot in balances) == [0, 10]
    assert sum(len(snapshot.entries) for snapshot in balances) == 1


async def test_twenty_concurrent_debits_of_one_task(context):
    await context.service.credit_payment(payment(context, tokens=2))
    task_id = await new_task(context)
    results = await asyncio.gather(
        *(
            context.transactions.run(lambda session: debit_generation(session, task_id))
            for _ in range(20)
        )
    )
    assert sum(result.applied for result in results) == 1
    assert len({result.entry.id for result in results}) == 1
    snapshot = await context.service.get_balance(context.user_id)
    assert snapshot.balance == 0 and len(snapshot.entries) == 2


async def test_concurrent_distinct_debits_cannot_overdraw(context):
    await context.service.credit_payment(payment(context, tokens=5))
    tasks = [await new_task(context) for _ in range(10)]
    results = await asyncio.gather(
        *(
            context.transactions.run(
                lambda session, task_id=task_id: debit_generation(session, task_id)
            )
            for task_id in tasks
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(result, InsufficientFunds) for result in results) == 8
    snapshot = await context.service.get_balance(context.user_id)
    assert snapshot.balance == 1
    assert [entry.amount for entry in snapshot.entries].count(-2) == 2


async def test_ten_concurrent_refunds_are_one_full_refund(context):
    await context.service.credit_payment(payment(context))
    task_id = await new_task(context)
    await context.transactions.run(lambda session: debit_generation(session, task_id))
    results = await asyncio.gather(
        *(
            context.transactions.run(lambda session: fail_and_refund(session, task_id))
            for _ in range(10)
        )
    )
    assert sum(result.applied for result in results) == 1
    assert {result.entry.amount for result in results} == {2}
    snapshot = await context.service.get_balance(context.user_id)
    assert snapshot.balance == 10
    assert (
        len([entry for entry in snapshot.entries if entry.kind == "generation_refund"])
        == 1
    )
    # A duplicate original debit after the refund cannot charge the user again.
    assert not (
        await context.transactions.run(
            lambda session: debit_generation(session, task_id)
        )
    ).applied
    assert (await context.service.get_balance(context.user_id)).balance == 10


@pytest.mark.parametrize("status", ["queued", "processing", "completed"])
async def test_refund_requires_failed_task(context, status):
    task_id = await new_task(context, status=status)
    with pytest.raises(InvalidTaskState):
        await context.transactions.run(
            lambda session: refund_generation(session, task_id)
        )


async def test_no_debit_for_terminal_task_and_no_refund_without_debit(context):
    task_id = await new_task(context, status="failed")
    with pytest.raises(InvalidTaskState):
        await context.transactions.run(
            lambda session: debit_generation(session, task_id)
        )
    with pytest.raises(LedgerIntegrityError):
        await context.transactions.run(
            lambda session: refund_generation(session, task_id)
        )


async def test_invalid_existing_refund_rolls_back_task_change(context):
    await context.service.credit_payment(payment(context))
    task_id = await new_task(context)
    await context.transactions.run(lambda session: debit_generation(session, task_id))
    # Simulate malformed preexisting history using a schema-valid, incorrect amount.
    async with context.engine.begin() as connection:
        await connection.execute(
            sa.insert(LedgerEntry).values(
                user_id=context.user_id,
                kind="generation_refund",
                amount=1,
                task_id=task_id,
            )
        )
    with pytest.raises(LedgerIntegrityError):
        await context.transactions.run(
            lambda session: fail_and_refund(session, task_id)
        )
    async with context.engine.connect() as connection:
        assert (
            await connection.scalar(
                sa.select(GenerationTask.status).where(GenerationTask.id == task_id)
            )
            == "queued"
        )
    assert (await context.service.get_balance(context.user_id)).balance == 9


async def test_task_creation_and_debit_roll_back_together(context):
    await context.service.credit_payment(payment(context))
    task_id = uuid4()

    async def operation(session):
        await new_task(context, task_id=task_id, session=session)
        await debit_generation(session, task_id)
        raise ValueError("abort operation")

    with pytest.raises(ValueError, match="abort operation"):
        await context.transactions.run(operation)
    async with context.engine.connect() as connection:
        assert (
            await connection.scalar(
                sa.select(GenerationTask.id).where(GenerationTask.id == task_id)
            )
            is None
        )
        assert (
            await connection.scalar(
                sa.select(LedgerEntry.id).where(LedgerEntry.task_id == task_id)
            )
            is None
        )
    assert (await context.service.get_balance(context.user_id)).balance == 10


async def test_failed_status_and_refund_roll_back_together(context):
    await context.service.credit_payment(payment(context))
    task_id = await new_task(context)
    await context.transactions.run(lambda session: debit_generation(session, task_id))

    async def operation(session):
        await fail_and_refund(session, task_id)
        raise ValueError("abort operation")

    with pytest.raises(ValueError):
        await context.transactions.run(operation)
    assert (await context.service.get_balance(context.user_id)).balance == 8
    async with context.engine.connect() as connection:
        assert (
            await connection.scalar(
                sa.select(GenerationTask.status).where(GenerationTask.id == task_id)
            )
            == "queued"
        )


async def test_payment_rollback_then_redelivery(context):
    event = payment(context)

    async def operation(session):
        await credit_payment(session, event)
        raise ValueError("abort operation")

    with pytest.raises(ValueError):
        await context.transactions.run(operation)
    assert (await context.service.get_balance(context.user_id)).balance == 0
    assert (await context.service.credit_payment(event)).applied
    assert not (await context.service.credit_payment(event)).applied


async def test_balance_and_history_share_snapshot(context, monkeypatch):
    observed = asyncio.Event()
    resume = asyncio.Event()
    original = finance._balance_value

    async def pause_after_sum(transaction, user_id):
        value = await original(transaction, user_id)
        observed.set()
        await resume.wait()
        return value

    monkeypatch.setattr(finance, "_balance_value", pause_after_sum)
    reader = asyncio.create_task(context.service.get_balance(context.user_id))
    await asyncio.wait_for(observed.wait(), timeout=5)
    await context.service.credit_payment(payment(context))
    resume.set()
    snapshot = await asyncio.wait_for(reader, timeout=5)
    assert snapshot.balance == 0 and snapshot.entries == ()
    assert (await context.service.get_balance(context.user_id)).balance == 10


async def test_history_limit_does_not_limit_balance(context):
    for _ in range(3):
        await context.service.credit_payment(payment(context, tokens=2))
    snapshot = await context.service.get_balance(context.user_id, history_limit=1)
    assert snapshot.balance == 6 and len(snapshot.entries) == 1
    for limit in (0, 101, True):
        with pytest.raises(ValueError):
            await context.service.get_balance(context.user_id, history_limit=limit)


async def test_lock_timeout_rolls_back_without_acknowledgement(context):
    async with context.sessions.begin() as blocking:
        await lock_user(blocking, context.user_id)
        service = FinancialService(
            TransactionRunner(context.sessions, lock_timeout_ms=50)
        )
        with pytest.raises(DatabaseUnavailable):
            await service.credit_payment(payment(context))
    assert (await context.service.get_balance(context.user_id)).balance == 0


@pytest.mark.parametrize("code", ["40001", "40P01"])
async def test_known_abort_retries_whole_transaction(context, code):
    event = payment(context)
    calls = 0

    async def operation(session):
        nonlocal calls
        calls += 1
        result = await credit_payment(session, event)
        if calls == 1:
            await session.execute(
                sa.text(f"DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '{code}'; END $$")
            )
        return result

    assert (await context.transactions.run(operation)).applied
    assert calls == 2
    assert (await context.service.get_balance(context.user_id)).balance == 10


async def test_retry_exhaustion_leaves_no_financial_effect(context):
    event = payment(context)
    calls = 0

    async def operation(session):
        nonlocal calls
        calls += 1
        await credit_payment(session, event)
        await session.execute(
            sa.text("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '40001'; END $$")
        )

    with pytest.raises(DatabaseUnavailable):
        await context.transactions.run(operation)
    assert calls == 3
    assert (await context.service.get_balance(context.user_id)).balance == 0


async def test_connection_failure_is_not_automatically_replayed(context):
    calls = 0

    async def operation(session):
        nonlocal calls
        calls += 1
        await credit_payment(session, payment(context))
        raise OperationalError(
            "COMMIT", {}, SimpleNamespace(sqlstate="08006"), connection_invalidated=True
        )

    with pytest.raises(DatabaseUnavailable):
        await context.transactions.run(operation)
    assert calls == 1


async def test_seed_repeat_does_not_restore_spent_tokens(context):
    await context.transactions.run(seed_demo)
    task_id = await new_task(context, user_id=1)
    await context.transactions.run(lambda session: debit_generation(session, task_id))
    await context.transactions.run(seed_demo)
    assert (await context.service.get_balance(1)).balance == 98


async def test_payment_duplicates_across_four_processes(context):
    mp = multiprocessing.get_context("spawn")
    barrier = mp.Barrier(4)
    payment_id = str(uuid4())

    def run_workers():
        with ProcessPoolExecutor(
            max_workers=4,
            mp_context=mp,
            initializer=wait_for_peers,
            initargs=(barrier,),
        ) as pool:
            jobs = [
                pool.submit(
                    credit_batch, context.urls["app"], context.user_id, payment_id
                )
                for _ in range(4)
            ]
            return [job.result(timeout=30) for job in jobs]

    results = await asyncio.to_thread(run_workers)
    assert len({process_id for process_id, applied in results}) == 4
    assert sum(applied for process_id, applied in results) == 1
    snapshot = await context.service.get_balance(context.user_id)
    assert snapshot.balance == 7 and len(snapshot.entries) == 1
