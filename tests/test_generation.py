import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from generation_service.config import Settings
from generation_service.db.locking import lock_task, lock_user
from generation_service.db.models import (
    GenerationAttempt,
    GenerationTask,
    LedgerEntry,
    User,
)
from generation_service.db.transactions import TransactionRunner
from generation_service.services import generation
from generation_service.services.errors import (
    ActiveTaskLimit,
    InsufficientFunds,
    ModelNotFound,
    UnsupportedReferences,
    UserNotFound,
)
from generation_service.services.finance import (
    FinancialService,
    PaymentEvent,
    refund_generation,
)
from generation_service.services.generation import GenerationRequest, GenerationService
from tests.db_support import alembic
from tests.finance_worker import wait_for_peers
from tests.generation_worker import create_batch


@pytest.fixture(scope="module")
def generation_urls(database_factory):
    urls = database_factory()
    alembic(urls["migration"], "upgrade", "head")
    return urls


@pytest.fixture
async def context(generation_urls):
    engine = create_async_engine(generation_urls["app"], pool_size=25, max_overflow=0)
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
        service=GenerationService(transactions),
        finance=FinancialService(transactions),
        user_id=user_id,
        urls=generation_urls,
    )
    await engine.dispose()


def request(context, **overrides):
    return GenerationRequest(
        **(
            {
                "user_id": context.user_id,
                "model_slug": "image-fast",
                "prompt": "Нарисуй море 🌊",
                "ref_images": [],
            }
            | overrides
        )
    )


async def fund(context, tokens=100, user_id=None):
    return await context.finance.credit_payment(
        PaymentEvent(
            payment_id=str(uuid4()),
            user_id=context.user_id if user_id is None else user_id,
            tokens=tokens,
            status="success",
        )
    )


async def counts(context):
    async with context.engine.connect() as connection:
        tasks = await connection.scalar(
            sa.select(sa.func.count())
            .select_from(GenerationTask)
            .where(GenerationTask.user_id == context.user_id)
        )
        attempts = await connection.scalar(
            sa.select(sa.func.count())
            .select_from(GenerationAttempt)
            .join(GenerationTask)
            .where(GenerationTask.user_id == context.user_id)
        )
        debits = await connection.scalar(
            sa.select(sa.func.count())
            .select_from(LedgerEntry)
            .where(
                LedgerEntry.user_id == context.user_id,
                LedgerEntry.kind == "generation_debit",
            )
        )
    return tasks, attempts, debits


@pytest.mark.parametrize(
    "overrides",
    [
        {"user_id": True},
        {"user_id": "1"},
        {"user_id": 2**63},
        {"model_slug": " "},
        {"model_slug": 3},
        {"prompt": ""},
        {"prompt": None},
        {"ref_images": "url"},
        {"ref_images": None},
        {"ref_images": {}},
        {"ref_images": [123]},
        {"ref_images": [""]},
        {"price_tokens": 0},
    ],
)
def test_generation_request_validation(overrides):
    with pytest.raises(ValidationError):
        GenerationRequest(
            **({"user_id": 1, "model_slug": "image-fast", "prompt": "test"} | overrides)
        )


@pytest.mark.parametrize(
    "url",
    [
        "ftp://callback.test",
        "http://api:8000?token=x",
        "https://callback.test/#fragment",
    ],
)
def test_invalid_callback_configuration(settings_values, url):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **settings_values, callback_base_url=url)


@pytest.mark.parametrize(
    "slug,ref_count,provider,price",
    [
        ("image-fast", 0, "alpha", 2),
        ("image-fast", 4, "alpha", 2),
        ("image-fast", 5, "beta", 2),
        ("image-fast", 10, "beta", 2),
        ("video-long", 0, "beta", 25),
        ("video-long", 2, "beta", 25),
    ],
)
async def test_task_debit_and_first_attempt_are_persisted(
    context, slug, ref_count, provider, price
):
    await fund(context)
    command = request(
        context,
        model_slug=slug,
        ref_images=[f"https://refs.test/{i}.png" for i in range(ref_count)],
    )
    result = await context.service.create(command)
    assert result.provider_name == provider and result.price_tokens == price
    async with context.engine.connect() as connection:
        task = (
            (
                await connection.execute(
                    sa.select(GenerationTask.__table__).where(
                        GenerationTask.id == result.task_id
                    )
                )
            )
            .mappings()
            .one()
        )
        attempt = (
            (
                await connection.execute(
                    sa.select(GenerationAttempt.__table__).where(
                        GenerationAttempt.id == result.attempt_id
                    )
                )
            )
            .mappings()
            .one()
        )
        debit = (
            (
                await connection.execute(
                    sa.select(LedgerEntry.__table__).where(
                        LedgerEntry.task_id == result.task_id
                    )
                )
            )
            .mappings()
            .one()
        )
    assert task["status"] == "queued" and task["price_tokens"] == price
    assert task["prompt"] == command.prompt and task["ref_images"] == list(
        command.ref_images
    )
    assert task["result_url"] is None and task["finished_at"] is None
    assert attempt["status"] == "pending" and attempt["attempt_number"] == 1
    assert (
        attempt["model_id"] == task["model_id"] and attempt["leg_id"] == result.leg_id
    )
    assert attempt["task_id"] == task["id"]
    for column in (
        "external_id",
        "sent_at",
        "deadline_at",
        "dispatch_token",
        "dispatch_lease_until",
        "finished_at",
    ):
        assert attempt[column] is None
    assert attempt["request_payload"] == {
        "client_attempt_id": str(result.attempt_id),
        "model_slug": slug,
        "prompt": command.prompt,
        "ref_images": list(command.ref_images),
        "callback_url": f"http://api:8000/webhooks/providers/{provider}",
    }
    assert debit["amount"] == -price and debit["kind"] == "generation_debit"
    assert debit["user_id"] == context.user_id
    assert await counts(context) == (1, 1, 1)
    assert (await context.finance.get_balance(context.user_id)).balance == 100 - price


async def test_callback_prefix_and_reference_snapshot(context):
    await fund(context)
    refs = ["https://refs.test/first.png"]
    command = request(context, ref_images=refs)
    refs.append("https://refs.test/second.png")
    service = GenerationService(
        context.transactions, "https://callback.test/service/v1/"
    )
    result = await service.create(command)
    async with context.engine.connect() as connection:
        payload = await connection.scalar(
            sa.select(GenerationAttempt.request_payload).where(
                GenerationAttempt.id == result.attempt_id
            )
        )
    assert payload["ref_images"] == ["https://refs.test/first.png"]
    assert (
        payload["callback_url"]
        == "https://callback.test/service/v1/webhooks/providers/alpha"
    )


@pytest.mark.parametrize(
    "slug,refs,error",
    [
        ("unknown", 0, ModelNotFound),
        ("image-fast", 11, UnsupportedReferences),
        ("video-long", 3, UnsupportedReferences),
    ],
)
async def test_catalog_rejections_do_not_persist_or_charge(context, slug, refs, error):
    # No balance: catalog validation must take precedence over insufficient funds.
    with pytest.raises(error):
        await context.service.create(
            request(
                context, model_slug=slug, ref_images=["https://refs.test/image"] * refs
            )
        )
    assert await counts(context) == (0, 0, 0)
    assert (await context.finance.get_balance(context.user_id)).balance == 0


async def test_unknown_user_does_not_create_records(context):
    with pytest.raises(UserNotFound):
        await context.service.create(request(context, user_id=-1))
    assert await counts(context) == (0, 0, 0)


async def test_insufficient_funds_does_not_create_records(context):
    await fund(context, tokens=1)
    with pytest.raises(InsufficientFunds):
        await context.service.create(request(context))
    assert await counts(context) == (0, 0, 0)
    assert (await context.finance.get_balance(context.user_id)).balance == 1


async def test_queued_and_processing_tasks_count_toward_limit(context):
    await fund(context)
    tasks = [await context.service.create(request(context)) for _ in range(3)]

    async def start(session):
        for task in tasks[:2]:
            await lock_task(session, task.task_id)
            await session.execute(
                sa.update(GenerationTask)
                .where(GenerationTask.id == task.task_id)
                .values(status="processing")
            )

    await context.transactions.run(start)
    with pytest.raises(ActiveTaskLimit):
        await context.service.create(request(context))
    assert await counts(context) == (3, 3, 3)
    assert (await context.finance.get_balance(context.user_id)).balance == 94


async def test_balance_error_precedes_active_task_limit(context):
    await fund(context, tokens=6)
    for _ in range(3):
        await context.service.create(request(context))
    with pytest.raises(InsufficientFunds):
        await context.service.create(request(context))
    assert await counts(context) == (3, 3, 3)


@pytest.mark.parametrize("status", ["completed", "failed"])
async def test_terminal_task_frees_active_slot(context, status):
    await fund(context)
    tasks = [await context.service.create(request(context)) for _ in range(3)]

    async def finish(session):
        first = tasks[0]
        await lock_task(session, first.task_id)
        await session.execute(
            sa.select(GenerationAttempt.id)
            .where(GenerationAttempt.id == first.attempt_id)
            .with_for_update()
        )
        await session.execute(
            sa.update(GenerationAttempt)
            .where(GenerationAttempt.id == first.attempt_id)
            .values(
                status=status,
                error_type="technical" if status == "failed" else None,
                finished_at=sa.func.greatest(
                    sa.func.clock_timestamp(), GenerationAttempt.created_at
                ),
            )
        )
        await session.execute(
            sa.update(GenerationTask)
            .where(GenerationTask.id == first.task_id)
            .values(
                status=status,
                result_url="https://files.test/result"
                if status == "completed"
                else None,
                finished_at=sa.func.greatest(
                    sa.func.clock_timestamp(), GenerationTask.created_at
                ),
            )
        )
        if status == "failed":
            await refund_generation(session, first.task_id)

    await context.transactions.run(finish)
    await context.service.create(request(context))
    assert await counts(context) == (4, 4, 4)
    assert (await context.finance.get_balance(context.user_id)).balance == (
        92 if status == "completed" else 94
    )


@pytest.mark.parametrize(
    "tokens,successes,error", [(100, 3, ActiveTaskLimit), (5, 2, InsufficientFunds)]
)
async def test_concurrent_creation_respects_balance_and_limit(
    context, tokens, successes, error
):
    await fund(context, tokens=tokens)
    command = request(context)
    results = await asyncio.gather(
        *(context.service.create(command) for _ in range(20)), return_exceptions=True
    )
    assert sum(isinstance(result, error) for result in results) == 20 - successes
    created = [result for result in results if not isinstance(result, Exception)]
    assert len({result.task_id for result in created}) == successes
    assert len({result.attempt_id for result in created}) == successes
    assert await counts(context) == (successes, successes, successes)
    assert (
        await context.finance.get_balance(context.user_id)
    ).balance == tokens - 2 * successes


async def test_identical_requests_are_distinct_generations(context):
    await fund(context)
    command = request(context)
    first, second = (
        await context.service.create(command),
        await context.service.create(command),
    )
    assert first.task_id != second.task_id and first.attempt_id != second.attempt_id
    assert await counts(context) == (2, 2, 2)


async def test_users_are_not_serialized_globally(context):
    await fund(context, user_id=context.user_id + 1)
    async with context.sessions.begin() as blocked:
        await lock_user(blocked, context.user_id)
        result = await asyncio.wait_for(
            context.service.create(request(context, user_id=context.user_id + 1)),
            timeout=3,
        )
    assert result.task_id


async def test_attempt_insert_failure_rolls_back_task_and_debit(context, monkeypatch):
    await fund(context)
    original = generation.build_attempt_payload

    def invalid_payload(**values):
        return original(**values) | {"client_attempt_id": str(uuid4())}

    monkeypatch.setattr(generation, "build_attempt_payload", invalid_payload)
    with pytest.raises(IntegrityError) as failure:
        await context.service.create(request(context))
    assert failure.value.orig.sqlstate == "23514"
    assert await counts(context) == (0, 0, 0)
    assert (await context.finance.get_balance(context.user_id)).balance == 100


async def test_known_transaction_abort_retries_entire_creation(context, monkeypatch):
    await fund(context)
    original = generation._create_generation
    attempts = []

    async def abort_once(transaction, command, callback_base_url):
        result = await original(transaction, command, callback_base_url)
        attempts.append(result)
        if len(attempts) == 1:
            await transaction.execute(
                sa.text("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '40001'; END $$")
            )
        return result

    monkeypatch.setattr(generation, "_create_generation", abort_once)
    result = await context.service.create(request(context))
    assert len(attempts) == 2 and result == attempts[1]
    async with context.engine.connect() as connection:
        assert (
            await connection.scalar(
                sa.select(GenerationTask.id).where(
                    GenerationTask.id == attempts[0].task_id
                )
            )
            is None
        )
    assert await counts(context) == (1, 1, 1)
    assert (await context.finance.get_balance(context.user_id)).balance == 98


async def test_four_processes_share_one_active_task_limit(context):
    await fund(context)
    mp = multiprocessing.get_context("spawn")
    barrier = mp.Barrier(4)

    def run_workers():
        with ProcessPoolExecutor(
            max_workers=4,
            mp_context=mp,
            initializer=wait_for_peers,
            initargs=(barrier,),
        ) as pool:
            jobs = [
                pool.submit(create_batch, context.urls["app"], context.user_id)
                for _ in range(4)
            ]
            return [job.result(timeout=30) for job in jobs]

    results = await asyncio.to_thread(run_workers)
    assert len({pid for pid, tasks in results}) == 4
    assert (
        len([task for pid, tasks in results for task in tasks if task is not None]) == 3
    )
    assert await counts(context) == (3, 3, 3)
    assert (await context.finance.get_balance(context.user_id)).balance == 94
