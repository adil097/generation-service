import asyncio
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from generation_service.db.locking import lock_task
from generation_service.db.models import (
    GenerationAttempt,
    GenerationModel,
    GenerationTask,
    LedgerEntry,
    ModelLeg,
    User,
)
from generation_service.db.transactions import TransactionRunner
from generation_service.services import attempts as operations
from generation_service.services.attempts import AttemptService, FinishCommand
from generation_service.services.errors import (
    AttemptNotFound,
    InvalidAttemptEvent,
    LedgerIntegrityError,
)
from generation_service.services.finance import FinancialService, PaymentEvent
from generation_service.services.generation import GenerationRequest, GenerationService
from tests.attempt_worker import finish_batch
from tests.db_support import alembic
from tests.finance_worker import wait_for_peers


@pytest.fixture(scope="module")
def attempt_urls(database_factory):
    urls = database_factory()
    alembic(urls["migration"], "upgrade", "head")
    return urls


@pytest.fixture
async def context(attempt_urls):
    engine = create_async_engine(attempt_urls["app"], pool_size=25, max_overflow=0)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    transactions = TransactionRunner(sessions)
    user_id = uuid4().int % (2**62)
    async with engine.begin() as connection:
        await connection.execute(sa.insert(User).values(id=user_id))
    finance = FinancialService(transactions)
    await finance.credit_payment(
        PaymentEvent(
            payment_id=str(uuid4()), user_id=user_id, tokens=100, status="success"
        )
    )
    yield SimpleNamespace(
        engine=engine,
        sessions=sessions,
        transactions=transactions,
        user_id=user_id,
        finance=finance,
        generations=GenerationService(transactions, "http://api:8000/prefix/"),
        service=AttemptService(transactions),
        urls=attempt_urls,
    )
    await engine.dispose()


async def create(context, slug="image-fast", refs=0):
    return await context.generations.create(
        GenerationRequest(
            user_id=context.user_id,
            model_slug=slug,
            prompt="test",
            ref_images=["https://refs.test/image"] * refs,
        )
    )


async def reference(context, attempt_id):
    async with context.engine.connect() as connection:
        row = (
            await connection.execute(
                sa.select(
                    GenerationAttempt.id,
                    GenerationAttempt.task_id,
                    GenerationAttempt.provider_name,
                ).where(GenerationAttempt.id == attempt_id)
            )
        ).one()
    return SimpleNamespace(
        attempt_id=row.id, task_id=row.task_id, provider_name=row.provider_name
    )


async def start(
    context,
    attempt,
    *,
    state="awaiting_result",
    expired=False,
    store_external=True,
    expired_lease=False,
):
    token = uuid4()

    async def operation(session):
        await lock_task(session, attempt.task_id)
        await session.execute(
            sa.select(GenerationAttempt.id)
            .where(GenerationAttempt.id == attempt.attempt_id)
            .with_for_update()
        )
        now = await session.scalar(sa.select(sa.func.clock_timestamp()))
        deadline = now + timedelta(seconds=-1 if expired else 60)
        lease = now + timedelta(seconds=-2 if expired or expired_lease else 15)
        await session.execute(
            sa.update(GenerationAttempt)
            .where(GenerationAttempt.id == attempt.attempt_id)
            .values(
                status=state,
                sent_at=now - timedelta(seconds=120),
                deadline_at=deadline,
                dispatch_token=token if state == "pending" else None,
                dispatch_lease_until=lease if state == "pending" else None,
                external_id=f"external-{attempt.attempt_id}"
                if store_external
                else None,
            )
        )
        await session.execute(
            sa.update(GenerationTask)
            .where(GenerationTask.id == attempt.task_id)
            .values(status="processing")
        )

    await context.transactions.run(operation)
    return token


def webhook(attempt, *, outcome="failed", error_type="technical", **overrides):
    values = {
        "attempt_id": attempt.attempt_id,
        "provider_name": attempt.provider_name,
        "source": "provider_webhook",
        "external_id": f"external-{attempt.attempt_id}",
        "outcome": outcome,
        "error_type": None if outcome == "completed" else error_type,
        "result_url": "https://files.test/result" if outcome == "completed" else None,
    }
    return FinishCommand(**(values | overrides))


def watchdog(attempt):
    return FinishCommand(
        attempt_id=attempt.attempt_id,
        provider_name=attempt.provider_name,
        source="watchdog",
        outcome="timeout",
    )


def dispatch(attempt, token, details="HTTP 503"):
    return FinishCommand(
        attempt_id=attempt.attempt_id,
        provider_name=attempt.provider_name,
        source="dispatch_error",
        outcome="failed",
        error_type="technical",
        dispatch_token=token,
        error_details=details,
    )


async def snapshot(context, task_id):
    async with context.engine.connect() as connection:
        task = dict(
            (
                await connection.execute(
                    sa.select(GenerationTask.__table__).where(
                        GenerationTask.id == task_id
                    )
                )
            )
            .mappings()
            .one()
        )
        attempts = [
            dict(row)
            for row in (
                await connection.execute(
                    sa.select(GenerationAttempt.__table__)
                    .where(GenerationAttempt.task_id == task_id)
                    .order_by(GenerationAttempt.attempt_number)
                )
            )
            .mappings()
            .all()
        ]
        ledger = [
            dict(row)
            for row in (
                await connection.execute(
                    sa.select(LedgerEntry.__table__)
                    .where(LedgerEntry.task_id == task_id)
                    .order_by(LedgerEntry.kind)
                )
            )
            .mappings()
            .all()
        ]
    balance = (await context.finance.get_balance(context.user_id)).balance
    return SimpleNamespace(task=task, attempts=attempts, ledger=ledger, balance=balance)


@pytest.mark.parametrize(
    "overrides",
    [
        {"outcome": "completed"},
        {"error_type": None},
        {"result_url": "https://files.test/result"},
        {"external_id": None},
        {"external_id": ""},
        {"source": "unknown"},
        {"provider_name": "unknown"},
        {"source": "dispatch_error"},
        {"source": "watchdog"},
        {"outcome": "timeout", "error_type": None},
        {"dispatch_token": uuid4()},
        {"attempt_id": "not-a-uuid"},
    ],
)
def test_inconsistent_commands_are_rejected(overrides):
    values = {
        "attempt_id": uuid4(),
        "provider_name": "alpha",
        "source": "provider_webhook",
        "outcome": "failed",
        "error_type": "technical",
        "external_id": "external",
    }
    with pytest.raises(ValidationError):
        FinishCommand(**(values | overrides))


@pytest.mark.parametrize(
    "state,store_external", [("awaiting_result", True), ("pending", False)]
)
async def test_success_and_early_webhook_are_atomic(context, state, store_external):
    task = await create(context)
    await start(context, task, state=state, store_external=store_external)
    result = await context.service.finish_attempt(webhook(task, outcome="completed"))
    data = await snapshot(context, task.task_id)
    assert result.status == "completed" and result.next_attempt_id is None
    assert data.task["status"] == data.attempts[0]["status"] == "completed"
    assert data.task["finished_at"] == data.attempts[0]["finished_at"]
    assert data.task["result_url"] == "https://files.test/result" and data.balance == 98
    assert [row["kind"] for row in data.ledger] == ["generation_debit"]
    assert data.attempts[0]["external_id"] == f"external-{task.attempt_id}"
    assert (
        data.attempts[0]["dispatch_token"] is None
        and data.attempts[0]["dispatch_lease_until"] is None
    )


@pytest.mark.parametrize(
    "source,state,details",
    [
        ("provider_webhook", "awaiting_result", "provider technical"),
        ("dispatch_error", "pending", "HTTP 503"),
        ("dispatch_error", "pending", "HTTP timeout"),
        ("watchdog", "pending", None),
        ("watchdog", "awaiting_result", None),
    ],
)
async def test_technical_sources_create_same_fallback(context, source, state, details):
    first = await create(context)
    token = await start(context, first, state=state, expired=source == "watchdog")
    before = await snapshot(context, first.task_id)
    command = (
        webhook(first, error_details=details)
        if source == "provider_webhook"
        else dispatch(first, token, details)
        if source == "dispatch_error"
        else watchdog(first)
    )
    result = await context.service.finish_attempt(command)
    data = await snapshot(context, first.task_id)
    assert result.status == "next_attempt_created"
    old, new = data.attempts
    assert old["status"] == ("timed_out" if source == "watchdog" else "failed")
    assert old["error_type"] == "technical"
    assert old["error_details"] == (details or "Provider deadline expired")
    assert (
        old["sent_at"] == before.attempts[0]["sent_at"]
        and old["deadline_at"] == before.attempts[0]["deadline_at"]
    )
    assert old["request_payload"] == before.attempts[0]["request_payload"]
    assert old["dispatch_token"] is None and old["dispatch_lease_until"] is None
    assert (
        new["id"] == result.next_attempt_id
        and new["provider_name"] == "beta"
        and new["attempt_number"] == 2
    )
    assert new["status"] == "pending" and new["created_at"] == old["finished_at"]
    for name in (
        "external_id",
        "sent_at",
        "deadline_at",
        "dispatch_token",
        "dispatch_lease_until",
        "finished_at",
        "error_type",
        "error_details",
    ):
        assert new[name] is None
    assert new["request_payload"] == old["request_payload"] | {
        "client_attempt_id": str(new["id"]),
        "callback_url": "http://api:8000/prefix/webhooks/providers/beta",
    }
    assert (
        data.task["status"] == "queued"
        and data.task["finished_at"] is None
        and data.task["result_url"] is None
    )
    assert len(data.ledger) == 1 and data.balance == 98


@pytest.mark.parametrize(
    "slug,refs,error_type,next_provider",
    [
        ("image-fast", 0, "payload", None),
        ("image-fast", 3, "moderation", "beta"),
        ("video-long", 1, "moderation", None),
        ("video-long", 2, "technical", None),
        ("video-long", 1, "technical", "alpha"),
    ],
)
async def test_routing_moderation_payload_and_references(
    context, slug, refs, error_type, next_provider
):
    first = await create(context, slug, refs)
    await start(context, first)
    result = await context.service.finish_attempt(webhook(first, error_type=error_type))
    data = await snapshot(context, first.task_id)
    if next_provider:
        assert (
            result.status == "next_attempt_created"
            and data.attempts[1]["provider_name"] == next_provider
        )
        assert len(data.ledger) == 1 and data.balance == 100 - first.price_tokens
    else:
        assert result.status == "failed_refunded" and len(data.attempts) == 1
        assert data.task["status"] == "failed" and data.task["result_url"] is None
        assert data.task["finished_at"] == data.attempts[0]["finished_at"]
        assert sorted(row["amount"] for row in data.ledger) == [
            -first.price_tokens,
            first.price_tokens,
        ]
        assert data.balance == 100


async def test_first_failure_then_second_success(context):
    first = await create(context)
    await start(context, first)
    result = await context.service.finish_attempt(webhook(first))
    second = await reference(context, result.next_attempt_id)
    await start(context, second)
    assert (
        await context.service.finish_attempt(webhook(second, outcome="completed"))
    ).status == "completed"
    data = await snapshot(context, first.task_id)
    assert [row["status"] for row in data.attempts] == ["failed", "completed"]
    assert (
        data.task["status"] == "completed"
        and len(data.ledger) == 1
        and data.balance == 98
    )


@pytest.mark.parametrize("last,count", [(False, 20), (True, 10)])
async def test_duplicate_failures_have_one_effect(context, last, count):
    first = await create(context, refs=5 if last else 0)
    await start(context, first)
    results = await asyncio.gather(
        *(context.service.finish_attempt(webhook(first)) for _ in range(count))
    )
    statuses = [result.status for result in results]
    assert statuses.count("failed_refunded" if last else "next_attempt_created") == 1
    assert statuses.count("ignored") == count - 1
    data = await snapshot(context, first.task_id)
    assert len(data.attempts) == (1 if last else 2)
    assert len(data.ledger) == (2 if last else 1)
    assert data.balance == (100 if last else 98)


@pytest.mark.parametrize(
    "mode",
    ["not_started", "not_due", "wrong_token", "expired_token", "already_awaiting"],
)
async def test_inapplicable_events_are_ignored(context, mode):
    first = await create(context)
    token = None
    if mode != "not_started":
        token = await start(
            context,
            first,
            state="awaiting_result" if mode == "already_awaiting" else "pending",
            expired_lease=mode == "expired_token",
        )
    command = (
        webhook(first)
        if mode == "not_started"
        else watchdog(first)
        if mode == "not_due"
        else dispatch(first, uuid4() if mode == "wrong_token" else token)
    )
    before = await snapshot(context, first.task_id)
    result = await context.service.finish_attempt(command)
    assert result.status == "ignored" and result.reason
    assert vars(await snapshot(context, first.task_id)) == vars(before)


async def test_mismatched_or_unknown_event_is_an_error(context):
    first = await create(context)
    await start(context, first)
    before = await snapshot(context, first.task_id)
    for command, error in [
        (webhook(first, provider_name="beta"), InvalidAttemptEvent),
        (webhook(first, external_id="wrong"), InvalidAttemptEvent),
        (webhook(first, attempt_id=uuid4()), AttemptNotFound),
    ]:
        with pytest.raises(error):
            await context.service.finish_attempt(command)
    assert vars(await snapshot(context, first.task_id)) == vars(before)


@pytest.mark.parametrize("state", ["pending", "awaiting_result"])
async def test_success_after_deadline_before_watchdog_is_accepted(context, state):
    first = await create(context, refs=5)
    await start(context, first, state=state, expired=True)
    assert (
        await context.service.finish_attempt(webhook(first, outcome="completed"))
    ).status == "completed"
    assert (await context.service.finish_attempt(watchdog(first))).status == "ignored"
    data = await snapshot(context, first.task_id)
    assert data.task["result_url"] and len(data.ledger) == 1


@pytest.mark.parametrize("new_state", ["active", "completed", "failed"])
async def test_late_old_result_never_rewrites_history(context, new_state, caplog):
    first = await create(context)
    await start(context, first, expired=True)
    result = await context.service.finish_attempt(watchdog(first))
    second = await reference(context, result.next_attempt_id)
    if new_state != "active":
        await start(context, second)
        await context.service.finish_attempt(
            webhook(
                second, outcome="completed" if new_state == "completed" else "failed"
            )
        )
    before = await snapshot(context, first.task_id)
    caplog.set_level(logging.INFO, logger=operations.__name__)
    assert (
        await context.service.finish_attempt(
            webhook(first, outcome="completed", result_url="https://files.test/late")
        )
    ).status == "ignored"
    assert (await context.service.finish_attempt(webhook(first))).status == "ignored"
    assert vars(await snapshot(context, first.task_id)) == vars(before)
    assert str(first.attempt_id) in caplog.text and "already_finished" in caplog.text
    assert "https://files.test/late" not in caplog.text


async def test_timeout_refund_then_late_success(context):
    first = await create(context, refs=5)
    await start(context, first, expired=True)
    assert (
        await context.service.finish_attempt(watchdog(first))
    ).status == "failed_refunded"
    before = await snapshot(context, first.task_id)
    assert (
        await context.service.finish_attempt(webhook(first, outcome="completed"))
    ).status == "ignored"
    assert vars(await snapshot(context, first.task_id)) == vars(before)
    assert before.task["result_url"] is None and before.balance == 100


async def test_success_failure_and_watchdog_race_has_one_winner(context):
    first = await create(context, refs=5)
    await start(context, first, expired=True)
    commands = [webhook(first, outcome="completed"), webhook(first), watchdog(first)]
    results = await asyncio.gather(
        *(context.service.finish_attempt(command) for command in commands)
    )
    assert [result.status for result in results].count("ignored") == 2
    data = await snapshot(context, first.task_id)
    assert (data.task["status"], len(data.ledger), data.balance) in [
        ("completed", 1, 98),
        ("failed", 2, 100),
    ]
    assert bool(data.task["result_url"]) == (data.task["status"] == "completed")


@pytest.mark.parametrize("target", ["user", "task", "attempt"])
async def test_watchdog_skip_locked_defers_and_releases_partial_locks(context, target):
    first = await create(context, refs=5)
    await start(context, first, expired=True)
    model, key = {
        "user": (User, context.user_id),
        "task": (GenerationTask, first.task_id),
        "attempt": (GenerationAttempt, first.attempt_id),
    }[target]
    before = await snapshot(context, first.task_id)
    async with context.sessions.begin() as blocking:
        await blocking.execute(
            sa.select(model.id).where(model.id == key).with_for_update()
        )
        result = await asyncio.wait_for(
            context.service.finish_attempt(watchdog(first), skip_locked=True), timeout=3
        )
        assert result.status == "deferred" and result.reason == f"{target}_locked"
        if target != "user":
            async with context.sessions.begin() as probe:
                await probe.execute(
                    sa.select(User.id)
                    .where(User.id == context.user_id)
                    .with_for_update(nowait=True)
                )
    assert vars(await snapshot(context, first.task_id)) == vars(before)
    assert (
        await context.service.finish_attempt(watchdog(first), skip_locked=True)
    ).status == "failed_refunded"


@pytest.mark.parametrize(
    "problem", ["missing_debit", "wrong_amount", "existing_refund"]
)
async def test_financial_corruption_prevents_transition(context, problem):
    first = await create(context)
    await start(context, first)
    if problem == "existing_refund":
        async with context.engine.begin() as connection:
            await connection.execute(
                sa.insert(LedgerEntry).values(
                    user_id=context.user_id,
                    task_id=first.task_id,
                    kind="generation_refund",
                    amount=2,
                )
            )
    else:
        engine = create_async_engine(context.urls["migration"])
        try:
            async with engine.begin() as connection:
                query = (
                    sa.delete(LedgerEntry)
                    if problem == "missing_debit"
                    else sa.update(LedgerEntry).values(amount=-1)
                )
                await connection.execute(
                    query.where(LedgerEntry.task_id == first.task_id)
                )
        finally:
            await engine.dispose()
    before = await snapshot(context, first.task_id)
    with pytest.raises(LedgerIntegrityError):
        await context.service.finish_attempt(webhook(first, outcome="completed"))
    assert vars(await snapshot(context, first.task_id)) == vars(before)


async def test_refund_insert_failure_rolls_back_statuses(context, monkeypatch):
    first = await create(context, refs=5)
    await start(context, first)
    before = await snapshot(context, first.task_id)

    async def invalid_refund(transaction, task_id):
        await transaction.execute(
            sa.insert(LedgerEntry).values(
                user_id=context.user_id,
                task_id=task_id,
                kind="generation_refund",
                amount=0,
            )
        )

    with monkeypatch.context() as patch:
        patch.setattr(operations, "refund_generation", invalid_refund)
        with pytest.raises(IntegrityError):
            await context.service.finish_attempt(webhook(first))
    assert vars(await snapshot(context, first.task_id)) == vars(before)
    assert (
        await context.service.finish_attempt(webhook(first))
    ).status == "failed_refunded"


async def test_next_attempt_failure_rolls_back_prior_attempt(context, monkeypatch):
    first = await create(context)
    await start(context, first)
    before = await snapshot(context, first.task_id)
    original = operations._next_payload
    with monkeypatch.context() as patch:
        patch.setattr(
            operations,
            "_next_payload",
            lambda *args: original(*args) | {"client_attempt_id": str(uuid4())},
        )
        with pytest.raises(IntegrityError):
            await context.service.finish_attempt(webhook(first))
    assert vars(await snapshot(context, first.task_id)) == vars(before)
    assert (
        await context.service.finish_attempt(webhook(first))
    ).status == "next_attempt_created"


async def test_four_processes_create_one_refund(context):
    first = await create(context, refs=5)
    await start(context, first)
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
                pool.submit(
                    finish_batch,
                    context.urls["app"],
                    webhook(first).model_dump(mode="json"),
                )
                for _ in range(4)
            ]
            return [job.result(timeout=30) for job in jobs]

    results = await asyncio.to_thread(run_workers)
    assert len({pid for pid, statuses in results}) == 4
    statuses = [status for pid, values in results for status in values]
    assert statuses.count("failed_refunded") == 1 and statuses.count("ignored") == 19
    data = await snapshot(context, first.task_id)
    assert data.balance == 100 and len(data.ledger) == 2


async def custom_model(context, flags):
    engine = create_async_engine(context.urls["migration"])
    slug = "route-" + uuid4().hex
    try:
        async with engine.begin() as connection:
            model_id = await connection.scalar(
                sa.insert(GenerationModel)
                .values(slug=slug, price_tokens=2)
                .returning(GenerationModel.id)
            )
            for priority, flag in enumerate(flags, 1):
                await connection.execute(
                    sa.insert(ModelLeg).values(
                        model_id=model_id,
                        provider_name="alpha" if priority % 2 else "beta",
                        priority=priority,
                        max_ref_images=4,
                        skip_on_moderation=flag,
                    )
                )
    finally:
        await engine.dispose()
    return slug


async def test_moderation_skips_all_flagged_legs_without_history(context):
    slug = await custom_model(context, [False, True, True, False])
    first = await create(context, slug)
    await start(context, first)
    result = await context.service.finish_attempt(
        webhook(first, error_type="moderation")
    )
    assert result.status == "next_attempt_created"
    data = await snapshot(context, first.task_id)
    assert [row["provider_name"] for row in data.attempts] == ["alpha", "beta"]
    async with context.engine.connect() as connection:
        priority = await connection.scalar(
            sa.select(ModelLeg.priority).where(
                ModelLeg.id == data.attempts[1]["leg_id"]
            )
        )
    assert priority == 4 and data.attempts[1]["attempt_number"] == 2


async def test_latest_actual_failure_controls_moderation_filter(context):
    slug = await custom_model(context, [False, False, True])
    first = await create(context, slug)
    await start(context, first)
    result = await context.service.finish_attempt(
        webhook(first, error_type="moderation")
    )
    second = await reference(context, result.next_attempt_id)
    await start(context, second)
    result = await context.service.finish_attempt(
        webhook(second, error_type="technical")
    )
    assert result.status == "next_attempt_created"
    data = await snapshot(context, first.task_id)
    assert [row["provider_name"] for row in data.attempts] == ["alpha", "beta", "alpha"]
    assert [row["error_type"] for row in data.attempts] == [
        "moderation",
        "technical",
        None,
    ]
    assert data.balance == 98 and len(data.ledger) == 1


@pytest.mark.parametrize("terminal", [False, True])
async def test_external_id_collision_is_rejected_even_for_closed_attempt(
    context, terminal
):
    first = await create(context)
    await start(context, first, state="pending", expired=terminal, store_external=False)
    if terminal:
        await context.service.finish_attempt(watchdog(first))
    other = await create(context)
    await start(context, other)
    before = await snapshot(context, first.task_id)
    with pytest.raises(InvalidAttemptEvent):
        await context.service.finish_attempt(
            webhook(first, external_id=f"external-{other.attempt_id}")
        )
    assert vars(await snapshot(context, first.task_id)) == vars(before)


async def test_terminal_attempt_still_requires_valid_correlation(context):
    first = await create(context)
    await start(context, first)
    await context.service.finish_attempt(webhook(first, outcome="completed"))
    before = await snapshot(context, first.task_id)
    for event in (
        webhook(first, provider_name="beta"),
        webhook(first, external_id="wrong"),
    ):
        with pytest.raises(InvalidAttemptEvent):
            await context.service.finish_attempt(event)
    assert vars(await snapshot(context, first.task_id)) == vars(before)


async def test_skip_locked_is_watchdog_only(context):
    first = await create(context)
    with pytest.raises(ValueError, match="only supported for the watchdog"):
        await context.service.finish_attempt(webhook(first), skip_locked=True)


@pytest.mark.parametrize("outcome", ["completed", "failed"])
async def test_backward_clock_correction_preserves_timestamp_invariants(
    context, outcome
):
    first = await create(context, refs=5)
    engine = create_async_engine(context.urls["migration"])
    try:
        async with engine.begin() as connection:
            future = await connection.scalar(
                sa.select(sa.func.clock_timestamp() + timedelta(hours=1))
            )
            await connection.execute(
                sa.update(GenerationTask)
                .where(GenerationTask.id == first.task_id)
                .values(created_at=future)
            )
            await connection.execute(
                sa.update(GenerationAttempt)
                .where(GenerationAttempt.id == first.attempt_id)
                .values(created_at=future + timedelta(seconds=2))
            )
    finally:
        await engine.dispose()
    await start(context, first)
    result = await context.service.finish_attempt(webhook(first, outcome=outcome))
    data = await snapshot(context, first.task_id)
    assert result.status == (
        "completed" if outcome == "completed" else "failed_refunded"
    )
    assert (
        data.task["finished_at"]
        == data.attempts[0]["finished_at"]
        == future + timedelta(seconds=2)
    )
