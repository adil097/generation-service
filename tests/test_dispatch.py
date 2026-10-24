import asyncio
import importlib
import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from fake_providers.config import ProviderSettings
from fake_providers.models import ProviderTask
from fake_providers.service import ProviderService
from generation_service.config import Settings
from generation_service.db.models import (
    GenerationAttempt,
    GenerationTask,
    LedgerEntry,
    User,
)
from generation_service.db.transactions import DatabaseUnavailable, TransactionRunner
from generation_service.services import dispatch as operations
from generation_service.services.attempts import FinishCommand
from generation_service.services.dispatch import DispatchService, WatchdogService
from generation_service.services.errors import InvalidAttemptEvent
from generation_service.services.finance import FinancialService, PaymentEvent
from generation_service.services.generation import GenerationRequest, GenerationService
from tests.db_support import alembic
from tests.dispatch_worker import (
    crash_after_accept,
    crash_at_dispatch_commit,
    run_batch,
)


@pytest.fixture(scope="module")
def dispatch_urls(database_factory):
    urls = database_factory()
    alembic(urls["migration"], "upgrade", "head")
    return urls


@pytest.fixture
async def context(dispatch_urls, settings_values):
    engine = create_async_engine(dispatch_urls["app"], pool_size=25, max_overflow=0)
    admin = create_async_engine(dispatch_urls["migration"])
    provider_engine = create_async_engine(dispatch_urls["provider"])
    transactions = TransactionRunner(async_sessionmaker(engine, expire_on_commit=False))
    config = Settings(
        _env_file=None,
        **(
            settings_values
            | {
                "database_url": dispatch_urls["app"],
                "provider_timeout": 600,
            }
        ),
    )
    provider = ProviderService(
        TransactionRunner(async_sessionmaker(provider_engine)),
        ProviderSettings(
            _env_file=None,
            database_url=dispatch_urls["provider"],
            provider_name="alpha",
            provider_webhook_secret="test-secret",
        ),
    )
    ctx = SimpleNamespace(
        engine=engine,
        admin=admin,
        transactions=transactions,
        settings=config,
        urls=dispatch_urls,
        service=DispatchService(transactions, config),
        watchdog=WatchdogService(transactions),
        finance=FinancialService(transactions),
        generations=GenerationService(transactions, "http://api:8000/prefix/"),
        provider=provider,
    )
    yield ctx
    async with admin.begin() as connection:
        for model in (
            LedgerEntry,
            GenerationAttempt,
            GenerationTask,
            User,
            ProviderTask,
        ):
            await connection.execute(sa.delete(model))
    await engine.dispose()
    await admin.dispose()
    await provider_engine.dispose()


async def create(context, refs=0):
    user_id = uuid4().int % (2**62)
    async with context.engine.begin() as connection:
        await connection.execute(sa.insert(User).values(id=user_id))
    await context.finance.credit_payment(
        PaymentEvent(
            payment_id=str(uuid4()), user_id=user_id, tokens=100, status="success"
        )
    )
    result = await context.generations.create(
        GenerationRequest(
            user_id=user_id,
            model_slug="image-fast",
            prompt="Море",
            ref_images=["https://refs.test/file"] * refs,
        )
    )
    return SimpleNamespace(
        task_id=result.task_id, attempt_id=result.attempt_id, user_id=user_id
    )


async def attempt(context, attempt_id):
    async with context.engine.connect() as connection:
        return (
            (
                await connection.execute(
                    sa.select(GenerationAttempt.__table__).where(
                        GenerationAttempt.id == attempt_id
                    )
                )
            )
            .mappings()
            .one()
        )


async def history(context, task_id):
    async with context.engine.connect() as connection:
        return (
            (
                await connection.execute(
                    sa.select(GenerationAttempt.__table__)
                    .where(GenerationAttempt.task_id == task_id)
                    .order_by(GenerationAttempt.attempt_number)
                )
            )
            .mappings()
            .all()
        )


async def expire_lease(context, attempt_id):
    async with context.engine.begin() as connection:
        await connection.execute(
            sa.update(GenerationAttempt)
            .where(GenerationAttempt.id == attempt_id)
            .values(dispatch_lease_until=sa.func.now() - sa.text("interval '1 day'"))
        )


async def expire_deadline(context, attempt_id):
    async with context.engine.begin() as connection:
        await connection.execute(
            sa.update(GenerationAttempt)
            .where(GenerationAttempt.id == attempt_id)
            .values(
                sent_at=sa.func.now() - sa.text("interval '2 days'"),
                deadline_at=sa.func.now() - sa.text("interval '1 day'"),
                dispatch_token=None,
                dispatch_lease_until=None,
            )
        )


async def assert_finances(context, item, *, refunded=False):
    balance = await context.finance.get_balance(item.user_id)
    assert balance.balance == (100 if refunded else 98)
    async with context.engine.connect() as connection:
        kinds = (
            (
                await connection.execute(
                    sa.select(LedgerEntry.kind).where(
                        LedgerEntry.task_id == item.task_id
                    )
                )
            )
            .scalars()
            .all()
        )
    assert sorted(kinds) == (
        ["generation_debit", "generation_refund"] if refunded else ["generation_debit"]
    )


async def test_twenty_claims_and_acknowledgements_have_one_owner(context):
    item = await create(context)
    claims = await asyncio.gather(
        *(context.service.claim(item.attempt_id) for _ in range(20))
    )
    claims = [claim for claim in claims if claim is not None]
    assert len(claims) == 1
    saved = await attempt(context, item.attempt_id)
    assert saved["status"] == "pending" and saved["dispatch_token"] == claims[0].token
    assert (saved["deadline_at"] - saved["sent_at"]).total_seconds() == 600
    assert saved["dispatch_lease_until"] <= saved["deadline_at"]
    replies = await asyncio.gather(
        *(context.service.save_accepted(claims[0], "external-one") for _ in range(20))
    )
    assert replies.count(True) == 1
    final = await attempt(context, item.attempt_id)
    assert (
        final["status"] == "awaiting_result" and final["external_id"] == "external-one"
    )
    assert final["dispatch_token"] is None and final["dispatch_lease_until"] is None
    assert final["deadline_at"] == saved["deadline_at"]
    assert await context.service.claim(item.attempt_id) is None
    await assert_finances(context, item)


async def test_recovery_preserves_identity_payload_and_original_deadline(context):
    item = await create(context)
    old = await context.service.claim(item.attempt_id)
    before = await attempt(context, item.attempt_id)
    await expire_lease(context, item.attempt_id)
    assert not await context.service.save_accepted(old, "stale-id")
    new = await context.service.claim_next()
    assert new.token != old.token and new.attempt_id == old.attempt_id
    saved = await attempt(context, item.attempt_id)
    for key in ("sent_at", "deadline_at", "request_payload", "id"):
        assert saved[key] == before[key]
    assert not await context.service.save_accepted(old, "stale-id")
    assert (
        await context.service._technical_failure(old, "stale failure")
    ).status == "ignored"
    assert await context.service.save_accepted(new, "current-id")
    await assert_finances(context, item)


async def test_recovered_lease_is_capped_by_remaining_deadline(context, monkeypatch):
    item = await create(context)
    await context.service.claim(item.attempt_id)
    before = await attempt(context, item.attempt_id)
    now = before["deadline_at"] - timedelta(seconds=3)
    await expire_lease(context, item.attempt_id)
    monkeypatch.setattr(
        operations, "func", SimpleNamespace(clock_timestamp=lambda: sa.literal(now))
    )
    claim = await context.service.claim(item.attempt_id)
    saved = await attempt(context, item.attempt_id)
    assert claim and saved["dispatch_lease_until"] == before["deadline_at"]
    assert saved["sent_at"] == before["sent_at"]


@pytest.mark.parametrize("table", [User, GenerationTask, GenerationAttempt])
async def test_dispatch_skip_locked_and_pagination_do_not_starve_other_work(
    context, monkeypatch, table
):
    first = await create(context)
    second = await create(context)
    # Stable order independent of UUID order and backwards host-clock changes.
    async with context.admin.begin() as connection:
        await connection.execute(
            sa.update(GenerationAttempt)
            .where(GenerationAttempt.id == first.attempt_id)
            .values(created_at=sa.func.now() - sa.text("interval '1 day'"))
        )
    monkeypatch.setattr(operations, "BATCH_SIZE", 1)
    identifier = {
        User: first.user_id,
        GenerationTask: first.task_id,
        GenerationAttempt: first.attempt_id,
    }[table]
    async with context.engine.begin() as blocker:
        await blocker.execute(
            sa.select(table).where(table.id == identifier).with_for_update()
        )
        async with asyncio.timeout(2):
            claim = await context.service.claim_next()
        assert claim.attempt_id == second.attempt_id
        assert (await attempt(context, first.attempt_id))["sent_at"] is None
    assert (await context.service.claim_next()).attempt_id == first.attempt_id


@pytest.mark.parametrize("status", [500, 503, 599])
async def test_observed_http_5xx_moves_to_next_leg_without_resending(context, status):
    item = await create(context)
    calls = []

    def receive(request):
        calls.append(request)
        return httpx.Response(
            status if request.url.host == "alpha" else 202,
            json={"external_id": "beta-success"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert (
            await context.service.dispatch(item.attempt_id, client)
            == "next_attempt_created"
        )
        assert await context.service.dispatch(item.attempt_id, client) == "ignored"
        assert await context.service.dispatch_next(client)
    rows = await history(context, item.task_id)
    assert [row["status"] for row in rows] == ["failed", "awaiting_result"]
    assert rows[0]["error_type"] == "technical" and rows[1]["provider_name"] == "beta"
    assert len(calls) == 2 and [request.url.path for request in calls] == [
        "/tasks",
        "/tasks",
    ]
    assert json.loads(calls[0].content) == rows[0]["request_payload"]
    assert json.loads(calls[1].content) == rows[1]["request_payload"]
    await assert_finances(context, item)


@pytest.mark.parametrize("error", [httpx.ConnectError, httpx.ReadTimeout])
async def test_network_failure_on_last_leg_refunds_once(context, error):
    item = await create(context, refs=5)
    calls = []

    def receive(request):
        calls.append(request)
        raise error("failure", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert (
            await context.service.dispatch(item.attempt_id, client) == "failed_refunded"
        )
        assert not await context.service.dispatch_next(client)
    assert len(calls) == 1
    await assert_finances(context, item, refunded=True)


async def test_overall_http_timeout_covers_streaming_response(context):
    item = await create(context, refs=5)
    config = context.settings.model_copy(update={"http_timeout": 0.05})
    dispatcher = DispatchService(context.transactions, config)

    class SlowBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            for _ in range(20):
                await asyncio.sleep(0.01)
                yield b" "

    calls = []

    def receive(request):
        calls.append(request)
        return httpx.Response(202, stream=SlowBody())

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        async with asyncio.timeout(1):
            assert (
                await dispatcher.dispatch(item.attempt_id, client) == "failed_refunded"
            )
    assert len(calls) == 1
    await assert_finances(context, item, refunded=True)


@pytest.mark.parametrize(
    "status,body",
    [
        (409, b"conflict"),
        (400, b"bad request"),
        (301, b"redirect"),
        (200, b"{}"),
        (202, b"not json"),
        (202, b"[]"),
        (202, b'{"external_id":true}'),
        (202, b'{"external_id":" "}'),
        (202, b" " * 65537),
    ],
)
async def test_protocol_errors_keep_same_pending_attempt_until_watchdog(
    context, status, body
):
    item = await create(context, refs=5)
    calls = []

    def receive(request):
        calls.append(request)
        return httpx.Response(
            status, content=body, headers={"Location": "http://other/"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert (
            await context.service.dispatch(item.attempt_id, client) == "protocol_error"
        )
        assert not await context.service.dispatch_next(client)
    saved = await attempt(context, item.attempt_id)
    assert saved["status"] == "pending" and saved["external_id"] is None
    assert len(calls) == 1
    await assert_finances(context, item)
    await expire_deadline(context, item.attempt_id)
    assert await context.service.claim_next() is None
    assert await context.watchdog.run_once()
    await assert_finances(context, item, refunded=True)


async def test_early_webhook_wins_over_late_202_and_failure(context):
    item = await create(context)

    async def receive(request):
        result = await context.service.attempts.finish_attempt(
            FinishCommand(
                attempt_id=item.attempt_id,
                provider_name="alpha",
                external_id="early-id",
                source="provider_webhook",
                outcome="completed",
                result_url="https://files.test/result",
            )
        )
        assert result.status == "completed"
        return httpx.Response(202, json={"external_id": "early-id"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert await context.service.dispatch(item.attempt_id, client) == "ignored"
    assert (await attempt(context, item.attempt_id))["status"] == "completed"
    assert not await context.watchdog.run_once()
    await assert_finances(context, item)


@pytest.mark.parametrize("committed", [False, True])
async def test_database_error_saving_202_never_becomes_provider_failure(
    context, monkeypatch, committed
):
    item = await create(context)
    original = context.service.save_accepted

    async def unavailable(claim, external_id):
        if committed:
            await original(claim, external_id)
        raise DatabaseUnavailable("Unknown commit outcome")

    monkeypatch.setattr(context.service, "save_accepted", unavailable)

    def receive(request):
        return httpx.Response(202, json={"external_id": "external-one"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        with pytest.raises(DatabaseUnavailable):
            await context.service.dispatch(item.attempt_id, client)
        assert len(await history(context, item.task_id)) == 1
        await assert_finances(context, item)
        saved = await attempt(context, item.attempt_id)
        assert saved["status"] == ("awaiting_result" if committed else "pending")
        monkeypatch.setattr(context.service, "save_accepted", original)
        if not committed:
            await expire_lease(context, item.attempt_id)
        assert await context.service.dispatch_next(client) is (not committed)
    assert (await attempt(context, item.attempt_id))["external_id"] == "external-one"


async def test_conflicting_external_ids_do_not_overwrite_correlation(context):
    first = await create(context)
    second = await create(context)
    claim = await context.service.claim(first.attempt_id)
    assert await context.service.save_accepted(claim, "shared-id")
    other = await context.service.claim(second.attempt_id)
    with pytest.raises(InvalidAttemptEvent):
        await context.service.save_accepted(other, "shared-id")
    async with context.engine.begin() as connection:
        await connection.execute(
            sa.update(GenerationAttempt)
            .where(GenerationAttempt.id == second.attempt_id)
            .values(external_id="original-id")
        )
    with pytest.raises(InvalidAttemptEvent):
        await context.service.save_accepted(other, "different-id")
    assert (await attempt(context, second.attempt_id))["external_id"] == "original-id"


async def test_stale_or_expired_claim_does_not_start_http(context):
    item = await create(context, refs=5)
    claim = await context.service.claim(item.attempt_id)
    await expire_deadline(context, item.attempt_id)

    def receive(request):
        pytest.fail("Expired attempt must not send HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert await context.service.send(claim, client) == "ignored"
        assert not await context.service.dispatch_next(client)
    assert not await context.service.save_accepted(claim, "late-id")
    assert await context.watchdog.run_once()
    assert not await context.watchdog.run_once()
    assert (await attempt(context, item.attempt_id))["status"] == "timed_out"
    await assert_finances(context, item, refunded=True)


async def test_watchdog_pagination_skips_locked_user_and_continues(
    context, monkeypatch
):
    first = await create(context, refs=5)
    second = await create(context, refs=5)
    for item in (first, second):
        await context.service.claim(item.attempt_id)
        await expire_deadline(context, item.attempt_id)
    async with context.engine.begin() as connection:
        await connection.execute(
            sa.update(GenerationAttempt)
            .where(GenerationAttempt.id == first.attempt_id)
            .values(deadline_at=sa.func.now() - sa.text("interval '36 hours'"))
        )
    monkeypatch.setattr(operations, "BATCH_SIZE", 1)
    async with context.engine.begin() as blocker:
        await blocker.execute(
            sa.select(User).where(User.id == first.user_id).with_for_update()
        )
        async with asyncio.timeout(2):
            assert await context.watchdog.run_once()
        assert (await attempt(context, first.attempt_id))["status"] == "pending"
        assert (await attempt(context, second.attempt_id))["status"] == "timed_out"
    assert await context.watchdog.run_once()
    await assert_finances(context, first, refunded=True)
    await assert_finances(context, second, refunded=True)


def settings_dict(context):
    return {
        "database_url": context.urls["app"],
        "alpha_webhook_secret": "test",
        "beta_webhook_secret": "test",
        "payments_webhook_secret": "test",
        "provider_timeout": 600,
    }


async def four_processes(context, watchdog=False):
    process_context = multiprocessing.get_context("spawn")
    with process_context.Manager() as manager:
        barrier = manager.Barrier(4)
        with ProcessPoolExecutor(max_workers=4, mp_context=process_context) as pool:
            jobs = [
                pool.submit(
                    run_batch, context.urls, settings_dict(context), barrier, watchdog
                )
                for _ in range(4)
            ]
            results = await asyncio.gather(*(asyncio.wrap_future(job) for job in jobs))
    assert len({pid for pid, _ in results}) == 4
    return sum(calls for _, calls in results)


async def test_four_processes_dispatch_and_watchdog_do_not_lose_or_duplicate_work(
    context,
):
    items = [await create(context, refs=5) for _ in range(3)]
    assert await four_processes(context) == 3
    for item in items:
        assert (await attempt(context, item.attempt_id))["status"] == "awaiting_result"
        await expire_deadline(context, item.attempt_id)
    assert await four_processes(context, watchdog=True) == 0
    for item in items:
        assert (await attempt(context, item.attempt_id))["status"] == "timed_out"
        await assert_finances(context, item, refunded=True)
    async with context.admin.connect() as connection:
        assert (
            await connection.scalar(
                sa.select(sa.func.count()).select_from(ProviderTask)
            )
            == 3
        )


async def test_process_crash_after_provider_commit_recovers_one_external_generation(
    context,
):
    item = await create(context)
    process_context = multiprocessing.get_context("spawn")
    parent, child = process_context.Pipe(duplex=False)
    process = process_context.Process(
        target=crash_after_accept,
        args=(context.urls, settings_dict(context), str(item.attempt_id), child),
    )
    process.start()
    child.close()
    try:
        assert await asyncio.to_thread(parent.poll, 20)
        external_id, payload = parent.recv()
        await asyncio.to_thread(process.join, 10)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.kill()
            await asyncio.to_thread(process.join)
        parent.close()
    before = await attempt(context, item.attempt_id)
    assert before["status"] == "pending" and before["external_id"] is None
    assert before["dispatch_token"] is not None
    await expire_lease(context, item.attempt_id)

    async def receive(request):
        assert json.loads(request.content) == payload
        reply = await context.provider.accept(json.loads(request.content))
        assert reply.external_id == external_id
        return httpx.Response(202, json={"external_id": reply.external_id})

    recovered = DispatchService(context.transactions, context.settings)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert await recovered.dispatch_next(client)
    saved = await attempt(context, item.attempt_id)
    assert saved["status"] == "awaiting_result" and saved["external_id"] == external_id
    assert (
        saved["deadline_at"] == before["deadline_at"]
        and saved["sent_at"] == before["sent_at"]
    )
    async with context.admin.connect() as connection:
        assert (
            await connection.scalar(
                sa.select(sa.func.count()).select_from(ProviderTask)
            )
            == 1
        )
    await assert_finances(context, item)


@pytest.mark.parametrize("checkpoint", ["claim", "accepted", "repeated_claim"])
async def test_abrupt_exit_at_dispatch_commits_preserves_deadline_and_work(
    context, checkpoint
):
    item = await create(context, refs=5)
    process_context = multiprocessing.get_context("spawn")
    original = None
    for _ in range(3 if checkpoint == "repeated_claim" else 1):
        parent, child = process_context.Pipe(duplex=False)
        process = process_context.Process(
            target=crash_at_dispatch_commit,
            args=(
                context.urls,
                settings_dict(context),
                str(item.attempt_id),
                "accepted" if checkpoint == "accepted" else "claim",
                child,
            ),
        )
        process.start()
        child.close()
        try:
            assert await asyncio.to_thread(parent.poll, 20)
            assert parent.recv() == process.pid
            await asyncio.to_thread(process.join, 10)
            assert process.exitcode == 73
        finally:
            if process.is_alive():
                process.kill()
                await asyncio.to_thread(process.join, 10)
            parent.close()
        saved = await attempt(context, item.attempt_id)
        assert saved["status"] == (
            "awaiting_result" if checkpoint == "accepted" else "pending"
        )
        if original is None:
            original = saved
        else:
            for field in ("sent_at", "deadline_at", "request_payload", "id"):
                assert saved[field] == original[field]
        if checkpoint != "accepted":
            await expire_lease(context, item.attempt_id)

    if checkpoint != "repeated_claim":
        assert await four_processes(context) == (0 if checkpoint == "accepted" else 1)
        recovered = await attempt(context, item.attempt_id)
        assert recovered["status"] == "awaiting_result"
        assert recovered["deadline_at"] == original["deadline_at"]
        async with context.admin.connect() as connection:
            assert (
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(ProviderTask)
                )
                == 1
            )
    await expire_deadline(context, item.attempt_id)
    assert await four_processes(context, watchdog=True) == 0
    assert (await attempt(context, item.attempt_id))["status"] == "timed_out"
    assert len(await history(context, item.task_id)) == 1
    await assert_finances(context, item, refunded=True)


async def test_timeout_after_provider_acceptance_falls_back_and_ignores_late_result(
    context,
):
    item = await create(context)
    dispatcher = DispatchService(
        context.transactions, context.settings.model_copy(update={"http_timeout": 0.05})
    )
    beta = ProviderService(
        context.provider.transactions,
        context.provider.settings.model_copy(update={"provider_name": "beta"}),
    )
    calls = []

    async def receive(request):
        calls.append(request.url.host)
        provider = context.provider if request.url.host == "alpha" else beta
        accepted = await provider.accept(json.loads(request.content))
        if request.url.host == "alpha":
            await asyncio.sleep(1)
        return httpx.Response(202, json={"external_id": accepted.external_id})

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert (
            await dispatcher.dispatch(item.attempt_id, client) == "next_attempt_created"
        )
        assert await dispatcher.dispatch_next(client)
        assert not await dispatcher.dispatch_next(client)
    rows = await history(context, item.task_id)
    assert [row["status"] for row in rows] == ["failed", "awaiting_result"]
    assert calls == ["alpha", "beta"]
    async with context.admin.begin() as connection:
        await connection.execute(
            sa.update(ProviderTask).values(
                created_at=sa.func.now() - sa.text("interval '1 day'"),
                ready_at=sa.func.now() - sa.text("interval '1 day'"),
            )
        )
    for provider, expected in ((context.provider, "ignored"), (beta, "completed")):
        assert await provider.generate_one()
        async with context.admin.connect() as connection:
            body = await connection.scalar(
                sa.select(ProviderTask.webhook_body).where(
                    ProviderTask.provider_name == provider.settings.provider_name
                )
            )
        event = json.loads(body)
        result = await dispatcher.attempts.finish_attempt(
            FinishCommand(
                attempt_id=event["client_attempt_id"],
                provider_name=provider.settings.provider_name,
                external_id=event["external_id"],
                source="provider_webhook",
                outcome=event["status"],
                result_url=event["result_url"],
            )
        )
        assert result.status == expected
    assert [row["status"] for row in await history(context, item.task_id)] == [
        "failed",
        "completed",
    ]
    await assert_finances(context, item)


async def test_lifespan_recovers_pending_and_runs_watchdog_over_real_http(
    context, monkeypatch
):
    item = await create(context)
    requests = []

    async def receive(reader, writer):
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            length = next(
                int(line.split(b":", 1)[1])
                for line in headers.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            payload = json.loads(await reader.readexactly(length))
            requests.append((headers.split(b"\r\n")[0], payload))
            body = json.dumps(
                {"external_id": f"external-{payload['client_attempt_id']}"}
            ).encode()
            writer.write(
                b"HTTP/1.1 202 Accepted\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: "
                + str(len(body)).encode()
                + b"\r\n\r\n"
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(receive, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    for key, value in settings_dict(context).items():
        monkeypatch.setenv(key.upper(), str(value))
    module = importlib.import_module("generation_service.main")
    config = Settings(
        _env_file=None,
        **(
            settings_dict(context)
            | {
                "alpha_url": f"http://127.0.0.1:{port}/alpha",
                "beta_url": f"http://127.0.0.1:{port}/beta",
                "poll_interval": 0.02,
            }
        ),
    )
    app = module.create_app(config)

    async def wait_for_state(number, status):
        async with asyncio.timeout(5):
            while True:
                rows = await history(context, item.task_id)
                if len(rows) == number and rows[-1]["status"] == status:
                    return rows[-1]
                await asyncio.sleep(0.02)

    async with server, app.router.lifespan_context(app):
        await wait_for_state(1, "awaiting_result")
        await expire_deadline(context, item.attempt_id)
        second = await wait_for_state(2, "awaiting_result")
        assert second["provider_name"] == "beta"
        await expire_deadline(context, second["id"])
        await wait_for_state(2, "timed_out")
    assert len(requests) == 2
    assert (
        requests[0][0] == b"POST /alpha/tasks HTTP/1.1"
        and requests[1][0] == b"POST /beta/tasks HTTP/1.1"
    )
    await assert_finances(context, item, refunded=True)
