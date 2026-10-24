import asyncio
import hashlib
import hmac
import importlib
import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from fake_providers.config import ProviderSettings
from fake_providers.service import (
    ProviderService,
    RequestConflict,
    SynchronousFailure,
    tasks,
)
from generation_service.db.transactions import DatabaseUnavailable, TransactionRunner
from tests.db_support import alembic
from tests.provider_worker import accept_batch, crash_after_send


def settings(url, **overrides):
    return ProviderSettings(
        _env_file=None,
        **{
            "database_url": url,
            "provider_name": "alpha",
            "provider_webhook_secret": "test-secret",
            "delay_min": 0,
            "delay_max": 0,
            **overrides,
        },
    )


def payload(**overrides):
    return {
        "client_attempt_id": str(uuid4()),
        "model_slug": "image-fast",
        "prompt": "Море",
        "ref_images": [],
        "callback_url": "http://receiver/hooks",
        **overrides,
    }


@pytest.fixture(scope="module")
def provider_urls(database_factory):
    urls = database_factory()
    alembic(urls["migration"], "upgrade", "head")
    return urls


@pytest.fixture
async def context(provider_urls):
    engine = create_async_engine(
        provider_urls["provider"], pool_size=25, max_overflow=0
    )
    admin = create_async_engine(provider_urls["migration"])
    transactions = TransactionRunner(async_sessionmaker(engine, expire_on_commit=False))
    config = settings(provider_urls["provider"])
    ctx = SimpleNamespace(
        engine=engine,
        admin=admin,
        transactions=transactions,
        settings=config,
        urls=provider_urls,
        service=ProviderService(transactions, config),
    )
    yield ctx
    async with admin.begin() as connection:
        await connection.execute(sa.delete(tasks))
    await engine.dispose()
    await admin.dispose()


def service(context, **overrides):
    return ProviderService(
        context.transactions, settings(context.urls["provider"], **overrides)
    )


async def row(context, attempt_id):
    async with context.engine.connect() as connection:
        return (
            (
                await connection.execute(
                    sa.select(tasks).where(
                        tasks.c.provider_name == "alpha",
                        tasks.c.client_attempt_id == UUID(attempt_id),
                    )
                )
            )
            .mappings()
            .one_or_none()
        )


async def due(context, **values):
    # Make test scheduling independent of the host's occasional backwards clock jumps.
    async with context.admin.begin() as connection:
        await connection.execute(
            sa.update(tasks).values(
                created_at=sa.func.now() - sa.text("interval '1 day'"),
                ready_at=sa.func.now() - sa.text("interval '1 day'"),
                **values,
            )
        )


async def generate(context):
    await due(context)
    assert await context.service.generate_one()
    await due(
        context,
        next_delivery_at=sa.func.now() - sa.text("interval '1 day'"),
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"failure_rate": -0.1},
        {"failure_rate": 1.1},
        {"failure_rate": float("nan")},
        {"delay_min": 2, "delay_max": 1},
        {"late_completed": True, "late_delay": 60},
        {"webhook_timeout": 15},
        {"retry_interval": 61},
        {"sync_mode": "invalid"},
        {"failure_type": "unknown"},
        {"poll_interval": 0},
    ],
)
def test_invalid_provider_configuration(settings_values, overrides):
    with pytest.raises(ValidationError):
        settings(settings_values["database_url"], **overrides)


async def test_twenty_concurrent_acceptances_and_semantic_json(context):
    request = payload()
    replies = await asyncio.gather(
        *(context.service.accept(request) for _ in range(20))
    )
    assert len({reply.external_id for reply in replies}) == 1
    reordered = dict(reversed(list(request.items())))
    assert (await context.service.accept(reordered)).external_id == replies[
        0
    ].external_id
    initial = await row(context, request["client_attempt_id"])
    with pytest.raises(RequestConflict):
        await context.service.accept({**request, "prompt": "different"})
    assert await row(context, request["client_attempt_id"]) == initial


async def test_keys_isolate_providers_and_settings_do_not_rewrite_existing(context):
    request = payload()
    original = await context.service.accept(request)
    await generate(context)
    initial = await row(context, request["client_attempt_id"])
    assert (
        await service(context, sync_mode="503", failure_rate=1).accept(request)
        == original
    )
    assert await row(context, request["client_attempt_id"]) == initial
    other = await service(context, provider_name="beta").accept(request)
    assert other.external_id != original.external_id
    with pytest.raises(RequestConflict):
        await service(context, sync_mode="503").accept(
            {**request, "callback_url": "http://other/hooks"}
        )


async def test_synchronous_503_does_not_persist(context):
    request = payload()
    with pytest.raises(SynchronousFailure):
        await service(context, sync_mode="503").accept(request)
    assert await row(context, request["client_attempt_id"]) is None


@pytest.mark.parametrize("error_type", ["technical", "moderation", "payload"])
async def test_failures_are_chosen_once_and_survive_new_service(context, error_type):
    request = payload()
    await service(context, failure_rate=1, failure_type=error_type).accept(request)
    # Recreated service has the opposite settings; the stored choice remains authoritative.
    await generate(context)
    saved = await row(context, request["client_attempt_id"])
    event = json.loads(saved["webhook_body"])
    assert saved["status"] == "failed" and saved["error_type"] == error_type
    assert event == {
        "client_attempt_id": request["client_attempt_id"],
        "external_id": saved["external_id"],
        "status": "failed",
        "error_type": error_type,
    }
    assert not await context.service.generate_one()


async def test_late_success_forces_completion_and_is_not_ready_early(context):
    request = payload()
    await service(context, failure_rate=1, late_completed=True).accept(request)
    saved = await row(context, request["client_attempt_id"])
    assert (saved["ready_at"] - saved["created_at"]).total_seconds() == 65
    assert not await context.service.generate_one()
    await generate(context)
    assert (await row(context, request["client_attempt_id"]))["status"] == "completed"


async def test_no_webhook_stays_disabled_after_settings_change(context):
    request = payload()
    await service(context, no_webhook=True).accept(request)
    await due(context)
    assert await context.service.generate_one()
    saved = await row(context, request["client_attempt_id"])
    assert saved["webhook_body"] and saved["next_delivery_at"] is None
    assert await context.service.claim_delivery() is None
    assert saved["delivered_at"] is None


@pytest.mark.parametrize("concurrent", [False, True])
async def test_signed_duplicate_callbacks_outside_locks(context, concurrent):
    request = payload()
    await service(
        context, duplicate_webhook=True, concurrent_duplicates=concurrent
    ).accept(request)
    await generate(context)
    requests = []
    both = asyncio.Event()

    async def receive(incoming):
        requests.append(incoming)
        # HTTP must run after COMMIT: an independent transaction can lock this row.
        if len(requests) == 1:
            async with context.engine.begin() as connection:
                await connection.execute(sa.select(tasks).with_for_update(nowait=True))
        if concurrent:
            if len(requests) == 2:
                both.set()
            await asyncio.wait_for(both.wait(), 2)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert await context.service.deliver_one(client)
        assert not await context.service.deliver_one(client)
    saved = await row(context, request["client_attempt_id"])
    assert (
        len(requests) == 2
        and saved["successful_deliveries"] == 2
        and saved["delivered_at"]
    )
    for incoming in requests:
        assert incoming.content == saved["webhook_body"]
        assert (
            incoming.headers["x-signature"]
            == hmac.new(b"test-secret", incoming.content, hashlib.sha256).hexdigest()
        )
        assert incoming.headers["content-type"] == "application/json"
        assert str(incoming.url) == request["callback_url"]


@pytest.mark.parametrize("failure", [503, 400, 302, "network", "timeout"])
async def test_failed_callback_is_durable_and_retried_with_identical_body(
    context, failure
):
    request = payload()
    await context.service.accept(request)
    await generate(context)
    requests = []

    async def receive(incoming):
        requests.append(incoming)
        if len(requests) == 1:
            if failure == "network":
                raise httpx.ConnectError("offline", request=incoming)
            if failure == "timeout":
                raise httpx.ReadTimeout("slow", request=incoming)
            return httpx.Response(failure, headers={"Location": "http://other/"})
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        await context.service.deliver_one(client)
        saved = await row(context, request["client_attempt_id"])
        assert saved["successful_deliveries"] == 0 and saved["delivered_at"] is None
        assert saved["next_delivery_at"] and saved["delivery_token"] is None
        await due(
            context,
            next_delivery_at=sa.func.now() - sa.text("interval '1 day'"),
        )
        await service(context).deliver_one(client)
    assert len(requests) == 2 and requests[0].content == requests[1].content
    assert requests[0].headers["x-signature"] == requests[1].headers["x-signature"]
    assert (await row(context, request["client_attempt_id"]))["delivered_at"]


async def test_partial_duplicate_success_retries_only_missing_delivery(context):
    request = payload()
    await service(context, duplicate_webhook=True).accept(request)
    await generate(context)
    calls = []

    def receive(incoming):
        calls.append(incoming.content)
        return httpx.Response(503 if len(calls) == 2 else 200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        await context.service.deliver_one(client)
        saved = await row(context, request["client_attempt_id"])
        assert saved["successful_deliveries"] == 1 and saved["delivered_at"] is None
        await due(
            context,
            next_delivery_at=sa.func.now() - sa.text("interval '1 day'"),
        )
        await service(context).deliver_one(client)
    assert len(calls) == 3 and len(set(calls)) == 1
    assert (await row(context, request["client_attempt_id"]))[
        "successful_deliveries"
    ] == 2


async def test_concurrent_claims_and_stale_expired_tokens(context):
    request = payload()
    await context.service.accept(request)
    await generate(context)
    claims = await asyncio.gather(
        *(context.service.claim_delivery() for _ in range(20))
    )
    old = [claim for claim in claims if claim is not None]
    assert len(old) == 1
    # Crash after send, before acknowledgement: durable intent remains leased.
    async with context.engine.begin() as connection:
        await connection.execute(
            sa.update(tasks).values(
                delivery_lease_until=sa.func.clock_timestamp()
                - sa.text("interval '1 day'")
            )
        )
    assert not await context.service.acknowledge(old[0], 1)
    replacement = await service(context).claim_delivery()
    assert replacement.token != old[0].token and replacement.body == old[0].body
    assert not await context.service.acknowledge(old[0], 1)
    assert await context.service.acknowledge(replacement, 1)
    assert not await context.service.acknowledge(replacement, 1)


async def test_acceptance_from_four_processes_has_one_generation(context):
    request = payload()
    process_context = multiprocessing.get_context("spawn")
    with process_context.Manager() as manager:
        barrier = manager.Barrier(4)
        with ProcessPoolExecutor(max_workers=4, mp_context=process_context) as pool:
            jobs = [
                pool.submit(accept_batch, context.urls["provider"], request, barrier)
                for _ in range(4)
            ]
            results = await asyncio.gather(*(asyncio.wrap_future(job) for job in jobs))
    assert len({identifier for batch in results for identifier in batch}) == 1
    # All accepting processes have exited. A new service still finds the persisted work.
    await generate(context)
    assert (await row(context, request["client_attempt_id"]))["status"] == "completed"


def create_app(monkeypatch, context, **overrides):
    monkeypatch.setenv("DATABASE_URL", context.urls["provider"])
    monkeypatch.setenv("PROVIDER_NAME", "alpha")
    monkeypatch.setenv("PROVIDER_WEBHOOK_SECRET", "test-secret")
    module = importlib.import_module("fake_providers.main")
    app = module.create_app(settings(context.urls["provider"], **overrides))
    app.state.provider = service(context, **overrides)
    return app


async def test_http_accept_conflict_503_and_validation(monkeypatch, context):
    app = create_app(monkeypatch, context)
    request = payload()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://provider"
    ) as client:
        response = await client.post("/tasks", json=request)
        assert response.status_code == 202 and set(response.json()) == {"external_id"}
        reordered = json.dumps(
            dict(reversed(list(request.items()))), indent=4, ensure_ascii=True
        )
        assert (
            await client.post(
                "/tasks",
                content=reordered,
                headers={"Content-Type": "application/json"},
            )
        ).json() == response.json()
        assert (
            await client.post("/tasks", json={**request, "prompt": "different"})
        ).status_code == 409
        assert (
            await client.post(
                "/tasks", json={**request, "callback_url": "file:///etc/passwd"}
            )
        ).status_code == 422
        assert (
            await client.post("/tasks", json={**request, "unknown": True})
        ).status_code == 422
        app.state.provider = service(context, sync_mode="503")
        assert (await client.post("/tasks", json=request)).status_code == 202
        assert (await client.post("/tasks", json=payload())).status_code == 503


async def test_timeout_after_commit_allows_prompt_retry(monkeypatch, context):
    app = create_app(monkeypatch, context, sync_mode="timeout", response_delay=10)
    request = payload()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://provider"
    ) as client:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.2):
                await client.post("/tasks", json=request)
        saved = await row(context, request["client_attempt_id"])
        assert saved is not None and saved["scenario"]["response_delay"] == 10
        async with asyncio.timeout(1):
            retry = await client.post("/tasks", json=request)
        assert (
            retry.status_code == 202
            and retry.json()["external_id"] == saved["external_id"]
        )
    await generate(context)


async def test_http_database_failure_returns_503(monkeypatch, context):
    app = create_app(monkeypatch, context)

    class Offline:
        async def accept(self, payload):
            raise DatabaseUnavailable("offline")

    app.state.provider = Offline()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://provider"
    ) as client:
        assert (await client.post("/tasks", json=payload())).status_code == 503


async def test_process_crash_after_send_recovers_exact_callback(context):
    request = payload()
    await context.service.accept(request)
    await generate(context)
    process_context = multiprocessing.get_context("spawn")
    parent, child = process_context.Pipe(duplex=False)
    process = process_context.Process(
        target=crash_after_send, args=(context.urls["provider"], child)
    )
    process.start()
    child.close()
    try:
        assert await asyncio.to_thread(parent.poll, 20)
        delivered_body = parent.recv()
        await asyncio.to_thread(process.join, 10)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.kill()
            await asyncio.to_thread(process.join)
        parent.close()
    saved = await row(context, request["client_attempt_id"])
    assert saved["delivery_token"] and saved["delivered_at"] is None
    assert saved["successful_deliveries"] == 0
    async with context.engine.begin() as connection:
        await connection.execute(
            sa.update(tasks).values(
                delivery_lease_until=sa.func.now() - sa.text("interval '1 day'")
            )
        )
    received = []

    def receive(incoming):
        received.append(incoming.content)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert await service(context).deliver_one(client)
    assert received == [delivered_body]
    assert (await row(context, request["client_attempt_id"]))["delivered_at"]


async def test_lifespan_background_workers_deliver_over_real_http(monkeypatch, context):
    received = asyncio.Event()
    requests = []

    async def receive(reader, writer):
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            length = next(
                int(line.split(b":", 1)[1])
                for line in headers.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            body = await reader.readexactly(length)
            requests.append((headers, body))
            writer.write(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            await writer.drain()
            received.set()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(receive, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    app = create_app(monkeypatch, context, poll_interval=0.02)
    request = payload(callback_url=f"http://127.0.0.1:{port}/hooks")
    async with server, app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://provider"
        ) as client:
            assert (await client.get("/health/ready")).status_code == 200
            assert (await client.post("/tasks", json=request)).status_code == 202
        await due(context)
        await asyncio.wait_for(received.wait(), 5)
        async with asyncio.timeout(5):
            while (await row(context, request["client_attempt_id"]))[
                "delivered_at"
            ] is None:
                await asyncio.sleep(0.02)
    assert len(requests) == 1
    headers, body = requests[0]
    expected = hmac.new(b"test-secret", body, hashlib.sha256).hexdigest().encode()
    assert b"x-signature: " + expected in headers.lower()
    assert json.loads(body)["status"] == "completed"
