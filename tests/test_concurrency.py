"""Four independent HTTP API processes sharing a disposable PostgreSQL database."""

import asyncio
import importlib
import json
import multiprocessing
import socket
from types import SimpleNamespace

import httpx
import pytest
import sqlalchemy as sa
import uvicorn

from fake_providers.config import ProviderSettings
from fake_providers.models import ProviderTask
from generation_service.db.models import GenerationAttempt, GenerationTask, User
from tests.http_worker import serve_api
from tests.test_api import api_urls, context, event, signature, user

__all__ = ["api_urls", "context"]


def listener():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    sock.setblocking(False)
    return sock


def signed_body(payload, secret="payments-test-secret"):
    raw = json.dumps(payload, ensure_ascii=False, indent=2).encode()
    return raw, {
        "Content-Type": "application/json",
        "X-Signature": signature(raw, secret),
    }


@pytest.fixture
async def cluster(context, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", context.urls["provider"])
    monkeypatch.setenv("PROVIDER_NAME", "alpha")
    monkeypatch.setenv("PROVIDER_WEBHOOK_SECRET", "alpha-test-secret")
    provider_module = importlib.import_module("fake_providers.main")
    provider_sockets = [listener(), listener()]
    provider_urls = [
        f"http://127.0.0.1:{sock.getsockname()[1]}" for sock in provider_sockets
    ]
    servers = [
        uvicorn.Server(
            uvicorn.Config(
                provider_module.create_app(
                    ProviderSettings(
                        _env_file=None,
                        database_url=context.urls["provider"],
                        provider_name=name,
                        provider_webhook_secret=f"{name}-test-secret",
                        no_webhook=True,
                        delay_min=0,
                        delay_max=0,
                        poll_interval=0.02,
                    )
                ),
                log_level="error",
                access_log=False,
            )
        )
        for name in ("alpha", "beta")
    ]
    server_tasks = [
        asyncio.create_task(server.serve(sockets=[sock]))
        for server, sock in zip(servers, provider_sockets)
    ]
    process_context = multiprocessing.get_context("spawn")
    processes, pipes, urls = [], [], []
    values = {
        "database_url": context.urls["app"],
        "alpha_webhook_secret": "alpha-test-secret",
        "beta_webhook_secret": "beta-test-secret",
        "payments_webhook_secret": "payments-test-secret",
        "alpha_url": provider_urls[0],
        "beta_url": provider_urls[1],
        "provider_timeout": 600,
        "poll_interval": 0.02,
    }
    try:
        for _ in range(4):
            sock = listener()
            url = f"http://127.0.0.1:{sock.getsockname()[1]}"
            parent, child = process_context.Pipe(duplex=False)
            process = process_context.Process(
                target=serve_api, args=(sock, values, child)
            )
            process.start()
            child.close()
            sock.close()
            processes.append(process)
            pipes.append(parent)
            urls.append(url)
        async with httpx.AsyncClient(timeout=10) as client:
            async with asyncio.timeout(30):
                for url, process in zip(urls, processes):
                    while True:
                        assert process.is_alive(), process.exitcode
                        try:
                            ready = await client.get(url + "/health/ready")
                            if ready.status_code == 200:
                                break
                        except httpx.TransportError:
                            pass
                        await asyncio.sleep(0.02)
                while not all(server.started for server in servers):
                    for task in server_tasks:
                        if task.done():
                            task.result()
                    await asyncio.sleep(0.02)
            yield SimpleNamespace(
                context=context,
                client=client,
                urls=urls,
                processes=processes,
                pipes=pipes,
            )
    finally:
        for process in processes:
            if process.is_alive():
                process.kill()
        for process in processes:
            await asyncio.to_thread(process.join, 10)
            assert not process.is_alive()
        for pipe in pipes:
            pipe.close()
        for server in servers:
            server.should_exit = True
        await asyncio.wait_for(asyncio.gather(*server_tasks), 10)
        for sock in provider_sockets:
            sock.close()


async def post(cluster, path, payload, peer=0, secret=None, checkpoint=None):
    if secret is None:
        raw = json.dumps(payload, ensure_ascii=False).encode()
        headers = {"Content-Type": "application/json"}
    else:
        raw, headers = signed_body(payload, secret)
    if checkpoint:
        headers["X-Test-Checkpoint"] = checkpoint
    return await cluster.client.post(
        cluster.urls[peer] + path, content=raw, headers=headers
    )


async def read_task(cluster, task_id, peer=1):
    response = await cluster.client.get(cluster.urls[peer] + f"/tasks/{task_id}")
    assert response.status_code == 200, response.text
    return response.json()


async def awaiting(cluster, task_id, provider):
    async with asyncio.timeout(15):
        while True:
            saved = await read_task(cluster, task_id)
            last = saved["attempts"][-1]
            if (
                last["provider_name"] == provider
                and last["status"] == "awaiting_result"
            ):
                return saved
            await asyncio.sleep(0.02)


async def create(cluster, owner, refs=0, peer=0):
    response = await post(
        cluster,
        "/generate",
        {
            "user_id": owner,
            "model_slug": "image-fast",
            "prompt": "Море",
            "ref_images": ["ref"] * refs,
        },
        peer=peer,
    )
    assert response.status_code == 202, response.text
    return await awaiting(
        cluster, response.json()["task_id"], "beta" if refs > 4 else "alpha"
    )


async def callback(cluster, attempt, peer=0, **overrides):
    return await post(
        cluster,
        f"/webhooks/providers/{attempt['provider_name']}",
        event(attempt, **overrides),
        peer=peer,
        secret=f"{attempt['provider_name']}-test-secret",
    )


def assert_peers(responses, processes):
    assert {int(response.headers["x-test-process"]) for response in responses} == {
        process.pid for process in processes
    }


async def money(cluster, owner):
    response = await cluster.client.get(cluster.urls[1] + f"/users/{owner}/balance")
    assert response.status_code == 200
    result = response.json()
    assert result["balance"] == sum(entry["amount"] for entry in result["entries"])
    return result


async def test_twenty_signed_payments_across_four_http_processes(cluster):
    owner = await user(cluster.context, tokens=0)
    payload = {
        "payment_id": "four-workers",
        "user_id": owner,
        "tokens": 7,
        "status": "success",
    }
    responses = await asyncio.gather(
        *(
            post(
                cluster,
                "/webhooks/payments",
                payload,
                peer=i % 4,
                secret="payments-test-secret",
            )
            for i in range(20)
        )
    )
    assert all(response.status_code == 200 for response in responses)
    assert_peers(responses, cluster.processes)
    assert sum(response.json()["applied"] for response in responses) == 1
    balance = await money(cluster, owner)
    assert balance["balance"] == 7 and len(balance["entries"]) == 1


@pytest.mark.parametrize("tokens,existing,rejected", [(2, 0, 402), (100, 2, 429)])
async def test_competing_http_generations_respect_balance_and_last_slot(
    cluster, tokens, existing, rejected
):
    owner = await user(cluster.context, tokens=tokens)
    for _ in range(existing):
        await create(cluster, owner)
    payload = {
        "user_id": owner,
        "model_slug": "image-fast",
        "prompt": "Море",
        "ref_images": [],
    }
    responses = await asyncio.gather(
        *(post(cluster, "/generate", payload, peer=i % 4) for i in range(20))
    )
    assert_peers(responses, cluster.processes)
    assert [response.status_code for response in responses].count(202) == 1
    assert [response.status_code for response in responses].count(rejected) == 19
    balance = await money(cluster, owner)
    assert balance["balance"] == tokens - 2 * (existing + 1)
    assert (
        sum(entry["kind"] == "generation_debit" for entry in balance["entries"])
        == existing + 1
    )
    async with cluster.context.engine.connect() as connection:
        assert (
            await connection.scalar(
                sa.select(sa.func.count())
                .select_from(GenerationTask)
                .where(GenerationTask.user_id == owner)
            )
            == existing + 1
        )


async def test_ten_last_leg_failures_and_late_results_across_four_processes(cluster):
    owner = await user(cluster.context)
    saved = await create(cluster, owner, refs=5)
    attempt = saved["attempts"][0]
    responses = await asyncio.gather(
        *(callback(cluster, attempt, peer=i % 4) for i in range(10))
    )
    assert all(response.status_code == 200 for response in responses)
    assert_peers(responses, cluster.processes)
    assert (
        sum(response.json()["result"] == "failed_refunded" for response in responses)
        == 1
    )
    final = await read_task(cluster, saved["task_id"])
    assert final["status"] == "failed" and final["result_url"] is None
    for peer in range(4):
        late = await callback(
            cluster,
            attempt,
            peer=peer,
            status="completed",
            error_type=None,
            result_url="https://files.test/late",
        )
        assert late.status_code == 200 and late.json()["result"] == "ignored"
    assert await read_task(cluster, saved["task_id"]) == final
    balance = await money(cluster, owner)
    assert balance["balance"] == 100
    assert (
        sum(entry["kind"] == "generation_refund" for entry in balance["entries"]) == 1
    )


@pytest.mark.parametrize("source", ["failure", "watchdog"])
async def test_fallback_and_late_old_result_with_four_live_executors(cluster, source):
    owner = await user(cluster.context)
    saved = await create(cluster, owner)
    first = saved["attempts"][0]
    if source == "failure":
        response = await callback(cluster, first)
        assert response.json()["result"] == "next_attempt_created"
    else:
        async with cluster.context.engine.begin() as connection:
            await connection.execute(
                sa.update(GenerationAttempt)
                .where(GenerationAttempt.id == first["attempt_id"])
                .values(
                    sent_at=sa.func.now() - sa.text("interval '2 days'"),
                    deadline_at=sa.func.now() - sa.text("interval '1 day'"),
                )
            )
    running = await awaiting(cluster, saved["task_id"], "beta")
    assert [item["status"] for item in running["attempts"]] == [
        "failed" if source == "failure" else "timed_out",
        "awaiting_result",
    ]
    for peer in range(4):
        late = await callback(
            cluster,
            first,
            peer=peer,
            status="completed",
            error_type=None,
            result_url="https://files.test/old",
        )
        assert late.status_code == 200 and late.json()["result"] == "ignored"
    assert await read_task(cluster, saved["task_id"]) == running
    response = await callback(
        cluster,
        running["attempts"][1],
        peer=3,
        status="completed",
        error_type=None,
        result_url="https://files.test/winner",
    )
    assert response.json()["result"] == "completed"
    final = await read_task(cluster, saved["task_id"])
    repeated = await callback(cluster, first, peer=2)
    assert repeated.json()["result"] == "ignored"
    assert await read_task(cluster, saved["task_id"]) == final
    assert final["result_url"] == "https://files.test/winner"
    assert (await money(cluster, owner))["balance"] == 98


async def test_provider_results_race_four_watchdogs_without_result_and_refund(cluster):
    owner = await user(cluster.context)
    saved = await create(cluster, owner, refs=5)
    attempt = saved["attempts"][0]
    requests = []
    try:
        async with cluster.context.engine.begin() as holder:
            holder_pid = await holder.scalar(sa.select(sa.func.pg_backend_pid()))
            await holder.execute(
                sa.select(User.id).where(User.id == owner).with_for_update()
            )
            await holder.execute(
                sa.update(GenerationAttempt)
                .where(GenerationAttempt.id == attempt["attempt_id"])
                .values(
                    sent_at=sa.func.now() - sa.text("interval '2 days'"),
                    deadline_at=sa.func.now() - sa.text("interval '1 day'"),
                )
            )
            for i in range(20):
                outcome = (
                    {
                        "status": "completed",
                        "error_type": None,
                        "result_url": "https://files.test/race",
                    }
                    if i % 2
                    else {}
                )
                requests.append(
                    asyncio.create_task(
                        callback(cluster, attempt, peer=i % 4, **outcome)
                    )
                )
            # Wait for a real lock waiter, not an arbitrary timing delay. Releasing
            # the row also publishes the expired deadline to all four watchdogs.
            async with asyncio.timeout(10):
                async with cluster.context.admin.connect() as observer:
                    while not await observer.scalar(
                        sa.text(
                            "SELECT count(*) FROM pg_stat_activity WHERE :holder = ANY(pg_blocking_pids(pid))"
                        ),
                        {"holder": holder_pid},
                    ):
                        await asyncio.sleep(0.01)
            assert not any(request.done() for request in requests)
        responses = await asyncio.gather(*requests)
    finally:
        for request in requests:
            request.cancel()
        await asyncio.gather(*requests, return_exceptions=True)
    assert all(response.status_code == 200 for response in responses)
    assert_peers(responses, cluster.processes)
    assert sum(response.json()["result"] != "ignored" for response in responses) <= 1
    final = await read_task(cluster, saved["task_id"])
    assert final["status"] in {"completed", "failed"}
    assert final["current_leg"] is None and len(final["attempts"]) == 1
    balance = await money(cluster, owner)
    refunded = final["status"] == "failed"
    assert bool(final["result_url"]) == (not refunded)
    assert balance["balance"] == (100 if refunded else 98)
    assert (
        sum(entry["kind"] == "generation_refund" for entry in balance["entries"])
        == refunded
    )


async def kill_paused_request(cluster, pending):
    process = cluster.processes[0]
    try:
        assert await asyncio.to_thread(cluster.pipes[0].poll, 20), (
            "Checkpoint not reached"
        )
        pid, phase, result = cluster.pipes[0].recv()
        assert pid == process.pid
        process.kill()
        await asyncio.to_thread(process.join, 10)
        assert process.exitcode is not None and process.exitcode != 0
        with pytest.raises(httpx.TransportError):
            await pending
        return phase, result
    finally:
        if process.is_alive():
            process.kill()
            await asyncio.to_thread(process.join, 10)
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
@pytest.mark.parametrize(
    "operation", ["payment", "generation", "fallback", "refund", "completion"]
)
async def test_killed_http_process_is_recovered_by_three_survivors(
    cluster, phase, operation
):
    owner = await user(cluster.context, tokens=0 if operation == "payment" else 100)
    if operation == "payment":
        path = "/webhooks/payments"
        payload = {
            "payment_id": "crash-payment",
            "user_id": owner,
            "tokens": 7,
            "status": "success",
        }
        secret = "payments-test-secret"
    elif operation == "generation":
        path = "/generate"
        payload = {
            "user_id": owner,
            "model_slug": "image-fast",
            "prompt": "Море",
            "ref_images": [],
        }
        secret = None
    else:
        saved = await create(
            cluster, owner, refs=5 if operation == "refund" else 0, peer=1
        )
        first = saved["attempts"][0]
        path = f"/webhooks/providers/{first['provider_name']}"
        payload = event(first)
        if operation == "completion":
            payload.update(
                status="completed",
                error_type=None,
                result_url="https://files.test/committed",
            )
        secret = f"{first['provider_name']}-test-secret"

    pending = asyncio.create_task(
        post(cluster, path, payload, secret=secret, checkpoint=phase)
    )
    observed_phase, result = await kill_paused_request(cluster, pending)
    assert observed_phase == phase
    if operation == "generation":
        if phase == "before_commit":
            assert (
                await cluster.client.get(cluster.urls[1] + f"/tasks/{result.task_id}")
            ).status_code == 404
            assert (await money(cluster, owner))["balance"] == 100
            recovered = await create(cluster, owner, peer=1)
            assert recovered["task_id"] != str(result.task_id)
        else:
            recovered = await awaiting(cluster, result.task_id, "alpha")
        assert len(recovered["attempts"]) == 1
        assert (await money(cluster, owner))["balance"] == 98
        async with cluster.context.admin.connect() as connection:
            assert (
                await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(GenerationTask)
                    .where(GenerationTask.user_id == owner)
                )
                == 1
            )
            assert (
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(ProviderTask)
                )
                == 1
            )
        return

    responses = await asyncio.gather(
        *(
            post(cluster, path, payload, peer=1 + i % 3, secret=secret)
            for i in range(12)
        )
    )
    assert all(response.status_code == 200 for response in responses)
    assert_peers(responses, cluster.processes[1:])
    applied = sum(
        response.json()["applied"]
        if operation == "payment"
        else response.json()["result"] != "ignored"
        for response in responses
    )
    assert applied == (1 if phase == "before_commit" else 0)
    if operation == "payment":
        balance = await money(cluster, owner)
        assert balance["balance"] == 7 and len(balance["entries"]) == 1
        return
    if operation == "fallback":
        running = await awaiting(cluster, saved["task_id"], "beta")
        assert len(running["attempts"]) == 2
        response = await callback(
            cluster,
            running["attempts"][1],
            peer=2,
            status="completed",
            error_type=None,
            result_url="https://files.test/recovered",
        )
        assert response.json()["result"] == "completed"
    final = await read_task(cluster, saved["task_id"])
    assert final["status"] == ("failed" if operation == "refund" else "completed")
    assert final["current_leg"] is None
    balance = await money(cluster, owner)
    assert balance["balance"] == (100 if operation == "refund" else 98)
    assert sum(entry["kind"] == "generation_debit" for entry in balance["entries"]) == 1
    assert sum(
        entry["kind"] == "generation_refund" for entry in balance["entries"]
    ) == (operation == "refund")
