"""Three live HTTP servers on a disposable PostgreSQL database, using real lifespans."""

import asyncio
import importlib
import socket

import httpx
import pytest
import sqlalchemy as sa
import uvicorn

from fake_providers.config import ProviderSettings
from fake_providers.models import ProviderTask
from generation_service.config import Settings
from generation_service.db.models import GenerationAttempt
from tests.test_api import api_urls, context, signed, user

__all__ = ["api_urls", "context"]


@pytest.mark.parametrize(
    "case,model,expected,providers,balance",
    [
        ("success", "image-fast", "completed", ["alpha"], 98),
        ("fallback", "image-fast", "completed", ["alpha", "beta"], 98),
        ("payload", "image-fast", "failed", ["alpha"], 100),
        ("moderation_skip", "video-long", "failed", ["beta"], 100),
        ("all_fail", "image-fast", "failed", ["alpha", "beta"], 100),
        ("silent", "image-fast", "completed", ["alpha", "beta"], 98),
        ("late", "image-fast", "completed", ["alpha", "beta"], 98),
    ],
)
async def test_generation_payment_and_callbacks_over_real_http(
    context, monkeypatch, case, model, expected, providers, balance
):
    sockets = []
    for _ in range(3):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sock.listen(128)
        sock.setblocking(False)
        sockets.append(sock)
    urls = [f"http://127.0.0.1:{sock.getsockname()[1]}" for sock in sockets]
    main_config = context.settings.model_dump()
    main_config.update(
        alpha_url=urls[1],
        beta_url=urls[2],
        callback_base_url=urls[0],
        poll_interval=0.01,
    )
    context.app.state.settings = Settings(_env_file=None, **main_config)
    monkeypatch.setenv("DATABASE_URL", context.urls["provider"])
    monkeypatch.setenv("PROVIDER_NAME", "alpha")
    monkeypatch.setenv("PROVIDER_WEBHOOK_SECRET", "alpha-test-secret")
    module = importlib.import_module("fake_providers.main")
    apps = [context.app]
    for name in ("alpha", "beta"):
        options = {
            "failure_rate": int(
                case == "all_fail"
                or (case in {"fallback", "payload"} and name == "alpha")
                or (case == "moderation_skip" and name == "beta")
            ),
            "failure_type": "payload"
            if case == "payload"
            else "moderation"
            if case == "moderation_skip"
            else "technical",
            "no_webhook": case == "silent" and name == "alpha",
            "late_completed": case == "late" and name == "alpha",
        }
        apps.append(
            module.create_app(
                ProviderSettings(
                    _env_file=None,
                    database_url=context.urls["provider"],
                    provider_name=name,
                    provider_webhook_secret=f"{name}-test-secret",
                    delay_min=0,
                    delay_max=0,
                    poll_interval=0.01,
                    duplicate_webhook=True,
                    concurrent_duplicates=True,
                    provider_timeout=600,
                    late_delay=650,
                    **options,
                )
            )
        )
    servers = [
        uvicorn.Server(uvicorn.Config(app, log_level="warning", access_log=False))
        for app in apps
    ]
    workers = [
        asyncio.create_task(server.serve(sockets=[sock]))
        for server, sock in zip(servers, sockets)
    ]

    async def due_provider_rows(include_late=False):
        # The host clock sometimes jumps backwards ~58 seconds. Age fixture rows
        # to avoid making a fast zero-delay test depend on wall-clock recovery.
        async with context.admin.begin() as connection:
            query = sa.update(ProviderTask).where(ProviderTask.status == "accepted")
            if case == "late" and not include_late:
                query = query.where(ProviderTask.provider_name != "alpha")
            await connection.execute(
                query.values(
                    created_at=sa.func.now() - sa.text("interval '1 day'"),
                    ready_at=sa.func.now() - sa.text("interval '1 day'"),
                )
            )
            await connection.execute(
                sa.update(ProviderTask)
                .where(
                    ProviderTask.delivered_at.is_(None),
                    ProviderTask.delivery_token.is_(None),
                    ProviderTask.next_delivery_at.is_not(None),
                )
                .values(next_delivery_at=sa.func.now() - sa.text("interval '1 day'"))
            )

    try:
        async with asyncio.timeout(10):
            while not all(server.started for server in servers):
                for worker in workers:
                    if worker.done():
                        worker.result()
                await asyncio.sleep(0.01)
        user_id = await user(context, tokens=0)
        async with httpx.AsyncClient(base_url=urls[0], timeout=5) as client:
            payment = await signed(
                client,
                "/webhooks/payments",
                {
                    "payment_id": "тестовая-оплата",
                    "user_id": user_id,
                    "tokens": 100,
                    "status": "success",
                },
            )
            assert payment.status_code == 200 and payment.json()["applied"]
            created = await client.post(
                "/generate",
                json={
                    "user_id": user_id,
                    "model_slug": model,
                    "prompt": "Море",
                    "ref_images": [],
                },
            )
            assert created.status_code == 202
            path = "/tasks/" + created.json()["task_id"]
            if case in {"silent", "late"}:
                saved = (await client.get(path)).json()
                first = saved["attempts"][0]
                async with context.engine.begin() as connection:
                    await connection.execute(
                        sa.update(GenerationAttempt)
                        .where(GenerationAttempt.id == first["attempt_id"])
                        .values(
                            sent_at=sa.func.now() - sa.text("interval '2 days'"),
                            deadline_at=sa.func.now() - sa.text("interval '1 day'"),
                        )
                    )
            async with asyncio.timeout(10):
                while True:
                    await due_provider_rows()
                    saved = (await client.get(path)).json()
                    if saved["status"] in {"completed", "failed"}:
                        break
                    await asyncio.sleep(0.02)
            assert saved["status"] == expected and saved["current_leg"] is None
            assert [item["provider_name"] for item in saved["attempts"]] == providers
            if case in {"late", "silent"}:
                assert saved["attempts"][0]["status"] == "timed_out"
            if case == "late":
                await due_provider_rows(include_late=True)
            async with asyncio.timeout(10):
                while True:
                    async with context.admin.connect() as connection:
                        rows = (
                            (
                                await connection.execute(
                                    sa.select(ProviderTask.__table__)
                                )
                            )
                            .mappings()
                            .all()
                        )
                    if all(
                        row["scenario"]["no_webhook"]
                        or row["successful_deliveries"] == 2
                        for row in rows
                    ):
                        break
                    await due_provider_rows(include_late=case == "late")
                    await asyncio.sleep(0.02)
            assert (await client.get(path)).json() == saved
            money = (await client.get(f"/users/{user_id}/balance")).json()
            assert (
                money["balance"]
                == balance
                == sum(entry["amount"] for entry in money["entries"])
            )
            assert [entry["kind"] for entry in money["entries"]].count(
                "generation_refund"
            ) == int(expected == "failed")
    finally:
        for server in servers:
            server.should_exit = True
        await asyncio.wait_for(asyncio.gather(*workers), 10)
        for sock in sockets:
            sock.close()
