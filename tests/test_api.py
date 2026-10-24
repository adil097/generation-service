import asyncio
import hashlib
import hmac
import importlib
import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from fake_providers.models import ProviderTask
from generation_service.config import Settings
from generation_service.db.models import (
    GenerationAttempt,
    GenerationTask,
    LedgerEntry,
    User,
)
from generation_service.db.transactions import DatabaseUnavailable, TransactionRunner
from generation_service.services.attempts import AttemptService, FinishCommand
from generation_service.services.dispatch import DispatchService, WatchdogService
from generation_service.services.errors import LedgerIntegrityError
from generation_service.services.finance import FinancialService, PaymentEvent


@pytest.fixture(scope="module")
def api_urls(database_factory):
    from tests.db_support import alembic

    urls = database_factory()
    alembic(urls["migration"], "upgrade", "head")
    return urls


def signature(body, secret="payments-test-secret"):
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


async def signed(client, path, data, secret="payments-test-secret", **kwargs):
    body = json.dumps(data, ensure_ascii=False, indent=2).encode()
    return await client.post(
        path,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Signature": signature(body, secret),
        },
        **kwargs,
    )


@pytest.fixture
async def context(api_urls, settings_values, monkeypatch):
    config = Settings(
        _env_file=None,
        **(
            settings_values | {"database_url": api_urls["app"], "provider_timeout": 600}
        ),
    )
    for name, value in settings_values.items():
        monkeypatch.setenv(name.upper(), value)
    module = importlib.import_module("generation_service.main")
    app = module.create_app(config)
    engine = create_async_engine(api_urls["app"], pool_size=25, max_overflow=0)
    admin = create_async_engine(api_urls["migration"])
    transactions = TransactionRunner(async_sessionmaker(engine, expire_on_commit=False))
    ctx = SimpleNamespace(
        app=app,
        settings=config,
        engine=engine,
        admin=admin,
        transactions=transactions,
        urls=api_urls,
        calls=[],
    )

    async def respond(request):
        return httpx.Response(
            202,
            json={
                "external_id": "external-"
                + json.loads(request.content)["client_attempt_id"]
            },
        )

    ctx.respond = respond

    async def receive(request):
        ctx.calls.append(request)
        return await ctx.respond(request)

    app.state.engine = engine
    app.state.transactions = transactions
    app.state.dispatch = DispatchService(transactions, config)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as outbound:
        app.state.http_client = outbound
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://api"
        ) as client:
            ctx.client = client
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


async def user(context, tokens=100):
    user_id = uuid4().int % (2**62)
    async with context.engine.begin() as connection:
        await connection.execute(sa.insert(User).values(id=user_id))
    if tokens:
        await FinancialService(context.transactions).credit_payment(
            PaymentEvent(
                payment_id=str(uuid4()),
                user_id=user_id,
                tokens=tokens,
                status="success",
            )
        )
    return user_id


async def generate(context, owner_id=None, **overrides):
    if owner_id is None:
        owner_id = await user(context)
    return await context.client.post(
        "/generate",
        json={
            "user_id": owner_id,
            "model_slug": "image-fast",
            "prompt": "Море",
            "ref_images": [],
            **overrides,
        },
    )


async def task(context, response):
    assert response.status_code == 202, response.text
    result = await context.client.get("/tasks/" + response.json()["task_id"])
    assert result.status_code == 200
    return result.json()


def event(attempt, **overrides):
    return {
        "client_attempt_id": attempt["attempt_id"],
        "external_id": attempt["external_id"],
        "status": "failed",
        "error_type": "technical",
        **overrides,
    }


async def provider_event(context, attempt, **overrides):
    name = attempt["provider_name"]
    return await signed(
        context.client,
        f"/webhooks/providers/{name}",
        event(attempt, **overrides),
        f"{name}-test-secret",
    )


async def balance(context, user_id, **params):
    response = await context.client.get(f"/users/{user_id}/balance", params=params)
    assert response.status_code == 200
    return response.json()


async def test_generate_read_task_and_balance_contract(context):
    user_id = await user(context)
    response = await generate(context, user_id)
    assert set(response.json()) == {"task_id"}
    saved = await task(context, response)
    assert saved["status"] == "processing" and saved["user_id"] == user_id
    assert saved["model_slug"] == "image-fast" and saved["price_tokens"] == 2
    assert saved["prompt"] == "Море" and saved["ref_images"] == []
    assert saved["current_leg"]["provider_name"] == "alpha"
    attempt = saved["attempts"][0]
    assert attempt["attempt_number"] == 1 and attempt["status"] == "awaiting_result"
    assert (
        attempt["sent_at"] and attempt["deadline_at"] and attempt["finished_at"] is None
    )
    assert (
        not {"request_payload", "dispatch_token", "dispatch_lease_until"}
        & attempt.keys()
    )
    assert len(context.calls) == 1
    money = await balance(context, user_id)
    assert (
        money["balance"] == 98
        and sum(entry["amount"] for entry in money["entries"]) == 98
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"user_id": True},
        {"user_id": "1"},
        {"prompt": " "},
        {"ref_images": [None]},
        {"ref_images": "bad"},
        {"ref_images": [""]},
        {"price_tokens": 0},
    ],
)
async def test_generation_validation_has_no_side_effects(context, overrides):
    user_id = await user(context)
    response = await generate(context, user_id, **overrides)
    assert (
        response.status_code == 422
        and response.json()["detail"]["code"] == "invalid_request"
    )
    assert (await balance(context, user_id))["balance"] == 100 and not context.calls


@pytest.mark.parametrize(
    "case,status,code",
    [
        ("model", 404, "model_not_found"),
        ("user", 404, "user_not_found"),
        ("refs", 400, "unsupported_references"),
        ("funds", 402, "insufficient_funds"),
        ("limit", 429, "active_task_limit"),
    ],
)
async def test_generation_domain_errors_are_consistent(context, case, status, code):
    user_id = await user(context, tokens=0 if case == "funds" else 100)
    options = {"model_slug": "unknown"} if case == "model" else {}
    if case == "user":
        user_id = 2**63 - 1
    if case == "refs":
        options["ref_images"] = ["ref"] * 11
    if case == "limit":
        for _ in range(3):
            assert (await generate(context, user_id)).status_code == 202
    before = len(context.calls)
    response = await generate(context, user_id, **options)
    assert response.status_code == status and response.json()["detail"]["code"] == code
    assert len(context.calls) == before


async def test_twenty_payment_webhooks_create_one_credit(context):
    user_id = await user(context, tokens=0)
    payload = {
        "payment_id": "оплата-одна",
        "user_id": user_id,
        "tokens": 7,
        "status": "success",
    }
    replies = await asyncio.gather(
        *(signed(context.client, "/webhooks/payments", payload) for _ in range(20))
    )
    assert all(reply.status_code == 200 for reply in replies)
    assert sum(reply.json()["applied"] for reply in replies) == 1
    money = await balance(context, user_id)
    assert money["balance"] == 7 and len(money["entries"]) == 1
    conflict = await signed(
        context.client, "/webhooks/payments", {**payload, "tokens": 8}
    )
    assert (
        conflict.status_code == 409
        and conflict.json()["detail"]["code"] == "payment_conflict"
    )


async def test_payment_identifier_cannot_credit_a_different_user(context):
    first = await user(context, tokens=0)
    second = await user(context, tokens=0)
    payload = {
        "payment_id": "shared",
        "user_id": first,
        "tokens": 4,
        "status": "success",
    }
    assert (
        await signed(context.client, "/webhooks/payments", payload)
    ).status_code == 200
    assert (
        await signed(
            context.client, "/webhooks/payments", {**payload, "user_id": second}
        )
    ).status_code == 409
    assert (await balance(context, first))["balance"] == 4
    assert (await balance(context, second))["balance"] == 0


@pytest.mark.parametrize("status", ["pending", "failed"])
async def test_unsuccessful_payment_is_acknowledged_without_credit(context, status):
    user_id = await user(context, tokens=0)
    payload = {
        "payment_id": str(uuid4()),
        "user_id": user_id,
        "tokens": 9,
        "status": status,
    }
    response = await signed(context.client, "/webhooks/payments", payload)
    assert response.json() == {"status": "ok", "applied": False}
    assert (await balance(context, user_id))["balance"] == 0
    assert (
        await signed(
            context.client, "/webhooks/payments", {**payload, "status": "success"}
        )
    ).json()["applied"]


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "a" * 63,
        "g" * 64,
        "sha256=" + "a" * 64,
        "a" * 64,
        "payments-test-secret",
    ],
)
async def test_invalid_signature_precedes_json_and_database(context, header):
    class NoDatabase:
        async def run(self, *args, **kwargs):
            pytest.fail("Authentication must precede database access")

    context.app.state.transactions = NoDatabase()
    headers = {} if header is None else {"X-Signature": header}
    for path in ("/webhooks/payments", "/webhooks/providers/alpha"):
        response = await context.client.post(path, content=b"not JSON", headers=headers)
        assert (
            response.status_code == 401
            and response.json()["detail"]["code"] == "invalid_signature"
        )


async def test_hmac_checks_raw_bytes_and_rejects_duplicate_headers(context):
    user_id = await user(context, tokens=0)
    payload = {
        "payment_id": "море",
        "user_id": user_id,
        "tokens": 1,
        "status": "success",
    }
    raw = json.dumps(payload, ensure_ascii=False, indent=4).encode()
    headers = {
        "X-Signature": signature(raw).upper(),
        "Content-Type": "application/json",
    }
    assert (
        await context.client.post("/webhooks/payments", content=raw, headers=headers)
    ).status_code == 200
    changed = json.dumps(payload, separators=(",", ":")).encode()
    assert (
        await context.client.post(
            "/webhooks/payments", content=changed, headers=headers
        )
    ).status_code == 401
    duplicate = [("X-Signature", signature(raw)), ("X-Signature", signature(raw))]
    assert (
        await context.client.post("/webhooks/payments", content=raw, headers=duplicate)
    ).status_code == 401
    malformed = b"not JSON"
    response = await context.client.post(
        "/webhooks/payments",
        content=malformed,
        headers={"X-Signature": signature(malformed)},
    )
    assert response.status_code == 422 and "not JSON" not in response.text
    assert (await balance(context, user_id))["balance"] == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"tokens": True},
        {"tokens": 1.5},
        {"tokens": "1"},
        {"status": "other"},
        {"extra": 1},
    ],
)
async def test_signed_payment_validation_does_not_credit(context, overrides):
    user_id = await user(context, tokens=0)
    response = await signed(
        context.client,
        "/webhooks/payments",
        {
            "payment_id": "payment",
            "user_id": user_id,
            "tokens": 1,
            "status": "success",
            **overrides,
        },
    )
    assert (
        response.status_code == 422
        and (await balance(context, user_id))["balance"] == 0
    )


async def test_ten_last_leg_failures_refund_once_and_late_success_is_ignored(context):
    user_id = await user(context)
    saved = await task(
        context, await generate(context, user_id, ref_images=["ref"] * 5)
    )
    attempt = saved["attempts"][0]
    replies = await asyncio.gather(
        *(provider_event(context, attempt) for _ in range(10))
    )
    assert all(reply.status_code == 200 for reply in replies)
    assert sum(reply.json()["result"] == "failed_refunded" for reply in replies) == 1
    late = await provider_event(
        context,
        attempt,
        status="completed",
        error_type=None,
        result_url="https://files.test/late",
    )
    assert late.status_code == 200 and late.json()["result"] == "ignored"
    final = (await context.client.get("/tasks/" + saved["task_id"])).json()
    assert (
        final["status"] == "failed"
        and final["current_leg"] is None
        and final["result_url"] is None
    )
    money = await balance(context, user_id)
    assert (
        money["balance"] == 100
        and [entry["kind"] for entry in money["entries"]].count("generation_refund")
        == 1
    )


async def test_provider_fallback_and_success_via_signed_http(context):
    user_id = await user(context)
    saved = await task(context, await generate(context, user_id))
    first = saved["attempts"][0]
    assert (await provider_event(context, first)).json()[
        "result"
    ] == "next_attempt_created"
    queued = (await context.client.get("/tasks/" + saved["task_id"])).json()
    assert (
        queued["status"] == "queued"
        and queued["current_leg"]["provider_name"] == "beta"
    )
    assert queued["attempts"][1]["sent_at"] is None
    assert await context.app.state.dispatch.dispatch_next(context.app.state.http_client)
    running = (await context.client.get("/tasks/" + saved["task_id"])).json()
    response = await provider_event(
        context,
        running["attempts"][1],
        status="completed",
        error_type=None,
        result_url="https://files.test/result",
    )
    assert response.json()["result"] == "completed"
    final = (await context.client.get("/tasks/" + saved["task_id"])).json()
    assert (
        final["status"] == "completed"
        and final["result_url"] == "https://files.test/result"
    )
    assert [item["provider_name"] for item in final["attempts"]] == ["alpha", "beta"]
    assert [item["status"] for item in final["attempts"]] == ["failed", "completed"]
    assert (await balance(context, user_id))["balance"] == 98


@pytest.mark.parametrize("failure", ["503", "timeout"])
async def test_generate_dispatch_failure_continues_on_next_provider(context, failure):
    original_respond = context.respond

    async def respond(request):
        if request.url.host == "alpha":
            if failure == "timeout":
                raise httpx.ReadTimeout("Provider did not answer", request=request)
            return httpx.Response(503)
        return await original_respond(request)

    context.respond = respond
    user_id = await user(context)
    saved = await task(context, await generate(context, user_id))
    assert saved["status"] == "queued"
    assert [item["provider_name"] for item in saved["attempts"]] == ["alpha", "beta"]
    assert saved["attempts"][0]["status"] == "failed"
    assert saved["attempts"][0]["error_type"] == "technical"
    assert saved["current_leg"]["provider_name"] == "beta"
    assert await context.app.state.dispatch.dispatch_next(context.app.state.http_client)
    running = (await context.client.get("/tasks/" + saved["task_id"])).json()
    completed = await provider_event(
        context,
        running["attempts"][1],
        status="completed",
        error_type=None,
        result_url="https://files.test/fallback",
    )
    assert completed.status_code == 200 and completed.json()["result"] == "completed"
    money = await balance(context, user_id)
    assert money["balance"] == 98
    assert [entry["kind"] for entry in money["entries"]].count("generation_debit") == 1
    assert all(entry["kind"] != "generation_refund" for entry in money["entries"])


@pytest.mark.parametrize(
    "overrides",
    [
        {"status": "unknown"},
        {"status": "completed", "error_type": None},
        {"result_url": "unexpected"},
        {"error_type": None},
        {"error_type": "unknown"},
        {"external_id": " "},
        {"client_attempt_id": "bad"},
        {"extra": True},
    ],
)
async def test_signed_provider_validation_preserves_state(context, overrides):
    saved = await task(context, await generate(context))
    response = await provider_event(context, saved["attempts"][0], **overrides)
    assert response.status_code == 422
    assert (await context.client.get("/tasks/" + saved["task_id"])).json() == saved


async def test_provider_secret_and_correlation_are_checked_even_for_terminal_events(
    context,
):
    saved = await task(context, await generate(context))
    attempt = saved["attempts"][0]
    payload = event(attempt)
    wrong_secret = await signed(
        context.client, "/webhooks/providers/alpha", payload, "beta-test-secret"
    )
    assert wrong_secret.status_code == 401
    wrong_provider = await signed(
        context.client, "/webhooks/providers/beta", payload, "beta-test-secret"
    )
    assert wrong_provider.status_code == 409
    assert (
        await provider_event(context, attempt, external_id="wrong-id")
    ).status_code == 409
    assert (
        await provider_event(context, attempt, client_attempt_id=str(uuid4()))
    ).status_code == 404
    assert (
        await context.client.post("/webhooks/providers/unknown", content=b"bad")
    ).status_code == 404
    assert (await provider_event(context, attempt, error_type="payload")).json()[
        "result"
    ] == "failed_refunded"
    assert (
        await provider_event(context, attempt, external_id="wrong-id")
    ).status_code == 409


async def test_early_signed_webhook_can_complete_during_generate_http(context):
    async def respond(request):
        payload = json.loads(request.content)
        external_id = "early-" + payload["client_attempt_id"]
        response = await signed(
            context.client,
            "/webhooks/providers/alpha",
            {
                "client_attempt_id": payload["client_attempt_id"],
                "external_id": external_id,
                "status": "completed",
                "result_url": "https://files.test/early",
            },
            "alpha-test-secret",
        )
        assert response.status_code == 200
        return httpx.Response(202, json={"external_id": external_id})

    context.respond = respond
    saved = await task(context, await generate(context))
    assert (
        saved["status"] == "completed" and saved["attempts"][0]["status"] == "completed"
    )


async def test_watchdog_late_success_is_acknowledged_over_http(context):
    user_id = await user(context)
    saved = await task(
        context, await generate(context, user_id, ref_images=["ref"] * 5)
    )
    attempt = saved["attempts"][0]
    async with context.engine.begin() as connection:
        await connection.execute(
            sa.update(GenerationAttempt).values(
                sent_at=sa.func.now() - sa.text("interval '2 days'"),
                deadline_at=sa.func.now() - sa.text("interval '1 day'"),
            )
        )
    assert await WatchdogService(context.transactions).run_once()
    late = await provider_event(
        context,
        attempt,
        status="completed",
        error_type=None,
        result_url="https://files.test/late",
    )
    assert late.status_code == 200 and late.json()["result"] == "ignored"
    assert (await balance(context, user_id))["balance"] == 100


@pytest.mark.parametrize(
    "path",
    [
        "/tasks/bad",
        "/users/bad/balance",
        "/users/9223372036854775808/balance",
        "/users/1/balance?history_limit=0",
        "/users/1/balance?history_limit=101",
    ],
)
async def test_invalid_paths_and_limits(context, path):
    response = await context.client.get(path)
    assert (
        response.status_code == 422
        and response.json()["detail"]["code"] == "invalid_request"
    )


@pytest.mark.parametrize(
    "path,status,code",
    [("/unknown", 404, "not_found"), ("/generate", 405, "method_not_allowed")],
)
async def test_routing_errors_use_the_api_error_contract(context, path, status, code):
    response = await context.client.get(path)
    assert response.status_code == status
    assert response.json()["detail"]["code"] == code
    if status == 405:
        assert "POST" in response.headers["allow"]


async def test_unknown_entities_and_limited_history(context):
    assert (await context.client.get(f"/tasks/{uuid4()}")).status_code == 404
    assert (
        await context.client.get("/users/9223372036854775807/balance")
    ).status_code == 404
    user_id = await user(context)
    await generate(context, user_id)
    money = await balance(context, user_id, history_limit=1)
    assert money["balance"] == 98 and len(money["entries"]) == 1


async def test_dispatch_failure_after_commit_returns_created_task_id(
    context, monkeypatch
):
    async def unavailable(*args):
        raise DatabaseUnavailable("offline")

    monkeypatch.setattr(context.app.state.dispatch, "dispatch", unavailable)
    user_id = await user(context)
    saved = await task(context, await generate(context, user_id))
    assert saved["status"] == "queued" and saved["attempts"][0]["status"] == "pending"
    assert (await balance(context, user_id))["balance"] == 98


async def test_unknown_creation_commit_returns_503_without_automatic_replay(context):
    original = context.transactions
    calls = []

    class LostCommit:
        async def run(self, operation, **kwargs):
            result = await original.run(operation, **kwargs)
            calls.append(result.task_id)
            raise DatabaseUnavailable("Lost commit response")

    user_id = await user(context)
    context.app.state.transactions = LostCommit()
    response = await generate(context, user_id)
    assert response.status_code == 503 and len(calls) == 1
    context.app.state.transactions = original
    saved = (await context.client.get(f"/tasks/{calls[0]}")).json()
    assert saved["status"] == "queued" and not context.calls
    assert (await balance(context, user_id))["balance"] == 98


@pytest.mark.parametrize(
    "path,method",
    [
        ("/generate", "generate"),
        ("/webhooks/payments", "payment"),
        ("/webhooks/providers/alpha", "provider"),
        ("/tasks/{id}", "task"),
        ("/users/{id}/balance", "balance"),
    ],
)
async def test_database_failure_returns_503_without_leaking_diagnostics(
    context, path, method
):
    user_id = await user(context)
    saved = await task(context, await generate(context, user_id))

    class Offline:
        async def run(self, *args, **kwargs):
            raise DatabaseUnavailable("postgresql://private:password@host/db")

    context.app.state.transactions = Offline()
    if method == "generate":
        response = await generate(context, user_id)
    elif method == "payment":
        response = await signed(
            context.client,
            path,
            {"payment_id": "pay", "user_id": user_id, "tokens": 1, "status": "success"},
        )
    elif method == "provider":
        response = await provider_event(context, saved["attempts"][0])
    else:
        response = await context.client.get(
            path.replace("{id}", saved["task_id"] if method == "task" else str(user_id))
        )
    assert response.status_code == 503
    assert (
        response.json()["detail"]["code"] == "database_unavailable"
        and "password" not in response.text
    )


async def test_integrity_failure_is_500_and_not_a_generation_refund(
    context, monkeypatch
):
    async def broken(*args):
        raise LedgerIntegrityError("private database diagnosis")

    monkeypatch.setattr(FinancialService, "credit_payment", broken)
    response = await signed(
        context.client,
        "/webhooks/payments",
        {"payment_id": "pay", "user_id": 1, "tokens": 1, "status": "success"},
    )
    assert (
        response.status_code == 500
        and response.json()["detail"]["code"] == "integrity_error"
    )
    assert "private" not in response.text


async def test_task_and_history_share_repeatable_read_snapshot(context):
    saved = await task(context, await generate(context))
    first = saved["attempts"][0]
    original = context.transactions

    class ConcurrentWriter:
        async def run(self, operation, **kwargs):
            async def run(session):
                class Interleaved:
                    calls = 0

                    async def execute(self, query):
                        result = await session.execute(query)
                        self.calls += 1
                        if self.calls == 1:
                            await AttemptService(original).finish_attempt(
                                FinishCommand(
                                    attempt_id=first["attempt_id"],
                                    external_id=first["external_id"],
                                    provider_name="alpha",
                                    source="provider_webhook",
                                    outcome="failed",
                                    error_type="technical",
                                )
                            )
                        return result

                return await operation(Interleaved())

            return await original.run(run, **kwargs)

    context.app.state.transactions = ConcurrentWriter()
    response = await context.client.get("/tasks/" + saved["task_id"])
    assert response.status_code == 200 and response.json() == saved
    context.app.state.transactions = original
    current = (await context.client.get("/tasks/" + saved["task_id"])).json()
    assert current["status"] == "queued" and len(current["attempts"]) == 2


async def test_openapi_describes_all_routes_and_signed_bodies(context):
    response = await context.client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    assert {
        "/generate",
        "/tasks/{task_id}",
        "/users/{user_id}/balance",
        "/webhooks/payments",
        "/webhooks/providers/{name}",
    } <= paths.keys()
    for path in ("/webhooks/payments", "/webhooks/providers/{name}"):
        operation = paths[path]["post"]
        assert operation["requestBody"]["required"]
        assert any(
            item["name"] == "X-Signature" and item["required"]
            for item in operation["parameters"]
        )
        assert operation["responses"]["422"]["content"]["application/json"]["schema"][
            "$ref"
        ].endswith("/ErrorResponse")
