"""Durable fake generation and leased, at-least-once callback delivery."""

import asyncio
import hashlib
import hmac
import json
import logging
import random
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID, uuid4

import httpx
import sqlalchemy as sa

from fake_providers.config import ProviderSettings
from fake_providers.models import ProviderTask
from generation_service.db.transactions import TransactionRunner

logger = logging.getLogger(__name__)
tasks = ProviderTask.__table__


class RequestConflict(Exception):
    pass


class SynchronousFailure(Exception):
    pass


@dataclass(frozen=True)
class Acceptance:
    external_id: str
    response_delay: float = 0


@dataclass(frozen=True)
class Delivery:
    attempt_id: UUID
    token: UUID
    callback_url: str
    body: bytes
    remaining: int
    concurrent: bool


class ProviderService:
    def __init__(self, transactions: TransactionRunner, settings: ProviderSettings):
        self.transactions = transactions
        self.settings = settings

    def identity(self, attempt_id):
        return sa.and_(
            tasks.c.provider_name == self.settings.provider_name,
            tasks.c.client_attempt_id == attempt_id,
        )

    async def accept(self, payload: dict) -> Acceptance:
        attempt_id = UUID(payload["client_attempt_id"])
        settings = self.settings

        async def operation(session):
            # Serialize even absent keys; existing keys must precede synchronous failure.
            await session.execute(
                sa.text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"{settings.provider_name}:{attempt_id}"},
            )
            row = (
                (
                    await session.execute(
                        sa.select(tasks).where(self.identity(attempt_id))
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is not None:
                if row["request_payload"] != payload:
                    raise RequestConflict("Idempotency key has different JSON payload")
                return Acceptance(row["external_id"])
            if settings.sync_mode == "503":
                raise SynchronousFailure("Configured synchronous failure")
            scenario = {
                "failed": random.random() < settings.failure_rate
                and not settings.late_completed,
                "error_type": settings.failure_type,
                "delay": settings.late_delay
                if settings.late_completed
                else random.uniform(settings.delay_min, settings.delay_max),
                "deliveries": 2 if settings.duplicate_webhook else 1,
                "concurrent": settings.concurrent_duplicates,
                "no_webhook": settings.no_webhook,
                "response_delay": settings.response_delay
                if settings.sync_mode == "timeout"
                else 0,
            }
            external_id = f"{settings.provider_name}-{uuid4()}"
            now = await session.scalar(sa.select(sa.func.clock_timestamp()))
            await session.execute(
                sa.insert(tasks).values(
                    provider_name=settings.provider_name,
                    client_attempt_id=attempt_id,
                    external_id=external_id,
                    request_payload=payload,
                    scenario=scenario,
                    created_at=now,
                    ready_at=now + timedelta(seconds=scenario["delay"]),
                )
            )
            return Acceptance(external_id, scenario["response_delay"])

        return await self.transactions.run(operation)

    async def generate_one(self) -> bool:
        async def operation(session):
            row = (
                (
                    await session.execute(
                        sa.select(tasks)
                        .where(
                            tasks.c.provider_name == self.settings.provider_name,
                            tasks.c.status == "accepted",
                            tasks.c.ready_at <= sa.func.clock_timestamp(),
                        )
                        .order_by(tasks.c.ready_at)
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return False
            now = await session.scalar(sa.select(sa.func.clock_timestamp()))
            if row["ready_at"] > now:
                return False
            failed = row["scenario"]["failed"]
            status = "failed" if failed else "completed"
            result_url = (
                None
                if failed
                else f"https://results.example/{self.settings.provider_name}/{row['external_id']}.bin"
            )
            error_type = row["scenario"]["error_type"] if failed else None
            event = {
                "client_attempt_id": str(row["client_attempt_id"]),
                "external_id": row["external_id"],
                "status": status,
                **(
                    {"error_type": error_type} if failed else {"result_url": result_url}
                ),
            }
            body = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
            await session.execute(
                sa.update(tasks)
                .where(self.identity(row["client_attempt_id"]))
                .values(
                    status=status,
                    finished_at=max(now, row["created_at"]),
                    result_url=result_url,
                    error_type=error_type,
                    webhook_body=body,
                    next_delivery_at=None if row["scenario"]["no_webhook"] else now,
                )
            )
            return True

        return await self.transactions.run(operation)

    async def claim_delivery(self) -> Delivery | None:
        async def operation(session):
            row = (
                (
                    await session.execute(
                        sa.select(tasks)
                        .where(
                            tasks.c.provider_name == self.settings.provider_name,
                            tasks.c.delivered_at.is_(None),
                            tasks.c.next_delivery_at <= sa.func.clock_timestamp(),
                            sa.or_(
                                tasks.c.delivery_token.is_(None),
                                tasks.c.delivery_lease_until
                                <= sa.func.clock_timestamp(),
                            ),
                        )
                        .order_by(tasks.c.next_delivery_at)
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            now = await session.scalar(sa.select(sa.func.clock_timestamp()))
            if row["next_delivery_at"] > now or (
                row["delivery_token"] is not None and row["delivery_lease_until"] > now
            ):
                return None
            token = uuid4()
            await session.execute(
                sa.update(tasks)
                .where(self.identity(row["client_attempt_id"]))
                .values(
                    delivery_token=token,
                    delivery_lease_until=now
                    + timedelta(seconds=self.settings.delivery_lease),
                )
            )
            return Delivery(
                row["client_attempt_id"],
                token,
                row["request_payload"]["callback_url"],
                bytes(row["webhook_body"]),
                row["scenario"]["deliveries"] - row["successful_deliveries"],
                row["scenario"]["concurrent"],
            )

        return await self.transactions.run(operation)

    async def acknowledge(self, delivery: Delivery, successes: int) -> bool:
        if type(successes) is not int or not 0 <= successes <= delivery.remaining:
            raise ValueError("Invalid successful delivery count")

        async def operation(session):
            row = (
                (
                    await session.execute(
                        sa.select(tasks)
                        .where(self.identity(delivery.attempt_id))
                        .with_for_update()
                    )
                )
                .mappings()
                .one()
            )
            now = await session.scalar(sa.select(sa.func.clock_timestamp()))
            if (
                row["delivery_token"] != delivery.token
                or row["delivery_lease_until"] <= now
            ):
                return False
            count = row["successful_deliveries"] + successes
            done = count >= row["scenario"]["deliveries"]
            await session.execute(
                sa.update(tasks)
                .where(self.identity(delivery.attempt_id))
                .values(
                    successful_deliveries=count,
                    delivery_token=None,
                    delivery_lease_until=None,
                    delivered_at=max(now, row["created_at"]) if done else None,
                    next_delivery_at=None
                    if done
                    else now + timedelta(seconds=self.settings.retry_interval),
                )
            )
            return True

        return await self.transactions.run(operation)

    async def deliver_one(self, client: httpx.AsyncClient) -> bool:
        delivery = await self.claim_delivery()
        if delivery is None:
            return False
        signature = hmac.new(
            self.settings.provider_webhook_secret.get_secret_value().encode(),
            delivery.body,
            hashlib.sha256,
        ).hexdigest()

        async def send():
            try:
                async with asyncio.timeout(self.settings.webhook_timeout):
                    async with client.stream(
                        "POST",
                        delivery.callback_url,
                        content=delivery.body,
                        headers={
                            "Content-Type": "application/json",
                            "X-Signature": signature,
                        },
                        follow_redirects=False,
                        timeout=self.settings.webhook_timeout,
                    ) as response:
                        return int(200 <= response.status_code < 300)
            except (httpx.HTTPError, TimeoutError):
                return 0

        if delivery.concurrent:
            successes = sum(
                await asyncio.gather(*(send() for _ in range(delivery.remaining)))
            )
        else:
            # Bound the entire sequential pair below the delivery lease, too.
            successes = 0
            try:
                async with asyncio.timeout(self.settings.webhook_timeout):
                    for _ in range(delivery.remaining):
                        successes += await send()
            except TimeoutError:
                pass
        await self.acknowledge(delivery, successes)
        return True


async def run_worker(operation, interval: float):
    while True:
        try:
            if await operation():
                continue
        except Exception:
            logger.exception("Provider background operation failed; retrying")
        await asyncio.sleep(interval)
