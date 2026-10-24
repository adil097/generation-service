"""Recoverable dispatch: commit a lease, call HTTP, then conditionally apply its result."""

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, tuple_, update

import httpx

from generation_service.config import Settings
from generation_service.db.locking import lock_user
from generation_service.db.models import GenerationAttempt, GenerationTask
from generation_service.db.transactions import TransactionRunner
from generation_service.services.attempts import (
    ACTIVE_ATTEMPTS,
    AttemptService,
    FinishCommand,
)
from generation_service.services.errors import AttemptNotFound, InvalidAttemptEvent
from generation_service.services.generation import ACTIVE_STATUSES

logger = logging.getLogger(__name__)
attempts = GenerationAttempt.__table__
tasks = GenerationTask.__table__
BATCH_SIZE = 100
MAX_RESPONSE_BYTES = 65536


@dataclass(frozen=True)
class DispatchClaim:
    attempt_id: UUID
    provider_name: str
    token: UUID
    request_payload: dict
    deadline_at: datetime


async def _lock_attempt(session, attempt_id: UUID, skip_locked: bool):
    owner = (
        await session.execute(
            select(tasks.c.user_id, tasks.c.id)
            .join(attempts, attempts.c.task_id == tasks.c.id)
            .where(attempts.c.id == attempt_id)
        )
    ).one_or_none()
    if owner is None:
        raise AttemptNotFound(attempt_id)
    if not await lock_user(session, owner.user_id, skip_locked=skip_locked):
        return None
    task = (
        (
            await session.execute(
                select(tasks)
                .where(tasks.c.id == owner.id)
                .with_for_update(skip_locked=skip_locked)
            )
        )
        .mappings()
        .one_or_none()
    )
    if task is None:
        return None
    attempt = (
        (
            await session.execute(
                select(attempts)
                .where(attempts.c.id == attempt_id)
                .with_for_update(skip_locked=skip_locked)
            )
        )
        .mappings()
        .one_or_none()
    )
    return (task, attempt) if attempt is not None else None


class DispatchService:
    def __init__(self, transactions: TransactionRunner, settings: Settings):
        self.transactions = transactions
        self.settings = settings
        self.attempts = AttemptService(transactions)

    async def claim(
        self, attempt_id: UUID, *, skip_locked: bool = True
    ) -> DispatchClaim | None:
        async def operation(session):
            locked = await _lock_attempt(session, attempt_id, skip_locked)
            if locked is None:
                return None
            task, attempt = locked
            now = await session.scalar(select(func.clock_timestamp()))
            if (
                task["status"] not in ACTIVE_STATUSES
                or attempt["status"] != "pending"
                or (
                    attempt["deadline_at"] is not None and attempt["deadline_at"] <= now
                )
                or (
                    attempt["dispatch_token"] is not None
                    and attempt["dispatch_lease_until"] > now
                )
            ):
                return None
            sent_at = attempt["sent_at"] if attempt["sent_at"] is not None else now
            deadline = (
                attempt["deadline_at"]
                if attempt["deadline_at"] is not None
                else sent_at + timedelta(seconds=self.settings.provider_timeout)
            )
            token = uuid4()
            await session.execute(
                update(attempts)
                .where(attempts.c.id == attempt_id)
                .values(
                    sent_at=sent_at,
                    deadline_at=deadline,
                    dispatch_token=token,
                    dispatch_lease_until=min(
                        now + timedelta(seconds=self.settings.dispatch_lease), deadline
                    ),
                )
            )
            await session.execute(
                update(tasks)
                .where(tasks.c.id == task["id"])
                .values(status="processing")
            )
            return DispatchClaim(
                attempt_id,
                attempt["provider_name"],
                token,
                attempt["request_payload"],
                deadline,
            )

        return await self.transactions.run(operation)

    async def claim_next(self) -> DispatchClaim | None:
        after = None
        while True:

            async def candidates(session):
                query = (
                    select(attempts.c.id, attempts.c.created_at)
                    .join(tasks, tasks.c.id == attempts.c.task_id)
                    .where(
                        tasks.c.status.in_(ACTIVE_STATUSES),
                        attempts.c.status == "pending",
                        or_(
                            attempts.c.deadline_at.is_(None),
                            attempts.c.deadline_at > func.clock_timestamp(),
                        ),
                        or_(
                            attempts.c.dispatch_token.is_(None),
                            attempts.c.dispatch_lease_until <= func.clock_timestamp(),
                        ),
                    )
                )
                if after is not None:
                    query = query.where(
                        tuple_(attempts.c.created_at, attempts.c.id) > after
                    )
                return (
                    await session.execute(
                        query.order_by(attempts.c.created_at, attempts.c.id).limit(
                            BATCH_SIZE
                        )
                    )
                ).all()

            batch = await self.transactions.run(candidates, read_only=True)
            for candidate in batch:
                claim = await self.claim(candidate.id)
                if claim is not None:
                    return claim
            if len(batch) < BATCH_SIZE:
                return None
            after = (batch[-1].created_at, batch[-1].id)

    async def _http_budget(self, claim: DispatchClaim) -> float:
        async def operation(session):
            row = (
                await session.execute(
                    select(
                        attempts.c.dispatch_lease_until,
                        func.clock_timestamp().label("observed_at"),
                    )
                    .join(tasks, tasks.c.id == attempts.c.task_id)
                    .where(
                        attempts.c.id == claim.attempt_id,
                        attempts.c.status == "pending",
                        attempts.c.dispatch_token == claim.token,
                        tasks.c.status.in_(ACTIVE_STATUSES),
                    )
                )
            ).one_or_none()
            if row is None:
                return 0
            return max(
                0,
                min(
                    self.settings.http_timeout,
                    (row.dispatch_lease_until - row.observed_at).total_seconds(),
                ),
            )

        return await self.transactions.run(operation, read_only=True)

    async def save_accepted(self, claim: DispatchClaim, external_id: str) -> bool:
        if not isinstance(external_id, str) or not external_id.strip():
            raise InvalidAttemptEvent("Provider returned an empty external identifier")

        async def operation(session):
            task, attempt = await _lock_attempt(session, claim.attempt_id, False)
            now = await session.scalar(select(func.clock_timestamp()))
            if (
                task["status"] not in ACTIVE_STATUSES
                or attempt["status"] != "pending"
                or attempt["dispatch_token"] != claim.token
                or attempt["dispatch_lease_until"] is None
                or attempt["dispatch_lease_until"] <= now
            ):
                return False
            if (
                attempt["external_id"] is not None
                and attempt["external_id"] != external_id
            ):
                raise InvalidAttemptEvent(
                    "External identifier does not match the attempt"
                )
            other = await session.scalar(
                select(attempts.c.id).where(
                    attempts.c.provider_name == attempt["provider_name"],
                    attempts.c.external_id == external_id,
                    attempts.c.id != claim.attempt_id,
                )
            )
            if other is not None:
                raise InvalidAttemptEvent(
                    "External identifier belongs to another attempt"
                )
            await session.execute(
                update(attempts)
                .where(attempts.c.id == claim.attempt_id)
                .values(
                    external_id=external_id,
                    status="awaiting_result",
                    dispatch_token=None,
                    dispatch_lease_until=None,
                )
            )
            return True

        return await self.transactions.run(operation)

    async def _technical_failure(self, claim: DispatchClaim, details: str):
        return await self.attempts.finish_attempt(
            FinishCommand(
                attempt_id=claim.attempt_id,
                provider_name=claim.provider_name,
                dispatch_token=claim.token,
                source="dispatch_error",
                outcome="failed",
                error_type="technical",
                error_details=details,
            )
        )

    async def send(self, claim: DispatchClaim, client: httpx.AsyncClient) -> str:
        budget = await self._http_budget(claim)
        if budget <= 0:
            return "ignored"
        base_url = (
            self.settings.alpha_url
            if claim.provider_name == "alpha"
            else self.settings.beta_url
        )
        url = str(base_url).rstrip("/") + "/tasks"
        try:
            # The outer bound covers pool acquisition, all phases and the entire body.
            async with asyncio.timeout(budget):
                async with client.stream(
                    "POST",
                    url,
                    json=claim.request_payload,
                    timeout=budget,
                    follow_redirects=False,
                ) as response:
                    status = response.status_code
                    body = bytearray()
                    if status == 202:
                        async for chunk in response.aiter_bytes():
                            body.extend(chunk)
                            if len(body) > MAX_RESPONSE_BYTES:
                                break
        except (httpx.HTTPError, TimeoutError) as error:
            # No automatic HTTP retry. Database errors below are outside this block.
            result = await self._technical_failure(
                claim, f"Provider HTTP failure: {type(error).__name__}"
            )
            return result.status
        if 500 <= status < 600:
            return (
                await self._technical_failure(claim, f"Provider returned HTTP {status}")
            ).status
        if status == 202 and len(body) <= MAX_RESPONSE_BYTES:
            try:
                data = json.loads(body)
                if not isinstance(data, dict):
                    raise ValueError("Expected a JSON object")
                external_id = data.get("external_id")
                if not isinstance(external_id, str) or not external_id.strip():
                    raise ValueError("Expected a nonempty external_id")
            except (ValueError, UnicodeDecodeError):
                pass
            else:
                try:
                    return (
                        "accepted"
                        if await self.save_accepted(claim, external_id)
                        else "ignored"
                    )
                except InvalidAttemptEvent:
                    logger.exception(
                        "Provider returned conflicting external_id attempt_id=%s",
                        claim.attempt_id,
                    )
                    return "protocol_error"
        # 409 and other malformed/unexpected responses never become payload failures.
        # Keep the same key and the original deadline for recovery or the watchdog.
        logger.error(
            "Provider protocol error attempt_id=%s provider=%s http_status=%s",
            claim.attempt_id,
            claim.provider_name,
            status,
        )
        return "protocol_error"

    async def dispatch(self, attempt_id: UUID, client: httpx.AsyncClient) -> str:
        claim = await self.claim(attempt_id)
        return await self.send(claim, client) if claim is not None else "ignored"

    async def dispatch_next(self, client: httpx.AsyncClient) -> bool:
        claim = await self.claim_next()
        if claim is None:
            return False
        await self.send(claim, client)
        return True


class WatchdogService:
    def __init__(self, transactions: TransactionRunner):
        self.transactions = transactions
        self.attempts = AttemptService(transactions)

    async def run_once(self) -> bool:
        after = None
        applied = False
        while True:

            async def candidates(session):
                query = (
                    select(
                        attempts.c.id, attempts.c.provider_name, attempts.c.deadline_at
                    )
                    .join(tasks, tasks.c.id == attempts.c.task_id)
                    .where(
                        tasks.c.status.in_(ACTIVE_STATUSES),
                        attempts.c.status.in_(ACTIVE_ATTEMPTS),
                        attempts.c.deadline_at <= func.clock_timestamp(),
                    )
                )
                if after is not None:
                    query = query.where(
                        tuple_(attempts.c.deadline_at, attempts.c.id) > after
                    )
                return (
                    await session.execute(
                        query.order_by(attempts.c.deadline_at, attempts.c.id).limit(
                            BATCH_SIZE
                        )
                    )
                ).all()

            batch = await self.transactions.run(candidates, read_only=True)
            for candidate in batch:
                result = await self.attempts.finish_attempt(
                    FinishCommand(
                        attempt_id=candidate.id,
                        provider_name=candidate.provider_name,
                        source="watchdog",
                        outcome="timeout",
                    ),
                    skip_locked=True,
                )
                applied |= result.status not in {"ignored", "deferred"}
            if len(batch) < BATCH_SIZE:
                return applied
            after = (batch[-1].deadline_at, batch[-1].id)
