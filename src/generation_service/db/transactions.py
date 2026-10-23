"""One session per transaction, with retries only for known PostgreSQL aborts."""

import asyncio
from collections.abc import Awaitable, Callable
from random import uniform
from typing import TypeVar

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, TimeoutError as PoolTimeout
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

T = TypeVar("T")


class DatabaseUnavailable(RuntimeError):
    """Temporary database failure; the caller must not acknowledge a webhook."""


class TransactionRunner:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        lock_timeout_ms: int = 5000,
        attempts: int = 3,
    ) -> None:
        if type(lock_timeout_ms) is not int or lock_timeout_ms <= 0:
            raise ValueError("lock_timeout_ms must be a positive integer")
        if type(attempts) is not int or attempts < 1:
            raise ValueError("attempts must be a positive integer")
        self.sessions = sessions
        self.lock_timeout_ms = lock_timeout_ms
        self.attempts = attempts

    async def run(
        self,
        operation: Callable[[AsyncSession], Awaitable[T]],
        *,
        read_only: bool = False,
    ) -> T:
        for attempt in range(self.attempts):
            try:
                async with self.sessions() as session:
                    async with session.begin():
                        mode = (
                            "ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                            if read_only
                            else "ISOLATION LEVEL READ COMMITTED"
                        )
                        await session.execute(text(f"SET TRANSACTION {mode}"))
                        await session.execute(
                            text("SELECT set_config('lock_timeout', :timeout, true)"),
                            {"timeout": f"{self.lock_timeout_ms}ms"},
                        )
                        result = await operation(session)
                    # Never return before the commit has completed successfully.
                    return result
            except DBAPIError as error:
                code = getattr(error.orig, "sqlstate", None)
                if code in {"40P01", "40001"}:
                    if attempt + 1 == self.attempts:
                        raise DatabaseUnavailable(
                            "Transaction retry limit reached"
                        ) from error
                    await asyncio.sleep(0.01 * 2**attempt + uniform(0, 0.01))
                    continue
                if (
                    code == "55P03"
                    or error.connection_invalidated
                    or (
                        code
                        and (
                            code.startswith("08") or code in {"57P01", "57P02", "57P03"}
                        )
                    )
                ):
                    # A lost COMMIT response is ambiguous. Do not replay it automatically.
                    raise DatabaseUnavailable(
                        "Database operation is unavailable"
                    ) from error
                raise
            except (PoolTimeout, OSError, TimeoutError) as error:
                raise DatabaseUnavailable(
                    "Database connection is unavailable"
                ) from error
        raise AssertionError("Unreachable transaction state")
