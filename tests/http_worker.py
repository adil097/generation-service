"""Real API processes with test-only pauses at transaction boundaries."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
import os

import uvicorn


def serve_api(sock, values, checkpoint_pipe):
    # main creates a default app at import time; each child configures its own env.
    for name, value in values.items():
        os.environ[name.upper()] = str(value)
    from generation_service.config import Settings
    from generation_service.main import create_app

    checkpoint = ContextVar("http_test_checkpoint", default=None)

    async def pause(result):
        checkpoint_pipe.send((os.getpid(), checkpoint.get(), result))
        # The parent kills the process here. No rollback, finally or graceful exit.
        await asyncio.Event().wait()

    class PausingTransactions:
        def __init__(self, original):
            self.original = original

        async def run(self, operation, *, read_only=False):
            async def execute(session):
                result = await operation(session)
                if not read_only and checkpoint.get() == "before_commit":
                    await pause(result)
                return result

            result = await self.original.run(execute, read_only=read_only)
            if not read_only and checkpoint.get() == "after_commit":
                await pause(result)
            return result

    app = create_app(Settings(_env_file=None, **values))
    lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def instrumented_lifespan(application):
        async with lifespan(application):
            # Background executors retain the real runner. Only this HTTP request
            # pauses; the other three processes continue normal recovery work.
            application.state.transactions = PausingTransactions(
                application.state.transactions
            )
            yield

    app.router.lifespan_context = instrumented_lifespan

    async def instrumented(scope, receive, send):
        if scope["type"] != "http":
            return await app(scope, receive, send)
        headers = dict(scope["headers"])
        token = checkpoint.set(headers.get(b"x-test-checkpoint", b"").decode())

        async def tagged_send(message):
            if message["type"] == "http.response.start":
                message["headers"] = list(message["headers"]) + [
                    (b"x-test-process", str(os.getpid()).encode())
                ]
            await send(message)

        try:
            await app(scope, receive, tagged_send)
        finally:
            checkpoint.reset(token)

    server = uvicorn.Server(
        uvicorn.Config(instrumented, log_level="error", access_log=False)
    )
    server.run(sockets=[sock])
