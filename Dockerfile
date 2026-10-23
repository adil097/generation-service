# syntax=docker/dockerfile:1
FROM python:3.12-slim AS builder

ENV UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
WORKDIR /app

RUN python -m pip install --no-cache-dir --disable-pip-version-check uv==0.13.5
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable && uv pip check

FROM builder AS test
RUN uv sync --locked --no-editable && uv pip check
COPY alembic.ini ./
COPY migrations ./migrations
COPY tests ./tests
CMD ["/app/.venv/bin/python", "-m", "pytest"]

FROM python:3.12-slim AS runtime
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --no-create-home app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --chown=app:app alembic.ini ./
COPY --chown=app:app migrations ./migrations
USER app
EXPOSE 8000
CMD ["uvicorn", "generation_service.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
