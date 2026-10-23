# Сервис генерации

Реализация [тестового задания](TASK.md) по [плану](PLAN.md)
и [архитектуре](docs/architecture.md). Сейчас подготовлен каркас;
бизнес-операции и миграции будут добавляться следующими этапами.

Используется Python 3.12. Зависимости описаны в `pyproject.toml`, точные
версии и хеши зафиксированы в `uv.lock`. Для управления окружением используется
[uv](https://docs.astral.sh/uv/).

## Локальная разработка

Скопируйте `.env.example` в `.env` и настройте адрес PostgreSQL и секреты.
`.env` исключён из Git. Примеры секретов предназначены для локального запуска.

```powershell
Copy-Item .env.example .env
uv sync --locked
uv run --locked uvicorn generation_service.main:app --host 0.0.0.0 --port 8000 --workers 4
```

Проверка процесса: `GET http://localhost:8000/health/live`.
OpenAPI: `http://localhost:8000/docs`.

```powershell
uv run --locked pytest
```

Настройки читаются из окружения и `.env`; окружение имеет приоритет.
`DATABASE_URL` должен использовать драйвер `postgresql+asyncpg`.
Секреты обязательны. Интервалы задаются в секундах и проверяются при запуске:
`HTTP_TIMEOUT < DISPATCH_LEASE < PROVIDER_TIMEOUT`.

Для обновления зависимостей после изменения `pyproject.toml` выполните
`uv lock`, проверьте изменения `uv.lock` и повторите тесты.
