import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine

from generation_service.config import DatabaseSettings
from generation_service.db.models import Base
from fake_providers.models import ProviderTask  # noqa: F401

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)
target_metadata = Base.metadata


def include_name(name, type_, parent_names):
    if type_ == "schema":
        return name in (None, "fake_providers")
    return True


def configure(connection=None, url=None):
    context.configure(
        connection=connection,
        url=url,
        target_metadata=target_metadata,
        include_schemas=True,
        include_name=include_name,
        compare_type=True,
        compare_server_default=True,
        literal_binds=connection is None,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


async def online():
    engine = create_async_engine(
        str(DatabaseSettings().database_url), poolclass=pool.NullPool
    )
    try:
        async with engine.connect() as connection:
            await connection.run_sync(configure)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    configure(url=str(DatabaseSettings().database_url))
else:
    asyncio.run(online())
