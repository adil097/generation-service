"""Provision database logins separately from schema migrations and application startup."""

import asyncio

from pydantic import Field, SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from generation_service.config import DatabaseSettings


class BootstrapSettings(DatabaseSettings):
    migration_database_password: SecretStr = Field(min_length=1)
    app_database_password: SecretStr = Field(min_length=1)
    provider_database_password: SecretStr = Field(min_length=1)


async def bootstrap(settings: BootstrapSettings) -> None:
    engine = create_async_engine(str(settings.database_url), hide_parameters=True)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SELECT pg_advisory_xact_lock(674001)"))
            for name, secret in (
                ("generation_migrator", settings.migration_database_password),
                ("generation_app", settings.app_database_password),
                ("generation_provider", settings.provider_database_password),
            ):
                # Passwords stay in bound parameters, outside source files and SQL logs.
                await connection.execute(
                    text(
                        "SELECT set_config('bootstrap.role_password', :password, true)"
                    ),
                    {"password": secret.get_secret_value()},
                )
                await connection.execute(
                    text(f"""
                    DO $bootstrap$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{name}') THEN
                            CREATE ROLE {name};
                        END IF;
                        EXECUTE format('ALTER ROLE {name} WITH LOGIN NOSUPERUSER NOCREATEDB '
                            'NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
                            current_setting('bootstrap.role_password'));
                    END $bootstrap$;
                """)
                )
                await connection.execute(
                    text(f"ALTER ROLE {name} SET timezone TO 'UTC'")
                )
            database = await connection.scalar(text("SELECT current_database()"))
            quoted = connection.dialect.identifier_preparer.quote_identifier(database)
            await connection.execute(
                text(
                    f"GRANT CONNECT, CREATE ON DATABASE {quoted} TO generation_migrator"
                )
            )
            await connection.execute(
                text(
                    f"GRANT CONNECT ON DATABASE {quoted} TO generation_app, generation_provider"
                )
            )
            await connection.execute(text("REVOKE CREATE ON SCHEMA public FROM PUBLIC"))
            await connection.execute(
                text(
                    "GRANT USAGE, CREATE ON SCHEMA public TO generation_migrator WITH GRANT OPTION"
                )
            )
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(bootstrap(BootstrapSettings()))
