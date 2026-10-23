import pytest
from pydantic import ValidationError

from generation_service.config import Settings


def test_reads_dotenv_and_environment_override(tmp_path, monkeypatch, settings_values):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key.upper()}={value}" for key, value in settings_values.items())
        + "\nPROVIDER_TIMEOUT=80\nPOSTGRES_DB=ignored-compose-variable\n",
        encoding="utf-8",
    )
    for name in (*settings_values, "http_timeout", "dispatch_lease", "poll_interval"):
        monkeypatch.delenv(name.upper(), raising=False)
    monkeypatch.setenv("PROVIDER_TIMEOUT", "90")
    settings = Settings(_env_file=env_file)
    assert settings.provider_timeout == 90
    assert settings.alpha_webhook_secret.get_secret_value() == "alpha-test-secret"
    assert settings.database_url.path == "/test"


@pytest.mark.parametrize(
    "overrides",
    [
        {"http_timeout": 15},
        {"dispatch_lease": 60},
        {"provider_timeout": 10},
        {"poll_interval": 0},
        {"http_timeout": float("inf")},
        {"database_url": "postgresql://test:test@localhost/test"},
        {"database_url": "postgresql+asyncpg://test:test@localhost"},
        {"alpha_webhook_secret": ""},
    ],
)
def test_rejects_invalid_configuration(settings_values, overrides):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **(settings_values | overrides))


def test_secrets_are_masked(settings_values):
    settings = Settings(_env_file=None, **settings_values)
    assert "alpha-test-secret" not in repr(settings)
    assert "test:test" not in repr(settings)
