"""Persistent provider state, isolated from the generation service's tables."""

from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from generation_service.db.models import Base, CreatedAt


class ProviderTask(CreatedAt, Base):
    __tablename__ = "provider_tasks"
    provider_name: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    client_attempt_id: Mapped[UUID] = mapped_column(sa.Uuid, primary_key=True)
    external_id: Mapped[str] = mapped_column(sa.Text)
    request_payload: Mapped[dict] = mapped_column(JSONB)
    scenario: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(sa.Text, server_default="accepted")
    ready_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    result_url: Mapped[str | None] = mapped_column(sa.Text)
    error_type: Mapped[str | None] = mapped_column(sa.Text)
    webhook_body: Mapped[bytes | None] = mapped_column(sa.LargeBinary)
    next_delivery_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True)
    )
    delivery_token: Mapped[UUID | None] = mapped_column(sa.Uuid)
    delivery_lease_until: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True)
    )
    delivered_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    successful_deliveries: Mapped[int] = mapped_column(sa.Integer, server_default="0")
    __table_args__ = (
        sa.UniqueConstraint(
            "provider_name", "external_id", name="uq_provider_task_external"
        ),
        sa.CheckConstraint("provider_name IN ('alpha', 'beta')", name="provider_name"),
        sa.CheckConstraint("btrim(external_id) <> ''", name="external_id"),
        sa.CheckConstraint("jsonb_typeof(scenario) = 'object'", name="scenario_object"),
        sa.CheckConstraint(
            "jsonb_typeof(request_payload) = 'object' AND request_payload ? 'client_attempt_id' "
            "AND jsonb_typeof(request_payload -> 'client_attempt_id') = 'string' "
            "AND request_payload ->> 'client_attempt_id' = client_attempt_id::text",
            name="request_identity",
        ),
        sa.CheckConstraint(
            "status IN ('accepted', 'completed', 'failed')", name="status"
        ),
        sa.CheckConstraint("ready_at >= created_at", name="ready_time"),
        sa.CheckConstraint(
            "(status = 'accepted' AND finished_at IS NULL AND webhook_body IS NULL) OR "
            "(status IN ('completed', 'failed') AND finished_at IS NOT NULL "
            "AND finished_at >= created_at AND webhook_body IS NOT NULL)",
            name="finished",
        ),
        sa.CheckConstraint(
            "(status = 'completed' AND result_url IS NOT NULL AND btrim(result_url) <> '' AND error_type IS NULL) OR "
            "(status = 'failed' AND result_url IS NULL AND error_type IS NOT NULL "
            "AND error_type IN ('technical', 'moderation', 'payload')) OR "
            "(status = 'accepted' AND result_url IS NULL AND error_type IS NULL)",
            name="outcome",
        ),
        sa.CheckConstraint(
            "(delivery_token IS NULL AND delivery_lease_until IS NULL) OR "
            "(delivery_token IS NOT NULL AND delivery_lease_until IS NOT NULL "
            "AND webhook_body IS NOT NULL AND delivered_at IS NULL)",
            name="delivery_lease",
        ),
        sa.CheckConstraint(
            "next_delivery_at IS NULL OR webhook_body IS NOT NULL",
            name="delivery_ready",
        ),
        sa.CheckConstraint(
            "delivered_at IS NULL OR (webhook_body IS NOT NULL AND delivered_at >= created_at)",
            name="delivered",
        ),
        sa.CheckConstraint("successful_deliveries >= 0", name="delivery_count"),
        sa.Index(
            "ix_provider_task_ready",
            "ready_at",
            postgresql_where=sa.text("status = 'accepted'"),
        ),
        sa.Index(
            "ix_provider_task_delivery",
            "next_delivery_at",
            postgresql_where=sa.text(
                "delivered_at IS NULL AND next_delivery_at IS NOT NULL"
            ),
        ),
        {"schema": "fake_providers"},
    )
