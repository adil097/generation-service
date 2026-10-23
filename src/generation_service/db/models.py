"""Database contract. Cross-row business invariants are enforced by transactions."""

from datetime import datetime
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    metadata = sa.MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_name)s",
            "pk": "pk_%(table_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
        }
    )


class CreatedAt:
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )


class User(CreatedAt, Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(
        sa.BigInteger, primary_key=True, autoincrement=False
    )


class Provider(Base):
    __tablename__ = "providers"
    name: Mapped[str] = mapped_column(sa.Text, primary_key=True)


class GenerationModel(Base):
    __tablename__ = "generation_models"
    id: Mapped[int] = mapped_column(sa.BigInteger, sa.Identity(), primary_key=True)
    slug: Mapped[str] = mapped_column(sa.Text, unique=True)
    price_tokens: Mapped[int] = mapped_column(sa.BigInteger)
    __table_args__ = (
        sa.CheckConstraint("price_tokens > 0", name="positive_price"),
        sa.CheckConstraint("btrim(slug) <> ''", name="nonempty_slug"),
    )


class ModelLeg(Base):
    __tablename__ = "model_legs"
    id: Mapped[int] = mapped_column(sa.BigInteger, sa.Identity(), primary_key=True)
    model_id: Mapped[int] = mapped_column(
        sa.BigInteger, sa.ForeignKey("generation_models.id", ondelete="RESTRICT")
    )
    provider_name: Mapped[str] = mapped_column(
        sa.Text, sa.ForeignKey("providers.name", ondelete="RESTRICT")
    )
    priority: Mapped[int] = mapped_column(sa.Integer)
    max_ref_images: Mapped[int] = mapped_column(sa.Integer)
    skip_on_moderation: Mapped[bool] = mapped_column(
        sa.Boolean, server_default=sa.false()
    )
    __table_args__ = (
        sa.UniqueConstraint(
            "model_id", "priority", name="uq_model_legs_model_priority"
        ),
        sa.UniqueConstraint(
            "id", "model_id", "provider_name", name="uq_model_legs_identity"
        ),
        sa.CheckConstraint("priority > 0", name="positive_priority"),
        sa.CheckConstraint("max_ref_images >= 0", name="nonnegative_refs"),
    )


class GenerationTask(CreatedAt, Base):
    __tablename__ = "generation_tasks"
    id: Mapped[UUID] = mapped_column(sa.Uuid, primary_key=True, default=uuid4)
    user_id: Mapped[int] = mapped_column(
        sa.BigInteger, sa.ForeignKey("users.id", ondelete="RESTRICT")
    )
    model_id: Mapped[int] = mapped_column(
        sa.BigInteger, sa.ForeignKey("generation_models.id", ondelete="RESTRICT")
    )
    price_tokens: Mapped[int] = mapped_column(sa.BigInteger)
    prompt: Mapped[str] = mapped_column(sa.Text)
    ref_images: Mapped[list[str]] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(sa.Text, server_default="queued")
    result_url: Mapped[str | None] = mapped_column(sa.Text)
    finished_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    __table_args__ = (
        sa.UniqueConstraint("id", "model_id", name="uq_generation_tasks_model"),
        sa.UniqueConstraint("id", "user_id", name="uq_generation_tasks_owner"),
        sa.CheckConstraint("price_tokens > 0", name="positive_price"),
        sa.CheckConstraint("jsonb_typeof(ref_images) = 'array'", name="refs_array"),
        sa.CheckConstraint(
            "status IN ('queued', 'processing', 'completed', 'failed')", name="status"
        ),
        sa.CheckConstraint(
            "(status = 'completed' AND result_url IS NOT NULL AND btrim(result_url) <> '') "
            "OR (status <> 'completed' AND result_url IS NULL)",
            name="result",
        ),
        sa.CheckConstraint(
            "(status IN ('completed', 'failed') AND finished_at IS NOT NULL AND finished_at >= created_at) "
            "OR (status IN ('queued', 'processing') AND finished_at IS NULL)",
            name="finished",
        ),
        sa.Index(
            "ix_generation_tasks_active_user",
            "user_id",
            postgresql_where=sa.text("status IN ('queued', 'processing')"),
        ),
    )


class GenerationAttempt(CreatedAt, Base):
    __tablename__ = "generation_attempts"
    id: Mapped[UUID] = mapped_column(sa.Uuid, primary_key=True, default=uuid4)
    task_id: Mapped[UUID] = mapped_column(sa.Uuid)
    model_id: Mapped[int] = mapped_column(sa.BigInteger)
    leg_id: Mapped[int] = mapped_column(sa.BigInteger)
    provider_name: Mapped[str] = mapped_column(sa.Text)
    attempt_number: Mapped[int] = mapped_column(sa.Integer)
    external_id: Mapped[str | None] = mapped_column(sa.Text)
    request_payload: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(sa.Text, server_default="pending")
    error_type: Mapped[str | None] = mapped_column(sa.Text)
    error_details: Mapped[str | None] = mapped_column(sa.Text)
    sent_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    deadline_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    dispatch_token: Mapped[UUID | None] = mapped_column(sa.Uuid)
    dispatch_lease_until: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True)
    )
    finished_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    __table_args__ = (
        sa.ForeignKeyConstraint(
            ["task_id", "model_id"],
            ["generation_tasks.id", "generation_tasks.model_id"],
            ondelete="RESTRICT",
            name="fk_attempt_task_model",
        ),
        sa.ForeignKeyConstraint(
            ["leg_id", "model_id", "provider_name"],
            ["model_legs.id", "model_legs.model_id", "model_legs.provider_name"],
            ondelete="RESTRICT",
            name="fk_attempt_leg_model_provider",
        ),
        sa.UniqueConstraint("task_id", "attempt_number", name="uq_attempt_task_number"),
        sa.UniqueConstraint("task_id", "leg_id", name="uq_attempt_task_leg"),
        sa.UniqueConstraint(
            "provider_name", "external_id", name="uq_attempt_external_id"
        ),
        sa.CheckConstraint("attempt_number > 0", name="positive_number"),
        sa.CheckConstraint(
            "status IN ('pending', 'awaiting_result', 'completed', 'failed', 'timed_out')",
            name="status",
        ),
        sa.CheckConstraint(
            "(status IN ('failed', 'timed_out') AND error_type IS NOT NULL "
            "AND error_type IN ('technical', 'moderation', 'payload')) "
            "OR (status IN ('pending', 'awaiting_result', 'completed') AND error_type IS NULL)",
            name="error",
        ),
        sa.CheckConstraint(
            "status <> 'timed_out' OR error_type = 'technical'", name="timeout_error"
        ),
        sa.CheckConstraint(
            "jsonb_typeof(request_payload) = 'object' AND request_payload ? 'client_attempt_id' "
            "AND jsonb_typeof(request_payload -> 'client_attempt_id') = 'string' "
            "AND request_payload ->> 'client_attempt_id' = id::text",
            name="request_identity",
        ),
        sa.CheckConstraint(
            "(sent_at IS NULL AND deadline_at IS NULL) OR "
            "(sent_at IS NOT NULL AND deadline_at IS NOT NULL AND deadline_at > sent_at)",
            name="deadline",
        ),
        sa.CheckConstraint(
            "status <> 'awaiting_result' OR sent_at IS NOT NULL", name="awaiting_sent"
        ),
        sa.CheckConstraint(
            "(dispatch_token IS NULL AND dispatch_lease_until IS NULL) OR "
            "(dispatch_token IS NOT NULL AND dispatch_lease_until IS NOT NULL "
            "AND status = 'pending' AND sent_at IS NOT NULL AND dispatch_lease_until <= deadline_at)",
            name="dispatch_lease",
        ),
        sa.CheckConstraint(
            "(status IN ('completed', 'failed', 'timed_out') AND finished_at IS NOT NULL AND finished_at >= created_at) "
            "OR (status IN ('pending', 'awaiting_result') AND finished_at IS NULL)",
            name="finished",
        ),
        sa.Index(
            "uq_attempt_active_task",
            "task_id",
            unique=True,
            postgresql_where=sa.text("status IN ('pending', 'awaiting_result')"),
        ),
        sa.Index(
            "ix_attempt_deadline",
            "deadline_at",
            postgresql_where=sa.text(
                "status IN ('pending', 'awaiting_result') AND deadline_at IS NOT NULL"
            ),
        ),
        sa.Index(
            "ix_attempt_pending",
            "created_at",
            "id",
            postgresql_where=sa.text("status = 'pending'"),
        ),
    )


class LedgerEntry(CreatedAt, Base):
    __tablename__ = "ledger_entries"
    id: Mapped[UUID] = mapped_column(sa.Uuid, primary_key=True, default=uuid4)
    user_id: Mapped[int] = mapped_column(
        sa.BigInteger, sa.ForeignKey("users.id", ondelete="RESTRICT")
    )
    kind: Mapped[str] = mapped_column(sa.Text)
    amount: Mapped[int] = mapped_column(sa.BigInteger)
    payment_id: Mapped[str | None] = mapped_column(sa.Text, unique=True)
    task_id: Mapped[UUID | None] = mapped_column(sa.Uuid)
    __table_args__ = (
        sa.ForeignKeyConstraint(
            ["task_id", "user_id"],
            ["generation_tasks.id", "generation_tasks.user_id"],
            ondelete="RESTRICT",
            name="fk_ledger_task_owner",
        ),
        sa.UniqueConstraint("task_id", "kind", name="uq_ledger_task_kind"),
        sa.CheckConstraint(
            "(kind = 'payment_credit' AND amount > 0 AND payment_id IS NOT NULL "
            "AND btrim(payment_id) <> '' AND task_id IS NULL) OR "
            "(kind = 'generation_debit' AND amount < 0 AND payment_id IS NULL AND task_id IS NOT NULL) OR "
            "(kind = 'generation_refund' AND amount > 0 AND payment_id IS NULL AND task_id IS NOT NULL)",
            name="operation",
        ),
        sa.Index("ix_ledger_user_history", "user_id", sa.text("created_at DESC"), "id"),
    )
