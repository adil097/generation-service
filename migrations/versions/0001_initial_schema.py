"""Create constrained generation and provider state

Revision ID: 0001
Revises:
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA fake_providers")
    op.create_table(
        "provider_tasks",
        sa.Column("provider_name", sa.Text(), nullable=False),
        sa.Column("client_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column(
            "request_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column("scenario", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), server_default="accepted", nullable=False),
        sa.Column("ready_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_url", sa.Text(), nullable=True),
        sa.Column("error_type", sa.Text(), nullable=True),
        sa.Column("webhook_body", sa.LargeBinary(), nullable=True),
        sa.Column("next_delivery_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivery_token", sa.Uuid(), nullable=True),
        sa.Column("delivery_lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "successful_deliveries", sa.Integer(), server_default="0", nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(status = 'accepted' AND finished_at IS NULL AND webhook_body IS NULL) OR (status IN ('completed', 'failed') AND finished_at IS NOT NULL AND finished_at >= created_at AND webhook_body IS NOT NULL)",
            name=op.f("ck_provider_tasks_finished"),
        ),
        sa.CheckConstraint(
            "(status = 'completed' AND result_url IS NOT NULL AND btrim(result_url) <> '' AND error_type IS NULL) OR (status = 'failed' AND result_url IS NULL AND error_type IS NOT NULL AND error_type IN ('technical', 'moderation', 'payload')) OR (status = 'accepted' AND result_url IS NULL AND error_type IS NULL)",
            name=op.f("ck_provider_tasks_outcome"),
        ),
        sa.CheckConstraint(
            "btrim(external_id) <> ''", name=op.f("ck_provider_tasks_external_id")
        ),
        sa.CheckConstraint(
            "jsonb_typeof(request_payload) = 'object' AND request_payload ? 'client_attempt_id' AND jsonb_typeof(request_payload -> 'client_attempt_id') = 'string' AND request_payload ->> 'client_attempt_id' = client_attempt_id::text",
            name=op.f("ck_provider_tasks_request_identity"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(scenario) = 'object'",
            name=op.f("ck_provider_tasks_scenario_object"),
        ),
        sa.CheckConstraint(
            "provider_name IN ('alpha', 'beta')",
            name=op.f("ck_provider_tasks_provider_name"),
        ),
        sa.CheckConstraint(
            "status IN ('accepted', 'completed', 'failed')",
            name=op.f("ck_provider_tasks_status"),
        ),
        sa.CheckConstraint(
            "(delivery_token IS NULL AND delivery_lease_until IS NULL) OR (delivery_token IS NOT NULL AND delivery_lease_until IS NOT NULL AND webhook_body IS NOT NULL AND delivered_at IS NULL)",
            name=op.f("ck_provider_tasks_delivery_lease"),
        ),
        sa.CheckConstraint(
            "delivered_at IS NULL OR (webhook_body IS NOT NULL AND delivered_at >= created_at)",
            name=op.f("ck_provider_tasks_delivered"),
        ),
        sa.CheckConstraint(
            "next_delivery_at IS NULL OR webhook_body IS NOT NULL",
            name=op.f("ck_provider_tasks_delivery_ready"),
        ),
        sa.CheckConstraint(
            "ready_at >= created_at", name=op.f("ck_provider_tasks_ready_time")
        ),
        sa.CheckConstraint(
            "successful_deliveries >= 0", name=op.f("ck_provider_tasks_delivery_count")
        ),
        sa.PrimaryKeyConstraint(
            "provider_name", "client_attempt_id", name=op.f("pk_provider_tasks")
        ),
        sa.UniqueConstraint(
            "provider_name", "external_id", name="uq_provider_task_external"
        ),
        schema="fake_providers",
    )
    op.create_index(
        "ix_provider_task_delivery",
        "provider_tasks",
        ["next_delivery_at"],
        unique=False,
        schema="fake_providers",
        postgresql_where=sa.text(
            "delivered_at IS NULL AND next_delivery_at IS NOT NULL"
        ),
    )
    op.create_index(
        "ix_provider_task_ready",
        "provider_tasks",
        ["ready_at"],
        unique=False,
        schema="fake_providers",
        postgresql_where=sa.text("status = 'accepted'"),
    )
    op.create_table(
        "generation_models",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("price_tokens", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "btrim(slug) <> ''", name=op.f("ck_generation_models_nonempty_slug")
        ),
        sa.CheckConstraint(
            "price_tokens > 0", name=op.f("ck_generation_models_positive_price")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_generation_models")),
        sa.UniqueConstraint("slug", name=op.f("uq_generation_models_slug")),
    )
    op.create_table(
        "providers",
        sa.Column("name", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("name", name=op.f("pk_providers")),
    )
    op.create_table(
        "users",
        sa.Column("id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_users")),
    )
    op.create_table(
        "generation_tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("model_id", sa.BigInteger(), nullable=False),
        sa.Column("price_tokens", sa.BigInteger(), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column(
            "ref_images", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column("status", sa.Text(), server_default="queued", nullable=False),
        sa.Column("result_url", sa.Text(), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(status = 'completed' AND result_url IS NOT NULL AND btrim(result_url) <> '') OR (status <> 'completed' AND result_url IS NULL)",
            name=op.f("ck_generation_tasks_result"),
        ),
        sa.CheckConstraint(
            "(status IN ('completed', 'failed') AND finished_at IS NOT NULL AND finished_at >= created_at) OR (status IN ('queued', 'processing') AND finished_at IS NULL)",
            name=op.f("ck_generation_tasks_finished"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(ref_images) = 'array'",
            name=op.f("ck_generation_tasks_refs_array"),
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'processing', 'completed', 'failed')",
            name=op.f("ck_generation_tasks_status"),
        ),
        sa.CheckConstraint(
            "price_tokens > 0", name=op.f("ck_generation_tasks_positive_price")
        ),
        sa.ForeignKeyConstraint(
            ["model_id"],
            ["generation_models.id"],
            name=op.f("fk_generation_tasks_model_id_generation_models"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_generation_tasks_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_generation_tasks")),
        sa.UniqueConstraint("id", "model_id", name="uq_generation_tasks_model"),
        sa.UniqueConstraint("id", "user_id", name="uq_generation_tasks_owner"),
    )
    op.create_index(
        "ix_generation_tasks_active_user",
        "generation_tasks",
        ["user_id"],
        unique=False,
        postgresql_where=sa.text("status IN ('queued', 'processing')"),
    )
    op.create_table(
        "model_legs",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("model_id", sa.BigInteger(), nullable=False),
        sa.Column("provider_name", sa.Text(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("max_ref_images", sa.Integer(), nullable=False),
        sa.Column(
            "skip_on_moderation",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "max_ref_images >= 0", name=op.f("ck_model_legs_nonnegative_refs")
        ),
        sa.CheckConstraint(
            "priority > 0", name=op.f("ck_model_legs_positive_priority")
        ),
        sa.ForeignKeyConstraint(
            ["model_id"],
            ["generation_models.id"],
            name=op.f("fk_model_legs_model_id_generation_models"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["provider_name"],
            ["providers.name"],
            name=op.f("fk_model_legs_provider_name_providers"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_model_legs")),
        sa.UniqueConstraint(
            "id", "model_id", "provider_name", name="uq_model_legs_identity"
        ),
        sa.UniqueConstraint(
            "model_id", "priority", name="uq_model_legs_model_priority"
        ),
    )
    op.create_table(
        "generation_attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("model_id", sa.BigInteger(), nullable=False),
        sa.Column("leg_id", sa.BigInteger(), nullable=False),
        sa.Column("provider_name", sa.Text(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column(
            "request_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column("status", sa.Text(), server_default="pending", nullable=False),
        sa.Column("error_type", sa.Text(), nullable=True),
        sa.Column("error_details", sa.Text(), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dispatch_token", sa.Uuid(), nullable=True),
        sa.Column("dispatch_lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(dispatch_token IS NULL AND dispatch_lease_until IS NULL) OR (dispatch_token IS NOT NULL AND dispatch_lease_until IS NOT NULL AND status = 'pending' AND sent_at IS NOT NULL AND dispatch_lease_until <= deadline_at)",
            name=op.f("ck_generation_attempts_dispatch_lease"),
        ),
        sa.CheckConstraint(
            "(status IN ('completed', 'failed', 'timed_out') AND finished_at IS NOT NULL AND finished_at >= created_at) OR (status IN ('pending', 'awaiting_result') AND finished_at IS NULL)",
            name=op.f("ck_generation_attempts_finished"),
        ),
        sa.CheckConstraint(
            "(status IN ('failed', 'timed_out') AND error_type IS NOT NULL AND error_type IN ('technical', 'moderation', 'payload')) OR (status IN ('pending', 'awaiting_result', 'completed') AND error_type IS NULL)",
            name=op.f("ck_generation_attempts_error"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(request_payload) = 'object' AND request_payload ? 'client_attempt_id' AND jsonb_typeof(request_payload -> 'client_attempt_id') = 'string' AND request_payload ->> 'client_attempt_id' = id::text",
            name=op.f("ck_generation_attempts_request_identity"),
        ),
        sa.CheckConstraint(
            "status <> 'awaiting_result' OR sent_at IS NOT NULL",
            name=op.f("ck_generation_attempts_awaiting_sent"),
        ),
        sa.CheckConstraint(
            "status <> 'timed_out' OR error_type = 'technical'",
            name=op.f("ck_generation_attempts_timeout_error"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'awaiting_result', 'completed', 'failed', 'timed_out')",
            name=op.f("ck_generation_attempts_status"),
        ),
        sa.CheckConstraint(
            "(sent_at IS NULL AND deadline_at IS NULL) OR (sent_at IS NOT NULL AND deadline_at IS NOT NULL AND deadline_at > sent_at)",
            name=op.f("ck_generation_attempts_deadline"),
        ),
        sa.CheckConstraint(
            "attempt_number > 0", name=op.f("ck_generation_attempts_positive_number")
        ),
        sa.ForeignKeyConstraint(
            ["leg_id", "model_id", "provider_name"],
            ["model_legs.id", "model_legs.model_id", "model_legs.provider_name"],
            name="fk_attempt_leg_model_provider",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["task_id", "model_id"],
            ["generation_tasks.id", "generation_tasks.model_id"],
            name="fk_attempt_task_model",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_generation_attempts")),
        sa.UniqueConstraint(
            "provider_name", "external_id", name="uq_attempt_external_id"
        ),
        sa.UniqueConstraint("task_id", "attempt_number", name="uq_attempt_task_number"),
        sa.UniqueConstraint("task_id", "leg_id", name="uq_attempt_task_leg"),
    )
    op.create_index(
        "ix_attempt_deadline",
        "generation_attempts",
        ["deadline_at"],
        unique=False,
        postgresql_where=sa.text(
            "status IN ('pending', 'awaiting_result') AND deadline_at IS NOT NULL"
        ),
    )
    op.create_index(
        "ix_attempt_pending",
        "generation_attempts",
        ["created_at", "id"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "uq_attempt_active_task",
        "generation_attempts",
        ["task_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'awaiting_result')"),
    )
    op.create_table(
        "ledger_entries",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("amount", sa.BigInteger(), nullable=False),
        sa.Column("payment_id", sa.Text(), nullable=True),
        sa.Column("task_id", sa.Uuid(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(kind = 'payment_credit' AND amount > 0 AND payment_id IS NOT NULL AND btrim(payment_id) <> '' AND task_id IS NULL) OR (kind = 'generation_debit' AND amount < 0 AND payment_id IS NULL AND task_id IS NOT NULL) OR (kind = 'generation_refund' AND amount > 0 AND payment_id IS NULL AND task_id IS NOT NULL)",
            name=op.f("ck_ledger_entries_operation"),
        ),
        sa.ForeignKeyConstraint(
            ["task_id", "user_id"],
            ["generation_tasks.id", "generation_tasks.user_id"],
            name="fk_ledger_task_owner",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_ledger_entries_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_ledger_entries")),
        sa.UniqueConstraint("payment_id", name=op.f("uq_ledger_entries_payment_id")),
        sa.UniqueConstraint("task_id", "kind", name="uq_ledger_task_kind"),
    )
    op.create_index(
        "ix_ledger_user_history",
        "ledger_entries",
        ["user_id", sa.literal_column("created_at DESC"), "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_ledger_user_history", table_name="ledger_entries")
    op.drop_table("ledger_entries")
    op.drop_index(
        "uq_attempt_active_task",
        table_name="generation_attempts",
        postgresql_where=sa.text("status IN ('pending', 'awaiting_result')"),
    )
    op.drop_index(
        "ix_attempt_pending",
        table_name="generation_attempts",
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.drop_index(
        "ix_attempt_deadline",
        table_name="generation_attempts",
        postgresql_where=sa.text(
            "status IN ('pending', 'awaiting_result') AND deadline_at IS NOT NULL"
        ),
    )
    op.drop_table("generation_attempts")
    op.drop_table("model_legs")
    op.drop_index(
        "ix_generation_tasks_active_user",
        table_name="generation_tasks",
        postgresql_where=sa.text("status IN ('queued', 'processing')"),
    )
    op.drop_table("generation_tasks")
    op.drop_table("users")
    op.drop_table("providers")
    op.drop_table("generation_models")
    op.drop_index(
        "ix_provider_task_ready",
        table_name="provider_tasks",
        schema="fake_providers",
        postgresql_where=sa.text("status = 'accepted'"),
    )
    op.drop_index(
        "ix_provider_task_delivery",
        table_name="provider_tasks",
        schema="fake_providers",
        postgresql_where=sa.text(
            "delivered_at IS NULL AND next_delivery_at IS NOT NULL"
        ),
    )
    op.drop_table("provider_tasks", schema="fake_providers")
    op.execute("DROP SCHEMA fake_providers")
