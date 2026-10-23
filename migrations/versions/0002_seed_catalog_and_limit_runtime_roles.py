"""Seed the fixed routing catalog and protect immutable financial history.

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("INSERT INTO providers (name) VALUES ('alpha'), ('beta')")
    op.execute(
        "INSERT INTO generation_models (slug, price_tokens) VALUES ('image-fast', 2), ('video-long', 25)"
    )
    op.execute("""
        INSERT INTO model_legs (model_id, provider_name, priority, max_ref_images, skip_on_moderation)
        SELECT m.id, leg.provider_name, leg.priority, leg.max_ref_images, leg.skip_on_moderation
        FROM (VALUES
            ('image-fast', 'alpha', 1, 4, false),
            ('image-fast', 'beta', 2, 10, false),
            ('video-long', 'beta', 1, 2, false),
            ('video-long', 'alpha', 2, 1, true)
        ) AS leg(slug, provider_name, priority, max_ref_images, skip_on_moderation)
        JOIN generation_models m ON m.slug = leg.slug
        ORDER BY m.id, leg.priority
    """)
    op.execute("GRANT USAGE ON SCHEMA public TO generation_app")
    op.execute(
        "GRANT SELECT ON providers, generation_models, model_legs TO generation_app"
    )
    op.execute(
        "GRANT SELECT, INSERT ON users, generation_tasks, generation_attempts, ledger_entries TO generation_app"
    )
    # PostgreSQL requires UPDATE on at least one column for SELECT ... FOR UPDATE.
    op.execute("GRANT UPDATE (id) ON users TO generation_app")
    op.execute(
        "GRANT UPDATE (status, result_url, finished_at) ON generation_tasks TO generation_app"
    )
    op.execute("""
        GRANT UPDATE (external_id, status, error_type, error_details, sent_at, deadline_at,
                      dispatch_token, dispatch_lease_until, finished_at)
        ON generation_attempts TO generation_app
    """)
    op.execute("GRANT USAGE ON SCHEMA fake_providers TO generation_provider")
    op.execute(
        "GRANT SELECT, INSERT ON fake_providers.provider_tasks TO generation_provider"
    )
    op.execute("""
        GRANT UPDATE (status, finished_at, result_url, error_type, webhook_body,
                      next_delivery_at, delivery_token, delivery_lease_until,
                      delivered_at, successful_deliveries)
        ON fake_providers.provider_tasks TO generation_provider
    """)


def downgrade() -> None:
    # Never use CASCADE: an existing generation history must block catalog deletion.
    op.execute("DELETE FROM model_legs")
    op.execute("DELETE FROM generation_models")
    op.execute("DELETE FROM providers")
    # REVOKE table UPDATE does not revoke separately granted column privileges.
    op.execute("REVOKE UPDATE (id) ON users FROM generation_app")
    op.execute(
        "REVOKE UPDATE (status, result_url, finished_at) ON generation_tasks FROM generation_app"
    )
    op.execute("""
        REVOKE UPDATE (external_id, status, error_type, error_details, sent_at, deadline_at,
                       dispatch_token, dispatch_lease_until, finished_at)
        ON generation_attempts FROM generation_app
    """)
    op.execute("""
        REVOKE UPDATE (status, finished_at, result_url, error_type, webhook_body,
                       next_delivery_at, delivery_token, delivery_lease_until,
                       delivered_at, successful_deliveries)
        ON fake_providers.provider_tasks FROM generation_provider
    """)
    op.execute(
        "REVOKE ALL ON providers, generation_models, model_legs, users, generation_tasks, generation_attempts, ledger_entries FROM generation_app"
    )
    op.execute("REVOKE ALL ON fake_providers.provider_tasks FROM generation_provider")
    op.execute("REVOKE USAGE ON SCHEMA public FROM generation_app")
    op.execute("REVOKE USAGE ON SCHEMA fake_providers FROM generation_provider")
