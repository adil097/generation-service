"""Atomically reserve tokens and persist the first provider attempt."""

from dataclasses import dataclass
from uuid import UUID, uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    TypeAdapter,
    field_validator,
)
from sqlalchemy import func, insert, select

from generation_service.config import CallbackBaseUrl
from generation_service.db.locking import Transaction, lock_user
from generation_service.db.models import (
    GenerationAttempt,
    GenerationModel,
    GenerationTask,
    ModelLeg,
)
from generation_service.db.transactions import TransactionRunner
from generation_service.services.errors import (
    ActiveTaskLimit,
    ModelNotFound,
    UnsupportedReferences,
)
from generation_service.services.finance import (
    BIGINT_MAX,
    debit_generation,
    ensure_funds,
)

ACTIVE_STATUSES = ("queued", "processing")
MAX_ACTIVE_TASKS = 3


class GenerationRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    user_id: StrictInt = Field(ge=-(2**63), le=BIGINT_MAX)
    model_slug: StrictStr
    prompt: StrictStr
    ref_images: tuple[StrictStr, ...] = ()

    @field_validator("ref_images", mode="before")
    @classmethod
    def references_are_sequence(cls, value):
        if not isinstance(value, (list, tuple)):
            raise ValueError("ref_images must be an array of strings")
        return value

    @field_validator("model_slug", "prompt")
    @classmethod
    def nonempty_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Text must not be empty")
        return value

    @field_validator("ref_images")
    @classmethod
    def nonempty_references(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not reference.strip() for reference in value):
            raise ValueError("Reference images must not be empty")
        return value


@dataclass(frozen=True)
class CreatedGeneration:
    task_id: UUID
    attempt_id: UUID
    leg_id: int
    provider_name: str
    price_tokens: int


def build_attempt_payload(
    *,
    attempt_id: UUID,
    model_slug: str,
    prompt: str,
    ref_images: tuple[str, ...],
    provider_name: str,
    callback_base_url: str,
) -> dict:
    return {
        "client_attempt_id": str(attempt_id),
        "model_slug": model_slug,
        "prompt": prompt,
        "ref_images": list(ref_images),
        "callback_url": f"{callback_base_url.rstrip('/')}/webhooks/providers/{provider_name}",
    }


async def _create_generation(
    transaction: Transaction,
    request: GenerationRequest,
    callback_base_url: str,
) -> CreatedGeneration:
    model = (
        await transaction.execute(
            select(GenerationModel.id, GenerationModel.price_tokens).where(
                GenerationModel.slug == request.model_slug
            )
        )
    ).one_or_none()
    if model is None:
        raise ModelNotFound(request.model_slug)
    leg = (
        await transaction.execute(
            select(ModelLeg.id, ModelLeg.provider_name)
            .where(
                ModelLeg.model_id == model.id,
                ModelLeg.max_ref_images >= len(request.ref_images),
            )
            .order_by(ModelLeg.priority)
            .limit(1)
        )
    ).one_or_none()
    if leg is None:
        raise UnsupportedReferences(request.model_slug)

    await lock_user(transaction, request.user_id)
    await ensure_funds(transaction, request.user_id, model.price_tokens)
    active = await transaction.scalar(
        select(func.count())
        .select_from(GenerationTask)
        .where(
            GenerationTask.user_id == request.user_id,
            GenerationTask.status.in_(ACTIVE_STATUSES),
        )
    )
    if active >= MAX_ACTIVE_TASKS:
        raise ActiveTaskLimit(request.user_id)

    task_id, attempt_id = uuid4(), uuid4()
    await transaction.execute(
        insert(GenerationTask).values(
            id=task_id,
            user_id=request.user_id,
            model_id=model.id,
            price_tokens=model.price_tokens,
            prompt=request.prompt,
            ref_images=list(request.ref_images),
            status="queued",
        )
    )
    await debit_generation(transaction, task_id)
    await transaction.execute(
        insert(GenerationAttempt).values(
            id=attempt_id,
            task_id=task_id,
            model_id=model.id,
            leg_id=leg.id,
            provider_name=leg.provider_name,
            attempt_number=1,
            status="pending",
            request_payload=build_attempt_payload(
                attempt_id=attempt_id,
                model_slug=request.model_slug,
                prompt=request.prompt,
                ref_images=request.ref_images,
                provider_name=leg.provider_name,
                callback_base_url=callback_base_url,
            ),
        )
    )
    return CreatedGeneration(
        task_id=task_id,
        attempt_id=attempt_id,
        leg_id=leg.id,
        provider_name=leg.provider_name,
        price_tokens=model.price_tokens,
    )


class GenerationService:
    def __init__(
        self,
        transactions: TransactionRunner,
        callback_base_url: str = "http://api:8000",
    ) -> None:
        self.transactions = transactions
        self.callback_base_url = str(
            TypeAdapter(CallbackBaseUrl).validate_python(callback_base_url)
        )

    async def create(self, request: GenerationRequest) -> CreatedGeneration:
        # The request is immutable, including its reference list, across transaction retries.
        return await self.transactions.run(
            lambda session: _create_generation(session, request, self.callback_base_url)
        )
