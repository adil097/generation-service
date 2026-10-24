import logging
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Path, Query, Request
from pydantic import BaseModel, ValidationError

from generation_service.api.errors import api_error, error_responses, validation_detail
from generation_service.api.webhooks import ProviderEvent, signed_model, webhook_docs
from generation_service.services.attempts import AttemptService, FinishCommand
from generation_service.services.finance import (
    BIGINT_MAX,
    BalanceSnapshot,
    FinancialService,
    PaymentEvent,
)
from generation_service.services.generation import GenerationRequest, GenerationService
from generation_service.services.tasks import TaskService, TaskSnapshot

logger = logging.getLogger(__name__)
router = APIRouter(responses=error_responses(422, 500, 503))


class CreatedTask(BaseModel):
    task_id: UUID


class PaymentAcknowledgement(BaseModel):
    status: Literal["ok"] = "ok"
    applied: bool


class ProviderAcknowledgement(BaseModel):
    status: Literal["ok"] = "ok"
    result: Literal["completed", "next_attempt_created", "failed_refunded", "ignored"]


@router.post(
    "/generate",
    status_code=202,
    response_model=CreatedTask,
    tags=["tasks"],
    responses=error_responses(400, 402, 404, 429),
)
async def generate(payload: GenerationRequest, request: Request):
    state = request.app.state
    result = await GenerationService(
        state.transactions, str(state.settings.callback_base_url)
    ).create(payload)
    try:
        await state.dispatch.dispatch(result.attempt_id, state.http_client)
    except Exception:
        # Creation has committed. Return its ID even when dispatch cannot save a result;
        # persisted pending work is recovered independently of this HTTP request.
        logger.exception(
            "Created task will continue in background task_id=%s", result.task_id
        )
    return CreatedTask(task_id=result.task_id)


@router.post(
    "/webhooks/payments",
    response_model=PaymentAcknowledgement,
    tags=["webhooks"],
    openapi_extra=webhook_docs(PaymentEvent),
    responses=error_responses(401, 404, 409),
)
async def payment_webhook(request: Request):
    state = request.app.state
    event = await signed_model(
        request, state.settings.payments_webhook_secret.get_secret_value(), PaymentEvent
    )
    result = await FinancialService(state.transactions).credit_payment(event)
    return PaymentAcknowledgement(applied=result.applied)


@router.post(
    "/webhooks/providers/{name}",
    response_model=ProviderAcknowledgement,
    tags=["webhooks"],
    openapi_extra=webhook_docs(ProviderEvent),
    responses=error_responses(401, 404, 409),
)
async def provider_webhook(name: str, request: Request):
    state = request.app.state
    if name not in {"alpha", "beta"}:
        raise api_error(404, "provider_not_found", "Provider not found")
    secret = (
        state.settings.alpha_webhook_secret
        if name == "alpha"
        else state.settings.beta_webhook_secret
    )
    event = await signed_model(request, secret.get_secret_value(), ProviderEvent)
    try:
        command = FinishCommand(
            attempt_id=event.client_attempt_id,
            provider_name=name,
            external_id=event.external_id,
            source="provider_webhook",
            outcome=event.status,
            result_url=event.result_url,
            error_type=event.error_type,
            error_details=event.error_details,
        )
    except ValidationError as error:
        raise HTTPException(
            422, detail=validation_detail(error.errors(), prefix=("body",))
        ) from error
    result = await AttemptService(state.transactions).finish_attempt(command)
    # The committed next pending is picked up by the independent dispatcher. Never
    # hold a provider's acknowledgement open while waiting for another provider.
    return ProviderAcknowledgement(result=result.status)


@router.get(
    "/tasks/{task_id}",
    response_model=TaskSnapshot,
    tags=["tasks"],
    responses=error_responses(404),
)
async def get_task(task_id: UUID, request: Request):
    return await TaskService(request.app.state.transactions).get(task_id)


@router.get(
    "/users/{user_id}/balance",
    response_model=BalanceSnapshot,
    tags=["users"],
    responses=error_responses(404),
)
async def get_balance(
    request: Request,
    user_id: Annotated[int, Path(ge=-(2**63), le=BIGINT_MAX)],
    history_limit: Annotated[int, Query(ge=1, le=100)] = 20,
):
    return await FinancialService(request.app.state.transactions).get_balance(
        user_id, history_limit
    )
