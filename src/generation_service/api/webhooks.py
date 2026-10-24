"""Authenticate the exact request bytes before parsing or touching persistent state."""

import hashlib
import hmac
import re
from typing import Literal
from uuid import UUID

from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, StrictStr, ValidationError

from generation_service.api.errors import api_error, validation_detail


class ProviderEvent(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "description": "completed requires a nonempty result_url and no error fields; failed requires error_type and no result_url."
        },
    )
    client_attempt_id: UUID
    external_id: StrictStr
    status: Literal["completed", "failed"]
    result_url: StrictStr | None = None
    error_type: Literal["technical", "moderation", "payload"] | None = None
    error_details: StrictStr | None = None


async def signed_model(request: Request, secret: str, model: type[BaseModel]):
    body = await request.body()
    signatures = request.headers.getlist("x-signature")
    if len(signatures) != 1 or re.fullmatch(r"[0-9a-fA-F]{64}", signatures[0]) is None:
        raise api_error(
            401, "invalid_signature", "Missing or invalid webhook signature"
        )
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signatures[0].lower()):
        raise api_error(
            401, "invalid_signature", "Missing or invalid webhook signature"
        )
    try:
        return model.model_validate_json(body)
    except ValidationError as error:
        raise HTTPException(
            422, detail=validation_detail(error.errors(), prefix=("body",))
        ) from error


def webhook_docs(model: type[BaseModel]) -> dict:
    return {
        "parameters": [
            {
                "in": "header",
                "name": "X-Signature",
                "required": True,
                "schema": {"type": "string", "pattern": "^[0-9a-fA-F]{64}$"},
                "description": "Hex HMAC-SHA256 of the raw request body",
            }
        ],
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": model.model_json_schema()}},
        },
    }
