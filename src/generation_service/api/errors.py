"""Stable API errors without database diagnostics or echoed webhook bodies."""

import logging

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException

from generation_service.db.transactions import DatabaseUnavailable
from generation_service.services import errors

logger = logging.getLogger(__name__)


class ValidationIssue(BaseModel):
    loc: list[str | int]
    type: str
    message: str


class ErrorDetail(BaseModel):
    code: str
    message: str
    errors: list[ValidationIssue] | None = None


class ErrorResponse(BaseModel):
    detail: ErrorDetail


def error_responses(*codes: int) -> dict:
    return {code: {"model": ErrorResponse} for code in codes}


ERRORS = {
    errors.UserNotFound: (404, "user_not_found", "User not found"),
    errors.ModelNotFound: (404, "model_not_found", "Model not found"),
    errors.TaskNotFound: (404, "task_not_found", "Task not found"),
    errors.AttemptNotFound: (404, "attempt_not_found", "Attempt not found"),
    errors.UnsupportedReferences: (
        400,
        "unsupported_references",
        "No provider accepts this many reference images",
    ),
    errors.InsufficientFunds: (402, "insufficient_funds", "Insufficient token balance"),
    errors.ActiveTaskLimit: (
        429,
        "active_task_limit",
        "Three tasks are already active",
    ),
    errors.PaymentConflict: (
        409,
        "payment_conflict",
        "Payment identifier has conflicting data",
    ),
    errors.InvalidAttemptEvent: (
        409,
        "attempt_conflict",
        "Event does not match the provider attempt",
    ),
    DatabaseUnavailable: (
        503,
        "database_unavailable",
        "Database temporarily unavailable",
    ),
    errors.LedgerIntegrityError: (
        500,
        "integrity_error",
        "Stored data is inconsistent",
    ),
    errors.InvalidTaskState: (500, "integrity_error", "Stored data is inconsistent"),
    SQLAlchemyError: (500, "internal_error", "Internal server error"),
}


def api_error(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status, detail={"code": code, "message": message})


def validation_detail(items: list, *, prefix: tuple[str, ...] = ()) -> dict:
    return {
        "code": "invalid_request",
        "message": "Request validation failed",
        "errors": [
            {
                "loc": list(prefix + tuple(item["loc"])),
                "type": item["type"],
                "message": item["msg"],
            }
            for item in items
        ],
    }


def install_error_handlers(app):
    async def domain_error(request: Request, error: Exception):
        status, code, message = next(
            value for kind, value in ERRORS.items() if isinstance(error, kind)
        )
        if status >= 500:
            logger.error(
                "API operation failed path=%s code=%s",
                request.url.path,
                code,
                exc_info=error,
            )
        return JSONResponse(
            status_code=status, content={"detail": {"code": code, "message": message}}
        )

    for kind in ERRORS:
        app.add_exception_handler(kind, domain_error)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, error: RequestValidationError):
        return JSONResponse(
            status_code=422, content={"detail": validation_detail(error.errors())}
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, error: StarletteHTTPException):
        detail = error.detail
        if not request.url.path.startswith("/health/") and not isinstance(detail, dict):
            detail = {
                "code": {404: "not_found", 405: "method_not_allowed"}.get(
                    error.status_code, "http_error"
                ),
                "message": str(detail),
            }
        return JSONResponse(
            status_code=error.status_code,
            content={"detail": detail},
            headers=error.headers,
        )

    @app.exception_handler(Exception)
    async def internal_error(request: Request, error: Exception):
        logger.error("Unhandled API failure path=%s", request.url.path, exc_info=error)
        return JSONResponse(
            status_code=500,
            content={
                "detail": {"code": "internal_error", "message": "Internal server error"}
            },
        )
