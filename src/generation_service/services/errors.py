class UserNotFound(LookupError):
    pass


class TaskNotFound(LookupError):
    pass


class AttemptNotFound(LookupError):
    pass


class InvalidAttemptEvent(ValueError):
    """The event cannot be correlated with the specified provider attempt."""


class ModelNotFound(LookupError):
    pass


class UnsupportedReferences(ValueError):
    """No leg of the requested model accepts this many reference images."""


class ActiveTaskLimit(ValueError):
    """The user already has three queued or processing tasks."""


class PaymentConflict(ValueError):
    """A payment identifier was reused with different financial data."""


class InsufficientFunds(ValueError):
    pass


class InvalidTaskState(ValueError):
    pass


class LedgerIntegrityError(RuntimeError):
    """Existing financial history contradicts the operation being requested."""
