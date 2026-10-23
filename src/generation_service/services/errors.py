class UserNotFound(LookupError):
    pass


class TaskNotFound(LookupError):
    pass


class PaymentConflict(ValueError):
    """A payment identifier was reused with different financial data."""


class InsufficientFunds(ValueError):
    pass


class InvalidTaskState(ValueError):
    pass


class LedgerIntegrityError(RuntimeError):
    """Existing financial history contradicts the operation being requested."""
