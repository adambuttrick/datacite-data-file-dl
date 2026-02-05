"""Retry logic with exponential backoff."""

import time
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionError as BotoConnectionError,
)
from requests.exceptions import RequestException

from .log import get_logger

if TYPE_CHECKING:
    from .auth import CredentialManager

T = TypeVar("T")

# Default exceptions that are safe to retry
DEFAULT_RETRYABLE = (
    ConnectionError,
    TimeoutError,
    RequestException,
    BotoCoreError,
    BotoConnectionError,
    ClientError,  # Includes credential errors which we handle specially
)


class RetryExhausted(Exception):
    """Raised when all retry attempts have been exhausted."""

    def __init__(self, message: str, last_exception: Exception):
        super().__init__(message)
        self.last_exception = last_exception


class CredentialRefreshNeeded(Exception):
    """Raised when credentials need to be refreshed before retry."""

    pass


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple = DEFAULT_RETRYABLE,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator that retries a function with exponential backoff."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            logger = get_logger()
            last_exception: Exception | None = None
            attempts = max(max_retries, 1)  # At least 1 attempt

            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e

                    if attempt >= attempts:
                        break

                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)

                    logger.warning(
                        f"Attempt {attempt}/{attempts} failed for {func.__name__}: {e}. "
                        f"Waiting {delay:.1f}s..."
                    )
                    time.sleep(delay)
                except Exception:
                    raise

            assert last_exception is not None
            raise RetryExhausted(
                f"Failed after {attempts} attempts: {last_exception}",
                last_exception,
            )

        return wrapper

    return decorator


def retry_with_credential_refresh(
    func: Callable[..., T],
    credential_manager: "CredentialManager | None",
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple = DEFAULT_RETRYABLE,
    *args: Any,
    **kwargs: Any,
) -> T:
    """Execute a function with retry logic and credential refresh on auth errors.

    Unlike the decorator version, this can refresh credentials and pass a fresh
    client to subsequent retry attempts. If credential_manager is provided, func
    should accept 'client' as its first argument.
    """
    from .auth import is_credential_error

    logger = get_logger()
    last_exception: Exception | None = None
    attempts = max(max_retries, 1)
    credential_refreshed = False

    if credential_manager is not None:
        client = credential_manager.get_client()
    else:
        client = None

    for attempt in range(1, attempts + 1):
        try:
            if client is not None:
                return func(client, *args, **kwargs)
            else:
                return func(*args, **kwargs)

        except retryable_exceptions as e:
            last_exception = e

            if is_credential_error(e) and credential_manager is not None:
                if not credential_refreshed:
                    logger.warning(
                        f"Credential error on attempt {attempt}/{attempts}: {e}. "
                        f"Refreshing credentials..."
                    )
                    client = credential_manager.force_refresh()
                    credential_refreshed = True
                    continue
                else:
                    logger.warning(
                        f"Credential error persists after refresh on attempt {attempt}/{attempts}: {e}"
                    )

            if attempt >= attempts:
                break

            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)

            logger.warning(
                f"Attempt {attempt}/{attempts} failed: {e}. "
                f"Waiting {delay:.1f}s..."
            )
            time.sleep(delay)

        except Exception:
            raise

    assert last_exception is not None
    raise RetryExhausted(
        f"Failed after {attempts} attempts: {last_exception}",
        last_exception,
    )
