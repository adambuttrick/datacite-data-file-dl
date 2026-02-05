"""DataCite API authentication for AWS credentials."""

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import boto3
import requests
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

API_URL = "https://api.datacite.org/credentials/datafile"

# Default AWS STS tokens last 1 hour (3600s)
DEFAULT_CREDENTIAL_LIFETIME_SECONDS = 3600

# Default refresh interval: refresh credentials after 20 minutes of use
# (buffer = 3600 - 1200 = 2400 seconds remaining)
DEFAULT_REFRESH_INTERVAL_SECONDS = 1200

# AWS error codes that indicate credential issues
CREDENTIAL_ERROR_CODES = frozenset({
    "ExpiredToken",
    "ExpiredTokenException",
    "InvalidToken",
    "InvalidIdentityToken",
    "AccessDenied",
    "InvalidAccessKeyId",
    "SignatureDoesNotMatch",
})


def is_credential_error(exc: BaseException) -> bool:
    """Check if an exception indicates an AWS credential problem."""
    if isinstance(exc, (NoCredentialsError, PartialCredentialsError)):
        return True

    if isinstance(exc, ClientError):
        error_code = exc.response.get("Error", {}).get("Code", "")
        return error_code in CREDENTIAL_ERROR_CODES

    return False


class AuthenticationError(Exception):
    """Raised when authentication fails."""

    pass


@dataclass
class AWSCredentials:
    """Temporary AWS credentials for S3 access."""

    access_key_id: str
    secret_access_key: str
    session_token: str
    fetched_at: float = field(default_factory=time.time)
    lifetime_seconds: int = DEFAULT_CREDENTIAL_LIFETIME_SECONDS

    def is_expiring_soon(self, buffer_seconds: int | None = None) -> bool:
        """Check if credentials will expire within the buffer period."""
        if buffer_seconds is None:
            buffer_seconds = self.lifetime_seconds - DEFAULT_REFRESH_INTERVAL_SECONDS
        age = time.time() - self.fetched_at
        return age >= (self.lifetime_seconds - buffer_seconds)

    def seconds_until_expiry(self) -> float:
        """Return seconds until credentials expire."""
        age = time.time() - self.fetched_at
        return max(0, self.lifetime_seconds - age)


def fetch_credentials(username: str, password: str) -> AWSCredentials:
    """Fetch temporary AWS credentials from DataCite API."""
    try:
        response = requests.get(
            API_URL,
            auth=(username, password),
            timeout=30,
        )
    except requests.RequestException as e:
        raise AuthenticationError(f"Network error: {e}") from e

    if response.status_code == 401:
        raise AuthenticationError("Invalid username or password")
    elif response.status_code == 403:
        raise AuthenticationError("Access denied. Check your account permissions.")
    elif response.status_code != 200:
        raise AuthenticationError(f"Unexpected response from DataCite API: {response.status_code}")

    try:
        data = response.json()
    except requests.JSONDecodeError as e:
        raise AuthenticationError(f"Invalid response from API: {e}") from e

    try:
        return AWSCredentials(
            access_key_id=data["access_key_id"],
            secret_access_key=data["secret_access_key"],
            session_token=data["session_token"],
        )
    except KeyError as e:
        available_keys = list(data.keys())
        raise AuthenticationError(f"Missing field {e}. Available fields: {available_keys}") from e


class CredentialManager:
    """Manages AWS credentials with automatic refresh before expiration.

    Thread-safe credential manager that proactively refreshes credentials
    before they expire and provides fresh S3 clients as needed.
    """

    def __init__(
        self,
        username: str,
        password: str,
        refresh_interval_seconds: int = DEFAULT_REFRESH_INTERVAL_SECONDS,
        credential_lifetime_seconds: int = DEFAULT_CREDENTIAL_LIFETIME_SECONDS,
    ):
        import threading

        self._username = username
        self._password = password
        self._refresh_interval = refresh_interval_seconds
        self._credential_lifetime = credential_lifetime_seconds
        self._refresh_buffer = credential_lifetime_seconds - refresh_interval_seconds
        self._credentials: AWSCredentials | None = None
        self._client: "S3Client | None" = None
        self._lock = threading.Lock()
        self._refresh_count = 0

    def _create_client(self, creds: AWSCredentials) -> "S3Client":
        return boto3.client(
            "s3",
            aws_access_key_id=creds.access_key_id,
            aws_secret_access_key=creds.secret_access_key,
            aws_session_token=creds.session_token,
        )

    def _needs_refresh(self) -> bool:
        if self._credentials is None:
            return True
        return self._credentials.is_expiring_soon(self._refresh_buffer)

    def _refresh_credentials(self) -> None:
        from .log import get_logger

        logger = get_logger()

        self._credentials = fetch_credentials(self._username, self._password)
        self._client = self._create_client(self._credentials)
        self._refresh_count += 1

        remaining = self._credentials.seconds_until_expiry()
        logger.info(
            f"Credentials refreshed (refresh #{self._refresh_count}). "
            f"Valid for {remaining / 60:.0f} minutes, "
            f"will refresh in {self._refresh_interval / 60:.0f} minutes."
        )

    def get_client(self) -> "S3Client":
        """Get an S3 client, refreshing credentials if needed. Thread-safe."""
        if not self._needs_refresh() and self._client is not None:
            return self._client

        # Slow path: acquire lock and refresh
        with self._lock:
            # Double-check after acquiring lock
            if self._needs_refresh() or self._client is None:
                self._refresh_credentials()
            return self._client  # type: ignore[return-value]

    def ensure_fresh(self) -> None:
        """Proactively refresh credentials if they're expiring soon."""
        if self._needs_refresh():
            with self._lock:
                if self._needs_refresh():
                    self._refresh_credentials()

    def force_refresh(self) -> "S3Client":
        """Force an immediate credential refresh. Thread-safe."""
        from .log import get_logger

        logger = get_logger()
        logger.info("Forcing credential refresh due to authentication error...")

        with self._lock:
            self._refresh_credentials()
            return self._client  # type: ignore[return-value]

    @property
    def refresh_count(self) -> int:
        """Number of times credentials have been refreshed."""
        return self._refresh_count

    @property
    def credentials(self) -> AWSCredentials | None:
        """Current credentials (may be None if not yet fetched)."""
        return self._credentials

    @property
    def refresh_interval(self) -> int:
        """Configured refresh interval in seconds."""
        return self._refresh_interval
