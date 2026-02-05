"""Tests for authentication and credential management."""

import time
import threading
from unittest.mock import patch, MagicMock

import pytest

from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

from datacite_data_file_dl.auth import (
    AWSCredentials,
    CredentialManager,
    fetch_credentials,
    AuthenticationError,
    is_credential_error,
    DEFAULT_REFRESH_INTERVAL_SECONDS,
    DEFAULT_CREDENTIAL_LIFETIME_SECONDS,
)


class TestAWSCredentials:
    """Tests for AWSCredentials dataclass."""

    def test_fetched_at_defaults_to_now(self):
        """Should set fetched_at to current time by default."""
        before = time.time()
        creds = AWSCredentials(
            access_key_id="key",
            secret_access_key="secret",
            session_token="token",
        )
        after = time.time()

        assert before <= creds.fetched_at <= after

    def test_is_expiring_soon_false_when_fresh(self):
        """Should not be expiring when just created."""
        creds = AWSCredentials(
            access_key_id="key",
            secret_access_key="secret",
            session_token="token",
        )

        assert not creds.is_expiring_soon()

    def test_is_expiring_soon_true_when_old(self):
        """Should be expiring when past the buffer threshold."""
        # Credentials are considered expiring when age >= (lifetime - buffer)
        # With default refresh interval of 1200s, buffer = 3600 - 1200 = 2400s
        default_buffer = DEFAULT_CREDENTIAL_LIFETIME_SECONDS - DEFAULT_REFRESH_INTERVAL_SECONDS
        old_time = time.time() - (DEFAULT_CREDENTIAL_LIFETIME_SECONDS - default_buffer + 10)
        creds = AWSCredentials(
            access_key_id="key",
            secret_access_key="secret",
            session_token="token",
            fetched_at=old_time,
        )

        assert creds.is_expiring_soon()

    def test_seconds_until_expiry(self):
        """Should calculate remaining time correctly."""
        creds = AWSCredentials(
            access_key_id="key",
            secret_access_key="secret",
            session_token="token",
        )

        remaining = creds.seconds_until_expiry()
        # Should be close to the full lifetime
        assert remaining > DEFAULT_CREDENTIAL_LIFETIME_SECONDS - 5
        assert remaining <= DEFAULT_CREDENTIAL_LIFETIME_SECONDS

    def test_seconds_until_expiry_never_negative(self):
        """Should return 0 when expired, not negative."""
        old_time = time.time() - (DEFAULT_CREDENTIAL_LIFETIME_SECONDS + 100)
        creds = AWSCredentials(
            access_key_id="key",
            secret_access_key="secret",
            session_token="token",
            fetched_at=old_time,
        )

        assert creds.seconds_until_expiry() == 0


class TestCredentialManager:
    """Tests for CredentialManager."""

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_initial_fetch(self, mock_boto_client, mock_fetch):
        """Should fetch credentials on first get_client call."""
        mock_fetch.return_value = AWSCredentials(
            access_key_id="key1",
            secret_access_key="secret1",
            session_token="token1",
        )
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        manager = CredentialManager("user", "pass")
        client = manager.get_client()

        mock_fetch.assert_called_once_with("user", "pass")
        mock_boto_client.assert_called_once()
        assert client == mock_s3_client
        assert manager.refresh_count == 1

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_reuses_client_when_fresh(self, mock_boto_client, mock_fetch):
        """Should reuse the same client when credentials are fresh."""
        mock_fetch.return_value = AWSCredentials(
            access_key_id="key1",
            secret_access_key="secret1",
            session_token="token1",
        )
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        manager = CredentialManager("user", "pass")
        client1 = manager.get_client()
        client2 = manager.get_client()
        client3 = manager.get_client()

        assert mock_fetch.call_count == 1
        assert client1 == client2 == client3
        assert manager.refresh_count == 1

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_refreshes_when_expiring(self, mock_boto_client, mock_fetch):
        """Should refresh credentials when they're about to expire."""
        # First call returns soon-to-expire credentials
        # With default 20-min interval, buffer = 2400s, so 100s left triggers refresh
        old_creds = AWSCredentials(
            access_key_id="key1",
            secret_access_key="secret1",
            session_token="token1",
            fetched_at=time.time() - (DEFAULT_CREDENTIAL_LIFETIME_SECONDS - 100),  # 100s left
        )
        new_creds = AWSCredentials(
            access_key_id="key2",
            secret_access_key="secret2",
            session_token="token2",
        )
        mock_fetch.side_effect = [old_creds, new_creds]

        mock_client1 = MagicMock()
        mock_client2 = MagicMock()
        mock_boto_client.side_effect = [mock_client1, mock_client2]

        manager = CredentialManager("user", "pass")
        client1 = manager.get_client()

        assert manager.refresh_count >= 1

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_thread_safety(self, mock_boto_client, mock_fetch):
        """Should handle concurrent access safely."""
        call_count = 0

        def mock_fetch_delayed(*args):
            nonlocal call_count
            call_count += 1
            time.sleep(0.1)  # Simulate network delay
            return AWSCredentials(
                access_key_id=f"key{call_count}",
                secret_access_key=f"secret{call_count}",
                session_token=f"token{call_count}",
            )

        mock_fetch.side_effect = mock_fetch_delayed
        mock_boto_client.return_value = MagicMock()

        manager = CredentialManager("user", "pass")
        results = []
        errors = []

        def get_client_thread():
            try:
                client = manager.get_client()
                results.append(client)
            except Exception as e:
                errors.append(e)

        # Start multiple threads simultaneously
        threads = [threading.Thread(target=get_client_thread) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 10

        assert call_count == 1
        assert manager.refresh_count == 1

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_ensure_fresh_proactive(self, mock_boto_client, mock_fetch):
        """ensure_fresh should proactively refresh if needed."""
        old_creds = AWSCredentials(
            access_key_id="key1",
            secret_access_key="secret1",
            session_token="token1",
            fetched_at=time.time() - (DEFAULT_CREDENTIAL_LIFETIME_SECONDS - 100),
        )
        new_creds = AWSCredentials(
            access_key_id="key2",
            secret_access_key="secret2",
            session_token="token2",
        )
        mock_fetch.side_effect = [old_creds, new_creds]
        mock_boto_client.return_value = MagicMock()

        manager = CredentialManager("user", "pass")
        manager.get_client()

        manager.ensure_fresh()

        assert mock_fetch.call_count >= 1

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    def test_propagates_auth_error(self, mock_fetch):
        """Should propagate authentication errors."""
        mock_fetch.side_effect = AuthenticationError("Invalid password")

        manager = CredentialManager("user", "wrong_pass")

        with pytest.raises(AuthenticationError) as exc_info:
            manager.get_client()

        assert "Invalid password" in str(exc_info.value)

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_credentials_property(self, mock_boto_client, mock_fetch):
        """Should expose current credentials via property."""
        expected_creds = AWSCredentials(
            access_key_id="key1",
            secret_access_key="secret1",
            session_token="token1",
        )
        mock_fetch.return_value = expected_creds
        mock_boto_client.return_value = MagicMock()

        manager = CredentialManager("user", "pass")

        assert manager.credentials is None

        manager.get_client()

        assert manager.credentials == expected_creds

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_custom_refresh_interval(self, mock_boto_client, mock_fetch):
        """Should respect custom refresh interval."""
        mock_fetch.return_value = AWSCredentials(
            access_key_id="key1",
            secret_access_key="secret1",
            session_token="token1",
        )
        mock_boto_client.return_value = MagicMock()

        # Create manager with 30-minute refresh interval (1800 seconds)
        manager = CredentialManager("user", "pass", refresh_interval_seconds=1800)

        assert manager.refresh_interval == 1800
        assert manager._refresh_buffer == 1800

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_custom_refresh_interval_affects_refresh(self, mock_boto_client, mock_fetch):
        """Custom refresh interval should change when credentials refresh."""
        # With a 50-minute (3000s) interval, buffer = 3600 - 3000 = 600s
        # Credentials with 700s remaining should NOT trigger refresh
        fresh_creds = AWSCredentials(
            access_key_id="key1",
            secret_access_key="secret1",
            session_token="token1",
            fetched_at=time.time() - (DEFAULT_CREDENTIAL_LIFETIME_SECONDS - 700),  # 700s left
        )
        mock_fetch.return_value = fresh_creds
        mock_boto_client.return_value = MagicMock()

        manager = CredentialManager("user", "pass", refresh_interval_seconds=3000)
        manager.get_client()

        assert manager.refresh_count == 1

        manager.get_client()
        assert manager.refresh_count == 1

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_force_refresh(self, mock_boto_client, mock_fetch):
        """force_refresh should always refresh credentials."""
        creds1 = AWSCredentials(
            access_key_id="key1",
            secret_access_key="secret1",
            session_token="token1",
        )
        creds2 = AWSCredentials(
            access_key_id="key2",
            secret_access_key="secret2",
            session_token="token2",
        )
        mock_fetch.side_effect = [creds1, creds2]
        mock_client1 = MagicMock()
        mock_client2 = MagicMock()
        mock_boto_client.side_effect = [mock_client1, mock_client2]

        manager = CredentialManager("user", "pass")
        client1 = manager.get_client()
        assert manager.refresh_count == 1
        assert client1 == mock_client1

        client2 = manager.force_refresh()
        assert manager.refresh_count == 2
        assert client2 == mock_client2


class TestIsCredentialError:
    """Tests for is_credential_error helper function."""

    def test_no_credentials_error(self):
        """Should detect NoCredentialsError."""
        exc = NoCredentialsError()
        assert is_credential_error(exc) is True

    def test_partial_credentials_error(self):
        """Should detect PartialCredentialsError."""
        exc = PartialCredentialsError(provider="test", cred_var="access_key")
        assert is_credential_error(exc) is True

    def test_client_error_expired_token(self):
        """Should detect ClientError with ExpiredToken code."""
        exc = ClientError(
            {"Error": {"Code": "ExpiredToken", "Message": "Token expired"}},
            "GetObject",
        )
        assert is_credential_error(exc) is True

    def test_client_error_expired_token_exception(self):
        """Should detect ClientError with ExpiredTokenException code."""
        exc = ClientError(
            {"Error": {"Code": "ExpiredTokenException", "Message": "Token expired"}},
            "GetObject",
        )
        assert is_credential_error(exc) is True

    def test_client_error_invalid_token(self):
        """Should detect ClientError with InvalidToken code."""
        exc = ClientError(
            {"Error": {"Code": "InvalidToken", "Message": "Invalid token"}},
            "GetObject",
        )
        assert is_credential_error(exc) is True

    def test_client_error_access_denied(self):
        """Should detect ClientError with AccessDenied code."""
        exc = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "GetObject",
        )
        assert is_credential_error(exc) is True

    def test_client_error_other_code(self):
        """Should NOT flag non-credential ClientErrors."""
        exc = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}},
            "GetObject",
        )
        assert is_credential_error(exc) is False

    def test_non_credential_exception(self):
        """Should NOT flag unrelated exceptions."""
        assert is_credential_error(ValueError("test")) is False
        assert is_credential_error(ConnectionError("test")) is False
        assert is_credential_error(TimeoutError("test")) is False
