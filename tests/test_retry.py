"""Tests for retry logic."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from datacite_data_file_dl.retry import retry_with_backoff, retry_with_credential_refresh, RetryExhausted


class TestRetryWithBackoff:
    """Test the retry decorator."""

    def test_success_on_first_try(self):
        """Should return result immediately on success."""
        call_count = 0

        @retry_with_backoff(max_retries=3)
        def always_succeeds():
            nonlocal call_count
            call_count += 1
            return "success"

        result = always_succeeds()
        assert result == "success"
        assert call_count == 1

    def test_success_after_retry(self):
        """Should retry and succeed."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01)
        def succeeds_on_third():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("temporary failure")
            return "success"

        result = succeeds_on_third()
        assert result == "success"
        assert call_count == 3

    def test_exhausted_retries(self):
        """Should raise RetryExhausted after max retries."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("permanent failure")

        with pytest.raises(RetryExhausted) as exc_info:
            always_fails()

        assert call_count == 3
        assert "permanent failure" in str(exc_info.value)

    def test_non_retryable_error_not_retried(self):
        """Non-retryable errors should propagate immediately."""
        call_count = 0

        @retry_with_backoff(max_retries=3, retryable_exceptions=(ConnectionError,))
        def raises_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            raises_value_error()

        assert call_count == 1

    def test_zero_retries_means_no_retry(self):
        """With max_retries=0, should not retry at all."""
        call_count = 0

        @retry_with_backoff(max_retries=0)
        def fails_once():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("fail")

        with pytest.raises(RetryExhausted):
            fails_once()

        assert call_count == 1


class TestRetryWithCredentialRefresh:
    """Tests for retry_with_credential_refresh function."""

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_success_on_first_try(self, mock_boto_client, mock_fetch):
        """Should return result immediately on success."""
        from datacite_data_file_dl.auth import AWSCredentials, CredentialManager

        mock_fetch.return_value = AWSCredentials("key1", "secret1", "token1")
        mock_boto_client.return_value = MagicMock()

        cred_manager = CredentialManager("user", "pass")
        call_count = 0

        def succeeds(client):
            nonlocal call_count
            call_count += 1
            return "success"

        result = retry_with_credential_refresh(
            succeeds,
            credential_manager=cred_manager,
            max_retries=3,
        )
        assert result == "success"
        assert call_count == 1

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_success_after_retry(self, mock_boto_client, mock_fetch):
        """Should retry and succeed on transient errors."""
        from datacite_data_file_dl.auth import AWSCredentials, CredentialManager

        mock_fetch.return_value = AWSCredentials("key1", "secret1", "token1")
        mock_boto_client.return_value = MagicMock()

        cred_manager = CredentialManager("user", "pass")
        call_count = 0

        def succeeds_on_third(client):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("temporary failure")
            return "success"

        result = retry_with_credential_refresh(
            succeeds_on_third,
            credential_manager=cred_manager,
            max_retries=3,
            base_delay=0.01,
        )
        assert result == "success"
        assert call_count == 3

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_exhausted_retries(self, mock_boto_client, mock_fetch):
        """Should raise RetryExhausted after max retries."""
        from datacite_data_file_dl.auth import AWSCredentials, CredentialManager

        mock_fetch.return_value = AWSCredentials("key1", "secret1", "token1")
        mock_boto_client.return_value = MagicMock()

        cred_manager = CredentialManager("user", "pass")
        call_count = 0

        def always_fails(client):
            nonlocal call_count
            call_count += 1
            raise ConnectionError("permanent failure")

        with pytest.raises(RetryExhausted) as exc_info:
            retry_with_credential_refresh(
                always_fails,
                credential_manager=cred_manager,
                max_retries=3,
                base_delay=0.01,
            )

        assert call_count == 3
        assert "permanent failure" in str(exc_info.value)

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_credential_error_triggers_refresh(self, mock_boto_client, mock_fetch):
        """Should refresh credentials on credential error and retry."""
        from datacite_data_file_dl.auth import AWSCredentials, CredentialManager

        mock_fetch.side_effect = [
            AWSCredentials("key1", "secret1", "token1"),
            AWSCredentials("key2", "secret2", "token2"),
        ]
        mock_client1 = MagicMock()
        mock_client2 = MagicMock()
        mock_boto_client.side_effect = [mock_client1, mock_client2]

        cred_manager = CredentialManager("user", "pass")

        call_count = 0
        clients_seen = []

        def fails_then_succeeds(client):
            nonlocal call_count
            call_count += 1
            clients_seen.append(client)
            if call_count == 1:
                raise ClientError(
                    {"Error": {"Code": "ExpiredToken", "Message": "Token expired"}},
                    "GetObject",
                )
            return "success"

        result = retry_with_credential_refresh(
            fails_then_succeeds,
            credential_manager=cred_manager,
            max_retries=3,
            base_delay=0.01,
        )

        assert result == "success"
        assert call_count == 2
        assert clients_seen[0] == mock_client1
        assert clients_seen[1] == mock_client2
        assert cred_manager.refresh_count == 2

    @patch("datacite_data_file_dl.auth.fetch_credentials")
    @patch("datacite_data_file_dl.auth.boto3.client")
    def test_credential_refresh_only_once(self, mock_boto_client, mock_fetch):
        """Should only refresh credentials once per retry sequence."""
        from datacite_data_file_dl.auth import AWSCredentials, CredentialManager

        mock_fetch.side_effect = [
            AWSCredentials("key1", "secret1", "token1"),
            AWSCredentials("key2", "secret2", "token2"),
        ]
        mock_boto_client.side_effect = [MagicMock(), MagicMock()]

        cred_manager = CredentialManager("user", "pass")

        call_count = 0

        def always_credential_error(client):
            nonlocal call_count
            call_count += 1
            raise ClientError(
                {"Error": {"Code": "ExpiredToken", "Message": "Token expired"}},
                "GetObject",
            )

        with pytest.raises(RetryExhausted):
            retry_with_credential_refresh(
                always_credential_error,
                credential_manager=cred_manager,
                max_retries=3,
                base_delay=0.01,
            )

        assert call_count >= 3
        assert cred_manager.refresh_count == 2
