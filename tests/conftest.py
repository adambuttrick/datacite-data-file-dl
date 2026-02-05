"""Pytest configuration and fixtures."""

import pytest
from moto import mock_aws
import boto3

from datacite_data_file_dl.download import BUCKET


@pytest.fixture
def tmp_output_dir(tmp_path):
    """Provide a temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def mock_s3():
    """Provide a mocked S3 environment."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        # Create the bucket
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def populated_s3(mock_s3):
    """Provide S3 with test data."""
    # Add some test files
    mock_s3.put_object(
        Bucket=BUCKET,
        Key="MANIFEST",
        Body=b"manifest content",
    )
    mock_s3.put_object(
        Bucket=BUCKET,
        Key="dois/updated_2024-01/part-001.json",
        Body=b'{"test": "data1"}',
    )
    mock_s3.put_object(
        Bucket=BUCKET,
        Key="dois/updated_2024-01/part-002.json",
        Body=b'{"test": "data2"}',
    )
    mock_s3.put_object(
        Bucket=BUCKET,
        Key="dois/updated_2024-02/part-001.json",
        Body=b'{"test": "data3"}',
    )
    return mock_s3
