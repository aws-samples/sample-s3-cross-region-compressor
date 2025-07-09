"""
Shared pytest fixtures for the target_region application tests.

This module belongs to the target_region_tests package.
"""

import os
import json
import pytest
import boto3
import tempfile
import tarfile
from moto import mock_aws
from unittest.mock import patch


@pytest.fixture(scope='function', autouse=True)
def aws_credentials():
	"""Mocked AWS Credentials for moto."""
	# Mock credentials
	os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
	os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
	os.environ['AWS_SECURITY_TOKEN'] = 'testing'
	os.environ['AWS_SESSION_TOKEN'] = 'testing'
	os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

	yield

	# Clean up after the test
	os.environ.pop('AWS_ACCESS_KEY_ID', None)
	os.environ.pop('AWS_SECRET_ACCESS_KEY', None)
	os.environ.pop('AWS_SECURITY_TOKEN', None)
	os.environ.pop('AWS_SESSION_TOKEN', None)
	os.environ.pop('AWS_DEFAULT_REGION', None)


@pytest.fixture
def s3_client():
	"""Create a boto3 S3 client with moto mock."""
	with mock_aws():
		s3 = boto3.client('s3', region_name='us-east-1')
		# Create test bucket
		s3.create_bucket(Bucket='test-bucket')
		yield s3


@pytest.fixture
def sqs_client():
	"""Create a boto3 SQS client with moto mock."""
	with mock_aws():
		client = boto3.client('sqs', region_name='us-east-1')
		yield client


@pytest.fixture
def cloudwatch_client():
	"""Create a boto3 CloudWatch client with moto mock."""
	with mock_aws():
		client = boto3.client('cloudwatch', region_name='us-east-1')
		yield client


@pytest.fixture
def temp_directory():
	"""Create a temporary directory for test files."""
	temp_dir = tempfile.mkdtemp()
	yield temp_dir
	# Cleanup is handled by the OS, but we could add explicit cleanup if needed


@pytest.fixture
def staging_bucket(s3_client):
	"""Create a test staging S3 bucket."""
	bucket_name = 'test-staging-bucket'
	s3_client.create_bucket(Bucket=bucket_name)
	yield bucket_name


@pytest.fixture
def target_bucket(s3_client):
	"""Create a test target S3 bucket."""
	bucket_name = 'test-target-bucket'
	s3_client.create_bucket(Bucket=bucket_name)
	yield bucket_name


@pytest.fixture
def sqs_queue(sqs_client):
	"""Create a test SQS queue."""
	response = sqs_client.create_queue(QueueName='test-queue')
	queue_url = response['QueueUrl']
	yield queue_url


@pytest.fixture
def sample_s3_event():
	"""Create a sample S3 event message for a compressed object."""
	return {
		'Body': json.dumps(
			{
				'Records': [
					{
						'eventSource': 'aws:s3',
						'eventName': 'ObjectCreated:Put',
						's3': {
							'bucket': {'name': 'test-staging-bucket'},
							'object': {'key': 'test/compressed_archive.tar.zstd'},
						},
					}
				]
			}
		),
		'ReceiptHandle': 'sample-receipt-handle-1',
	}


@pytest.fixture
def s3_test_event():
	"""Create a sample S3 test event message."""
	return {
		'Body': json.dumps({'Event': 's3:TestEvent', 'Bucket': 'test-staging-bucket'}),
		'ReceiptHandle': 'test-event-receipt-handle',
	}


@pytest.fixture
def test_manifest_data():
	"""Create a sample manifest data structure."""
	return {
		'format_version': '1.0',
		'source_bucket': 'test-source-bucket',
		'source_prefix': 'test',
		'compression_level': 12,
		'timestamp': '2023-01-01T12:00:00Z',
		'objects': [
			{
				'object_name': 'test_file1.txt',
				'relative_key': 'file1.txt',
				'source_bucket': 'test-source-bucket',
				'source_prefix': 'test',
				'size': 1024,
				'etag': '"1234567890abcdef"',
				'storage_class': 'STANDARD',
				'creation_time': '2023-01-01T11:00:00Z',
				'tags': [{'Purpose': 'Testing'}, {'Environment': 'Dev'}],
			},
			{
				'object_name': 'test_file2.txt',
				'relative_key': 'file2.txt',
				'source_bucket': 'test-source-bucket',
				'source_prefix': 'test',
				'size': 2048,
				'etag': '"abcdef1234567890"',
				'storage_class': 'STANDARD_IA',
				'creation_time': '2023-01-01T10:30:00Z',
				'tags': [],
			},
		],
		'targets': [
			{'region': 'us-east-1', 'bucket': 'test-target-bucket', 'storage_class': 'STANDARD'},
			{
				'region': 'us-west-2',
				'bucket': 'test-target-west-bucket',
				'storage_class': 'STANDARD_IA',
				'kms_key_arn': 'arn:aws:kms:us-west-2:123456789012:key/test-key',
			},
		],
	}


@pytest.fixture
def test_manifest_file(temp_directory, test_manifest_data):
	"""Create a sample manifest file for testing."""
	manifest_path = os.path.join(temp_directory, 'manifest.json')

	with open(manifest_path, 'w') as f:
		json.dump(test_manifest_data, f)

	yield manifest_path


@pytest.fixture
def create_test_archive(temp_directory, test_manifest_file):
	"""Create a test tar archive with a manifest and test files."""
	# Create directory for archive contents
	extract_dir = os.path.join(temp_directory, 'archive_contents')
	objects_dir = os.path.join(extract_dir, 'objects')
	os.makedirs(objects_dir, exist_ok=True)

	# Create test files
	file1_path = os.path.join(objects_dir, 'file1.txt')
	with open(file1_path, 'w') as f:
		f.write('This is test file 1 content')

	file2_path = os.path.join(objects_dir, 'file2.txt')
	with open(file2_path, 'w') as f:
		f.write('This is test file 2 content with more data')

	# Copy manifest to archive contents dir
	manifest_path = os.path.join(extract_dir, 'manifest.json')
	with open(test_manifest_file, 'r') as src, open(manifest_path, 'w') as dst:
		dst.write(src.read())

	# Create tar archive
	tar_path = os.path.join(temp_directory, 'archive.tar')
	with tarfile.open(tar_path, 'w') as tar:
		# Add manifest
		tar.add(manifest_path, arcname='manifest.json')
		# Add test files
		tar.add(file1_path, arcname='objects/file1.txt')
		tar.add(file2_path, arcname='objects/file2.txt')

	# Create a mock compressed archive (we don't actually compress it for tests)
	compressed_path = os.path.join(temp_directory, 'archive.tar.zstd')
	with open(compressed_path, 'wb') as f:
		with open(tar_path, 'rb') as src:
			f.write(src.read())

	yield {
		'tar_path': tar_path,
		'compressed_path': compressed_path,
		'extract_dir': extract_dir,
		'files': ['objects/file1.txt', 'objects/file2.txt', 'manifest.json'],
	}


@pytest.fixture
def setup_s3_compressed_object(s3_client, staging_bucket, create_test_archive):
	"""Setup compressed object in S3 staging bucket."""
	compressed_path = create_test_archive['compressed_path']
	key = 'test/compressed_archive.tar.zstd'

	with open(compressed_path, 'rb') as f:
		s3_client.put_object(Bucket=staging_bucket, Key=key, Body=f.read())

	return {'bucket': staging_bucket, 'key': key, 'local_path': compressed_path}


@pytest.fixture
def mock_decompress_stream():
	"""Mock the pyzstd decompress_stream function."""
	with patch('bin.target_region.utils.decompression.pyzstd.decompress_stream') as mock:
		# Configure the mock to "decompress" by copying input to output
		def side_effect(input_stream, output_stream, read_size=None, write_size=None):
			data = input_stream.read()
			output_stream.write(data)
			return len(data), len(data)

		mock.side_effect = side_effect
		yield mock


@pytest.fixture
def setup_environment_variables():
	"""Setup required environment variables for tests."""
	os.environ['SQS_QUEUE_URL'] = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-queue'
	os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
	os.environ['LOG_LEVEL'] = 'DEBUG'
	os.environ['STACK_NAME'] = 'test-stack'

	yield

	# Clean up
	os.environ.pop('SQS_QUEUE_URL', None)
	os.environ.pop('LOG_LEVEL', None)
	os.environ.pop('STACK_NAME', None)
