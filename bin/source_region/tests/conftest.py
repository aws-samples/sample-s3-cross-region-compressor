"""
Shared pytest fixtures for the source_region application tests.

This module belongs to the source_region_tests package.
"""

import os
import json
import pytest
import boto3
import tempfile
from moto import mock_aws


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
		# Create S3 bucket in us-east-1
		s3.create_bucket(Bucket='test-bucket')
		yield s3


@pytest.fixture
def sqs_client():
	"""Create a boto3 SQS client with moto mock."""
	with mock_aws():
		client = boto3.client('sqs', region_name='us-east-1')
		yield client


@pytest.fixture
def dynamodb_client():
	"""Create a boto3 DynamoDB client with moto mock."""
	with mock_aws():
		yield boto3.client('dynamodb', region_name='us-east-1')


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
def s3_bucket(s3_client):
	"""Create a test S3 bucket."""
	bucket_name = 'test-source-bucket'
	s3_client.create_bucket(Bucket=bucket_name)
	yield bucket_name


@pytest.fixture
def sqs_queue(sqs_client):
	"""Create a test SQS queue."""
	response = sqs_client.create_queue(QueueName='test-queue')
	queue_url = response['QueueUrl']
	yield queue_url


@pytest.fixture
def dynamodb_tables(dynamodb_client):
	"""Create test DynamoDB tables for parameters and compression settings."""
	# Create parameters table
	dynamodb_client.create_table(
		TableName='test-replication-parameters',
		KeySchema=[{'AttributeName': 'ParameterName', 'KeyType': 'HASH'}],
		AttributeDefinitions=[{'AttributeName': 'ParameterName', 'AttributeType': 'S'}],
		BillingMode='PAY_PER_REQUEST',
	)

	# Create compression settings table
	dynamodb_client.create_table(
		TableName='test-compression-settings',
		KeySchema=[{'AttributeName': 'BucketPrefix', 'KeyType': 'HASH'}],
		AttributeDefinitions=[{'AttributeName': 'BucketPrefix', 'AttributeType': 'S'}],
		BillingMode='PAY_PER_REQUEST',
	)

	# Set environment variables for tables
	os.environ['REPLICATION_PARAMETERS_TABLE'] = 'test-replication-parameters'
	os.environ['COMPRESSION_SETTINGS_TABLE'] = 'test-compression-settings'

	yield {'parameters_table': 'test-replication-parameters', 'settings_table': 'test-compression-settings'}

	# Clean up environment variables
	os.environ.pop('REPLICATION_PARAMETERS_TABLE', None)
	os.environ.pop('COMPRESSION_SETTINGS_TABLE', None)


@pytest.fixture
def sample_s3_event():
	"""Create a sample S3 event message."""
	return {
		'Body': json.dumps(
			{
				'Records': [
					{
						'eventSource': 'aws:s3',
						'eventName': 'ObjectCreated:Put',
						's3': {'bucket': {'name': 'test-source-bucket'}, 'object': {'key': 'test/object.txt'}},
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
		'Body': json.dumps({'Event': 's3:TestEvent', 'Bucket': 'test-source-bucket'}),
		'ReceiptHandle': 'test-event-receipt-handle',
	}


@pytest.fixture
def setup_s3_objects(s3_client, s3_bucket):
	"""Setup test objects in S3."""
	# Create test object
	s3_client.put_object(Bucket=s3_bucket, Key='test/object1.txt', Body=b'This is test content for object 1')

	# Create another test object with a different prefix
	s3_client.put_object(Bucket=s3_bucket, Key='other/object2.txt', Body=b'This is test content for object 2')

	# Add tags to one object
	s3_client.put_object_tagging(
		Bucket=s3_bucket,
		Key='test/object1.txt',
		Tagging={'TagSet': [{'Key': 'Purpose', 'Value': 'Testing'}, {'Key': 'Environment', 'Value': 'Dev'}]},
	)

	return {
		'bucket': s3_bucket,
		'objects': [{'key': 'test/object1.txt', 'size': 31}, {'key': 'other/object2.txt', 'size': 31}],
	}


@pytest.fixture
def setup_dynamodb_parameters(dynamodb_client, dynamodb_tables):
	"""Setup test parameters in DynamoDB."""
	# Add a test parameter for bucket/prefix
	dynamodb_client.put_item(
		TableName=dynamodb_tables['parameters_table'],
		Item={
			'ParameterName': {'S': '/test-stack/test-source-bucket/test'},
			'Destinations': {
				'L': [
					{
						'M': {
							'region': {'S': 'us-west-2'},
							'bucket': {'S': 'target-bucket-west'},
							'kms_key_arn': {'S': 'arn:aws:kms:us-west-2:123456789012:key/test-key'},
							'storage_class': {'S': 'STANDARD'},
						}
					},
					{
						'M': {
							'region': {'S': 'eu-west-1'},
							'bucket': {'S': 'target-bucket-eu'},
							'storage_class': {'S': 'STANDARD_IA'},
						}
					},
				]
			},
			'LastUpdated': {'N': '1619712000'},
		},
	)

	# Add a bucket-level parameter (no prefix)
	dynamodb_client.put_item(
		TableName=dynamodb_tables['parameters_table'],
		Item={
			'ParameterName': {'S': '/test-stack/test-source-bucket'},
			'Destinations': {
				'L': [
					{
						'M': {
							'region': {'S': 'us-west-1'},
							'bucket': {'S': 'target-bucket-default'},
							'storage_class': {'S': 'STANDARD'},
						}
					}
				]
			},
			'LastUpdated': {'N': '1619712000'},
		},
	)

	return {'prefix_param': '/test-stack/test-source-bucket/test', 'bucket_param': '/test-stack/test-source-bucket'}


@pytest.fixture
def setup_compression_settings(dynamodb_client, dynamodb_tables):
	"""Setup test compression settings in DynamoDB."""
	# Add a test setting for bucket/prefix
	dynamodb_client.put_item(
		TableName=dynamodb_tables['settings_table'],
		Item={
			'BucketPrefix': {'S': 'test-source-bucket/test/'},
			'OptimalLevel': {'N': '12'},
			'TotalProcessed': {'N': '100'},
			'Version': {'N': '1'},
			'LastUpdated': {'N': '1619712000'},
			'MetricsHistory': {
				'L': [
					{
						'M': {
							'Level': {'N': '12'},
							'OriginalSize': {'N': '1000'},
							'CompressedSize': {'N': '400'},
							'ProcessingTime': {'N': '2'},
							'NumRegions': {'N': '2'},
							'Timestamp': {'N': '1619712000'},
							'CostBenefitScore': {'N': '0.8'},
						}
					},
					{
						'M': {
							'Level': {'N': '10'},
							'OriginalSize': {'N': '1000'},
							'CompressedSize': {'N': '450'},
							'ProcessingTime': {'N': '1.5'},
							'NumRegions': {'N': '2'},
							'Timestamp': {'N': '1619711900'},
							'CostBenefitScore': {'N': '0.75'},
						}
					},
				]
			},
		},
	)

	return 'test-source-bucket/test/'


@pytest.fixture
def setup_environment_variables():
	"""Setup required environment variables for tests."""
	os.environ['SQS_QUEUE_URL'] = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-queue'
	os.environ['BUCKET'] = 'test-outbound-bucket'
	os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
	os.environ['STACK_NAME'] = 'test-stack'
	os.environ['MONITORED_PREFIX'] = 'test'
	os.environ['LOG_LEVEL'] = 'DEBUG'

	yield

	# Clean up
	os.environ.pop('SQS_QUEUE_URL', None)
	os.environ.pop('BUCKET', None)
	os.environ.pop('STACK_NAME', None)
	os.environ.pop('MONITORED_PREFIX', None)
	os.environ.pop('LOG_LEVEL', None)
