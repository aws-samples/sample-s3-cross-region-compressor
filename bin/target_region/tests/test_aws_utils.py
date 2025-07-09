"""
Unit tests for the aws_utils module in target_region.
"""

import json
import os
import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

# Set the AWS region before importing any boto3-dependent modules
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

# Import the module under test
from bin.target_region.utils.aws_utils import (
	get_sqs_messages,
	delete_sqs_message,
	delete_sqs_messages_batch,
	is_s3_test_event,
	extract_s3_event_info,
	get_s3_object,
	upload_to_s3,
	delete_s3_object,
	put_cloudwatch_metric,
	get_env_var,
	get_current_region,
)


class TestSQSFunctions:
	"""Tests for SQS related functions."""

	def test_get_sqs_messages(self, sqs_queue, sample_s3_event):
		"""Test retrieving messages from SQS queue."""
		# Given: A mocked SQS client and a queue with a message
		mock_response = {
			'Messages': [
				{
					'MessageId': '12345',
					'ReceiptHandle': 'test-receipt-handle',
					'Body': sample_s3_event['Body'],
					'Attributes': {'SentTimestamp': '1619712000000'},
				}
			]
		}

		# Create a mock for the SQS client's receive_message method
		with patch('bin.target_region.utils.aws_utils.sqs_client') as mock_sqs:
			mock_sqs.receive_message.return_value = mock_response

			# When: We get messages from the queue
			messages = get_sqs_messages(sqs_queue, max_messages=10)

			# Then: We should get the message
			assert len(messages) == 1
			body = json.loads(messages[0]['Body'])
			assert 'Records' in body
			assert body['Records'][0]['eventSource'] == 'aws:s3'

			# Verify the mock was called correctly
			mock_sqs.receive_message.assert_called_once_with(
				QueueUrl=sqs_queue, MaxNumberOfMessages=10, VisibilityTimeout=300, WaitTimeSeconds=20
			)

	def test_get_sqs_messages_empty_queue(self, sqs_queue):
		"""Test retrieving messages from an empty SQS queue."""
		# Given: A mocked SQS client and an empty queue
		with patch('bin.target_region.utils.aws_utils.sqs_client') as mock_sqs:
			# Configure mock to return empty response (no Messages key)
			mock_sqs.receive_message.return_value = {}

			# When: We get messages from the queue
			messages = get_sqs_messages(sqs_queue, max_messages=10)

			# Then: We should get an empty list
			assert messages == []

	def test_get_sqs_messages_error(self):
		"""Test handling errors when retrieving SQS messages."""
		# Given: A mocked SQS client and an invalid queue URL
		invalid_queue_url = 'https://sqs.us-east-1.amazonaws.com/123456789012/nonexistent-queue'

		with patch('bin.target_region.utils.aws_utils.sqs_client') as mock_sqs:
			# We need to make sure the exception is wrapped in a try/except in the tested function
			error = ClientError(
				error_response={'Error': {'Code': 'QueueDoesNotExist', 'Message': 'Queue not found'}},
				operation_name='ReceiveMessage',
			)
			mock_sqs.receive_message = MagicMock(side_effect=error)

			# When: We try to get messages from the nonexistent queue
			messages = get_sqs_messages(invalid_queue_url, max_messages=10)

			# Then: We should get an empty list due to error handling
			assert messages == []

	def test_delete_sqs_message(self, sqs_queue):
		"""Test deleting a message from SQS queue."""
		# Given: A mocked SQS client and a receipt handle
		receipt_handle = 'test-receipt-handle'

		with patch('bin.target_region.utils.aws_utils.sqs_client') as mock_sqs:
			# When: We delete the message
			result = delete_sqs_message(sqs_queue, receipt_handle)

			# Then: The deletion should be successful
			assert result is True
			mock_sqs.delete_message.assert_called_once_with(QueueUrl=sqs_queue, ReceiptHandle=receipt_handle)

	def test_delete_sqs_message_error(self, sqs_queue):
		"""Test handling errors when deleting SQS messages."""
		# Given: A mocked SQS client that raises an exception
		invalid_receipt_handle = 'invalid-receipt-handle'

		with patch('bin.target_region.utils.aws_utils.sqs_client') as mock_sqs:
			# We need to make sure the exception is wrapped in a try/except in the tested function
			error = ClientError(
				error_response={'Error': {'Code': 'InvalidReceiptHandle', 'Message': 'The receipt handle is invalid'}},
				operation_name='DeleteMessage',
			)
			mock_sqs.delete_message = MagicMock(side_effect=error)

			# When: We try to delete a message with an invalid receipt handle
			result = delete_sqs_message(sqs_queue, invalid_receipt_handle)

			# Then: The function should handle the error and return False
			assert result is False

	def test_delete_sqs_messages_batch(self, sqs_client, sqs_queue, sample_s3_event):
		"""Test deleting a batch of messages from SQS queue."""
		# Given: A queue with messages and receipt handles
		receipt_handles = ['receipt-handle-1', 'receipt-handle-2']

		# Mock the delete_message_batch response
		with patch('bin.target_region.utils.aws_utils.sqs_client.delete_message_batch') as mock_delete_batch:
			mock_delete_batch.return_value = {'Successful': [{'Id': '0'}, {'Id': '1'}], 'Failed': []}

			# When: We delete the messages in batch
			successful_ids, failed_ids = delete_sqs_messages_batch(sqs_queue, receipt_handles)

			# Then: All message deletions should be successful
			assert len(successful_ids) == 2
			assert len(failed_ids) == 0
			mock_delete_batch.assert_called_once()

	def test_delete_sqs_messages_batch_empty(self, sqs_client, sqs_queue):
		"""Test deleting an empty batch of messages."""
		# Given: An empty list of receipt handles
		receipt_handles = []

		# When: We try to delete an empty batch
		successful_ids, failed_ids = delete_sqs_messages_batch(sqs_queue, receipt_handles)

		# Then: Both result lists should be empty
		assert successful_ids == []
		assert failed_ids == []

	def test_delete_sqs_messages_batch_partial_failure(self, sqs_client, sqs_queue):
		"""Test handling partial failures when deleting message batches."""
		# Given: A list of receipt handles
		receipt_handles = ['handle1', 'handle2', 'handle3']

		# Mock the delete_message_batch response with partial failure
		with patch('bin.target_region.utils.aws_utils.sqs_client.delete_message_batch') as mock_delete_batch:
			mock_delete_batch.return_value = {'Successful': [{'Id': '0'}, {'Id': '2'}], 'Failed': [{'Id': '1'}]}

			# When: We delete the messages in batch
			successful_ids, failed_ids = delete_sqs_messages_batch(sqs_queue, receipt_handles)

			# Then: We should get the successful and failed IDs
			assert successful_ids == ['0', '2']
			assert failed_ids == ['1']


class TestS3EventHandling:
	"""Tests for S3 event handling functions."""

	def test_is_s3_test_event_direct_format(self, s3_test_event):
		"""Test detection of S3 test events in direct format."""
		# Given: A test event message

		# When: We check if it's a test event
		result = is_s3_test_event(s3_test_event)

		# Then: It should be identified as a test event
		assert result is True

	def test_is_s3_test_event_records_format(self):
		"""Test detection of S3 test events in records format."""
		# Given: A test event message in records format
		test_event_records = {
			'Body': json.dumps(
				{
					'Records': [
						{
							'eventSource': 'aws:s3',
							'eventName': 's3:TestEvent',
							's3': {'bucket': {'name': 'test-staging-bucket'}},
						}
					]
				}
			)
		}

		# When: We check if it's a test event
		result = is_s3_test_event(test_event_records)

		# Then: It should be identified as a test event
		assert result is True

	def test_is_s3_test_event_not_test(self, sample_s3_event):
		"""Test detection of regular S3 events."""
		# Given: A regular S3 event message

		# When: We check if it's a test event
		result = is_s3_test_event(sample_s3_event)

		# Then: It should not be identified as a test event
		assert result is False

	def test_is_s3_test_event_invalid_json(self):
		"""Test handling invalid JSON in event message."""
		# Given: A message with invalid JSON
		invalid_message = {'Body': 'not-json'}

		# When: We check if it's a test event
		result = is_s3_test_event(invalid_message)

		# Then: It should return False due to error handling
		assert result is False

	def test_extract_s3_event_info(self, sample_s3_event):
		"""Test extracting S3 event information."""
		# Given: An S3 event message

		# When: We extract the S3 object information
		s3_objects = extract_s3_event_info(sample_s3_event)

		# Then: We should get the correct bucket and key
		assert len(s3_objects) == 1
		assert s3_objects[0]['bucket'] == 'test-staging-bucket'
		assert s3_objects[0]['key'] == 'test/compressed_archive.tar.zstd'

	def test_extract_s3_event_info_multiple_records(self):
		"""Test extracting S3 event information from multiple records."""
		# Given: An S3 event with multiple records
		multi_record_event = {
			'Body': json.dumps(
				{
					'Records': [
						{
							'eventSource': 'aws:s3',
							'eventName': 'ObjectCreated:Put',
							's3': {
								'bucket': {'name': 'test-staging-bucket'},
								'object': {'key': 'test/object1.tar.zstd'},
							},
						},
						{
							'eventSource': 'aws:s3',
							'eventName': 'ObjectCreated:Put',
							's3': {
								'bucket': {'name': 'test-staging-bucket'},
								'object': {'key': 'test/object2.tar.zstd'},
							},
						},
					]
				}
			)
		}

		# When: We extract the S3 object information
		s3_objects = extract_s3_event_info(multi_record_event)

		# Then: We should get both objects
		assert len(s3_objects) == 2
		assert s3_objects[0]['bucket'] == 'test-staging-bucket'
		assert s3_objects[0]['key'] == 'test/object1.tar.zstd'
		assert s3_objects[1]['bucket'] == 'test-staging-bucket'
		assert s3_objects[1]['key'] == 'test/object2.tar.zstd'

	def test_extract_s3_event_info_invalid_json(self):
		"""Test handling invalid JSON in event message."""
		# Given: A message with invalid JSON
		invalid_message = {'Body': 'not-json'}

		# When: We try to extract S3 information
		s3_objects = extract_s3_event_info(invalid_message)

		# Then: We should get an empty list due to error handling
		assert s3_objects == []


class TestS3Operations:
	"""Tests for S3 operations."""

	def test_get_s3_object(self, staging_bucket, temp_directory):
		"""Test downloading an S3 object."""
		# Given: An S3 bucket with an object
		key = 'test/object.txt'
		local_path = os.path.join(temp_directory, 'downloaded_file.txt')

		with patch('bin.target_region.utils.aws_utils.s3_client') as mock_s3:
			# Configure mock to simulate successful download
			mock_s3.download_file.return_value = None

			# When: We download the object
			result = get_s3_object(staging_bucket, key, local_path)

			# Then: The download should be successful
			assert result is True
			mock_s3.download_file.assert_called_once_with(staging_bucket, key, local_path)

	def test_get_s3_object_error(self, staging_bucket, temp_directory):
		"""Test handling errors when downloading an S3 object."""
		# Given: A nonexistent object key
		key = 'nonexistent/object.txt'
		local_path = os.path.join(temp_directory, 'should_not_exist.txt')

		with patch('bin.target_region.utils.aws_utils.s3_client') as mock_s3:
			# Configure mock to raise an exception
			error = ClientError(
				error_response={'Error': {'Code': 'NoSuchKey', 'Message': 'The specified key does not exist'}},
				operation_name='GetObject',
			)
			mock_s3.download_file.side_effect = error

			# When: We try to download the nonexistent object
			result = get_s3_object(staging_bucket, key, local_path)

			# Then: The function should handle the error and return False
			assert result is False

	def test_upload_to_s3_basic(self, target_bucket, temp_directory):
		"""Test uploading a file to S3 with basic options."""
		# Given: A local file to upload
		local_path = os.path.join(temp_directory, 'upload_file.txt')
		with open(local_path, 'w') as f:
			f.write('This is a test file for uploading')
		key = 'uploads/upload_file.txt'

		with patch('bin.target_region.utils.aws_utils.s3_client') as mock_s3:
			# When: We upload the file to S3
			result = upload_to_s3(local_path, target_bucket, key)

			# Then: The upload should be successful
			assert result is True
			mock_s3.upload_file.assert_called_once()

	def test_upload_to_s3_with_storage_class(self, target_bucket, temp_directory):
		"""Test uploading a file to S3 with a specific storage class."""
		# Given: A local file to upload with storage class
		local_path = os.path.join(temp_directory, 'upload_file.txt')
		with open(local_path, 'w') as f:
			f.write('This is a test file for uploading')
		key = 'uploads/upload_file.txt'
		storage_class = 'STANDARD_IA'

		with patch('bin.target_region.utils.aws_utils.s3_client') as mock_s3:
			# When: We upload the file to S3 with storage class
			result = upload_to_s3(local_path, target_bucket, key, storage_class=storage_class)

			# Then: The upload should be successful with storage class in extra args
			assert result is True
			mock_s3.upload_file.assert_called_once()
			# Check that ExtraArgs contains the storage class
			call_args = mock_s3.upload_file.call_args
			assert call_args[1]['ExtraArgs']['StorageClass'] == 'STANDARD_IA'

	def test_upload_to_s3_with_kms(self, target_bucket, temp_directory):
		"""Test uploading a file to S3 with KMS encryption."""
		# Given: A local file to upload with KMS encryption
		local_path = os.path.join(temp_directory, 'upload_file.txt')
		with open(local_path, 'w') as f:
			f.write('This is a test file for uploading')
		key = 'uploads/upload_file.txt'
		kms_key_arn = 'arn:aws:kms:us-east-1:123456789012:key/test-key'

		with patch('bin.target_region.utils.aws_utils.s3_client') as mock_s3:
			# When: We upload the file to S3 with KMS
			result = upload_to_s3(local_path, target_bucket, key, kms_key_arn=kms_key_arn)

			# Then: The upload should be successful with KMS settings in extra args
			assert result is True
			mock_s3.upload_file.assert_called_once()
			# Check that ExtraArgs contains KMS settings
			call_args = mock_s3.upload_file.call_args
			assert call_args[1]['ExtraArgs']['ServerSideEncryption'] == 'aws:kms'
			assert call_args[1]['ExtraArgs']['SSEKMSKeyId'] == kms_key_arn

	def test_upload_to_s3_with_tags(self, target_bucket, temp_directory):
		"""Test uploading a file to S3 with tags."""
		# Given: A local file to upload with tags
		local_path = os.path.join(temp_directory, 'upload_file.txt')
		with open(local_path, 'w') as f:
			f.write('This is a test file for uploading')
		key = 'uploads/upload_file.txt'
		tags = {'Purpose': 'Testing', 'Environment': 'Dev'}

		with patch('bin.target_region.utils.aws_utils.s3_client') as mock_s3:
			# When: We upload the file to S3 with tags
			result = upload_to_s3(local_path, target_bucket, key, tags=tags)

			# Then: The upload should be successful and tags should be applied
			assert result is True
			mock_s3.upload_file.assert_called_once()
			mock_s3.put_object_tagging.assert_called_once()
			# Check that put_object_tagging was called with the right parameters
			call_args = mock_s3.put_object_tagging.call_args
			assert call_args[1]['Bucket'] == target_bucket
			assert call_args[1]['Key'] == key
			assert len(call_args[1]['Tagging']['TagSet']) == 2

	def test_upload_to_s3_error(self, target_bucket, temp_directory):
		"""Test handling errors when uploading to S3."""
		# Given: A local file but with an error during upload
		local_path = os.path.join(temp_directory, 'upload_file.txt')
		with open(local_path, 'w') as f:
			f.write('This is a test file for uploading')
		key = 'uploads/upload_file.txt'

		with patch('bin.target_region.utils.aws_utils.s3_client') as mock_s3:
			# Configure mock to raise an exception
			error = ClientError(
				error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
				operation_name='PutObject',
			)
			mock_s3.upload_file.side_effect = error

			# When: We try to upload the file
			result = upload_to_s3(local_path, target_bucket, key)

			# Then: The function should handle the error and return False
			assert result is False

	def test_delete_s3_object(self, staging_bucket):
		"""Test deleting an S3 object."""
		# Given: An S3 object to delete
		key = 'test/object_to_delete.txt'

		with patch('bin.target_region.utils.aws_utils.s3_client') as mock_s3:
			# When: We delete the object
			result = delete_s3_object(staging_bucket, key)

			# Then: The deletion should be successful
			assert result is True
			mock_s3.delete_object.assert_called_once_with(Bucket=staging_bucket, Key=key)

	def test_delete_s3_object_error(self, staging_bucket):
		"""Test handling errors when deleting an S3 object."""
		# Given: An S3 object but with an error during deletion
		key = 'test/object_to_delete.txt'

		with patch('bin.target_region.utils.aws_utils.s3_client') as mock_s3:
			# Configure mock to raise an exception
			error = ClientError(
				error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
				operation_name='DeleteObject',
			)
			mock_s3.delete_object.side_effect = error

			# When: We try to delete the object
			result = delete_s3_object(staging_bucket, key)

			# Then: The function should handle the error and return False
			assert result is False


class TestCloudWatchMetrics:
	"""Tests for CloudWatch metrics."""

	def test_put_cloudwatch_metric(self, cloudwatch_client):
		"""Test putting a metric data point to CloudWatch."""
		# Given: CloudWatch metric data
		namespace = 'TestNamespace'
		metric_name = 'TestMetric'
		value = 123.45
		unit = 'Count'
		dimensions = [{'Name': 'TestDimension', 'Value': 'TestValue'}]

		with patch('bin.target_region.utils.aws_utils.cloudwatch_client') as mock_cw:
			# When: We put a metric data point
			result = put_cloudwatch_metric(namespace, metric_name, value, unit, dimensions)

			# Then: The operation should be successful
			assert result is True
			mock_cw.put_metric_data.assert_called_once_with(
				Namespace='TestNamespace',
				MetricData=[
					{
						'MetricName': 'TestMetric',
						'Value': 123.45,
						'Unit': 'Count',
						'Dimensions': [{'Name': 'TestDimension', 'Value': 'TestValue'}],
					}
				],
			)

	def test_put_cloudwatch_metric_error(self):
		"""Test handling errors when putting CloudWatch metrics."""
		# Given: CloudWatch metric data but with an error
		namespace = 'TestNamespace'
		metric_name = 'TestMetric'
		value = 123.45
		unit = 'Count'
		dimensions = [{'Name': 'TestDimension', 'Value': 'TestValue'}]

		with patch('bin.target_region.utils.aws_utils.cloudwatch_client') as mock_cw:
			# Configure mock to raise an exception
			error = ClientError(
				error_response={'Error': {'Code': 'InternalServiceError', 'Message': 'CloudWatch service error'}},
				operation_name='PutMetricData',
			)
			mock_cw.put_metric_data.side_effect = error

			# When: We try to put a metric
			result = put_cloudwatch_metric(namespace, metric_name, value, unit, dimensions)

			# Then: The function should handle the error and return False
			assert result is False


class TestUtilityFunctions:
	"""Tests for utility functions."""

	def test_get_env_var_exists(self):
		"""Test getting an environment variable that exists."""
		# Given: An environment variable is set
		os.environ['TEST_VAR'] = 'test-value'

		# When: We get the environment variable
		result = get_env_var('TEST_VAR')

		# Then: We should get the value
		assert result == 'test-value'

		# Clean up
		os.environ.pop('TEST_VAR', None)

	def test_get_env_var_not_exists_required(self):
		"""Test getting a required environment variable that doesn't exist."""
		# Given: An environment variable that doesn't exist
		if 'TEST_VAR_MISSING' in os.environ:
			os.environ.pop('TEST_VAR_MISSING')

		# When/Then: Getting the variable should exit the program
		with pytest.raises(SystemExit):
			get_env_var('TEST_VAR_MISSING', required=True)

	def test_get_env_var_not_exists_optional(self):
		"""Test getting an optional environment variable that doesn't exist."""
		# Given: An environment variable that doesn't exist
		if 'TEST_VAR_MISSING' in os.environ:
			os.environ.pop('TEST_VAR_MISSING')

		# When: We get the optional environment variable
		result = get_env_var('TEST_VAR_MISSING', required=False)

		# Then: We should get None
		assert result is None

	def test_get_current_region_from_env(self):
		"""Test getting the current region from environment variable."""
		# Given: AWS_DEFAULT_REGION is set
		os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'

		# When: We get the current region
		region = get_current_region()

		# Then: We should get the region from the environment variable
		assert region == 'us-west-2'

		# Reset
		os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

	def test_get_current_region_from_session(self):
		"""Test getting the current region from boto3 session."""
		# Given: AWS_DEFAULT_REGION is not set
		original_region = os.environ.get('AWS_DEFAULT_REGION')
		os.environ.pop('AWS_DEFAULT_REGION', None)

		# Mock boto3 session region
		with patch('bin.target_region.utils.aws_utils.boto3.session.Session') as mock_session:
			mock_session_instance = MagicMock()
			mock_session_instance.region_name = 'eu-west-1'
			mock_session.return_value = mock_session_instance

			# When: We get the current region
			region = get_current_region()

			# Then: We should get the region from the boto3 session
			assert region == 'eu-west-1'

		# Restore original region if it existed
		if original_region is not None:
			os.environ['AWS_DEFAULT_REGION'] = original_region

	def test_get_current_region_fallback(self):
		"""Test getting the current region with fallback to default."""
		# Given: AWS_DEFAULT_REGION is not set and boto3 session returns None
		original_region = os.environ.get('AWS_DEFAULT_REGION')
		os.environ.pop('AWS_DEFAULT_REGION', None)

		# Mock boto3 session region as None
		with patch('bin.target_region.utils.aws_utils.boto3.session.Session') as mock_session:
			mock_session_instance = MagicMock()
			mock_session_instance.region_name = None
			mock_session.return_value = mock_session_instance

			# When: We get the current region
			region = get_current_region()

			# Then: We should get the default region
			assert region == 'us-east-1'

		# Restore original region if it existed
		if original_region is not None:
			os.environ['AWS_DEFAULT_REGION'] = original_region
