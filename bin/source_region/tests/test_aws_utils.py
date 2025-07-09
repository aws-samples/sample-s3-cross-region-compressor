"""
Unit tests for the aws_utils module.
"""

import json
import os
import tempfile
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

# Set the AWS region before importing any boto3-dependent modules
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

# Import the module under test
from bin.source_region.utils.aws_utils import (
	get_sqs_messages,
	delete_sqs_message,
	delete_sqs_messages_batch,
	is_s3_test_event,
	extract_s3_event_info,
	get_s3_object,
	get_s3_object_metadata,
	upload_to_s3,
	get_target_info_from_dynamodb,
	put_cloudwatch_metric,
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
		with patch('bin.source_region.utils.aws_utils.sqs_client') as mock_sqs:
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
				QueueUrl=sqs_queue, MaxNumberOfMessages=10, WaitTimeSeconds=20
			)

	def test_get_sqs_messages_empty_queue(self, sqs_queue):
		"""Test retrieving messages from an empty SQS queue."""
		# Given: A mocked SQS client and an empty queue
		with patch('bin.source_region.utils.aws_utils.sqs_client') as mock_sqs:
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

		with patch('bin.source_region.utils.aws_utils.sqs_client') as mock_sqs:
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

		with patch('bin.source_region.utils.aws_utils.sqs_client') as mock_sqs:
			# When: We delete the message
			result = delete_sqs_message(sqs_queue, receipt_handle)

			# Then: The deletion should be successful
			assert result is True
			mock_sqs.delete_message.assert_called_once_with(QueueUrl=sqs_queue, ReceiptHandle=receipt_handle)

	def test_delete_sqs_message_error(self, sqs_queue):
		"""Test handling errors when deleting SQS messages."""
		# Given: A mocked SQS client that raises an exception
		invalid_receipt_handle = 'invalid-receipt-handle'

		with patch('bin.source_region.utils.aws_utils.sqs_client') as mock_sqs:
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
		# Given: A queue with messages
		response1 = sqs_client.send_message(QueueUrl=sqs_queue, MessageBody=sample_s3_event['Body'])
		response2 = sqs_client.send_message(QueueUrl=sqs_queue, MessageBody=sample_s3_event['Body'])
		receipt_handles = ['receipt-handle-1', 'receipt-handle-2']

		# Mock the delete_message_batch response
		with patch('bin.source_region.utils.aws_utils.sqs_client.delete_message_batch') as mock_delete_batch:
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
		with patch('bin.source_region.utils.aws_utils.sqs_client.delete_message_batch') as mock_delete_batch:
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
							's3': {'bucket': {'name': 'test-source-bucket'}},
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
		assert s3_objects[0]['bucket'] == 'test-source-bucket'
		assert s3_objects[0]['key'] == 'test/object.txt'

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
							's3': {'bucket': {'name': 'test-source-bucket'}, 'object': {'key': 'test/object1.txt'}},
						},
						{
							'eventSource': 'aws:s3',
							'eventName': 'ObjectCreated:Put',
							's3': {'bucket': {'name': 'test-source-bucket'}, 'object': {'key': 'test/object2.txt'}},
						},
					]
				}
			)
		}

		# When: We extract the S3 object information
		s3_objects = extract_s3_event_info(multi_record_event)

		# Then: We should get both objects
		assert len(s3_objects) == 2
		assert s3_objects[0]['bucket'] == 'test-source-bucket'
		assert s3_objects[0]['key'] == 'test/object1.txt'
		assert s3_objects[1]['bucket'] == 'test-source-bucket'
		assert s3_objects[1]['key'] == 'test/object2.txt'

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

	def test_get_s3_object(self, setup_s3_objects, temp_directory):
		"""Test downloading an S3 object."""
		# Given: An S3 bucket with objects
		bucket = setup_s3_objects['bucket']
		key = setup_s3_objects['objects'][0]['key']
		local_path = os.path.join(temp_directory, 'downloaded_file.txt')

		# Create the file that would be downloaded
		with open(local_path, 'wb') as f:
			f.write(b'This is test content for object 1')

		with patch('bin.source_region.utils.aws_utils.s3_client') as mock_s3:
			# Configure mock
			def download_side_effect(bucket_name, key_name, filename):
				# File already created above
				assert bucket_name == bucket
				assert key_name == key
				assert filename == local_path
				return None

			mock_s3.download_file.side_effect = download_side_effect

			# When: We download the object
			result = get_s3_object(bucket, key, local_path)

			# Then: The download should be successful
			assert result is True
			assert os.path.exists(local_path)
			with open(local_path, 'rb') as f:
				assert f.read() == b'This is test content for object 1'

	def test_get_s3_object_nonexistent(self, s3_bucket, temp_directory):
		"""Test downloading a nonexistent S3 object."""
		# Given: A nonexistent object key
		key = 'nonexistent/object.txt'
		local_path = os.path.join(temp_directory, 'should_not_exist.txt')

		with patch('bin.source_region.utils.aws_utils.s3_client') as mock_s3:
			# We need to make sure the exception is wrapped in a try/except in the tested function
			error = ClientError(
				error_response={'Error': {'Code': 'NoSuchKey', 'Message': 'The specified key does not exist'}},
				operation_name='GetObject',
			)
			mock_s3.download_file = MagicMock(side_effect=error)

			# When: We try to download the nonexistent object
			result = get_s3_object(s3_bucket, key, local_path)

			# Then: The download should fail
			assert result is False
			assert not os.path.exists(local_path)

	def test_get_s3_object_metadata(self, setup_s3_objects):
		"""Test getting S3 object metadata."""
		# Given: An S3 bucket with tagged objects
		bucket = setup_s3_objects['bucket']
		key = setup_s3_objects['objects'][0]['key']

		with patch('bin.source_region.utils.aws_utils.s3_client') as mock_s3:
			# Configure mock response
			mock_last_modified = MagicMock()
			mock_last_modified.strftime.return_value = '2023-01-01 00:00:00'

			mock_response = {
				'ContentLength': 31,
				'ETag': '"1234567890abcdef"',
				'LastModified': mock_last_modified,
				'StorageClass': 'STANDARD',
				'Metadata': {},
			}
			mock_s3.head_object.return_value = mock_response

			# Configure get_object_tagging response
			mock_s3.get_object_tagging.return_value = {
				'TagSet': [{'Key': 'Purpose', 'Value': 'Testing'}, {'Key': 'Environment', 'Value': 'Dev'}]
			}

			# When: We get the object metadata
			metadata = get_s3_object_metadata(bucket, key)

			# Then: We should get the correct metadata
			assert metadata['source_bucket'] == bucket
			assert metadata['source_prefix'] == 'test'
			assert metadata['object_name'] == 'object1.txt'
			assert metadata['size'] == 31
			assert metadata['storage_class'] == 'STANDARD'

			# And the tags should be included
			assert len(metadata['tags']) == 2
			assert {'Purpose': 'Testing'} in metadata['tags']
			assert {'Environment': 'Dev'} in metadata['tags']

	def test_get_s3_object_metadata_no_tags(self, setup_s3_objects):
		"""Test getting S3 object metadata for an object without tags."""
		# Given: An S3 bucket with an untagged object
		bucket = setup_s3_objects['bucket']
		key = setup_s3_objects['objects'][1]['key']

		with patch('bin.source_region.utils.aws_utils.s3_client') as mock_s3:
			# Configure mock response
			mock_last_modified = MagicMock()
			mock_last_modified.strftime.return_value = '2023-01-01 00:00:00'

			mock_response = {
				'ContentLength': 31,
				'ETag': '"1234567890abcdef"',
				'LastModified': mock_last_modified,
				'StorageClass': 'STANDARD',
				'Metadata': {},
			}
			mock_s3.head_object.return_value = mock_response

			# Configure empty tag response
			mock_s3.get_object_tagging.return_value = {'TagSet': []}

			# When: We get the object metadata
			metadata = get_s3_object_metadata(bucket, key)

			# Then: We should get the correct metadata with empty tags
			assert metadata['source_bucket'] == bucket
			assert metadata['source_prefix'] == 'other'
			assert metadata['object_name'] == 'object2.txt'
			assert metadata['size'] == 31
			assert metadata['tags'] == []

	def test_get_s3_object_metadata_nonexistent(self, s3_bucket):
		"""Test getting metadata for a nonexistent S3 object."""
		# Given: A nonexistent object key
		key = 'nonexistent/object.txt'

		with patch('bin.source_region.utils.aws_utils.s3_client') as mock_s3:
			# We need to make sure the exception is wrapped in a try/except in the tested function
			error = ClientError(
				error_response={'Error': {'Code': 'NoSuchKey', 'Message': 'The specified key does not exist'}},
				operation_name='HeadObject',
			)
			mock_s3.head_object = MagicMock(side_effect=error)

			# When: We try to get metadata for the nonexistent object
			metadata = get_s3_object_metadata(s3_bucket, key)

			# Then: We should get an empty dictionary
			assert metadata == {}

	def test_upload_to_s3(self, s3_bucket, temp_directory):
		"""Test uploading a file to S3."""
		# Given: A local file to upload
		local_path = os.path.join(temp_directory, 'upload_file.txt')
		with open(local_path, 'w') as f:
			f.write('This is a test file for uploading')
		key = 'uploads/upload_file.txt'

		with patch('bin.source_region.utils.aws_utils.s3_client') as mock_s3:
			# Configure mock
			mock_s3.upload_file.return_value = None

			# When: We upload the file to S3
			result = upload_to_s3(local_path, s3_bucket, key)

			# Then: The upload should be successful
			assert result is True
			assert os.path.exists(local_path)  # Local file should exist

			# Verify s3_client.upload_file was called with the right parameters
			mock_s3.upload_file.assert_called_once_with(local_path, s3_bucket, key)

	def test_upload_to_s3_nonexistent_file(self, s3_bucket):
		"""Test uploading a nonexistent local file to S3."""
		with tempfile.TemporaryDirectory() as temp_directory:
			# Given: A nonexistent local file
			local_path = os.path.join(temp_directory, 'nonexistent_file.txt')
			key = 'uploads/should_not_exist.txt'

			with patch('bin.source_region.utils.aws_utils.s3_client') as mock_s3:
				# We need to make sure the exception is wrapped in a try/except in the tested function
				error = ClientError(
					error_response={'Error': {'Code': 'InvalidRequest', 'Message': 'File not found'}},
					operation_name='PutObject',
				)
				mock_s3.upload_file = MagicMock(side_effect=error)

				# When: We try to upload the nonexistent file
				result = upload_to_s3(local_path, s3_bucket, key)

				# Then: The upload should fail
				assert result is False


class TestDynamoDBOperations:
	"""Tests for DynamoDB operations."""

	def test_get_target_info_from_dynamodb(self, dynamodb_client, setup_dynamodb_parameters):
		"""Test getting target information from DynamoDB with prefix."""
		# Given: A DynamoDB table with parameters

		# When: We get target information for a bucket and prefix
		with patch('bin.source_region.utils.aws_utils._get_parameters_repository') as mock_get_repo:
			# Mock the parameters repository
			mock_repo = MagicMock()
			mock_repo.get_parameter_with_prefix.return_value = (
				'/test-stack/test-source-bucket/test',
				[
					{
						'region': 'us-west-2',
						'bucket': 'target-bucket-west',
						'kms_key_arn': 'arn:aws:kms:us-west-2:123456789012:key/test-key',
						'storage_class': 'STANDARD',
					},
					{'region': 'eu-west-1', 'bucket': 'target-bucket-eu', 'storage_class': 'STANDARD_IA'},
				],
			)
			mock_get_repo.return_value = mock_repo

			param_name, targets = get_target_info_from_dynamodb('test-stack', 'test-source-bucket', 'test')

		# Then: We should get the correct parameter name and targets
		assert param_name == '/test-stack/test-source-bucket/test'
		assert len(targets) == 2
		assert targets[0]['bucket'] == 'target-bucket-west'
		assert targets[1]['region'] == 'eu-west-1'

	def test_get_target_info_from_dynamodb_no_prefix(self, dynamodb_client, setup_dynamodb_parameters):
		"""Test getting target information from DynamoDB without prefix."""
		# Given: A DynamoDB table with parameters

		# When: We get target information for a bucket without prefix
		with patch('bin.source_region.utils.aws_utils._get_parameters_repository') as mock_get_repo:
			# Mock the parameters repository
			mock_repo = MagicMock()
			mock_repo.get_parameter_with_prefix.return_value = (
				'/test-stack/test-source-bucket',
				[{'region': 'us-west-1', 'bucket': 'target-bucket-default', 'storage_class': 'STANDARD'}],
			)
			mock_get_repo.return_value = mock_repo

			param_name, targets = get_target_info_from_dynamodb('test-stack', 'test-source-bucket')

		# Then: We should get the bucket-level parameter
		assert param_name == '/test-stack/test-source-bucket'
		assert len(targets) == 1
		assert targets[0]['bucket'] == 'target-bucket-default'

	def test_get_target_info_from_dynamodb_not_found(self, dynamodb_client):
		"""Test getting target information when not found in DynamoDB."""
		# Given: No matching parameters

		# When: We get target information for a nonexistent bucket
		with patch('bin.source_region.utils.aws_utils._get_parameters_repository') as mock_get_repo:
			# Mock the parameters repository
			mock_repo = MagicMock()
			mock_repo.get_parameter_with_prefix.return_value = ('', None)
			mock_get_repo.return_value = mock_repo

			param_name, targets = get_target_info_from_dynamodb('test-stack', 'nonexistent-bucket')

		# Then: We should get empty results
		assert param_name == ''
		assert targets == []


class TestCloudWatchOperations:
	"""Tests for CloudWatch operations."""

	def test_put_cloudwatch_metric(self, cloudwatch_client):
		"""Test putting a metric data point to CloudWatch."""
		# Given: CloudWatch metric data
		namespace = 'TestNamespace'
		metric_name = 'TestMetric'
		value = 123.45
		unit = 'Count'
		dimensions = [{'Name': 'TestDimension', 'Value': 'TestValue'}]

		# When: We put a metric data point to CloudWatch
		with patch('bin.source_region.utils.aws_utils.cloudwatch_client.put_metric_data') as mock_put_metric:
			result = put_cloudwatch_metric(namespace, metric_name, value, unit, dimensions)

			# Then: The operation should be successful
			assert result is True
			mock_put_metric.assert_called_once_with(
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
		# Given: CloudWatch metric data
		namespace = 'TestNamespace'
		metric_name = 'TestMetric'
		value = 123.45
		unit = 'Count'
		dimensions = [{'Name': 'TestDimension', 'Value': 'TestValue'}]

		with patch('bin.source_region.utils.aws_utils.cloudwatch_client') as mock_cw:
			# We need to make sure the exception is wrapped in a try/except in the tested function
			error = ClientError(
				error_response={'Error': {'Code': 'InternalServiceError', 'Message': 'CloudWatch service error'}},
				operation_name='PutMetricData',
			)
			mock_cw.put_metric_data = MagicMock(side_effect=error)

			# When: We try to put a metric
			result = put_cloudwatch_metric(namespace, metric_name, value, unit, dimensions)

			# Then: The function should handle the error and return False
			assert result is False
