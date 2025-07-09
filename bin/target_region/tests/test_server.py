"""
Unit tests for the server module in target_region.
"""

import os
from unittest.mock import patch
import tempfile

# Import the module under test
from bin.target_region.server import (
	process_s3_object,
	upload_object_to_targets,
	process_message_batch,
	signal_handler,
	main,
)


class TestSignalHandling:
	"""Tests for signal handling."""

	def test_signal_handler(self):
		"""Test signal handler sets running to False."""
		# Given: The global running variable is True
		import bin.target_region.server

		bin.target_region.server.running = True

		# When: We call the signal handler
		signal_handler(15, None)  # 15 is SIGTERM

		# Then: The running variable should be set to False
		assert bin.target_region.server.running is False

		# Reset for other tests
		bin.target_region.server.running = True


class TestS3ObjectProcessing:
	"""Tests for S3 object processing."""

	def test_process_s3_object_success(self):
		"""Test successful processing of an S3 object."""
		# Given: A mock S3 client and object information
		s3_object = {'bucket': 'test-staging-bucket', 'key': 'test/compressed_archive.tar.zstd'}

		with tempfile.TemporaryDirectory() as temp_directory:
			# Mock the get_s3_object function to simulate successful download
			with patch('bin.target_region.server.get_s3_object') as mock_get_s3:
				mock_get_s3.return_value = True

				# When: We process the S3 object
				success, local_path, object_info = process_s3_object(s3_object, temp_directory)

				# Then: The object should be processed successfully
				assert success is True
				assert local_path != ''
				assert os.path.dirname(local_path) == temp_directory
				assert object_info == s3_object

				# Verify the S3 object was downloaded
				mock_get_s3.assert_called_once_with(s3_object['bucket'], s3_object['key'], local_path)

	def test_process_s3_object_download_failure(self):
		"""Test handling download failure during S3 object processing."""
		# Given: A mock S3 client that fails to download
		s3_object = {'bucket': 'test-staging-bucket', 'key': 'test/nonexistent.tar.zstd'}

		with tempfile.TemporaryDirectory() as temp_directory:
			# Mock the get_s3_object function to simulate failed download
			with patch('bin.target_region.server.get_s3_object') as mock_get_s3:
				mock_get_s3.return_value = False

				# When: We process the S3 object
				success, local_path, object_info = process_s3_object(s3_object, temp_directory)

				# Then: The function should handle the failure
				assert success is False
				assert local_path == ''
				assert object_info == {}

	def test_process_s3_object_exception(self):
		"""Test handling exceptions during S3 object processing."""
		# Given: A mock S3 client that raises an exception
		s3_object = {'bucket': 'test-staging-bucket', 'key': 'test/error.tar.zstd'}

		with tempfile.TemporaryDirectory() as temp_directory:
			# Mock the get_s3_object function to raise an exception
			with patch('bin.target_region.server.get_s3_object') as mock_get_s3:
				mock_get_s3.side_effect = Exception('Test exception')

				# When: We process the S3 object
				success, local_path, object_info = process_s3_object(s3_object, temp_directory)

				# Then: The function should handle the exception
				assert success is False
				assert local_path == ''
				assert object_info == {}


class TestObjectUpload:
	"""Tests for object upload to targets."""

	def test_upload_object_to_targets_current_region(self):
		"""Test uploading object to targets in current region."""
		with tempfile.TemporaryDirectory() as temp_directory:
			local_file = os.path.join(temp_directory, 'test_file.txt')
			
			# Given: Object information with targets in current region
			object_info = {
				'object_name': 'test_file.txt',
				'local_path': local_file,
				'source_prefix': 'test',
				'storage_class': 'STANDARD',
				'targets': [
					{
						'region': 'us-east-1',  # Current region
						'bucket': 'test-target-bucket',
					},
					{
						'region': 'us-west-2',  # Different region
						'bucket': 'test-target-west-bucket',
						'storage_class': 'STANDARD_IA',
					},
				],
			}

			# Mock the necessary functions
			with (
				patch('bin.target_region.server.get_current_region') as mock_get_region,
				patch('bin.target_region.server.os.path.exists') as mock_exists,
				patch('bin.target_region.server.prepare_object_tags') as mock_prepare_tags,
				patch('bin.target_region.server.upload_to_s3') as mock_upload,
			):
				# Configure mocks
				mock_get_region.return_value = 'us-east-1'
				mock_exists.return_value = True
				mock_prepare_tags.return_value = {'Purpose': 'Testing'}
				mock_upload.return_value = True

				# When: We upload the object
				success = upload_object_to_targets(object_info)

				# Then: The upload should succeed
				assert success is True

				# Verify only current region target was uploaded to
				mock_upload.assert_called_once()
				args = mock_upload.call_args[0]
				assert args[1] == 'test-target-bucket'  # Bucket name
				assert args[2] == 'test/test_file.txt'  # Target key with prefix
				assert args[3] == {'Purpose': 'Testing'}  # Tags
				assert args[4] == 'STANDARD'  # Storage class
				assert args[5] is None  # KMS key ARN

	def test_upload_object_to_targets_with_storage_class_override(self):
		"""Test uploading object with storage class override from target config."""
		with tempfile.TemporaryDirectory() as temp_directory:
			local_file = os.path.join(temp_directory, 'test_file.txt')
			
			# Given: Object information with storage class override in target
			object_info = {
				'object_name': 'test_file.txt',
				'local_path': local_file,
				'source_prefix': 'test',
				'storage_class': 'STANDARD',
				'targets': [
					{
						'region': 'us-east-1',
						'bucket': 'test-target-bucket',
						'storage_class': 'STANDARD_IA',  # Override in target
					}
				],
			}

			# Mock the necessary functions
			with (
				patch('bin.target_region.server.get_current_region') as mock_get_region,
				patch('bin.target_region.server.os.path.exists') as mock_exists,
				patch('bin.target_region.server.prepare_object_tags') as mock_prepare_tags,
				patch('bin.target_region.server.upload_to_s3') as mock_upload,
			):
				# Configure mocks
				mock_get_region.return_value = 'us-east-1'
				mock_exists.return_value = True
				mock_prepare_tags.return_value = {'Purpose': 'Testing'}
				mock_upload.return_value = True

				# When: We upload the object
				success = upload_object_to_targets(object_info)

				# Then: The upload should succeed
				assert success is True

				# Verify the storage class was overridden
				mock_upload.assert_called_once()
				args = mock_upload.call_args[0]
				assert args[4] == 'STANDARD_IA'  # Overridden from target (positional arg)

	def test_upload_object_to_targets_with_kms(self):
		"""Test uploading object with KMS encryption."""
		with tempfile.TemporaryDirectory() as temp_directory:
			local_file = os.path.join(temp_directory, 'test_file.txt')
			
			# Given: Object information with KMS key in target
			object_info = {
				'object_name': 'test_file.txt',
				'local_path': local_file,
				'source_prefix': 'test',
				'targets': [
					{
						'region': 'us-east-1',
						'bucket': 'test-target-bucket',
						'kms_key_arn': 'arn:aws:kms:us-east-1:123456789012:key/test-key',
					}
				],
			}

			# Mock the necessary functions
			with (
				patch('bin.target_region.server.get_current_region') as mock_get_region,
				patch('bin.target_region.server.os.path.exists') as mock_exists,
				patch('bin.target_region.server.prepare_object_tags') as mock_prepare_tags,
				patch('bin.target_region.server.upload_to_s3') as mock_upload,
			):
				# Configure mocks
				mock_get_region.return_value = 'us-east-1'
				mock_exists.return_value = True
				mock_prepare_tags.return_value = {'Purpose': 'Testing'}
				mock_upload.return_value = True

				# When: We upload the object
				success = upload_object_to_targets(object_info)

				# Then: The upload should succeed
				assert success is True

				# Verify the KMS key was used
				mock_upload.assert_called_once()
				args = mock_upload.call_args[0]
				assert args[5] == 'arn:aws:kms:us-east-1:123456789012:key/test-key'  # KMS key ARN (positional arg)

	def test_upload_object_to_targets_no_targets_in_current_region(self):
		"""Test uploading object when no targets are in current region."""
		with tempfile.TemporaryDirectory() as temp_directory:
			local_file = os.path.join(temp_directory, 'test_file.txt')
			
			# Given: Object information with targets only in other regions
			object_info = {
				'object_name': 'test_file.txt',
				'local_path': local_file,
				'targets': [
					{
						'region': 'us-west-2',  # Different region
						'bucket': 'test-target-west-bucket',
					},
					{
						'region': 'eu-west-1',  # Different region
						'bucket': 'test-target-eu-bucket',
					},
				],
			}

			# Mock the necessary functions
			with (
				patch('bin.target_region.server.get_current_region') as mock_get_region,
				patch('bin.target_region.server.os.path.exists') as mock_exists,
				patch('bin.target_region.server.prepare_object_tags') as mock_prepare_tags,
				patch('bin.target_region.server.upload_to_s3') as mock_upload,
			):
				# Configure mocks
				mock_get_region.return_value = 'us-east-1'
				mock_exists.return_value = True

				# When: We upload the object
				success = upload_object_to_targets(object_info)

				# Then: The function should succeed without uploading
				assert success is True
				mock_upload.assert_not_called()
			# The implementation calls prepare_object_tags anyway, so we can't assert it wasn't called
			# Instead, verify it was called with the expected arguments
			mock_prepare_tags.assert_called_once_with(object_info)

	def test_upload_object_to_targets_missing_local_file(self):
		"""Test uploading object when local file doesn't exist."""
		# Given: Object information with nonexistent local file
		object_info = {
			'object_name': 'test_file.txt',
			'local_path': '/tmp/extracted/nonexistent.txt',
			'targets': [{'region': 'us-east-1', 'bucket': 'test-target-bucket'}],
		}

		# Mock the necessary functions
		with (
			patch('bin.target_region.server.get_current_region') as mock_get_region,
			patch('bin.target_region.server.os.path.exists') as mock_exists,
			patch('bin.target_region.server.upload_to_s3') as mock_upload,
		):
			# Configure mocks
			mock_get_region.return_value = 'us-east-1'
			mock_exists.return_value = False  # File doesn't exist

			# When: We try to upload the object
			success = upload_object_to_targets(object_info)

			# Then: The upload should fail
			assert success is False
			mock_upload.assert_not_called()

	def test_upload_object_to_targets_upload_failure(self):
		"""Test handling upload failure."""
		# Given: Object information with target in current region
		object_info = {
			'object_name': 'test_file.txt',
			'local_path': '/tmp/extracted/test_file.txt',
			'targets': [{'region': 'us-east-1', 'bucket': 'test-target-bucket'}],
		}

		# Mock the necessary functions
		with (
			patch('bin.target_region.server.get_current_region') as mock_get_region,
			patch('bin.target_region.server.os.path.exists') as mock_exists,
			patch('bin.target_region.server.prepare_object_tags') as mock_prepare_tags,
			patch('bin.target_region.server.upload_to_s3') as mock_upload,
		):
			# Configure mocks
			mock_get_region.return_value = 'us-east-1'
			mock_exists.return_value = True
			mock_prepare_tags.return_value = {'Purpose': 'Testing'}
			mock_upload.return_value = False  # Upload fails

			# When: We try to upload the object
			success = upload_object_to_targets(object_info)

			# Then: The upload should fail
			assert success is False
			mock_upload.assert_called_once()

	def test_upload_object_to_targets_multiple_targets_partial_failure(self):
		"""Test handling partial failure with multiple targets in current region."""
		# Given: Object information with multiple targets in current region
		object_info = {
			'object_name': 'test_file.txt',
			'local_path': '/tmp/extracted/test_file.txt',
			'targets': [
				{'region': 'us-east-1', 'bucket': 'test-target-bucket-1'},
				{'region': 'us-east-1', 'bucket': 'test-target-bucket-2'},
			],
		}

		# Mock the necessary functions
		with (
			patch('bin.target_region.server.get_current_region') as mock_get_region,
			patch('bin.target_region.server.os.path.exists') as mock_exists,
			patch('bin.target_region.server.prepare_object_tags') as mock_prepare_tags,
			patch('bin.target_region.server.upload_to_s3') as mock_upload,
		):
			# Configure mocks
			mock_get_region.return_value = 'us-east-1'
			mock_exists.return_value = True
			mock_prepare_tags.return_value = {'Purpose': 'Testing'}
			# First upload succeeds, second fails
			mock_upload.side_effect = [True, False]

			# When: We try to upload the object
			success = upload_object_to_targets(object_info)

			# Then: The upload should fail overall
			assert success is False
			assert mock_upload.call_count == 2


class TestMessageBatchProcessing:
	"""Tests for SQS message batch processing."""

	def test_process_message_batch_empty(self, setup_environment_variables):
		"""Test processing an empty message batch."""
		# Given: A queue URL but no messages
		queue_url = os.environ.get('SQS_QUEUE_URL')

		# Mock the SQS client to return no messages
		with patch('bin.target_region.server.get_sqs_messages') as mock_get_messages:
			mock_get_messages.return_value = []

			# When: We process the message batch
			processed = process_message_batch(queue_url)

			# Then: No messages should be processed
			assert processed == 0

	def test_process_message_batch_test_events(self, setup_environment_variables, s3_test_event):
		"""Test processing a batch with only test events."""
		# Given: A queue URL with test events
		queue_url = os.environ.get('SQS_QUEUE_URL')

		# Mock the necessary functions
		with (
			patch('bin.target_region.server.get_sqs_messages') as mock_get_messages,
			patch('bin.target_region.server.is_s3_test_event') as mock_is_test,
			patch('bin.target_region.server.delete_sqs_messages_batch') as mock_delete_batch,
		):
			# Configure mocks
			mock_get_messages.return_value = [s3_test_event]
			mock_is_test.return_value = True

			# When: We process the message batch
			processed = process_message_batch(queue_url)

			# Then: The test event should be deleted without processing
			assert processed == 1
			mock_delete_batch.assert_called_once_with(queue_url, [s3_test_event['ReceiptHandle']])

	def test_process_message_batch_full_flow(self, setup_environment_variables, sample_s3_event, temp_directory):
		"""Test processing a batch with a full successful flow."""
		# Given: A queue URL with a message containing an S3 event
		queue_url = os.environ.get('SQS_QUEUE_URL')

		# Mock all the necessary functions for a full flow
		with (
			patch('bin.target_region.server.get_sqs_messages') as mock_get_messages,
			patch('bin.target_region.server.is_s3_test_event') as mock_is_test,
			patch('bin.target_region.server.extract_s3_event_info') as mock_extract_info,
			patch('bin.target_region.server.create_temp_directory') as mock_create_temp,
			patch('bin.target_region.server.process_s3_object') as mock_process_obj,
			patch('bin.target_region.server.decompress_and_extract') as mock_decompress,
			patch('bin.target_region.server.read_manifest_from_file') as mock_read_manifest,
			patch('bin.target_region.server.get_tar_members') as mock_get_members,
			patch('bin.target_region.server.get_object_paths_from_manifest') as mock_get_paths,
			patch('bin.target_region.server.stream_extract_file') as mock_stream_extract,
			patch('bin.target_region.server.upload_object_to_targets') as mock_upload,
			patch('bin.target_region.server.report_decompression_metrics') as mock_report_metrics,
			patch('bin.target_region.server.delete_s3_object') as mock_delete_obj,
			patch('bin.target_region.server.delete_sqs_messages_batch') as mock_delete_batch,
			patch('bin.target_region.server.cleanup_temp_directory') as mock_cleanup,
			patch('bin.target_region.server.os.path.exists') as mock_exists,
			patch('bin.target_region.server.os.remove') as mock_remove,
		):
			# Configure mocks for successful flow
			mock_get_messages.return_value = [sample_s3_event]
			mock_is_test.return_value = False
			mock_extract_info.return_value = [
				{'bucket': 'test-staging-bucket', 'key': 'test/compressed_archive.tar.zstd'}
			]
			mock_create_temp.return_value = temp_directory

			mock_process_obj.return_value = (
				True,
				os.path.join(temp_directory, 'archive.tar.zstd'),
				{'bucket': 'test-staging-bucket', 'key': 'test/compressed_archive.tar.zstd'},
			)

			extract_dir = os.path.join(temp_directory, 'extracted')
			mock_decompress.return_value = (True, extract_dir, 1000, 5000)

			manifest_path = os.path.join(extract_dir, 'manifest.json')
			mock_exists.return_value = True

			# Create a mock manifest
			mock_manifest = {
				'format_version': '1.0',
				'objects': [{'object_name': 'test_file.txt', 'relative_key': 'file.txt'}],
				'targets': [{'region': 'us-east-1', 'bucket': 'test-target-bucket'}],
			}
			mock_read_manifest.return_value = mock_manifest

			mock_get_members.return_value = ['manifest.json', 'objects/file.txt']

			mock_get_paths.return_value = [
				{
					'object_name': 'test_file.txt',
					'relative_key': 'file.txt',
					'local_path': os.path.join(extract_dir, 'objects/file.txt'),
					'targets': [{'region': 'us-east-1', 'bucket': 'test-target-bucket'}],
				}
			]

			mock_stream_extract.return_value = True
			mock_upload.return_value = True
			mock_delete_obj.return_value = True

			# When: We process the message batch
			processed = process_message_batch(queue_url)

			# Then: The message should be processed successfully
			assert processed == 1

			# Verify the full flow was executed
			mock_get_messages.assert_called_once()
			mock_extract_info.assert_called_once()
			mock_create_temp.assert_called_once()
			mock_process_obj.assert_called_once()
			mock_decompress.assert_called_once()
			mock_read_manifest.assert_called_once()
			mock_get_members.assert_called_once()
			mock_get_paths.assert_called_once()
			mock_stream_extract.assert_called_once()
			mock_upload.assert_called_once()
			mock_report_metrics.assert_called_once()
			mock_delete_obj.assert_called_once()
			mock_delete_batch.assert_called_once_with(queue_url, [sample_s3_event['ReceiptHandle']])
			mock_cleanup.assert_called_once()

	def test_process_message_batch_object_download_failure(
		self, setup_environment_variables, sample_s3_event, temp_directory
	):
		"""Test handling object download failure during message processing."""
		# Given: A queue URL with a message but download will fail
		queue_url = os.environ.get('SQS_QUEUE_URL')

		# Mock the necessary functions
		with (
			patch('bin.target_region.server.get_sqs_messages') as mock_get_messages,
			patch('bin.target_region.server.is_s3_test_event') as mock_is_test,
			patch('bin.target_region.server.extract_s3_event_info') as mock_extract_info,
			patch('bin.target_region.server.create_temp_directory') as mock_create_temp,
			patch('bin.target_region.server.process_s3_object') as mock_process_obj,
			patch('bin.target_region.server.delete_sqs_messages_batch') as mock_delete_batch,
			patch('bin.target_region.server.cleanup_temp_directory') as mock_cleanup,
		):
			# Configure mocks
			mock_get_messages.return_value = [sample_s3_event]
			mock_is_test.return_value = False
			mock_extract_info.return_value = [{'bucket': 'test-staging-bucket', 'key': 'test/nonexistent.tar.zstd'}]
			mock_create_temp.return_value = temp_directory

			# Configure object download to fail
			mock_process_obj.return_value = (False, '', {})

			# When: We process the message batch
			processed = process_message_batch(queue_url)

			# Then: The message should still be considered processed
			assert processed == 1

			# Verify the error was handled
			mock_process_obj.assert_called_once()
			mock_delete_batch.assert_called_once_with(queue_url, [sample_s3_event['ReceiptHandle']])
			mock_cleanup.assert_called_once()

	def test_process_message_batch_decompress_failure(
		self, setup_environment_variables, sample_s3_event, temp_directory
	):
		"""Test handling decompression failure during message processing."""
		# Given: A queue URL with a message but decompression will fail
		queue_url = os.environ.get('SQS_QUEUE_URL')

		# Mock the necessary functions
		with (
			patch('bin.target_region.server.get_sqs_messages') as mock_get_messages,
			patch('bin.target_region.server.is_s3_test_event') as mock_is_test,
			patch('bin.target_region.server.extract_s3_event_info') as mock_extract_info,
			patch('bin.target_region.server.create_temp_directory') as mock_create_temp,
			patch('bin.target_region.server.process_s3_object') as mock_process_obj,
			patch('bin.target_region.server.decompress_and_extract') as mock_decompress,
			patch('bin.target_region.server.delete_sqs_messages_batch') as mock_delete_batch,
			patch('bin.target_region.server.cleanup_temp_directory') as mock_cleanup,
		):
			# Configure mocks
			mock_get_messages.return_value = [sample_s3_event]
			mock_is_test.return_value = False
			mock_extract_info.return_value = [
				{'bucket': 'test-staging-bucket', 'key': 'test/compressed_archive.tar.zstd'}
			]
			mock_create_temp.return_value = temp_directory

			mock_process_obj.return_value = (
				True,
				os.path.join(temp_directory, 'archive.tar.zstd'),
				{'bucket': 'test-staging-bucket', 'key': 'test/compressed_archive.tar.zstd'},
			)

			# Configure decompression to fail
			mock_decompress.return_value = (False, '', 0, 0)

			# When: We process the message batch
			processed = process_message_batch(queue_url)

			# Then: The message should still be considered processed
			assert processed == 1

			# Verify the error was handled
			mock_process_obj.assert_called_once()
			mock_decompress.assert_called_once()
			mock_delete_batch.assert_called_once_with(queue_url, [sample_s3_event['ReceiptHandle']])
			mock_cleanup.assert_called_once()


class TestMainFunction:
	"""Tests for the main function."""

	def test_main_function(self, setup_environment_variables):
		"""Test the main function loop."""
		# Given: Environment variables are set
		queue_url = os.environ.get('SQS_QUEUE_URL')

		# Mock the necessary functions
		with (
			patch('bin.target_region.server.process_message_batch') as mock_process_batch,
			patch('bin.target_region.server.time.sleep') as mock_sleep,
			patch('bin.target_region.server.signal') as mock_signal,
		):
			# Configure mocks
			# First call processes messages, second call returns 0 messages to trigger sleep
			mock_process_batch.side_effect = [1, 0]

			# Need to stop the infinite loop
			def stop_loop(*args):
				# Set running to False after first sleep
				import bin.target_region.server

				bin.target_region.server.running = False

			mock_sleep.side_effect = stop_loop

			# When: We run the main function
			main()

			# Then: The batch should be processed and signal handlers registered
			assert mock_process_batch.call_count == 2
			mock_process_batch.assert_called_with(queue_url)
			mock_sleep.assert_called_once()

			# Verify signal handlers were registered
			assert mock_signal.signal.call_count == 2

	def test_main_function_exception(self, setup_environment_variables):
		"""Test the main function handles exceptions in the processing loop."""
		# Given: Environment variables are set
		queue_url = os.environ.get('SQS_QUEUE_URL')

		# Define a simpler approach that will allow us to verify exception handling
		# without depending on mocking the function itself

		# First we'll force the main function to exit after one iteration
		import bin.target_region.server

		bin.target_region.server.running = True

		# Now set up a mock for process_message_batch that will raise an exception
		with patch('bin.target_region.server.process_message_batch') as mock_process_batch:
			# Configure mock to raise exception
			mock_process_batch.side_effect = Exception('Test exception')

			# And force the loop to exit after the first exception
			def stop_loop(*args, **kwargs):
				bin.target_region.server.running = False

			# Capture when time.sleep is called, this will indicate exception was handled
			with patch('bin.target_region.server.time.sleep', side_effect=stop_loop) as mock_sleep:
				# When: We run the main function
				main()

				# Then: The exception should be handled and we'll see the sleep call
				# indicating the retry delay
				mock_sleep.assert_called()  # Verify sleep was called, which means exception was caught
